#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use compact_encoding::{Codec, Reader, Result as EncResult, Writer, uint_len};
use hypercore::{Core, CoreError, GetOptions};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// npm hyperblobs' default block size.
pub const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

/// Blocks a writer buffers before one append (npm hyperblobs: 16).
const APPEND_BATCH: usize = 16;

/// Where a blob lives in its core. The compact encoding (four uints) is the
/// one PearTube puts in blob links.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct BlobId {
    pub block_offset: u64,
    pub block_length: u64,
    pub byte_offset: u64,
    pub byte_length: u64,
}

impl BlobId {
    /// Block index one past the blob.
    pub fn block_end(&self) -> u64 {
        self.block_offset + self.block_length
    }
}

impl Codec for BlobId {
    fn encoded_len(&self) -> usize {
        uint_len(self.block_offset)
            + uint_len(self.block_length)
            + uint_len(self.byte_offset)
            + uint_len(self.byte_length)
    }
    fn encode(&self, w: &mut Writer) {
        w.uint(self.block_offset);
        w.uint(self.block_length);
        w.uint(self.byte_offset);
        w.uint(self.byte_length);
    }
    fn decode(r: &mut Reader) -> EncResult<Self> {
        Ok(Self {
            block_offset: r.uint()?,
            block_length: r.uint()?,
            byte_offset: r.uint()?,
            byte_length: r.uint()?,
        })
    }
}

/// A blob store over one core. Clones share the write lock.
#[derive(Clone)]
pub struct Blobs {
    core: Core,
    block_size: usize,
    lock: Arc<Mutex<()>>,
}

impl Blobs {
    pub fn new(core: Core, block_size: usize) -> Self {
        assert!(block_size > 0, "block size must be positive");
        Self {
            core,
            block_size,
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn core(&self) -> &Core {
        &self.core
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Stores `data` split into `block_size` blocks.
    pub async fn put(&self, data: &[u8]) -> Result<BlobId, CoreError> {
        let mut w = self.writer().await;
        for chunk in data.chunks(self.block_size) {
            w.write(Bytes::copy_from_slice(chunk)).await?;
        }
        w.finish().await
    }

    /// A streaming writer: each written chunk becomes one block, as in npm
    /// `createWriteStream`. Writers are exclusive, so ids stay contiguous.
    pub async fn writer(&self) -> BlobWriter {
        let guard = self.lock.clone().lock_owned().await;
        BlobWriter {
            core: self.core.clone(),
            start: (self.core.length(), self.core.byte_length()),
            batch: Vec::with_capacity(APPEND_BATCH),
            _guard: guard,
        }
    }

    /// The whole blob; `None` if a block is missing and `opts.wait` is off.
    pub async fn get(&self, id: &BlobId, opts: GetOptions) -> Result<Option<Bytes>, CoreError> {
        if id.block_length == 0 {
            return Ok(Some(Bytes::new()));
        }
        // One range fetches every block in parallel; the gets below then wait in order.
        let _range = opts.wait.then(|| {
            self.core
                .download(id.block_offset, Some(id.block_end()), false)
        });
        if id.block_length == 1 {
            return self.core.get(id.block_offset, opts).await;
        }
        let mut out = BytesMut::with_capacity(id.byte_length as usize);
        for i in id.block_offset..id.block_end() {
            match self.core.get(i, opts).await? {
                Some(b) => out.extend_from_slice(&b),
                None => return Ok(None),
            }
        }
        Ok(Some(out.freeze()))
    }

    /// Drops the blob's blocks from local storage.
    pub async fn clear(&self, id: &BlobId) -> Result<(), CoreError> {
        self.core.clear(id.block_offset, id.block_end()).await
    }
}

/// Holds the store's write lock until finished or dropped.
pub struct BlobWriter {
    core: Core,
    start: (u64, u64),
    batch: Vec<Bytes>,
    _guard: OwnedMutexGuard<()>,
}

impl BlobWriter {
    /// Appends `chunk` as one block (buffered, flushed every 16 blocks).
    pub async fn write(&mut self, chunk: Bytes) -> Result<(), CoreError> {
        self.batch.push(chunk);
        if self.batch.len() >= APPEND_BATCH {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), CoreError> {
        if !self.batch.is_empty() {
            self.core.append(&self.batch).await?;
            self.batch.clear();
        }
        Ok(())
    }

    /// Appends what is buffered and returns the blob's id.
    pub async fn finish(mut self) -> Result<BlobId, CoreError> {
        self.flush().await?;
        let (block_offset, byte_offset) = self.start;
        Ok(BlobId {
            block_offset,
            block_length: self.core.length() - block_offset,
            byte_offset,
            byte_length: self.core.byte_length() - byte_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypercore::CoreOptions;
    use hypercore::crypto::KeyPair;
    use hypercore::storage::Store;

    fn core(dir: &std::path::Path) -> Core {
        let store = Store::open(dir).unwrap();
        let opts = CoreOptions {
            key_pair: Some(KeyPair::from_seed(&[1; 32])),
            ..Default::default()
        };
        Core::open(&store, opts).unwrap()
    }

    #[tokio::test]
    async fn ids_follow_npm_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let blobs = Blobs::new(core(tmp.path()), 4);
        let a = blobs.put(b"hello world").await.unwrap();
        assert_eq!(
            a,
            BlobId {
                block_offset: 0,
                block_length: 3,
                byte_offset: 0,
                byte_length: 11
            }
        );
        let b = blobs.put(b"").await.unwrap();
        assert_eq!(
            b,
            BlobId {
                block_offset: 3,
                block_length: 0,
                byte_offset: 11,
                byte_length: 0
            }
        );
        let mut w = blobs.writer().await;
        for chunk in [&b"abcdefg"[..], b"h", b"ij"] {
            w.write(Bytes::from_static(chunk)).await.unwrap();
        }
        let c = w.finish().await.unwrap();
        assert_eq!(
            c,
            BlobId {
                block_offset: 3,
                block_length: 3,
                byte_offset: 11,
                byte_length: 10
            }
        );
        let get = |id| {
            let blobs = blobs.clone();
            async move {
                blobs
                    .get(&id, GetOptions::default())
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        assert_eq!(get(a).await, &b"hello world"[..]);
        assert_eq!(get(b).await, &b""[..]);
        assert_eq!(get(c).await, &b"abcdefghij"[..]);
        blobs.clear(&a).await.unwrap();
        let wait = GetOptions {
            wait: false,
            ..Default::default()
        };
        assert_eq!(blobs.get(&a, wait).await.unwrap(), None);
    }

    #[test]
    fn id_encoding_is_four_uints() {
        let id = BlobId {
            block_offset: 1,
            block_length: 300,
            byte_offset: 70000,
            byte_length: 0,
        };
        let enc = compact_encoding::encode(&id);
        assert_eq!(
            &enc[..],
            &[1, 0xfd, 0x2c, 0x01, 0xfe, 0x70, 0x11, 0x01, 0x00, 0][..]
        );
        assert_eq!(compact_encoding::decode::<BlobId>(enc).unwrap(), id);
    }
}
