# hyperblobs-rs

[Hyperblobs](https://github.com/holepunchto/hyperblobs) 2 for [hypercore-rs](../hypercore-rs):
blobs stored as runs of blocks in one core, with the same layout and ids as npm hyperblobs, so
either side reads blobs the other wrote.

```rust,no_run
use hyperblobs::{Blobs, BlobId};
use hypercore::GetOptions;

# async fn demo(core: hypercore::Core) -> Result<(), hypercore::CoreError> {
let blobs = Blobs::new(core, 512 * 1024);
let id: BlobId = blobs.put(b"hello").await?; // {blockOffset, blockLength, byteOffset, byteLength}
let mut w = blobs.writer().await;            // createWriteStream: one block per chunk
w.write(bytes::Bytes::from_static(b"chunk")).await?;
let other = w.finish().await?;
assert_eq!(blobs.get(&id, GetOptions::default()).await?.as_deref(), Some(&b"hello"[..]));
# Ok(()) }
```

- `put` splits by the block size; a writer appends each chunk as one block, 16 blocks per append.
  Writers hold the store's lock, so concurrent blobs never interleave.
- `BlobId` implements the four-uint compact encoding PearTube puts in blob links.
- Range reads, the sliding download window and the HTTP server are in
  [byte-stream-rs](../byte-stream-rs).

Not ported: block maps and dedup (`blockMap`, `dedup`), monitors, batches.

## License

MIT OR Apache-2.0
