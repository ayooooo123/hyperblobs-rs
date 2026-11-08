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

- `put` splits borrowed data by the block size and appends borrowed slices in
  groups of 16, without allocating a payload copy for each block. Streaming
  writers append owned `Bytes` chunks with the same batching. Both hold the
  store's lock, so concurrent blobs never interleave.
- `BlobId` implements the four-uint compact encoding PearTube puts in blob links.
- Range reads, the sliding download window and the HTTP server are in
  [byte-stream-rs](../byte-stream-rs).

Not ported: block maps and dedup (`blockMap`, `dedup`), monitors, batches.

`cargo test --release --test allocations -- --ignored --nocapture` measures an
isolated 64 MiB + 1 byte upload (1,025 blocks) and checks the stored bytes. The
allocation assertion rejects a second full payload allocation. Three-run release
medians on Apple Silicon: 148.764 → 110.446 ms, 9,006 → 8,003 allocation calls,
129.5 → 66.1 MiB allocated. Whole-process CPU stayed about 0.18 s; peak RSS
varied 167.6–176.4 MiB before and 165.2–170.5 MiB after, including the input and
full readback. Single-block `get` already returns the core's `Bytes` directly;
assembling a multi-block contiguous result still necessarily copies.

## License

MIT OR Apache-2.0
