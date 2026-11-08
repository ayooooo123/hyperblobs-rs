//! Opt-in allocation measurement for borrowed uploads and single-block reads.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use hyperblobs::Blobs;
use hypercore::crypto::KeyPair;
use hypercore::storage::Store;
use hypercore::{Core, CoreOptions, GetOptions};

struct Allocator;
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED.fetch_add(size as u64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

#[tokio::test]
#[ignore = "64 MiB borrowed upload allocation benchmark, run in release mode"]
async fn borrowed_upload_allocations() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    let core = Core::open(
        &store,
        CoreOptions {
            key_pair: Some(KeyPair::from_seed(&[0x42; 32])),
            ..Default::default()
        },
    )
    .unwrap();
    let blobs = Blobs::new(core, 64 * 1024);
    let data = vec![42; 64 * 1024 * 1024 + 1];
    let start = Instant::now();
    COUNTING.store(true, Ordering::Relaxed);
    let id = blobs.put(&data).await.unwrap();
    COUNTING.store(false, Ordering::Relaxed);
    eprintln!(
        "PERF borrowed-upload blocks={} elapsed_ms={:.3} allocations={} allocated_bytes={}",
        id.block_length,
        start.elapsed().as_secs_f64() * 1000.0,
        ALLOCATIONS.load(Ordering::Relaxed),
        ALLOCATED.load(Ordering::Relaxed),
    );
    assert!(
        ALLOCATED.load(Ordering::Relaxed) < data.len() as u64 + 8 * 1024 * 1024,
        "upload must not allocate another full copy of the payload"
    );
    assert_eq!(id.block_length, 1025);
    assert_eq!(id.byte_length, data.len() as u64);
    assert_eq!(
        blobs
            .get(&id, GetOptions::default())
            .await
            .unwrap()
            .unwrap(),
        data
    );
}
