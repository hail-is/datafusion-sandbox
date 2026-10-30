//! The in-memory object store a test registers on its own session, and its views that fail the
//! writes under a prefix.

use crate::testing::{fixture::block_on, memory_store::MemoryStore};

use datafusion::{datasource::listing::ListingTableUrl, prelude::SessionContext};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart, path::Path};
use std::sync::Arc;

fn put(
    store: &Arc<dyn ObjectStore>,
    location: &str,
    bytes: &'static [u8],
) -> object_store::Result<()> {
    block_on(store.put(&Path::from(location), PutPayload::from_static(bytes))).map(drop)
}

fn get(store: &Arc<dyn ObjectStore>, location: &str) -> object_store::Result<Vec<u8>> {
    block_on(async { store.get(&Path::from(location)).await?.bytes().await })
        .map(|bytes| bytes.to_vec())
}

#[test]
fn a_new_store_is_empty_and_reached_at_memory_slash_slash_its_name() {
    let store = MemoryStore::new("fresh");

    assert_eq!(store.url().as_str(), "memory://fresh/");
    assert_eq!(block_on(store.locations_under("")), Vec::<Path>::new());
}

#[test]
#[should_panic(expected = "parsing the a/b memory store URL")]
fn a_name_that_is_not_a_url_authority_panics() {
    drop(MemoryStore::new("a/b"));
}

#[test]
fn clones_share_the_store_and_new_stores_do_not() {
    let store = MemoryStore::new("shared");
    let clone = store.clone();
    let other = MemoryStore::new("shared");

    put(clone.store(), "data/a", b"a").unwrap();

    assert_eq!(get(store.store(), "data/a").unwrap(), b"a");
    assert_eq!(block_on(other.locations_under("")), Vec::<Path>::new());
}

#[test]
fn a_registered_store_serves_its_url_on_the_session() {
    let store = MemoryStore::new("registered");
    put(store.store(), "data/a", b"a").unwrap();
    let ctx = SessionContext::new();

    store.register(&ctx);

    let url = ListingTableUrl::parse("memory://registered/data/a").unwrap();
    let served = ctx.runtime_env().object_store(&url).unwrap();
    assert_eq!(get(&served, "data/a").unwrap(), b"a");
}

#[test]
fn locations_under_lists_every_object_beneath_whole_prefix_segments_in_path_order() {
    let store = MemoryStore::new("listed");
    for location in ["out/b", "out/a/2", "out/a/1", "outer/c", "elsewhere"] {
        put(store.store(), location, b"").unwrap();
    }

    let locations = block_on(store.locations_under("out"));

    assert_eq!(
        locations,
        ["out/a/1", "out/a/2", "out/b"].map(Path::from).to_vec()
    );
}

#[test]
fn failing_writes_under_fails_writes_beneath_the_prefix_and_serves_the_rest() {
    let store = MemoryStore::failing_writes_under("failing", "metrics/runs");

    let failed = put(store.store(), "metrics/runs/run-a.parquet", b"a");
    put(store.store(), "metrics/progress/run-a.parquet", b"b").unwrap();

    assert!(failed.is_err());
    assert_eq!(store.url().as_str(), "memory://failing/");
    assert_eq!(
        block_on(store.locations_under("")),
        [Path::from("metrics/progress/run-a.parquet")]
    );
    assert_eq!(
        get(store.store(), "metrics/progress/run-a.parquet").unwrap(),
        b"b"
    );
}

#[test]
#[should_panic(expected = "parsing the a/b memory store URL")]
fn a_failing_store_whose_name_is_not_a_url_authority_panics() {
    drop(MemoryStore::failing_writes_under("a/b", "out"));
}

/// Puts, uploads in parts, and copies from `source` to `location` on `store`, each write's
/// result in that order.
fn every_write(store: &Arc<dyn ObjectStore>, location: &str) -> [object_store::Result<()>; 3] {
    let location = Path::from(location);
    block_on(async {
        let put = store
            .put(&location, PutPayload::from_static(b"put"))
            .await
            .map(drop);
        let multipart = async {
            let mut upload = WriteMultipart::new(store.put_multipart(&location).await?);
            upload.write(b"multipart");
            upload.finish().await.map(drop)
        }
        .await;
        let copy = store.copy(&Path::from("source"), &location).await;
        [put, multipart, copy]
    })
}

#[test]
fn both_failing_stores_fail_every_kind_of_write_beneath_the_prefix_only() {
    let fresh = MemoryStore::failing_writes_under("every-write", "out");
    let viewed = MemoryStore::new("every-write-view").with_failing_writes_under("out");
    for store in [fresh, viewed] {
        put(store.store(), "source", b"source").unwrap();

        let beneath = every_write(store.store(), "out/a");
        let outside = every_write(store.store(), "outer/a");

        assert!(beneath.iter().all(Result::is_err), "{beneath:?}");
        assert!(outside.iter().all(Result::is_ok), "{outside:?}");
        assert_eq!(
            block_on(store.locations_under("")),
            ["outer/a", "source"].map(Path::from)
        );
    }
}

#[test]
fn with_failing_writes_under_views_the_same_objects_at_the_same_url() {
    let store = MemoryStore::new("viewed");
    put(store.store(), "out/a", b"before").unwrap();

    let failing = store.with_failing_writes_under("out");

    assert_eq!(failing.url(), store.url());
    assert_eq!(get(failing.store(), "out/a").unwrap(), b"before");
    put(failing.store(), "elsewhere", b"c").unwrap();
    assert_eq!(get(store.store(), "elsewhere").unwrap(), b"c");
}

#[test]
fn with_failing_writes_under_fails_a_later_write_where_an_earlier_one_succeeded() {
    let store = MemoryStore::new("later");
    put(store.store(), "out/a", b"before").unwrap();
    let failing = store.with_failing_writes_under("out");

    let failed = put(failing.store(), "out/a", b"after");

    assert!(failed.is_err());
    assert_eq!(get(store.store(), "out/a").unwrap(), b"before");
    // The original store still writes there.
    put(store.store(), "out/a", b"again").unwrap();
    assert_eq!(get(failing.store(), "out/a").unwrap(), b"again");
}

/// Reads `location` from `store` on a thread of its own, so the calling test can watch the read
/// be held at a gate.
fn read_on_a_thread(
    store: &Arc<dyn ObjectStore>,
    location: &'static str,
) -> std::thread::JoinHandle<object_store::Result<Vec<u8>>> {
    let store = Arc::clone(store);
    std::thread::spawn(move || get(&store, location))
}

#[test]
fn with_gated_reads_under_holds_reads_beneath_the_prefix_until_the_gate_is_released() {
    let store = MemoryStore::new("gated");
    put(store.store(), "data/a", b"a").unwrap();
    put(store.store(), "elsewhere", b"b").unwrap();
    let (gated, gate) = store.with_gated_reads_under("data");

    let held = read_on_a_thread(gated.store(), "data/a");
    block_on(gate.reached());

    // A read outside the prefix passes while one beneath it is held.
    assert_eq!(get(gated.store(), "elsewhere").unwrap(), b"b");
    assert!(!held.is_finished());
    gate.release();
    assert_eq!(held.join().unwrap().unwrap(), b"a");
    // The gate stays open: a later read beneath the prefix is not held.
    assert_eq!(get(gated.store(), "data/a").unwrap(), b"a");
}

#[test]
fn with_gated_reads_under_holds_no_write_or_listing_and_views_the_same_objects() {
    let store = MemoryStore::new("gated-view");
    put(store.store(), "data/a", b"before").unwrap();
    let (gated, gate) = store.with_gated_reads_under("data");

    put(gated.store(), "data/b", b"b").unwrap();
    let listed = block_on(gated.locations_under("data"));

    assert_eq!(gated.url(), store.url());
    assert_eq!(listed, ["data/a", "data/b"].map(Path::from));
    assert_eq!(get(store.store(), "data/b").unwrap(), b"b");
    gate.release();
    assert_eq!(get(gated.store(), "data/a").unwrap(), b"before");
}
