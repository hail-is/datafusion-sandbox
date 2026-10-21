use crate::testing::recording_cache::{RecordingCache, cache_key, session_with_cache};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::{Statistics, config::ConfigNonZeroUsize},
    datasource::listing::PartitionedFile,
    execution::cache::{
        Cache, SchemaFingerprint, TableScopedPath, cache_manager::CachedFileMetadata,
    },
    prelude::SessionConfig,
};
use std::sync::Arc;

fn entry(path: &str) -> CachedFileMetadata {
    let schema = Schema::new(vec![Field::new("position", DataType::Int32, false)]);
    CachedFileMetadata::new(
        PartitionedFile::new(path, 8).object_meta,
        Arc::new(SchemaFingerprint::from_schema(&schema)),
        Arc::new(Statistics::new_unknown(&schema)),
        None,
    )
}

#[test]
fn gets_and_puts_are_recorded_in_call_order_until_taken() {
    let cache = RecordingCache::new();

    cache.put(&cache_key("b"), entry("b"));
    assert_eq!(cache.get(&cache_key("a")), None);
    cache.put(&cache_key("a"), entry("a"));
    assert_eq!(cache.get(&cache_key("b")), Some(entry("b")));

    assert_eq!(cache.take_puts(), [cache_key("b"), cache_key("a")]);
    assert_eq!(cache.take_gets(), [cache_key("a"), cache_key("b")]);
    assert_eq!(cache.take_puts(), Vec::<TableScopedPath>::new());
    assert_eq!(cache.take_gets(), Vec::<TableScopedPath>::new());
}

#[test]
fn entries_are_kept_and_dropped_as_in_an_unrecorded_cache() {
    let cache = RecordingCache::new();
    cache.put(&cache_key("a"), entry("a"));
    cache.put(&cache_key("b"), entry("b"));

    assert_eq!(cache.len(), 2);
    assert!(cache.contains_key(&cache_key("a")));
    assert_eq!(cache.remove(&cache_key("a")), Some(entry("a")));
    assert!(!cache.contains_key(&cache_key("a")));
    cache.clear();
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.get(&cache_key("b")), None);
}

#[test]
fn a_session_with_the_cache_reads_and_writes_file_statistics_through_it() {
    let cache = Arc::new(RecordingCache::new());
    let mut config = SessionConfig::new();
    config.options_mut().execution.meta_fetch_concurrency = ConfigNonZeroUsize::try_new(3).unwrap();

    let ctx = session_with_cache(config, Arc::clone(&cache));

    let installed = ctx
        .runtime_env()
        .cache_manager
        .get_file_statistic_cache()
        .expect("the session must have a file statistics cache");
    installed.put(&cache_key("a"), entry("a"));
    assert_eq!(cache.take_puts(), [cache_key("a")]);
    assert_eq!(cache.get(&cache_key("a")), Some(entry("a")));
    assert_eq!(
        ctx.state()
            .config_options()
            .execution
            .meta_fetch_concurrency
            .get(),
        3
    );
}

#[test]
fn a_cache_key_is_the_path_scoped_to_no_table() {
    let key = cache_key("fixtures/a.parquet");

    assert_eq!(key.table, None);
    assert_eq!(
        key.path,
        object_store::path::Path::from("fixtures/a.parquet")
    );
}
