//! A file statistics cache adapter that records which keys were read and written, the key a path
//! is cached under, and a session whose runtime holds the cache.
//!
//! The cache behaves as `DataFusion`'s default cache with no size limit; recording changes nothing
//! a caller sees.

use datafusion::{
    common::{Result, TableReference},
    execution::{
        cache::{
            Cache, CacheEntryInfo, TableScopedPath,
            cache_manager::{CacheManagerConfig, CachedFileMetadata},
            default_cache::DefaultCache,
        },
        runtime_env::RuntimeEnvBuilder,
    },
    prelude::{SessionConfig, SessionContext},
};
use std::{
    mem,
    sync::{Arc, Mutex},
    time::Duration,
};

/// The runtime's file statistics cache, recording the key of every `get` and `put`.
pub(crate) struct RecordingCache {
    inner: DefaultCache<TableScopedPath, CachedFileMetadata>,
    gets: Mutex<Vec<TableScopedPath>>,
    puts: Mutex<Vec<TableScopedPath>>,
}

impl RecordingCache {
    /// An empty cache that has recorded nothing.
    pub(crate) fn new() -> Self {
        Self {
            inner: DefaultCache::new(usize::MAX),
            gets: Mutex::new(Vec::new()),
            puts: Mutex::new(Vec::new()),
        }
    }

    /// The key of every `get` since the last call, in call order.
    pub(crate) fn take_gets(&self) -> Vec<TableScopedPath> {
        mem::take(&mut *self.gets.lock().unwrap())
    }

    /// The key of every `put` since the last call, in call order.
    pub(crate) fn take_puts(&self) -> Vec<TableScopedPath> {
        mem::take(&mut *self.puts.lock().unwrap())
    }
}

/// The key `path`'s statistics are cached under outside any table's scope, as a listing table
/// with no table reference and a sorted table key them.
pub(crate) fn cache_key(path: &str) -> TableScopedPath {
    TableScopedPath {
        table: None,
        path: object_store::path::Path::from(path),
    }
}

/// A session with `config` whose runtime holds `cache` as its file statistics cache.
pub(crate) fn session_with_cache(
    config: SessionConfig,
    cache: Arc<RecordingCache>,
) -> SessionContext {
    let runtime = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default().with_file_statistics_cache(Some(cache)))
        .build_arc()
        .unwrap();
    SessionContext::new_with_config_rt(config, runtime)
}

impl Cache<TableScopedPath, CachedFileMetadata> for RecordingCache {
    fn get(&self, key: &TableScopedPath) -> Option<CachedFileMetadata> {
        self.gets.lock().unwrap().push(key.clone());
        self.inner.get(key)
    }

    fn put(&self, key: &TableScopedPath, value: CachedFileMetadata) -> Option<CachedFileMetadata> {
        self.puts.lock().unwrap().push(key.clone());
        self.inner.put(key, value)
    }

    fn remove(&self, key: &TableScopedPath) -> Option<CachedFileMetadata> {
        self.inner.remove(key)
    }

    fn contains_key(&self, key: &TableScopedPath) -> bool {
        self.inner.contains_key(key)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn clear(&self) {
        self.inner.clear();
    }

    fn name(&self) -> String {
        "RecordingCache".to_string()
    }

    fn cache_limit(&self) -> usize {
        self.inner.cache_limit()
    }

    fn update_cache_limit(&self, limit: usize) {
        self.inner.update_cache_limit(limit);
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.inner.cache_ttl()
    }

    fn update_cache_ttl(&self, ttl: Option<Duration>) {
        self.inner.update_cache_ttl(ttl);
    }

    fn drop_table_entries(&self, table_ref: &TableReference) -> Result<()> {
        self.inner.drop_table_entries(table_ref)
    }

    fn list_entries(
        &self,
    ) -> datafusion::common::HashMap<TableScopedPath, CacheEntryInfo<CachedFileMetadata>> {
        self.inner.list_entries()
    }
}
