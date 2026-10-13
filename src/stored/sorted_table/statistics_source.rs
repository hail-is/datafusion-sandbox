//! Where a sorted table's per-file statistics come from: attached by the caller, reused from
//! the runtime's file statistics cache, or inferred from file footers.
//!
//! Cache entries have the listing table's key and shape: the file path scoped to no table,
//! validated by object size, modification time, and a fingerprint of the file schema, with the
//! footer's own ordering stored alongside the statistics. The sorted table stores that ordering
//! for compatibility only; it recovers its file order from ordering statistics.

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::{Result, Statistics},
    datasource::{file_format::FileFormat, listing::PartitionedFile},
    execution::{
        cache::{
            SchemaFingerprint, TableScopedPath,
            cache_manager::{CachedFileMetadata, FileStatisticsCache},
        },
        object_store::ObjectStoreUrl,
    },
};
use futures::{StreamExt, TryStreamExt, stream};
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct StatisticsSource {
    format: Arc<dyn FileFormat>,
    object_store_url: ObjectStoreUrl,
    file_schema: SchemaRef,
    fingerprint: Arc<SchemaFingerprint>,
}

impl StatisticsSource {
    pub(super) fn new(
        format: Arc<dyn FileFormat>,
        object_store_url: ObjectStoreUrl,
        file_schema: SchemaRef,
    ) -> Self {
        let fingerprint = Arc::new(SchemaFingerprint::from_schema(&file_schema));
        Self {
            format,
            object_store_url,
            file_schema,
            fingerprint,
        }
    }

    /// Returns `files` in their given order, each carrying its statistics extended by its
    /// partition values. Files without attached statistics are looked up in the session's file
    /// statistics cache, and the rest are inferred concurrently up to the session's metadata
    /// fetch concurrency, with one footer read yielding both statistics and ordering.
    ///
    /// The bounded stream keeps input order, so completion order cannot change which file an
    /// error names or the order the files reach order recovery in.
    pub(super) async fn for_files(
        &self,
        state: &dyn Session,
        files: Vec<PartitionedFile>,
    ) -> Result<Vec<PartitionedFile>> {
        let cache = state.runtime_env().cache_manager.get_file_statistic_cache();
        let concurrency = state
            .config_options()
            .execution
            .meta_fetch_concurrency
            .get();
        stream::iter(files)
            .map(|file| self.file_with_statistics(state, cache.as_deref(), file))
            .buffered(concurrency)
            .try_collect()
            .await
    }

    /// Attaching also appends the file's partition values to the statistics.
    async fn file_with_statistics(
        &self,
        state: &dyn Session,
        cache: Option<&FileStatisticsCache>,
        file: PartitionedFile,
    ) -> Result<PartitionedFile> {
        let statistics = self.statistics_for(state, cache, &file).await?;
        Ok(file.with_statistics(statistics))
    }

    /// Attached statistics win without consulting the cache. A valid cache entry is reused; a
    /// missing or stale one is replaced by inference.
    async fn statistics_for(
        &self,
        state: &dyn Session,
        cache: Option<&FileStatisticsCache>,
        file: &PartitionedFile,
    ) -> Result<Arc<Statistics>> {
        if let Some(attached) = &file.statistics {
            return Ok(Arc::clone(attached));
        }
        let key = TableScopedPath {
            table: None,
            path: file.object_meta.location.clone(),
        };
        if let Some(cached) = cache.and_then(|cache| cache.get(&key))
            && cached.is_valid_for(&file.object_meta, &self.fingerprint)
        {
            return Ok(cached.statistics);
        }
        self.infer(state, cache, &key, file).await
    }

    /// Reads the file's footer once for both statistics and ordering, and caches both.
    async fn infer(
        &self,
        state: &dyn Session,
        cache: Option<&FileStatisticsCache>,
        key: &TableScopedPath,
        file: &PartitionedFile,
    ) -> Result<Arc<Statistics>> {
        let store = state.runtime_env().object_store(&self.object_store_url)?;
        let inferred = self
            .format
            .infer_stats_and_ordering(
                state,
                &store,
                Arc::clone(&self.file_schema),
                &file.object_meta,
            )
            .await?;
        let statistics = Arc::new(inferred.statistics);
        if let Some(cache) = cache {
            cache.put(
                key,
                CachedFileMetadata::new(
                    file.object_meta.clone(),
                    Arc::clone(&self.fingerprint),
                    Arc::clone(&statistics),
                    inferred.ordering,
                ),
            );
        }
        Ok(statistics)
    }
}
