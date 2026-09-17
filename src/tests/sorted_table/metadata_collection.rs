//! How a sorted table obtains per-file statistics: attached by the caller, reused from the
//! runtime's file statistics cache, or inferred concurrently from file footers.

use super::{column_statistics, file, ordering, sample_table_with_format, schema, statistics};
use crate::fixture::{self, FixtureFormat, MemoryStore, block_on};
use crate::locus::LocusRepresentation;
use crate::sorted_table::SortedTable;
use crate::tests::plan_shape::PlanShape;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    catalog::Session,
    common::{Result, Statistics, TableReference, config::ConfigNonZeroUsize, stats::Precision},
    datasource::{
        file_format::{
            FileFormat, FileMeta, file_compression_type::FileCompressionType,
            parquet::ParquetFormat,
        },
        listing::PartitionedFile,
        physical_plan::{FileScanConfig, FileSource},
        table_schema::TableSchema,
    },
    execution::{
        cache::{
            Cache, CacheEntryInfo, SchemaFingerprint, TableScopedPath,
            cache_manager::{CacheManagerConfig, CachedFileMetadata},
            default_cache::DefaultCache,
        },
        runtime_env::RuntimeEnvBuilder,
    },
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::ExecutionPlan,
    prelude::{SessionConfig, SessionContext},
};
use object_store::{ObjectMeta, ObjectStore};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

/// Fails a planning future that would otherwise hang on a gate the table never opens.
const PLANNING_TIMEOUT: Duration = Duration::from_secs(10);

/// How a metered format releases an inference once it has started.
#[derive(Clone)]
enum Gate {
    /// Release immediately.
    Open,
    /// Hold every inference until `inferences` of them have started at once.
    UntilStarted { inferences: usize },
    /// Complete inferences in exactly this path order.
    CompleteInOrder(Vec<String>),
}

/// A file format that records every metadata inference and can script its results.
///
/// Delegates everything else to `inner`. `infer_stats` and `infer_ordering` panic: the
/// table must ask for both products with one call.
pub(super) struct MeteredFormat {
    inner: Arc<dyn FileFormat>,
    scripted: Option<HashMap<String, FileMeta>>,
    gate: Gate,
    started: Mutex<Vec<String>>,
    completed: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
}

impl std::fmt::Debug for MeteredFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MeteredFormat")
            .finish_non_exhaustive()
    }
}

impl MeteredFormat {
    pub(super) fn delegating(inner: Arc<dyn FileFormat>) -> Self {
        Self {
            inner,
            scripted: None,
            gate: Gate::Open,
            started: Mutex::new(Vec::new()),
            completed: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
        }
    }

    /// Answers inferences from `scripted` without touching any store.
    fn scripted(scripted: Vec<(&str, FileMeta)>, gate: Gate) -> Self {
        Self {
            scripted: Some(
                scripted
                    .into_iter()
                    .map(|(path, meta)| (path.to_string(), meta))
                    .collect(),
            ),
            gate,
            ..Self::delegating(Arc::new(ParquetFormat::default()))
        }
    }

    pub(super) fn started(&self) -> Vec<String> {
        self.started.lock().unwrap().clone()
    }

    fn completed(&self) -> Vec<String> {
        self.completed.lock().unwrap().clone()
    }

    async fn wait_for_gate(&self, path: &str) {
        match &self.gate {
            Gate::Open => {}
            Gate::UntilStarted { inferences } => {
                while self.started.lock().unwrap().len() < *inferences {
                    tokio::task::yield_now().await;
                }
            }
            Gate::CompleteInOrder(order) => {
                let turn = order
                    .iter()
                    .position(|scripted| scripted == path)
                    .unwrap_or_else(|| panic!("{path} is not in the completion order"));
                while self.completed.lock().unwrap().len() < turn {
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}

#[async_trait]
impl FileFormat for MeteredFormat {
    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(&self, compression: &FileCompressionType) -> Result<String> {
        self.inner.get_ext_with_compression(compression)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        panic!(
            "statistics for {} must be inferred together with ordering",
            object.location
        )
    }

    async fn infer_ordering(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Option<LexOrdering>> {
        panic!(
            "ordering for {} must be inferred together with statistics",
            object.location
        )
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<FileMeta> {
        let path = object.location.to_string();
        self.started.lock().unwrap().push(path.clone());
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_in_flight.fetch_max(in_flight, Ordering::SeqCst);
        self.wait_for_gate(&path).await;
        let meta = match &self.scripted {
            Some(scripted) => scripted
                .get(&path)
                .cloned()
                .unwrap_or_else(|| panic!("no scripted metadata for {path}")),
            None => {
                self.inner
                    .infer_stats_and_ordering(state, store, table_schema, object)
                    .await?
            }
        };
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.completed.lock().unwrap().push(path);
        Ok(meta)
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(state, conf).await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}

/// The runtime's file statistics cache, recording which keys were read and written.
struct RecordingCache {
    inner: DefaultCache<TableScopedPath, CachedFileMetadata>,
    gets: Mutex<Vec<TableScopedPath>>,
    puts: Mutex<Vec<TableScopedPath>>,
}

impl RecordingCache {
    fn new() -> Self {
        Self {
            inner: DefaultCache::new(usize::MAX),
            gets: Mutex::new(Vec::new()),
            puts: Mutex::new(Vec::new()),
        }
    }
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

/// A session whose runtime holds `cache` as its file statistics cache.
fn session_with_cache(config: SessionConfig, cache: Arc<RecordingCache>) -> SessionContext {
    let runtime = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default().with_file_statistics_cache(Some(cache)))
        .build_arc()
        .unwrap();
    SessionContext::new_with_config_rt(config, runtime)
}

/// `super::table` reading through a metered `format`.
fn table_with_format(
    store: &MemoryStore,
    format: Arc<MeteredFormat>,
    files: Vec<PartitionedFile>,
) -> SortedTable {
    SortedTable::new(
        store.url().clone(),
        format,
        files,
        schema(),
        ordering(),
        None,
    )
}

/// A file with no statistics attached, so the table has to look them up or infer them.
fn bare_file(path: &str) -> PartitionedFile {
    PartitionedFile::new(path, 8)
}

/// Statistics for one row with `position` in `min..=max`, as a footer would report them.
fn position_meta(min: i32, max: i32) -> FileMeta {
    FileMeta::new(Statistics {
        num_rows: Precision::Exact(1),
        total_byte_size: Precision::Exact(8),
        column_statistics: vec![column_statistics(Some(min), Some(max))],
    })
}

fn footer_ordering() -> LexOrdering {
    LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
        "position", 0,
    )))])
    .unwrap()
}

fn key(path: &str) -> TableScopedPath {
    TableScopedPath {
        table: None,
        path: object_store::path::Path::from(path),
    }
}

fn fingerprint(schema: &Schema) -> Arc<SchemaFingerprint> {
    Arc::new(SchemaFingerprint::from_schema(schema))
}

async fn planned(ctx: &SessionContext, table: Arc<SortedTable>) -> Arc<dyn ExecutionPlan> {
    tokio::time::timeout(
        PLANNING_TIMEOUT,
        ctx.read_table(table).unwrap().create_physical_plan(),
    )
    .await
    .expect("planning must not wait on a gate the table never opens")
    .unwrap()
}

async fn planned_paths(ctx: &SessionContext, table: Arc<SortedTable>) -> Vec<String> {
    let plan = planned(ctx, table).await;
    PlanShape::of(&plan)
        .files_in_only_scan()
        .into_iter()
        .map(|path| path.to_string())
        .collect()
}

#[test]
fn a_second_scan_in_one_session_infers_no_metadata_in_either_format() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        let fixture = fixture::dataset_fixture(format, LocusRepresentation::ContigPosition);
        block_on(async {
            let ctx = SessionContext::new();
            fixture.register(&ctx);
            let metered = Arc::new(MeteredFormat::delegating(
                fixture.input_format().read_format(),
            ));
            let table =
                || sample_table_with_format(&ctx, fixture, fixture::SAMPLES[0], metered.clone());
            let table = Arc::new(table().await);

            let first = planned_paths(&ctx, Arc::clone(&table)).await;
            let mut inferred = metered.started();
            inferred.sort();
            assert_eq!(inferred.len(), 4, "{inferred:?}");
            let mut expected = first.clone();
            expected.sort();
            assert_eq!(inferred, expected);

            let second = planned_paths(&ctx, Arc::clone(&table)).await;
            assert_eq!(second, first);
            assert_eq!(
                metered.started().len(),
                4,
                "second scan must reuse cached metadata"
            );

            // Another table over the same files in the same session shares the cache.
            let other = Arc::new(
                sample_table_with_format(&ctx, fixture, fixture::SAMPLES[0], metered.clone()).await,
            );
            assert_eq!(planned_paths(&ctx, other).await, first);
            assert_eq!(metered.started().len(), 4);
        });
    }
}

#[tokio::test]
async fn inference_runs_concurrently_up_to_the_session_metadata_fetch_concurrency() {
    for concurrency in [1, 2, 3] {
        let mut config = SessionConfig::new();
        config.options_mut().execution.meta_fetch_concurrency =
            ConfigNonZeroUsize::try_new(concurrency).unwrap();
        let ctx = SessionContext::new_with_config(config);
        let store = MemoryStore::new("concurrent-inference");
        store.register(&ctx);
        // With four files and a bound below four, an inference that waits for `concurrency`
        // started inferences only completes if that many run at once.
        let format = Arc::new(MeteredFormat::scripted(
            vec![
                ("d.parquet", position_meta(1, 10)),
                ("c.parquet", position_meta(11, 20)),
                ("b.parquet", position_meta(21, 30)),
                ("a.parquet", position_meta(31, 40)),
            ],
            Gate::UntilStarted {
                inferences: concurrency,
            },
        ));
        let table = Arc::new(table_with_format(
            &store,
            Arc::clone(&format),
            ["a", "b", "c", "d"]
                .into_iter()
                .map(|stem| bare_file(&format!("{stem}.parquet")))
                .collect(),
        ));

        let paths = planned_paths(&ctx, table).await;

        assert_eq!(paths, ["d.parquet", "c.parquet", "b.parquet", "a.parquet"]);
        assert_eq!(
            format.peak_in_flight.load(Ordering::SeqCst),
            concurrency,
            "peak in-flight inferences must equal the session bound {concurrency}"
        );
        assert_eq!(format.completed().len(), 4);
    }
}

#[tokio::test]
async fn completion_order_does_not_change_the_recovered_file_order_or_the_plan() {
    let ctx = SessionContext::new_with_config(crate::pipeline::session_config());
    let store = MemoryStore::new("completion-order");
    store.register(&ctx);
    let metas = vec![
        ("c.parquet", position_meta(1, 10)),
        ("b.parquet", position_meta(11, 20)),
        ("a.parquet", position_meta(21, 30)),
    ];
    let attached = Arc::new(super::table(
        &store,
        vec![
            file("a.parquet", Some(21), Some(30)),
            file("b.parquet", Some(11), Some(20)),
            file("c.parquet", Some(1), Some(10)),
        ],
    ));
    let baseline = planned(&ctx, attached).await;

    for order in [
        ["a.parquet", "b.parquet", "c.parquet"],
        ["c.parquet", "b.parquet", "a.parquet"],
        ["b.parquet", "a.parquet", "c.parquet"],
    ] {
        // A fresh session per order, so no run reuses the previous run's cached metadata.
        let ctx = SessionContext::new_with_config(crate::pipeline::session_config());
        store.register(&ctx);
        let format = Arc::new(MeteredFormat::scripted(
            metas.clone(),
            Gate::CompleteInOrder(order.iter().map(ToString::to_string).collect()),
        ));
        let table = Arc::new(table_with_format(
            &store,
            Arc::clone(&format),
            vec![
                bare_file("a.parquet"),
                bare_file("b.parquet"),
                bare_file("c.parquet"),
            ],
        ));
        let plan = planned(&ctx, table).await;
        assert_eq!(
            format.completed(),
            order,
            "the gate must have controlled completion"
        );
        assert_eq!(
            PlanShape::of(&plan).to_string(),
            PlanShape::of(&baseline).to_string()
        );
        assert_eq!(
            statistics(plan.as_ref(), None),
            statistics(baseline.as_ref(), None)
        );
    }
}

#[tokio::test]
async fn valid_cache_entries_are_reused_and_stale_ones_are_replaced() {
    let store = MemoryStore::new("cache-validity");
    let ctx = SessionContext::new();
    store.register(&ctx);
    let cache = ctx
        .runtime_env()
        .cache_manager
        .get_file_statistic_cache()
        .expect("the default runtime has a file statistics cache");
    let stored = bare_file("stored.parquet");
    let other = bare_file("other.parquet");
    // The footer says `stored` comes second; a cache entry that says it comes first is
    // observable through the file order.
    let format = Arc::new(MeteredFormat::scripted(
        vec![
            (
                "stored.parquet",
                position_meta(11, 20).with_ordering(Some(footer_ordering())),
            ),
            ("other.parquet", position_meta(1, 10)),
        ],
        Gate::Open,
    ));
    let seeded_statistics = Arc::new(position_meta(-10, -1).statistics);
    let current_meta = stored.object_meta.clone();
    let mut stale_size = current_meta.clone();
    stale_size.size += 1;
    let mut stale_time = current_meta.clone();
    stale_time.last_modified += Duration::from_secs(60);
    let other_schema = Schema::new(vec![Field::new("position", DataType::Int64, false)]);
    let cases = [
        ("valid", current_meta.clone(), fingerprint(&schema()), true),
        ("size", stale_size, fingerprint(&schema()), false),
        (
            "modification time",
            stale_time,
            fingerprint(&schema()),
            false,
        ),
        (
            "schema fingerprint",
            current_meta.clone(),
            fingerprint(&other_schema),
            false,
        ),
    ];
    for (case, meta, schema_fingerprint, reused) in cases {
        cache.clear();
        cache.put(
            &key("stored.parquet"),
            CachedFileMetadata::new(
                meta,
                schema_fingerprint,
                Arc::clone(&seeded_statistics),
                None,
            ),
        );
        let inferences_before = format.started().len();
        let table = Arc::new(table_with_format(
            &store,
            Arc::clone(&format),
            vec![stored.clone(), other.clone()],
        ));

        let paths = planned_paths(&ctx, table).await;

        let inferred = format.started()[inferences_before..].to_vec();
        if reused {
            assert_eq!(paths, ["stored.parquet", "other.parquet"], "{case}");
            assert_eq!(inferred, ["other.parquet"], "{case}");
        } else {
            assert_eq!(paths, ["other.parquet", "stored.parquet"], "{case}");
            let mut inferred = inferred;
            inferred.sort();
            assert_eq!(inferred, ["other.parquet", "stored.parquet"], "{case}");
            assert_eq!(
                cache.get(&key("stored.parquet")),
                Some(CachedFileMetadata::new(
                    stored.object_meta.clone(),
                    fingerprint(&schema()),
                    Arc::new(position_meta(11, 20).statistics),
                    Some(footer_ordering()),
                )),
                "{case}: the entry must have the listing table's shape, footer ordering included"
            );
        }
        assert_eq!(
            cache.get(&key("other.parquet")),
            Some(CachedFileMetadata::new(
                other.object_meta.clone(),
                fingerprint(&schema()),
                Arc::new(position_meta(1, 10).statistics),
                None,
            )),
            "{case}"
        );
    }
}

#[tokio::test]
async fn attached_statistics_bypass_the_cache_and_inference() {
    let cache = Arc::new(RecordingCache::new());
    let ctx = session_with_cache(SessionConfig::new(), Arc::clone(&cache));
    let store = MemoryStore::new("attached-statistics");
    store.register(&ctx);
    let attached = file("attached.parquet", Some(11), Some(20));
    // A valid entry that would place `attached` first if it were consulted.
    cache.put(
        &key("attached.parquet"),
        CachedFileMetadata::new(
            attached.object_meta.clone(),
            fingerprint(&schema()),
            Arc::new(position_meta(-10, -1).statistics),
            None,
        ),
    );
    cache.puts.lock().unwrap().clear();
    let format = Arc::new(MeteredFormat::scripted(
        vec![("inferred.parquet", position_meta(1, 10))],
        Gate::Open,
    ));
    let table = Arc::new(table_with_format(
        &store,
        Arc::clone(&format),
        vec![attached, bare_file("inferred.parquet")],
    ));

    let paths = planned_paths(&ctx, table).await;

    assert_eq!(paths, ["inferred.parquet", "attached.parquet"]);
    assert_eq!(format.started(), ["inferred.parquet"]);
    assert_eq!(*cache.gets.lock().unwrap(), [key("inferred.parquet")]);
    assert_eq!(*cache.puts.lock().unwrap(), [key("inferred.parquet")]);
}
