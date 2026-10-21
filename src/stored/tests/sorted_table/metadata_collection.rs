//! How a sorted table obtains per-file statistics: attached by the caller, reused from the
//! runtime's file statistics cache, or inferred concurrently from file footers.

use super::{column_statistics, file, ordering, sample_table_with_format, schema, statistics};
use crate::locus::LocusRepresentation;
use crate::stored::sorted_table::SortedTable;
use crate::testing::{
    fixture::{self, FixtureFormat, block_on},
    memory_store::MemoryStore,
    metered_format::{Gate, MeteredFormat},
    plan_shape::PlanShape,
    recording_cache::{RecordingCache, cache_key, session_with_cache},
};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::{Statistics, config::ConfigNonZeroUsize, stats::Precision},
    datasource::{file_format::FileMeta, listing::PartitionedFile},
    execution::cache::{Cache, SchemaFingerprint, cache_manager::CachedFileMetadata},
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::ExecutionPlan,
    prelude::{SessionConfig, SessionContext},
};
use std::{sync::Arc, time::Duration};

/// Fails a planning future that would otherwise hang on a gate the table never opens.
const PLANNING_TIMEOUT: Duration = Duration::from_secs(10);

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
            format.peak_in_flight(),
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
            &cache_key("stored.parquet"),
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
                cache.get(&cache_key("stored.parquet")),
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
            cache.get(&cache_key("other.parquet")),
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
        &cache_key("attached.parquet"),
        CachedFileMetadata::new(
            attached.object_meta.clone(),
            fingerprint(&schema()),
            Arc::new(position_meta(-10, -1).statistics),
            None,
        ),
    );
    cache.take_puts();
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
    assert_eq!(cache.take_gets(), [cache_key("inferred.parquet")]);
    assert_eq!(cache.take_puts(), [cache_key("inferred.parquet")]);
}
