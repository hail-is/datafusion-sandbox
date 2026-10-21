use crate::stored::sorted_table::statistics_source::StatisticsSource;
use crate::testing::{
    memory_store::MemoryStore,
    metered_format::{Gate, MeteredFormat},
    recording_cache::{RecordingCache, cache_key, session_with_cache},
};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    common::{
        ColumnStatistics, ScalarValue, Statistics, config::ConfigNonZeroUsize, stats::Precision,
    },
    datasource::{file_format::FileMeta, listing::PartitionedFile},
    execution::cache::{Cache, SchemaFingerprint, cache_manager::CachedFileMetadata},
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    prelude::{SessionConfig, SessionContext},
};
use std::{sync::Arc, time::Duration};

/// Fails a lookup that would otherwise hang on a gate the source never opens.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "position",
        DataType::Int32,
        false,
    )]))
}

/// File statistics reporting `rows` rows, so a test can tell where a file's statistics came
/// from.
fn rows(rows: usize) -> Statistics {
    Statistics {
        num_rows: Precision::Exact(rows),
        ..Statistics::new_unknown(&schema())
    }
}

/// A file with no statistics attached, so the source has to look them up or infer them.
fn bare_file(path: &str) -> PartitionedFile {
    PartitionedFile::new(path, 8)
}

fn fingerprint(schema: &Schema) -> Arc<SchemaFingerprint> {
    Arc::new(SchemaFingerprint::from_schema(schema))
}

/// A cache entry for `file` as the source would write it after inferring `statistics`.
fn entry(file: &PartitionedFile, statistics: Statistics) -> CachedFileMetadata {
    CachedFileMetadata::new(
        file.object_meta.clone(),
        fingerprint(&schema()),
        Arc::new(statistics),
        None,
    )
}

fn footer_ordering() -> LexOrdering {
    LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
        "position", 0,
    )))])
    .unwrap()
}

/// A session on `store` with the default file statistics cache.
fn session(store: &MemoryStore) -> SessionContext {
    let ctx = SessionContext::new();
    store.register(&ctx);
    ctx
}

/// `files` with their statistics from a source reading through `format`.
async fn for_files(
    ctx: &SessionContext,
    store: &MemoryStore,
    format: Arc<MeteredFormat>,
    files: Vec<PartitionedFile>,
) -> Vec<PartitionedFile> {
    let source = StatisticsSource::new(format, store.url().clone(), schema());
    tokio::time::timeout(LOOKUP_TIMEOUT, source.for_files(&ctx.state(), files))
        .await
        .expect("the lookup must not wait on a gate the source never opens")
        .unwrap()
}

/// Every file's path and the row count of the statistics it carries, in output order.
fn rows_by_path(files: &[PartitionedFile]) -> Vec<(String, Precision<usize>)> {
    files
        .iter()
        .map(|file| {
            (
                file.object_meta.location.to_string(),
                file.statistics.as_ref().unwrap().num_rows,
            )
        })
        .collect()
}

#[tokio::test]
async fn attached_statistics_win_over_the_cache_which_wins_over_inference() {
    let cache = Arc::new(RecordingCache::new());
    let ctx = session_with_cache(SessionConfig::new(), Arc::clone(&cache));
    let store = MemoryStore::new("statistics-precedence");
    store.register(&ctx);
    let attached = bare_file("attached.parquet").with_statistics(Arc::new(rows(1)));
    let cached = bare_file("cached.parquet");
    // Valid entries for both, so only precedence keeps the attached file from using its own.
    cache.put(&cache_key("attached.parquet"), entry(&attached, rows(20)));
    cache.put(&cache_key("cached.parquet"), entry(&cached, rows(2)));
    cache.take_puts();
    let format = Arc::new(MeteredFormat::scripted(
        vec![
            ("attached.parquet", FileMeta::new(rows(30))),
            ("cached.parquet", FileMeta::new(rows(30))),
            ("inferred.parquet", FileMeta::new(rows(3))),
        ],
        Gate::Open,
    ));

    let files = for_files(
        &ctx,
        &store,
        Arc::clone(&format),
        vec![attached, cached, bare_file("inferred.parquet")],
    )
    .await;

    assert_eq!(
        rows_by_path(&files),
        [
            ("attached.parquet".to_string(), Precision::Exact(1)),
            ("cached.parquet".to_string(), Precision::Exact(2)),
            ("inferred.parquet".to_string(), Precision::Exact(3)),
        ]
    );
    assert_eq!(format.started(), ["inferred.parquet"]);
    assert_eq!(
        cache.take_gets(),
        [cache_key("cached.parquet"), cache_key("inferred.parquet")],
        "attached statistics must not consult the cache"
    );
    assert_eq!(cache.take_puts(), [cache_key("inferred.parquet")]);
}

#[tokio::test]
async fn valid_cache_entries_are_reused_and_stale_ones_are_replaced() {
    let store = MemoryStore::new("cache-validity");
    let file = bare_file("stored.parquet");
    let current = file.object_meta.clone();
    let mut stale_size = current.clone();
    stale_size.size += 1;
    let mut stale_time = current.clone();
    stale_time.last_modified += Duration::from_secs(60);
    let other_schema = Schema::new(vec![Field::new("position", DataType::Int64, false)]);
    let cases = [
        ("valid", current.clone(), fingerprint(&schema()), true),
        ("size", stale_size, fingerprint(&schema()), false),
        (
            "modification time",
            stale_time,
            fingerprint(&schema()),
            false,
        ),
        (
            "schema fingerprint",
            current,
            fingerprint(&other_schema),
            false,
        ),
    ];
    for (case, meta, schema_fingerprint, reused) in cases {
        let ctx = session(&store);
        let cache = ctx
            .runtime_env()
            .cache_manager
            .get_file_statistic_cache()
            .expect("the default runtime has a file statistics cache");
        cache.put(
            &cache_key("stored.parquet"),
            CachedFileMetadata::new(meta, schema_fingerprint, Arc::new(rows(2)), None),
        );
        let format = Arc::new(MeteredFormat::scripted(
            vec![(
                "stored.parquet",
                FileMeta::new(rows(3)).with_ordering(Some(footer_ordering())),
            )],
            Gate::Open,
        ));

        let files = for_files(&ctx, &store, Arc::clone(&format), vec![file.clone()]).await;

        if reused {
            assert_eq!(files[0].statistics, Some(Arc::new(rows(2))), "{case}");
            assert!(format.started().is_empty(), "{case}");
        } else {
            assert_eq!(files[0].statistics, Some(Arc::new(rows(3))), "{case}");
            assert_eq!(format.started(), ["stored.parquet"], "{case}");
            assert_eq!(
                cache.get(&cache_key("stored.parquet")),
                Some(CachedFileMetadata::new(
                    file.object_meta.clone(),
                    fingerprint(&schema()),
                    Arc::new(rows(3)),
                    Some(footer_ordering()),
                )),
                "{case}: the entry must have the listing table's shape, footer ordering included"
            );
        }
    }
}

#[tokio::test]
async fn inference_runs_concurrently_up_to_the_session_metadata_fetch_concurrency() {
    let store = MemoryStore::new("concurrent-inference");
    let paths = ["a.parquet", "b.parquet", "c.parquet", "d.parquet"];
    let files = || paths.into_iter().map(bare_file).collect::<Vec<_>>();
    let format = |inferences| {
        Arc::new(MeteredFormat::scripted(
            paths
                .iter()
                .map(|&path| (path, FileMeta::new(rows(1))))
                .collect(),
            Gate::UntilStarted { inferences },
        ))
    };
    // A fresh session per lookup, so no lookup reuses another's cached statistics.
    let session = |concurrency| {
        let mut config = SessionConfig::new();
        config.options_mut().execution.meta_fetch_concurrency =
            ConfigNonZeroUsize::try_new(concurrency).unwrap();
        let ctx = SessionContext::new_with_config(config);
        store.register(&ctx);
        ctx
    };
    for concurrency in [1, 2, 3] {
        // Each inference waits for `concurrency` started inferences, so the lookup completes
        // only if that many run at once.
        let reached = format(concurrency);
        let found = for_files(&session(concurrency), &store, Arc::clone(&reached), files()).await;
        assert_eq!(found.len(), 4);
        assert_eq!(reached.peak_in_flight(), concurrency);

        // Each inference waits for one more than the bound, so a lookup that keeps to the bound
        // holds exactly `concurrency` inferences forever.
        let exceeded = format(concurrency + 1);
        let source = StatisticsSource::new(exceeded.clone(), store.url().clone(), schema());
        let held = tokio::time::timeout(
            Duration::from_millis(100),
            source.for_files(&session(concurrency).state(), files()),
        )
        .await;
        assert!(held.is_err(), "the lookup must not exceed {concurrency}");
        assert_eq!(exceeded.started().len(), concurrency);
    }
}

#[tokio::test]
async fn output_keeps_the_input_order_when_files_complete_out_of_order() {
    let store = MemoryStore::new("completion-order");
    let scripts = vec![
        ("a.parquet", FileMeta::new(rows(1))),
        ("b.parquet", FileMeta::new(rows(2))),
        ("c.parquet", FileMeta::new(rows(3))),
    ];
    for order in [
        ["c.parquet", "b.parquet", "a.parquet"],
        ["b.parquet", "c.parquet", "a.parquet"],
        ["a.parquet", "c.parquet", "b.parquet"],
    ] {
        // A fresh session per order, so no run reuses the previous run's cached statistics.
        let ctx = session(&store);
        let format = Arc::new(MeteredFormat::scripted(
            scripts.clone(),
            Gate::CompleteInOrder(order.iter().map(ToString::to_string).collect()),
        ));

        let files = for_files(
            &ctx,
            &store,
            Arc::clone(&format),
            vec![
                bare_file("a.parquet"),
                bare_file("b.parquet"),
                bare_file("c.parquet"),
            ],
        )
        .await;

        assert_eq!(
            format.completed(),
            order,
            "the gate must control completion"
        );
        assert_eq!(
            rows_by_path(&files),
            [
                ("a.parquet".to_string(), Precision::Exact(1)),
                ("b.parquet".to_string(), Precision::Exact(2)),
                ("c.parquet".to_string(), Precision::Exact(3)),
            ],
            "{order:?}"
        );
    }
}

/// The attached scalar reaches order recovery and pruning as exact bounds on its column, while
/// the cache keeps the file's own statistics, as a listing table's would.
#[tokio::test]
async fn statistics_are_extended_by_partition_values_that_the_cache_entry_leaves_out() {
    let store = MemoryStore::new("partition-values");
    let ctx = session(&store);
    let value = ScalarValue::Utf8(Some("NA18534".to_string()));
    let with_value = |file: PartitionedFile| PartitionedFile {
        partition_values: vec![value.clone()],
        ..file
    };
    let format = Arc::new(MeteredFormat::scripted(
        vec![("inferred.parquet", FileMeta::new(rows(3)))],
        Gate::Open,
    ));

    let files = for_files(
        &ctx,
        &store,
        format,
        vec![
            // As the table does, the value is set after the caller attached file statistics.
            with_value(bare_file("attached.parquet").with_statistics(Arc::new(rows(1)))),
            with_value(bare_file("inferred.parquet")),
        ],
    )
    .await;

    for file in &files {
        let columns = &file.statistics.as_ref().unwrap().column_statistics;
        assert_eq!(columns.len(), 2, "{}", file.object_meta.location);
        assert_eq!(columns[0], ColumnStatistics::new_unknown());
        assert_eq!(columns[1].min_value, Precision::Exact(value.clone()));
        assert_eq!(columns[1].max_value, Precision::Exact(value.clone()));
    }
    let cache = ctx
        .runtime_env()
        .cache_manager
        .get_file_statistic_cache()
        .unwrap();
    assert_eq!(
        cache.get(&cache_key("inferred.parquet")),
        Some(entry(&files[1], rows(3)))
    );
}
