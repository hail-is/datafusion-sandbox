//! Checking and recording runs in a metrics directory on in-memory object stores, including ones
//! that fail the writes under one of its tables.

use crate::{
    fixture::{self, MemoryStore, RecordedRun},
    generated::make_range_table,
    metrics_directory::MetricsDirectory,
    pipeline::{self, PipelineOptions},
    run_metrics,
    sink::{self, CollectingSink, DataSinkTarget},
    tests::support::{run_record, string_values},
};

use datafusion::{
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
    prelude::{JoinType, SessionContext, col},
};
use object_store::{ObjectStoreExt, PutPayload};
use std::{future::Future, sync::Arc};

/// The metrics directory every test here records under, on the store named `store`, spelled with
/// a trailing slash that the layout does not repeat.
fn directory(store: &MemoryStore) -> MetricsDirectory {
    MetricsDirectory::new(&format!("{}benchmarks/", store.url().as_str()))
}

/// The one check of the layout itself, against ADR 0016 and the CLI's help. Every other test
/// names a run's files through the path functions.
#[test]
fn the_layout_names_one_file_per_run_in_each_table() {
    let directory = MetricsDirectory::new("gs://bucket/benchmarks/");

    assert_eq!(directory.path(), "gs://bucket/benchmarks");
    assert_eq!(
        directory.run_record_path("run-a"),
        "gs://bucket/benchmarks/runs/run-a.parquet"
    );
    assert_eq!(
        directory.run_metrics_path("run-a"),
        "gs://bucket/benchmarks/metrics/run-a.parquet"
    );
}

#[test]
fn a_fresh_run_id_is_unrecorded() {
    let store = MemoryStore::new("fresh");
    let directory = directory(&store);

    let run_id = on(&store, move |ctx| async move {
        Ok(directory
            .unrecorded(&ctx, "run-a")
            .await?
            .run_id()
            .to_string())
    });

    assert_eq!(run_id, "run-a");
}

/// A run record marks its id recorded whatever the file holds, and marks no other id.
#[test]
fn a_run_id_with_a_run_record_is_refused_naming_the_record() {
    let store = MemoryStore::new("refused");
    let directory = directory(&store);
    let record_path = directory.run_record_path("run-a");
    let put_store = store.clone();

    let (refused, other) = on(&store, move |ctx| async move {
        put(&put_store, &directory.run_record_path("run-a")).await?;
        Ok((
            directory.unrecorded(&ctx, "run-a").await.map(|_| ()),
            directory.unrecorded(&ctx, "run-b").await.map(|_| ()),
        ))
    });

    let error = refused.unwrap_err();
    assert!(
        matches!(error, DataFusionError::Configuration(_)),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("run id 'run-a'"), "{message}");
    assert!(message.contains(&record_path), "{message}");
    other.unwrap();
}

/// Run metrics left without a run record, as a failure between the two writes leaves them, do
/// not make the id recorded, and recording the id replaces them.
#[test]
fn orphaned_run_metrics_do_not_make_a_run_id_recorded_and_recording_replaces_them() {
    let store = MemoryStore::new("orphaned");
    let directory = directory(&store);
    let put_store = store.clone();

    let recorded = on(&store, move |ctx| async move {
        put(&put_store, &directory.run_metrics_path("run-a")).await?;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        run.record(
            &ctx,
            &run_record("run-a"),
            &executed_plan(&ctx, false).await?,
        )
        .await?;
        fixture::read_recorded_run(&ctx, &directory, "run-a").await
    });

    let metrics = recorded.metrics.unwrap();
    assert!(metrics.num_rows() > 0);
    assert!(
        string_values(&metrics, "run_id")
            .iter()
            .all(|run_id| run_id == "run-a"),
        "{metrics:?}"
    );
    assert!(recorded.record.is_some());
}

/// Recording writes the run record and the run metrics of the plan under the run's id, and hands
/// back the unrecorded metric names the run metrics module reports for the plan. Once recorded,
/// the id is refused.
#[test]
fn recording_a_run_writes_both_tables_and_hands_back_its_unrecorded_metrics() {
    let store = MemoryStore::new("recorded");
    let directory = directory(&store);
    let record = run_record("run-a");
    let expected_record = run_metrics::run_record_batch(&record).unwrap();

    let (unrecorded, expected_unrecorded, recorded, refused) = on(&store, move |ctx| async move {
        // A hash join reports metrics no run metrics column takes.
        let plan = executed_plan(&ctx, true).await?;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        let unrecorded = run.record(&ctx, &record, &plan).await?;
        Ok((
            unrecorded,
            run_metrics::run_metrics_batch("run-a", &plan)?.unrecorded,
            fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
            directory.unrecorded(&ctx, "run-a").await.is_err(),
        ))
    });

    assert_ne!(unrecorded, Vec::<String>::new());
    assert_eq!(unrecorded, expected_unrecorded);
    assert_eq!(recorded.record.unwrap(), expected_record);
    let metrics = recorded.metrics.unwrap();
    assert!(
        string_values(&metrics, "operator").contains(&"HashJoinExec".to_string()),
        "{metrics:?}"
    );
    assert!(refused);
}

/// A run whose run record write fails keeps the run metrics written before it, has no record,
/// and so may be checked again under the same id.
#[test]
fn a_failed_run_record_write_leaves_run_metrics_and_the_id_unrecorded() {
    let store = MemoryStore::failing_writes_under("record-fails", "benchmarks/runs");
    let directory = directory(&store);

    let (failed, recorded, retried) = on(&store, move |ctx| async move {
        let plan = executed_plan(&ctx, false).await?;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        let failed = run.record(&ctx, &run_record("run-a"), &plan).await;
        Ok((
            failed,
            fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
            directory.unrecorded(&ctx, "run-a").await.map(|_| ()),
        ))
    });

    let error = failed.unwrap_err().to_string();
    assert!(error.contains("benchmarks/runs"), "{error}");
    let RecordedRun { record, metrics } = recorded;
    assert!(record.is_none(), "{record:?}");
    assert!(metrics.is_some());
    retried.unwrap();
}

/// A run whose run metrics write fails writes no run record either: the metrics go first.
#[test]
fn a_failed_run_metrics_write_records_nothing() {
    let store = MemoryStore::failing_writes_under("metrics-fail", "benchmarks/metrics");
    let directory = directory(&store);

    let (failed, recorded) = on(&store, move |ctx| async move {
        let plan = executed_plan(&ctx, false).await?;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        let failed = run.record(&ctx, &run_record("run-a"), &plan).await;
        Ok((
            failed,
            fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
        ))
    });

    let error = failed.unwrap_err().to_string();
    assert!(error.contains("benchmarks/metrics"), "{error}");
    let RecordedRun { record, metrics } = recorded;
    assert!(record.is_none(), "{record:?}");
    assert!(metrics.is_none(), "{metrics:?}");
}

/// A record naming another run than the one checked is refused before anything is written.
#[test]
fn a_record_of_another_run_is_refused() {
    let store = MemoryStore::new("mismatched");
    let directory = directory(&store);

    let (failed, recorded) = on(&store, move |ctx| async move {
        let plan = executed_plan(&ctx, false).await?;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        let failed = run.record(&ctx, &run_record("run-b"), &plan).await;
        Ok((
            failed,
            fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
        ))
    });

    let error = failed.unwrap_err().to_string();
    assert!(error.contains("run-b"), "{error}");
    assert!(recorded.record.is_none() && recorded.metrics.is_none());
}

/// Runs `pipeline` on a session with `store` registered.
fn on<T, Fut>(
    store: &MemoryStore,
    pipeline: impl FnOnce(SessionContext) -> Fut + Send + 'static,
) -> T
where
    T: Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    let store = store.clone();
    pipeline::run(
        move |ctx| {
            store.register(&ctx);
            pipeline(ctx)
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

/// Puts a few bytes that are not a Parquet file at `path` on `store`.
async fn put(store: &MemoryStore, path: &str) -> Result<()> {
    store
        .store()
        .put(
            ListingTableUrl::parse(path)?.prefix(),
            PutPayload::from_static(b"not a parquet file"),
        )
        .await?;
    Ok(())
}

/// The executed plan of a generated table run into a collecting sink, joined to a second one
/// when `join` holds.
async fn executed_plan(ctx: &SessionContext, join: bool) -> Result<Arc<dyn ExecutionPlan>> {
    let mut frame = make_range_table(ctx, 100, 32)?;
    if join {
        let other = make_range_table(ctx, 50, 32)?.select(vec![col("idx").alias("other")])?;
        frame = frame.join(other, JoinType::Inner, &["idx"], &["other"], None)?;
    }
    let collecting = Arc::new(CollectingSink::new(Arc::clone(frame.schema().inner())));
    let target = Arc::new(DataSinkTarget::new(collecting));
    Ok(
        sink::execute_and_retain(sink::run_into(frame, "collect", None, target)?)
            .await?
            .plan,
    )
}
