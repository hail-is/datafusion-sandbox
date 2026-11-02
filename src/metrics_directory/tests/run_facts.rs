//! Run facts: what measuring a write records, however the write ends, and the run record of a run
//! that completed.

use crate::{
    metrics_directory::{
        FormulationRecord, MetricsDirectory, RunStatus, WriteRecord,
        run_facts::{Coverage, RunFacts, Started},
    },
    pipeline::{self, PipelineOptions},
    sink::ExecutedSink,
    testing::{
        columns::{string_values, u64_values},
        fixture::{self, RecordedRun},
        memory_store::MemoryStore,
    },
};

use datafusion::{
    arrow::datatypes::Schema, common::DataFusionError, error::Result,
    physical_plan::empty::EmptyExec,
};
use std::{future, sync::Arc};
use tokio_util::sync::CancellationToken;

fn facts() -> RunFacts {
    RunFacts {
        started: Started::now(),
        formulation: FormulationRecord {
            name: "grouped-merge".to_string(),
            groups: Some(2),
            split_points: None,
        },
        row_ordering: "locus,alleles".to_string(),
        dataset_path: "memory://dataset/".to_string(),
        input_format: "parquet".to_string(),
        threads: 3,
    }
}

fn write_record() -> WriteRecord {
    WriteRecord {
        output_path: "memory://out/g.parquet".to_string(),
        output_format: "parquet".to_string(),
        compression: None,
    }
}

const COVERAGE: Coverage = Coverage {
    input_tables: 4,
    samples: 5,
};

/// A completed run's record holds every fact the run gathered, and what only completing gives.
#[test]
fn a_completed_run_s_record_holds_its_facts_coverage_and_measurements() {
    let record = facts()
        .completed("run-a".to_string(), COVERAGE, None, 17, 23, None)
        .unwrap();

    assert_eq!(record.run_id, "run-a");
    assert_eq!(record.status, RunStatus::Completed);
    assert_eq!(record.formulation, facts().formulation);
    assert_eq!(record.row_ordering, "locus,alleles");
    assert_eq!(record.dataset_path, "memory://dataset/");
    assert_eq!(record.input_format, "parquet");
    assert_eq!(record.threads, 3);
    assert_eq!(record.input_tables, Some(4));
    assert_eq!(record.samples, Some(5));
    assert_eq!(record.rows_written, Some(17));
    assert_eq!(record.execute_ns, Some(23));
    assert!(record.peak_rss_bytes > 0, "{record:?}");
}

/// Measuring a write that completes records it as completed, with its run metrics, and hands back
/// how it went.
#[test]
fn a_write_that_completes_is_recorded_completed_with_its_run_metrics() {
    let store = MemoryStore::new("run-facts-completed");

    let (measured, recorded) = pipeline::run(
        move |ctx| async move {
            store.register(&ctx);
            let directory = MetricsDirectory::new(&format!("{}metrics", store.url().as_str()));
            let run = directory.unrecorded(&ctx, "run-a").await?;
            let measured = facts()
                .measure(
                    &ctx,
                    run,
                    write_record(),
                    async { Ok((COVERAGE, 17)) },
                    |rows_written| async move {
                        Ok(ExecutedSink {
                            rows_written,
                            plan: Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
                            execute_ns: 23,
                        })
                    },
                )
                .await?;
            let recorded = fixture::read_recorded_run(&ctx, &directory, "run-a").await?;
            Ok((measured, recorded))
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    assert_eq!(measured.rows_written, 17);
    assert_eq!(measured.unrecorded_metrics, Vec::<String>::new());
    assert!(recorded.metrics.is_some());
    let record = recorded.record.unwrap();
    for (column, expected) in [
        ("status", "completed"),
        ("output_path", "memory://out/g.parquet"),
        ("input_tables", "4"),
        ("samples", "5"),
        ("rows_written", "17"),
        ("execute_ns", "23"),
    ] {
        assert_eq!(string_values(&record, column), [expected], "{column}");
    }
}

/// Where a test stops a measured write.
enum StopAt {
    Planning,
    Writing,
}

/// The error a measured write stopped at `stop` with `error` returns, and what it leaves in a
/// directory on `store`.
fn stopped(
    store: &MemoryStore,
    stop: StopAt,
    error: DataFusionError,
) -> (DataFusionError, RecordedRun) {
    let run_store = store.clone();
    pipeline::run(
        move |ctx| async move {
            run_store.register(&ctx);
            let directory = MetricsDirectory::new(&format!("{}metrics", run_store.url().as_str()));
            let run = directory.unrecorded(&ctx, "run-a").await?;
            let (planning, writing) = match stop {
                StopAt::Planning => (Err(error), None),
                StopAt::Writing => (Ok((COVERAGE, ())), Some(error)),
            };
            let returned = facts()
                .measure(&ctx, run, write_record(), async { planning }, |()| async {
                    Err::<ExecutedSink, _>(writing.unwrap_or_else(|| {
                        DataFusionError::Internal("a stopped plan went on to write".to_string())
                    }))
                })
                .await
                .unwrap_err();
            let recorded = fixture::read_recorded_run(&ctx, &directory, "run-a").await?;
            Ok((returned, recorded))
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

/// A run its write stopped had planned, so its record says what it covered, with no measurement
/// only completing gives and no run metrics.
#[test]
fn a_run_its_write_stopped_is_recorded_failed_with_its_coverage_and_returns_the_error() {
    let store = MemoryStore::new("run-facts-write-stopped");

    let (error, recorded) = stopped(
        &store,
        StopAt::Writing,
        DataFusionError::Execution("the store went away".to_string()),
    );

    assert!(error.to_string().contains("the store went away"), "{error}");
    assert!(recorded.metrics.is_none(), "{:?}", recorded.metrics);
    let record = recorded.record.unwrap();
    for (column, expected) in [
        ("status", "failed"),
        ("formulation", "grouped-merge"),
        ("output_path", "memory://out/g.parquet"),
        ("input_tables", "4"),
        ("samples", "5"),
        ("rows_written", ""),
        ("execute_ns", ""),
    ] {
        assert_eq!(string_values(&record, column), [expected], "{column}");
    }
    assert!(u64_values(&record, "run_ns")[0].is_some(), "{record:?}");
}

/// A run its planning stopped has no coverage to record.
#[test]
fn a_run_its_planning_stopped_is_recorded_failed_with_no_coverage() {
    let store = MemoryStore::new("run-facts-plan-stopped");

    let (_, recorded) = stopped(
        &store,
        StopAt::Planning,
        DataFusionError::Plan("no such dataset".to_string()),
    );

    let record = recorded.record.unwrap();
    for (column, expected) in [("status", "failed"), ("input_tables", ""), ("samples", "")] {
        assert_eq!(string_values(&record, column), [expected], "{column}");
    }
}

/// The error a cancelled pipeline returns.
fn cancellation() -> DataFusionError {
    let token = CancellationToken::new();
    token.cancel();
    pipeline::run(
        |ctx| async move { pipeline::until_cancelled(&ctx, future::pending::<Result<()>>()).await },
        PipelineOptions::single_threaded().cancelled_by(token),
    )
    .unwrap_err()
}

/// A run its cancellation stopped is recorded as cancelled, not failed.
#[test]
fn a_run_its_cancellation_stopped_is_recorded_cancelled() {
    let store = MemoryStore::new("run-facts-cancelled");

    let (error, recorded) = stopped(&store, StopAt::Writing, cancellation());

    assert!(pipeline::is_cancelled(&error), "{error}");
    let record = recorded.record.unwrap();
    assert_eq!(string_values(&record, "status"), ["cancelled"]);
}
