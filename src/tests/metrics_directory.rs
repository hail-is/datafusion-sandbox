//! Checking and recording runs in a metrics directory on in-memory object stores, including ones
//! that fail the writes under one of its tables. `shared_fixture` checks the shared fixture the
//! Python readers read.

mod shared_fixture;

use crate::{
    format::InputFormat,
    metrics_directory::{Grid, MetricsDirectory, ProbeRecord, ReplayCount, RunRecord, RunStatus},
    pipeline::{self, PipelineOptions},
    sink::{self, CollectingSink, DataSinkTarget},
    testing::{
        columns::{f64_values, string_values, timestamp_values, u64_values},
        fixture::{self, RecordedRun},
        generated::make_range_table,
        memory_store::MemoryStore,
        probe_series::{noisy_series, recorded_would_stop},
        run_records::run_record,
    },
    throughput_probe::{self, ProbeSettings, ProbedKind, ProgressSample},
};

use datafusion::{
    arrow::record_batch::RecordBatch,
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
    prelude::{JoinType, SessionContext, col},
};
use object_store::{ObjectStoreExt, PutPayload};
use std::{future::Future, num::NonZeroU32, sync::Arc, time::Duration};

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
    assert_eq!(
        directory.progress_samples_path("run-a"),
        "gs://bucket/benchmarks/progress/run-a.parquet"
    );
    assert_eq!(
        directory.checks_path("run-a"),
        "gs://bucket/benchmarks/checks/run-a.parquet"
    );
    assert_eq!(
        directory.replay_baselines_path("run-a"),
        "gs://bucket/benchmarks/replay-baselines/run-a.parquet"
    );
    assert_eq!(
        directory.replays_path("run-a"),
        "gs://bucket/benchmarks/replays/run-a.parquet"
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
/// back the names of the metrics the plan reported that the run metrics table has no column for.
/// Once recorded, the id is refused.
#[test]
fn recording_a_run_writes_both_tables_and_hands_back_its_unrecorded_metrics() {
    let store = MemoryStore::new("recorded");
    let directory = directory(&store);
    let record = run_record("run-a");

    let (unrecorded, recorded, refused) = on(&store, move |ctx| async move {
        // A hash join reports metrics no run metrics column takes.
        let plan = executed_plan(&ctx, true).await?;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        let unrecorded = run.record(&ctx, &record, &plan).await?;
        Ok((
            unrecorded,
            fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
            directory.unrecorded(&ctx, "run-a").await.is_err(),
        ))
    });

    for name in ["build_time", "join_time"] {
        assert!(unrecorded.contains(&name.to_string()), "{unrecorded:?}");
    }
    assert_holds_run_record_of_run_a(&recorded.record.unwrap());
    let metrics = recorded.metrics.unwrap();
    for name in &unrecorded {
        assert!(metrics.column_by_name(name).is_none(), "{name}");
    }
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
    let RecordedRun {
        record, metrics, ..
    } = recorded;
    assert!(record.is_none(), "{record:?}");
    assert!(metrics.is_some());
    retried.unwrap();
}

/// Recording a probe writes its progress samples beside the run metrics and the run record, and
/// a measured write's recording writes none.
#[test]
fn recording_a_probe_writes_its_progress_samples_as_a_third_table() {
    let store = MemoryStore::new("probe-recorded");
    let directory = directory(&store);
    let samples = [(1_000, 0), (100_002_000, 640)]
        .map(|(elapsed_ns, rows)| ProgressSample { elapsed_ns, rows });

    let (probe, measured) = on(&store, move |ctx| async move {
        let plan = executed_plan(&ctx, false).await?;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        run.record_probe(&ctx, &run_record("run-a"), &plan, &samples)
            .await?;
        let run = directory.unrecorded(&ctx, "run-b").await?;
        run.record(&ctx, &run_record("run-b"), &plan).await?;
        Ok((
            fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
            fixture::read_recorded_run(&ctx, &directory, "run-b").await?,
        ))
    });

    let progress_samples = probe.progress_samples.unwrap();
    assert_eq!(string_values(&progress_samples, "run_id"), ["run-a"; 2]);
    assert_eq!(
        u64_values(&progress_samples, "sample_index"),
        [Some(0), Some(1)]
    );
    assert_eq!(
        u64_values(&progress_samples, "elapsed_ns"),
        [Some(1_000), Some(100_002_000)]
    );
    assert_eq!(u64_values(&progress_samples, "rows"), [Some(0), Some(640)]);
    assert_holds_run_record_of_run_a(&probe.record.unwrap());
    assert!(probe.metrics.is_some());
    assert!(measured.record.is_some());
    assert!(measured.progress_samples.is_none());
}

/// A probe whose progress samples write fails records nothing: the samples go first.
#[test]
fn a_failed_progress_samples_write_records_nothing() {
    let store = MemoryStore::failing_writes_under("progress-fails", "benchmarks/progress");
    let directory = directory(&store);
    let samples = [ProgressSample {
        elapsed_ns: 1_000,
        rows: 0,
    }];

    let (failed, recorded) = on(&store, move |ctx| async move {
        let plan = executed_plan(&ctx, false).await?;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        let failed = run
            .record_probe(&ctx, &run_record("run-a"), &plan, &samples)
            .await;
        Ok((
            failed,
            fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
        ))
    });

    let error = failed.unwrap_err().to_string();
    assert!(error.contains("benchmarks/progress"), "{error}");
    let RecordedRun {
        record,
        metrics,
        progress_samples,
    } = recorded;
    assert!(record.is_none(), "{record:?}");
    assert!(metrics.is_none(), "{metrics:?}");
    assert!(progress_samples.is_none(), "{progress_samples:?}");
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
    let RecordedRun {
        record, metrics, ..
    } = recorded;
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

/// A run that failed or was cancelled is recorded by its run record alone, which names its status,
/// and its id is then recorded: checking it again is refused.
#[test]
fn recording_an_unfinished_run_writes_its_run_record_alone_and_records_the_id() {
    for (status, name) in [
        (RunStatus::Failed, "failed"),
        (RunStatus::Cancelled, "cancelled"),
    ] {
        let store = MemoryStore::new("unfinished");
        let directory = directory(&store);
        let record = RunRecord {
            status,
            rows_written: None,
            execute_ns: None,
            ..run_record("run-a")
        };

        let (recorded, rechecked) = on(&store, move |ctx| async move {
            let run = directory.unrecorded(&ctx, "run-a").await?;
            run.record_unfinished(&ctx, &record).await?;
            Ok((
                fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
                directory.unrecorded(&ctx, "run-a").await.map(|_| ()),
            ))
        });

        let RecordedRun {
            record,
            metrics,
            progress_samples,
        } = recorded;
        let record = record.unwrap();
        assert_eq!(string_values(&record, "run_id"), ["run-a"], "{name}");
        assert_eq!(string_values(&record, "status"), [name]);
        assert!(metrics.is_none(), "{name}: {metrics:?}");
        assert!(progress_samples.is_none(), "{name}: {progress_samples:?}");
        let message = rechecked.unwrap_err().to_string();
        assert!(message.contains("already has a run record"), "{message}");
    }
}

/// Recording as unfinished a record whose status is completed, or that names another run than the
/// one checked, is refused before anything is written: a completed run records its run metrics.
#[test]
fn recording_a_completed_or_another_run_as_unfinished_is_refused() {
    let store = MemoryStore::new("unfinished-refused");
    let directory = directory(&store);
    let another = RunRecord {
        status: RunStatus::Failed,
        ..run_record("run-b")
    };

    let (completed, mismatched, recorded) = on(&store, move |ctx| async move {
        let run = directory.unrecorded(&ctx, "run-a").await?;
        let completed = run.record_unfinished(&ctx, &run_record("run-a")).await;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        let mismatched = run.record_unfinished(&ctx, &another).await;
        Ok((
            completed,
            mismatched,
            fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
        ))
    });

    let completed = completed.unwrap_err().to_string();
    assert!(completed.contains("completed"), "{completed}");
    let mismatched = mismatched.unwrap_err().to_string();
    assert!(mismatched.contains("run-b"), "{mismatched}");
    assert!(recorded.record.is_none() && recorded.metrics.is_none());
}

/// Settings under which [`noisy_series`] settles quickly.
fn eager_settings() -> ProbeSettings {
    ProbeSettings {
        batch_duration: Duration::from_millis(300),
        precision: 0.3,
        consecutive_checks: NonZeroU32::MIN,
        window_groups: 5,
        min_duration: Duration::ZERO,
        ..ProbeSettings::default()
    }
}

/// The run record of a shadow probe of `run_id` with `settings` over `samples`, whose first
/// partition end is `first_partition_end_ns`, as a shadow probe deciding after every sample
/// records it.
fn shadow_record(
    run_id: &str,
    settings: &ProbeSettings,
    samples: &[ProgressSample],
    first_partition_end_ns: Option<u64>,
) -> RunRecord {
    let would_stop = recorded_would_stop(settings, samples, first_partition_end_ns);
    RunRecord {
        probe: Some(ProbeRecord {
            settings: settings.clone(),
            decision: throughput_probe::decide(settings, samples, first_partition_end_ns).unwrap(),
            first_partition_end_ns,
            kind: ProbedKind::Shadow { would_stop },
        }),
        ..run_record(run_id)
    }
}

/// The grid of the one combination `settings`.
fn grid_at(settings: &ProbeSettings) -> Grid {
    Grid {
        batch_durations: vec![settings.batch_duration],
        window_groups: vec![settings.window_groups],
        precisions: vec![settings.precision],
        consecutive_checks: vec![settings.consecutive_checks],
        min_durations: vec![settings.min_duration],
    }
}

/// Replaying a directory replays its shadow probe and skips its probe and its measured write.
/// Over a grid holding the shadow probe's recorded settings, the replay at them reproduces the
/// would-stop and the end-of-run estimate it recorded.
#[test]
fn a_replay_at_a_shadow_probe_s_recorded_settings_reproduces_its_record() {
    let store = MemoryStore::new("replayed");
    let directory = directory(&store);
    let settings = eager_settings();
    let samples = noisy_series(2, 4, 0.05);
    let first_partition_end_ns = samples.get(150).map(|sample| sample.elapsed_ns);
    let shadow = shadow_record("shadow", &settings, &samples, first_partition_end_ns);
    let ProbedKind::Shadow {
        would_stop: Some(would_stop),
    } = shadow.probe.as_ref().unwrap().kind.clone()
    else {
        panic!("the shadow probe never settled: {shadow:?}");
    };
    let decision = shadow.probe.as_ref().unwrap().decision.clone();
    let probe = RunRecord {
        probe: Some(ProbeRecord {
            kind: ProbedKind::Probe,
            ..shadow.probe.clone().unwrap()
        }),
        ..run_record("probe")
    };
    let grid = grid_at(&settings);

    let (count, replays, baselines) = on(&store, move |ctx| async move {
        let plan = executed_plan(&ctx, false).await?;
        for record in [&shadow, &probe] {
            let run = directory.unrecorded(&ctx, &record.run_id).await?;
            run.record_probe(&ctx, record, &plan, &samples).await?;
        }
        let run = directory.unrecorded(&ctx, "write").await?;
        run.record(&ctx, &run_record("write"), &plan).await?;
        let count = directory.replay(&ctx, &grid).await?;
        let read = |path: String| {
            let ctx = ctx.clone();
            async move { fixture::read_file(&ctx, &path, &InputFormat::PARQUET, None).await }
        };
        Ok((
            count,
            read(directory.replays_path("shadow")).await?,
            read(directory.replay_baselines_path("shadow")).await?,
        ))
    });

    assert_eq!(
        count,
        ReplayCount {
            replayed: 1,
            skipped: 2,
            removed: 0,
        }
    );
    let [replays] = replays.as_slice() else {
        panic!("{replays:?}");
    };
    assert_eq!(
        (
            u64_values(replays, "would_stop_ns"),
            f64_values(replays, "would_be_steady_state_throughput"),
            f64_values(replays, "would_be_relative_half_width"),
            u64_values(replays, "would_be_warmup_end_ns"),
        ),
        (
            vec![Some(would_stop.window_end_ns)],
            vec![would_stop.steady_state_throughput],
            vec![would_stop.relative_half_width],
            vec![would_stop.warmup_end_ns],
        )
    );
    let [baselines] = baselines.as_slice() else {
        panic!("{baselines:?}");
    };
    assert_eq!(
        (
            f64_values(baselines, "steady_state_throughput"),
            f64_values(baselines, "relative_half_width"),
            u64_values(baselines, "warmup_end_ns"),
            u64_values(baselines, "window_end_ns"),
        ),
        (
            vec![decision.steady_state_throughput],
            vec![decision.relative_half_width],
            vec![decision.warmup_end_ns],
            vec![Some(decision.window_end_ns)],
        )
    );
}

/// A replay deletes the replay tables' files of runs that are not shadow probes here, such as
/// one whose run record was removed, and leaves the current shadow probe's files and any file
/// that is not a table's alone.
#[test]
fn a_replay_removes_the_replays_of_runs_no_longer_recorded() {
    let store = MemoryStore::new("stale-replays");
    let directory = directory(&store);
    let settings = eager_settings();
    let samples = noisy_series(2, 4, 0.05);
    let first_partition_end_ns = samples.get(150).map(|sample| sample.elapsed_ns);
    let shadow = shadow_record("shadow", &settings, &samples, first_partition_end_ns);
    let grid = grid_at(&settings);
    let stale = [
        directory.checks_path("gone"),
        directory.replay_baselines_path("gone"),
        directory.replays_path("gone"),
        directory.replays_path("older"),
    ];
    let note = format!("{}/replays/notes.txt", directory.path());

    let held = store.clone();
    let (count, left) = on(&store, move |ctx| async move {
        let plan = executed_plan(&ctx, false).await?;
        let run = directory.unrecorded(&ctx, "shadow").await?;
        run.record_probe(&ctx, &shadow, &plan, &samples).await?;
        for path in stale.iter().chain([&note]) {
            put(&held, path).await?;
        }
        let count = directory.replay(&ctx, &grid).await?;
        let mut left = Vec::new();
        for table in ["checks", "replay-baselines", "replays"] {
            left.extend(held.locations_under(&format!("benchmarks/{table}")).await);
        }
        Ok((count, left))
    });

    assert_eq!(
        count,
        ReplayCount {
            replayed: 1,
            skipped: 0,
            removed: 2,
        }
    );
    assert_eq!(
        left.iter().map(ToString::to_string).collect::<Vec<_>>(),
        [
            "benchmarks/checks/shadow.parquet",
            "benchmarks/replay-baselines/shadow.parquet",
            "benchmarks/replays/notes.txt",
            "benchmarks/replays/shadow.parquet",
        ]
    );
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
/// Asserts that `record`, a run record read back, holds what [`run_record`] gives for `run-a`:
/// every fact of a measured write, and no probe.
fn assert_holds_run_record_of_run_a(record: &RecordBatch) {
    for (column, value) in [
        ("run_id", "run-a"),
        ("formulation", "grouped-merge"),
        ("split_points", ""),
        ("row_ordering", "locus"),
        ("dataset_path", "gs://bucket/refs"),
        ("input_format", "vortex"),
        ("output_format", "parquet"),
        ("compression", "snappy"),
        ("output_path", "gs://bucket/combined.parquet"),
        ("action", ""),
    ] {
        assert_eq!(string_values(record, column), [value], "{column}");
    }
    for (column, value) in [
        ("groups", 2),
        ("threads", 1),
        ("input_tables", 4),
        ("samples", 4),
        ("rows_written", 32),
        ("run_ns", 2_000),
        ("execute_ns", 1_000),
        ("peak_rss_bytes", 1 << 20),
    ] {
        assert_eq!(u64_values(record, column), [Some(value)], "{column}");
    }
    assert_eq!(
        timestamp_values(record, "started_at"),
        [Some(1_700_000_000_000_000_000)]
    );
}

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
        sink::execute_and_retain(sink::run_into(frame, "collect", &[], target)?)
            .await?
            .plan,
    )
}
