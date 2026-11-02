//! Writes: a plan's rows written to an in-memory store of its own, what lies beside them, and what
//! a measured write records.

use super::{MemoryStores, hostile, sorted_union, wheel_bytes};
use crate::{
    backend::{
        Action, Backend, BackendError, ErrorCode, InputTableScan, ObjectStoreResolver, Outcome,
        Settings, Versions, Write,
    },
    format::InputFormat,
    locus::LocusRepresentation,
    metrics_directory::{FormulationRecord, MetricsDirectory},
    ordered_frame::OutputLayout,
    pipeline::{self, PipelineOptions},
    row_ordering::{RowOrdering, SortColumn},
    testing::{
        columns::{string_values, u64_values},
        fixture::{self, DatasetFixture, FixtureFormat, RecordedRun},
        memory_store::MemoryStore,
        plan_shape::exec_names,
    },
};

use datafusion::{
    arrow::record_batch::RecordBatch,
    catalog::TableProvider,
    error::Result,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::SessionContext,
};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

fn mixed() -> &'static Arc<DatasetFixture> {
    fixture::mixed_dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::ContigPosition)
}

/// A backend over `fixture`'s store and `output`, whose runs check their rows' order at run time.
fn backend(fixture: &'static Arc<DatasetFixture>, output: &MemoryStore) -> Backend {
    Backend::new(Arc::new(stores(fixture, output))).verifying_order()
}

fn stores(fixture: &'static Arc<DatasetFixture>, output: &MemoryStore) -> MemoryStores {
    MemoryStores(vec![
        (
            fixture.table_path().object_store(),
            Arc::clone(fixture.store()),
        ),
        (output.url().clone(), Arc::clone(output.store())),
    ])
}

/// A write of a Parquet file at `path` on `output`, holding `ordering`.
pub(super) fn parquet_write(
    output: &MemoryStore,
    path: &str,
    ordering: &[SortColumn],
    annotate_samples: bool,
) -> Write {
    Write::new(
        format!("{}{path}", output.url().as_str()),
        "parquet",
        None,
        ordering.to_vec(),
        OutputLayout::SingleFile,
        annotate_samples,
    )
    .unwrap()
}

fn run(
    backend: &Backend,
    bytes: &[u8],
    action: Action,
    run_id: Option<&str>,
) -> Result<Outcome, BackendError> {
    backend
        .start(
            bytes,
            action,
            &Settings::new(),
            run_id.map(str::to_string),
            Versions::backend(),
        )?
        .wait()
}

/// The plan bytes of the sorted union of the named input tables of `fixture`, and their ordering.
fn sorted_union_of(
    fixture: &'static Arc<DatasetFixture>,
    names: &[&str],
) -> (Vec<u8>, Vec<SortColumn>) {
    let opened = open(fixture, RowOrdering::locus());
    let scans = opened
        .input_tables
        .iter()
        .filter(|table| names.contains(&table.scan.description().input_table.as_str()))
        .collect::<Vec<_>>();
    let plan = LogicalPlanBuilder::from(union_of(&scans))
        .sort(opened.ordering.iter().map(SortColumn::sort_expression))
        .unwrap()
        .build()
        .unwrap();
    (wheel_bytes(&plan), opened.ordering)
}

fn open(
    fixture: &'static Arc<DatasetFixture>,
    row_ordering: RowOrdering,
) -> crate::backend::OpenedDataset {
    Backend::new(Arc::new(MemoryStores(vec![(
        fixture.table_path().object_store(),
        Arc::clone(fixture.store()),
    )])))
    .open_dataset(
        fixture.table_path().as_str(),
        fixture.input_format(),
        row_ordering,
        Versions::backend(),
    )
    .unwrap()
}

/// The union of `scans`' scans, unsorted.
fn union_of(scans: &[&InputTableScan]) -> LogicalPlan {
    let ctx = SessionContext::new();
    let mut plans = scans.iter().map(|table| {
        let provider: Arc<dyn TableProvider> = table.scan.clone();
        ctx.read_table(provider).unwrap().into_unoptimized_plan()
    });
    let first = LogicalPlanBuilder::from(plans.next().unwrap());
    plans
        .fold(first, |union, scan| union.union(scan).unwrap())
        .build()
        .unwrap()
}

/// What `read` makes of a session serving `store`.
fn reading<T: Send + 'static, Fut: Future<Output = Result<T>> + Send + 'static>(
    store: &MemoryStore,
    read: impl FnOnce(SessionContext) -> Fut + Send + 'static,
) -> T {
    let store = store.clone();
    pipeline::run(
        move |ctx| {
            store.register(&ctx);
            read(ctx)
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

/// The Parquet file at `path` on `store`.
fn file_at(store: &MemoryStore, path: &str) -> Vec<RecordBatch> {
    let path = format!("{}{path}", store.url().as_str());
    reading(store, move |ctx| async move {
        fixture::read_file(&ctx, &path, &InputFormat::PARQUET, None).await
    })
}

/// What `metrics` on `store` holds for `run_id`.
pub(super) fn recorded(
    store: &MemoryStore,
    metrics: &MetricsDirectory,
    run_id: &str,
) -> RecordedRun {
    let metrics = metrics.clone();
    let run_id = run_id.to_string();
    reading(store, move |ctx| async move {
        fixture::read_recorded_run(&ctx, &metrics, &run_id).await
    })
}

/// The paths of what `store` holds under `out`.
pub(super) fn locations(store: &MemoryStore) -> Vec<String> {
    let listed = store.clone();
    reading(store, move |_| async move {
        Ok(listed
            .locations_under("out")
            .await
            .iter()
            .map(ToString::to_string)
            .collect())
    })
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn samples_of(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| fixture::string_column(batch, "s"))
        .collect()
}

/// A write puts every row in one file, in the Action's ordering (the backend checks that at run
/// time), and nothing else beside it.
#[test]
fn a_write_writes_every_row_to_its_file_and_no_annotation_table_unless_asked() {
    let fixture = mixed();
    let output = MemoryStore::new("backend-write");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());

    let outcome = run(
        &backend(fixture, &output),
        &bytes,
        Action::Write(parquet_write(&output, "out/g.parquet", &ordering, false)),
        None,
    )
    .unwrap();

    let written = file_at(&output, "out/g.parquet");
    let Outcome::RowsWritten(rows_written) = outcome else {
        panic!("expected rows written, got {outcome:?}");
    };
    assert_eq!(usize::try_from(rows_written).unwrap(), row_count(&written));
    assert!(rows_written > 0);
    assert_eq!(locations(&output), ["out/g.parquet"]);
}

/// The annotation table's samples are the union of the sample sets of the input tables the plan
/// read, not of the whole dataset's: the mixed fixture's other input tables hold other samples.
#[test]
fn a_write_annotating_samples_over_some_input_tables_writes_exactly_their_samples() {
    let fixture = mixed();
    let opened = open(fixture, RowOrdering::locus());
    let [first, second, third] = opened.input_tables.as_slice() else {
        panic!("the mixed fixture has three input tables");
    };
    let output = MemoryStore::new("backend-annotated-write");
    let (bytes, ordering) = sorted_union_of(
        fixture,
        &[
            first.scan.description().input_table.as_str(),
            third.scan.description().input_table.as_str(),
        ],
    );

    run(
        &backend(fixture, &output),
        &bytes,
        Action::Write(parquet_write(&output, "out/g.parquet", &ordering, true)),
        None,
    )
    .unwrap();

    let mut expected = [first.sample_set.clone(), third.sample_set.clone()].concat();
    expected.sort();
    assert_eq!(
        samples_of(&file_at(&output, "out/g.samples.parquet")),
        expected
    );
    for sample in &second.sample_set {
        assert!(!expected.contains(sample), "{sample} is in the third table");
    }
}

/// The table marks complete data, so a failed write leaves none, not even an earlier write's.
#[test]
fn a_failed_data_write_leaves_no_annotation_table() {
    let fixture = mixed();
    let output = MemoryStore::new("backend-failed-write");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let write = parquet_write(&output, "out/g.parquet", &ordering, true);
    run(
        &backend(fixture, &output),
        &bytes,
        Action::Write(write.clone()),
        None,
    )
    .unwrap();
    assert!(row_count(&file_at(&output, "out/g.samples.parquet")) > 0);
    let failing = output.with_failing_writes_under("out/g.parquet");

    let error = run(
        &backend(fixture, &failing),
        &bytes,
        Action::Write(write),
        None,
    )
    .unwrap_err();

    assert_eq!(error.code(), ErrorCode::Backend, "{error}");
    let locations = locations(&output);
    assert_eq!(locations, ["out/g.parquet"], "{error}");
}

/// A write of one file per partition waits for per-partition assertions.
#[test]
fn a_write_of_one_file_per_partition_is_an_invalid_action() {
    let error = Write::new(
        "memory://out/g".to_string(),
        "parquet",
        None,
        vec![],
        OutputLayout::FilePerPartition,
        false,
    )
    .unwrap_err();

    assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
    assert!(error.message().contains("per partition"), "{error}");
}

/// What a write's fields can't mean is refused as the Action being malformed.
#[test]
fn a_write_of_an_unknown_format_compression_or_path_is_an_invalid_action() {
    for (path, format, compression, problem) in [
        ("memory://out/g.parquet", "csv", None, "'csv'"),
        (
            "memory://out/g.parquet",
            "parquet",
            Some("brotli"),
            "'brotli'",
        ),
        ("memory://out/g", "parquet", None, "no extension"),
        (
            "memory://out/g.samples.parquet",
            "parquet",
            None,
            "sample annotation table",
        ),
        ("memory://out/.g.parquet", "parquet", None, "hidden"),
    ] {
        let error = Write::new(
            path.to_string(),
            format,
            compression,
            vec![],
            OutputLayout::SingleFile,
            false,
        )
        .unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
        assert!(error.message().contains(problem), "{error}");
    }
}

/// An explain renders the write's sink over the plan a write runs, under the hostile session too,
/// and leaves the store as it was.
#[test]
fn explaining_a_write_renders_its_sink_with_no_sort_and_writes_nothing() {
    let fixture = mixed();
    let output = MemoryStore::new("backend-explained-write");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let write = parquet_write(&output, "out/g.parquet", &ordering, true);

    let outcome = backend(fixture, &output)
        .start(
            &bytes,
            Action::Explain {
                ordering,
                write: Some(write),
            },
            &hostile(),
            None,
            Versions::backend(),
        )
        .unwrap()
        .wait()
        .unwrap();

    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    let names = exec_names(&plan);
    assert!(names.contains(&"DataSinkExec"), "{plan}");
    assert!(names.contains(&"OrderingAssertionExec"), "{plan}");
    assert!(!names.contains(&"SortExec"), "{plan}");
    let locations = locations(&output);
    assert!(locations.is_empty(), "{locations:?}");
}

/// An explain analyze performs the write it renders, table included.
#[test]
fn explain_analyzing_a_write_performs_it_and_renders_its_metrics() {
    let fixture = mixed();
    let output = MemoryStore::new("backend-analyzed-write");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());

    let outcome = run(
        &backend(fixture, &output),
        &bytes,
        Action::ExplainAnalyze {
            ordering: ordering.clone(),
            write: Some(parquet_write(&output, "out/g.parquet", &ordering, true)),
        },
        None,
    )
    .unwrap();

    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(plan.contains("Plan with Metrics"), "{plan}");
    assert!(plan.contains("DataSinkExec"), "{plan}");
    assert!(row_count(&file_at(&output, "out/g.parquet")) > 0);
    assert!(row_count(&file_at(&output, "out/g.samples.parquet")) > 0);
}

/// An explain's two orderings can't disagree: one holds the rows, the other labels them.
#[test]
fn an_explain_of_a_write_with_another_ordering_is_an_invalid_action() {
    let fixture = mixed();
    let output = MemoryStore::new("backend-mismatched-explain");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let write = parquet_write(&output, "out/g.parquet", &ordering[..1], false);

    for action in [
        Action::Explain {
            ordering: ordering.clone(),
            write: Some(write.clone()),
        },
        Action::ExplainAnalyze {
            ordering,
            write: Some(write),
        },
    ] {
        let error = run(&backend(fixture, &output), &bytes, action, None).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
    }
}

/// A measured write of `bytes` to `out/g.parquet`, recorded under `metrics` on `output`.
pub(super) fn measured(
    output: &MemoryStore,
    ordering: &[SortColumn],
    annotate_samples: bool,
) -> (Action, MetricsDirectory) {
    let metrics = MetricsDirectory::new(&format!("{}metrics", output.url().as_str()));
    (
        Action::MeasuredWrite {
            write: parquet_write(output, "out/g.parquet", ordering, annotate_samples),
            metrics_directory: metrics.clone(),
            formulation: FormulationRecord {
                name: "grouped-merge".to_string(),
                groups: Some(2),
                split_points: None,
            },
        },
        metrics,
    )
}

/// The record's dataset facts come from the plan's scans, and only the formulation from the Action.
#[test]
fn a_measured_write_records_the_dataset_from_its_scans_and_the_formulation_from_its_action() {
    let fixture = mixed();
    let opened = open(fixture, RowOrdering::locus());
    let [first, second, _] = opened.input_tables.as_slice() else {
        panic!("the mixed fixture has three input tables");
    };
    let names = [first, second].map(|table| table.scan.description().input_table.as_str());
    let (bytes, ordering) = sorted_union_of(fixture, &names);
    let output = MemoryStore::new("backend-measured-write");
    let (action, metrics) = measured(&output, &ordering, true);
    let samples = first.sample_set.len() + second.sample_set.len();

    let outcome = run(&backend(fixture, &output), &bytes, action, Some("run-a")).unwrap();

    let Outcome::Measured {
        rows_written,
        unrecorded_metrics,
    } = outcome
    else {
        panic!("expected a measured outcome, got {outcome:?}");
    };
    let RecordedRun {
        record,
        metrics: run_metrics,
        ..
    } = recorded(&output, &metrics, "run-a");
    let record = record.unwrap();
    let output_path = format!("{}out/g.parquet", output.url().as_str());
    let (rows_text, samples_text) = (rows_written.to_string(), samples.to_string());
    for (column, expected) in [
        ("run_id", "run-a"),
        ("status", "completed"),
        ("formulation", "grouped-merge"),
        ("groups", "2"),
        ("split_points", ""),
        ("dataset_path", fixture.table_path().as_str()),
        ("input_format", "parquet"),
        ("row_ordering", "locus"),
        ("output_path", output_path.as_str()),
        ("output_format", "parquet"),
        ("input_tables", "2"),
        ("samples", samples_text.as_str()),
        ("rows_written", rows_text.as_str()),
    ] {
        assert_eq!(string_values(&record, column), [expected], "{column}");
    }
    assert!(u64_values(&record, "execute_ns")[0].is_some(), "{record:?}");
    assert!(run_metrics.is_some_and(|metrics| metrics.num_rows() > 0));
    assert!(
        unrecorded_metrics.windows(2).all(|pair| pair[0] < pair[1]),
        "sorted without repeats: {unrecorded_metrics:?}"
    );
    assert_eq!(
        usize::try_from(rows_written).unwrap(),
        row_count(&file_at(&output, "out/g.parquet"))
    );
    assert_eq!(
        samples_of(&file_at(&output, "out/g.samples.parquet")).len(),
        samples
    );
}

/// A failed data write is recorded before the run's runtimes go, with the scans' facts and no
/// measurement it never made, and its error is still the run's.
#[test]
fn a_measured_write_whose_data_write_fails_records_failed_and_raises_a_backend_error() {
    let fixture = mixed();
    let output = MemoryStore::failing_writes_under("backend-failed-measured", "out");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (action, metrics) = measured(&output, &ordering, true);

    let error = run(&backend(fixture, &output), &bytes, action, Some("run-a")).unwrap_err();

    assert_eq!(error.code(), ErrorCode::Backend, "{error}");
    let RecordedRun {
        record,
        metrics: run_metrics,
        ..
    } = recorded(&output, &metrics, "run-a");
    let record = record.unwrap();
    for (column, expected) in [
        ("status", "failed"),
        ("formulation", "grouped-merge"),
        ("dataset_path", fixture.table_path().as_str()),
        ("input_tables", "3"),
        ("rows_written", ""),
        ("execute_ns", ""),
    ] {
        assert_eq!(string_values(&record, column), [expected], "{column}");
    }
    assert!(u64_values(&record, "samples")[0].is_some(), "{record:?}");
    assert!(run_metrics.is_none(), "{run_metrics:?}");
}

/// A plan that fails its assertion never executes, but it did start, so a campaign sees it. Its
/// error keeps its own code.
#[test]
fn a_measured_write_that_fails_its_ordering_assertion_records_failed_with_that_code() {
    let fixture = mixed();
    let (bytes, _) = sorted_union(fixture, RowOrdering::locus());
    let output = MemoryStore::new("backend-unheld-measured");
    let ordering = [SortColumn {
        column: "alleles".to_string(),
        ascending: true,
        nulls_first: false,
    }];
    let (action, metrics) = measured(&output, &ordering, false);

    let error = run(&backend(fixture, &output), &bytes, action, Some("run-a")).unwrap_err();

    assert_eq!(error.code(), ErrorCode::OrderingAssertionFailed, "{error}");
    let record = recorded(&output, &metrics, "run-a").record.unwrap();
    assert_eq!(string_values(&record, "status"), ["failed"]);
}

/// A recorded run is never replaced: a repeated id fails the run before it writes anything.
#[test]
fn a_measured_write_under_a_recorded_run_id_fails_and_replaces_nothing() {
    let fixture = mixed();
    let output = MemoryStore::new("backend-repeated-id");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (action, metrics) = measured(&output, &ordering, false);
    run(
        &backend(fixture, &output),
        &bytes,
        action.clone(),
        Some("run-a"),
    )
    .unwrap();
    let first = recorded(&output, &metrics, "run-a").record.unwrap();

    let error = run(&backend(fixture, &output), &bytes, action, Some("run-a")).unwrap_err();

    assert_eq!(error.code(), ErrorCode::Backend, "{error}");
    assert!(error.message().contains("run-a"), "{error}");
    let record = recorded(&output, &metrics, "run-a").record.unwrap();
    assert_eq!(u64_values(&record, "run_ns"), u64_values(&first, "run_ns"));
    assert_eq!(string_values(&record, "status"), ["completed"]);
}

/// A run record describes one dataset, which is one path, format and row ordering.
#[test]
fn a_measured_write_of_a_plan_over_two_datasets_is_an_invalid_action() {
    let fixture = mixed();
    let output = MemoryStore::new("backend-two-datasets");
    let by_locus = open(fixture, RowOrdering::locus());
    let by_alleles = open(fixture, RowOrdering::locus_then_alleles());
    let scans = [&by_locus.input_tables[0], &by_alleles.input_tables[1]];
    let plan = LogicalPlanBuilder::from(union_of(&scans))
        .sort(by_alleles.ordering.iter().map(SortColumn::sort_expression))
        .unwrap()
        .build()
        .unwrap();
    let (action, metrics) = measured(&output, &by_alleles.ordering, false);

    let error = run(
        &backend(fixture, &output),
        &wheel_bytes(&plan),
        action,
        Some("run-a"),
    )
    .unwrap_err();

    assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
    assert!(error.message().contains("one dataset"), "{error}");
    assert!(recorded(&output, &metrics, "run-a").record.is_none());
}

/// With no scans there is no dataset to record and no sample set to annotate.
#[test]
fn a_plan_with_no_scans_can_t_be_measured_or_annotated() {
    let fixture = mixed();
    let output = MemoryStore::new("backend-no-scans");
    let (_, ordering) = sorted_union(fixture, RowOrdering::locus());
    let bytes = wheel_bytes(&LogicalPlanBuilder::empty(false).build().unwrap());
    let (measured_write, _) = measured(&output, &ordering, false);

    for action in [
        measured_write,
        Action::Write(parquet_write(&output, "out/g.parquet", &ordering, true)),
    ] {
        let error = run(&backend(fixture, &output), &bytes, action, None).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
    }
}

/// A resolver that remembers the paths it was asked to serve, over in-memory stores.
#[derive(Debug)]
struct RecordingPaths {
    stores: MemoryStores,
    paths: Mutex<Vec<String>>,
}

impl ObjectStoreResolver for RecordingPaths {
    fn pipeline_options(&self, threads: NonZeroUsize, paths: &[&str]) -> Result<PipelineOptions> {
        self.paths
            .lock()
            .unwrap()
            .extend(paths.iter().map(ToString::to_string));
        self.stores.pipeline_options(threads, paths)
    }
}

/// The resolver registers a store per path it is given, so a write's output and metrics paths must
/// reach it, or a `gs://` output on another bucket has no store. The in-memory adapter serves any
/// path, so this asks the seam itself.
#[test]
fn a_measured_write_asks_the_resolver_for_its_output_and_metrics_paths() {
    let fixture = mixed();
    let output = MemoryStore::new("backend-resolved-paths");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (action, metrics) = measured(&output, &ordering, false);
    let resolver = Arc::new(RecordingPaths {
        stores: stores(fixture, &output),
        paths: Mutex::default(),
    });

    run(&Backend::new(resolver.clone()), &bytes, action, None).unwrap();

    let paths = resolver.paths.lock().unwrap().clone();
    for path in [
        fixture.table_path().as_str(),
        &format!("{}out/g.parquet", output.url().as_str()),
        metrics.path(),
    ] {
        assert!(
            paths.iter().any(|asked| asked == path),
            "{path} in {paths:?}"
        );
    }
}
