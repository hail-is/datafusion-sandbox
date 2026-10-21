//! Reading back one file a test wrote, and the tables a run recorded under a metrics directory.

use crate::{
    format::{InputFormat, OutputFormat},
    metrics_directory::{self, MetricsDirectory},
    pipeline::{self, PipelineOptions},
    testing::{
        columns::string_values,
        fixture::{self, RecordedRun},
        generated::make_range_table,
        memory_store::MemoryStore,
        run_records::run_record,
    },
    throughput_probe::ProgressSample,
    write::WriteTarget,
};

use datafusion::{
    arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    error::Result,
    prelude::SessionContext,
};
use std::{future::Future, sync::Arc};

/// A batch of two rows: a non-nullable `n` and a nullable `label`.
fn written_batch() -> RecordBatch {
    let n: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let label: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
    RecordBatch::try_from_iter_with_nullable([("n", n, false), ("label", label, true)]).unwrap()
}

/// Runs `pipeline` in a session with `store` registered.
fn on<T, Fut>(
    store: &MemoryStore,
    pipeline: impl FnOnce(SessionContext) -> Fut + Send + 'static,
) -> Result<T>
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
}

/// Writes [`written_batch`] as one file at `path`, in `format`, through `ctx`.
async fn write(ctx: &SessionContext, path: &str, format: OutputFormat) -> Result<()> {
    WriteTarget {
        output_path: path.to_string(),
        output_format: format,
    }
    .write_unordered(ctx.read_batch(written_batch())?)
    .await?;
    Ok(())
}

const FORMATS: [(OutputFormat, InputFormat); 2] = [
    (OutputFormat::PARQUET, InputFormat::PARQUET),
    (OutputFormat::VORTEX, InputFormat::VORTEX),
];

fn i64_values(batch: &RecordBatch, name: &str) -> Vec<i64> {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn read_file_reads_back_the_one_file_written_at_a_path_inferring_its_schema() {
    for (output_format, input_format) in FORMATS {
        let store = MemoryStore::new("read-file");
        let extension = output_format.extension();
        let path = format!("{}out/table.{extension}", store.url().as_str());

        let batches = on(&store, move |ctx| async move {
            write(&ctx, &path, output_format).await?;
            fixture::read_file(&ctx, &path, &input_format, None).await
        })
        .unwrap();

        let context = extension;
        let [batch] = batches.as_slice() else {
            panic!("{context}: expected one batch, got {batches:?}");
        };
        assert_eq!(i64_values(batch, "n"), [1, 2], "{context}");
        assert_eq!(string_values(batch, "label"), ["a", ""], "{context}");
    }
}

#[test]
fn read_file_reads_with_the_schema_given() {
    for (output_format, input_format) in FORMATS {
        let store = MemoryStore::new("read-file-schema");
        let extension = output_format.extension();
        let path = format!("{}out/table.{extension}", store.url().as_str());
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let given = Arc::clone(&schema);

        let batches = on(&store, move |ctx| async move {
            write(&ctx, &path, output_format).await?;
            fixture::read_file(&ctx, &path, &input_format, Some(given)).await
        })
        .unwrap();

        let context = extension;
        let [batch] = batches.as_slice() else {
            panic!("{context}: expected one batch, got {batches:?}");
        };
        assert_eq!(batch.schema(), schema, "{context}");
        assert_eq!(i64_values(batch, "n"), [1, 2], "{context}");
    }
}

#[test]
fn read_file_fails_on_a_store_the_session_does_not_serve() {
    let error = pipeline::run(
        |ctx| async move {
            Ok(fixture::read_file(
                &ctx,
                "memory://unregistered/out/table.parquet",
                &InputFormat::PARQUET,
                None,
            )
            .await
            .unwrap_err())
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    assert!(
        error.to_string().contains("memory://unregistered"),
        "{error}"
    );
}

/// The metrics directory the run tests record under, on `store`.
fn directory(store: &MemoryStore) -> MetricsDirectory {
    MetricsDirectory::new(&format!("{}benchmarks", store.url().as_str()))
}

/// Records `run-a` under the store's metrics directory, as a throughput probe that took
/// `samples` if given, otherwise as a measured write, then reads back `run-a` and `run-b`.
fn recorded(samples: Option<Vec<ProgressSample>>) -> (RecordedRun, RecordedRun) {
    let store = MemoryStore::new("recorded-runs");
    let directory = directory(&store);
    on(&store, move |ctx| async move {
        let plan = make_range_table(&ctx, 10, 4)?
            .create_physical_plan()
            .await?;
        datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        let run = directory.unrecorded(&ctx, "run-a").await?;
        match samples {
            Some(samples) => {
                run.record_probe(&ctx, &run_record("run-a"), &plan, &samples)
                    .await?
            }
            None => run.record(&ctx, &run_record("run-a"), &plan).await?,
        };
        Ok((
            fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
            fixture::read_recorded_run(&ctx, &directory, "run-b").await?,
        ))
    })
    .unwrap()
}

#[test]
fn read_recorded_run_reads_each_table_a_probe_recorded_as_one_batch() {
    let samples = vec![
        ProgressSample {
            elapsed_ns: 0,
            rows: 0,
        },
        ProgressSample {
            elapsed_ns: 100,
            rows: 4,
        },
        ProgressSample {
            elapsed_ns: 200,
            rows: 10,
        },
    ];

    let (run_a, _) = recorded(Some(samples));

    let record = run_a.record.unwrap();
    assert_eq!(string_values(&record, "run_id"), ["run-a"]);
    let metrics = run_a.metrics.unwrap();
    assert!(metrics.num_rows() > 0);
    assert!(
        string_values(&metrics, "run_id")
            .iter()
            .all(|run_id| run_id == "run-a")
    );
    let progress_samples = run_a.progress_samples.unwrap();
    assert_eq!(progress_samples.num_rows(), 3);
    assert_eq!(record.schema(), metrics_directory::run_record_schema());
    assert_eq!(metrics.schema(), metrics_directory::run_metrics_schema());
    assert_eq!(
        progress_samples.schema(),
        metrics_directory::progress_samples_schema()
    );
}

#[test]
fn read_recorded_run_has_no_progress_samples_for_a_measured_write_and_nothing_for_another_run() {
    let (run_a, run_b) = recorded(None);

    assert!(run_a.record.is_some() && run_a.metrics.is_some());
    assert!(run_a.progress_samples.is_none());
    assert!(run_b.record.is_none() && run_b.metrics.is_none() && run_b.progress_samples.is_none());
}

#[test]
fn read_recorded_run_fails_on_a_directory_the_session_does_not_serve() {
    let directory = directory(&MemoryStore::new("unregistered-runs"));

    let failed = pipeline::run(
        move |ctx| async move {
            Ok(fixture::read_recorded_run(&ctx, &directory, "run-a")
                .await
                .is_err())
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    assert!(failed);
}
