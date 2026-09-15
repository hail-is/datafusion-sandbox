#![expect(
    clippy::as_conversions,
    reason = "the cast supplies the error branch's otherwise unconstrained result type"
)]

use crate::format::{OutputFormat, OutputLayout};
use crate::generated::make_range_table;
use crate::pipeline::{self, PipelineOptions};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::prelude::{DataFrame, col, lit};

#[test]
fn returns_the_pipeline_result_to_the_calling_thread() {
    let result = pipeline::run(
        |_ctx| async move { Ok::<_, DataFusionError>(42_u64) },
        PipelineOptions::default(),
    )
    .unwrap();

    assert_eq!(result, 42);
}

#[test]
fn returns_explain_output_without_writing() {
    let explained_plan: String = pipeline::run(
        |ctx| async move {
            let df = make_range_table(&ctx, 1000, 128)?;
            let batches = df.explain(false, false)?.collect().await?;
            Ok::<_, DataFusionError>(pretty_format_batches(&batches)?.to_string())
        },
        PipelineOptions::default(),
    )
    .unwrap();

    assert!(
        explained_plan.contains("StreamingTableExec"),
        "got:\n{explained_plan}"
    );
}

#[test]
fn invokes_the_pipeline_on_the_cpu_runtime() {
    let has_runtime = pipeline::run(
        |_ctx| {
            let has_runtime = tokio::runtime::Handle::try_current().is_ok();
            async move { Ok::<_, DataFusionError>(has_runtime) }
        },
        PipelineOptions::default(),
    )
    .unwrap();

    assert!(has_runtime);
}

/// The pipeline entry point is synchronous and runs a pipeline to completion.
#[test]
fn runs_a_pipeline_that_writes_output() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.vortex");
    let output_path = output.to_str().unwrap().to_string();

    let rows_written = pipeline::run(
        move |ctx| async move {
            let df = make_range_table(&ctx, 1000, 128)?;
            OutputFormat::VORTEX
                .write(df, &output_path, None, OutputLayout::SingleFile)
                .await
        },
        PipelineOptions::default(),
    )
    .unwrap();

    assert_eq!(rows_written, 1000);
    assert!(output.exists());
    assert!(output.metadata().unwrap().len() > 0);
}

#[test]
fn runs_a_pipeline_that_writes_parquet_output() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.parquet");
    let output_path = output.to_str().unwrap().to_string();

    pipeline::run(
        move |ctx| async move {
            let df = make_range_table(&ctx, 1000, 128)?;
            OutputFormat::PARQUET
                .write(df, &output_path, None, OutputLayout::SingleFile)
                .await
        },
        PipelineOptions::default(),
    )
    .unwrap();

    assert!(output.exists());
    assert!(output.metadata().unwrap().len() > 0);
}

/// A plan builder that fails while building surfaces its error to the caller
/// rather than printing and discarding it — that's what lets the CLI exit
/// non-zero.
#[test]
fn surfaces_plan_builder_errors() {
    let err = pipeline::run(
        |_ctx| async move { Err(DataFusionError::Plan("boom".to_string())) as Result<DataFrame, _> },
        PipelineOptions::default(),
    )
    .unwrap_err();

    assert!(err.to_string().contains("boom"), "got: {err}");
}

/// A plan that builds perfectly well and only fails once it runs still surfaces
/// its error. Dividing by `idx - 1` over a range starting at 1 divides by zero
/// on the first row, which nothing can catch before execution.
#[test]
fn surfaces_errors_from_plans_that_fail_at_execution() {
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("out.vortex").to_str().unwrap().to_string();

    let err = pipeline::run(
        move |ctx| async move {
            let df = make_range_table(&ctx, 1000, 128)?
                .select(vec![(lit(1) / (col("idx") - lit(1))).alias("boom")])?;
            OutputFormat::VORTEX
                .write(df, &output_path, None, OutputLayout::SingleFile)
                .await
        },
        PipelineOptions::default(),
    )
    .unwrap_err();

    assert!(err.to_string().contains("Divide by zero"), "got: {err}");
}

/// Object stores are registered from a declared list of base URLs; any scheme
/// other than gs:// is a clear error.
#[test]
fn rejects_object_store_urls_with_unsupported_schemes() {
    let err = pipeline::run(
        move |ctx| async move { make_range_table(&ctx, 10, 8) },
        PipelineOptions {
            object_stores: vec!["s3://some-bucket".to_string()],
            ..Default::default()
        },
    )
    .unwrap_err();

    let message = err.to_string();
    assert!(message.contains("s3"), "got: {message}");
    assert!(message.contains("gs://"), "got: {message}");
}

/// Thread count defaults to available parallelism.
#[test]
fn default_thread_count_is_available_parallelism() {
    assert_eq!(
        PipelineOptions::default().threads,
        std::thread::available_parallelism().unwrap().get(),
    );
}

/// A one-thread pipeline still executes on a multi-thread runtime, because `DataFusion`'s
/// `spawn_buffered` only buffers when it finds `RuntimeFlavor::MultiThread`. See
/// `docs/adr/0001-always-use-multi-thread-tokio-runtimes.md`.
#[test]
fn one_thread_still_executes_on_a_multi_thread_runtime() {
    let flavor = pipeline::run(
        |_ctx| async move {
            Ok::<_, DataFusionError>(tokio::runtime::Handle::current().runtime_flavor())
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(flavor, tokio::runtime::RuntimeFlavor::MultiThread);
}

/// One thread runs a pipeline end to end, including the write.
#[test]
fn runs_with_one_thread() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.vortex");
    let output_path = output.to_str().unwrap().to_string();

    pipeline::run(
        move |ctx| async move {
            let df = make_range_table(&ctx, 1000, 128)?;
            OutputFormat::VORTEX
                .write(df, &output_path, None, OutputLayout::SingleFile)
                .await
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap();

    assert!(output.exists());
}
