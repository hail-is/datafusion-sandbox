use datafusion::error::DataFusionError;
use datafusion::prelude::{DataFrame, col, lit};
use datafusion_sandbox::make_range_table;
use datafusion_sandbox::pipeline::{self, PipelineOptions};

/// The pipeline entry point is synchronous, runs a plan builder to completion,
/// and writes its output as vortex.
#[test]
fn runs_a_plan_builder_and_writes_output() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.vortex");
    let output_path = output.to_str().unwrap().to_string();

    pipeline::run(
        move |ctx| async move { make_range_table(&ctx, 1000, 128) },
        &output_path,
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
        "/tmp/should-never-be-written.vortex",
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
            make_range_table(&ctx, 1000, 128)?
                .select(vec![(lit(1) / (col("idx") - lit(1))).alias("boom")])
        },
        &output_path,
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
        "/tmp/should-never-be-written.vortex",
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

/// Single-threaded mode runs the whole pipeline on current-thread runtimes.
#[test]
fn runs_single_threaded() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.vortex");
    let output_path = output.to_str().unwrap().to_string();

    pipeline::run(
        move |ctx| async move { make_range_table(&ctx, 1000, 128) },
        &output_path,
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap();

    assert!(output.exists());
}
