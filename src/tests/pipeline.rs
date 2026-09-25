#![expect(
    clippy::as_conversions,
    reason = "the cast supplies the error branch's otherwise unconstrained result type"
)]

use crate::format::OutputFormat;
use crate::generated::make_range_table;
use crate::pipeline::{self, PipelineOptions};
use crate::write::WriteTarget;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::prelude::{DataFrame, col, lit};
use std::num::NonZeroUsize;

#[test]
fn returns_the_pipeline_result_to_the_calling_thread() {
    let result = pipeline::run(
        |_ctx| async move { Ok::<_, DataFusionError>(42_u64) },
        PipelineOptions::single_threaded(),
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
        PipelineOptions::single_threaded(),
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
        PipelineOptions::single_threaded(),
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
            let executed = WriteTarget {
                output_path,
                output_format: OutputFormat::VORTEX,
            }
            .write_unordered(df)
            .await?;
            Ok(executed.rows_written)
        },
        PipelineOptions::single_threaded(),
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
            WriteTarget {
                output_path,
                output_format: OutputFormat::PARQUET,
            }
            .write_unordered(df)
            .await
        },
        PipelineOptions::single_threaded(),
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
        PipelineOptions::single_threaded(),
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
            WriteTarget {
                output_path,
                output_format: OutputFormat::VORTEX,
            }
            .write_unordered(df)
            .await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap_err();

    assert!(err.to_string().contains("Divide by zero"), "got: {err}");
}

/// Local paths, bare or under `file://`, need no object store, so options built from them run
/// on the session's built-in local store. Nothing here reads the paths.
#[test]
fn builds_options_from_local_paths_without_registering_a_store() {
    let options = PipelineOptions::for_paths(
        NonZeroUsize::MIN,
        ["data/samples", "file:///data/out.vortex"],
    )
    .unwrap();

    let result = pipeline::run(
        |_ctx| async move { Ok::<_, DataFusionError>(42_u64) },
        options,
    )
    .unwrap();

    assert_eq!(result, 42);
}

/// A path on a store the pipeline cannot serve is rejected when the options are built, before
/// any runtime exists, and the error names the path the caller gave. Every path is inspected,
/// whichever position it holds.
#[test]
fn rejects_paths_with_unsupported_schemes_when_options_are_built() {
    for paths in [
        ["data/samples", "s3://some-bucket/out.vortex"],
        ["s3://some-bucket/out.vortex", "data/samples"],
    ] {
        let err = PipelineOptions::for_paths(NonZeroUsize::MIN, paths).unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("s3://some-bucket/out.vortex"),
            "{paths:?}: {message}"
        );
        assert!(message.contains("gs://"), "{paths:?}: {message}");
    }
}

/// A `gs://` path without a bucket has no store to register, so it is rejected when the options
/// are built rather than when the runtime tries to register it.
#[test]
fn rejects_a_gs_path_that_names_no_bucket_when_options_are_built() {
    for path in ["gs://", "gs:///data/samples"] {
        let err = PipelineOptions::for_paths(NonZeroUsize::MIN, [path]).unwrap_err();

        let message = err.to_string();
        assert!(message.contains(path), "{path}: {message}");
        assert!(message.contains("bucket"), "{path}: {message}");
    }
}

/// Thread count defaults to available parallelism.
#[test]
fn default_thread_count_is_available_parallelism() {
    assert_eq!(
        pipeline::default_thread_count(),
        std::thread::available_parallelism().unwrap(),
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
        PipelineOptions::single_threaded(),
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
            WriteTarget {
                output_path,
                output_format: OutputFormat::VORTEX,
            }
            .write_unordered(df)
            .await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    assert!(output.exists());
}

/// The pipeline reaches the IO runtime through its session, and a task spawned there runs on
/// the IO runtime's threads rather than the CPU runtime's the pipeline runs on.
#[test]
fn gives_the_pipeline_the_io_runtime() {
    let (pipeline_thread, io_thread) = pipeline::run(
        |ctx| async move {
            let io = pipeline::io_runtime(ctx.state().config())?;
            let io_thread = io
                .spawn(async { std::thread::current().id() })
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok::<_, DataFusionError>((std::thread::current().id(), io_thread))
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    assert_ne!(pipeline_thread, io_thread);
}
