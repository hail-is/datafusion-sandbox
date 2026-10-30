#![expect(
    clippy::as_conversions,
    reason = "the cast supplies the error branch's otherwise unconstrained result type"
)]

use crate::format::OutputFormat;
use crate::pipeline::{self, PipelineOptions};
use crate::testing::{fixture::block_on, generated::make_range_table, memory_store::MemoryStore};
use crate::write::WriteTarget;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{DataFrame, ParquetReadOptions, col, lit};
use object_store::{ObjectStoreExt, path::Path};
use std::{num::NonZeroUsize, sync::Arc};
use tokio_util::sync::CancellationToken;

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

/// The pipeline entry point is synchronous and runs a pipeline to completion, on a CPU runtime
/// with several workers.
#[test]
fn runs_a_pipeline_that_writes_output() {
    let (workers, rows_written, locations) =
        run_a_write(PipelineOptions::new(NonZeroUsize::new(4).unwrap()));

    assert_eq!(workers, 4);
    assert_eq!(rows_written, 1000);
    assert_eq!(locations, [Path::from("out.vortex")]);
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
    let store = MemoryStore::new("pipeline");
    let target = vortex_target(&store);

    let err = pipeline::run(
        move |ctx| async move {
            store.register(&ctx);
            let df = make_range_table(&ctx, 1000, 128)?
                .select(vec![(lit(1) / (col("idx") - lit(1))).alias("boom")])?;
            target.write_unordered(df).await
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

/// Every distinct `gs://` bucket among the paths gets a store on the session, so a run reading one
/// bucket and writing another can reach both. Building a store contacts no server, so nothing here
/// needs network access or credentials.
///
/// This test touches the filesystem, which is unavoidable: the pipeline hard-wires the Google
/// Cloud Storage builder, and building a store without credentials in the environment looks for
/// the application default credentials file under `$HOME/.config/gcloud`.
#[test]
fn registers_a_store_for_each_gs_bucket_in_the_paths() {
    let options = PipelineOptions::for_paths(
        NonZeroUsize::MIN,
        [
            "gs://input-bucket/samples",
            "gs://output-bucket/combined.vortex",
        ],
    )
    .unwrap();

    pipeline::run(
        |ctx| async move {
            for bucket in ["gs://input-bucket", "gs://output-bucket"] {
                ctx.runtime_env()
                    .object_store(ObjectStoreUrl::parse(bucket)?)?;
            }
            Ok(())
        },
        options,
    )
    .unwrap();
}

/// A store the caller built is registered as it is, so the run reaches the objects already in it.
#[test]
fn registers_a_store_the_caller_built_under_its_url() {
    let store = MemoryStore::new("prebuilt");
    let location = Path::from("data/greeting");
    block_on(store.store().put(&location, "hello".into())).unwrap();
    let options = PipelineOptions::new(NonZeroUsize::MIN)
        .with_object_store(store.url(), Arc::clone(store.store()));

    let read = pipeline::run(
        move |ctx| async move {
            let store = ctx
                .runtime_env()
                .object_store(ObjectStoreUrl::parse("memory://prebuilt")?)?;
            Ok(store.get(&location).await?.bytes().await?)
        },
        options,
    )
    .unwrap();

    assert_eq!(read.as_ref(), b"hello");
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
    let (workers, rows_written, locations) = run_a_write(PipelineOptions::single_threaded());

    assert_eq!(workers, 1);
    assert_eq!(rows_written, 1000);
    assert_eq!(locations, [Path::from("out.vortex")]);
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

/// Writes 1000 rows to `out.vortex` on an in-memory store under `options`, returning the number
/// of workers on the runtime the write ran on, the rows written, and every object the write left
/// in the store.
fn run_a_write(options: PipelineOptions) -> (usize, u64, Vec<Path>) {
    let store = MemoryStore::new("pipeline");
    let target = vortex_target(&store);

    pipeline::run(
        move |ctx| async move {
            store.register(&ctx);
            let df = make_range_table(&ctx, 1000, 128)?;
            let executed = target.write_unordered(df).await?;
            let workers = tokio::runtime::Handle::current().metrics().num_workers();
            Ok((
                workers,
                executed.rows_written,
                store.locations_under("").await,
            ))
        },
        options,
    )
    .unwrap()
}

fn vortex_target(store: &MemoryStore) -> WriteTarget {
    WriteTarget {
        output_path: format!("{}out.vortex", store.url().as_str()),
        output_format: OutputFormat::VORTEX,
    }
}

/// A run cancelled while its plan's reads are held at a gate ends with the cancelled error once
/// the gate is released, and runs no further: the copy the plan was making is never written.
#[test]
fn a_run_cancelled_while_its_reads_are_held_ends_cancelled_and_runs_no_further() {
    let store = MemoryStore::new("cancelled");
    let source = format!("{}in.parquet", store.url().as_str());
    let copy = WriteTarget {
        output_path: format!("{}copy.parquet", store.url().as_str()),
        output_format: OutputFormat::PARQUET,
    };
    let written = store.clone();
    let path = source.clone();
    pipeline::run(
        move |ctx| async move {
            written.register(&ctx);
            WriteTarget {
                output_path: path,
                output_format: OutputFormat::PARQUET,
            }
            .write_unordered(make_range_table(&ctx, 1000, 128)?)
            .await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();
    let (gated, gate) = store.with_gated_reads_under("in.parquet");
    let token = CancellationToken::new();
    let options = PipelineOptions::single_threaded().cancelled_by(token.clone());

    let run = std::thread::spawn(move || {
        pipeline::run(
            move |ctx| async move {
                gated.register(&ctx);
                pipeline::until_cancelled(&ctx, async {
                    let rows = ctx
                        .read_parquet(source, ParquetReadOptions::default())
                        .await?;
                    copy.write_unordered(rows).await
                })
                .await
            },
            options,
        )
    });
    block_on(gate.reached());
    token.cancel();
    gate.release();

    let error = run.join().unwrap().unwrap_err();
    assert!(pipeline::is_cancelled(&error), "{error}");
    assert_eq!(error.to_string(), "External error: the run was cancelled");
    assert_eq!(
        block_on(store.locations_under("")),
        [Path::from("in.parquet")]
    );
}

#[test]
fn a_run_cancelled_before_it_starts_ends_cancelled_without_running_its_pipeline() {
    let token = CancellationToken::new();
    token.cancel();

    let result = pipeline::run(
        |_ctx| async move {
            panic!("the pipeline of a run cancelled before it started ran");
            #[expect(unreachable_code, reason = "fixes the pipeline's result type")]
            Ok::<_, DataFusionError>(())
        },
        PipelineOptions::single_threaded().cancelled_by(token),
    );

    let error = result.unwrap_err();
    assert!(pipeline::is_cancelled(&error), "{error}");
}

/// Work a run's pipeline does until cancelled hands back its own result, success or failure, when
/// the run is not cancelled, as it does on a session the runner did not build, which has no
/// token to be cancelled by.
#[test]
fn until_cancelled_hands_back_the_work_s_own_result_when_the_run_is_not_cancelled() {
    let options = PipelineOptions::single_threaded().cancelled_by(CancellationToken::new());

    let (succeeded, failed) = pipeline::run(
        |ctx| async move {
            let succeeded = pipeline::until_cancelled(&ctx, async { Ok(42_u64) }).await;
            let failed: Result<u64, _> = pipeline::until_cancelled(&ctx, async {
                Err(DataFusionError::Execution("the work failed".to_string()))
            })
            .await;
            Ok::<_, DataFusionError>((succeeded, failed))
        },
        options,
    )
    .unwrap();
    let unbuilt = block_on(pipeline::until_cancelled(
        &datafusion::prelude::SessionContext::new(),
        async { Ok(7_u64) },
    ));

    assert_eq!(succeeded.unwrap(), 42);
    let error = failed.unwrap_err();
    assert!(!pipeline::is_cancelled(&error), "{error}");
    assert_eq!(unbuilt.unwrap(), 7);
}

/// A pipeline that stops its work on cancellation can go on to do more before it ends, such as
/// recording the run, and the run then ends with whatever the pipeline returns.
#[test]
fn a_pipeline_goes_on_after_its_work_is_cancelled_and_the_run_returns_what_it_returns() {
    let token = CancellationToken::new();
    let options = PipelineOptions::single_threaded().cancelled_by(token.clone());

    let (stopped, after) = pipeline::run(
        move |ctx| async move {
            let stopped = pipeline::until_cancelled(&ctx, async {
                token.cancel();
                std::future::pending::<datafusion::error::Result<()>>().await
            })
            .await;
            let after = pipeline::until_cancelled(&ctx, async { Ok(()) }).await;
            Ok::<_, DataFusionError>((stopped, after))
        },
        options,
    )
    .unwrap();

    assert!(pipeline::is_cancelled(&stopped.unwrap_err()));
    // Once the run is cancelled, later work is not started.
    assert!(pipeline::is_cancelled(&after.unwrap_err()));
}

#[test]
fn is_cancelled_recognises_the_cancelled_error_beneath_context() {
    let token = CancellationToken::new();
    token.cancel();
    let cancelled = pipeline::run(
        |_ctx| async move { Ok::<_, DataFusionError>(()) },
        PipelineOptions::single_threaded().cancelled_by(token),
    )
    .unwrap_err();

    let wrapped = cancelled.context("while writing the copy");

    assert!(pipeline::is_cancelled(&wrapped), "{wrapped}");
    assert!(!pipeline::is_cancelled(&DataFusionError::Execution(
        "the run was cancelled".to_string()
    )));
}
