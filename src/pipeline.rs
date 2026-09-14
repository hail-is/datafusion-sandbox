//! The execution side of every pipeline: builds the runtimes and the session,
//! then runs a pipeline closure to completion.

use crate::cpu_runtime::CpuRuntime;

use datafusion::{
    common::runtime::JoinSet,
    error::{DataFusionError, Result},
    execution::object_store::ObjectStoreUrl,
    object_store::{client::SpawnedReqwestConnector, gcp::GoogleCloudStorageBuilder},
    prelude::{SessionConfig, SessionContext},
};
use tokio::runtime::Handle;

use std::{future::Future, num::NonZeroUsize, sync::Arc, thread::available_parallelism};

/// Everything the runner needs besides the pipeline closure itself.
pub struct PipelineOptions {
    /// Number of worker threads for each runtime. Independent of the session's target-partition
    /// setting.
    pub threads: usize,
    /// Base URLs of the object stores to register on the session, such as
    /// `gs://my-bucket`. Only `gs://` URLs are supported.
    pub object_stores: Vec<String>,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            threads: available_parallelism().map_or(1, NonZeroUsize::get),
            object_stores: Vec::new(),
        }
    }
}

/// The shared `DataFusion` configuration used by every pipeline.
#[must_use]
pub fn session_config() -> SessionConfig {
    let mut config = SessionConfig::new();
    let options = config.options_mut();
    options.optimizer.prefer_existing_sort = true;
    // Parquet reports a pushed filter exact only when it filters at decode time, and only an
    // exact filter leaves a filtered Parquet plan with the shape of a filtered Vortex plan.
    // Filter reordering stays at its default. See ADR 0003.
    options.execution.parquet.pushdown_filters = true;
    config
}

/// Runs `pipeline` to completion and returns its result to the calling thread.
///
/// Owns both runtimes: an IO runtime for object store requests, and a separate
/// CPU runtime the plan executes on, so that IO and CPU-bound work don't
/// contend for the same threads.
///
/// Every `DataFusion` plan in this crate runs through here, test fixtures
/// included. See `docs/adr/0006-run-every-plan-through-the-pipeline-runner.md`.
///
/// # Errors
///
/// Returns an error if a runtime or object store cannot be created, the pipeline fails, or its
/// task cannot be joined.
pub fn run<T, Fut>(
    pipeline: impl FnOnce(SessionContext) -> Fut + Send + 'static,
    options: PipelineOptions,
) -> Result<T>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    let PipelineOptions {
        threads,
        object_stores,
    } = options;

    // Multi-thread even at one worker: TLS and HTTP framing for every object store request runs
    // here, so throughput has to scale with the thread count. See
    // docs/adr/0001-always-use-multi-thread-tokio-runtimes.md.
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;
    let cpu_runtime = CpuRuntime::try_new(threads)?;

    let ctx = SessionContext::new_with_config(session_config());
    for base_url in &object_stores {
        register_object_store(&ctx, base_url, io_runtime.handle())?;
    }

    io_runtime.block_on(async {
        let mut join_set = JoinSet::new();
        join_set.spawn_on(async move { pipeline(ctx).await }, cpu_runtime.handle());
        match join_set.join_next().await {
            Some(result) => result.map_err(|e| DataFusionError::External(Box::new(e)))?,
            None => Err(DataFusionError::Internal(
                "pipeline task was missing from its join set".to_string(),
            )),
        }
    })
}

fn register_object_store(ctx: &SessionContext, base_url: &str, io_handle: &Handle) -> Result<()> {
    if base_url.split_once("://").map(|(scheme, _)| scheme) != Some("gs") {
        return Err(DataFusionError::Configuration(format!(
            "cannot register object store '{base_url}': only gs:// URLs are supported"
        )));
    }
    let url = ObjectStoreUrl::parse(base_url)?;
    let store = GoogleCloudStorageBuilder::from_env()
        .with_url(base_url)
        // Run the HTTP requests on the IO runtime. Without this line,
        // you will see an error such as:
        // A Tokio 1.x context was found, but IO is disabled.
        .with_http_connector(SpawnedReqwestConnector::new(io_handle.clone()))
        .build()?;
    ctx.register_object_store(url.as_ref(), Arc::new(store));
    Ok(())
}
