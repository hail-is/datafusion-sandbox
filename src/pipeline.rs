//! The execution side of every pipeline: builds the runtimes and the session,
//! then runs a pipeline closure to completion.

use crate::cpu_runtime::CpuRuntime;

use datafusion::{
    common::runtime::JoinSet,
    error::{DataFusionError, Result},
    execution::object_store::ObjectStoreUrl,
    object_store::{client::SpawnedReqwestConnector, gcp::GoogleCloudStorageBuilder},
    prelude::SessionContext,
};
use tokio::runtime::Handle;

use std::{future::Future, sync::Arc, thread::available_parallelism};

/// Everything the runner needs besides the pipeline closure itself.
pub struct PipelineOptions {
    /// Number of worker threads for each runtime. Independent of `target_partitions`, which
    /// each formulation settles for itself.
    pub threads: usize,
    /// Base URLs of the object stores to register on the session, e.g.
    /// "gs://my-bucket". Only gs:// URLs are supported.
    pub object_stores: Vec<String>,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            threads: available_parallelism().map(|n| n.get()).unwrap_or(1),
            object_stores: Vec::new(),
        }
    }
}

/// Runs `pipeline` to completion and returns its result to the calling thread.
///
/// Owns both runtimes: an IO runtime for object store requests, and a separate
/// CPU runtime the plan executes on, so that IO and CPU-bound work don't
/// contend for the same threads.
///
/// Every DataFusion plan in this crate runs through here, test fixtures
/// included. See docs/adr/0006-run-every-plan-through-the-pipeline-runner.md.
pub fn run<T, Fut>(
    pipeline: impl FnOnce(SessionContext) -> Fut + Send + 'static,
    options: PipelineOptions,
) -> Result<T>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    // Multi-thread even at one worker: TLS and HTTP framing for every object store request runs
    // here, so throughput has to scale with the thread count. See
    // docs/adr/0001-always-use-multi-thread-tokio-runtimes.md.
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(options.threads)
        .enable_all()
        .build()?;
    let cpu_runtime = CpuRuntime::try_new(options.threads)?;

    let ctx = SessionContext::new();
    for base_url in &options.object_stores {
        register_object_store(&ctx, base_url, io_runtime.handle())?;
    }

    io_runtime.block_on(async {
        let mut join_set = JoinSet::new();
        join_set.spawn_on(async move { pipeline(ctx).await }, cpu_runtime.handle());
        match join_set.join_next().await {
            Some(result) => result.map_err(|e| DataFusionError::External(Box::new(e)))?,
            None => unreachable!("the pipeline join set always contains one task"),
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
