//! The execution side of every pipeline: builds the runtimes and the session,
//! runs a plan builder against it, and writes the resulting DataFrame as vortex.

use crate::{cpu_runtime::CpuRuntime, write};

use datafusion::{
    common::runtime::JoinSet,
    error::{DataFusionError, Result},
    execution::object_store::ObjectStoreUrl,
    object_store::{client::SpawnedReqwestConnector, gcp::GoogleCloudStorageBuilder},
    prelude::*,
};
use tokio::runtime::Handle;
use vortex_datafusion::{VortexFormatFactory, VortexTableOptions};

use std::{future::Future, sync::Arc, thread::available_parallelism};

/// A plan builder: an async function from a session to a DataFrame, and nothing
/// else — no runtime setup, no object store registration, no writing.
pub trait PlanBuilder: Send + 'static {
    fn build(self, ctx: SessionContext) -> impl Future<Output = Result<DataFrame>> + Send + 'static;
}

impl<F, Fut> PlanBuilder for F
where
    F: FnOnce(SessionContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<DataFrame>> + Send + 'static,
{
    fn build(self, ctx: SessionContext) -> impl Future<Output = Result<DataFrame>> + Send + 'static {
        self(ctx)
    }
}

/// Everything a pipeline needs besides the plan itself.
pub struct PipelineOptions {
    /// Number of worker threads for each runtime. 1 runs on current-thread
    /// runtimes, for timing single-threaded performance.
    pub threads: usize,
    pub session_config: SessionConfig,
    /// Base URLs of the object stores to register on the session, e.g.
    /// "gs://my-bucket". Only gs:// URLs are supported.
    pub object_stores: Vec<String>,
    /// Vortex writer options for the output file. None uses the writer defaults.
    pub writer_options: Option<VortexTableOptions>,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            threads: available_parallelism().map(|n| n.get()).unwrap_or(1),
            session_config: SessionConfig::new(),
            object_stores: Vec::new(),
            writer_options: None,
        }
    }
}

/// Runs `plan_builder` to completion and writes its DataFrame to `output_path`.
///
/// Owns both runtimes: an IO runtime for object store requests, and a separate
/// CPU runtime the plan executes on, so that IO and CPU-bound work don't
/// contend for the same threads.
pub fn run(
    plan_builder: impl PlanBuilder,
    output_path: &str,
    options: PipelineOptions,
) -> Result<()> {
    let io_runtime = runtime_builder(options.threads).enable_all().build()?;
    let cpu_runtime = CpuRuntime::try_new(options.threads)?;

    let ctx = SessionContext::new_with_config(options.session_config);
    for base_url in &options.object_stores {
        register_object_store(&ctx, base_url, io_runtime.handle())?;
    }

    let output_path = output_path.to_string();
    let writer_options = options.writer_options;
    let pipeline_task = async move {
        let df = plan_builder.build(ctx).await?;
        let format = if let Some(vortex_opts) = writer_options {
            Arc::new(VortexFormatFactory::new().with_options(vortex_opts))
        } else {
            Arc::new(VortexFormatFactory::new())
        };
        write(df, &output_path, format).await?;
        Ok(()) as Result<()>
    };

    io_runtime.block_on(async {
        let mut join_set = JoinSet::new();
        join_set.spawn_on(pipeline_task, cpu_runtime.handle());
        match join_set.join_next().await {
            Some(result) => result.map_err(|e| DataFusionError::External(Box::new(e)))?,
            None => Ok(()),
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

/// A runtime builder for `threads` worker threads: a current-thread runtime
/// when 1, so both the IO and CPU runtimes mean the same thing by
/// "single-threaded".
pub(crate) fn runtime_builder(threads: usize) -> tokio::runtime::Builder {
    if threads == 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(threads);
        builder
    }
}
