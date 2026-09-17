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

/// Everything the runner needs besides the pipeline closure itself: the thread count of each
/// runtime and the object stores the session registers before the pipeline runs.
#[derive(Debug)]
pub struct PipelineOptions {
    /// Number of worker threads for each runtime. Independent of the session's target-partition
    /// setting.
    threads: NonZeroUsize,
    /// The object stores to register on the session, by base URL such as `gs://my-bucket`.
    object_stores: Vec<ObjectStoreUrl>,
}

impl PipelineOptions {
    /// Options for a pipeline that touches no object store beyond the session's built-in local
    /// one.
    #[must_use]
    pub const fn new(threads: NonZeroUsize) -> Self {
        Self {
            threads,
            object_stores: Vec::new(),
        }
    }

    /// Options for a one-thread pipeline that touches no object store: what a test or fixture
    /// that only needs a plan executed asks for.
    #[must_use]
    pub const fn single_threaded() -> Self {
        Self::new(NonZeroUsize::MIN)
    }

    /// Options for a pipeline that reads and writes `paths`. A `gs://` path registers its
    /// bucket's store; a bare local path or a `file://` path needs no store.
    ///
    /// # Errors
    ///
    /// Returns a configuration error naming the first path on a store the pipeline cannot serve.
    pub fn for_paths<'a>(
        threads: NonZeroUsize,
        paths: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self> {
        let mut object_stores = Vec::new();
        for path in paths {
            if let Some(url) = object_store_url(path)?
                && !object_stores.contains(&url)
            {
                object_stores.push(url);
            }
        }
        Ok(Self {
            threads,
            object_stores,
        })
    }
}

/// The thread count a pipeline runs with when its caller states none: available parallelism.
#[must_use]
pub fn default_thread_count() -> NonZeroUsize {
    available_parallelism().unwrap_or(NonZeroUsize::MIN)
}

/// The object store `path` lives on, or `None` when the session's built-in local store serves
/// it.
fn object_store_url(path: &str) -> Result<Option<ObjectStoreUrl>> {
    let Some((scheme, rest)) = path.split_once("://") else {
        return Ok(None);
    };
    match scheme {
        "file" => Ok(None),
        "gs" => match rest.split('/').next() {
            Some(bucket) if !bucket.is_empty() => {
                ObjectStoreUrl::parse(format!("gs://{bucket}")).map(Some)
            }
            _ => Err(DataFusionError::Configuration(format!(
                "cannot read or write '{path}': a gs:// URL must name a bucket"
            ))),
        },
        _ => Err(DataFusionError::Configuration(format!(
            "cannot read or write '{path}': only gs:// URLs and local paths are supported"
        ))),
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
        .worker_threads(threads.get())
        .enable_all()
        .build()?;
    let cpu_runtime = CpuRuntime::try_new(threads.get())?;

    let ctx = SessionContext::new_with_config(session_config());
    for url in &object_stores {
        register_object_store(&ctx, url, io_runtime.handle())?;
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

/// Registers the Google Cloud Storage bucket at `url`.
fn register_object_store(
    ctx: &SessionContext,
    url: &ObjectStoreUrl,
    io_handle: &Handle,
) -> Result<()> {
    let store = GoogleCloudStorageBuilder::from_env()
        .with_url(url.as_str())
        // Run the HTTP requests on the IO runtime. Without this line,
        // you will see an error such as:
        // A Tokio 1.x context was found, but IO is disabled.
        .with_http_connector(SpawnedReqwestConnector::new(io_handle.clone()))
        .build()?;
    ctx.register_object_store(url.as_ref(), Arc::new(store));
    Ok(())
}
