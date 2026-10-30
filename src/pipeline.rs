//! The execution side of every pipeline: builds the runtimes and the session,
//! then runs a pipeline closure to completion.
//!
//! The pipeline runs on the CPU runtime. It reaches the IO runtime through its session, with
//! [`io_runtime`], to spawn work that must not wait behind plan execution.
//!
//! The CPU runtime lives on its own thread, in `cpu_runtime`, so the runner can drop it from
//! async code.
//!
//! A run is cancelled through the token its options carry, and cancellation is cooperative: the
//! pipeline stops its work with [`until_cancelled`], which drops the work, and with it any plan
//! the work was executing, and fails with the cancelled error [`is_cancelled`] recognises. The
//! pipeline may then do more before it ends, such as recording the run, and the run ends with
//! what the pipeline returns. Work outside [`until_cancelled`] runs to completion.

mod cpu_runtime;

#[cfg(test)]
mod tests;

use cpu_runtime::CpuRuntime;

use datafusion::{
    common::runtime::JoinSet,
    error::{DataFusionError, Result},
    execution::object_store::ObjectStoreUrl,
    object_store::{client::SpawnedReqwestConnector, gcp::GoogleCloudStorageBuilder},
    prelude::{SessionConfig, SessionContext},
};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use std::{
    error::Error, fmt, future::Future, num::NonZeroUsize, sync::Arc, thread::available_parallelism,
};

/// Everything the runner needs besides the pipeline closure itself.
///
/// That is the thread count of each runtime, the object stores the session registers before the
/// pipeline runs, and the token that cancels the run, if any.
#[derive(Debug)]
pub struct PipelineOptions {
    /// Number of worker threads for each runtime. Independent of the session's target-partition
    /// setting.
    threads: NonZeroUsize,
    /// The object stores to register on the session, by base URL such as `gs://my-bucket`.
    object_stores: Vec<ObjectStoreUrl>,
    /// The token that cancels the run; `None` for a run nothing cancels.
    cancellation: Option<CancellationToken>,
}

impl PipelineOptions {
    /// Options for a pipeline that touches no object store beyond the session's built-in local
    /// one.
    #[must_use]
    pub const fn new(threads: NonZeroUsize) -> Self {
        Self {
            threads,
            object_stores: Vec::new(),
            cancellation: None,
        }
    }

    /// These options, for a run that cancelling `token` cancels. See [`until_cancelled`] for
    /// what cancelling does.
    #[must_use]
    pub fn cancelled_by(self, token: CancellationToken) -> Self {
        Self {
            cancellation: Some(token),
            ..self
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
            cancellation: None,
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
/// The session carries the options' cancellation token, through which [`until_cancelled`] stops
/// the pipeline's work. A run cancelled before it starts never calls `pipeline`.
///
/// # Errors
///
/// Returns the cancelled error if the run was cancelled before it started, and otherwise an error
/// if a runtime or object store cannot be created, the pipeline fails, or its task cannot be
/// joined.
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
        cancellation,
    } = options;
    if cancellation
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
    {
        return Err(cancelled());
    }

    // Multi-thread even at one worker: TLS and HTTP framing for every object store request runs
    // here, so throughput has to scale with the thread count. See
    // docs/adr/0001-always-use-multi-thread-tokio-runtimes.md.
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.get())
        .enable_all()
        .build()?;
    let cpu_runtime = CpuRuntime::try_new(threads.get())?;

    let mut config =
        session_config().with_extension(Arc::new(IoRuntime(io_runtime.handle().clone())));
    if let Some(token) = cancellation {
        config = config.with_extension(Arc::new(Cancellation(token)));
    }
    let ctx = SessionContext::new_with_config(config);
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

/// The IO runtime of the runner that built a session, carried in its configuration.
#[derive(Debug)]
struct IoRuntime(Handle);

/// The IO runtime of the session configured by `config`.
///
/// A task spawned on the handle runs on the IO runtime's threads, so a saturated CPU runtime
/// neither delays it nor loses plan time to it.
///
/// # Errors
///
/// Returns an internal error if `config` is not the configuration of a session the pipeline
/// runner built.
pub fn io_runtime(config: &SessionConfig) -> Result<Handle> {
    config
        .get_extension::<IoRuntime>()
        .map(|io_runtime| io_runtime.0.clone())
        .ok_or_else(|| {
            DataFusionError::Internal(
                "the session was not built by the pipeline runner, so it has no IO runtime"
                    .to_string(),
            )
        })
}

/// The token that cancels the run of the runner that built a session, carried in its
/// configuration.
#[derive(Debug)]
struct Cancellation(CancellationToken);

/// Runs `work` until it completes or the run of the pipeline `ctx` belongs to is cancelled,
/// whichever comes first, and hands back its result.
///
/// On cancellation `work` is dropped, which stops any plan it was executing and leaves its
/// unfinished writes unwritten, and the cancelled error is returned. Work started after the run
/// is cancelled is never polled. A session the runner did not build, or built without a token,
/// is never cancelled.
///
/// # Errors
///
/// Returns the cancelled error if the run is cancelled before `work` completes, and otherwise
/// whatever error `work` returns.
pub async fn until_cancelled<T>(
    ctx: &SessionContext,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    let Some(cancellation) = ctx.state().config().get_extension::<Cancellation>() else {
        return work.await;
    };
    tokio::select! {
        biased;
        () = cancellation.0.cancelled() => Err(cancelled()),
        result = work => result,
    }
}

/// Whether `error` is the cancelled error a cancelled run ends with, possibly beneath context
/// added on its way out.
#[must_use]
pub fn is_cancelled(error: &DataFusionError) -> bool {
    matches!(
        error.find_root(),
        DataFusionError::External(source) if source.is::<Cancelled>()
    )
}

/// The cancelled error: what a cancelled run ends with.
fn cancelled() -> DataFusionError {
    DataFusionError::External(Box::new(Cancelled))
}

/// The source of the cancelled error.
#[derive(Debug)]
struct Cancelled;

impl fmt::Display for Cancelled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the run was cancelled")
    }
}

impl Error for Cancelled {}

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
