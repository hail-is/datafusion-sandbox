//! The CPU runtime: a Tokio runtime for plan execution that runs on, and is dropped on, a thread
//! of its own.

use std::sync::Arc;

use datafusion::error::Result;
use tokio::runtime::Handle;
use tokio::sync::Notify;

/// A Tokio runtime for CPU-bound tasks, on a thread of its own.
///
/// Tokio forbids dropping a `Runtime` in an async context, so this keeps the runtime on a separate
/// thread, and dropping this structure stops that thread. It has timers but no IO driver.
///
/// # Notes
/// Dropping stops the runtime as dropping a Tokio `Runtime` does: a task still pending is dropped
/// at its next yield and does not run to completion. Wait for the tasks whose results matter
/// before dropping, as the pipeline runner does.
///
/// # Credits
/// This code is derived from code originally written for [InfluxDB 3.0],
/// by way of <https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/thread_pools.rs>
///
/// [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
pub(super) struct CpuRuntime {
    /// Handle is the tokio structure for interacting with a Runtime.
    handle: Handle,
    /// Signal to start shutting down
    notify_shutdown: Arc<Notify>,
    /// When thread is active, is Some
    thread_join_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for CpuRuntime {
    fn drop(&mut self) {
        // Notify the thread to shutdown.
        self.notify_shutdown.notify_one();
        if let Some(thread_join_handle) = self.thread_join_handle.take() {
            // If the thread is still running, we wait for it to finish. Report a panicked thread
            // on stderr: Drop cannot propagate it, and panicking here would be worse.
            if let Err(e) = thread_join_handle.join() {
                eprintln!("Error joining CPU runtime thread: {e:?}");
            }
        }
    }
}

impl CpuRuntime {
    /// Creates a new Tokio runtime for CPU-bound tasks with `worker_threads` worker threads.
    ///
    /// # Errors
    ///
    /// Returns an error if Tokio cannot build the runtime.
    pub(super) fn try_new(worker_threads: usize) -> Result<Self> {
        // Multi-thread even at one worker: DataFusion's `spawn_buffered` keys off the runtime
        // flavor, not the thread count. See
        // docs/adr/0001-always-use-multi-thread-tokio-runtimes.md.
        let cpu_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_time()
            .build()?;
        let handle = cpu_runtime.handle().clone();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        // The cpu_runtime runs and is dropped on a separate thread
        let thread_join_handle = std::thread::spawn(move || {
            cpu_runtime.block_on(async move {
                notify_shutdown_captured.notified().await;
            });
            // cpu_runtime is dropped here, which drops any task still pending at its next yield.
        });

        Ok(Self {
            handle,
            notify_shutdown,
            thread_join_handle: Some(thread_join_handle),
        })
    }

    /// Return a handle suitable for spawning CPU bound tasks
    ///
    /// # Notes
    ///
    /// If a task spawned on this handle attempts to do IO, it will panic with a
    /// message such as:
    ///
    /// ```text
    /// A Tokio 1.x context was found, but IO is disabled.
    /// ```
    #[must_use]
    pub(super) const fn handle(&self) -> &Handle {
        &self.handle
    }
}
