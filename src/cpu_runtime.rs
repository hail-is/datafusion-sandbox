use std::sync::Arc;

use datafusion::error::Result;
use tokio::runtime::Handle;
use tokio::sync::Notify;

/// Creates a Tokio [`Runtime`] for use with CPU bound tasks
///
/// Tokio forbids dropping `Runtime`s in async contexts, so creating a separate
/// `Runtime` correctly is somewhat tricky. This structure manages the creation
/// and shutdown of a separate thread.
///
/// # Notes
/// On drop, the thread will wait for all remaining tasks to complete.
///
/// Depending on your application, more sophisticated shutdown logic may be
/// required, such as ensuring that no new tasks are added to the runtime.
///
/// # Credits
/// This code is derived from code originally written for [InfluxDB 3.0],
/// by way of https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/thread_pools.rs
///
/// [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
pub struct CpuRuntime {
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
        // In a production system you also need to ensure your code stops adding
        // new tasks to the underlying runtime after this point to allow the
        // thread to complete its work and exit cleanly.
        if let Some(thread_join_handle) = self.thread_join_handle.take() {
            // If the thread is still running, we wait for it to finish
            print!("Shutting down CPU runtime thread...");
            if let Err(e) = thread_join_handle.join() {
                eprintln!("Error joining CPU runtime thread: {e:?}",);
            } else {
                println!("CPU runtime thread shutdown successfully.");
            }
        }
    }
}

impl CpuRuntime {
    /// Create a new Tokio Runtime for CPU bound tasks, with `worker_threads` worker threads.
    /// 1 runs on a current-thread runtime, for timing single-threaded performance.
    pub fn try_new(worker_threads: usize) -> Result<Self> {
        let cpu_runtime = crate::pipeline::runtime_builder(worker_threads)
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
            // Note: cpu_runtime is dropped here, which will wait for all tasks
            // to complete
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
    /// If a task spawned on this handle attempts to do IO, it will error with a
    /// message such as:
    ///
    /// ```text
    /// A Tokio 1.x context was found, but IO is disabled.
    /// ```
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}
