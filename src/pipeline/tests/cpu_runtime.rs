use crate::pipeline::cpu_runtime::CpuRuntime;

use std::time::Duration;
use tokio::runtime::RuntimeFlavor;

/// The runtime is multi-thread even at one worker, since `DataFusion` keys buffering off the
/// flavor, and has the worker threads it was asked for. See ADR 0001.
#[test]
fn runs_a_multi_thread_runtime_with_the_requested_workers() {
    for workers in [1, 3] {
        let runtime = CpuRuntime::try_new(workers).unwrap();

        assert_eq!(
            runtime.handle().runtime_flavor(),
            RuntimeFlavor::MultiThread,
            "{workers}"
        );
        assert_eq!(runtime.handle().metrics().num_workers(), workers);
    }
}

/// Tasks spawned through the handle run to completion, and can wait on Tokio timers.
#[test]
fn runs_tasks_that_wait_on_timers() {
    let runtime = CpuRuntime::try_new(1).unwrap();

    let slept = futures::executor::block_on(runtime.handle().spawn(async {
        tokio::time::sleep(Duration::from_millis(1)).await;
        true
    }))
    .unwrap();

    assert!(slept);
}

/// The runtime has no IO driver, so a task that registers an IO resource panics instead of doing
/// IO on a CPU thread, where it would wait behind plan execution.
#[test]
fn a_task_that_registers_io_panics() {
    let runtime = CpuRuntime::try_new(1).unwrap();

    let error = futures::executor::block_on(runtime.handle().spawn(async {
        let (socket, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        tokio::net::UnixStream::from_std(socket)
    }))
    .unwrap_err();

    assert!(error.is_panic());
    let panic = error.into_panic();
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(message.contains("IO is disabled"), "{message}");
}

/// Dropping the runtime inside another runtime's async context does not panic, which dropping a
/// Tokio runtime there would, and returns only once the runtime's thread has stopped.
#[test]
fn drops_inside_an_async_context() {
    let outer = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let runtime = CpuRuntime::try_new(1).unwrap();
    let handle = runtime.handle().clone();

    outer.block_on(async move { drop(runtime) });

    let spawned = futures::executor::block_on(handle.spawn(async {}));
    assert!(spawned.unwrap_err().is_cancelled());
}
