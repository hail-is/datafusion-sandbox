"""Cancelling a run through the real backend, by `Backend.cancel` and by an interrupt during `wait`.

What cancelling does to a run is the Rust suite's claim, on stores it can hold open. These tests
claim what crosses the binding, and hold a run open the only way the wheel allows, by giving it far
more to read than a test takes to reach its cancel: see `write_large_dataset`.
"""

import os
import signal
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import hail_df
import pyarrow.parquet as pq
import pytest
from conftest import write_large_dataset, write_mixed_dataset
from datafusion import col, functions, lit
from hail_df import (
    BackendError,
    Collect,
    ExplainAnalyze,
    FormulationRecord,
    InvalidAction,
    MeasuredWrite,
    OutputLayout,
    Rows,
    RunCancelled,
    Write,
)


@pytest.fixture
def ctx():
    return hail_df.context()


@pytest.fixture(scope="module")
def large(backend, tmp_path_factory):
    path = write_large_dataset(tmp_path_factory.mktemp("cancellation"), 10_000_000)
    return backend.open_dataset(str(path), "parquet", "locus,alleles")


def sort_keys(dataset) -> list:
    return [
        col(column).sort(ascending, nulls_first)
        for column, ascending, nulls_first in dataset.ordering
    ]


def long_run_frame(ctx, dataset):
    """The rows of `dataset` in its ordering, whose run lasts as long as `dataset` is large."""
    return dataset.read(ctx).sort(*sort_keys(dataset))


def long_measured_write(dataset, root: Path) -> MeasuredWrite:
    write = Write(
        str(root / "out" / "g.parquet"),
        "parquet",
        None,
        dataset.ordering,
        OutputLayout.SINGLE_FILE,
        False,
    )
    return MeasuredWrite(write, str(root / "metrics"), FormulationRecord("union"))


def run_status(root: Path, run_id: str) -> str:
    table = pq.read_table(root / "metrics" / "runs" / f"{run_id}.parquet")
    return table.column("status").to_pylist()[0]


def await_path(path: Path) -> None:
    """Returns once `path` exists: the wait for what a run no one is waiting on leaves behind."""
    deadline = time.monotonic() + 60
    while not path.exists():
        assert time.monotonic() < deadline, f"nothing appeared at {path}"
        time.sleep(0.001)


@contextmanager
def interrupting_wait(interrupts: int = 1, settle: float = 0.05) -> Iterator[None]:
    """Sends this process up to `interrupts` SIGINTs, starting `settle` seconds after the main
    thread is in `Run.wait`, and so past the point where one would raise before `wait` could see
    it. Each waits for the signal handler to have seen the last, since signals sent between two of
    `wait`'s checks arrive as one, and none is sent once the main thread has left `wait`."""
    main_thread = threading.main_thread().ident
    leaving = threading.Event()
    seen = threading.Semaphore(0)
    handler = signal.getsignal(signal.SIGINT)

    def seeing(signum, frame) -> None:
        try:
            handler(signum, frame)
        finally:
            seen.release()

    def waiting() -> bool:
        return sys._current_frames()[main_thread].f_code.co_name == "wait"

    def interrupt() -> None:
        while not waiting():
            if leaving.wait(0.001):
                return
        if leaving.wait(settle):
            return
        for _ in range(interrupts):
            if not waiting():
                return
            os.kill(os.getpid(), signal.SIGINT)
            while not seen.acquire(timeout=0.001):
                if leaving.is_set():
                    return

    signal.signal(signal.SIGINT, seeing)
    interrupter = threading.Thread(target=interrupt)
    interrupter.start()
    try:
        yield
    finally:
        leaving.set()
        interrupter.join()
        signal.signal(signal.SIGINT, handler)


def test_cancel_then_wait_raises_run_cancelled_with_no_outcome(backend, large, ctx):
    run = backend.start(ctx, long_run_frame(ctx, large), ExplainAnalyze(large.ordering))

    backend.cancel(run.run_id)

    with pytest.raises(RunCancelled, match="^run_cancelled") as raised:
        run.wait()
    assert isinstance(raised.value, BackendError)
    assert not isinstance(raised.value, InvalidAction)
    with pytest.raises(RunCancelled):
        run.wait()


def test_cancel_after_a_run_completed_leaves_its_outcome(backend, dataset, ctx):
    frame = dataset.read(ctx).sort(*sort_keys(dataset))
    run = backend.start(ctx, frame, Collect(dataset.ordering))
    outcome = run.wait()

    backend.cancel(run.run_id)

    assert isinstance(outcome, Rows)
    assert run.wait() == outcome


def test_cancelling_an_unknown_run_id_is_not_an_error(backend):
    assert backend.cancel("no-such-run") is None


def test_dropping_a_handle_leaves_its_run_for_cancel_to_reach(backend, large, ctx, tmp_path):
    action = long_measured_write(large, tmp_path)

    # An id of its own: with no handle to wait on, the run may still be ending when this test ends.
    backend.start(ctx, long_run_frame(ctx, large), action, run_id="run-dropped")
    backend.cancel("run-dropped")

    await_path(tmp_path / "metrics" / "runs" / "run-dropped.parquet")
    assert run_status(tmp_path, "run-dropped") == "cancelled"


def test_starting_under_the_id_of_a_going_run_raises_invalid_action(backend, large, ctx):
    frame = long_run_frame(ctx, large)
    going = backend.start(ctx, frame, ExplainAnalyze(large.ordering), run_id="run-a")

    with pytest.raises(InvalidAction, match="run-a"):
        backend.start(ctx, frame, ExplainAnalyze(large.ordering), run_id="run-a")

    backend.cancel("run-a")
    with pytest.raises(RunCancelled):
        going.wait()


def test_an_interrupt_during_wait_cancels_the_run_and_raises_keyboard_interrupt(
    backend, large, ctx
):
    run = backend.start(ctx, long_run_frame(ctx, large), ExplainAnalyze(large.ordering))

    with interrupting_wait(), pytest.raises(KeyboardInterrupt):
        run.wait()

    with pytest.raises(RunCancelled):
        run.wait()


def test_an_interrupted_measured_write_has_recorded_itself_when_keyboard_interrupt_raises(
    backend, large, ctx, tmp_path
):
    action = long_measured_write(large, tmp_path)
    run = backend.start(ctx, long_run_frame(ctx, large), action, run_id="run-a")

    with interrupting_wait(), pytest.raises(KeyboardInterrupt):
        run.wait()

    assert run_status(tmp_path, "run-a") == "cancelled"


def test_a_signal_handler_that_doesn_t_raise_leaves_the_run_going(backend, large, ctx):
    frame = long_run_frame(ctx, large)
    handled = []
    previous = signal.signal(signal.SIGINT, lambda *_: handled.append(True))
    try:
        run = backend.start(ctx, frame, ExplainAnalyze(large.ordering))
        with interrupting_wait():
            run.wait()
    finally:
        signal.signal(signal.SIGINT, previous)

    assert handled


def test_a_measured_write_cancelled_once_it_is_writing_records_cancelled_and_no_table(
    backend, large, ctx, tmp_path
):
    # Vortex's writer makes the output's directory as it starts.
    path = tmp_path / "out" / "g.vortex"
    write = Write(str(path), "vortex", None, large.ordering, OutputLayout.SINGLE_FILE, True)
    action = MeasuredWrite(write, str(tmp_path / "metrics"), FormulationRecord("union"))
    run = backend.start(ctx, long_run_frame(ctx, large), action, run_id="run-a")

    await_path(path.parent)
    backend.cancel("run-a")

    with pytest.raises(RunCancelled):
        run.wait()
    record = pq.read_table(tmp_path / "metrics" / "runs" / "run-a.parquet")
    assert record.column("status").to_pylist() == ["cancelled"]
    assert record.column("rows_written").to_pylist() == [None]
    assert not (tmp_path / "metrics" / "metrics" / "run-a.parquet").exists()
    assert not (tmp_path / "out" / "g.samples.vortex").exists()


def test_a_second_interrupt_stops_waiting_but_the_run_has_recorded_itself_before_it_ends(
    backend, ctx, tmp_path
):
    # A hash join's fan-out beneath an aggregate holds its threads between yields, so the run takes
    # far longer to end once cancelled than an interrupt takes to arrive.
    path = write_mixed_dataset(tmp_path, "parquet")
    dataset = backend.open_dataset(str(path), "parquet", "locus,alleles")
    keys = [col(column) for column, _, _ in dataset.ordering]
    frame = dataset.read(ctx).select(*keys).with_column("j", lit(1))
    for table, names in enumerate([None, None, None, None, ["s=HG02230"]]):
        right = dataset.read(ctx, names).select(lit(1).alias(f"j{table}"))
        frame = frame.join(right, left_on="j", right_on=f"j{table}").select(*keys, col("j"))
    frame = frame.aggregate(keys, [functions.count(lit(1)).alias("n")]).sort(*sort_keys(dataset))
    action = long_measured_write(dataset, tmp_path)
    run = backend.start(ctx, frame, action, run_id="run-a")

    started = time.monotonic()
    # Long enough for the run to be past planning, which stops at once, and in the fan-out.
    with interrupting_wait(interrupts=2, settle=0.3), pytest.raises(KeyboardInterrupt):
        run.wait()
    raised = time.monotonic()
    await_path(tmp_path / "metrics" / "runs" / "run-a.parquet")
    recorded = time.monotonic()
    with pytest.raises(RunCancelled):
        run.wait()
    ended = time.monotonic()

    assert raised - started < (ended - started) / 2
    assert recorded - started < (ended - started) / 2
    assert run_status(tmp_path, "run-a") == "cancelled"
