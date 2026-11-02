"""The in-process backend, the datasets it opens for a frontend to plan against, and its runs."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from functools import reduce
from typing import NamedTuple

import pyarrow as pa

from hail_df import _native
from hail_df._native import BackendError, InvalidAction, RunCancelled, ScanDescription
from hail_df.datafusion import DATAFUSION_MAJOR, WIRE_VERSION, DataFrame, SessionContext

__all__ = [
    "Backend",
    "BackendError",
    "Collect",
    "Dataset",
    "Explain",
    "ExplainAnalyze",
    "FormulationRecord",
    "InputTable",
    "InvalidAction",
    "MeasuredWrite",
    "MeasuredWritten",
    "OutputLayout",
    "PlanText",
    "Rows",
    "RowsWritten",
    "Run",
    "RunCancelled",
    "ScanDescription",
    "SortColumn",
    "Write",
]


class SortColumn(NamedTuple):
    """One entry of a plain sort list: a column, its direction, and where its nulls sort."""

    column: str
    ascending: bool
    nulls_first: bool


@dataclass(frozen=True)
class InputTable:
    """One input table of an opened dataset: its name, its sample set, and its scan description."""

    name: str
    sample_set: tuple[str, ...]
    scan_description: ScanDescription
    _scan: _native.InputTableScan = field(repr=False, compare=False)


class Dataset:
    """A dataset the backend opened, for a frontend to plan against.

    Its scans plan like tables but refuse to scan in Python: only the backend runs a plan that
    reads them.
    """

    def __init__(self, path: str, input_format: str, row_ordering: str, opened) -> None:
        self.path = path
        self.input_format = input_format
        self.row_ordering = row_ordering
        self.input_tables: tuple[InputTable, ...] = tuple(
            InputTable(scan.name, tuple(scan.sample_set), scan.description, scan)
            for scan in opened.input_tables
        )
        self.ordering: tuple[SortColumn, ...] = tuple(
            SortColumn(*column) for column in opened.ordering
        )
        self.schema: pa.Schema = pa.schema(opened.schema)
        self._opened = opened

    def __repr__(self) -> str:
        return (
            f"Dataset(path={self.path!r}, input_format={self.input_format!r}, "
            f"row_ordering={self.row_ordering!r}, input_tables={len(self.input_tables)})"
        )

    def read(self, ctx: SessionContext, names: Sequence[str] | None = None) -> DataFrame:
        """A frame over the named input tables' scans, in the order named: the union of their
        scans, or the single scan of one. Every input table, in name order, when `names` is None.

        `ctx` must be a session from `hail_df.context()`, so that plans over the scans encode.

        Raises `ValueError` if `names` is empty, repeats a name, or names no input table of the
        dataset.
        """
        tables = {table.name: table for table in self.input_tables}
        if names is None:
            names = list(tables)
        elif isinstance(names, str):
            raise TypeError("names must be a sequence of input table names, not one name")
        if not names:
            raise ValueError("read at least one input table")
        if len(set(names)) != len(names):
            raise ValueError(f"input table names repeat: {list(names)}")
        unknown = [name for name in names if name not in tables]
        if unknown:
            raise ValueError(f"no input tables named {unknown} in dataset {self.path!r}")
        frames = [ctx.read_table(tables[name]._scan) for name in names]
        return reduce(lambda left, right: left.union(right), frames)


@dataclass(frozen=True)
class Collect:
    """The Action that collects a plan's rows, held in `ordering`, into the run's Outcome."""

    ordering: Sequence[SortColumn]


class OutputLayout(Enum):
    """How a write lays rows out at its output path: one file, or one file per partition."""

    SINGLE_FILE = "single_file"
    FILE_PER_PARTITION = "file_per_partition"


@dataclass(frozen=True)
class Write:
    """The Action that writes the rows, held in `ordering`, to `output_path` in `output_format`,
    with `compression` or the format's default when None.

    With `annotate_samples`, the sample annotation table written beside the output covers the union
    of the sample sets of the input tables the plan's scans read. A data write that fails leaves no
    table. An `OutputLayout.FILE_PER_PARTITION` layout raises `InvalidAction` until per-partition
    assertions exist.
    """

    output_path: str
    output_format: str
    compression: str | None
    ordering: Sequence[SortColumn]
    layout: OutputLayout
    annotate_samples: bool = False


@dataclass(frozen=True)
class MeasuredWrite:
    """The Action that performs `write` and records the run under `metrics_directory` with
    `formulation`: a run record, and for a completed run its run metrics.

    The backend takes the record's dataset path, input format, row ordering, input table count and
    sample count from the plan's scans. A plan that doesn't read exactly one dataset raises
    `InvalidAction`.
    """

    write: Write
    metrics_directory: str
    formulation: "FormulationRecord"


@dataclass(frozen=True)
class Explain:
    """The Action that renders the plan of a run that drains the rows, held in `ordering`, or
    performs `write`, without running it. A `write` must hold the rows in the same `ordering`."""

    ordering: Sequence[SortColumn]
    write: Write | None = None


@dataclass(frozen=True)
class ExplainAnalyze:
    """The Action that runs the plan, draining the rows, held in `ordering`, or performing `write`,
    and renders it with per-operator metrics. A `write` must hold the rows in the same `ordering`,
    and is performed as a `Write` is."""

    ordering: Sequence[SortColumn]
    write: Write | None = None


Action = Collect | Explain | ExplainAnalyze | Write | MeasuredWrite


@dataclass(frozen=True)
class Rows:
    """The Outcome of a `Collect`: the rows, in its ordering, as Arrow data."""

    rows: pa.Table


@dataclass(frozen=True)
class PlanText:
    """The Outcome of an `Explain` or `ExplainAnalyze`: the plan as a table of text, with a section
    each for the decoded `frontend_plan`, the `logical_plan` and the `physical_plan`, and for an
    `ExplainAnalyze` the `Plan with Metrics`."""

    text: str


@dataclass(frozen=True)
class RowsWritten:
    """The Outcome of a `Write`: how many rows it wrote."""

    rows_written: int


@dataclass(frozen=True)
class MeasuredWritten:
    """The Outcome of a `MeasuredWrite`: how many rows it wrote, and what it could not record: the
    names of the metrics its plan reported that the run metrics table has no column for."""

    rows_written: int
    unrecorded_metrics: tuple[str, ...]


Outcome = Rows | PlanText | RowsWritten | MeasuredWritten


@dataclass(frozen=True)
class FormulationRecord:
    """The settings of a formulation that a run record holds."""

    name: str
    groups: int | None = None
    split_points: str | None = None


class Run:
    """A started run. It goes on in the background: dropping the handle doesn't stop it."""

    def __init__(self, run: _native.Run) -> None:
        self._run = run

    def __repr__(self) -> str:
        return f"Run(run_id={self.run_id!r})"

    @property
    def run_id(self) -> str:
        """The run's id, the one given to `Backend.start` or a generated one."""
        return self._run.run_id

    def wait(self) -> Outcome:
        """Blocks until the run ends and returns its Outcome. Other Python threads keep running.

        Raises `RunCancelled` if the run was cancelled and `BackendError` if it failed. Waiting
        again hands back the same result.

        On Ctrl-C, in the main thread, it cancels the run, waits for it to end so that a measured
        write has recorded itself, and raises `KeyboardInterrupt`. A second Ctrl-C raises at once,
        leaving the cancelled run to end in the background.
        """
        match self._run.wait():
            case str() as text:
                return PlanText(text)
            case int() as rows_written:
                return RowsWritten(rows_written)
            case (int() as rows_written, list() as unrecorded_metrics):
                return MeasuredWritten(rows_written, tuple(unrecorded_metrics))
            case rows:
                return Rows(pa.table(rows))


class Backend:
    """The in-process backend. It resolves object stores and credentials from URLs."""

    def __init__(self) -> None:
        self._backend = _native.Backend()

    def open_dataset(self, path: str, input_format: str, row_ordering: str) -> Dataset:
        """Opens the dataset at `path`, a local path or a `gs://` URL.

        `input_format` is `parquet` or `vortex`, and `row_ordering` is the order its rows are
        declared in: `locus`, `locus,alleles` or `locus,s`. Raises `BackendError` if the dataset
        can't be discovered or its rows lack a column of the row ordering.
        """
        opened = self._backend.open_dataset(
            path, input_format, row_ordering, DATAFUSION_MAJOR, WIRE_VERSION
        )
        return Dataset(path, input_format, row_ordering, opened)

    def scan_descriptions(self, plan_bytes: bytes) -> list[ScanDescription]:
        """The scan descriptions plan bytes read, each once, in the order the plan first reads
        them: what the backend will read for the plan.

        Raises `BackendError` if the bytes are not a plan encoded in a `hail_df.context()`
        session.
        """
        return self._backend.scan_descriptions(plan_bytes, DATAFUSION_MAJOR, WIRE_VERSION)

    def builtin_plan(self, dataset: Dataset, formulation: FormulationRecord) -> bytes:
        """The plan bytes of a built-in Rust formulation over `dataset`'s input tables, every scan
        encoded as the scan description `dataset` handed out. They run through `start` as a
        frontend's do.

        Raises `InvalidAction` if the wheel's versions aren't the backend's or `formulation` is not
        built in: only `FormulationRecord("union")` and `FormulationRecord("grouped-merge", groups)`
        with a positive group count are. Raises `BackendError` if the dataset can no longer be read
        as it was opened.
        """
        if not isinstance(formulation, FormulationRecord):
            raise TypeError(f"formulation must be a FormulationRecord, not {formulation!r}")
        try:
            return self._backend.builtin_plan(
                dataset._opened,
                formulation.name,
                formulation.groups,
                formulation.split_points,
                DATAFUSION_MAJOR,
                WIRE_VERSION,
            )
        except OverflowError as error:
            # A count the native call can't take, such as a negative one, is no built-in's.
            raise InvalidAction(
                f"formulation {formulation.name!r} has no built-in plan with "
                f"groups={formulation.groups}"
            ) from error

    def start(
        self,
        ctx: SessionContext,
        frame_or_plan_bytes: DataFrame | bytes,
        action: Action,
        settings: Mapping[str, str] | None = None,
        run_id: str | None = None,
    ) -> Run:
        """Starts a run of a plan that performs `action` under `settings`, as `run_id` or under a
        generated id, and returns its handle.

        A frame is encoded unoptimized in `ctx`, a session from `hail_df.context()`; plan bytes
        were already encoded in one, here or elsewhere, and cross as they are.

        `settings` takes exactly `hail_df.threads`, `hail_df.target_partitions` and
        `hail_df.repartition_file_min_size`, each a string.

        Raises `InvalidAction`, before the run starts, if the wheel's versions aren't the
        backend's, a setting's name is unknown or its value doesn't parse, the Action's ordering is
        empty, a write can't be performed as it asks, an explain's write holds another ordering, a
        measured write's plan doesn't read exactly one dataset, a run under `run_id` is still going,
        or the plan carries a payload framed at another wire version or an inlined Python function.
        Raises `BackendError` if the
        bytes are not otherwise a plan encoded in such a session. Raises `TypeError` if `action` is
        not an Action, or a setting's name or value is not a string.
        """
        native_action = _native_action(action)
        plan_bytes = (
            frame_or_plan_bytes.logical_plan().to_bytes(ctx)
            if isinstance(frame_or_plan_bytes, DataFrame)
            else frame_or_plan_bytes
        )
        return Run(
            self._backend.start(
                plan_bytes,
                native_action,
                dict(settings or {}),
                run_id,
                DATAFUSION_MAJOR,
                WIRE_VERSION,
            )
        )

    def cancel(self, run_id: str) -> None:
        """Cancels the going run named `run_id`, whether or not a handle to it is left. Its `wait`
        raises `RunCancelled`, and a measured write records `status = cancelled`.

        An id this backend has no run for, or whose run has already ended, is not an error: a run
        that completed keeps its Outcome.
        """
        self._backend.cancel(run_id)


def _native_write(write: Write | None) -> _native.Write | None:
    """`write` as the backend takes it."""
    if write is None:
        return None
    if not isinstance(write, Write):
        raise TypeError(f"write must be a Write, not {write!r}")
    layout = write.layout.value if isinstance(write.layout, OutputLayout) else write.layout
    return _native.Write(
        write.output_path,
        write.output_format,
        write.compression,
        list(write.ordering),
        layout,
        write.annotate_samples,
    )


def _native_action(action: Action) -> _native.Action:
    """`action` as the backend takes it."""
    match action:
        case Collect():
            return _native.Action.collect(list(action.ordering))
        case Explain():
            return _native.Action.explain(list(action.ordering), _native_write(action.write))
        case ExplainAnalyze():
            return _native.Action.explain_analyze(
                list(action.ordering), _native_write(action.write)
            )
        case Write():
            return _native.Action.write(_native_write(action))
        case MeasuredWrite():
            return _native_measured_write(action)
    raise TypeError(
        f"action must be a Collect, Explain, ExplainAnalyze, Write or MeasuredWrite, not {action!r}"
    )


def _native_measured_write(action: MeasuredWrite) -> _native.Action:
    """`action` as the backend takes it."""
    if not isinstance(action.formulation, FormulationRecord):
        raise TypeError(f"formulation must be a FormulationRecord, not {action.formulation!r}")
    try:
        return _native.Action.measured_write(
            _native_write(action.write),
            action.metrics_directory,
            action.formulation.name,
            action.formulation.groups,
            action.formulation.split_points,
        )
    except OverflowError as error:
        # A run record holds a group count as an unsigned count, so a negative one is no record's.
        raise InvalidAction(
            f"a run record can't hold groups={action.formulation.groups}"
        ) from error
