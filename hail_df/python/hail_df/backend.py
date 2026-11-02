"""The in-process backend, the datasets it opens for a frontend to plan against, and its runs."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from functools import reduce
from typing import NamedTuple

import pyarrow as pa

from hail_df import _native
from hail_df._native import BackendError, InvalidAction, ScanDescription
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
    "PlanText",
    "Rows",
    "Run",
    "ScanDescription",
    "SortColumn",
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


@dataclass(frozen=True)
class Explain:
    """The Action that renders the plan of a run that drains the rows, held in `ordering`, without
    running it. `write`, the plan of a write instead, must be None until writes land."""

    ordering: Sequence[SortColumn]
    write: None = None


@dataclass(frozen=True)
class ExplainAnalyze:
    """The Action that runs the plan, draining the rows, held in `ordering`, and renders it with
    per-operator metrics. `write`, a run that writes instead, must be None until writes land."""

    ordering: Sequence[SortColumn]
    write: None = None


Action = Collect | Explain | ExplainAnalyze


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

    def wait(self) -> Rows | PlanText:
        """Blocks until the run ends and returns its Outcome. Other Python threads keep running.

        Raises `BackendError` if the run failed. Waiting again hands back the same result.
        """
        outcome = self._run.wait()
        return PlanText(outcome) if isinstance(outcome, str) else Rows(pa.table(outcome))


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
        empty or it asks for a write, or the plan carries a payload framed at another wire version
        or an inlined Python function. Raises `BackendError` if the bytes are not otherwise a plan
        encoded in such a session. Raises `TypeError` if `action` is not an Action, or a setting's
        name or value is not a string.
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


def _native_action(action: Action) -> _native.Action:
    """`action` as the backend takes it."""
    if isinstance(action, Explain | ExplainAnalyze) and action.write is not None:
        raise InvalidAction(f"{type(action).__name__} can't take a write yet: {action.write!r}")
    ordering = list(getattr(action, "ordering", ()))
    match action:
        case Collect():
            return _native.Action.collect(ordering)
        case Explain():
            return _native.Action.explain(ordering)
        case ExplainAnalyze():
            return _native.Action.explain_analyze(ordering)
    raise TypeError(f"action must be a Collect, Explain or ExplainAnalyze, not {action!r}")
