"""The in-process backend, and the datasets it opens for a frontend to plan against."""

from collections.abc import Sequence
from dataclasses import dataclass, field
from functools import reduce
from typing import NamedTuple

import pyarrow as pa

from hail_df import _native
from hail_df._native import BackendError, ScanDescription
from hail_df.datafusion import DataFrame, SessionContext

__all__ = ["Backend", "BackendError", "Dataset", "InputTable", "ScanDescription", "SortColumn"]


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
        opened = self._backend.open_dataset(path, input_format, row_ordering)
        return Dataset(path, input_format, row_ordering, opened)

    def scan_descriptions(self, plan_bytes: bytes) -> list[ScanDescription]:
        """The scan descriptions plan bytes read, each once, in the order the plan first reads
        them: what the backend will read for the plan.

        Raises `BackendError` if the bytes are not a plan encoded in a `hail_df.context()`
        session.
        """
        return self._backend.scan_descriptions(plan_bytes)
