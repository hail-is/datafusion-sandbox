"""The reference combiner's formulations, built in datafusion-python's own `DataFrame` API."""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from functools import reduce

from hail_df.backend import Dataset, FormulationRecord, InputTable, SortColumn
from hail_df.datafusion import DataFrame, SessionContext, SortExpr, col


class OutputLayout(Enum):
    """How a write lays a formulation's rows out at the output path."""

    SINGLE_FILE = "single_file"
    FILE_PER_PARTITION = "file_per_partition"


@dataclass(frozen=True)
class OrderedFrame:
    """What a formulation builds: a frame, the ordering a run holds its rows in, the output layout
    a write gives them, and its formulation record."""

    frame: DataFrame
    ordering: tuple[SortColumn, ...]
    layout: OutputLayout
    formulation: FormulationRecord


def union(ctx: SessionContext, dataset: Dataset) -> OrderedFrame:
    """The union of every input table's scan, sorted by the dataset's ordering, for one file.

    `ctx` must be a session from `hail_df.context()`.
    """
    return OrderedFrame(
        dataset.read(ctx).sort(*_sort_keys(dataset)),
        dataset.ordering,
        OutputLayout.SINGLE_FILE,
        FormulationRecord("union"),
    )


def grouped_merge(ctx: SessionContext, dataset: Dataset, groups: int) -> OrderedFrame:
    """The union of `groups` input groups, each the union of its input tables' scans sorted by the
    dataset's ordering, with no sort above them, for one file. The root assertion of the run merges
    the groups, as it does the Rust formulation's. The formulation record keeps `groups` as given.

    `ctx` must be a session from `hail_df.context()`.

    Raises `ValueError` if `groups` is not positive.
    """
    if groups < 1:
        raise ValueError(f"groups must be positive, not {groups}")
    keys = _sort_keys(dataset)
    frames = [
        dataset.read(ctx, [table.name for table in group]).sort(*keys)
        for group in _input_groups(dataset.input_tables, groups)
    ]
    return OrderedFrame(
        reduce(lambda left, right: left.union(right), frames),
        dataset.ordering,
        OutputLayout.SINGLE_FILE,
        FormulationRecord("grouped-merge", groups=groups),
    )


def _sort_keys(dataset: Dataset) -> list[SortExpr]:
    return [
        col(column.column).sort(ascending=column.ascending, nulls_first=column.nulls_first)
        for column in dataset.ordering
    ]


def _input_groups(input_tables: Sequence[InputTable], groups: int) -> list[Sequence[InputTable]]:
    """`input_tables` split into contiguous, count-balanced input groups, larger groups first. A
    group count above the input table count clamps to one input table per group."""
    groups = min(groups, len(input_tables))
    quotient, remainder = divmod(len(input_tables), groups)
    split = []
    start = 0
    for index in range(groups):
        end = start + quotient + (index < remainder)
        split.append(input_tables[start:end])
        start = end
    return split
