"""The reference combiner's formulations, built in datafusion-python's own `DataFrame` API."""

from dataclasses import dataclass
from enum import Enum

from hail_df.backend import Dataset, FormulationRecord, SortColumn
from hail_df.datafusion import DataFrame, SessionContext, col


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
    keys = [
        col(column.column).sort(ascending=column.ascending, nulls_first=column.nulls_first)
        for column in dataset.ordering
    ]
    return OrderedFrame(
        dataset.read(ctx).sort(*keys),
        dataset.ordering,
        OutputLayout.SINGLE_FILE,
        FormulationRecord("union"),
    )
