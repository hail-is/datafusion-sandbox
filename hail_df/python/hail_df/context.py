"""Frontend sessions: the DataFusion `SessionContext` a frontend plans in, and the ordering
assertions it marks its plans with."""

from collections.abc import Iterable

from hail_df._native import AssertOrdered, Codec
from hail_df.backend import SortColumn
from hail_df.datafusion import DataFrame, ScalarUDF, SessionContext, col, lit

_ASSERT_ORDERED = ScalarUDF.from_pycapsule(AssertOrdered())


def context() -> SessionContext:
    """A fresh `SessionContext` with the backend's codec installed and `assert_ordered` registered.

    Plans built in it encode for the backend with `frame.logical_plan().to_bytes(ctx)`. It is
    frontend state: the backend runs every plan in a session of its own.
    """
    ctx = SessionContext().with_logical_extension_codec(Codec())
    ctx.register_udf(_ASSERT_ORDERED)
    return ctx


def assert_ordered(frame: DataFrame, ordering: Iterable[SortColumn]) -> DataFrame:
    """`frame` with an ordering assertion above it: the rows already arrive in `ordering` there.

    The backend holds that order with a merge tree, with the rows on one partition from there on,
    or raises `OrderingAssertionFailed` before the plan executes if only a sort it inserted could
    meet the order. A sort `frame` wrote counts as meeting it.

    The assertion is a marker the backend reads from the plan as written:
    `Filter(assert_ordered(<key column>, '<direction>', …))`, with a direction of `ASC NULLS LAST`
    and the like. Each key column is by name, in `frame`'s schema.

    Raises `ValueError` if `ordering` holds no key.
    """
    args = [
        expr
        for column, ascending, nulls_first in ordering
        for expr in (col(column), lit(_direction(ascending, nulls_first)))
    ]
    if not args:
        raise ValueError("an ordering assertion needs at least one key")
    return frame.filter(_ASSERT_ORDERED(*args))


def _direction(ascending: bool, nulls_first: bool) -> str:
    return f"{'ASC' if ascending else 'DESC'} NULLS {'FIRST' if nulls_first else 'LAST'}"
