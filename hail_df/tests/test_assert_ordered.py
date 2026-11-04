"""`hail_df.assert_ordered`: a mark in the plan where rows already arrive in an order, which the
backend holds with a merge tree or fails before execution."""

import hail_df
import pytest
from conftest import ROWS, plan_sections
from datafusion import col, lit
from hail_df import (
    BackendError,
    Collect,
    Explain,
    InvalidAction,
    OrderingAssertionFailed,
    OutputLayout,
    SortColumn,
    Write,
)

SETTINGS = {
    "default": None,
    "hostile": {"hail_df.target_partitions": "8", "hail_df.repartition_file_min_size": "0"},
}

CONTIG = SortColumn("contig", True, False)
POSITION = SortColumn("position", True, False)
ALLELES = SortColumn("alleles", True, False)
POSITION_FIRST = (POSITION, CONTIG, ALLELES)


@pytest.fixture(params=SETTINGS)
def settings(request):
    return SETTINGS[request.param]


@pytest.fixture
def ctx():
    return hail_df.context()


def sort_keys(ordering):
    return [
        col(column).sort(ascending=ascending, nulls_first=nulls_first)
        for column, ascending, nulls_first in ordering
    ]


def physical_plan(outcome) -> str:
    return plan_sections(outcome.text)["physical_plan"]


def test_a_context_registers_the_function_under_its_name(ctx):
    assert "assert_ordered" in ctx.udfs()
    assert ctx.udf("assert_ordered").name == "assert_ordered"


def test_the_marker_is_a_filter_of_each_key_column_and_its_direction_above_the_frame(ctx, dataset):
    ordering = [
        SortColumn("contig", True, False),
        SortColumn("position", True, True),
        SortColumn("alleles", False, False),
        SortColumn("s", False, True),
    ]

    plan = hail_df.assert_ordered(dataset.read(ctx), ordering).logical_plan().display_indent()

    first_line, beneath = plan.splitlines()[0], plan.splitlines()[1]
    assert first_line.startswith("Filter: assert_ordered(")
    for column, direction in [
        ("contig", "ASC NULLS LAST"),
        ("position", "ASC NULLS FIRST"),
        ("alleles", "DESC NULLS LAST"),
        ("s", "DESC NULLS FIRST"),
    ]:
        assert f'{column}, Utf8View("{direction}")' in first_line
    assert "Union" in beneath


def test_asserting_no_key_raises_value_error(ctx, dataset):
    with pytest.raises(ValueError, match="key"):
        hail_df.assert_ordered(dataset.read(ctx), [])


def test_the_marker_survives_to_bytes_and_the_backend_s_decoding_as_written(backend, ctx, dataset):
    frame = hail_df.assert_ordered(dataset.read(ctx), dataset.ordering)

    outcome = backend.start(ctx, frame, Explain(dataset.ordering)).wait()

    sections = plan_sections(outcome.text)
    assert sections["frontend_plan"] == frame.logical_plan().display_indent().rstrip()
    assert "assert_ordered" not in sections["logical_plan"]
    assert "OrderingAssertion" in sections["logical_plan"]


def test_plan_bytes_of_a_marked_frame_start_as_the_frame_does(backend, ctx, dataset):
    frame = hail_df.assert_ordered(dataset.read(ctx), dataset.ordering)
    action = Explain(dataset.ordering)

    from_frame = backend.start(ctx, frame, action).wait()
    from_bytes = backend.start(ctx, frame.logical_plan().to_bytes(ctx), action).wait()

    assert physical_plan(from_frame) == physical_plan(from_bytes)


def test_a_mid_plan_assertion_over_a_union_holds_a_merge_tree_beneath_it_with_no_sort(
    backend, ctx, dataset, settings
):
    # The marker holds alleles too, so the projection dropping it stays above the marker.
    frame = hail_df.assert_ordered(dataset.read(ctx), dataset.ordering).select("contig", "position")

    outcome = backend.start(ctx, frame, Explain((CONTIG, POSITION)), settings=settings).wait()

    operators = [line.strip().split(":")[0] for line in physical_plan(outcome).splitlines()]
    # Above the root assertion sits only what drains the rows.
    root = operators.index("OrderingAssertionExec")
    assert operators[root : root + 5] == [
        "OrderingAssertionExec",
        "ProjectionExec",
        "OrderingAssertionExec",
        "SortPreservingMergeExec",
        "UnionExec",
    ]
    assert "SortExec" not in outcome.text


def test_a_mid_plan_assertion_collects_every_row_in_its_order(backend, ctx, dataset, settings):
    frame = hail_df.assert_ordered(dataset.read(ctx), dataset.ordering)

    outcome = backend.start(ctx, frame, Collect(dataset.ordering), settings=settings).wait()

    rows = outcome.rows.to_pylist()
    assert [(row["contig"], row["position"], row["alleles"]) for row in rows] == sorted(ROWS * 4)


def test_an_assertion_per_input_group_holds_each_group_beneath_a_final_merge(
    backend, ctx, dataset, settings
):
    groups = [["g0", "s=HG02230"], ["s=NA18534"]]
    left, right = [
        hail_df.assert_ordered(dataset.read(ctx, names), dataset.ordering) for names in groups
    ]

    outcome = backend.start(
        ctx, left.union(right), Explain(dataset.ordering), settings=settings
    ).wait()

    plan = physical_plan(outcome)
    assert plan.count("OrderingAssertionExec") == 3
    assert plan.count("SortPreservingMergeExec") == 2
    assert "SortExec" not in outcome.text


def lacking_order(ctx, dataset):
    return hail_df.assert_ordered(dataset.read(ctx), POSITION_FIRST), POSITION_FIRST


def computed_key(ctx, dataset):
    computed = dataset.read(ctx).select(
        col("contig"), (col("position") + lit(1)).alias("position"), col("alleles")
    )
    return hail_df.assert_ordered(computed, dataset.ordering), dataset.ordering


@pytest.mark.parametrize("control", [lacking_order, computed_key], ids=lambda f: f.__name__)
def test_an_assertion_only_a_sort_could_meet_raises_ordering_assertion_failed(
    backend, ctx, dataset, settings, control
):
    frame, ordering = control(ctx, dataset)

    # Explaining plans the run and never executes it, so it fails where a Collect fails.
    for action in [Explain(ordering), Collect(ordering)]:
        with pytest.raises(
            OrderingAssertionFailed, match="(?s)ordering_assertion_failed.*by sorting"
        ):
            backend.start(ctx, frame, action, settings=settings).wait()


def test_a_failed_assertion_writes_nothing(backend, ctx, dataset, tmp_path):
    frame, ordering = lacking_order(ctx, dataset)
    output = tmp_path / "out.parquet"
    write = Write(str(output), "parquet", None, ordering, OutputLayout.SINGLE_FILE)

    with pytest.raises(OrderingAssertionFailed):
        backend.start(ctx, frame, write).wait()

    assert not output.exists()


def test_an_assertion_above_a_frontend_sort_keeps_its_merge_tree(backend, ctx, dataset, settings):
    sorted_frame = dataset.read(ctx).sort(*sort_keys(dataset.ordering))
    frame = hail_df.assert_ordered(sorted_frame, dataset.ordering)

    outcome = backend.start(ctx, frame, Explain(dataset.ordering), settings=settings).wait()

    plan = physical_plan(outcome)
    assert plan.count("OrderingAssertionExec") == 2
    assert "EstablishedOrderExec" in plan
    assert "SortPreservingMergeExec" in plan
    assert "SortExec" not in outcome.text


def test_an_assertion_above_a_frontend_sort_into_an_order_the_scans_lack_is_accepted(
    backend, ctx, dataset, settings
):
    sorted_frame = dataset.read(ctx).sort(*sort_keys(POSITION_FIRST))
    frame = hail_df.assert_ordered(sorted_frame, POSITION_FIRST)

    outcome = backend.start(ctx, frame, Collect(POSITION_FIRST), settings=settings).wait()

    rows = outcome.rows.to_pylist()
    keys = [(row["position"], row["contig"], row["alleles"]) for row in rows]
    assert keys == sorted(keys)
    assert len(keys) == len(ROWS) * 4


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("nulls_first", [True, False])
def test_an_assertion_holds_the_direction_it_states(backend, ctx, dataset, ascending, nulls_first):
    ordering = [SortColumn("position", ascending, nulls_first)]
    sorted_frame = dataset.read(ctx).sort(*sort_keys(ordering))
    frame = hail_df.assert_ordered(sorted_frame, ordering)

    outcome = backend.start(ctx, frame, Collect(ordering)).wait()

    positions = outcome.rows.column("position").to_pylist()
    assert positions == sorted(positions, reverse=not ascending)
    assert len(positions) == len(ROWS) * 4


def test_a_marker_in_a_larger_predicate_raises_invalid_action_from_start(backend, ctx, dataset):
    marker = ctx.udf("assert_ordered")(col("contig"), lit("ASC NULLS LAST"))
    frame = dataset.read(ctx).filter(marker & (col("position") > lit(0)))

    with pytest.raises(InvalidAction, match="whole predicate"):
        backend.start(ctx, frame, Collect(dataset.ordering))


def test_a_marker_with_an_unknown_direction_raises_invalid_action_from_start(backend, ctx, dataset):
    frame = dataset.read(ctx).filter(ctx.udf("assert_ordered")(col("contig"), lit("UP")))

    with pytest.raises(InvalidAction, match="direction"):
        backend.start(ctx, frame, Collect(dataset.ordering))


def test_the_wheel_can_t_evaluate_a_marker(ctx):
    with pytest.raises(Exception, match="computes nothing"):
        ctx.sql("SELECT assert_ordered(1, 'ASC NULLS LAST')").collect()


def test_ordering_assertion_failed_is_a_backend_error_and_not_an_invalid_action():
    assert issubclass(OrderingAssertionFailed, BackendError)
    assert not issubclass(OrderingAssertionFailed, InvalidAction)
