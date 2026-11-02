"""`Backend.builtin_plan`, and its parity with the same formulation written in Python: the matrix
is both formats, the three settings maps, and the Actions that have landed."""

import hail_df
import pytest
from conftest import ROWS, SAMPLE_SETS, plan_sections
from hail_df import Collect, Explain, FormulationRecord, InvalidAction, formulations

UNION = FormulationRecord("union")

SETTINGS = {
    "default": None,
    "one-partition": {"hail_df.target_partitions": "1"},
    "hostile": {"hail_df.target_partitions": "8", "hail_df.repartition_file_min_size": "0"},
}


@pytest.fixture(params=SETTINGS)
def settings(request):
    return SETTINGS[request.param]


@pytest.fixture
def ctx():
    return hail_df.context()


def test_the_built_in_union_reads_every_input_table_as_its_scan_description(backend, dataset):
    plan_bytes = backend.builtin_plan(dataset, UNION)

    assert isinstance(plan_bytes, bytes)
    assert backend.scan_descriptions(plan_bytes) == [
        table.scan_description for table in dataset.input_tables
    ]


def test_the_python_and_built_in_unions_explain_to_the_same_physical_plan_with_no_sort(
    backend, dataset, ctx, settings
):
    union = formulations.union(ctx, dataset)
    action = Explain(union.ordering)

    from_python = backend.start(ctx, union.frame, action, settings=settings).wait()
    built_in = backend.start(
        ctx, backend.builtin_plan(dataset, union.formulation), action, settings=settings
    ).wait()

    physical_plan = plan_sections(from_python.text)["physical_plan"]
    assert physical_plan == plan_sections(built_in.text)["physical_plan"]
    # The comparison is of a merge tree held by the root assertion, not of two empty sections.
    assert "OrderingAssertionExec" in physical_plan
    assert "SortPreservingMergeExec" in physical_plan
    assert "DataSourceExec" in physical_plan
    assert "SortExec" not in from_python.text
    assert "SortExec" not in built_in.text


def test_the_python_and_built_in_unions_collect_the_same_rows_in_the_same_order(
    backend, dataset, ctx, settings
):
    union = formulations.union(ctx, dataset)
    action = Collect(union.ordering)

    from_python = backend.start(ctx, union.frame, action, settings=settings).wait().rows
    built_in = backend.start(
        ctx, backend.builtin_plan(dataset, union.formulation), action, settings=settings
    ).wait()

    assert built_in.rows.equals(from_python)
    samples = sum(len(sample_set) for sample_set in SAMPLE_SETS)
    assert from_python.num_rows == len(ROWS) * samples


@pytest.mark.parametrize(
    "formulation",
    [
        FormulationRecord("grouped-merge", groups=2),
        FormulationRecord("interval-merge", split_points="chr01:100"),
        FormulationRecord("no such formulation"),
        FormulationRecord("union", groups=2),
    ],
)
def test_any_other_formulation_raises_invalid_action(backend, dataset, formulation):
    with pytest.raises(InvalidAction, match=formulation.name):
        backend.builtin_plan(dataset, formulation)


def test_a_formulation_that_is_no_record_raises_type_error(backend, dataset):
    with pytest.raises(TypeError, match="FormulationRecord"):
        backend.builtin_plan(dataset, "union")
