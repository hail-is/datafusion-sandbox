"""`Backend.builtin_plan`, and its parity with the same formulation written in Python: the matrix
is every formulation with a built-in plan, both formats, the three settings maps, and the Actions
that have landed."""

import re

import hail_df
import pytest
from conftest import ROWS, SAMPLE_SETS, plan_sections, write_single_sample_dataset
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


def input_groups(physical_plan: str) -> list[list[str]]:
    """The input tables beneath each established order of `physical_plan`: each group's sort is one,
    and each input table's scan names its directory."""
    groups = []
    for line in physical_plan.splitlines():
        if "EstablishedOrderExec" in line:
            groups.append([])
        elif "DataSourceExec" in line:
            groups[-1].extend(re.findall(r"/samples/(s=\w+)/", line)[:1])
    return groups


@pytest.mark.parametrize(
    "record",
    [UNION, *(FormulationRecord("grouped-merge", groups=groups) for groups in [1, 2, 3, 8])],
    ids=lambda record: f"{record.name}-{record.groups}",
)
def test_a_built_in_plan_reads_every_input_table_as_its_scan_description(backend, dataset, record):
    plan_bytes = backend.builtin_plan(dataset, record)

    assert isinstance(plan_bytes, bytes)
    assert backend.scan_descriptions(plan_bytes) == [
        table.scan_description for table in dataset.input_tables
    ]


def test_the_python_and_built_in_plans_explain_to_the_same_physical_plan_with_no_sort(
    backend, dataset, ctx, formulation, settings
):
    ordered = formulation(ctx, dataset)
    action = Explain(ordered.ordering)

    from_python = backend.start(ctx, ordered.frame, action, settings=settings).wait()
    built_in = backend.start(
        ctx, backend.builtin_plan(dataset, ordered.formulation), action, settings=settings
    ).wait()

    physical_plan = plan_sections(from_python.text)["physical_plan"]
    assert physical_plan == plan_sections(built_in.text)["physical_plan"]
    # The comparison is of a merge tree held by the root assertion, not of two empty sections.
    assert "OrderingAssertionExec" in physical_plan
    assert "SortPreservingMergeExec" in physical_plan
    assert "DataSourceExec" in physical_plan
    assert "SortExec" not in from_python.text
    assert "SortExec" not in built_in.text


def test_the_python_and_built_in_plans_collect_the_same_rows_in_the_same_order(
    backend, dataset, ctx, formulation, settings
):
    ordered = formulation(ctx, dataset)
    action = Collect(ordered.ordering)

    from_python = backend.start(ctx, ordered.frame, action, settings=settings).wait().rows
    built_in = backend.start(
        ctx, backend.builtin_plan(dataset, ordered.formulation), action, settings=settings
    ).wait()

    assert built_in.rows.equals(from_python)
    samples = sum(len(sample_set) for sample_set in SAMPLE_SETS)
    assert from_python.num_rows == len(ROWS) * samples


# Worked by hand from the spec: contiguous, count-balanced, larger groups first, and clamped to one
# input table per group.
@pytest.mark.parametrize(
    ("groups", "expected"),
    [
        (1, [["a", "b", "c", "d", "e"]]),
        (2, [["a", "b", "c"], ["d", "e"]]),
        (3, [["a", "b"], ["c", "d"], ["e"]]),
        (4, [["a", "b"], ["c"], ["d"], ["e"]]),
        (5, [["a"], ["b"], ["c"], ["d"], ["e"]]),
        (8, [["a"], ["b"], ["c"], ["d"], ["e"]]),
    ],
)
def test_the_python_and_built_in_plans_split_the_input_tables_into_the_same_input_groups(
    backend, tmp_path, input_format, ctx, groups, expected
):
    path = write_single_sample_dataset(tmp_path, input_format, ["a", "b", "c", "d", "e"])
    dataset = backend.open_dataset(str(path), input_format, "locus,alleles")
    grouped = formulations.grouped_merge(ctx, dataset, groups)
    action = Explain(grouped.ordering)

    from_python = backend.start(ctx, grouped.frame, action).wait()
    built_in = backend.start(ctx, backend.builtin_plan(dataset, grouped.formulation), action).wait()

    physical_plan = plan_sections(from_python.text)["physical_plan"]
    assert input_groups(physical_plan) == [
        [f"s={sample}" for sample in group] for group in expected
    ]
    assert physical_plan == plan_sections(built_in.text)["physical_plan"]


def test_one_group_plans_as_the_union_formulation(backend, dataset, ctx, settings):
    union = formulations.union(ctx, dataset)
    one_group = formulations.grouped_merge(ctx, dataset, 1)
    action = Explain(union.ordering)

    expected = backend.start(ctx, union.frame, action, settings=settings).wait().text

    from_python = backend.start(ctx, one_group.frame, action, settings=settings).wait()
    built_in = backend.start(
        ctx, backend.builtin_plan(dataset, one_group.formulation), action, settings=settings
    ).wait()
    assert from_python.text == expected
    assert built_in.text == expected


@pytest.mark.parametrize(
    "formulation",
    [
        FormulationRecord("grouped-merge"),
        FormulationRecord("grouped-merge", groups=0),
        FormulationRecord("grouped-merge", groups=-1),
        FormulationRecord("grouped-merge", groups=2**64),
        FormulationRecord("grouped-merge", groups=2, split_points="chr01:100"),
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
