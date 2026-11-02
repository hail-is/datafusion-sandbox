import hail_df
from conftest import ROWS, SAMPLE_SETS
from hail_df import Collect, formulations
from hail_df.formulations import FormulationRecord, OutputLayout

SAMPLES = [sample for sample_set in SAMPLE_SETS for sample in sample_set]


def test_collecting_the_union_returns_every_row_of_every_input_table_in_the_dataset_s_ordering(
    backend, dataset
):
    ctx = hail_df.context()
    union = formulations.union(ctx, dataset)

    rows = backend.start(ctx, union.frame, Collect(union.ordering)).wait().rows.to_pylist()

    # The fixture's rows are literals in locus-then-alleles order, and every sample holds them all.
    assert [(row["contig"], row["position"], row["alleles"]) for row in rows] == [
        row for row in ROWS for _ in SAMPLES
    ]
    assert sorted((row["contig"], row["position"], row["alleles"], row["s"]) for row in rows) == (
        sorted((*row, sample) for row in ROWS for sample in SAMPLES)
    )


def test_the_union_is_held_in_the_ordering_the_dataset_states(backend, mixed_dataset, input_format):
    dataset = backend.open_dataset(str(mixed_dataset), input_format, "locus")
    ctx = hail_df.context()
    union = formulations.union(ctx, dataset)

    assert union.ordering == dataset.ordering

    rows = backend.start(ctx, union.frame, Collect(union.ordering)).wait().rows.to_pylist()
    assert [(row["contig"], row["position"]) for row in rows] == [
        (contig, position) for contig, position, _ in ROWS for _ in SAMPLES
    ]


def test_the_union_states_its_output_layout_and_formulation_record(dataset):
    union = formulations.union(hail_df.context(), dataset)

    assert union.layout is OutputLayout.SINGLE_FILE
    assert union.formulation == FormulationRecord("union", groups=None, split_points=None)
