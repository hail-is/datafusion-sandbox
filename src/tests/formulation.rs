//! Tests of the formulations as a whole: what every formulation keeps of its plan shape, and what
//! `Formulation` itself reports about each one.
//!
//! What matters about these plans is not what they return but how they get
//! there: a sort-preserving merge over one partition per sample, or one per
//! input group with a merge per group beneath it, and no re-sort. A regression
//! from merging to re-sorting is invisible in the results and costs an order of
//! magnitude in time, so it is asserted structurally here. Every plan is
//! observed through a sink, as `formulation_plans` explains.
//!
//! The tests here build unfiltered plans of the single-file formulations, and
//! `filtered_plans` holds the same formulations under a caller's contig or
//! locus-interval filter. What is particular to one formulation belongs to its
//! plan builder's suite, under `formulation`'s own `tests` subtree.

mod filtered_plans;

use crate::{
    formulation::Formulation,
    locus::LocusRepresentation,
    row_ordering::RowOrdering,
    stored::dataset::InputTable,
    testing::fixture::{FixtureFormat, block_on},
    tests::{
        formulation_plans::{
            FORMATS, FixtureDataset, REPRESENTATIONS, dataset, dataset_with_ordering,
            drained_plan_and_ordering, file_sink_plan, physical_plan, physical_plan_with_target,
            planned, shared_datasets,
        },
        plan_shape::PlanShape,
        support::{grouped_merge, hostile_config, interval_merge},
    },
};
use datafusion::{
    common::DataFusionError, datasource::sink::DataSinkExec, prelude::SessionContext,
};

use std::num::NonZeroUsize;

/// The group count the shared loops run grouped-merge with: two groups over the fixture's four
/// samples, so every group holds more than one sample and there is more than one group.
const GROUPS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

/// The formulations the shared loops cover: every one that writes a single file. A function
/// rather than a constant because a formulation may hold split points, so it is not `Copy`.
fn formulations() -> [Formulation; 3] {
    [
        Formulation::CombineRefsUnion,
        Formulation::CombineRefsGroupedMerge { groups: GROUPS },
        Formulation::CombineAllelesUnion,
    ]
}

#[test]
fn rejects_a_dataset_with_an_insufficient_row_ordering() {
    let dataset = dataset_with_ordering(
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
        RowOrdering::locus(),
    );
    let ctx = SessionContext::new();
    dataset.fixture.register(&ctx);

    let error = block_on(Formulation::CombineAllelesUnion.plan(&ctx, &dataset.dataset))
        .expect_err("the allele formulation requires alleles ordering");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("row ordering"));
}

/// Over every shared dataset, each formulation merges each input table as one ordered partition,
/// grouped-merge forms its groups from input tables, and the draining sink requires the
/// formulation's ordering.
#[test]
fn formulations_keep_their_plan_shape_under_a_hostile_session() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                for formulation in &formulations_over(&dataset) {
                    let (plan, ordering) =
                        drained_plan_and_ordering(formulation, &dataset, hostile_config(8), None);
                    let shape = PlanShape::of(&plan);
                    shape.assert_merge_tree(&expected_groups(
                        formulation,
                        dataset.input_tables.len(),
                    ));
                    shape.assert_ends_in_sink_requiring(&ordering);
                }
            }
        }
    }
}

/// The file sink holds the same merge tree as the draining sink, and requires the formulation's
/// ordering in the dataset's representation, in both output formats.
#[test]
fn formulations_keep_their_plan_shape_through_the_file_sink() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                for formulation in &formulations_over(&dataset) {
                    let (plan, ordering) =
                        file_sink_plan(formulation, &dataset, hostile_config(8), None);
                    let shape = PlanShape::of(&plan);
                    assert!(plan.is::<DataSinkExec>(), "{shape}");
                    shape.assert_merge_tree(&expected_groups(
                        formulation,
                        dataset.input_tables.len(),
                    ));
                    shape.assert_ends_in_sink_requiring(&ordering);
                }
            }
        }
    }
}

#[test]
fn target_partitions_do_not_introduce_sorts_into_either_formulation() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                for formulation in &formulations_over(&dataset) {
                    let single_target = physical_plan_with_target(formulation, &dataset, 1);
                    let eight_targets = physical_plan_with_target(formulation, &dataset, 8);
                    PlanShape::of(&single_target).assert_no_sorts();
                    PlanShape::of(&eight_targets).assert_no_sorts();
                }
            }
        }
    }
}

/// Restricting a shared dataset to its last two input tables merges two inputs. Over the mixed
/// dataset those are one of each kind, the multi-sample one still one input.
#[test]
fn restricting_the_input_tables_changes_input_count_for_every_formulation() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                let formulations = formulations_over(&dataset);
                let dataset = dataset.restricted_to_last(2);
                for formulation in &formulations {
                    let plan = physical_plan(formulation, &dataset);
                    PlanShape::of(&plan).assert_merge_tree(&expected_groups(formulation, 2));
                }
            }
        }
    }
}

/// The input groups a formulation's plan should merge over `n_input_tables`, as their sizes,
/// counted in input tables, in name order. Written out rather than computed, so the test cannot
/// agree with a wrong split. Interval-merge merges every input table once per locus interval; its
/// plan builder's suite spells that out.
fn expected_groups(formulation: &Formulation, n_input_tables: usize) -> Vec<usize> {
    match (formulation, n_input_tables) {
        (Formulation::CombineRefsGroupedMerge { groups }, 4) if *groups == GROUPS => vec![2, 2],
        (Formulation::CombineRefsGroupedMerge { groups }, 3) if *groups == GROUPS => vec![2, 1],
        (Formulation::CombineRefsGroupedMerge { groups }, 2) if *groups == GROUPS => vec![1, 1],
        (Formulation::CombineRefsGroupedMerge { groups }, n) => {
            panic!("no expected groups for {groups} groups over {n} input tables")
        }
        (Formulation::CombineRefsIntervalMerge { split_points }, n) => {
            panic!("no expected groups for split points {split_points} over {n} input tables")
        }
        (Formulation::CombineRefsUnion | Formulation::CombineAllelesUnion, n) => vec![n],
    }
}

/// The reference combiner's formulations among the shared list.
fn reference_formulations() -> [Formulation; 2] {
    let [union, grouped_merge, _] = formulations();
    [union, grouped_merge]
}

/// The shared formulations that plan over `dataset`: every one, or only the reference combiner's
/// if the dataset holds a multi-sample input table, which the allele combiner rejects.
fn formulations_over(dataset: &FixtureDataset) -> Vec<Formulation> {
    if dataset
        .dataset
        .input_tables()
        .iter()
        .any(InputTable::is_multi_sample)
    {
        reference_formulations().to_vec()
    } else {
        formulations().to_vec()
    }
}

#[test]
fn displays_each_formulation_by_its_command_line_name() {
    assert_eq!(Formulation::CombineRefsUnion.to_string(), "union");
    assert_eq!(Formulation::CombineAllelesUnion.to_string(), "union");
    assert_eq!(grouped_merge(3).to_string(), "grouped-merge");
    assert_eq!(interval_merge("1:3,2:2").to_string(), "interval-merge");
}

#[test]
fn every_reference_formulation_requires_the_reference_combiners_ordering() {
    for formulation in [grouped_merge(3), interval_merge("1:3,2:2")] {
        assert_eq!(
            formulation.required_ordering(),
            Formulation::CombineRefsUnion.required_ordering(),
            "{formulation}"
        );
    }
}

/// The output layout a formulation reports before planning, which a run checks its output path
/// against, is the layout its planned frame carries.
#[test]
fn each_formulation_plans_the_output_layout_it_reports() {
    let dataset = dataset(FixtureFormat::Vortex, LocusRepresentation::ContigPosition);
    for formulation in [
        Formulation::CombineRefsUnion,
        Formulation::CombineAllelesUnion,
        grouped_merge(2),
        interval_merge("1:3,2:2"),
    ] {
        let (_, ordered) = block_on(planned(&formulation, &dataset, hostile_config(8))).unwrap();
        assert_eq!(
            ordered.layout,
            formulation.output_layout(),
            "{formulation:?}"
        );
    }
}
