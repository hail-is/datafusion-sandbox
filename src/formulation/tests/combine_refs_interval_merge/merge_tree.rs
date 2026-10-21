use super::{IntervalMerge, THREE_INTERVALS};
use crate::locus::LocusRepresentation;
use crate::pipeline;
use crate::sink::PartitionedSinkExec;
use crate::testing::{
    fixture::{self, SAMPLES},
    formulation_plans::{
        FORMATS, REPRESENTATIONS, dataset, drained_plan, drained_plan_and_ordering, file_sink_plan,
        mixed_dataset, shared_datasets,
    },
    plan_shape::PlanShape,
    session::hostile_config,
};

use datafusion::physical_plan::{
    ExecutionPlanProperties, coalesce_partitions::CoalescePartitionsExec, union::UnionExec,
};

use std::sync::Arc;

/// Through the partitioned file sink, the plan is the sink over a union of one merge per
/// interval, each merge over every input table's scan, with the sink keeping one partition per
/// interval: no coalesce, sort, repartition, or filter operator anywhere, over every shared
/// dataset in either format and representation, under the shared session and the hostile one.
/// The sink requires the formulation's ordering in the dataset's representation.
#[test]
fn writes_through_a_partitioned_sink_over_one_merge_per_interval() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                for hostile in [false, true] {
                    let context = format!(
                        "{format:?} {representation:?} {:?} hostile={hostile}",
                        dataset.input_tables
                    );
                    let config = if hostile {
                        hostile_config(8)
                    } else {
                        pipeline::session_config()
                    };
                    let (plan, ordering) = file_sink_plan(
                        &IntervalMerge::new(THREE_INTERVALS),
                        &dataset,
                        config,
                        None,
                    );
                    let shape = PlanShape::of(&plan);

                    shape.assert_ends_in_sink_requiring(&ordering);
                    let sink_nodes = shape.nodes_of::<PartitionedSinkExec>();
                    assert_eq!(sink_nodes.len(), 1, "{context}:\n{shape}");
                    assert_eq!(
                        plan.output_partitioning().partition_count(),
                        3,
                        "{context}:\n{shape}"
                    );
                    shape.assert_one_merge_per_interval(
                        3,
                        dataset.input_tables.len(),
                        representation,
                    );
                    let unions = shape.nodes_of::<UnionExec>();
                    let outer_union = unions
                        .first()
                        .expect("the interval plan contains a union of intervals");
                    assert!(
                        Arc::ptr_eq(plan.children()[0], outer_union),
                        "{context}: expected the union of intervals directly beneath the sink:\n{shape}"
                    );
                    assert!(
                        shape.nodes_of::<CoalescePartitionsExec>().is_empty(),
                        "{context}: expected no coalesce beneath the partitioned sink:\n{shape}"
                    );
                }
            }
        }
    }
}

/// Over a dataset of every kind of input table, each interval's merge is over one scan per input
/// table, and each scan keeps only its table's files whose ordering statistics overlap the
/// interval. The multi-sample file `g0` is one file spanning every interval, so every interval
/// keeps it. The multi-sample directory `g1` and the single-sample table hold files d, c, b and a,
/// pruned alike to the files written out per representation.
#[test]
fn prunes_each_input_table_of_a_mixed_dataset_to_the_files_of_each_interval() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            // The files of `g1` and of a single-sample table whose statistics overlap each of
            // `THREE_INTERVALS`. File c holds chr01:2 and chr01:3, b holds chr01:4 and chr02:1,
            // and a starts at chr02:2. Packed statistics bound b by its first and last locus.
            // Contig and position statistics bound it column by column, contig chr01 to chr02
            // and position 1 to 4, which admits a locus in every interval.
            let expected_stems: [&[&str]; 3] = match representation {
                LocusRepresentation::Packed => [&["d", "c"], &["c", "b"], &["a"]],
                LocusRepresentation::ContigPosition => [&["d", "c", "b"], &["c", "b"], &["b", "a"]],
            };
            let context = format!("{format:?} {representation:?}");
            let dataset = mixed_dataset(format, representation);
            let plan = drained_plan(
                &IntervalMerge::new(THREE_INTERVALS),
                &dataset,
                hostile_config(8),
                None,
            );
            let shape = PlanShape::of(&plan);

            shape.assert_one_merge_per_interval(
                3,
                fixture::MIXED_INPUT_TABLES.len(),
                representation,
            );
            let scanned_files = shape.scanned_files();
            let scans = scanned_files.chunks(fixture::MIXED_INPUT_TABLES.len());
            assert_eq!(scans.len(), 3, "{context}:\n{shape}");
            for (scans, expected_stems) in scans.zip(expected_stems) {
                let [g0, g1, single_sample] = scans else {
                    panic!("{context}: one scan per input table:\n{shape}");
                };
                assert_eq!(
                    g0.iter().map(|path| path.filename()).collect::<Vec<_>>(),
                    [Some(
                        format!("g0.{}", dataset.fixture.input_format().name()).as_str()
                    )],
                    "{context}:\n{shape}"
                );
                assert_eq!(
                    fixture::file_stems(g1),
                    expected_stems,
                    "{context}:\n{shape}"
                );
                assert_eq!(
                    fixture::file_stems(single_sample),
                    expected_stems,
                    "{context}:\n{shape}"
                );
            }
        }
    }
}

/// Through a single-partition sink, the interval merges feed one more merge above their union,
/// so collect and explain see global locus order without a sort, over every shared dataset. The
/// draining sink requires the formulation's ordering.
#[test]
fn a_single_partition_sink_merges_the_interval_merges() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                for hostile in [false, true] {
                    let config = if hostile {
                        hostile_config(8)
                    } else {
                        pipeline::session_config()
                    };
                    let (plan, ordering) = drained_plan_and_ordering(
                        &IntervalMerge::new(THREE_INTERVALS),
                        &dataset,
                        config,
                        None,
                    );
                    let shape = PlanShape::of(&plan);

                    let n_input_tables = dataset.input_tables.len();
                    shape.assert_merge_tree(&[n_input_tables; 3]);
                    shape.assert_one_merge_per_interval(3, n_input_tables, representation);
                    shape.assert_ends_in_sink_requiring(&ordering);
                }
            }
        }
    }
}

/// Restricting a shared dataset to its last two input tables leaves each interval's merge over
/// two scans.
#[test]
fn restricting_the_input_tables_changes_each_intervals_input_count() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in shared_datasets(format, representation) {
                let dataset = dataset.restricted_to_last(2);
                let plan = drained_plan(
                    &IntervalMerge::new(THREE_INTERVALS),
                    &dataset,
                    hostile_config(8),
                    None,
                );
                PlanShape::of(&plan).assert_one_merge_per_interval(3, 2, representation);
            }
        }
    }
}

/// A row limit promises one file, not a plan shape: the limit above the union of intervals forces
/// one partition, so the partitioned sink holds one partition sink and writes one file.
#[test]
fn a_row_limit_collapses_the_write_to_one_file() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            let (plan, _) = file_sink_plan(
                &IntervalMerge::new(THREE_INTERVALS),
                &dataset,
                hostile_config(8),
                Some(3),
            );
            let shape = PlanShape::of(&plan);
            let sink_nodes = shape.nodes_of::<PartitionedSinkExec>();
            assert_eq!(
                sink_nodes.len(),
                1,
                "{format:?} {representation:?}:\n{shape}"
            );
            assert!(
                Arc::ptr_eq(&sink_nodes[0], &plan),
                "{format:?} {representation:?}: expected the partitioned sink at the plan root:\n{shape}"
            );
            assert_eq!(
                plan.output_partitioning().partition_count(),
                1,
                "{format:?} {representation:?}:\n{shape}"
            );
        }
    }
}

/// With one split point there are two intervals and two files; with none, the plan is the union
/// formulation's under a sink of one partition, and a write is a directory of one file.
#[test]
fn one_split_point_gives_two_intervals_and_none_gives_one() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);

            let (two, _) = file_sink_plan(
                &IntervalMerge::new("1:3"),
                &dataset,
                hostile_config(8),
                None,
            );
            let two_shape = PlanShape::of(&two);
            assert_eq!(
                two.output_partitioning().partition_count(),
                2,
                "{format:?} {representation:?}:\n{two_shape}"
            );
            two_shape.assert_one_merge_per_interval(2, SAMPLES.len(), representation);

            let (one, _) =
                file_sink_plan(&IntervalMerge::new(""), &dataset, hostile_config(8), None);
            let one_shape = PlanShape::of(&one);
            let sink_nodes = one_shape.nodes_of::<PartitionedSinkExec>();
            assert_eq!(
                sink_nodes.len(),
                1,
                "{format:?} {representation:?}:\n{one_shape}"
            );
            assert!(
                Arc::ptr_eq(&sink_nodes[0], &one),
                "{format:?} {representation:?}: expected the partitioned sink at the plan root:\n{one_shape}"
            );
            assert_eq!(
                one.output_partitioning().partition_count(),
                1,
                "{format:?} {representation:?}:\n{one_shape}"
            );
            one_shape.assert_one_merge_per_interval(1, SAMPLES.len(), representation);
        }
    }
}
