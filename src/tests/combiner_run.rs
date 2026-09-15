use crate::fixture;

use crate::{
    combiner_run::{Action, CombinerRun, Outcome, WriteTarget},
    format::{InputFormat, OutputFormat},
    formulation::Formulation,
    locus::LocusRepresentation,
};
use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    error::Result,
    parquet::{
        basic::Compression,
        file::{
            metadata::RowGroupMetaData,
            reader::{FileReader, SerializedFileReader},
        },
    },
};
use std::{num::NonZeroUsize, path::Path, sync::Arc};

use fixture::{FixtureFormat, SAMPLES};

#[test]
fn renders_outcomes() {
    assert_eq!(Outcome::RowsWritten(42).render().unwrap(), "42");
    assert_eq!(
        Outcome::Plan("physical plan".to_string()).render().unwrap(),
        "physical plan"
    );

    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let batch = RecordBatch::try_from_iter(vec![("idx", values)]).unwrap();
    let rendered = Outcome::Batches(vec![batch]).render().unwrap();
    assert!(rendered.contains("| 1   |"), "rendered batch:\n{rendered}");
    assert!(rendered.contains("| 2   |"), "rendered batch:\n{rendered}");
}

#[test]
fn renders_collected_contig_position_rows() {
    let dataset = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);

    let rendered = run(
        Formulation::CombineAllelesUnion,
        dataset.table_path(),
        dataset.input_format(),
        Action::Collect,
        None,
        None,
    )
    .unwrap()
    .render()
    .unwrap();

    assert!(
        rendered.contains("| contig | position | alleles |"),
        "rendered outcome:\n{rendered}"
    );
    assert!(
        rendered.contains("| chr1   | 1        | A,G     | 1"),
        "rendered outcome:\n{rendered}"
    );
}

#[test]
fn renders_collected_packed_rows() {
    let dataset = fixture::packed_disk_fixture(FixtureFormat::Vortex);

    let rendered = run(
        Formulation::CombineAllelesUnion,
        dataset.table_path(),
        dataset.input_format(),
        Action::Collect,
        None,
        None,
    )
    .unwrap()
    .render()
    .unwrap();

    assert!(
        rendered.contains("| locus      | alleles |"),
        "rendered outcome:\n{rendered}"
    );
    assert!(
        rendered.contains("| 4294967297 | A,G     | 1"),
        "rendered outcome:\n{rendered}"
    );
}

#[test]
fn both_combiners_report_rows_written() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);

    for (formulation, output_name, expected) in [
        (Formulation::CombineRefsUnion, "combined_refs.vortex", 32),
        (
            Formulation::CombineAllelesUnion,
            "combined_alleles.vortex",
            8,
        ),
    ] {
        let outcome = run(
            formulation,
            input.table_path(),
            input.input_format(),
            Action::Write(WriteTarget {
                output_path: dir.path().join(output_name).to_str().unwrap().to_string(),
                output_format: OutputFormat::VORTEX,
            }),
            None,
            None,
        )
        .unwrap();

        let Outcome::RowsWritten(rows) = outcome else {
            panic!("expected rows written, got {outcome:?}");
        };
        assert_eq!(rows, expected);
    }
}

#[test]
fn writes_uncompressed_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let output_path = dir.path().join("combined_refs.parquet");
    let outcome = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Write(WriteTarget {
            output_path: output_path.to_str().unwrap().to_string(),
            output_format: OutputFormat::PARQUET
                .with_compression("uncompressed")
                .unwrap(),
        }),
        None,
        None,
    )
    .unwrap();

    let Outcome::RowsWritten(rows) = outcome else {
        panic!("expected rows written, got {outcome:?}");
    };
    assert_eq!(rows, 32);
    let reader = SerializedFileReader::try_from(output_path.as_path()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 32);
    assert!(
        reader
            .metadata()
            .row_groups()
            .iter()
            .flat_map(RowGroupMetaData::columns)
            .all(|column| column.compression() == Compression::UNCOMPRESSED)
    );
}

#[test]
fn compact_and_standard_vortex_have_different_file_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let sizes = ["standard", "compact"].map(|compression| {
        let output_path = dir
            .path()
            .join(format!("combined_refs_{compression}.vortex"));
        let outcome = run(
            Formulation::CombineRefsUnion,
            input.table_path(),
            input.input_format(),
            Action::Write(WriteTarget {
                output_path: output_path.to_str().unwrap().to_string(),
                output_format: OutputFormat::VORTEX.with_compression(compression).unwrap(),
            }),
            None,
            None,
        )
        .unwrap();
        let Outcome::RowsWritten(rows) = outcome else {
            panic!("expected rows written, got {outcome:?}");
        };
        assert_eq!(rows, 32);
        output_path.metadata().unwrap().len()
    });

    assert_ne!(sizes[0], sizes[1], "standard and compact file sizes match");
}

#[test]
fn collects_ordered_alleles_with_ranks_across_file_cuts() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let input = match representation {
                LocusRepresentation::ContigPosition => {
                    fixture::contig_position_disk_fixture(format)
                }
                LocusRepresentation::Packed => fixture::packed_disk_fixture(format),
            };
            let batches = expect_batches(
                run(
                    Formulation::CombineAllelesUnion,
                    input.table_path(),
                    input.input_format(),
                    Action::Collect,
                    None,
                    None,
                )
                .unwrap(),
            );
            let rows = batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows()).map(|row| {
                        batch
                            .columns()
                            .iter()
                            .map(|column| array_value_to_string(column, row).unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            let expected = match representation {
                LocusRepresentation::ContigPosition => vec![
                    vec!["chr1", "1", "A,G", "1"],
                    vec!["chr1", "2", "A,C", "1"],
                    vec!["chr1", "2", "A,G", "2"],
                    vec!["chr1", "3", "A,C", "1"],
                    vec!["chr1", "4", "A,G", "1"],
                    vec!["chr2", "1", "A,C", "1"],
                    vec!["chr2", "2", "A,G", "1"],
                    vec!["chr2", "3", "A,C", "1"],
                ],
                LocusRepresentation::Packed => vec![
                    vec!["4294967297", "A,G", "1"],
                    vec!["4294967298", "A,C", "1"],
                    vec!["4294967298", "A,G", "2"],
                    vec!["4294967299", "A,C", "1"],
                    vec!["4294967300", "A,G", "1"],
                    vec!["8589934593", "A,C", "1"],
                    vec!["8589934594", "A,G", "1"],
                    vec!["8589934595", "A,C", "1"],
                ],
            };
            assert_eq!(rows, expected);
        }
    }
}

/// Both reference combiner formulations collect the same rows in locus order; grouped-merge
/// only does so because collect runs through a sink that requires the ordering.
#[test]
fn collects_references_in_locus_order_across_file_cuts() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            for formulation in [Formulation::CombineRefsUnion, grouped_merge(2)] {
                let input = match representation {
                    LocusRepresentation::ContigPosition => {
                        fixture::contig_position_disk_fixture(format)
                    }
                    LocusRepresentation::Packed => fixture::packed_disk_fixture(format),
                };
                let batches = expect_batches(
                    run(
                        formulation,
                        input.table_path(),
                        input.input_format(),
                        Action::Collect,
                        None,
                        None,
                    )
                    .unwrap(),
                );
                let columns = match representation {
                    LocusRepresentation::ContigPosition => vec!["contig", "position"],
                    LocusRepresentation::Packed => vec!["locus"],
                };
                let loci = batches
                    .iter()
                    .flat_map(|batch| {
                        let columns = &columns;
                        (0..batch.num_rows()).map(move |row| {
                            columns
                                .iter()
                                .map(|name| {
                                    array_value_to_string(batch.column_by_name(name).unwrap(), row)
                                        .unwrap()
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect::<Vec<_>>();
                // The reference combiner orders only by locus, not alleles or sample id.
                let expected = match representation {
                    LocusRepresentation::ContigPosition => vec![
                        vec!["chr1", "1"],
                        vec!["chr1", "2"],
                        vec!["chr1", "2"],
                        vec!["chr1", "3"],
                        vec!["chr1", "4"],
                        vec!["chr2", "1"],
                        vec!["chr2", "2"],
                        vec!["chr2", "3"],
                    ],
                    LocusRepresentation::Packed => vec![
                        vec!["4294967297"],
                        vec!["4294967298"],
                        vec!["4294967298"],
                        vec!["4294967299"],
                        vec!["4294967300"],
                        vec!["8589934593"],
                        vec!["8589934594"],
                        vec!["8589934595"],
                    ],
                }
                .into_iter()
                .flat_map(|locus| std::iter::repeat_n(locus, SAMPLES.len()))
                .collect::<Vec<_>>();
                assert_eq!(
                    loci, expected,
                    "{format:?} {representation:?} {formulation:?}"
                );
            }
        }
    }
}

#[test]
fn explicit_limit_applies_under_every_action() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);

    let batches = expect_batches(
        run(
            Formulation::CombineAllelesUnion,
            input.table_path(),
            input.input_format(),
            Action::Collect,
            None,
            Some(1),
        )
        .unwrap(),
    );
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let outcome = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Write(WriteTarget {
            output_path: dir
                .path()
                .join("limited.vortex")
                .to_str()
                .unwrap()
                .to_string(),
            output_format: OutputFormat::VORTEX,
        }),
        None,
        Some(3),
    )
    .unwrap();
    let Outcome::RowsWritten(rows) = outcome else {
        panic!("expected rows written, got {outcome:?}");
    };
    assert_eq!(rows, 3);

    for action in [
        Action::Explain { write: None },
        Action::ExplainAnalyze { write: None },
    ] {
        let outcome = run(
            Formulation::CombineAllelesUnion,
            input.table_path(),
            input.input_format(),
            action,
            None,
            Some(1),
        )
        .unwrap();
        let Outcome::Plan(plan) = outcome else {
            panic!("expected a plan, got {outcome:?}");
        };
        assert!(plan.contains("fetch=1"), "plan:\n{plan}");
    }
}

#[test]
fn explain_actions_return_plain_and_analyzed_plans() {
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);

    let outcome = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Explain { write: None },
        None,
        None,
    )
    .unwrap();
    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(plan.contains("physical_plan"), "plan:\n{plan}");
    assert!(!plan.contains("LimitExec"), "plan:\n{plan}");
    assert!(
        plan.contains("DataSinkExec: sink=DrainingSink"),
        "plan:\n{plan}"
    );

    let outcome = run(
        Formulation::CombineAllelesUnion,
        input.table_path(),
        input.input_format(),
        Action::ExplainAnalyze { write: None },
        None,
        None,
    )
    .unwrap();
    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(plan.contains("Plan with Metrics"), "plan:\n{plan}");
    assert!(plan.contains("elapsed_compute"), "plan:\n{plan}");
    assert!(!plan.contains("LimitExec"), "plan:\n{plan}");
    assert!(
        plan.contains("DataSinkExec: sink=DrainingSink"),
        "plan:\n{plan}"
    );
}

/// An explain renders the plan the action would execute: with a write, the file sink's plan,
/// and the plain explain writes nothing.
#[test]
fn explain_with_a_write_renders_the_file_sink_plan_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let output_path = dir.path().join("explained.vortex");

    let outcome = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Explain {
            write: Some(WriteTarget {
                output_path: output_path.to_str().unwrap().to_string(),
                output_format: OutputFormat::VORTEX,
            }),
        },
        None,
        None,
    )
    .unwrap();

    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(
        plan.contains("DataSinkExec: sink=VortexSink"),
        "plan:\n{plan}"
    );
    assert!(!plan.contains("DrainingSink"), "plan:\n{plan}");
    assert!(
        !output_path.exists(),
        "explain wrote {}",
        output_path.display()
    );
}

#[test]
fn explain_analyze_with_a_write_performs_the_write() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let output_path = dir.path().join("analyzed.parquet");

    let outcome = run(
        grouped_merge(2),
        input.table_path(),
        input.input_format(),
        Action::ExplainAnalyze {
            write: Some(WriteTarget {
                output_path: output_path.to_str().unwrap().to_string(),
                output_format: OutputFormat::PARQUET,
            }),
        },
        None,
        None,
    )
    .unwrap();

    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(plan.contains("Plan with Metrics"), "plan:\n{plan}");
    assert!(plan.contains("DataSinkExec"), "plan:\n{plan}");
    let reader = SerializedFileReader::try_from(output_path.as_path()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 32);
}

/// On stored files, grouped-merge over two groups shows one merge per group beneath the final
/// merge and no sort, whether the frame ends in the draining sink or the file sink, and the
/// plain explain lists the operators the analyzed run executed.
#[test]
fn grouped_merge_explains_a_merge_per_group_beneath_the_final_merge() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let write = || WriteTarget {
        output_path: dir
            .path()
            .join("grouped.vortex")
            .to_str()
            .unwrap()
            .to_string(),
        output_format: OutputFormat::VORTEX,
    };

    for (explain, analyze) in [
        (
            Action::Explain { write: None },
            Action::ExplainAnalyze { write: None },
        ),
        (
            Action::Explain {
                write: Some(write()),
            },
            Action::ExplainAnalyze {
                write: Some(write()),
            },
        ),
    ] {
        let context = format!("{explain:?}");
        let explained = expect_plan(
            run(
                grouped_merge(2),
                input.table_path(),
                input.input_format(),
                explain,
                None,
                None,
            )
            .unwrap(),
        );
        let analyzed = expect_plan(
            run(
                grouped_merge(2),
                input.table_path(),
                input.input_format(),
                analyze,
                None,
                None,
            )
            .unwrap(),
        );

        assert_eq!(
            explained.matches("SortPreservingMergeExec").count(),
            3,
            "{context}:\n{explained}"
        );
        assert!(!explained.contains("SortExec"), "{context}:\n{explained}");
        assert_eq!(exec_names(&explained), exec_names(&analyzed), "{context}");
    }
}

/// The execution plan node names in a rendered plan, in display order.
fn exec_names(plan: &str) -> Vec<&str> {
    plan.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| word.ends_with("Exec"))
        .collect()
}

fn grouped_merge(groups: usize) -> Formulation {
    Formulation::CombineRefsGroupedMerge {
        groups: NonZeroUsize::new(groups).unwrap(),
    }
}

fn expect_plan(outcome: Outcome) -> String {
    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    plan
}

#[test]
fn restricts_the_dataset_to_the_requested_sample_set() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let output_path = dir.path().join("restricted.vortex");

    let outcome = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Write(WriteTarget {
            output_path: output_path.to_str().unwrap().to_string(),
            output_format: OutputFormat::VORTEX,
        }),
        Some(SAMPLES[..2].iter().map(ToString::to_string).collect()),
        None,
    )
    .unwrap();

    let Outcome::RowsWritten(rows) = outcome else {
        panic!("expected rows written, got {outcome:?}");
    };
    assert_eq!(rows, 16);
}

#[test]
fn reports_a_dataset_with_no_samples() {
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let input_path = Path::new(input.table_path()).join("no-samples");

    let err = run(
        Formulation::CombineRefsUnion,
        input_path.to_str().unwrap(),
        input.input_format(),
        Action::Collect,
        None,
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        format!(
            "Error during planning: dataset 'file://{}/' contains no samples",
            input_path.display()
        )
    );
}

#[test]
fn reports_sample_ids_absent_from_the_dataset() {
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);

    let err = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Collect,
        Some(vec!["NOT_A_SAMPLE".to_string()]),
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Error during planning: samples not found in dataset: NOT_A_SAMPLE"
    );
}

fn run(
    formulation: Formulation,
    input_path: &str,
    input_format: InputFormat,
    action: Action,
    sample_set: Option<Vec<String>>,
    row_limit: Option<usize>,
) -> Result<Outcome> {
    CombinerRun {
        formulation,
        input_path: input_path.to_string(),
        input_format,
        action,
        sample_set,
        row_limit,
        threads: 1,
    }
    .execute()
}

fn expect_batches(outcome: Outcome) -> Vec<RecordBatch> {
    let Outcome::Batches(batches) = outcome else {
        panic!("expected batches, got {outcome:?}");
    };
    batches
}
