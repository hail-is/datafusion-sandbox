use crate::fixture;

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
use datafusion_sandbox::{
    combiner_run::{Action, CombinerRun, Outcome},
    format::{InputFormat, OutputFormat},
    formulation::Formulation,
    locus::LocusRepresentation,
};
use std::{path::Path, sync::Arc};

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
            Action::Write {
                output_path: dir.path().join(output_name).to_str().unwrap().to_string(),
                output_format: OutputFormat::VORTEX,
            },
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
        Action::Write {
            output_path: output_path.to_str().unwrap().to_string(),
            output_format: OutputFormat::PARQUET
                .with_compression("uncompressed")
                .unwrap(),
        },
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
            Action::Write {
                output_path: output_path.to_str().unwrap().to_string(),
                output_format: OutputFormat::VORTEX.with_compression(compression).unwrap(),
            },
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

#[test]
fn collects_references_in_locus_order_across_file_cuts() {
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
                    Formulation::CombineRefsUnion,
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
            assert_eq!(loci, expected);
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
        Action::Write {
            output_path: dir
                .path()
                .join("limited.vortex")
                .to_str()
                .unwrap()
                .to_string(),
            output_format: OutputFormat::VORTEX,
        },
        None,
        Some(3),
    )
    .unwrap();
    let Outcome::RowsWritten(rows) = outcome else {
        panic!("expected rows written, got {outcome:?}");
    };
    assert_eq!(rows, 3);

    for action in [Action::Explain, Action::ExplainAnalyze] {
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
        Action::Explain,
        None,
        None,
    )
    .unwrap();
    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(plan.contains("physical_plan"), "plan:\n{plan}");
    assert!(!plan.contains("LimitExec"), "plan:\n{plan}");

    let outcome = run(
        Formulation::CombineAllelesUnion,
        input.table_path(),
        input.input_format(),
        Action::ExplainAnalyze,
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
        Action::Write {
            output_path: output_path.to_str().unwrap().to_string(),
            output_format: OutputFormat::VORTEX,
        },
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
