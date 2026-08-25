mod fixture;

use datafusion::{
    arrow::{array::StringArray, record_batch::RecordBatch},
    error::Result,
    parquet::{
        basic::Compression,
        file::reader::{FileReader, SerializedFileReader},
    },
};
use datafusion_sandbox::{
    Formulation,
    combiner_run::{Action, CombinerRun, Outcome},
    format::{InputFormat, OutputFormat},
};
use std::fs;

use fixture::SAMPLES;

#[test]
fn both_combiners_report_rows_written() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    for (formulation, output_name, expected) in [
        (Formulation::CombineRefsUnion, "combined_refs.vortex", 400),
        (
            Formulation::CombineAllelesUnion,
            "combined_alleles.vortex",
            8,
        ),
    ] {
        let outcome = run(
            formulation,
            &input,
            InputFormat::VORTEX,
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
fn one_scan_reference_formulation_supports_every_action() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    let outcome = run(
        Formulation::CombineRefsOneScan,
        &input,
        InputFormat::VORTEX,
        Action::Write {
            output_path: dir
                .path()
                .join("combined_refs_one_scan.vortex")
                .to_str()
                .unwrap()
                .to_string(),
            output_format: OutputFormat::VORTEX,
        },
        None,
        None,
    )
    .unwrap();
    let Outcome::RowsWritten(rows) = outcome else {
        panic!("expected rows written, got {outcome:?}");
    };
    assert_eq!(rows, 400);

    let batches = expect_batches(
        run(
            Formulation::CombineRefsOneScan,
            &input,
            InputFormat::VORTEX,
            Action::Collect,
            None,
            Some(20),
        )
        .unwrap(),
    );
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 20);

    let outcome = run(
        Formulation::CombineRefsOneScan,
        &input,
        InputFormat::VORTEX,
        Action::Explain,
        None,
        None,
    )
    .unwrap();
    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(plan.contains("SortPreservingMergeExec"), "plan:\n{plan}");
    assert!(!plan.contains("SortExec:"), "plan:\n{plan}");

    let outcome = run(
        Formulation::CombineRefsOneScan,
        &input,
        InputFormat::VORTEX,
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
}

#[test]
fn writes_uncompressed_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);
    let output_path = dir.path().join("combined_refs.parquet");
    let outcome = run(
        Formulation::CombineRefsUnion,
        &input,
        InputFormat::VORTEX,
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
    assert_eq!(rows, 400);
    let reader = SerializedFileReader::try_from(output_path.as_path()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 400);
    assert!(
        reader
            .metadata()
            .row_groups()
            .iter()
            .flat_map(|row_group| row_group.columns())
            .all(|column| column.compression() == Compression::UNCOMPRESSED)
    );
}

#[test]
fn compact_and_standard_vortex_have_different_file_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);
    let sizes = ["standard", "compact"].map(|compression| {
        let output_path = dir
            .path()
            .join(format!("combined_refs_{compression}.vortex"));
        let outcome = run(
            Formulation::CombineRefsUnion,
            &input,
            InputFormat::VORTEX,
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
        assert_eq!(rows, 400);
        output_path.metadata().unwrap().len()
    });

    assert_ne!(sizes[0], sizes[1], "standard and compact file sizes match");
}

#[test]
fn reads_parquet_dataset_into_record_batches() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_parquet_sample_tables(dir.path(), SAMPLES);

    let batches = expect_batches(
        run(
            Formulation::CombineAllelesUnion,
            &input,
            InputFormat::PARQUET,
            Action::Collect,
            None,
            None,
        )
        .unwrap(),
    );

    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 8);
    let first = batches.first().unwrap();
    let alleles = first
        .column_by_name("alleles")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(alleles.value(0), "A,G");
}

#[test]
fn explicit_limit_applies_under_every_action() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    let batches = expect_batches(
        run(
            Formulation::CombineAllelesUnion,
            &input,
            InputFormat::VORTEX,
            Action::Collect,
            None,
            Some(1),
        )
        .unwrap(),
    );
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let outcome = run(
        Formulation::CombineRefsUnion,
        &input,
        InputFormat::VORTEX,
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
            &input,
            InputFormat::VORTEX,
            action,
            None,
            Some(1),
        )
        .unwrap();
        let Outcome::Plan(plan) = outcome else {
            panic!("expected a plan, got {outcome:?}");
        };
        assert!(
            plan.contains("GlobalLimitExec: skip=0, fetch=1"),
            "plan:\n{plan}"
        );
    }
}

#[test]
fn explain_actions_return_plain_and_analyzed_plans() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    let outcome = run(
        Formulation::CombineRefsUnion,
        &input,
        InputFormat::VORTEX,
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
        &input,
        InputFormat::VORTEX,
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
    let input = fixture::write_sample_tables(dir.path(), &SAMPLES[..4]);
    let output_path = dir.path().join("restricted.vortex");

    let outcome = run(
        Formulation::CombineRefsUnion,
        &input,
        InputFormat::VORTEX,
        Action::Write {
            output_path: output_path.to_str().unwrap().to_string(),
            output_format: OutputFormat::VORTEX,
        },
        Some(
            SAMPLES[..2]
                .iter()
                .map(|sample| sample.to_string())
                .collect(),
        ),
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
    let dir = tempfile::tempdir().unwrap();
    let input_path = dir.path().join("samples");
    fs::create_dir(&input_path).unwrap();

    let err = run(
        Formulation::CombineRefsUnion,
        input_path.to_str().unwrap(),
        InputFormat::VORTEX,
        Action::Collect,
        None,
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        format!(
            "Execution error: dataset 'file://{}/' contains no samples",
            input_path.display()
        )
    );
}

#[test]
fn reports_sample_ids_absent_from_the_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), &SAMPLES[..2]);

    let err = run(
        Formulation::CombineRefsUnion,
        &input,
        InputFormat::VORTEX,
        Action::Collect,
        Some(vec!["NOT_A_SAMPLE".to_string()]),
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Execution error: samples not found in dataset: NOT_A_SAMPLE"
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
