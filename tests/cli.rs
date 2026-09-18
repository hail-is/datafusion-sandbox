//! End-to-end tests of the binary.

#![cfg(test)]
// `debug_assertions` here is a proxy for dev builds. Optimized builds don't trigger the linker warning.
#![cfg_attr(
    all(target_os = "macos", debug_assertions),
    allow(
        linker_messages,
        reason = "Apple ld falls back to DWARF when the CLI exceeds compact unwind's 16 MiB offset range; rust-lang/rust#159105 tracks this diagnostic"
    )
)]

use datafusion_sandbox::{
    fixture::{self, FixtureFormat},
    format::InputFormat,
    pipeline::{self, PipelineOptions},
};

use datafusion::arrow::{record_batch::RecordBatch, util::display::array_value_to_string};

use std::process::Command;

#[test]
fn successful_binary_prints_the_formulation_and_a_rendered_table() {
    let dataset = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "--threads",
            "1",
            "combine-alleles",
            dataset.table_path(),
            "--show",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.starts_with("formulation: union\n"),
        "stdout:\n{stdout}"
    );
    assert!(
        stdout.lines().any(|line| line.starts_with("+---")),
        "stdout:\n{stdout}"
    );
}

#[test]
fn failing_binary_exits_nonzero_and_prints_the_error_on_stderr() {
    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "combine-refs",
            "missing",
            "--input-format",
            "parquet",
            "--write",
            "combined.vortex",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("output path 'combined.vortex' has extension '.vortex', which contradicts output format 'parquet'"),
        "stderr:\n{stderr}"
    );
}

/// `--write --metrics` performs the write, prints the run id under the formulation line, and
/// records the run in two Parquet tables that read back with that id. Every metric the plan
/// reported has a column, so no warning line follows the row count. The binary is one combiner
/// run per process, so the run record's peak resident set size is the run's own, and it is no
/// smaller than the output the run held in memory.
#[test]
fn a_measured_write_prints_the_run_id_and_records_both_tables() {
    let dataset = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("combined.vortex");
    let metrics_directory = dir.path().join("metrics").to_str().unwrap().to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "--threads",
            "1",
            "combine-refs",
            dataset.table_path(),
            "--write",
            output_path.to_str().unwrap(),
            "--metrics",
            &metrics_directory,
            "--run-id",
            "cli-run",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout, "formulation: union\nrun id: cli-run\n32\n");
    assert!(output_path.exists());
    for table in ["runs", "metrics"] {
        let path = format!("{metrics_directory}/{table}/cli-run.parquet");
        let batches = read_file(&path, &InputFormat::PARQUET);
        let run_ids: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column_by_name("run_id").unwrap();
                (0..batch.num_rows()).map(|row| array_value_to_string(column, row).unwrap())
            })
            .collect();
        assert!(!run_ids.is_empty(), "{path} is empty");
        assert!(
            run_ids.iter().all(|run_id| run_id == "cli-run"),
            "{path}: {run_ids:?}"
        );
    }
    let record = read_file(
        &format!("{metrics_directory}/runs/cli-run.parquet"),
        &InputFormat::PARQUET,
    );
    let peak_rss_bytes: u64 =
        array_value_to_string(record[0].column_by_name("peak_rss_bytes").unwrap(), 0)
            .unwrap()
            .parse()
            .unwrap();
    let output_bytes: usize = read_file(output_path.to_str().unwrap(), &InputFormat::VORTEX)
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum();
    assert!(
        peak_rss_bytes >= u64::try_from(output_bytes).unwrap(),
        "peak rss {peak_rss_bytes} smaller than the output's {output_bytes} bytes"
    );
}

#[test]
fn metrics_without_a_write_is_rejected_before_any_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["combine-refs", "missing", "--metrics", "runs"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--write"), "stderr:\n{stderr}");
}

/// Reads the one file at `path` on disk, written in `input_format`, back into batches.
fn read_file(path: &str, input_format: &InputFormat) -> Vec<RecordBatch> {
    let path = path.to_string();
    let input_format = input_format.clone();
    pipeline::run(
        move |ctx| async move { fixture::read_file(&ctx, &path, &input_format, None).await },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}
