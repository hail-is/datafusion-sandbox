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
    fixture::{self, FixtureFormat, RecordedRun},
    format::InputFormat,
    metrics_directory::MetricsDirectory,
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
fn balance_split_points_prints_pasteable_points_from_an_interval_merge_directory() {
    let dataset = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let dir = tempfile::tempdir().unwrap();
    let combined = dir.path().join("combined");

    let write = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "--threads",
            "1",
            "combine-refs",
            dataset.table_path(),
            "--formulation",
            "interval-merge",
            "--split-points",
            "1:3,2:2",
            "--write",
            combined.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        write.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&write.stderr)
    );

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "--threads",
            "1",
            "balance-split-points",
            combined.to_str().unwrap(),
            "--intervals",
            "4",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stderr, Vec::<u8>::new());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout, "1:2,1:4,2:2\n");
    stdout
        .trim_end()
        .parse::<datafusion_sandbox::locus::SplitPoints>()
        .unwrap();
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
    let recorded = read_recorded_run(&metrics_directory, "cli-run");
    let record = recorded.record.expect("a run record");
    let metrics = recorded.metrics.expect("run metrics");
    for (table, batch) in [("runs", &record), ("metrics", &metrics)] {
        let column = batch.column_by_name("run_id").unwrap();
        let run_ids: Vec<String> = (0..batch.num_rows())
            .map(|row| array_value_to_string(column, row).unwrap())
            .collect();
        assert!(!run_ids.is_empty(), "{table} is empty");
        assert!(
            run_ids.iter().all(|run_id| run_id == "cli-run"),
            "{table}: {run_ids:?}"
        );
    }
    let peak_rss_bytes: u64 =
        array_value_to_string(record.column_by_name("peak_rss_bytes").unwrap(), 0)
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

/// A second measured write with the run id of a recorded run is refused, naming the id on
/// stderr, and its output path is never written.
#[test]
fn a_repeated_run_id_is_refused_before_the_write() {
    let dataset = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let dir = tempfile::tempdir().unwrap();
    let metrics_directory = dir.path().join("metrics").to_str().unwrap().to_string();
    let measured_write = |output_path: &std::path::Path| {
        Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
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
            .unwrap()
    };

    let first = measured_write(&dir.path().join("first.vortex"));
    assert!(
        first.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let second_output = dir.path().join("second.vortex");
    let second = measured_write(&second_output);

    assert!(!second.status.success());
    let stderr = String::from_utf8(second.stderr).unwrap();
    assert!(
        stderr.contains("run id 'cli-run' already has a run record"),
        "stderr:\n{stderr}"
    );
    assert!(!second_output.exists());
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

/// Reads back what the metrics directory at `path` on disk holds for `run_id`.
fn read_recorded_run(path: &str, run_id: &str) -> RecordedRun {
    let directory = MetricsDirectory::new(path);
    let run_id = run_id.to_string();
    pipeline::run(
        move |ctx| async move { fixture::read_recorded_run(&ctx, &directory, &run_id).await },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}
