mod fixture;

use datafusion_sandbox::SAMPLES;
use std::{path::Path, process::Command};

#[test]
fn write_mode_reports_rows_written() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    assert_rows_written(
        "combine-refs",
        &input,
        &dir.path().join("combined_refs.vortex"),
        400,
    );
    assert_rows_written(
        "combine-alleles",
        &input,
        &dir.path().join("combined_alleles.vortex"),
        8,
    );
}

#[test]
fn show_mode_prints_combiner_rows() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["--threads", "1", "combine-alleles", &input, "--show"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("| position | alleles | contig |"),
        "stdout:\n{stdout}"
    );
    assert!(
        stdout.contains("| 1        | A,G     | chr22  | 1"),
        "stdout:\n{stdout}"
    );
}

#[test]
fn explain_mode_prints_the_combiner_plan() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["--threads", "1", "combine-refs", &input, "--explain"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("physical_plan"), "stdout:\n{stdout}");
    assert!(
        stdout.contains("SortPreservingMergeExec"),
        "stdout:\n{stdout}"
    );
}

#[test]
fn explain_analyze_mode_prints_operator_timings() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "--threads",
            "1",
            "combine-alleles",
            &input,
            "--explain-analyze",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Plan with Metrics"), "stdout:\n{stdout}");
    assert!(stdout.contains("elapsed_compute"), "stdout:\n{stdout}");
}

#[test]
fn combiner_requires_exactly_one_mode() {
    let no_mode = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["combine-refs", "missing"])
        .output()
        .unwrap();

    assert!(!no_mode.status.success());
    let stderr = String::from_utf8(no_mode.stderr).unwrap();
    for mode in ["--write", "--show", "--explain", "--explain-analyze"] {
        assert!(stderr.contains(mode), "stderr:\n{stderr}");
    }

    let two_modes = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["combine-alleles", "missing", "--show", "--explain"])
        .output()
        .unwrap();

    assert!(!two_modes.status.success());
    let stderr = String::from_utf8(two_modes.stderr).unwrap();
    assert!(stderr.contains("cannot be used with"), "stderr:\n{stderr}");
}

#[test]
fn failing_combiner_exits_non_zero() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("missing");
    for mode in ["--show", "--explain", "--explain-analyze"] {
        let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
            .args(["--threads", "1", "combine-refs"])
            .arg(&input)
            .arg(mode)
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "mode {mode} unexpectedly succeeded"
        );
    }

    let write_output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["--threads", "1", "combine-refs"])
        .arg(input)
        .arg("--write")
        .arg(dir.path().join("combined.vortex"))
        .output()
        .unwrap();
    assert!(!write_output.status.success());
}

fn assert_rows_written(command: &str, input: &str, output_path: &Path, expected: u64) {
    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["--threads", "1", command, input, "--write"])
        .arg(output_path)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.lines().last(), Some(expected.to_string().as_str()));
}
