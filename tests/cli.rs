mod fixture;

use datafusion_sandbox::SAMPLES;
use std::{path::Path, process::Command};

#[test]
fn combiner_commands_report_rows_written() {
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
fn failing_combiner_exits_non_zero() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["--threads", "1", "combine-refs"])
        .arg(dir.path().join("missing"))
        .arg("--output")
        .arg(dir.path().join("combined.vortex"))
        .output()
        .unwrap();

    assert!(!output.status.success());
}

fn assert_rows_written(command: &str, input: &str, output_path: &Path, expected: u64) {
    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["--threads", "1", command, input, "--output"])
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
