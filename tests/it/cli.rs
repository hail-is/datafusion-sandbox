//! End-to-end tests of the binary.

use crate::fixture::{self, FixtureFormat};

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
