use crate::fixture;

use std::process::Command;

use fixture::SAMPLES;

#[test]
fn rejects_an_output_extension_that_contradicts_the_default_format() {
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
    assert!(stderr.contains("combined.vortex"), "stderr:\n{stderr}");
    assert!(stderr.contains("parquet"), "stderr:\n{stderr}");
}

#[test]
fn show_action_defaults_to_vortex_input() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    let stdout = successful_combiner_stdout("combine-alleles", &input, "--show");
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
fn show_action_defaults_to_twenty_rows() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);

    let implicit = successful_combiner_stdout("combine-refs", &input, "--show");
    let explicit = successful_stdout(Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox")).args(
        [
            "--threads",
            "1",
            "combine-refs",
            &input,
            "--show",
            "--limit",
            "20",
        ],
    ));
    assert_eq!(implicit, explicit);
}

#[test]
fn combiner_requires_exactly_one_action() {
    let no_action = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["combine-refs", "missing"])
        .output()
        .unwrap();

    assert!(!no_action.status.success());
    let stderr = String::from_utf8(no_action.stderr).unwrap();
    for action in ["--write", "--show", "--explain", "--explain-analyze"] {
        assert!(stderr.contains(action), "stderr:\n{stderr}");
    }

    let two_actions = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args(["combine-alleles", "missing", "--show", "--explain"])
        .output()
        .unwrap();

    assert!(!two_actions.status.success());
    let stderr = String::from_utf8(two_actions.stderr).unwrap();
    assert!(stderr.contains("cannot be used with"), "stderr:\n{stderr}");
}

#[test]
fn compression_requires_a_write_action() {
    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "combine-refs",
            "missing",
            "--output-format",
            "parquet",
            "--compression",
            "snappy",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("required arguments were not provided"),
        "stderr:\n{stderr}"
    );
    assert!(stderr.contains("--compression"), "stderr:\n{stderr}");
    assert!(stderr.contains("--write"), "stderr:\n{stderr}");
}

#[test]
fn compression_is_rejected_in_non_write_actions() {
    for command in ["combine-refs", "combine-alleles"] {
        for action in ["--show", "--explain", "--explain-analyze"] {
            let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
                .args([
                    command,
                    "missing",
                    "--output-format",
                    "parquet",
                    "--compression",
                    "snappy",
                    action,
                ])
                .output()
                .unwrap();

            assert!(!output.status.success(), "{command} {action} succeeded");
            let stderr = String::from_utf8(output.stderr).unwrap();
            assert!(
                stderr.contains("cannot be used with"),
                "{command} {action} stderr:\n{stderr}"
            );
        }
    }
}

#[test]
fn rejects_compression_unknown_to_the_output_format() {
    for (format, compression) in [("parquet", "zip"), ("vortex", "zstd(3)")] {
        let output_path = format!("combined.{format}");
        let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
            .args([
                "combine-refs",
                "missing",
                "--output-format",
                format,
                "--compression",
                compression,
                "--write",
                &output_path,
            ])
            .output()
            .unwrap();

        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains(compression), "stderr:\n{stderr}");
        assert!(stderr.contains(format), "stderr:\n{stderr}");
    }
}

#[test]
fn failing_combiner_exits_non_zero_under_every_action() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("missing");
    for action in ["--show", "--explain", "--explain-analyze"] {
        let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
            .args(["--threads", "1", "combine-refs"])
            .arg(&input)
            .arg(action)
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "action {action} unexpectedly succeeded"
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

#[test]
fn combine_alleles_rejects_a_formulation_argument_before_running() {
    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "combine-alleles",
            "missing",
            "--formulation",
            "one-scan",
            "--show",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--formulation"), "stderr:\n{stderr}");
    assert!(stderr.contains("unexpected argument"), "stderr:\n{stderr}");
}

#[test]
fn samples_argument_accepts_a_comma_separated_list() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::write_sample_tables(dir.path(), SAMPLES);
    let requested = format!("{},{}", SAMPLES[0], SAMPLES[1]);
    let stdout = successful_stdout(
        Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
            .args([
                "--threads",
                "1",
                "combine-refs",
                &input,
                "--samples",
                &requested,
                "--write",
            ])
            .arg(dir.path().join("restricted.vortex")),
    );

    assert_eq!(stdout.lines().next(), Some("formulation: union"));
    assert_eq!(stdout.lines().last(), Some("16"));
}

fn successful_combiner_stdout(command: &str, input: &str, action: &str) -> String {
    let stdout = successful_stdout(
        Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox")).args([
            "--threads",
            "1",
            command,
            input,
            action,
        ]),
    );
    assert_eq!(stdout.lines().next(), Some("formulation: union"));
    stdout
}

fn successful_stdout(command: &mut Command) -> String {
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}
