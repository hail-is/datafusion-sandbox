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
    throughput_probe::{self, ProbeSettings, ProgressSample},
};

use datafusion::arrow::{record_batch::RecordBatch, util::display::array_value_to_string};

use std::{num::NonZeroU32, process::Command, time::Duration};

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

/// `--probe --metrics` drains the plan, prints the run id under the formulation line and then
/// the rows received, the steady-state throughput, and the stop reason, and records the run in
/// three Parquet tables that read back with that id. The fixture finishes in well under the
/// maximum duration, so the probe completes, and its progress samples end at every row.
#[test]
fn a_drained_probe_prints_its_estimate_and_records_three_tables() {
    let dataset = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let dir = tempfile::tempdir().unwrap();
    let metrics_directory = dir.path().join("metrics").to_str().unwrap().to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "--threads",
            "1",
            "combine-refs",
            dataset.table_path(),
            "--formulation",
            "interval-merge",
            "--split-points",
            "1:3,2:2",
            "--probe",
            "--metrics",
            &metrics_directory,
            "--run-id",
            "cli-probe",
            "--max-duration",
            "60",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();
    let [formulation, run_id, rows, throughput, stop_reason] = lines.as_slice() else {
        panic!("stdout:\n{stdout}");
    };
    assert_eq!(
        [*formulation, *run_id, *rows, *stop_reason],
        [
            "formulation: interval-merge",
            "run id: cli-probe",
            "32",
            "stop reason: completed"
        ]
    );
    let rate: f64 = throughput
        .strip_prefix("steady-state throughput: ")
        .and_then(|rest| rest.strip_suffix(" rows/s"))
        .and_then(|rate| rate.parse().ok())
        .unwrap_or_else(|| panic!("stdout:\n{stdout}"));
    assert!(rate > 0.0, "stdout:\n{stdout}");

    let recorded = read_recorded_run(&metrics_directory, "cli-probe");
    let record = recorded.record.expect("a run record");
    let metrics = recorded.metrics.expect("run metrics");
    let samples = recorded.progress_samples.expect("progress samples");
    for (table, batch) in [
        ("runs", &record),
        ("metrics", &metrics),
        ("progress", &samples),
    ] {
        let run_ids = column_strings(batch, "run_id");
        assert!(!run_ids.is_empty(), "{table} is empty");
        assert!(
            run_ids.iter().all(|run_id| run_id == "cli-probe"),
            "{table}: {run_ids:?}"
        );
    }
    for (column, expected) in [
        ("action", "probe"),
        ("stop_reason", "completed"),
        ("rows_written", "32"),
        ("max_duration_ns", "60000000000"),
    ] {
        assert_eq!(column_strings(&record, column), [expected], "{column}");
    }
    assert_eq!(
        column_strings(&samples, "rows").last().map(String::as_str),
        Some("32")
    );
}

/// `--probe --write PATH --metrics` probes the write of either output layout, prints what a
/// drained probe prints, records the three tables with the write's columns filled, and leaves
/// nothing beside the metrics directory: no file or directory at PATH, and no staging file of an
/// aborted upload next to it. Interval-merge writes a directory of one file per interval and
/// completes at the first finished interval; grouped-merge writes one file and completes with
/// every row.
#[test]
fn a_written_probe_records_three_tables_and_leaves_nothing_on_disk() {
    let dataset = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    for (formulation, formulation_args, output_name, output_format, compression) in [
        (
            "interval-merge",
            ["--split-points", "1:3,2:2"],
            "combined",
            "vortex",
            "compact",
        ),
        (
            "grouped-merge",
            ["--groups", "2"],
            "combined.parquet",
            "parquet",
            "snappy",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let metrics_directory = dir.path().join("metrics").to_str().unwrap().to_string();
        let output_path = dir.path().join(output_name);

        let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
            .args(["--threads", "1", "combine-refs", dataset.table_path()])
            .args(["--formulation", formulation])
            .args(formulation_args)
            .args(["--output-format", output_format])
            .args(["--probe", "--write", output_path.to_str().unwrap()])
            .args(["--compression", compression])
            .args(["--metrics", &metrics_directory])
            .args(["--run-id", "cli-written-probe"])
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "{formulation} stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let lines: Vec<&str> = stdout.lines().collect();
        let [formulation_line, run_id, rows, throughput, stop_reason] = lines.as_slice() else {
            panic!("stdout:\n{stdout}");
        };
        assert_eq!(
            [*formulation_line, *run_id, *stop_reason],
            [
                format!("formulation: {formulation}").as_str(),
                "run id: cli-written-probe",
                "stop reason: completed"
            ]
        );
        // Interval-merge's first interval to finish stops the probe, whichever it is and however
        // far the others got, so its rows received are only known to be some of the 32.
        let rows_received: u64 = rows.parse().unwrap_or_else(|_| panic!("stdout:\n{stdout}"));
        if formulation == "interval-merge" {
            assert!((1..=32).contains(&rows_received), "stdout:\n{stdout}");
        } else {
            assert_eq!(rows_received, 32, "stdout:\n{stdout}");
        }
        assert!(
            throughput.starts_with("steady-state throughput: "),
            "stdout:\n{stdout}"
        );

        let recorded = read_recorded_run(&metrics_directory, "cli-written-probe");
        let record = recorded.record.expect("a run record");
        assert!(
            recorded
                .metrics
                .is_some_and(|metrics| metrics.num_rows() > 0)
        );
        assert!(
            recorded
                .progress_samples
                .is_some_and(|samples| samples.num_rows() > 0)
        );
        for (column, expected) in [
            ("action", "probe"),
            ("stop_reason", "completed"),
            ("rows_written", rows),
            ("output_path", output_path.to_str().unwrap()),
            ("output_format", output_format),
            ("compression", compression),
        ] {
            assert_eq!(
                column_strings(&record, column),
                [expected],
                "{formulation}: {column}"
            );
        }
        let left: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(left, ["metrics"], "{formulation}");
    }
}

/// `--probe --shadow` records the relative half-width of the estimate interval it completed
/// with, and of the one it would have stopped with, as replaying its recorded progress samples
/// through the stopping rule gives them. The fixture finishes in milliseconds, so fine settings
/// give the rule samples to judge: the window it completes with always has an interval, since its
/// last batch holds the rows, but it settles before the first partition end only sometimes.
#[test]
fn a_shadow_probe_records_the_relative_half_widths_its_samples_replay_to() {
    let dataset = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let dir = tempfile::tempdir().unwrap();
    let metrics_directory = dir.path().join("metrics").to_str().unwrap().to_string();
    let settings = ProbeSettings {
        poll_period: Duration::from_millis(1),
        batch_duration: Duration::from_millis(2),
        precision: 1e9,
        consecutive_checks: NonZeroU32::MIN,
        window_groups: 2,
        min_duration: Duration::from_millis(1),
        ..ProbeSettings::default()
    };

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-sandbox"))
        .args([
            "--threads",
            "1",
            "combine-refs",
            dataset.table_path(),
            "--probe",
            "--shadow",
            "--metrics",
            &metrics_directory,
            "--run-id",
            "cli-shadow",
            "--poll-period",
            "0.001",
            "--batch",
            "0.002",
            "--precision",
            "1e9",
            "--consecutive",
            "1",
            "--window-groups",
            "2",
            "--min-duration",
            "0.001",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let recorded = read_recorded_run(&metrics_directory, "cli-shadow");
    let record = recorded.record.expect("a run record");
    let progress = recorded.progress_samples.expect("progress samples");
    let samples: Vec<ProgressSample> = column_strings(&progress, "elapsed_ns")
        .into_iter()
        .zip(column_strings(&progress, "rows"))
        .map(|(elapsed_ns, rows)| ProgressSample {
            elapsed_ns: elapsed_ns.parse().unwrap(),
            rows: rows.parse().unwrap(),
        })
        .collect();
    let first_partition_end_ns: Option<u64> = column_strings(&record, "first_partition_end_ns")[0]
        .parse()
        .ok();
    let replayed_would_stop = (1..=samples.len()).find_map(|taken| {
        let seen = samples.get(..taken)?;
        let latest_ns = seen.last()?.elapsed_ns;
        throughput_probe::would_stop(
            &settings,
            seen,
            first_partition_end_ns.filter(|&end_ns| end_ns <= latest_ns),
        )
    });
    let replayed_end = throughput_probe::decide(&settings, &samples, first_partition_end_ns);

    assert_eq!(column_strings(&record, "action"), ["shadow"]);
    assert!(
        optional_f64(&record, "relative_half_width").is_some(),
        "{samples:?}"
    );
    assert_eq!(
        column_strings(&record, "stop_reason"),
        [replayed_end.as_ref().unwrap().stop_reason.name()]
    );
    assert_eq!(
        [
            optional_f64(&record, "relative_half_width"),
            optional_f64(&record, "would_be_relative_half_width"),
        ],
        [
            replayed_end.and_then(|decision| decision.relative_half_width),
            replayed_would_stop.and_then(|decision| decision.relative_half_width),
        ],
        "{samples:?}"
    );
}

/// The one value of the Float64 column `name` of `batch`; `None` when it is empty.
fn optional_f64(batch: &RecordBatch, name: &str) -> Option<f64> {
    let [value] = column_strings(batch, name).try_into().unwrap();
    (!value.is_empty()).then(|| value.parse().unwrap())
}

/// The values of the column `name` of `batch`, rendered.
fn column_strings(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = batch.column_by_name(name).unwrap();
    (0..batch.num_rows())
        .map(|row| array_value_to_string(column, row).unwrap())
        .collect()
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
