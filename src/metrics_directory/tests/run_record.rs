//! The run record table, filled from records with distinct and missing facts, and the record
//! read from a formulation, a write target, and a probe's row.

use crate::{
    format::OutputFormat,
    formulation::Formulation,
    metrics_directory::run_record::{
        self, FormulationRecord, ProbeRecord, RecordedProbe, RunRecord, RunStatus, WriteRecord,
    },
    testing::{
        columns::{assert_columns, string_values},
        formulations::{grouped_merge, interval_merge},
        run_records,
    },
    throughput_probe::{Decision, ProbeKind, ProbeSettings, ProbedKind, StopReason},
    write::WriteTarget,
};

use datafusion::arrow::{
    array::Array,
    datatypes::{DataType, TimeUnit},
};

use std::{
    num::NonZeroU32,
    time::{Duration, SystemTime},
};

/// A write record reads a write target's settings: its output path, its format's name, and its
/// compression as the caller spelled it, or none at the format's default.
#[test]
fn a_write_record_reads_its_write_target_s_settings() {
    let record = |output_path: &str, output_format: &str, compression: Option<&str>| WriteRecord {
        output_path: output_path.to_string(),
        output_format: output_format.to_string(),
        compression: compression.map(ToString::to_string),
    };

    for (output_path, output_format, expected) in [
        (
            "gs://bucket/out.parquet",
            OutputFormat::PARQUET,
            record("gs://bucket/out.parquet", "parquet", None),
        ),
        (
            "out.parquet",
            OutputFormat::PARQUET.with_compression("zstd(3)").unwrap(),
            record("out.parquet", "parquet", Some("zstd(3)")),
        ),
        (
            "out.vortex",
            OutputFormat::VORTEX,
            record("out.vortex", "vortex", None),
        ),
        (
            "out.vortex",
            OutputFormat::VORTEX.with_compression("compact").unwrap(),
            record("out.vortex", "vortex", Some("compact")),
        ),
    ] {
        let target = WriteTarget {
            output_path: output_path.to_string(),
            output_format,
        };
        assert_eq!(WriteRecord::from(&target), expected, "{target:?}");
    }
}

/// A formulation record reads each formulation's settings: its name, and the group count or split
/// points only the formulation that takes one has.
#[test]
fn a_formulation_record_reads_each_formulation_s_settings() {
    let record =
        |name: &str, groups: Option<usize>, split_points: Option<&str>| FormulationRecord {
            name: name.to_string(),
            groups,
            split_points: split_points.map(ToString::to_string),
        };

    for (formulation, expected) in [
        (
            Formulation::CombineAllelesUnion,
            record("union", None, None),
        ),
        (Formulation::CombineRefsUnion, record("union", None, None)),
        (grouped_merge(3), record("grouped-merge", Some(3), None)),
        (
            interval_merge("1:5,2:1"),
            record("interval-merge", None, Some("1:5,2:1")),
        ),
    ] {
        assert_eq!(
            FormulationRecord::from(&formulation),
            expected,
            "{formulation:?}"
        );
    }
}

/// The run record table's columns, in order, with their types and whether they may be null: the
/// settings a formulation may leave unset, the write a drained probe does not make, the facts a
/// run that did not complete may not have learned, and the facts only a probe has are the only
/// nullable ones.
#[test]
fn the_run_record_schema_names_its_columns_in_order() {
    let schema = run_record::run_record_schema();

    assert_columns(
        &schema,
        &[
            ("run_id", DataType::Utf8, false),
            ("status", DataType::Utf8, false),
            (
                "started_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            ("formulation", DataType::Utf8, false),
            ("groups", DataType::UInt64, true),
            ("split_points", DataType::Utf8, true),
            ("row_ordering", DataType::Utf8, false),
            ("dataset_path", DataType::Utf8, false),
            ("input_format", DataType::Utf8, false),
            ("output_format", DataType::Utf8, true),
            ("compression", DataType::Utf8, true),
            ("threads", DataType::UInt64, false),
            ("input_tables", DataType::UInt64, true),
            ("samples", DataType::UInt64, true),
            ("output_path", DataType::Utf8, true),
            ("rows_written", DataType::UInt64, true),
            ("run_ns", DataType::UInt64, false),
            ("execute_ns", DataType::UInt64, true),
            ("peak_rss_bytes", DataType::UInt64, false),
            ("action", DataType::Utf8, true),
            ("stop_reason", DataType::Utf8, true),
            ("steady_state_throughput", DataType::Float64, true),
            ("relative_half_width", DataType::Float64, true),
            ("warmup_end_ns", DataType::UInt64, true),
            ("window_end_ns", DataType::UInt64, true),
            ("window_rows", DataType::UInt64, true),
            ("first_partition_end_ns", DataType::UInt64, true),
            ("poll_period_ns", DataType::UInt64, true),
            ("batch_duration_ns", DataType::UInt64, true),
            ("precision", DataType::Float64, true),
            ("consecutive_checks", DataType::UInt64, true),
            ("window_groups", DataType::UInt64, true),
            ("min_duration_ns", DataType::UInt64, true),
            ("max_duration_ns", DataType::UInt64, true),
            ("would_stop_ns", DataType::UInt64, true),
            ("would_be_steady_state_throughput", DataType::Float64, true),
            ("would_be_relative_half_width", DataType::Float64, true),
            ("would_be_warmup_end_ns", DataType::UInt64, true),
        ],
    );
}

/// A record with a distinct value in every field lands each value in the column of the field's
/// name, so no two columns can have swapped accessors unnoticed.
#[test]
fn each_run_record_field_lands_in_the_column_of_its_name() {
    let record = RunRecord {
        run_id: "run-1".to_string(),
        status: RunStatus::Completed,
        started_at: SystemTime::UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789),
        formulation: FormulationRecord {
            name: "grouped-merge".to_string(),
            groups: Some(3),
            split_points: Some("1:5,2:1".to_string()),
        },
        row_ordering: "locus,s".to_string(),
        dataset_path: "gs://bucket/refs".to_string(),
        input_format: "vortex".to_string(),
        write: Some(WriteRecord {
            output_path: "gs://bucket/combined.parquet".to_string(),
            output_format: "parquet".to_string(),
            compression: Some("zstd(3)".to_string()),
        }),
        threads: 4,
        input_tables: Some(7),
        samples: Some(50),
        rows_written: Some(123_456),
        run_ns: 2_000_000_000,
        execute_ns: Some(1_500_000_000),
        peak_rss_bytes: 3_221_225_472,
        probe: Some(ProbeRecord {
            settings: ProbeSettings {
                poll_period: Duration::from_millis(100),
                batch_duration: Duration::from_millis(1_500),
                precision: 0.05,
                consecutive_checks: NonZeroU32::new(4).unwrap(),
                window_groups: 12,
                min_duration: Duration::from_secs(30),
                max_duration: Duration::from_secs(300),
            },
            decision: Decision {
                stop_reason: StopReason::Completed,
                steady_state_throughput: Some(2_500.5),
                warmup_end_ns: Some(400_000_000),
                window_end_ns: 1_400_000_000,
                window_rows: 120_000,
                relative_half_width: Some(0.0125),
            },
            first_partition_end_ns: Some(1_400_000_001),
            kind: ProbedKind::Shadow {
                would_stop: Some(Decision {
                    stop_reason: StopReason::Steady,
                    steady_state_throughput: Some(2_400.25),
                    warmup_end_ns: Some(300_000_000),
                    window_end_ns: 1_200_000_000,
                    window_rows: 100_000,
                    relative_half_width: Some(0.015_625),
                }),
            },
        }),
    };

    let batch = run_record::run_record_batch(&record).unwrap();

    assert_eq!(batch.schema(), run_record::run_record_schema());
    assert_eq!(batch.num_rows(), 1);
    for (column, expected) in [
        ("run_id", "run-1"),
        ("status", "completed"),
        ("started_at", "2023-11-14T22:13:20.123456789Z"),
        ("formulation", "grouped-merge"),
        ("groups", "3"),
        ("split_points", "1:5,2:1"),
        ("row_ordering", "locus,s"),
        ("dataset_path", "gs://bucket/refs"),
        ("input_format", "vortex"),
        ("output_format", "parquet"),
        ("compression", "zstd(3)"),
        ("threads", "4"),
        ("input_tables", "7"),
        ("samples", "50"),
        ("output_path", "gs://bucket/combined.parquet"),
        ("rows_written", "123456"),
        ("run_ns", "2000000000"),
        ("execute_ns", "1500000000"),
        ("peak_rss_bytes", "3221225472"),
        ("action", "shadow"),
        ("stop_reason", "completed"),
        ("steady_state_throughput", "2500.5"),
        ("relative_half_width", "0.0125"),
        ("warmup_end_ns", "400000000"),
        ("window_end_ns", "1400000000"),
        ("window_rows", "120000"),
        ("first_partition_end_ns", "1400000001"),
        ("poll_period_ns", "100000000"),
        ("batch_duration_ns", "1500000000"),
        ("precision", "0.05"),
        ("consecutive_checks", "4"),
        ("window_groups", "12"),
        ("min_duration_ns", "30000000000"),
        ("max_duration_ns", "300000000000"),
        ("would_stop_ns", "1200000000"),
        ("would_be_steady_state_throughput", "2400.25"),
        ("would_be_relative_half_width", "0.015625"),
        ("would_be_warmup_end_ns", "300000000"),
    ] {
        assert_eq!(string_values(&batch, column), [expected], "{column}");
    }
}

/// A formulation without a group count or split points and a format at its default compression
/// leave those three columns null, and a measured write leaves every probe column null.
#[test]
fn unset_run_record_settings_are_null() {
    let record = RunRecord {
        formulation: FormulationRecord {
            name: "union".to_string(),
            groups: None,
            split_points: None,
        },
        write: Some(WriteRecord {
            compression: None,
            ..run_records::run_record("run-1").write.unwrap()
        }),
        ..run_records::run_record("run-1")
    };

    let batch = run_record::run_record_batch(&record).unwrap();

    for column in [
        "groups",
        "split_points",
        "compression",
        "action",
        "stop_reason",
        "steady_state_throughput",
        "relative_half_width",
        "warmup_end_ns",
        "window_end_ns",
        "window_rows",
        "first_partition_end_ns",
        "poll_period_ns",
        "batch_duration_ns",
        "precision",
        "consecutive_checks",
        "window_groups",
        "min_duration_ns",
        "max_duration_ns",
        "would_stop_ns",
        "would_be_steady_state_throughput",
        "would_be_relative_half_width",
        "would_be_warmup_end_ns",
    ] {
        assert!(batch.column_by_name(column).unwrap().is_null(0), "{column}");
    }
    assert_eq!(string_values(&batch, "formulation"), ["union"]);
}

/// A run that failed or was cancelled names its status, and leaves null the facts it may not
/// have learned before it ended: here, how much of the dataset it covered, the rows it wrote, and
/// how long its plan executed.
#[test]
fn a_run_that_did_not_complete_names_its_status_and_leaves_its_unlearned_facts_null() {
    for (status, name) in [
        (RunStatus::Failed, "failed"),
        (RunStatus::Cancelled, "cancelled"),
    ] {
        let record = RunRecord {
            status,
            input_tables: None,
            samples: None,
            rows_written: None,
            execute_ns: None,
            ..run_records::run_record("run-1")
        };

        let batch = run_record::run_record_batch(&record).unwrap();

        assert_eq!(string_values(&batch, "status"), [name]);
        for column in ["input_tables", "samples", "rows_written", "execute_ns"] {
            assert!(
                batch.column_by_name(column).unwrap().is_null(0),
                "{name} {column}"
            );
        }
        assert_eq!(string_values(&batch, "run_ns"), ["2000"], "{name}");
    }
}

/// A drained probe writes nothing, so its write settings are null; a probe with no estimate, no
/// estimate interval, no end of warmup and no finished partition leaves those four columns null
/// too, and a probe that is no shadow probe leaves the would-stop columns null.
#[test]
fn a_drained_probe_leaves_its_write_and_missing_facts_null() {
    let record = RunRecord {
        write: None,
        probe: Some(ProbeRecord {
            settings: ProbeSettings::default(),
            decision: Decision {
                stop_reason: StopReason::Capped,
                steady_state_throughput: None,
                warmup_end_ns: None,
                window_end_ns: 0,
                window_rows: 0,
                relative_half_width: None,
            },
            first_partition_end_ns: None,
            kind: ProbedKind::Probe,
        }),
        ..run_records::run_record("run-1")
    };

    let batch = run_record::run_record_batch(&record).unwrap();

    for column in [
        "output_format",
        "compression",
        "output_path",
        "steady_state_throughput",
        "relative_half_width",
        "warmup_end_ns",
        "first_partition_end_ns",
        "would_stop_ns",
        "would_be_steady_state_throughput",
        "would_be_relative_half_width",
        "would_be_warmup_end_ns",
    ] {
        assert!(batch.column_by_name(column).unwrap().is_null(0), "{column}");
    }
    assert_eq!(string_values(&batch, "action"), ["probe"]);
    assert_eq!(string_values(&batch, "stop_reason"), ["capped"]);
}

/// A shadow probe whose rule never stopped steady records that it is a shadow probe, completed,
/// with the relative half-width it completed with, and leaves its would-stop columns null.
#[test]
fn a_shadow_probe_that_never_would_have_stopped_leaves_its_would_stop_columns_null() {
    let record = RunRecord {
        probe: Some(ProbeRecord {
            settings: ProbeSettings::default(),
            decision: Decision {
                stop_reason: StopReason::Completed,
                steady_state_throughput: Some(1_000.0),
                warmup_end_ns: None,
                window_end_ns: 1_000,
                window_rows: 32,
                relative_half_width: Some(0.5),
            },
            first_partition_end_ns: Some(1_000),
            kind: ProbedKind::Shadow { would_stop: None },
        }),
        ..run_records::run_record("run-1")
    };

    let batch = run_record::run_record_batch(&record).unwrap();

    for column in [
        "would_stop_ns",
        "would_be_steady_state_throughput",
        "would_be_relative_half_width",
        "would_be_warmup_end_ns",
    ] {
        assert!(batch.column_by_name(column).unwrap().is_null(0), "{column}");
    }
    assert_eq!(string_values(&batch, "action"), ["shadow"]);
    assert_eq!(string_values(&batch, "stop_reason"), ["completed"]);
    assert_eq!(string_values(&batch, "relative_half_width"), ["0.5"]);
}

/// A probe's run record reads back as the probe it records, a shadow probe's or another's, and a
/// measured write's as none.
#[test]
fn a_probe_s_run_record_reads_back_as_its_probe() {
    let settings = ProbeSettings {
        poll_period: Duration::from_millis(50),
        batch_duration: Duration::from_millis(2_500),
        precision: 0.05,
        consecutive_checks: NonZeroU32::new(5).unwrap(),
        window_groups: 20,
        min_duration: Duration::from_secs(60),
        max_duration: Duration::from_secs(600),
    };
    let probe = |kind| ProbeRecord {
        settings: settings.clone(),
        decision: Decision {
            stop_reason: StopReason::Completed,
            steady_state_throughput: Some(1_000.0),
            warmup_end_ns: None,
            window_end_ns: 1_000,
            window_rows: 32,
            relative_half_width: Some(0.5),
        },
        first_partition_end_ns: Some(1_000),
        kind,
    };
    let records = [
        RunRecord {
            probe: Some(probe(ProbedKind::Shadow { would_stop: None })),
            ..run_records::run_record("shadow")
        },
        RunRecord {
            probe: Some(ProbeRecord {
                first_partition_end_ns: None,
                ..probe(ProbedKind::Probe)
            }),
            ..run_records::run_record("probe")
        },
        run_records::run_record("write"),
    ];

    let read_back: Vec<Option<RecordedProbe>> = records
        .iter()
        .map(|record| {
            let batch = run_record::run_record_batch(record).unwrap();
            run_record::recorded_probe(&batch, 0).unwrap()
        })
        .collect();

    assert_eq!(
        read_back,
        [
            Some(RecordedProbe {
                run_id: "shadow".to_string(),
                kind: ProbeKind::Shadow,
                settings: settings.clone(),
                first_partition_end_ns: Some(1_000),
            }),
            Some(RecordedProbe {
                run_id: "probe".to_string(),
                kind: ProbeKind::Probe,
                settings,
                first_partition_end_ns: None,
            }),
            None,
        ]
    );
}
