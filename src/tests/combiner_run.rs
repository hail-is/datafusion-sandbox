use crate::fixture;

use crate::{
    combiner_run::{Action, CombinerRun, Outcome, PlanInputs},
    format::{InputFormat, OutputFormat},
    formulation::Formulation,
    locus::{Locus, LocusRepresentation, RowOrdering},
    metrics_directory::MetricsDirectory,
    ordered_frame::OutputLayout,
    pipeline::{self, PipelineOptions},
    tests::{
        plan_shape::exec_names,
        support::{
            f64_values, grouped_merge, interval_merge, string_values, timestamp_values, u64_values,
        },
    },
    throughput_probe::{self, Decision, ProbeKind, ProbeSettings, ProgressSample, StopReason},
    write::WriteTarget,
};
use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::DataType,
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    error::{DataFusionError, Result},
    parquet::file::reader::{FileReader, SerializedFileReader},
    prelude::SessionContext,
};
use futures::executor::block_on;
use object_store::{ObjectStoreExt, path::Path as ObjectPath};
use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};

use fixture::{FixtureFormat, INPUT_TABLES, MemoryStore, RecordedRun, SAMPLES};

#[test]
fn renders_outcomes() {
    assert_eq!(Outcome::RowsWritten(42).render().unwrap(), "42");
    assert_eq!(
        Outcome::Measured {
            rows_written: 42,
            unrecorded_metrics: Vec::new(),
        }
        .render()
        .unwrap(),
        "42"
    );
    assert_eq!(
        Outcome::Measured {
            rows_written: 42,
            unrecorded_metrics: vec!["bytes_written".to_string(), "rows_written".to_string()],
        }
        .render()
        .unwrap(),
        "42\nwarning: metric 'bytes_written' has no column in the run metrics table and was not recorded\nwarning: metric 'rows_written' has no column in the run metrics table and was not recorded"
    );
    assert_eq!(
        Outcome::Probed {
            rows_received: 42,
            steady_state_throughput: Some(1_234.56),
            stop_reason: StopReason::Capped,
            unrecorded_metrics: vec!["bytes_written".to_string()],
        }
        .render()
        .unwrap(),
        "42\nsteady-state throughput: 1234.6 rows/s\nstop reason: capped\nwarning: metric 'bytes_written' has no column in the run metrics table and was not recorded"
    );
    assert_eq!(
        Outcome::Probed {
            rows_received: 0,
            steady_state_throughput: None,
            stop_reason: StopReason::Completed,
            unrecorded_metrics: Vec::new(),
        }
        .render()
        .unwrap(),
        "0\nsteady-state throughput: none\nstop reason: completed"
    );
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
        rendered.contains("| chr01  | 1        | A,G     | 1"),
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
            Action::Write(WriteTarget {
                output_path: dir.path().join(output_name).to_str().unwrap().to_string(),
                output_format: OutputFormat::VORTEX,
            }),
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
            let expected = [
                (Locus::new(1, 1).unwrap(), "A,G", 1),
                (Locus::new(1, 2).unwrap(), "A,C", 1),
                (Locus::new(1, 2).unwrap(), "A,G", 2),
                (Locus::new(1, 3).unwrap(), "A,C", 1),
                (Locus::new(1, 4).unwrap(), "A,G", 1),
                (Locus::new(2, 1).unwrap(), "A,C", 1),
                (Locus::new(2, 2).unwrap(), "A,G", 1),
                (Locus::new(2, 3).unwrap(), "A,C", 1),
            ]
            .into_iter()
            .map(|(locus, alleles, rank)| {
                let mut row = fixture::locus_cells(locus, representation);
                row.push(alleles.to_string());
                row.push(rank.to_string());
                row
            })
            .collect::<Vec<_>>();
            assert_eq!(rows, expected);
        }
    }
}

/// Both reference combiner formulations collect the same rows in locus order; grouped-merge
/// only does so because collect runs through a sink that requires the ordering.
#[test]
fn collects_references_in_locus_order_across_file_cuts() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            for formulation in [Formulation::CombineRefsUnion, grouped_merge(2)] {
                let input = match representation {
                    LocusRepresentation::ContigPosition => {
                        fixture::contig_position_disk_fixture(format)
                    }
                    LocusRepresentation::Packed => fixture::packed_disk_fixture(format),
                };
                let batches = expect_batches(
                    run(
                        formulation.clone(),
                        input.table_path(),
                        input.input_format(),
                        Action::Collect,
                        None,
                        None,
                    )
                    .unwrap(),
                );
                let columns = RowOrdering::locus().expand(representation).column_names();
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
                let expected = [
                    Locus::new(1, 1).unwrap(),
                    Locus::new(1, 2).unwrap(),
                    Locus::new(1, 2).unwrap(),
                    Locus::new(1, 3).unwrap(),
                    Locus::new(1, 4).unwrap(),
                    Locus::new(2, 1).unwrap(),
                    Locus::new(2, 2).unwrap(),
                    Locus::new(2, 3).unwrap(),
                ]
                .into_iter()
                .map(|locus| fixture::locus_cells(locus, representation))
                .flat_map(|locus| std::iter::repeat_n(locus, SAMPLES.len()))
                .collect::<Vec<_>>();
                assert_eq!(
                    loci, expected,
                    "{format:?} {representation:?} {formulation:?}"
                );
            }
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
        Action::Write(WriteTarget {
            output_path: dir
                .path()
                .join("limited.vortex")
                .to_str()
                .unwrap()
                .to_string(),
            output_format: OutputFormat::VORTEX,
        }),
        None,
        Some(3),
    )
    .unwrap();
    let Outcome::RowsWritten(rows) = outcome else {
        panic!("expected rows written, got {outcome:?}");
    };
    assert_eq!(rows, 3);

    for action in [
        Action::Explain { write: None },
        Action::ExplainAnalyze { write: None },
    ] {
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
        Action::Explain { write: None },
        None,
        None,
    )
    .unwrap();
    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(plan.contains("physical_plan"), "plan:\n{plan}");
    assert!(!exec_names(&plan).contains(&"LimitExec"), "plan:\n{plan}");
    assert!(
        plan.contains("DataSinkExec: sink=DrainingSink"),
        "plan:\n{plan}"
    );

    let outcome = run(
        Formulation::CombineAllelesUnion,
        input.table_path(),
        input.input_format(),
        Action::ExplainAnalyze { write: None },
        None,
        None,
    )
    .unwrap();
    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(plan.contains("Plan with Metrics"), "plan:\n{plan}");
    assert!(plan.contains("elapsed_compute"), "plan:\n{plan}");
    assert!(!exec_names(&plan).contains(&"LimitExec"), "plan:\n{plan}");
    assert!(
        plan.contains("DataSinkExec: sink=DrainingSink"),
        "plan:\n{plan}"
    );
}

/// An explain renders the plan the action would execute: with a write, the file sink's plan,
/// and the plain explain writes nothing.
#[test]
fn explain_with_a_write_renders_the_file_sink_plan_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let output_path = dir.path().join("explained.vortex");

    let outcome = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Explain {
            write: Some(WriteTarget {
                output_path: output_path.to_str().unwrap().to_string(),
                output_format: OutputFormat::VORTEX,
            }),
        },
        None,
        None,
    )
    .unwrap();

    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(
        plan.contains("DataSinkExec: sink=VortexSink"),
        "plan:\n{plan}"
    );
    assert!(!plan.contains("DrainingSink"), "plan:\n{plan}");
    assert!(
        !output_path.exists(),
        "explain wrote {}",
        output_path.display()
    );
}

#[test]
fn explain_analyze_with_a_write_performs_the_write() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let output_path = dir.path().join("analyzed.parquet");

    let outcome = run(
        grouped_merge(2),
        input.table_path(),
        input.input_format(),
        Action::ExplainAnalyze {
            write: Some(WriteTarget {
                output_path: output_path.to_str().unwrap().to_string(),
                output_format: OutputFormat::PARQUET,
            }),
        },
        None,
        None,
    )
    .unwrap();

    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    assert!(plan.contains("Plan with Metrics"), "plan:\n{plan}");
    assert!(exec_names(&plan).contains(&"DataSinkExec"), "plan:\n{plan}");
    let reader = SerializedFileReader::try_from(output_path.as_path()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 32);
}

/// On stored files, grouped-merge over two groups shows one merge per group beneath the final
/// merge and no sort, whether the frame ends in the draining sink or the file sink, and the
/// plain explain lists the operators the analyzed run executed.
#[test]
fn grouped_merge_explains_a_merge_per_group_beneath_the_final_merge() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let write = || WriteTarget {
        output_path: dir
            .path()
            .join("grouped.vortex")
            .to_str()
            .unwrap()
            .to_string(),
        output_format: OutputFormat::VORTEX,
    };

    for (explain, analyze) in [
        (
            Action::Explain { write: None },
            Action::ExplainAnalyze { write: None },
        ),
        (
            Action::Explain {
                write: Some(write()),
            },
            Action::ExplainAnalyze {
                write: Some(write()),
            },
        ),
    ] {
        let context = format!("{explain:?}");
        let explained = expect_plan(
            run(
                grouped_merge(2),
                input.table_path(),
                input.input_format(),
                explain,
                None,
                None,
            )
            .unwrap(),
        );
        let analyzed = expect_plan(
            run(
                grouped_merge(2),
                input.table_path(),
                input.input_format(),
                analyze,
                None,
                None,
            )
            .unwrap(),
        );

        let explained_names = exec_names(&explained);
        assert_eq!(
            explained_names
                .iter()
                .filter(|&&name| name == "SortPreservingMergeExec")
                .count(),
            3,
            "{context}:\n{explained}"
        );
        assert!(
            !explained_names.contains(&"SortExec"),
            "{context}:\n{explained}"
        );
        assert_eq!(explained_names, exec_names(&analyzed), "{context}");
    }
}

/// On stored files, interval-merge writes a directory of one file per locus interval, named by
/// index, whose rows read back in index order are the union formulation's rows in locus order;
/// the count reported is the total. The middle interval here holds no locus and writes an empty
/// file. Both output formats, from both stored representations.
#[test]
fn interval_merge_writes_one_file_per_interval_in_locus_order() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let context = format!("{format:?} {representation:?}");
            let dir = tempfile::tempdir().unwrap();
            let input = match representation {
                LocusRepresentation::ContigPosition => {
                    fixture::contig_position_disk_fixture(format)
                }
                LocusRepresentation::Packed => fixture::packed_disk_fixture(format),
            };
            let output_format = || match format {
                FixtureFormat::Parquet => OutputFormat::PARQUET,
                FixtureFormat::Vortex => OutputFormat::VORTEX,
            };
            let directory = dir.path().join("intervals").to_str().unwrap().to_string();
            let union = expect_batches(
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
            let union_rows = rows_of(&union);

            let outcome = run(
                interval_merge("1:5,2:1"),
                input.table_path(),
                input.input_format(),
                Action::Write(WriteTarget {
                    output_path: directory.clone(),
                    output_format: output_format(),
                }),
                None,
                None,
            )
            .unwrap();
            let Outcome::RowsWritten(rows) = outcome else {
                panic!("{context}: expected rows written, got {outcome:?}");
            };
            assert_eq!(rows, 32, "{context}");

            let mut listed: Vec<String> = std::fs::read_dir(&directory)
                .unwrap()
                .map(|entry| entry.unwrap().path().to_str().unwrap().to_string())
                .collect();
            listed.sort();
            let paths: Vec<String> = (0..3)
                .map(|index| output_format().partition_file_path(&directory, index, 3))
                .collect();
            assert_eq!(listed, paths, "{context}");
            let mut written = Vec::new();
            for (index, path) in paths.iter().enumerate() {
                let file_rows = rows_of(&read_back(path, format));
                if index == 1 {
                    assert!(file_rows.is_empty(), "{context}: {path}: {file_rows:?}");
                } else {
                    assert!(!file_rows.is_empty(), "{context}: {path} is empty");
                }
                written.extend(file_rows);
            }
            // The reference combiner orders by locus alone, so the loci match in order and the
            // rows match as sets.
            assert_eq!(loci_of(&written), loci_of(&union_rows), "{context}");
            written.sort();
            let mut union_rows = union_rows;
            union_rows.sort();
            assert_eq!(written, union_rows, "{context}");
        }
    }
}

/// The locus of each rendered row: every column but the trailing alleles and sample.
fn loci_of(rows: &[Vec<String>]) -> Vec<&[String]> {
    rows.iter()
        .map(|row| &row[..row.len().checked_sub(2).unwrap()])
        .collect()
}

/// Explaining an interval-merge write renders the partitioned sink over the union of interval
/// merges and writes nothing; analyzing it performs the writes.
#[test]
fn interval_merge_explains_the_partitioned_sink_and_analyze_performs_the_writes() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let directory = dir.path().join("explained");
    let write = || WriteTarget {
        output_path: directory.to_str().unwrap().to_string(),
        output_format: OutputFormat::VORTEX,
    };

    let explained = expect_plan(
        run(
            interval_merge("1:3,2:2"),
            input.table_path(),
            input.input_format(),
            Action::Explain {
                write: Some(write()),
            },
            None,
            None,
        )
        .unwrap(),
    );
    assert!(
        explained.contains("PartitionedSinkExec: partitions=3, sink=VortexSink"),
        "plan:\n{explained}"
    );
    let explained_names = exec_names(&explained);
    assert_eq!(
        explained_names
            .iter()
            .filter(|&&name| name == "SortPreservingMergeExec")
            .count(),
        3,
        "plan:\n{explained}"
    );
    assert!(!explained_names.contains(&"SortExec"), "plan:\n{explained}");
    assert!(
        !explained_names.contains(&"CoalescePartitionsExec"),
        "plan:\n{explained}"
    );
    assert!(!directory.exists(), "explain wrote {}", directory.display());

    let analyzed = expect_plan(
        run(
            interval_merge("1:3,2:2"),
            input.table_path(),
            input.input_format(),
            Action::ExplainAnalyze {
                write: Some(write()),
            },
            None,
            None,
        )
        .unwrap(),
    );
    assert!(analyzed.contains("Plan with Metrics"), "plan:\n{analyzed}");
    assert_eq!(explained_names, exec_names(&analyzed));
    // The partitioned sink reports its partition sinks' metrics together, so the analyzed line
    // carries the Vortex sink's row counter.
    let sink_line = analyzed
        .lines()
        .find(|line| line.contains("PartitionedSinkExec"))
        .unwrap_or_else(|| panic!("plan:\n{analyzed}"));
    assert!(
        sink_line.contains("rows_written"),
        "sink line without write metrics: {sink_line}\nplan:\n{analyzed}"
    );
    let mut listed: Vec<String> = std::fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().path().to_str().unwrap().to_string())
        .collect();
    listed.sort();
    let paths: Vec<String> = (0..3)
        .map(|index| {
            OutputFormat::VORTEX.partition_file_path(directory.to_str().unwrap(), index, 3)
        })
        .collect();
    assert_eq!(listed, paths);
    // The one check of the naming scheme itself, against ADR 0015 rather than the predictor.
    let names: Vec<&str> = paths
        .iter()
        .map(|path| path.rsplit('/').next().unwrap())
        .collect();
    assert_eq!(names, ["0.vortex", "1.vortex", "2.vortex"]);
    assert_eq!(
        paths
            .iter()
            .map(|path| rows_of(&read_back(path, FixtureFormat::Vortex)).len())
            .sum::<usize>(),
        32
    );
}

/// A measured write writes the rows a plain write does and records the run beside them, in two
/// Parquet tables named by the run id. The run record's settings columns are the run's resolved
/// settings and its durations are positive, the whole run taking at least as long as execution.
/// Its peak resident set size is positive and, being the whole process's, no smaller than the
/// output it wrote holds in memory. Every metric the plan reported has a column, so the outcome
/// names none.
#[test]
fn a_measured_write_writes_the_output_and_its_run_record() {
    let dir = tempfile::tempdir().unwrap();
    let before = SystemTime::now();
    let measured = measured_grouped_merge(dir.path(), "run-a");
    let after = SystemTime::now();

    let Outcome::Measured {
        rows_written,
        unrecorded_metrics,
    } = measured.outcome
    else {
        panic!("expected a measured write, got {:?}", measured.outcome);
    };
    assert_eq!(rows_written, 24);
    assert_eq!(unrecorded_metrics, Vec::<String>::new());
    let reader = SerializedFileReader::try_from(measured.output_path.as_path()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 24);

    let record = recorded(&measured.metrics_directory, "run-a")
        .record
        .unwrap();
    assert_eq!(record.num_rows(), 1);
    for (column, expected) in [
        ("run_id", "run-a"),
        ("formulation", "grouped-merge"),
        ("groups", "2"),
        ("split_points", ""),
        ("dataset_path", measured.input.table_path()),
        ("input_format", "vortex"),
        ("output_format", "parquet"),
        ("compression", "snappy"),
        ("threads", "1"),
        ("input_tables", "3"),
        ("samples", "3"),
        ("output_path", measured.output_path.to_str().unwrap()),
        ("rows_written", "24"),
    ] {
        assert_eq!(string_values(&record, column), [expected], "{column}");
    }
    assert_eq!(u64_values(&record, "groups"), [Some(2)]);
    let started_at = timestamp_values(&record, "started_at")[0].unwrap();
    let nanos = |time: SystemTime| {
        i64::try_from(
            time.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap()
    };
    assert!(
        nanos(before) <= started_at && started_at <= nanos(after),
        "{started_at} outside {}..={}",
        nanos(before),
        nanos(after)
    );
    let run_ns = u64_values(&record, "run_ns")[0].unwrap();
    let execute_ns = u64_values(&record, "execute_ns")[0].unwrap();
    assert!(execute_ns > 0, "{record:?}");
    assert!(run_ns >= execute_ns, "{record:?}");
    let peak_rss_bytes = u64_values(&record, "peak_rss_bytes")[0].unwrap();
    let output_bytes: usize = read_back(
        measured.output_path.to_str().unwrap(),
        FixtureFormat::Parquet,
    )
    .iter()
    .map(RecordBatch::get_array_memory_size)
    .sum();
    assert!(peak_rss_bytes > 0, "{record:?}");
    assert!(
        peak_rss_bytes >= u64::try_from(output_bytes).unwrap(),
        "peak rss {peak_rss_bytes} smaller than the output's {output_bytes} bytes"
    );
}

/// The run metrics of a measured write list the operators the explained write lists, in the same
/// pre-order, ending in the file sink at the root, and every row carries the run id.
#[test]
fn a_measured_write_records_the_operators_of_the_explained_plan() {
    let dir = tempfile::tempdir().unwrap();
    let measured = measured_grouped_merge(dir.path(), "run-a");
    let explained = expect_plan(
        run(
            grouped_merge(2),
            measured.input.table_path(),
            measured.input.input_format(),
            Action::Explain {
                write: Some(measured.write()),
            },
            Some(three_input_tables()),
            None,
        )
        .unwrap(),
    );

    let metrics = recorded(&measured.metrics_directory, "run-a")
        .metrics
        .unwrap();
    assert!(
        string_values(&metrics, "run_id")
            .iter()
            .all(|run_id| run_id == "run-a"),
        "{metrics:?}"
    );
    let mut operators_by_node: Vec<(Option<u64>, String)> = u64_values(&metrics, "node")
        .into_iter()
        .zip(string_values(&metrics, "operator"))
        .collect();
    operators_by_node.dedup();
    let operators: Vec<&str> = operators_by_node
        .iter()
        .map(|(_, operator)| operator.as_str())
        .collect();
    assert_eq!(operators, exec_names(&explained), "plan:\n{explained}");
    // Three samples in two groups: one group merge, the other group its lone scan, and the
    // final merge.
    assert_eq!(
        operators
            .iter()
            .filter(|&&operator| operator == "SortPreservingMergeExec")
            .count(),
        2
    );
    assert!(
        string_values(&metrics, "display")[0].starts_with("DataSinkExec: sink=ParquetSink"),
        "{metrics:?}"
    );
    assert_eq!(u64_values(&metrics, "parent")[0], None);
}

/// Every formulation of both combiners takes a measured write, the allele combiner included: the
/// rows written are the plain write's, and both tables appear under the metrics directory.
#[test]
fn every_combiner_takes_a_measured_write() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);

    for (formulation, run_id, expected) in [
        (Formulation::CombineAllelesUnion, "alleles", 8),
        (Formulation::CombineRefsUnion, "refs", 32),
    ] {
        let metrics_directory = MetricsDirectory::new(dir.path().join("metrics").to_str().unwrap());
        let outcome = run(
            formulation,
            input.table_path(),
            input.input_format(),
            Action::MeasuredWrite {
                write: WriteTarget {
                    output_path: dir
                        .path()
                        .join(format!("{run_id}.vortex"))
                        .to_str()
                        .unwrap()
                        .to_string(),
                    output_format: OutputFormat::VORTEX,
                },
                metrics_directory: metrics_directory.clone(),
                run_id: run_id.to_string(),
            },
            None,
            None,
        )
        .unwrap();

        let Outcome::Measured { rows_written, .. } = outcome else {
            panic!("{run_id}: expected a measured write, got {outcome:?}");
        };
        assert_eq!(rows_written, expected, "{run_id}");
        let RecordedRun {
            record, metrics, ..
        } = recorded(&metrics_directory, run_id);
        for (table, batch) in [("runs", record.unwrap()), ("metrics", metrics.unwrap())] {
            assert!(
                string_values(&batch, "run_id")
                    .iter()
                    .all(|id| id == run_id),
                "{run_id} {table}: {batch:?}"
            );
        }
    }
}

/// A measured grouped-merge write of three samples, as snappy Parquet, and what it was given.
struct MeasuredGroupedMerge {
    input: fixture::DiskDatasetFixture,
    output_path: std::path::PathBuf,
    metrics_directory: MetricsDirectory,
    outcome: Outcome,
}

impl MeasuredGroupedMerge {
    fn write(&self) -> WriteTarget {
        WriteTarget {
            output_path: self.output_path.to_str().unwrap().to_string(),
            output_format: OutputFormat::PARQUET.with_compression("snappy").unwrap(),
        }
    }
}

/// The first three fixture input tables, as the names a run is narrowed to.
fn three_input_tables() -> Vec<String> {
    INPUT_TABLES[..3].iter().map(ToString::to_string).collect()
}

/// Runs a measured grouped-merge write of three samples of the contig-position Vortex fixture
/// into `dir`, its output beside its metrics directory.
fn measured_grouped_merge(dir: &Path, run_id: &str) -> MeasuredGroupedMerge {
    let mut measured = MeasuredGroupedMerge {
        input: fixture::contig_position_disk_fixture(FixtureFormat::Vortex),
        output_path: dir.join("measured.parquet"),
        metrics_directory: MetricsDirectory::new(dir.join("metrics").to_str().unwrap()),
        outcome: Outcome::RowsWritten(0),
    };
    measured.outcome = run(
        grouped_merge(2),
        measured.input.table_path(),
        measured.input.input_format(),
        Action::MeasuredWrite {
            write: measured.write(),
            metrics_directory: measured.metrics_directory.clone(),
            run_id: run_id.to_string(),
        },
        Some(three_input_tables()),
        None,
    )
    .unwrap();
    measured
}

/// A measured write refuses a run id that already has a run record under its metrics directory
/// before it discovers the dataset, so the refusal is the error even when the dataset path names
/// nothing. It writes nothing: the second output path is never written, and the first run's
/// tables read back as they were.
#[test]
fn refuses_a_run_id_that_already_has_a_run_record_before_discovery() {
    let input = memory_dataset();
    let store = MemoryStore::new("refused");
    let url = store.url().as_str().to_string();
    let metrics_directory = MetricsDirectory::new(&format!("{url}metrics"));
    let record_path = metrics_directory.run_record_path("run-a");
    let measured_write = |input_path: String, output: &str| {
        let mut run = measured_run(
            &input,
            format!("{url}{output}"),
            metrics_directory.clone(),
            "run-a",
        );
        run.inputs.input_path = input_path;
        run
    };
    let first = measured_write(input.table_path().to_string(), "first.parquet");
    let second = measured_write(format!("{url}no-such-dataset/"), "second.parquet");
    let directory = metrics_directory.clone();
    let output_store = store.clone();

    let (before, refused, after, second_output) =
        on_memory_stores(&input, &store, move |ctx| async move {
            first.execute_in(&ctx).await?;
            let before = fixture::read_recorded_run(&ctx, &directory, "run-a").await?;
            let refused = second.execute_in(&ctx).await;
            let after = fixture::read_recorded_run(&ctx, &directory, "run-a").await?;
            let second_output = output_store
                .store()
                .head(&ObjectPath::from("second.parquet"))
                .await;
            Ok((before, refused, after, second_output))
        });

    let message = refused.unwrap_err().to_string();
    assert!(message.contains("run id 'run-a'"), "{message}");
    assert!(message.contains(&record_path), "{message}");
    assert!(
        matches!(second_output, Err(object_store::Error::NotFound { .. })),
        "refused write wrote {second_output:?}"
    );
    assert_eq!(after.record, before.record);
    assert_eq!(after.metrics, before.metrics);
}

/// A measured write whose data write fails, here on a store failing every write under the output
/// path, records nothing: neither table gets a file, so a partial run never unions into a history.
#[test]
fn a_failed_data_write_records_nothing() {
    let input = memory_dataset();
    let store = MemoryStore::failing_writes_under("failed-write", "out");
    let url = store.url().as_str().to_string();
    let metrics_directory = MetricsDirectory::new(&format!("{url}metrics"));
    let run = measured_run(
        &input,
        format!("{url}out/combined.parquet"),
        metrics_directory.clone(),
        "run-a",
    );

    let (failed, recorded) = on_memory_stores(&input, &store, move |ctx| async move {
        let failed = run.execute_in(&ctx).await;
        let recorded = fixture::read_recorded_run(&ctx, &metrics_directory, "run-a").await?;
        Ok((failed, recorded))
    });

    let message = failed.unwrap_err().to_string();
    assert!(message.contains("out/combined.parquet"), "{message}");
    let RecordedRun {
        record, metrics, ..
    } = recorded;
    assert!(record.is_none(), "{record:?}");
    assert!(metrics.is_none(), "{metrics:?}");
}

/// A write of every reference formulation, in either format and so in both output layouts, leaves
/// a sample annotation table beside its output after the data, and lays the data out as before.
/// The table holds the run's sample set, the samples of the requested input tables in sorted
/// order, in a non-null view string `s`. The data's stored `s` reads back as the same view type,
/// as the dataset's attached `s` is, so a later run can union the two.
#[test]
fn every_reference_write_leaves_a_sample_annotation_table_beside_its_output() {
    let input = memory_dataset();
    for formulation in [
        Formulation::CombineRefsUnion,
        grouped_merge(2),
        interval_merge("1:3,2:2"),
    ] {
        for (output_format, input_format) in [
            (OutputFormat::PARQUET, InputFormat::PARQUET),
            (OutputFormat::VORTEX, InputFormat::VORTEX),
        ] {
            let extension = output_format.extension();
            let store = MemoryStore::new("annotated");
            let url = store.url().as_str().to_string();
            // The output path, the path that reads its data back, and the data's locations.
            let (output_path, data_path, mut expected) = match formulation.output_layout() {
                OutputLayout::SingleFile => (
                    format!("{url}out/g.{extension}"),
                    format!("{url}out/g.{extension}"),
                    vec![format!("out/g.{extension}")],
                ),
                OutputLayout::FilePerPartition => (
                    format!("{url}out/g"),
                    format!("{url}out/g/"),
                    (0..3)
                        .map(|index| output_format.partition_file_path("out/g", index, 3))
                        .collect(),
                ),
            };
            expected.push(format!("out/g.samples.{extension}"));
            expected.sort();
            let table_path = format!("{url}out/g.samples.{extension}");
            let description = format!("{formulation} {extension}");
            let mut run = whole_dataset_run(
                &input,
                formulation.clone(),
                Action::Write(WriteTarget {
                    output_path,
                    output_format,
                }),
            );
            run.inputs.input_tables = Some(vec![
                INPUT_TABLES[3].to_string(),
                INPUT_TABLES[1].to_string(),
            ]);
            let listed = store.clone();

            let (outcome, locations, table, data) =
                on_memory_stores(&input, &store, move |ctx| async move {
                    let outcome = run.execute_in(&ctx).await?;
                    let locations = listed.locations_under("out").await;
                    let table = fixture::read_file(&ctx, &table_path, &input_format, None).await?;
                    let data = fixture::read_file(&ctx, &data_path, &input_format, None).await?;
                    Ok((outcome, locations, table, data))
                });

            assert!(
                matches!(outcome, Outcome::RowsWritten(16)),
                "{description}: {outcome:?}"
            );
            assert_eq!(
                locations,
                expected
                    .into_iter()
                    .map(ObjectPath::from)
                    .collect::<Vec<_>>(),
                "{description}"
            );
            let schema = table[0].schema();
            assert_eq!(schema.fields().len(), 1, "{description}: {schema:?}");
            let field = schema.field_with_name("s").unwrap();
            assert_eq!(field.data_type(), &DataType::Utf8View, "{description}");
            assert!(!field.is_nullable(), "{description}");
            assert_eq!(
                samples_of(&table),
                [SAMPLES[1], SAMPLES[3]],
                "{description}"
            );
            assert_eq!(
                data[0].schema().field_with_name("s").unwrap().data_type(),
                &DataType::Utf8View,
                "{description}"
            );
        }
    }
}

/// A hierarchy of combiner runs returns what one run over every sample returns. Two first-level
/// runs over disjoint input tables write into one directory, one a single file and the other a
/// directory of one file per interval, and a held-back sample joins them there as a single-sample
/// input table. A second-level run over that directory, with each reference formulation, collects
/// the same loci in the same order as the one-level union, and the same rows.
#[test]
fn a_second_level_run_over_first_level_outputs_returns_the_one_level_unions_rows() {
    for HierarchyRows {
        extension,
        one_level: expected,
        second_level,
    } in hierarchy_rows(&RowOrdering::locus())
    {
        assert_eq!(expected.len(), 32, "{extension}");
        let mut sorted_expected = expected.clone();
        sorted_expected.sort();
        for (formulation, mut rows) in second_level {
            let context = format!("{extension} {formulation}");
            assert_eq!(loci_of(&rows), loci_of(&expected), "{context}");
            rows.sort();
            assert_eq!(rows, sorted_expected, "{context}");
        }
    }
}

/// Under the `(locus, s)` row ordering at both levels, a second-level run over first-level outputs
/// collects the one-level union's rows in the same locus-then-sample order, so a flagged run's
/// written output is an input table a flagged run above it merges without re-sorting.
#[test]
fn a_second_level_run_under_sample_ordering_returns_the_one_level_rows_in_locus_then_sample_order()
{
    for HierarchyRows {
        extension,
        one_level: expected,
        second_level,
    } in hierarchy_rows(&RowOrdering::locus_then_sample())
    {
        assert_eq!(expected.len(), 32, "{extension}");
        let mut sorted_expected = expected.clone();
        sorted_expected.sort();
        for (formulation, mut rows) in second_level {
            let context = format!("{extension} {formulation}");
            assert_eq!(
                locus_samples_of(&rows),
                locus_samples_of(&expected),
                "{context}"
            );
            rows.sort();
            assert_eq!(rows, sorted_expected, "{context}");
        }
    }
}

/// The rendered rows of one output format's hierarchy: those of a one-level union over every
/// sample, and those each reference formulation collects at the second level, by its name.
struct HierarchyRows {
    extension: String,
    one_level: Vec<Vec<String>>,
    second_level: Vec<(String, Vec<Vec<String>>)>,
}

/// For each output format, the rows of a hierarchy whose every run is under `row_ordering`. Two
/// first-level runs over disjoint input tables write into one directory, one a single file and
/// the other a directory of one file per interval, and a held-back sample joins them there as a
/// single-sample input table. Each reference formulation's second-level run reads that directory.
fn hierarchy_rows(row_ordering: &RowOrdering) -> Vec<HierarchyRows> {
    let mut hierarchies = Vec::new();
    for (fixture_format, output_format) in [
        (FixtureFormat::Parquet, OutputFormat::PARQUET),
        (FixtureFormat::Vortex, OutputFormat::VORTEX),
    ] {
        let input = Arc::clone(fixture::dataset_fixture(
            fixture_format,
            LocusRepresentation::ContigPosition,
        ));
        let store = MemoryStore::new("hierarchy");
        let url = store.url().as_str().to_string();
        let extension = output_format.extension().to_string();
        let ordered_run = |formulation, action| {
            let mut run = whole_dataset_run(&input, formulation, action);
            run.inputs.row_ordering = row_ordering.clone();
            run
        };
        let first_level = [
            (
                grouped_merge(2),
                format!("{url}dataset/g0.{extension}"),
                &INPUT_TABLES[..2],
            ),
            (
                interval_merge("1:3,2:2"),
                format!("{url}dataset/g1"),
                &INPUT_TABLES[2..3],
            ),
        ]
        .map(|(formulation, output_path, input_tables)| {
            let mut run = ordered_run(
                formulation,
                Action::Write(WriteTarget {
                    output_path,
                    output_format: output_format.clone(),
                }),
            );
            run.inputs.input_tables = Some(input_tables.iter().map(ToString::to_string).collect());
            run
        });
        let one_level = ordered_run(Formulation::CombineRefsUnion, Action::Collect);
        let second_level = [
            Formulation::CombineRefsUnion,
            grouped_merge(2),
            interval_merge("1:3,2:2"),
        ]
        .map(|formulation| {
            let mut run = ordered_run(formulation, Action::Collect);
            run.inputs.input_path = format!("{url}dataset/");
            run
        });
        let held_back = SAMPLES[3];
        let copied = store.clone();
        let source = Arc::clone(&input);

        let (one_level, second_level) = on_memory_stores(&input, &store, move |ctx| async move {
            for run in first_level {
                run.execute_in(&ctx).await?;
            }
            for file in source.sample_files(held_back).await {
                let bytes = source.store().get(&file.location).await?.bytes().await?;
                let filename = file.location.filename().unwrap();
                copied
                    .store()
                    .put(
                        &ObjectPath::from(format!("dataset/s={held_back}/{filename}")),
                        bytes.into(),
                    )
                    .await?;
            }
            let one_level = expect_batches(one_level.execute_in(&ctx).await?);
            let mut collected = Vec::new();
            for run in second_level {
                let formulation = run.inputs.formulation.to_string();
                collected.push((formulation, expect_batches(run.execute_in(&ctx).await?)));
            }
            Ok((one_level, collected))
        });
        let second_level = second_level
            .into_iter()
            .map(|(formulation, batches)| (formulation, rows_of(&batches)))
            .collect();
        hierarchies.push(HierarchyRows {
            extension,
            one_level: rows_of(&one_level),
            second_level,
        });
    }
    hierarchies
}

/// Under the `(locus, s)` row ordering, every reference formulation collects the mixed dataset's
/// rows in locus-then-sample order. The ordering reaches every kind of input table: its
/// multi-sample file is stored in that order, each file of its multi-sample directory holds one
/// sample, and its single-sample table's sample is attached rather than stored.
#[test]
fn each_reference_formulation_collects_rows_in_locus_then_sample_order_under_sample_ordering() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let input = Arc::clone(fixture::mixed_dataset_fixture(format, representation));
            for formulation in [
                Formulation::CombineRefsUnion,
                grouped_merge(2),
                interval_merge("1:3,2:2"),
            ] {
                let context = format!("{format:?} {representation:?} {formulation}");
                let run = sample_ordered_run(&input, formulation, Action::Collect);
                let batches = on_memory_stores(
                    &input,
                    &MemoryStore::new("sample-ordered-collect"),
                    move |ctx| async move { Ok(expect_batches(run.execute_in(&ctx).await?)) },
                );

                let mut rows = decoded_rows(&batches, representation);
                assert_eq!(
                    locus_sample_keys(&rows),
                    sorted_locus_sample_keys(&expected_mixed_rows()),
                    "{context}"
                );
                rows.sort();
                assert_eq!(rows, expected_mixed_rows(), "{context}");
            }
        }
    }
}

/// Under the `(locus, s)` row ordering, interval-merge writes each locus interval's file in
/// locus-then-sample order, so its file-per-partition output is in the run's row ordering.
#[test]
fn interval_merge_writes_each_interval_in_locus_then_sample_order_under_sample_ordering() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        let representation = LocusRepresentation::ContigPosition;
        let input = Arc::clone(fixture::mixed_dataset_fixture(format, representation));
        let store = MemoryStore::new("sample-ordered-intervals");
        let output_format = match format {
            FixtureFormat::Parquet => OutputFormat::PARQUET,
            FixtureFormat::Vortex => OutputFormat::VORTEX,
        };
        let directory = format!("{}intervals", store.url().as_str());
        let paths = (0..3)
            .map(|index| output_format.partition_file_path(&directory, index, 3))
            .collect::<Vec<_>>();
        let run = sample_ordered_run(
            &input,
            interval_merge("1:3,2:2"),
            Action::Write(WriteTarget {
                output_path: directory,
                output_format,
            }),
        );
        let input_format = input.input_format();
        let read_paths = paths.clone();

        let files = on_memory_stores(&input, &store, move |ctx| async move {
            run.execute_in(&ctx).await?;
            let mut files = Vec::new();
            for path in read_paths {
                files.push(fixture::read_file(&ctx, &path, &input_format, None).await?);
            }
            Ok(files)
        });

        let mut written = Vec::new();
        for (path, batches) in paths.iter().zip(files) {
            let rows = decoded_rows(&batches, representation);
            assert!(!rows.is_empty(), "{format:?}: {path} is empty");
            let keys = locus_sample_keys(&rows);
            assert!(keys.is_sorted(), "{format:?}: {path}: {keys:?}");
            written.extend(rows);
        }
        assert_eq!(
            locus_sample_keys(&written),
            sorted_locus_sample_keys(&expected_mixed_rows()),
            "{format:?}"
        );
    }
}

/// A measured write records the run's row ordering: `locus,s` under sample ordering and `locus`
/// without it, over the same dataset.
#[test]
fn a_measured_write_records_its_row_ordering() {
    let input = Arc::clone(fixture::mixed_dataset_fixture(
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    ));
    let store = MemoryStore::new("recorded-row-ordering");
    let url = store.url().as_str().to_string();
    let metrics_directory = MetricsDirectory::new(&format!("{url}metrics"));
    let runs = [
        ("sample-ordered", RowOrdering::locus_then_sample()),
        ("locus-ordered", RowOrdering::locus()),
    ]
    .map(|(run_id, row_ordering)| {
        let mut run = measured_run(
            &input,
            format!("{url}{run_id}.parquet"),
            metrics_directory.clone(),
            run_id,
        );
        run.inputs.row_ordering = row_ordering;
        run
    });

    let records = on_memory_stores(&input, &store, move |ctx| async move {
        let mut records = Vec::new();
        for run in runs {
            run.execute_in(&ctx).await?;
        }
        for run_id in ["sample-ordered", "locus-ordered"] {
            let recorded = fixture::read_recorded_run(&ctx, &metrics_directory, run_id).await?;
            records.push(recorded.record.expect("a measured write records its run"));
        }
        Ok(records)
    });

    let [sample_ordered, locus_ordered] = records.as_slice() else {
        panic!("expected two records, got {}", records.len());
    };
    assert_eq!(string_values(sample_ordered, "row_ordering"), ["locus,s"]);
    assert_eq!(string_values(locus_ordered, "row_ordering"), ["locus"]);
}

/// Under the `(locus, s)` row ordering, the explained grouped-merge write requires the locus
/// fields followed by `s` of its sink, and stays a merge tree with no sort above its scans.
#[test]
fn an_explained_write_under_sample_ordering_requires_locus_then_sample_and_stays_a_merge_tree() {
    for representation in [
        LocusRepresentation::ContigPosition,
        LocusRepresentation::Packed,
    ] {
        let input = Arc::clone(fixture::mixed_dataset_fixture(
            FixtureFormat::Vortex,
            representation,
        ));
        let store = MemoryStore::new("sample-ordered-explain");
        let explain = sample_ordered_run(
            &input,
            grouped_merge(2),
            Action::Explain {
                write: Some(WriteTarget {
                    output_path: format!("{}combined.parquet", store.url().as_str()),
                    output_format: OutputFormat::PARQUET,
                }),
            },
        );
        let collect = sample_ordered_run(&input, grouped_merge(2), Action::Collect);

        let (explained, schema) = on_memory_stores(&input, &store, move |ctx| async move {
            let explained = expect_plan(explain.execute_in(&ctx).await?);
            let batches = expect_batches(collect.execute_in(&ctx).await?);
            Ok((explained, batches[0].schema()))
        });

        let required = RowOrdering::locus_then_sample()
            .expand(representation)
            .column_names()
            .iter()
            .map(|name| format!("{name}@{} ASC NULLS LAST", schema.index_of(name).unwrap()))
            .collect::<Vec<_>>()
            .join(", ");
        let required = format!("[{required}]");
        let final_merge = explained
            .lines()
            .find(|line| line.contains("SortPreservingMergeExec"))
            .unwrap_or_else(|| panic!("{representation:?}: no merge in\n{explained}"));
        assert!(
            final_merge.contains(&required),
            "{representation:?}: {final_merge}\n{explained}"
        );
        assert!(
            !exec_names(&explained).contains(&"SortExec"),
            "{representation:?}\n{explained}"
        );
    }
}

/// A run of `formulation` over the whole of `input` under the `(locus, s)` row ordering that
/// performs `action`.
fn sample_ordered_run(
    input: &fixture::DatasetFixture,
    formulation: Formulation,
    action: Action,
) -> CombinerRun {
    let mut run = whole_dataset_run(input, formulation, action);
    run.inputs.row_ordering = RowOrdering::locus_then_sample();
    run
}

/// The locus, alleles, and sample of every row of `batches`, in row order.
fn decoded_rows(batches: &[RecordBatch], representation: LocusRepresentation) -> Vec<fixture::Row> {
    batches
        .iter()
        .flat_map(|batch| fixture::decode_rows(batch, representation))
        .collect()
}

/// The locus and sample of each row, in row order.
fn locus_sample_keys(rows: &[fixture::Row]) -> Vec<(Locus, &str)> {
    rows.iter()
        .map(|(locus, _, sample)| (*locus, sample.as_str()))
        .collect()
}

/// Every row of the mixed dataset, sorted.
fn expected_mixed_rows() -> Vec<fixture::Row> {
    let mut rows = SAMPLES
        .iter()
        .flat_map(|&sample| {
            fixture::sample_rows()
                .into_iter()
                .map(move |(locus, alleles)| (locus, alleles.to_string(), sample.to_string()))
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// The locus and sample of each row, in locus-then-sample order.
fn sorted_locus_sample_keys(rows: &[fixture::Row]) -> Vec<(Locus, &str)> {
    let mut keys = locus_sample_keys(rows);
    keys.sort();
    keys
}

/// The locus and sample of each rendered row: every column but the alleles before the trailing
/// sample.
fn locus_samples_of(rows: &[Vec<String>]) -> Vec<Vec<&String>> {
    rows.iter()
        .map(|row| {
            let (sample, rest) = row.split_last().unwrap();
            let (_, locus) = rest.split_last().unwrap();
            locus.iter().chain([sample]).collect()
        })
        .collect()
}

/// A first-level write that replaces an earlier write's output, and whose data write fails, leaves
/// that earlier data without its sample annotation table. A second-level run over the directory
/// it was written into rejects that entry with a plan error naming it, rather than read it as an
/// input table.
#[test]
fn a_second_level_run_rejects_a_first_level_output_whose_data_write_failed() {
    let input = memory_dataset();
    let store = MemoryStore::new("failed-first-level");
    let failing = store.with_failing_writes_under("dataset/g0.vortex");
    let url = store.url().as_str().to_string();
    let first_level = || {
        [
            (
                grouped_merge(2),
                format!("{url}dataset/g0.vortex"),
                &INPUT_TABLES[..2],
            ),
            (
                interval_merge("1:3,2:2"),
                format!("{url}dataset/g1"),
                &INPUT_TABLES[2..],
            ),
        ]
        .map(|(formulation, output_path, input_tables)| {
            let mut run = whole_dataset_run(
                &input,
                formulation,
                Action::Write(WriteTarget {
                    output_path,
                    output_format: OutputFormat::VORTEX,
                }),
            );
            run.inputs.input_tables = Some(input_tables.iter().map(ToString::to_string).collect());
            run
        })
    };
    let [g0, g1] = first_level();
    let [rewritten_g0, _] = first_level();
    let mut second_level =
        whole_dataset_run(&input, Formulation::CombineRefsUnion, Action::Collect);
    second_level.inputs.input_path = format!("{url}dataset/");

    on_memory_stores(&input, &store, move |ctx| async move {
        g0.execute_in(&ctx).await?;
        g1.execute_in(&ctx).await
    });
    let listed = store;
    let (failed, locations, rejected) = on_memory_stores(&input, &failing, move |ctx| async move {
        let failed = rewritten_g0.execute_in(&ctx).await.map(|_| ());
        let locations = listed.locations_under("dataset").await;
        Ok((failed, locations, second_level.execute_in(&ctx).await))
    });

    let message = failed.unwrap_err().to_string();
    assert!(message.contains("dataset/g0.vortex"), "{message}");
    assert_eq!(
        locations,
        [
            "dataset/g0.vortex",
            "dataset/g1.samples.vortex",
            "dataset/g1/0.vortex",
            "dataset/g1/1.vortex",
            "dataset/g1/2.vortex",
        ]
        .map(ObjectPath::from),
    );
    let error = rejected.unwrap_err();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error:?}");
    let message = error.to_string();
    assert!(
        message.contains(&format!("{url}dataset/g0.vortex")),
        "{message}"
    );
    assert!(message.contains("no sample annotation table"), "{message}");
}

/// A measured write leaves a sample annotation table beside its output too, holding the whole
/// sample set when the run does not narrow it.
#[test]
fn a_measured_write_leaves_a_sample_annotation_table_beside_its_output() {
    let input = memory_dataset();
    let store = MemoryStore::new("measured");
    let url = store.url().as_str().to_string();
    let metrics_directory = MetricsDirectory::new(&format!("{url}metrics"));
    let run = measured_run(
        &input,
        format!("{url}out/combined.parquet"),
        metrics_directory,
        "run-a",
    );
    let listed = store.clone();

    let (locations, table) = on_memory_stores(&input, &store, move |ctx| async move {
        run.execute_in(&ctx).await?;
        let table = fixture::read_file(
            &ctx,
            &format!("{url}out/combined.samples.parquet"),
            &InputFormat::PARQUET,
            None,
        )
        .await?;
        Ok((listed.locations_under("out").await, table))
    });

    assert_eq!(
        locations,
        [
            ObjectPath::from("out/combined.parquet"),
            ObjectPath::from("out/combined.samples.parquet"),
        ]
    );
    assert_eq!(samples_of(&table), SAMPLES);
}

/// A write or a measured write whose data write fails leaves no sample annotation table, though
/// the store here fails only the data file's write and would take the table's. Nor does it leave
/// the table an earlier write left there, which would mark the partial data complete.
#[test]
fn a_failed_data_write_leaves_no_sample_annotation_table() {
    let input = memory_dataset();
    let store = MemoryStore::failing_writes_under("failed-data", "out/combined.parquet");
    let url = store.url().as_str().to_string();
    let output_path = format!("{url}out/combined.parquet");
    let measured = measured_run(
        &input,
        output_path.clone(),
        MetricsDirectory::new(&format!("{url}metrics")),
        "run-a",
    );
    let plain = whole_dataset_run(
        &input,
        grouped_merge(2),
        Action::Write(WriteTarget {
            output_path,
            output_format: OutputFormat::PARQUET,
        }),
    );

    for run in [plain, measured] {
        let description = format!("{:?}", run.action);
        let listed = store.clone();
        let (failed, locations) = on_memory_stores(&input, &store, move |ctx| async move {
            listed
                .store()
                .put(
                    &ObjectPath::from("out/combined.samples.parquet"),
                    "earlier".into(),
                )
                .await?;
            let failed = run.execute_in(&ctx).await;
            Ok((failed, listed.locations_under("out").await))
        });

        let message = failed.unwrap_err().to_string();
        assert!(
            message.contains("out/combined.parquet"),
            "{description}: {message}"
        );
        assert!(locations.is_empty(), "{description}: {locations:?}");
    }
}

/// A write of one file per partition that executes, plain, measured or analyzed, refuses an output
/// path where a file or a non-empty directory exists, before writing or recording anything:
/// writing there could leave an earlier write's files among its own, which its sample annotation
/// table would mark complete. An unanalyzed explain writes nothing and goes ahead, and a write of
/// one file still replaces the file at its path.
#[test]
fn a_write_of_one_file_per_partition_refuses_an_occupied_output_path() {
    let write: WriteRunOn = |input, target, _| {
        whole_dataset_run(input, interval_merge("1:3,2:2"), Action::Write(target))
    };
    let measured: WriteRunOn = |input, target, url| {
        let action = Action::MeasuredWrite {
            write: target,
            metrics_directory: MetricsDirectory::new(&format!("{url}metrics")),
            run_id: "run-a".to_string(),
        };
        whole_dataset_run(input, interval_merge("1:3,2:2"), action)
    };
    let analyzed: WriteRunOn = |input, target, _| {
        let write = Some(target);
        whole_dataset_run(
            input,
            interval_merge("1:3,2:2"),
            Action::ExplainAnalyze { write },
        )
    };
    let explained: WriteRunOn = |input, target, _| {
        let write = Some(target);
        whole_dataset_run(input, interval_merge("1:3,2:2"), Action::Explain { write })
    };
    let single_file: WriteRunOn =
        |input, target, _| whole_dataset_run(input, grouped_merge(2), Action::Write(target));

    let input = memory_dataset();
    for (name, run_on, output, existing, refused) in [
        (
            "write",
            write,
            "out/g",
            "out/g/1.parquet",
            Some("a non-empty directory"),
        ),
        ("file", write, "out/g", "out/g", Some("a file")),
        (
            "measured",
            measured,
            "out/g",
            "out/g/1.parquet",
            Some("a non-empty directory"),
        ),
        (
            "analyzed",
            analyzed,
            "out/g",
            "out/g/1.parquet",
            Some("a non-empty directory"),
        ),
        ("explained", explained, "out/g", "out/g/1.parquet", None),
        (
            "single-file",
            single_file,
            "out/g.parquet",
            "out/g.parquet",
            None,
        ),
    ] {
        let store = MemoryStore::new(name);
        let url = store.url().as_str().to_string();
        let output_path = format!("{url}{output}");
        let target = WriteTarget {
            output_path: output_path.clone(),
            output_format: OutputFormat::PARQUET,
        };
        let run = run_on(&input, target, &url);
        let listed = store.clone();

        let (result, locations) = on_memory_stores(&input, &store, move |ctx| async move {
            listed
                .store()
                .put(&ObjectPath::from(existing), "earlier".into())
                .await?;
            let result = run.execute_in(&ctx).await;
            Ok((result, listed.locations_under("").await))
        });

        match refused {
            Some(existing_as) => {
                let message = result.unwrap_err().to_string();
                assert!(
                    message.contains(&format!(
                        "the output path '{output_path}' already exists as {existing_as}"
                    )),
                    "{name}: {message}"
                );
                assert_eq!(locations, [ObjectPath::from(existing)], "{name}");
            }
            None => {
                result.unwrap();
            }
        }
    }
}

/// Builds a run of `input` that writes to the given target, on a store at the given URL.
type WriteRunOn = fn(&fixture::DatasetFixture, WriteTarget, &str) -> CombinerRun;

/// Only a write or a measured write of the reference combiner leaves a sample annotation table. A
/// written probe removes its output and leaves no table beside it, an explain analyze performs the
/// write without one, and the allele combiner's write writes only its data. Each still removes the
/// table an earlier write left beside its output path, which would otherwise mark data it did not
/// write as complete. A collect names no output path, so it has nowhere to leave one.
#[test]
fn a_probe_an_explain_analyze_or_an_allele_write_leaves_no_sample_annotation_table() {
    let probe: WriteRunOn = |input, target, url| {
        written_probe_run(
            input,
            target,
            MetricsDirectory::new(&format!("{url}metrics")),
            "run-a",
            ProbeSettings::default(),
        )
    };
    let explain_analyze: WriteRunOn = |input, target, _| {
        let write = Some(target);
        whole_dataset_run(input, grouped_merge(2), Action::ExplainAnalyze { write })
    };
    let allele_write: WriteRunOn = |input, target, _| {
        whole_dataset_run(
            input,
            Formulation::CombineAllelesUnion,
            Action::Write(target),
        )
    };

    let input = memory_dataset();
    for (name, run_on, expected) in [
        ("probed", probe, None),
        ("explained", explain_analyze, Some("out/explained.parquet")),
        ("alleles", allele_write, Some("out/alleles.parquet")),
    ] {
        let store = MemoryStore::new(name);
        let url = store.url().as_str().to_string();
        let target = WriteTarget {
            output_path: format!("{url}out/{name}.parquet"),
            output_format: OutputFormat::PARQUET,
        };
        let run = run_on(&input, target, &url);
        let listed = store.clone();

        let locations = on_memory_stores(&input, &store, move |ctx| async move {
            let earlier = ObjectPath::from(format!("out/{name}.samples.parquet"));
            listed.store().put(&earlier, "earlier".into()).await?;
            run.execute_in(&ctx).await?;
            Ok(listed.locations_under("out").await)
        });

        let expected = expected
            .map(ObjectPath::from)
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(locations, expected, "{name}");
    }
}

/// A drained probe of every formulation runs the plan to its end, which the fixture reaches in
/// well under the maximum duration, so it completes at the first partition end, having received
/// every row. It records all three tables: a run record whose probe columns say so and whose
/// write columns are empty, the run metrics, and progress samples whose rows only increase to
/// the rows received.
#[test]
fn a_drained_probe_of_every_formulation_completes_and_records_three_tables() {
    let input = memory_dataset();
    for (formulation, rows) in [
        (Formulation::CombineAllelesUnion, 8),
        (Formulation::CombineRefsUnion, 32),
        (grouped_merge(2), 32),
        (interval_merge("1:3,2:2"), 32),
    ] {
        let store = MemoryStore::new("probed");
        let directory = MetricsDirectory::new(&format!("{}metrics", store.url().as_str()));
        let mut run = probe_run(&input, directory.clone(), "run-a");
        run.inputs.formulation = formulation.clone();
        run.inputs.row_ordering = formulation.required_ordering();

        let (outcome, recorded) = on_memory_stores(&input, &store, move |ctx| async move {
            let outcome = run.execute_in(&ctx).await?;
            Ok((
                outcome,
                fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
            ))
        });

        let Outcome::Probed {
            rows_received,
            steady_state_throughput,
            stop_reason,
            unrecorded_metrics,
        } = outcome
        else {
            panic!("{formulation}: expected a probe, got {outcome:?}");
        };
        assert_eq!(rows_received, rows, "{formulation}");
        assert_eq!(stop_reason, StopReason::Completed, "{formulation}");
        assert!(
            steady_state_throughput.is_some_and(|throughput| throughput > 0.0),
            "{formulation}: {steady_state_throughput:?}"
        );
        assert_eq!(unrecorded_metrics, Vec::<String>::new(), "{formulation}");

        let record = recorded.record.unwrap();
        for (column, expected) in [
            ("run_id", "run-a"),
            ("action", "probe"),
            ("stop_reason", "completed"),
            ("rows_written", &rows.to_string()),
            ("window_rows", &rows.to_string()),
            ("warmup_end_ns", ""),
            ("poll_period_ns", "100000000"),
            ("batch_duration_ns", "1000000000"),
            ("precision", "0.02"),
            ("consecutive_checks", "3"),
            ("window_groups", "10"),
            ("min_duration_ns", "20000000000"),
            ("max_duration_ns", "300000000000"),
            ("output_path", ""),
            ("output_format", ""),
        ] {
            assert_eq!(
                string_values(&record, column),
                [expected],
                "{formulation}: {column}"
            );
        }
        let window_end_ns = u64_values(&record, "window_end_ns")[0].unwrap();
        assert_eq!(
            u64_values(&record, "first_partition_end_ns"),
            [Some(window_end_ns)],
            "{formulation}"
        );
        let execute_ns = u64_values(&record, "execute_ns")[0].unwrap();
        assert!(execute_ns >= window_end_ns, "{formulation}");

        let samples = recorded.progress_samples.unwrap();
        assert!(
            string_values(&samples, "run_id")
                .iter()
                .all(|id| id == "run-a"),
            "{formulation}"
        );
        let sample_rows: Vec<u64> = u64_values(&samples, "rows")
            .into_iter()
            .map(Option::unwrap)
            .collect();
        assert!(sample_rows.len() >= 2, "{formulation}: {sample_rows:?}");
        assert!(
            sample_rows.windows(2).all(|pair| pair[0] <= pair[1]),
            "{formulation}: {sample_rows:?}"
        );
        assert_eq!(sample_rows.last(), Some(&rows), "{formulation}");
        assert_eq!(
            u64_values(&samples, "elapsed_ns").last(),
            Some(&Some(window_end_ns)),
            "{formulation}"
        );
        assert!(recorded.metrics.unwrap().num_rows() > 0, "{formulation}");
    }
}

/// A recorded probe replays exactly. Deciding after each of its recorded progress samples, as the
/// probe did, with the settings its run record holds and the first partition end revealed from
/// the sample that showed it, keeps running until the last sample and then reaches the decision
/// its run record holds. So the tables hold every input the live decision had, and the samples it
/// had, no more and no fewer. The settings are off their defaults, and short enough that MSER
/// finds an end of warmup in a fixture's few milliseconds. A fixture's probe always completes, so
/// the settings only the steady check reads show only if they would have stopped it sooner.
#[test]
fn a_recorded_probe_replays_to_its_recorded_decision() {
    let input = memory_dataset();
    let settings = ProbeSettings {
        poll_period: Duration::from_millis(2),
        batch_duration: Duration::from_millis(1),
        precision: 1.0,
        consecutive_checks: NonZeroU32::new(2).unwrap(),
        window_groups: 3,
        min_duration: Duration::from_millis(1),
        max_duration: Duration::from_secs(60),
    };
    for formulation in [
        Formulation::CombineAllelesUnion,
        Formulation::CombineRefsUnion,
        grouped_merge(2),
        interval_merge("1:3,2:2"),
    ] {
        let store = MemoryStore::new("replayed");
        let directory = MetricsDirectory::new(&format!("{}metrics", store.url().as_str()));
        let mut run = probe_run(&input, directory.clone(), "run-a");
        run.inputs.formulation = formulation.clone();
        run.inputs.row_ordering = formulation.required_ordering();
        run.action = Action::Probe {
            write: None,
            metrics_directory: directory.clone(),
            run_id: "run-a".to_string(),
            settings: settings.clone(),
            kind: ProbeKind::Probe,
        };

        let (outcome, recorded) = on_memory_stores(&input, &store, move |ctx| async move {
            let outcome = run.execute_in(&ctx).await?;
            Ok((
                outcome,
                fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
            ))
        });

        let record = recorded.record.unwrap();
        let progress = recorded.progress_samples.unwrap();
        let samples: Vec<ProgressSample> = u64_values(&progress, "elapsed_ns")
            .into_iter()
            .zip(u64_values(&progress, "rows"))
            .map(|(elapsed_ns, rows)| ProgressSample {
                elapsed_ns: elapsed_ns.unwrap(),
                rows: rows.unwrap(),
            })
            .collect();
        let recorded_settings = recorded_probe_settings(&record);
        let first_partition_end_ns = u64_values(&record, "first_partition_end_ns")[0];
        let (taken, replayed) = (1..=samples.len())
            .find_map(|taken| {
                let seen = samples.get(..taken)?;
                let latest_ns = seen.last()?.elapsed_ns;
                throughput_probe::decide(
                    &recorded_settings,
                    seen,
                    first_partition_end_ns.filter(|&end_ns| end_ns <= latest_ns),
                )
                .map(|decision| (taken, decision))
            })
            .unwrap_or_else(|| panic!("{formulation}: the replay keeps running over {samples:?}"));
        assert_eq!(taken, samples.len(), "{formulation}: {samples:?}");

        let recorded_decision = Decision {
            stop_reason: replayed.stop_reason,
            steady_state_throughput: f64_values(&record, "steady_state_throughput")[0],
            warmup_end_ns: u64_values(&record, "warmup_end_ns")[0],
            window_end_ns: u64_values(&record, "window_end_ns")[0].unwrap(),
            window_rows: u64_values(&record, "window_rows")[0].unwrap(),
            relative_half_width: f64_values(&record, "relative_half_width")[0],
        };
        assert_eq!(replayed, recorded_decision, "{formulation}");
        assert_eq!(
            string_values(&record, "stop_reason"),
            [replayed.stop_reason.name()],
            "{formulation}"
        );
        let Outcome::Probed {
            steady_state_throughput,
            stop_reason,
            ..
        } = outcome
        else {
            panic!("{formulation}: expected a probe, got {outcome:?}");
        };
        assert_eq!(
            (stop_reason, steady_state_throughput),
            (replayed.stop_reason, replayed.steady_state_throughput),
            "{formulation}"
        );
    }
}

/// The probe settings a run record holds.
fn recorded_probe_settings(record: &RecordBatch) -> ProbeSettings {
    let duration = |column| Duration::from_nanos(u64_values(record, column)[0].unwrap());
    let count = |column| u32::try_from(u64_values(record, column)[0].unwrap()).unwrap();
    ProbeSettings {
        poll_period: duration("poll_period_ns"),
        batch_duration: duration("batch_duration_ns"),
        precision: f64_values(record, "precision")[0].unwrap(),
        consecutive_checks: NonZeroU32::new(count("consecutive_checks")).unwrap(),
        window_groups: count("window_groups"),
        min_duration: duration("min_duration_ns"),
        max_duration: duration("max_duration_ns"),
    }
}

/// A probe refuses a run id that already has a run record before it discovers the dataset, and
/// leaves the recorded run's tables as they were.
#[test]
fn a_probe_refuses_a_run_id_that_already_has_a_run_record_before_discovery() {
    let input = memory_dataset();
    let store = MemoryStore::new("probe-refused");
    let url = store.url().as_str().to_string();
    let directory = MetricsDirectory::new(&format!("{url}metrics"));
    let first = probe_run(&input, directory.clone(), "run-a");
    let mut second = probe_run(&input, directory.clone(), "run-a");
    second.inputs.input_path = format!("{url}no-such-dataset/");

    let (before, refused, after) = on_memory_stores(&input, &store, move |ctx| async move {
        first.execute_in(&ctx).await?;
        let before = fixture::read_recorded_run(&ctx, &directory, "run-a").await?;
        let refused = second.execute_in(&ctx).await;
        let after = fixture::read_recorded_run(&ctx, &directory, "run-a").await?;
        Ok((before, refused, after))
    });

    let message = refused.unwrap_err().to_string();
    assert!(message.contains("run id 'run-a'"), "{message}");
    assert!(before.record.is_some());
    assert_eq!(after.record, before.record);
    assert_eq!(after.metrics, before.metrics);
    assert_eq!(after.progress_samples, before.progress_samples);
}

/// A probe whose progress samples write fails, the first of its three, records nothing.
#[test]
fn a_probe_whose_progress_samples_write_fails_records_nothing() {
    let input = memory_dataset();
    let store = MemoryStore::failing_writes_under("probe-fails", "metrics/progress");
    let directory = MetricsDirectory::new(&format!("{}metrics", store.url().as_str()));
    let run = probe_run(&input, directory.clone(), "run-a");

    let (failed, recorded) = on_memory_stores(&input, &store, move |ctx| async move {
        let failed = run.execute_in(&ctx).await;
        let recorded = fixture::read_recorded_run(&ctx, &directory, "run-a").await?;
        Ok((failed, recorded))
    });

    let message = failed.unwrap_err().to_string();
    assert!(message.contains("metrics/progress"), "{message}");
    let RecordedRun {
        record,
        metrics,
        progress_samples,
    } = recorded;
    assert!(record.is_none(), "{record:?}");
    assert!(metrics.is_none(), "{metrics:?}");
    assert!(progress_samples.is_none(), "{progress_samples:?}");
}

/// A written probe of each output layout writes through the sink a plain write chooses, and
/// records all three tables with the write's columns filled. Grouped-merge writes one Parquet
/// file; interval-merge writes a Vortex file per interval, whose progress samples count the rows
/// of the operator beneath the partition sinks, and whose first finished interval closes the
/// window and stops the probe. Either way nothing is left under the output path.
#[test]
fn a_written_probe_of_each_output_layout_writes_through_the_plain_writes_sink_and_keeps_nothing() {
    let input = memory_dataset();
    for (formulation, output, output_format, sink) in [
        (
            grouped_merge(2),
            "out/combined.parquet",
            OutputFormat::PARQUET,
            "DataSinkExec: sink=ParquetSink",
        ),
        (
            interval_merge("1:3,2:2"),
            "out/combined",
            OutputFormat::VORTEX,
            "PartitionedSinkExec: partitions=3, sink=VortexSink",
        ),
    ] {
        let store = MemoryStore::new("written-probe");
        let url = store.url().as_str().to_string();
        let directory = MetricsDirectory::new(&format!("{url}metrics"));
        let output_path = format!("{url}{output}");
        let mut run = written_probe_run(
            &input,
            WriteTarget {
                output_path: output_path.clone(),
                output_format: output_format.clone(),
            },
            directory.clone(),
            "run-a",
            ProbeSettings::default(),
        );
        run.inputs.formulation = formulation.clone();
        run.inputs.row_ordering = formulation.required_ordering();

        let (outcome, recorded) = on_memory_stores(&input, &store, move |ctx| async move {
            let outcome = run.execute_in(&ctx).await?;
            Ok((
                outcome,
                fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
            ))
        });

        let Outcome::Probed {
            rows_received,
            stop_reason,
            ..
        } = outcome
        else {
            panic!("{formulation}: expected a probe, got {outcome:?}");
        };
        // Grouped-merge's one partition ends with every row. Interval-merge's first interval to
        // finish stops the probe, however far the others got.
        match formulation {
            Formulation::CombineRefsIntervalMerge { .. } => {
                assert!((1..=32).contains(&rows_received), "{rows_received}");
            }
            _ => assert_eq!(rows_received, 32, "{formulation}"),
        }
        assert_eq!(stop_reason, StopReason::Completed, "{formulation}");

        let record = recorded.record.unwrap();
        for (column, expected) in [
            ("action", "probe"),
            ("stop_reason", "completed"),
            ("rows_written", &rows_received.to_string()),
            ("output_path", output_path.as_str()),
            ("output_format", output_format.name()),
        ] {
            assert_eq!(
                string_values(&record, column),
                [expected],
                "{formulation}: {column}"
            );
        }
        let window_end_ns = u64_values(&record, "window_end_ns")[0].unwrap();
        assert_eq!(
            u64_values(&record, "first_partition_end_ns"),
            [Some(window_end_ns)],
            "{formulation}"
        );
        let samples = recorded.progress_samples.unwrap();
        let sample_rows = u64_values(&samples, "rows");
        assert!(
            sample_rows
                .last()
                .is_some_and(|rows| rows.is_some_and(|rows| rows > 0 && rows <= rows_received)),
            "{formulation}: {sample_rows:?}"
        );
        let metrics = recorded.metrics.unwrap();
        let root = &string_values(&metrics, "display")[0];
        assert!(root.starts_with(sink), "{formulation}: {root}");

        assert_eq!(
            block_on(store.locations_under("out")),
            Vec::<ObjectPath>::new(),
            "{formulation}"
        );
    }
}

/// A written probe capped before its plan could finish, and one whose data write fails, both leave
/// nothing under the output path. A failed probe also records nothing, whether every write failed
/// or, under interval-merge, only one interval's file did.
#[test]
fn a_capped_or_failed_written_probe_keeps_nothing() {
    let input = memory_dataset();
    let capped = MemoryStore::new("capped-probe");
    let url = capped.url().as_str().to_string();
    let directory = MetricsDirectory::new(&format!("{url}metrics"));
    let mut run = written_probe_run(
        &input,
        WriteTarget {
            output_path: format!("{url}out/combined"),
            output_format: OutputFormat::VORTEX,
        },
        directory,
        "run-a",
        ProbeSettings {
            max_duration: Duration::from_nanos(1),
            ..ProbeSettings::default()
        },
    );
    run.inputs.formulation = interval_merge("1:3,2:2");
    let outcome = on_memory_stores(&input, &capped, move |ctx| async move {
        run.execute_in(&ctx).await
    });
    let Outcome::Probed { stop_reason, .. } = outcome else {
        panic!("expected a probe, got {outcome:?}");
    };
    assert_eq!(stop_reason, StopReason::Capped);
    assert_eq!(
        block_on(capped.locations_under("out")),
        Vec::<ObjectPath>::new()
    );

    for (formulation, output_path, output_format, failing_prefix) in [
        (
            grouped_merge(2),
            "out/combined.parquet",
            OutputFormat::PARQUET,
            "out",
        ),
        (
            interval_merge("1:3,2:2"),
            "out/combined",
            OutputFormat::VORTEX,
            "out/combined/2.vortex",
        ),
    ] {
        let failing = MemoryStore::failing_writes_under("failed-probe", failing_prefix);
        let url = failing.url().as_str().to_string();
        let directory = MetricsDirectory::new(&format!("{url}metrics"));
        let mut run = written_probe_run(
            &input,
            WriteTarget {
                output_path: format!("{url}{output_path}"),
                output_format,
            },
            directory.clone(),
            "run-a",
            ProbeSettings::default(),
        );
        run.inputs.formulation = formulation.clone();
        run.inputs.row_ordering = formulation.required_ordering();
        let (failed, recorded) = on_memory_stores(&input, &failing, move |ctx| async move {
            let failed = run.execute_in(&ctx).await;
            let recorded = fixture::read_recorded_run(&ctx, &directory, "run-a").await?;
            Ok((failed, recorded))
        });
        let message = failed.unwrap_err().to_string();
        assert!(message.contains(failing_prefix), "{formulation}: {message}");
        let RecordedRun {
            record,
            metrics,
            progress_samples,
        } = recorded;
        assert!(record.is_none(), "{formulation}: {record:?}");
        assert!(metrics.is_none(), "{formulation}: {metrics:?}");
        assert!(
            progress_samples.is_none(),
            "{formulation}: {progress_samples:?}"
        );
        assert_eq!(
            block_on(failing.locations_under("out")),
            Vec::<ObjectPath>::new(),
            "{formulation}"
        );
    }
}

/// A written probe refuses a local output path that is a symlink, dangling or to an empty
/// directory, before it discovers the dataset, and leaves the link as it was. A write would
/// replace a dangling link, which its removal would then delete, or write through a link to a
/// directory.
#[test]
fn a_written_probe_refuses_a_symlinked_local_output_path_before_discovery() {
    let input = memory_dataset();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("empty")).unwrap();
    for (name, target) in [("dangling", "nowhere"), ("to-empty", "empty")] {
        let link = dir.path().join(name);
        let target = dir.path().join(target);
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let output_path = link.to_str().unwrap().to_string();
        let mut run = written_probe_run(
            &input,
            WriteTarget {
                output_path: output_path.clone(),
                output_format: OutputFormat::PARQUET,
            },
            MetricsDirectory::new(dir.path().join("metrics").to_str().unwrap()),
            "run-a",
            ProbeSettings::default(),
        );
        run.inputs.input_path = dir
            .path()
            .join("no-such-dataset/")
            .to_str()
            .unwrap()
            .to_string();

        let message = run.execute().unwrap_err().to_string();

        assert!(message.contains(&output_path), "{name}: {message}");
        assert!(
            message.contains("already exists as a symlink"),
            "{name}: {message}"
        );
        assert_eq!(std::fs::read_link(&link).unwrap(), target, "{name}");
    }
}

/// A written probe refuses a metrics directory under its output path, whose records the removal
/// of its output would reach, before it discovers the dataset.
#[test]
fn a_written_probe_refuses_a_metrics_directory_under_its_output_path_before_discovery() {
    let input = memory_dataset();
    let store = MemoryStore::new("metrics-under-output");
    let url = store.url().as_str().to_string();
    let mut run = written_probe_run(
        &input,
        WriteTarget {
            output_path: format!("{url}out"),
            output_format: OutputFormat::VORTEX,
        },
        MetricsDirectory::new(&format!("{url}out/metrics")),
        "run-a",
        ProbeSettings::default(),
    );
    run.inputs.input_path = format!("{url}no-such-dataset/");

    let refused = on_memory_stores(&input, &store, move |ctx| async move {
        Ok(run.execute_in(&ctx).await)
    });

    let message = refused.unwrap_err().to_string();
    assert!(message.contains("metrics directory"), "{message}");
    assert!(message.contains(&format!("{url}out/metrics")), "{message}");
}

/// A written probe refuses an output path that already exists, as a file or as a non-empty
/// directory, before it discovers the dataset, and leaves what is there untouched.
#[test]
fn a_written_probe_refuses_an_existing_output_path_before_discovery() {
    let input = memory_dataset();
    for existing in ["out/combined", "out/combined/0.vortex"] {
        let store = MemoryStore::new("probe-output-exists");
        block_on(
            store
                .store()
                .put(&ObjectPath::from(existing), "kept".into()),
        )
        .unwrap();
        let url = store.url().as_str().to_string();
        let output_path = format!("{url}out/combined");
        let mut run = written_probe_run(
            &input,
            WriteTarget {
                output_path: output_path.clone(),
                output_format: OutputFormat::VORTEX,
            },
            MetricsDirectory::new(&format!("{url}metrics")),
            "run-a",
            ProbeSettings::default(),
        );
        run.inputs.formulation = interval_merge("1:3,2:2");
        run.inputs.input_path = format!("{url}no-such-dataset/");

        let refused = on_memory_stores(&input, &store, move |ctx| async move {
            Ok(run.execute_in(&ctx).await)
        });

        let message = refused.unwrap_err().to_string();
        assert!(message.contains(&output_path), "{existing}: {message}");
        assert!(message.contains("already exists"), "{existing}: {message}");
        assert_eq!(
            block_on(store.locations_under("out")),
            [ObjectPath::from(existing)],
            "{existing}"
        );
    }
}

/// A shadow probe runs past a maximum duration it reaches at its first sample, to the end of its
/// plan, so it completes having received every row. Its rule never stopped steady over a fixture
/// this small, so its record says it is a shadow probe and leaves the would-stop columns empty.
#[test]
fn a_shadow_probe_runs_past_its_maximum_duration_to_completion_without_a_would_stop() {
    let input = memory_dataset();
    for formulation in [grouped_merge(2), interval_merge("1:3,2:2")] {
        let store = MemoryStore::new("shadowed");
        let directory = MetricsDirectory::new(&format!("{}metrics", store.url().as_str()));
        let mut run = probe_run(&input, directory.clone(), "run-a");
        run.inputs.formulation = formulation.clone();
        run.inputs.row_ordering = formulation.required_ordering();
        run.action = Action::Probe {
            write: None,
            metrics_directory: directory.clone(),
            run_id: "run-a".to_string(),
            settings: ProbeSettings {
                max_duration: Duration::from_nanos(1),
                ..ProbeSettings::default()
            },
            kind: ProbeKind::Shadow,
        };

        let (outcome, recorded) = on_memory_stores(&input, &store, move |ctx| async move {
            let outcome = run.execute_in(&ctx).await?;
            Ok((
                outcome,
                fixture::read_recorded_run(&ctx, &directory, "run-a").await?,
            ))
        });

        let Outcome::Probed {
            rows_received,
            stop_reason,
            ..
        } = outcome
        else {
            panic!("{formulation}: expected a probe, got {outcome:?}");
        };
        assert_eq!(rows_received, 32, "{formulation}");
        assert_eq!(stop_reason, StopReason::Completed, "{formulation}");
        let record = recorded.record.unwrap();
        for (column, expected) in [
            ("action", "shadow"),
            ("stop_reason", "completed"),
            ("rows_written", "32"),
            ("max_duration_ns", "1"),
            ("would_stop_ns", ""),
            ("would_be_steady_state_throughput", ""),
            ("would_be_relative_half_width", ""),
            ("would_be_warmup_end_ns", ""),
        ] {
            assert_eq!(
                string_values(&record, column),
                [expected],
                "{formulation}: {column}"
            );
        }
        let samples = recorded.progress_samples.unwrap();
        assert_eq!(
            u64_values(&samples, "rows").last(),
            Some(&Some(32)),
            "{formulation}"
        );
    }
}

/// A written probe of `input` with `settings` to `write`, a grouped-merge unless the caller swaps
/// the formulation, recorded as `run_id` under `metrics_directory`.
fn written_probe_run(
    input: &fixture::DatasetFixture,
    write: WriteTarget,
    metrics_directory: MetricsDirectory,
    run_id: &str,
    settings: ProbeSettings,
) -> CombinerRun {
    CombinerRun {
        action: Action::Probe {
            write: Some(write),
            metrics_directory,
            run_id: run_id.to_string(),
            settings,
            kind: ProbeKind::Probe,
        },
        ..probe_run(input, MetricsDirectory::new(""), run_id)
    }
}

/// A drained grouped-merge probe of `input` with the default settings, recorded as `run_id` under
/// `metrics_directory`.
fn probe_run(
    input: &fixture::DatasetFixture,
    metrics_directory: MetricsDirectory,
    run_id: &str,
) -> CombinerRun {
    CombinerRun {
        inputs: PlanInputs {
            formulation: grouped_merge(2),
            row_ordering: RowOrdering::locus(),
            input_path: input.table_path().to_string(),
            input_format: input.input_format(),
            input_tables: None,
            row_limit: None,
        },
        action: Action::Probe {
            write: None,
            metrics_directory,
            run_id: run_id.to_string(),
            settings: ProbeSettings::default(),
            kind: ProbeKind::Probe,
        },
        threads: NonZeroUsize::MIN,
    }
}

/// The in-memory contig-position Vortex dataset fixture.
fn memory_dataset() -> Arc<fixture::DatasetFixture> {
    Arc::clone(fixture::dataset_fixture(
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    ))
}

/// A measured grouped-merge write of `input` to Parquet at `output_path`, recorded as `run_id`
/// under `metrics_directory`.
fn measured_run(
    input: &fixture::DatasetFixture,
    output_path: String,
    metrics_directory: MetricsDirectory,
    run_id: &str,
) -> CombinerRun {
    CombinerRun {
        inputs: PlanInputs {
            formulation: grouped_merge(2),
            row_ordering: RowOrdering::locus(),
            input_path: input.table_path().to_string(),
            input_format: input.input_format(),
            input_tables: None,
            row_limit: None,
        },
        action: Action::MeasuredWrite {
            write: WriteTarget {
                output_path,
                output_format: OutputFormat::PARQUET,
            },
            metrics_directory,
            run_id: run_id.to_string(),
        },
        threads: NonZeroUsize::MIN,
    }
}

/// A run of `formulation` over the whole of `input` that performs `action`.
fn whole_dataset_run(
    input: &fixture::DatasetFixture,
    formulation: Formulation,
    action: Action,
) -> CombinerRun {
    CombinerRun {
        inputs: PlanInputs {
            row_ordering: formulation.required_ordering(),
            formulation,
            input_path: input.table_path().to_string(),
            input_format: input.input_format(),
            input_tables: None,
            row_limit: None,
        },
        action,
        threads: NonZeroUsize::MIN,
    }
}

/// The sample ids in the `s` column of `batches`, in row order.
fn samples_of(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| fixture::string_column(batch, "s"))
        .collect()
}

/// Runs `pipeline` on a session serving the dataset fixture `input` and `store`.
fn on_memory_stores<T, Fut>(
    input: &Arc<fixture::DatasetFixture>,
    store: &MemoryStore,
    pipeline: impl FnOnce(SessionContext) -> Fut + Send + 'static,
) -> T
where
    T: Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    let input = Arc::clone(input);
    let store = store.clone();
    pipeline::run(
        move |ctx| {
            input.register(&ctx);
            store.register(&ctx);
            pipeline(ctx)
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

/// Reads back what `directory`, on disk, holds for `run_id`.
fn recorded(directory: &MetricsDirectory, run_id: &str) -> RecordedRun {
    let directory = directory.clone();
    let run_id = run_id.to_string();
    pipeline::run(
        move |ctx| async move { fixture::read_recorded_run(&ctx, &directory, &run_id).await },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

/// Every row of `batches` as its column values rendered in order.
fn rows_of(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    batches
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
        .collect()
}

/// Reads the one file at `path` on disk, written in `format`, back into batches.
fn read_back(path: &str, format: FixtureFormat) -> Vec<RecordBatch> {
    let path = path.to_string();
    let input_format = match format {
        FixtureFormat::Parquet => InputFormat::PARQUET,
        FixtureFormat::Vortex => InputFormat::VORTEX,
    };
    pipeline::run(
        move |ctx| async move { fixture::read_file(&ctx, &path, &input_format, None).await },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

fn expect_plan(outcome: Outcome) -> String {
    let Outcome::Plan(plan) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    plan
}

#[test]
fn restricts_the_dataset_to_the_requested_input_tables() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);
    let output_path = dir.path().join("restricted.vortex");

    let outcome = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Write(WriteTarget {
            output_path: output_path.to_str().unwrap().to_string(),
            output_format: OutputFormat::VORTEX,
        }),
        Some(INPUT_TABLES[..2].iter().map(ToString::to_string).collect()),
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
fn reports_input_tables_absent_from_the_dataset() {
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);

    let err = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Collect,
        Some(vec!["s=NOT_A_SAMPLE".to_string()]),
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Error during planning: input tables not found in dataset: s=NOT_A_SAMPLE"
    );
}

/// The output path of a write, explained or not, is among the paths the run hands the pipeline:
/// one on a store the pipeline cannot serve fails the run before dataset discovery, so the input
/// path here need not exist.
#[test]
fn rejects_an_output_path_on_an_unsupported_store_before_discovery() {
    let write_to_s3 = || WriteTarget {
        output_path: "s3://bucket/out.vortex".to_string(),
        output_format: OutputFormat::VORTEX,
    };

    for action in [
        Action::Write(write_to_s3()),
        Action::Explain {
            write: Some(write_to_s3()),
        },
        Action::ExplainAnalyze {
            write: Some(write_to_s3()),
        },
    ] {
        let description = format!("{action:?}");
        let err = run(
            Formulation::CombineRefsUnion,
            "no-such-dataset",
            InputFormat::VORTEX,
            action,
            None,
            None,
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("s3://bucket/out.vortex"),
            "{description}: {message}"
        );
        assert!(message.contains("gs://"), "{description}: {message}");
    }
}

fn run(
    formulation: Formulation,
    input_path: &str,
    input_format: InputFormat,
    action: Action,
    input_tables: Option<Vec<String>>,
    row_limit: Option<usize>,
) -> Result<Outcome> {
    CombinerRun {
        inputs: PlanInputs {
            row_ordering: formulation.required_ordering(),
            formulation,
            input_path: input_path.to_string(),
            input_format,
            input_tables,
            row_limit,
        },
        action,
        threads: NonZeroUsize::MIN,
    }
    .execute()
}

fn expect_batches(outcome: Outcome) -> Vec<RecordBatch> {
    let Outcome::Batches(batches) = outcome else {
        panic!("expected batches, got {outcome:?}");
    };
    batches
}
