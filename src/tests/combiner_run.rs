use crate::fixture;

use crate::{
    combiner_run::{Action, CombinerRun, Outcome},
    format::{InputFormat, OutputFormat},
    formulation::Formulation,
    locus::{Locus, LocusOrdering, LocusRepresentation},
    metrics_directory::MetricsDirectory,
    pipeline::{self, PipelineOptions},
    tests::{
        plan_shape::exec_names,
        support::{grouped_merge, interval_merge, string_values, timestamp_values, u64_values},
    },
    throughput_probe::{ProbeSettings, StopReason},
    write::WriteTarget,
};
use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    error::Result,
    parquet::file::reader::{FileReader, SerializedFileReader},
    prelude::SessionContext,
};
use object_store::{ObjectStoreExt, path::Path as ObjectPath};
use std::{future::Future, num::NonZeroUsize, path::Path, sync::Arc, time::SystemTime};

use fixture::{FixtureFormat, MemoryStore, RecordedRun, SAMPLES};

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
                let columns = LocusOrdering::locus().expand(representation).column_names();
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
            Some(three_samples()),
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

/// The first three fixture samples, as a run's sample set.
fn three_samples() -> Vec<String> {
    SAMPLES[..3].iter().map(ToString::to_string).collect()
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
        Some(three_samples()),
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
    let measured_write = |input_path: String, output: &str| CombinerRun {
        input_path,
        ..measured_run(
            &input,
            format!("{url}{output}"),
            metrics_directory.clone(),
            "run-a",
        )
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
        let run = CombinerRun {
            formulation: formulation.clone(),
            ..probe_run(&input, directory.clone(), "run-a")
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
            ("poll_period_ns", "100000000"),
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

/// A probe refuses a run id that already has a run record before it discovers the dataset, and
/// leaves the recorded run's tables as they were.
#[test]
fn a_probe_refuses_a_run_id_that_already_has_a_run_record_before_discovery() {
    let input = memory_dataset();
    let store = MemoryStore::new("probe-refused");
    let url = store.url().as_str().to_string();
    let directory = MetricsDirectory::new(&format!("{url}metrics"));
    let first = probe_run(&input, directory.clone(), "run-a");
    let second = CombinerRun {
        input_path: format!("{url}no-such-dataset/"),
        ..probe_run(&input, directory.clone(), "run-a")
    };

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

/// A drained grouped-merge probe of `input` with the default settings, recorded as `run_id` under
/// `metrics_directory`.
fn probe_run(
    input: &fixture::DatasetFixture,
    metrics_directory: MetricsDirectory,
    run_id: &str,
) -> CombinerRun {
    CombinerRun {
        formulation: grouped_merge(2),
        input_path: input.table_path().to_string(),
        input_format: input.input_format(),
        action: Action::Probe {
            metrics_directory,
            run_id: run_id.to_string(),
            settings: ProbeSettings::default(),
        },
        sample_set: None,
        row_limit: None,
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
        formulation: grouped_merge(2),
        input_path: input.table_path().to_string(),
        input_format: input.input_format(),
        action: Action::MeasuredWrite {
            write: WriteTarget {
                output_path,
                output_format: OutputFormat::PARQUET,
            },
            metrics_directory,
            run_id: run_id.to_string(),
        },
        sample_set: None,
        row_limit: None,
        threads: NonZeroUsize::MIN,
    }
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
fn restricts_the_dataset_to_the_requested_sample_set() {
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
        Some(SAMPLES[..2].iter().map(ToString::to_string).collect()),
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
fn reports_sample_ids_absent_from_the_dataset() {
    let input = fixture::contig_position_disk_fixture(FixtureFormat::Vortex);

    let err = run(
        Formulation::CombineRefsUnion,
        input.table_path(),
        input.input_format(),
        Action::Collect,
        Some(vec!["NOT_A_SAMPLE".to_string()]),
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Error during planning: samples not found in dataset: NOT_A_SAMPLE"
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
