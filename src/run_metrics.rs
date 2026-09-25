//! The tables a recorded run fills: the run record, the run metrics, and a throughput probe's
//! progress samples.
//!
//! This module owns the schemas and the pure functions that fill them: [`run_record_batch`] turns
//! the facts of one combiner run into its one-row run record, [`run_metrics_batch`] turns an
//! executed plan into its run metrics, one row per operator per partition, and
//! [`progress_samples_batch`] turns a probe's samples into one row each. None knows about
//! datasets, formulations, or storage. A formulation and a write target describe their
//! settings as a [`FormulationRecord`] and a [`WriteRecord`], the combiner run supplies the other
//! facts and the plan, and the metrics directory writes the batches where they go. See
//! [ADR 0016](../docs/adr/0016-record-run-metrics-as-wide-parquet-tables.md) for why the tables
//! are wide and why an unknown metric is dropped rather than failing the run.
//!
//! The metric columns are every metric the operators present in a current formulation's plan
//! record under the pinned `DataFusion`: the six baseline metrics every operator can record, the
//! file stream's on every scan, the Parquet scan's, the Vortex scan's counter, the repartition
//! timers, the aggregate's spill counters, peak memory, and timers, and the sink's rows and bytes
//! written. A metric of any other name is dropped and its name handed back for the caller to
//! warn about.
//!
//! Two columns are emptier than their names suggest. Vortex's own scan metrics (bytes read,
//! decode time) live in Vortex's registry and never reach the plan, so a Vortex scan's row holds
//! the file stream's metrics and nothing Vortex-specific; a near-empty Vortex scan row is not a
//! cheap scan. Issue #169 tracks recording them. And `peak_mem_used` is recorded only by
//! `DataFusion`'s fallback grouped hash aggregate stream; the streams it picks for the allele
//! combiner's distinct do not report it, so the column is null for every current plan.

use crate::throughput_probe::{Decision, ProbeSettings, ProgressSample};

use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    error::{DataFusionError, Result},
    physical_plan::{
        ExecutionPlan, displayable,
        metrics::{MetricValue, MetricsSet, RatioMergeStrategy},
    },
};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// The facts of one combiner run: its resolved settings and its whole-run measurements.
#[derive(Clone, Debug, PartialEq)]
pub struct RunRecord {
    pub run_id: String,
    pub started_at: SystemTime,
    pub formulation: FormulationRecord,
    pub dataset_path: String,
    pub input_format: String,
    /// The write the run made; `None` for a drained probe.
    pub write: Option<WriteRecord>,
    pub threads: usize,
    /// The size of the sample set the run covered.
    pub samples: usize,
    /// The rows written; for a probe, the rows the sink received before the stop.
    pub rows_written: u64,
    /// Wall-clock nanoseconds from resolved settings to the completed write, or a probe's stop.
    pub run_ns: u64,
    /// Wall-clock nanoseconds the physical plan's execution alone took, to a probe's stop.
    pub execute_ns: u64,
    /// The process's peak resident set size in bytes, over its lifetime up to the plan's
    /// completion. A whole-process figure: it counts scan buffers and the pages the allocator
    /// keeps resident, whatever the allocator.
    pub peak_rss_bytes: u64,
    /// What a throughput probe decided and how it was set; `None` for a measured write.
    pub probe: Option<ProbeRecord>,
}

/// A throughput probe's settings and the decision that stopped it, as the run record holds them.
#[derive(Clone, Debug, PartialEq)]
pub struct ProbeRecord {
    pub settings: ProbeSettings,
    pub decision: Decision,
    /// The elapsed nanoseconds of the first sample that showed a finished partition; `None` if
    /// none did before the stop.
    pub first_partition_end_ns: Option<u64>,
}

/// The settings of a formulation the run record holds. A formulation describes itself as one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormulationRecord {
    pub name: String,
    /// The sample group count of a grouped merge; `None` for every other formulation.
    pub groups: Option<usize>,
    /// The split points of an interval merge as the caller spelled them; `None` for every other
    /// formulation.
    pub split_points: Option<String>,
}

/// The settings of a write target the run record holds. A write target describes itself as one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRecord {
    pub output_path: String,
    pub output_format: String,
    /// The compression mode as a caller would spell it; `None` when the format's default applies.
    pub compression: Option<String>,
}

/// One column of the run record table: its name and how to read its one cell from a record.
struct RecordColumn {
    name: &'static str,
    cell: RecordCell,
}

/// How a run record column reads its cell, which fixes the column's type and whether it may be
/// null: only the optional variants make a nullable column.
enum RecordCell {
    String(fn(&RunRecord) -> &str),
    OptionalString(fn(&RunRecord) -> Option<&str>),
    UInt64(fn(&RunRecord) -> u64),
    OptionalUInt64(fn(&RunRecord) -> Option<u64>),
    Timestamp(fn(&RunRecord) -> SystemTime),
    /// A fact of a throughput probe, null for a measured write.
    Probe(ProbeCell),
}

/// How a probe column reads its cell from a probe's record. Every probe column is nullable, since
/// a measured write has no probe; the optional variants are null for some probes too.
enum ProbeCell {
    String(fn(&ProbeRecord) -> &'static str),
    UInt64(fn(&ProbeRecord) -> u64),
    OptionalUInt64(fn(&ProbeRecord) -> Option<u64>),
    Float64(fn(&ProbeRecord) -> f64),
    OptionalFloat64(fn(&ProbeRecord) -> Option<f64>),
}

/// The run record table's columns, in schema order. The schema and the batch both derive from
/// this table, so adding a field to the record is adding one line here.
const RUN_RECORD_COLUMNS: &[RecordColumn] = &[
    RecordColumn {
        name: "run_id",
        cell: RecordCell::String(|record| &record.run_id),
    },
    RecordColumn {
        name: "started_at",
        cell: RecordCell::Timestamp(|record| record.started_at),
    },
    RecordColumn {
        name: "formulation",
        cell: RecordCell::String(|record| &record.formulation.name),
    },
    RecordColumn {
        name: "groups",
        cell: RecordCell::OptionalUInt64(|record| record.formulation.groups.map(to_u64)),
    },
    RecordColumn {
        name: "split_points",
        cell: RecordCell::OptionalString(|record| record.formulation.split_points.as_deref()),
    },
    RecordColumn {
        name: "dataset_path",
        cell: RecordCell::String(|record| &record.dataset_path),
    },
    RecordColumn {
        name: "input_format",
        cell: RecordCell::String(|record| &record.input_format),
    },
    RecordColumn {
        name: "output_format",
        cell: RecordCell::OptionalString(|record| {
            record
                .write
                .as_ref()
                .map(|write| write.output_format.as_str())
        }),
    },
    RecordColumn {
        name: "compression",
        cell: RecordCell::OptionalString(|record| {
            record
                .write
                .as_ref()
                .and_then(|write| write.compression.as_deref())
        }),
    },
    RecordColumn {
        name: "threads",
        cell: RecordCell::UInt64(|record| to_u64(record.threads)),
    },
    RecordColumn {
        name: "samples",
        cell: RecordCell::UInt64(|record| to_u64(record.samples)),
    },
    RecordColumn {
        name: "output_path",
        cell: RecordCell::OptionalString(|record| {
            record
                .write
                .as_ref()
                .map(|write| write.output_path.as_str())
        }),
    },
    RecordColumn {
        name: "rows_written",
        cell: RecordCell::UInt64(|record| record.rows_written),
    },
    RecordColumn {
        name: "run_ns",
        cell: RecordCell::UInt64(|record| record.run_ns),
    },
    RecordColumn {
        name: "execute_ns",
        cell: RecordCell::UInt64(|record| record.execute_ns),
    },
    RecordColumn {
        name: "peak_rss_bytes",
        cell: RecordCell::UInt64(|record| record.peak_rss_bytes),
    },
    RecordColumn {
        name: "action",
        cell: RecordCell::Probe(ProbeCell::String(|_| "probe")),
    },
    RecordColumn {
        name: "stop_reason",
        cell: RecordCell::Probe(ProbeCell::String(|probe| probe.decision.stop_reason.name())),
    },
    RecordColumn {
        name: "steady_state_throughput",
        cell: RecordCell::Probe(ProbeCell::OptionalFloat64(|probe| {
            probe.decision.steady_state_throughput
        })),
    },
    RecordColumn {
        name: "warmup_end_ns",
        cell: RecordCell::Probe(ProbeCell::OptionalUInt64(|probe| {
            probe.decision.warmup_end_ns
        })),
    },
    RecordColumn {
        name: "window_end_ns",
        cell: RecordCell::Probe(ProbeCell::UInt64(|probe| probe.decision.window_end_ns)),
    },
    RecordColumn {
        name: "window_rows",
        cell: RecordCell::Probe(ProbeCell::UInt64(|probe| probe.decision.window_rows)),
    },
    RecordColumn {
        name: "first_partition_end_ns",
        cell: RecordCell::Probe(ProbeCell::OptionalUInt64(|probe| {
            probe.first_partition_end_ns
        })),
    },
    RecordColumn {
        name: "poll_period_ns",
        cell: RecordCell::Probe(ProbeCell::UInt64(|probe| {
            duration_ns(probe.settings.poll_period)
        })),
    },
    RecordColumn {
        name: "batch_duration_ns",
        cell: RecordCell::Probe(ProbeCell::UInt64(|probe| {
            duration_ns(probe.settings.batch_duration)
        })),
    },
    RecordColumn {
        name: "precision",
        cell: RecordCell::Probe(ProbeCell::Float64(|probe| probe.settings.precision)),
    },
    RecordColumn {
        name: "consecutive_checks",
        cell: RecordCell::Probe(ProbeCell::UInt64(|probe| {
            u64::from(probe.settings.consecutive_checks.get())
        })),
    },
    RecordColumn {
        name: "window_groups",
        cell: RecordCell::Probe(ProbeCell::UInt64(|probe| {
            u64::from(probe.settings.window_groups)
        })),
    },
    RecordColumn {
        name: "min_duration_ns",
        cell: RecordCell::Probe(ProbeCell::UInt64(|probe| {
            duration_ns(probe.settings.min_duration)
        })),
    },
    RecordColumn {
        name: "max_duration_ns",
        cell: RecordCell::Probe(ProbeCell::UInt64(|probe| {
            duration_ns(probe.settings.max_duration)
        })),
    },
];

impl RecordCell {
    fn data_type(&self) -> DataType {
        match self {
            Self::String(_) | Self::OptionalString(_) => DataType::Utf8,
            Self::UInt64(_) | Self::OptionalUInt64(_) => DataType::UInt64,
            Self::Timestamp(_) => timestamp_type(),
            Self::Probe(cell) => cell.data_type(),
        }
    }

    const fn nullable(&self) -> bool {
        match self {
            Self::OptionalString(_) | Self::OptionalUInt64(_) | Self::Probe(_) => true,
            Self::String(_) | Self::UInt64(_) | Self::Timestamp(_) => false,
        }
    }

    /// The one-cell column this reads from `record`.
    fn array(&self, record: &RunRecord) -> Result<ArrayRef> {
        Ok(match self {
            Self::String(cell) => Arc::new(StringArray::from(vec![cell(record)])),
            Self::OptionalString(cell) => Arc::new(StringArray::from(vec![cell(record)])),
            Self::UInt64(cell) => Arc::new(UInt64Array::from(vec![cell(record)])),
            Self::OptionalUInt64(cell) => Arc::new(UInt64Array::from(vec![cell(record)])),
            Self::Timestamp(cell) => {
                Arc::new(timestamp_array(vec![Some(timestamp_nanos(cell(record))?)]))
            }
            Self::Probe(cell) => cell.array(record.probe.as_ref()),
        })
    }
}

impl ProbeCell {
    const fn data_type(&self) -> DataType {
        match self {
            Self::String(_) => DataType::Utf8,
            Self::UInt64(_) | Self::OptionalUInt64(_) => DataType::UInt64,
            Self::Float64(_) | Self::OptionalFloat64(_) => DataType::Float64,
        }
    }

    /// The one-cell column this reads from `probe`, null without one.
    fn array(&self, probe: Option<&ProbeRecord>) -> ArrayRef {
        match self {
            Self::String(cell) => Arc::new(StringArray::from(vec![probe.map(cell)])),
            Self::UInt64(cell) => Arc::new(UInt64Array::from(vec![probe.map(cell)])),
            Self::OptionalUInt64(cell) => Arc::new(UInt64Array::from(vec![probe.and_then(cell)])),
            Self::Float64(cell) => Arc::new(Float64Array::from(vec![probe.map(cell)])),
            Self::OptionalFloat64(cell) => Arc::new(Float64Array::from(vec![probe.and_then(cell)])),
        }
    }
}

/// The schema of the run record table: one row per run.
#[must_use]
pub fn run_record_schema() -> SchemaRef {
    Arc::new(Schema::new(
        RUN_RECORD_COLUMNS
            .iter()
            .map(|column| Field::new(column.name, column.cell.data_type(), column.cell.nullable()))
            .collect::<Vec<_>>(),
    ))
}

/// The one-row batch of the run record table holding `record`.
///
/// # Errors
///
/// Returns an error if the start time precedes the Unix epoch or does not fit a nanosecond
/// timestamp, or if the batch cannot be assembled.
pub fn run_record_batch(record: &RunRecord) -> Result<RecordBatch> {
    let columns = RUN_RECORD_COLUMNS
        .iter()
        .map(|column| column.cell.array(record))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(run_record_schema(), columns)?)
}

/// The schema of the progress samples table: one row per progress sample of a throughput probe.
///
/// `sample_index` is the sample's position in the order the probe took them, from 0.
#[must_use]
pub fn progress_samples_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("sample_index", DataType::UInt64, false),
        Field::new("elapsed_ns", DataType::UInt64, false),
        Field::new("rows", DataType::UInt64, false),
    ]))
}

/// The batch of the progress samples table holding `samples`, in order, of the run `run_id`.
///
/// # Errors
///
/// Returns an error if the batch cannot be assembled.
pub fn progress_samples_batch(run_id: &str, samples: &[ProgressSample]) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![run_id; samples.len()])),
        Arc::new(UInt64Array::from_iter_values(
            (0..samples.len()).map(to_u64),
        )),
        Arc::new(UInt64Array::from_iter_values(
            samples.iter().map(|sample| sample.elapsed_ns),
        )),
        Arc::new(UInt64Array::from_iter_values(
            samples.iter().map(|sample| sample.rows),
        )),
    ];
    Ok(RecordBatch::try_new(progress_samples_schema(), columns)?)
}

/// The run metrics of one executed plan, and the names of the metrics it reported that the table
/// has no column for.
#[derive(Debug)]
pub struct RunMetrics {
    pub batch: RecordBatch,
    /// Sorted and without repeats.
    pub unrecorded: Vec<String>,
}

/// The schema of the run metrics table: one row per operator per partition.
///
/// `node` is the operator's pre-order index in the plan, `parent` the index of the operator
/// above it, null at the root, and `depth` its distance from the root. `partition` is null for a
/// metric the operator reports globally rather than per partition, and for an operator that
/// reports no metrics at all. Then one column per metric in [`METRICS`], every one nullable:
/// counts and gauges are integers, times are nanosecond integers, timestamps are nanosecond
/// timestamps, and a pruning metric or a ratio is two integer columns.
#[must_use]
pub fn run_metrics_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("node", DataType::UInt64, false),
        Field::new("parent", DataType::UInt64, true),
        Field::new("depth", DataType::UInt64, false),
        Field::new("operator", DataType::Utf8, false),
        Field::new("display", DataType::Utf8, false),
        Field::new("partition", DataType::UInt64, true),
    ];
    for metric in METRICS {
        fields.extend(
            metric
                .columns()
                .into_iter()
                .map(|(name, data_type)| Field::new(name, data_type, true)),
        );
    }
    Arc::new(Schema::new(fields))
}

/// The run metrics of `plan`, an executed plan, for the run `run_id`.
///
/// Every operator gets one row per partition it reported metrics for, and one row of nulls if it
/// reported none. Within a partition, metrics of one name are summed the way `DataFusion` sums
/// them: counts, times, and gauges add, a start timestamp takes the earliest and an end
/// timestamp the latest, a pruning metric adds both counts, and a ratio merges by its own
/// strategy. A Parquet scan reports its metrics once per file, so its partition's row holds the
/// sum over its files. A metric whose name is not a column, or whose kind is not the column's,
/// is dropped and named in the result.
///
/// # Errors
///
/// Returns an error if the batch cannot be assembled.
pub fn run_metrics_batch(run_id: &str, plan: &Arc<dyn ExecutionPlan>) -> Result<RunMetrics> {
    let mut rows = Vec::new();
    let mut unrecorded = BTreeSet::new();
    visit(plan, None, 0, &mut 0, &mut rows, &mut unrecorded);

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![run_id; rows.len()])),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.node),
        )),
        Arc::new(UInt64Array::from_iter(rows.iter().map(|row| row.parent))),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.depth),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.operator.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.display.as_str()),
        )),
        Arc::new(UInt64Array::from_iter(rows.iter().map(|row| row.partition))),
    ];
    for metric in METRICS {
        for (index, (_, data_type)) in metric.columns().iter().enumerate() {
            let cells = rows.iter().map(|row| row.metrics.get(metric.name()));
            columns.push(if *data_type == timestamp_type() {
                Arc::new(timestamp_array(
                    cells
                        .map(|cell| cell.and_then(Recorded::timestamp))
                        .collect(),
                ))
            } else {
                Arc::new(UInt64Array::from_iter(
                    cells.map(|cell| cell.and_then(|cell| cell.integer(index))),
                ))
            });
        }
    }
    Ok(RunMetrics {
        batch: RecordBatch::try_new(run_metrics_schema(), columns)?,
        unrecorded: unrecorded.into_iter().collect(),
    })
}

/// A metric the table has columns for, by the name it reports under.
#[derive(Clone, Copy, Debug)]
enum Metric {
    /// A count, gauge, or time: one integer column named for the metric.
    Integer(&'static str),
    /// One nanosecond timestamp column named for the metric.
    Timestamp(&'static str),
    /// A pruning metric: `<name>_pruned` and `<name>_total`.
    Pruning(&'static str),
    /// A ratio: `<name>_num` and `<name>_den`.
    Ratio(&'static str),
}

/// Every metric the operators present in a current formulation's plan record under the pinned
/// `DataFusion`, in the order their columns appear. The names are the ones the metrics print
/// under, confirmed against the pinned sources; the kinds are the `MetricValue` variants they
/// are built as.
const METRICS: &[Metric] = &[
    // Baseline, on every operator.
    Metric::Integer("output_rows"),
    Metric::Integer("output_batches"),
    Metric::Integer("output_bytes"),
    Metric::Integer("elapsed_compute"),
    Metric::Timestamp("start_timestamp"),
    Metric::Timestamp("end_timestamp"),
    // File stream, on every scan, and the scan's batch splitting.
    Metric::Integer("files_opened"),
    Metric::Integer("files_processed"),
    Metric::Integer("file_open_errors"),
    Metric::Integer("file_scan_errors"),
    Metric::Integer("batches_split"),
    Metric::Integer("time_elapsed_opening"),
    Metric::Integer("time_elapsed_scanning_until_data"),
    Metric::Integer("time_elapsed_scanning_total"),
    Metric::Integer("time_elapsed_processing"),
    // Parquet scan, reported once per file and summed within the partition.
    Metric::Integer("bytes_scanned"),
    Metric::Integer("metadata_load_time"),
    Metric::Integer("pushdown_rows_pruned"),
    Metric::Integer("pushdown_rows_matched"),
    Metric::Integer("predicate_evaluation_errors"),
    Metric::Integer("row_pushdown_eval_time"),
    Metric::Integer("statistics_eval_time"),
    Metric::Integer("bloom_filter_eval_time"),
    Metric::Integer("page_index_eval_time"),
    Metric::Integer("predicate_cache_inner_records"),
    Metric::Integer("predicate_cache_records"),
    Metric::Integer("row_groups_pruned_dynamic_filter"),
    // The next two are registered only once nonzero.
    Metric::Integer("page_index_pages_skipped_by_fully_matched"),
    Metric::Integer("page_index_load_skipped"),
    Metric::Pruning("files_ranges_pruned_statistics"),
    Metric::Pruning("row_groups_pruned_bloom_filter"),
    Metric::Pruning("limit_pruned_row_groups"),
    Metric::Pruning("row_groups_pruned_statistics"),
    Metric::Pruning("page_index_pages_pruned"),
    Metric::Pruning("page_index_rows_pruned"),
    Metric::Ratio("scan_efficiency_ratio"),
    Metric::Ratio("output_rows_skew"),
    // Vortex scan.
    Metric::Integer("num_predicate_creation_errors"),
    // Repartition.
    Metric::Integer("fetch_time"),
    Metric::Integer("repartition_time"),
    Metric::Integer("send_time"),
    // Aggregate.
    Metric::Integer("peak_mem_used"),
    Metric::Integer("spill_count"),
    Metric::Integer("spilled_bytes"),
    Metric::Integer("spilled_rows"),
    Metric::Integer("skipped_aggregation_rows"),
    // The group-by timers of the hash and ordered aggregate streams.
    Metric::Integer("time_calculating_group_ids"),
    Metric::Integer("aggregate_arguments_time"),
    Metric::Integer("aggregation_time"),
    Metric::Integer("emitting_time"),
    Metric::Ratio("reduction_factor"),
    // Sink.
    Metric::Integer("rows_written"),
    Metric::Integer("bytes_written"),
];

impl Metric {
    /// The name this metric reports under.
    const fn name(self) -> &'static str {
        match self {
            Self::Integer(name)
            | Self::Timestamp(name)
            | Self::Pruning(name)
            | Self::Ratio(name) => name,
        }
    }

    /// The columns this metric fills, in schema order.
    fn columns(self) -> Vec<(String, DataType)> {
        match self {
            Self::Integer(name) => vec![(name.to_string(), DataType::UInt64)],
            Self::Timestamp(name) => vec![(name.to_string(), timestamp_type())],
            Self::Pruning(name) => vec![
                (format!("{name}_pruned"), DataType::UInt64),
                (format!("{name}_total"), DataType::UInt64),
            ],
            Self::Ratio(name) => vec![
                (format!("{name}_num"), DataType::UInt64),
                (format!("{name}_den"), DataType::UInt64),
            ],
        }
    }

    /// The metric named `name`, if the table has columns for one.
    fn named(name: &str) -> Option<Self> {
        METRICS.iter().copied().find(|metric| metric.name() == name)
    }
}

/// One metric's value in one row, in the kind its columns hold.
#[derive(Clone, Copy, Debug)]
enum Recorded {
    /// A count, gauge, or time.
    Integer(u64),
    /// Nanoseconds since the Unix epoch.
    Timestamp(i64),
    Pruning {
        pruned: u64,
        total: u64,
    },
    Ratio {
        numerator: u64,
        denominator: u64,
    },
}

impl Recorded {
    /// `value` in the kind `metric`'s columns hold, or `None` when it is not of that kind.
    fn of(metric: Metric, value: &MetricValue) -> Option<Self> {
        match (metric, value) {
            (
                Metric::Integer(_),
                MetricValue::OutputRows(_)
                | MetricValue::OutputBatches(_)
                | MetricValue::OutputBytes(_)
                | MetricValue::SpillCount(_)
                | MetricValue::SpilledBytes(_)
                | MetricValue::SpilledRows(_)
                | MetricValue::CurrentMemoryUsage(_)
                | MetricValue::ElapsedCompute(_)
                | MetricValue::Count { .. }
                | MetricValue::Gauge { .. }
                | MetricValue::PeakMemoryUsage { .. }
                | MetricValue::Time { .. },
            ) => Some(Self::Integer(to_u64(value.as_usize()))),
            (
                Metric::Timestamp(_),
                MetricValue::StartTimestamp(timestamp) | MetricValue::EndTimestamp(timestamp),
            ) => timestamp
                .value()
                .and_then(|time| time.timestamp_nanos_opt())
                .map(Self::Timestamp),
            (
                Metric::Pruning(_),
                MetricValue::PruningMetrics {
                    pruning_metrics, ..
                },
            ) => {
                let pruned = to_u64(pruning_metrics.pruned());
                Some(Self::Pruning {
                    pruned,
                    total: pruned.saturating_add(to_u64(pruning_metrics.matched())),
                })
            }
            (Metric::Ratio(_), MetricValue::Ratio { ratio_metrics, .. }) => Some(Self::Ratio {
                numerator: to_u64(ratio_metrics.part()),
                denominator: to_u64(ratio_metrics.total()),
            }),
            (
                Metric::Integer(_) | Metric::Timestamp(_) | Metric::Pruning(_) | Metric::Ratio(_),
                _,
            ) => None,
        }
    }

    /// This value with `next`, another reading of the same metric within the partition, folded
    /// in the way `DataFusion` folds them: counts add, a start timestamp takes the earliest and
    /// an end timestamp the latest, a pruning metric adds both counts, and a ratio merges by the
    /// strategy `value` carries.
    fn fold(self, next: Self, value: &MetricValue) -> Self {
        match (self, next) {
            (Self::Integer(previous), Self::Integer(next)) => {
                Self::Integer(previous.saturating_add(next))
            }
            (Self::Timestamp(previous), Self::Timestamp(next)) => {
                Self::Timestamp(if matches!(value, MetricValue::StartTimestamp(_)) {
                    previous.min(next)
                } else {
                    previous.max(next)
                })
            }
            (
                Self::Pruning { pruned, total },
                Self::Pruning {
                    pruned: next_pruned,
                    total: next_total,
                },
            ) => Self::Pruning {
                pruned: pruned.saturating_add(next_pruned),
                total: total.saturating_add(next_total),
            },
            (
                Self::Ratio {
                    numerator,
                    denominator,
                },
                Self::Ratio {
                    numerator: next_numerator,
                    denominator: next_denominator,
                },
            ) => {
                let strategy = match value {
                    MetricValue::Ratio { ratio_metrics, .. } => ratio_metrics.merge_strategy(),
                    _ => &RatioMergeStrategy::AddPartAddTotal,
                };
                let (numerator, denominator) = match strategy {
                    RatioMergeStrategy::AddPartAddTotal => (
                        numerator.saturating_add(next_numerator),
                        denominator.saturating_add(next_denominator),
                    ),
                    RatioMergeStrategy::AddPartSetTotal => {
                        (numerator.saturating_add(next_numerator), next_denominator)
                    }
                    RatioMergeStrategy::SetPartAddTotal => {
                        (next_numerator, denominator.saturating_add(next_denominator))
                    }
                };
                Self::Ratio {
                    numerator,
                    denominator,
                }
            }
            // Two readings of one metric name are of one kind; keep the later one otherwise.
            (
                Self::Integer(_) | Self::Timestamp(_) | Self::Pruning { .. } | Self::Ratio { .. },
                next,
            ) => next,
        }
    }

    /// The integer cell this value holds in column `index` of its metric's columns.
    const fn integer(&self, index: usize) -> Option<u64> {
        match (self, index) {
            (
                Self::Integer(value)
                | Self::Pruning { pruned: value, .. }
                | Self::Ratio {
                    numerator: value, ..
                },
                0,
            )
            | (
                Self::Pruning { total: value, .. }
                | Self::Ratio {
                    denominator: value, ..
                },
                1,
            ) => Some(*value),
            (
                Self::Integer(_) | Self::Timestamp(_) | Self::Pruning { .. } | Self::Ratio { .. },
                _,
            ) => None,
        }
    }

    const fn timestamp(&self) -> Option<i64> {
        match self {
            Self::Timestamp(value) => Some(*value),
            Self::Integer(_) | Self::Pruning { .. } | Self::Ratio { .. } => None,
        }
    }
}

/// The metrics of one row, by the name each reports under.
type Metrics = BTreeMap<&'static str, Recorded>;

/// Folds `value` into `metrics`, failing with its name when no column takes it.
fn record<'a>(metrics: &mut Metrics, value: &'a MetricValue) -> Result<(), &'a str> {
    let name = value.name();
    let metric = Metric::named(name).ok_or(name)?;
    let next = match Recorded::of(metric, value) {
        Some(next) => next,
        // A timestamp not yet recorded fills nothing; any other kind mismatch is unrecorded.
        None if matches!(metric, Metric::Timestamp(_)) => return Ok(()),
        None => return Err(name),
    };
    let folded = metrics
        .get(metric.name())
        .map_or(next, |previous| previous.fold(next, value));
    metrics.insert(metric.name(), folded);
    Ok(())
}

/// One row of the run metrics table.
struct Row {
    node: u64,
    parent: Option<u64>,
    depth: u64,
    operator: String,
    display: String,
    partition: Option<u64>,
    metrics: Metrics,
}

/// Appends the rows of `node` and, in pre-order, of every operator beneath it.
fn visit(
    node: &Arc<dyn ExecutionPlan>,
    parent: Option<u64>,
    depth: u64,
    next_index: &mut u64,
    rows: &mut Vec<Row>,
    unrecorded: &mut BTreeSet<String>,
) {
    let index = *next_index;
    *next_index = next_index.saturating_add(1);
    let operator = node.name().to_string();
    let display = displayable(node.as_ref())
        .one_line()
        .to_string()
        .trim_end()
        .to_string();

    let mut partitions: BTreeMap<Option<u64>, Metrics> = BTreeMap::new();
    for metric in node.metrics().iter().flat_map(MetricsSet::iter) {
        let partition = metric.partition().map(to_u64);
        if let Err(name) = record(partitions.entry(partition).or_default(), metric.value()) {
            unrecorded.insert(name.to_string());
        }
    }
    if partitions.is_empty() {
        partitions.insert(None, Metrics::default());
    }
    rows.extend(partitions.into_iter().map(|(partition, metrics)| Row {
        node: index,
        parent,
        depth,
        operator: operator.clone(),
        display: display.clone(),
        partition,
        metrics,
    }));

    for child in node.children() {
        visit(
            child,
            Some(index),
            depth.saturating_add(1),
            next_index,
            rows,
            unrecorded,
        );
    }
}

/// The type every timestamp column has: nanoseconds, UTC.
fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

fn timestamp_array(values: Vec<Option<i64>>) -> TimestampNanosecondArray {
    TimestampNanosecondArray::from(values).with_timezone("UTC")
}

/// `time` as nanoseconds since the Unix epoch.
fn timestamp_nanos(time: SystemTime) -> Result<i64> {
    let since_epoch = time.duration_since(UNIX_EPOCH).map_err(|error| {
        DataFusionError::Internal(format!("a run started before the Unix epoch: {error}"))
    })?;
    i64::try_from(since_epoch.as_nanos()).map_err(|error| {
        DataFusionError::Internal(format!(
            "a run's start time does not fit a nanosecond timestamp: {error}"
        ))
    })
}

/// A count as the table stores it. Saturates rather than failing on a platform whose `usize` is
/// wider than 64 bits.
fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// `duration` in nanoseconds as the table stores it, saturating at `u64::MAX`.
fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
