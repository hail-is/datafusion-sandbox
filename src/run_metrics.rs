//! The two tables a measured write records: the run record and the run metrics.
//!
//! This module owns both schemas and the two pure functions that fill them: [`run_record_batch`]
//! turns the facts of one combiner run into its one-row run record, and [`run_metrics_batch`]
//! turns an executed plan into its run metrics, one row per operator per partition. Neither knows
//! about datasets, formulations, or storage; the combiner run supplies the facts and the plan and
//! writes the batches where they go. See
//! [ADR 0016](../docs/adr/0016-record-run-metrics-as-wide-parquet-tables.md) for why the tables
//! are wide and why an unknown metric is dropped rather than failing the run.
//!
//! The metric columns are the six baseline metrics every `DataFusion` operator can record. A
//! metric of any other name is dropped and its name handed back for the caller to warn about.
//! Vortex's own scan metrics (bytes read, decode time) live in Vortex's registry and never reach
//! the plan, so a Vortex scan's row is nearly empty without the scan being cheap; issue #169
//! tracks recording them.

use datafusion::{
    arrow::{
        array::{ArrayRef, StringArray, TimestampNanosecondArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    error::{DataFusionError, Result},
    physical_plan::{
        ExecutionPlan, displayable,
        metrics::{MetricValue, MetricsSet},
    },
};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

/// The facts of one combiner run: its resolved settings and its whole-run measurements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunRecord {
    pub run_id: String,
    pub started_at: SystemTime,
    pub formulation: String,
    /// The sample group count of a grouped merge; `None` for every other formulation.
    pub groups: Option<usize>,
    /// The split points of an interval merge as the caller spelled them; `None` for every other
    /// formulation.
    pub split_points: Option<String>,
    pub dataset_path: String,
    pub input_format: String,
    pub output_format: String,
    pub compression: Option<String>,
    pub threads: usize,
    /// The size of the sample set the run covered.
    pub samples: usize,
    pub output_path: String,
    pub rows_written: u64,
    /// Wall-clock nanoseconds from resolved settings to the completed write.
    pub run_ns: u64,
    /// Wall-clock nanoseconds the physical plan's execution alone took.
    pub execute_ns: u64,
}

/// The schema of the run record table: one row per run.
#[must_use]
pub fn run_record_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("started_at", timestamp_type(), false),
        Field::new("formulation", DataType::Utf8, false),
        Field::new("groups", DataType::UInt64, true),
        Field::new("split_points", DataType::Utf8, true),
        Field::new("dataset_path", DataType::Utf8, false),
        Field::new("input_format", DataType::Utf8, false),
        Field::new("output_format", DataType::Utf8, false),
        Field::new("compression", DataType::Utf8, true),
        Field::new("threads", DataType::UInt64, false),
        Field::new("samples", DataType::UInt64, false),
        Field::new("output_path", DataType::Utf8, false),
        Field::new("rows_written", DataType::UInt64, false),
        Field::new("run_ns", DataType::UInt64, false),
        Field::new("execute_ns", DataType::UInt64, false),
    ]))
}

/// The one-row batch of the run record table holding `record`.
///
/// # Errors
///
/// Returns an error if the start time precedes the Unix epoch or does not fit a nanosecond
/// timestamp, or if the batch cannot be assembled.
pub fn run_record_batch(record: &RunRecord) -> Result<RecordBatch> {
    let started_at = timestamp_nanos(record.started_at)?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![record.run_id.as_str()])),
        Arc::new(timestamp_array(vec![Some(started_at)])),
        Arc::new(StringArray::from(vec![record.formulation.as_str()])),
        Arc::new(UInt64Array::from(vec![record.groups.map(to_u64)])),
        Arc::new(StringArray::from(vec![record.split_points.as_deref()])),
        Arc::new(StringArray::from(vec![record.dataset_path.as_str()])),
        Arc::new(StringArray::from(vec![record.input_format.as_str()])),
        Arc::new(StringArray::from(vec![record.output_format.as_str()])),
        Arc::new(StringArray::from(vec![record.compression.as_deref()])),
        Arc::new(UInt64Array::from(vec![to_u64(record.threads)])),
        Arc::new(UInt64Array::from(vec![to_u64(record.samples)])),
        Arc::new(StringArray::from(vec![record.output_path.as_str()])),
        Arc::new(UInt64Array::from(vec![record.rows_written])),
        Arc::new(UInt64Array::from(vec![record.run_ns])),
        Arc::new(UInt64Array::from(vec![record.execute_ns])),
    ];
    Ok(RecordBatch::try_new(run_record_schema(), columns)?)
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
/// reports no metrics at all. Counts are integers, times are nanosecond integers, and timestamps
/// are nanosecond timestamps.
#[must_use]
pub fn run_metrics_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("node", DataType::UInt64, false),
        Field::new("parent", DataType::UInt64, true),
        Field::new("depth", DataType::UInt64, false),
        Field::new("operator", DataType::Utf8, false),
        Field::new("display", DataType::Utf8, false),
        Field::new("partition", DataType::UInt64, true),
        Field::new("output_rows", DataType::UInt64, true),
        Field::new("output_batches", DataType::UInt64, true),
        Field::new("output_bytes", DataType::UInt64, true),
        Field::new("elapsed_compute", DataType::UInt64, true),
        Field::new("start_timestamp", timestamp_type(), true),
        Field::new("end_timestamp", timestamp_type(), true),
    ]))
}

/// The run metrics of `plan`, an executed plan, for the run `run_id`.
///
/// Every operator gets one row per partition it reported metrics for, its counts summed and its
/// timestamps spanned within the partition, and one row of nulls if it reported none. A metric
/// whose name is not a column is dropped and named in the result.
///
/// # Errors
///
/// Returns an error if the batch cannot be assembled.
pub fn run_metrics_batch(run_id: &str, plan: &Arc<dyn ExecutionPlan>) -> Result<RunMetrics> {
    let mut rows = Vec::new();
    let mut unrecorded = BTreeSet::new();
    visit(plan, None, 0, &mut 0, &mut rows, &mut unrecorded);

    let columns: Vec<ArrayRef> = vec![
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
        Arc::new(UInt64Array::from_iter(
            rows.iter().map(|row| row.metrics.output_rows),
        )),
        Arc::new(UInt64Array::from_iter(
            rows.iter().map(|row| row.metrics.output_batches),
        )),
        Arc::new(UInt64Array::from_iter(
            rows.iter().map(|row| row.metrics.output_bytes),
        )),
        Arc::new(UInt64Array::from_iter(
            rows.iter().map(|row| row.metrics.elapsed_compute),
        )),
        Arc::new(timestamp_array(
            rows.iter().map(|row| row.metrics.start_timestamp).collect(),
        )),
        Arc::new(timestamp_array(
            rows.iter().map(|row| row.metrics.end_timestamp).collect(),
        )),
    ];
    Ok(RunMetrics {
        batch: RecordBatch::try_new(run_metrics_schema(), columns)?,
        unrecorded: unrecorded.into_iter().collect(),
    })
}

/// One row of the run metrics table.
struct Row {
    node: u64,
    parent: Option<u64>,
    depth: u64,
    operator: String,
    display: String,
    partition: Option<u64>,
    metrics: Baseline,
}

/// The baseline metric columns of one row, every one null until a metric fills it.
#[derive(Default)]
struct Baseline {
    output_rows: Option<u64>,
    output_batches: Option<u64>,
    output_bytes: Option<u64>,
    elapsed_compute: Option<u64>,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
}

impl Baseline {
    /// Folds `value` into this row, failing with its name when no column takes it.
    fn record<'a>(&mut self, value: &'a MetricValue) -> Result<(), &'a str> {
        match value {
            MetricValue::OutputRows(count) => add(&mut self.output_rows, count.value()),
            MetricValue::OutputBatches(count) => add(&mut self.output_batches, count.value()),
            MetricValue::OutputBytes(count) => add(&mut self.output_bytes, count.value()),
            MetricValue::ElapsedCompute(time) => add(&mut self.elapsed_compute, time.value()),
            MetricValue::StartTimestamp(timestamp) => {
                if let Some(nanos) = timestamp.value().and_then(|t| t.timestamp_nanos_opt()) {
                    self.start_timestamp =
                        Some(self.start_timestamp.map_or(nanos, |s| s.min(nanos)));
                }
            }
            MetricValue::EndTimestamp(timestamp) => {
                if let Some(nanos) = timestamp.value().and_then(|t| t.timestamp_nanos_opt()) {
                    self.end_timestamp = Some(self.end_timestamp.map_or(nanos, |e| e.max(nanos)));
                }
            }
            other => return Err(other.name()),
        }
        Ok(())
    }
}

/// Adds `value` to a count column, filling a null.
fn add(column: &mut Option<u64>, value: usize) {
    *column = Some(column.unwrap_or(0).saturating_add(to_u64(value)));
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

    let mut partitions: BTreeMap<Option<u64>, Baseline> = BTreeMap::new();
    for metric in node.metrics().iter().flat_map(MetricsSet::iter) {
        let partition = metric.partition().map(to_u64);
        if let Err(name) = partitions
            .entry(partition)
            .or_default()
            .record(metric.value())
        {
            unrecorded.insert(name.to_string());
        }
    }
    if partitions.is_empty() {
        partitions.insert(None, Baseline::default());
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
