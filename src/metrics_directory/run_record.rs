//! The run record: the facts of one combiner run, and the table that holds one row per run.
//!
//! The record types are plain data. A [`FormulationRecord`] and a [`WriteRecord`] are read from a
//! formulation and a write target, the combiner run supplies the other facts, and
//! [`run_record_batch`] turns a record into its one-row batch. [`recorded_probe`] reads a
//! throughput probe back out of a row of the table.

use super::{column_as, duration_ns, timestamp_array, timestamp_type, to_u64};
use crate::{
    formulation::Formulation,
    throughput_probe::{Decision, ProbeKind, ProbeSettings, ProbedKind},
    write::WriteTarget,
};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Float64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    common::cast::{as_float64_array, as_string_array, as_uint64_array},
    error::{DataFusionError, Result},
};

use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// The facts of one combiner run: how it ended, its resolved settings, and its whole-run
/// measurements.
#[derive(Clone, Debug, PartialEq)]
pub struct RunRecord {
    pub run_id: String,
    pub status: RunStatus,
    pub started_at: SystemTime,
    pub formulation: FormulationRecord,
    /// The run's row ordering, as its components joined by commas, such as `locus,s`.
    pub row_ordering: String,
    pub dataset_path: String,
    pub input_format: String,
    /// The write the run made; `None` for a drained probe.
    pub write: Option<WriteRecord>,
    pub threads: usize,
    /// The number of input tables the run merged, after narrowing; `None` for a run that ended
    /// before it planned its rows.
    pub input_tables: Option<usize>,
    /// The size of the sample set the run covered, over its input tables; `None` for a run that
    /// ended before it planned its rows.
    pub samples: Option<usize>,
    /// The rows written; for a probe, the rows the sink received before the stop. `None` for a
    /// run that did not complete.
    pub rows_written: Option<u64>,
    /// Wall-clock nanoseconds from resolved settings to the completed write, a probe's stop, or
    /// the end of a run that did not complete.
    pub run_ns: u64,
    /// Wall-clock nanoseconds the physical plan's execution alone took, to a probe's stop. `None`
    /// for a run that did not complete.
    pub execute_ns: Option<u64>,
    /// The process's peak resident set size in bytes, over its lifetime up to the plan's
    /// completion. A whole-process figure: it counts scan buffers and the pages the allocator
    /// keeps resident, whatever the allocator.
    pub peak_rss_bytes: u64,
    /// What a throughput probe decided and how it was set; `None` for a measured write.
    pub probe: Option<ProbeRecord>,
}

/// How a run ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStatus {
    /// It ran to its end: a measured write wrote its rows, or a probe stopped by its decision.
    Completed,
    /// It was cancelled before it completed.
    Cancelled,
    /// It failed before it completed.
    Failed,
}

impl RunStatus {
    /// The status as the run record's `status` column holds it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

/// A throughput probe's settings and the decision that stopped it, as the run record holds them.
#[derive(Clone, Debug, PartialEq)]
pub struct ProbeRecord {
    pub settings: ProbeSettings,
    pub decision: Decision,
    /// The elapsed nanoseconds of the first sample that showed a finished partition; `None` if
    /// none did before the stop.
    pub first_partition_end_ns: Option<u64>,
    /// Whether it is a shadow probe, and if so, its first steady decision, which stopped at the
    /// end of its window.
    pub kind: ProbedKind,
}

/// The settings of a formulation the run record holds, as [`From<&Formulation>`] reads them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormulationRecord {
    pub name: String,
    /// The input group count of a grouped merge; `None` for every other formulation.
    pub groups: Option<usize>,
    /// The split points of an interval merge as the caller spelled them; `None` for every other
    /// formulation.
    pub split_points: Option<String>,
}

/// The settings of a write target the run record holds, as [`From<&WriteTarget>`] reads them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRecord {
    pub output_path: String,
    pub output_format: String,
    /// The compression mode as a caller would spell it; `None` when the format's default applies.
    pub compression: Option<String>,
}

impl From<&Formulation> for FormulationRecord {
    fn from(formulation: &Formulation) -> Self {
        let (groups, split_points) = match formulation {
            Formulation::CombineAllelesUnion | Formulation::CombineRefsUnion => (None, None),
            Formulation::CombineRefsGroupedMerge { groups } => (Some(groups.get()), None),
            Formulation::CombineRefsIntervalMerge { split_points } => {
                (None, Some(split_points.to_string()))
            }
        };
        Self {
            name: formulation.to_string(),
            groups,
            split_points,
        }
    }
}

impl From<&WriteTarget> for WriteRecord {
    fn from(target: &WriteTarget) -> Self {
        Self {
            output_path: target.output_path.clone(),
            output_format: target.output_format.name().to_string(),
            compression: target.output_format.compression().map(str::to_string),
        }
    }
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
        name: "status",
        cell: RecordCell::String(|record| record.status.name()),
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
        name: "row_ordering",
        cell: RecordCell::String(|record| &record.row_ordering),
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
        name: "input_tables",
        cell: RecordCell::OptionalUInt64(|record| record.input_tables.map(to_u64)),
    },
    RecordColumn {
        name: "samples",
        cell: RecordCell::OptionalUInt64(|record| record.samples.map(to_u64)),
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
        cell: RecordCell::OptionalUInt64(|record| record.rows_written),
    },
    RecordColumn {
        name: "run_ns",
        cell: RecordCell::UInt64(|record| record.run_ns),
    },
    RecordColumn {
        name: "execute_ns",
        cell: RecordCell::OptionalUInt64(|record| record.execute_ns),
    },
    RecordColumn {
        name: "peak_rss_bytes",
        cell: RecordCell::UInt64(|record| record.peak_rss_bytes),
    },
    RecordColumn {
        name: "action",
        cell: RecordCell::Probe(ProbeCell::String(|probe| probe.kind.name())),
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
        name: "relative_half_width",
        cell: RecordCell::Probe(ProbeCell::OptionalFloat64(|probe| {
            probe.decision.relative_half_width
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
    RecordColumn {
        name: "would_stop_ns",
        cell: RecordCell::Probe(ProbeCell::OptionalUInt64(|probe| {
            probe
                .kind
                .would_stop()
                .map(|decision| decision.window_end_ns)
        })),
    },
    RecordColumn {
        name: "would_be_steady_state_throughput",
        cell: RecordCell::Probe(ProbeCell::OptionalFloat64(|probe| {
            probe
                .kind
                .would_stop()
                .and_then(|decision| decision.steady_state_throughput)
        })),
    },
    RecordColumn {
        name: "would_be_relative_half_width",
        cell: RecordCell::Probe(ProbeCell::OptionalFloat64(|probe| {
            probe
                .kind
                .would_stop()
                .and_then(|decision| decision.relative_half_width)
        })),
    },
    RecordColumn {
        name: "would_be_warmup_end_ns",
        cell: RecordCell::Probe(ProbeCell::OptionalUInt64(|probe| {
            probe
                .kind
                .would_stop()
                .and_then(|decision| decision.warmup_end_ns)
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
pub(crate) fn run_record_schema() -> SchemaRef {
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
pub(super) fn run_record_batch(record: &RunRecord) -> Result<RecordBatch> {
    let columns = RUN_RECORD_COLUMNS
        .iter()
        .map(|column| column.cell.array(record))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(run_record_schema(), columns)?)
}

/// A throughput probe as its run record holds it, read back.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct RecordedProbe {
    pub run_id: String,
    pub kind: ProbeKind,
    pub settings: ProbeSettings,
    /// The elapsed nanoseconds of the first sample that showed a finished partition; `None` if
    /// none did before the stop.
    pub first_partition_end_ns: Option<u64>,
}

/// The throughput probe the run record at `row` of `records`, a batch of the run record table,
/// holds; `None` for a measured write, which has no action.
///
/// # Errors
///
/// Returns an error if `records` lacks a column of the run record table or holds one of another
/// type, or if the run's action is not a probe's, or a probe lacks a setting or holds one out of
/// range.
pub(super) fn recorded_probe(records: &RecordBatch, row: usize) -> Result<Option<RecordedProbe>> {
    let run_id = string_cell(records, "run_id", row)?.unwrap_or_default();
    let invalid = |problem: String| {
        DataFusionError::Execution(format!("the run record of '{run_id}' {problem}"))
    };
    let kind = match string_cell(records, "action", row)?.as_deref() {
        None => return Ok(None),
        Some("probe") => ProbeKind::Probe,
        Some("shadow") => ProbeKind::Shadow,
        Some(action) => return Err(invalid(format!("has an unknown action '{action}'"))),
    };
    let setting =
        |name: &str| u64_cell(records, name, row)?.ok_or_else(|| invalid(format!("has no {name}")));
    let count = |name: &str| {
        u32::try_from(setting(name)?).map_err(|error| invalid(format!("has a {name} {error}")))
    };
    let settings = ProbeSettings {
        poll_period: Duration::from_nanos(setting("poll_period_ns")?),
        batch_duration: Duration::from_nanos(setting("batch_duration_ns")?),
        precision: f64_cell(records, "precision", row)?
            .ok_or_else(|| invalid("has no precision".to_string()))?,
        consecutive_checks: NonZeroU32::new(count("consecutive_checks")?)
            .ok_or_else(|| invalid("has no consecutive checks".to_string()))?,
        window_groups: count("window_groups")?,
        min_duration: Duration::from_nanos(setting("min_duration_ns")?),
        max_duration: Duration::from_nanos(setting("max_duration_ns")?),
    };
    Ok(Some(RecordedProbe {
        first_partition_end_ns: u64_cell(records, "first_partition_end_ns", row)?,
        run_id,
        kind,
        settings,
    }))
}

/// The cell of the string column `name` of `batch` at `row`; `None` if it is null.
fn string_cell(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<String>> {
    let column = column_as(batch, name, &DataType::Utf8)?;
    let column = as_string_array(&column)?;
    Ok((!column.is_null(row)).then(|| column.value(row).to_string()))
}

/// The cell of the `UInt64` column `name` of `batch` at `row`; `None` if it is null.
fn u64_cell(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<u64>> {
    let column = column_as(batch, name, &DataType::UInt64)?;
    let column = as_uint64_array(&column)?;
    Ok((!column.is_null(row)).then(|| column.value(row)))
}

/// The cell of the `Float64` column `name` of `batch` at `row`; `None` if it is null.
fn f64_cell(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<f64>> {
    let column = column_as(batch, name, &DataType::Float64)?;
    let column = as_float64_array(&column)?;
    Ok((!column.is_null(row)).then(|| column.value(row)))
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
