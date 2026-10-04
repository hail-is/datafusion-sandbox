//! A resolved combiner run, the action it performs, and its outcome.

use crate::{
    format::InputFormat,
    formulation::Formulation,
    locus::RowOrdering,
    metrics_directory::{MetricsDirectory, UnrecordedRun},
    ordered_frame::{OrderedFrame, OutputLayout},
    pipeline::{self, PipelineOptions},
    process,
    run_metrics::{FormulationRecord, ProbeRecord, RunRecord, WriteRecord},
    sample_annotation_table,
    sink::{self, ExecutedSink},
    stored::dataset::Dataset,
    throughput_probe::{ProbeKind, ProbeSettings, StopReason},
    write::{VacantTarget, WriteTarget},
};

use datafusion::{
    arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches},
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
    prelude::{DataFrame, SessionContext},
};
use std::{
    num::NonZeroUsize,
    time::{Instant, SystemTime},
};

/// What to do with the combined rows.
///
/// Every action runs the rows into a sink that requires the formulation's ordering: the file
/// sink for a write, measured, probed or not, a collecting sink for collect, and a draining sink
/// for a throughput probe or an explain without a write. The run supplies the ordering to each.
/// See ADR 0014 for why the plan ends in a sink rather than a sort.
#[derive(Debug)]
pub enum Action {
    /// Write the rows to the target, then, for the reference combiner, the sample annotation table
    /// of the run's sample set beside them. See ADR 0018.
    Write(WriteTarget),
    /// Write the rows to the target as a write does, sample annotation table included, and record
    /// the run as `run_id` under the metrics directory. See ADR 0016.
    MeasuredWrite {
        write: WriteTarget,
        metrics_directory: MetricsDirectory,
        run_id: String,
    },
    /// Write the rows to the target, or drain them without one, until the throughput probe's
    /// settings stop the run, and record it as `run_id` under the metrics directory with its
    /// progress samples. Everything under the target's output path is removed afterwards, so it
    /// must not exist beforehand. A shadow probe runs to completion instead, recording when the
    /// settings would have stopped it. See ADR 0017.
    Probe {
        write: Option<WriteTarget>,
        metrics_directory: MetricsDirectory,
        run_id: String,
        settings: ProbeSettings,
        kind: ProbeKind,
    },
    /// Collect the rows in memory.
    Collect,
    /// Render the logical and physical plan of the write, or of a draining run without one,
    /// without executing it.
    Explain { write: Option<WriteTarget> },
    /// Execute the write, or a draining run without one, and render its plan with per-operator
    /// metrics.
    ExplainAnalyze { write: Option<WriteTarget> },
}

impl Action {
    /// The path the write this action performs or renders puts its rows at, if any.
    #[must_use]
    pub fn output_path(&self) -> Option<&str> {
        match self {
            Self::Write(target)
            | Self::MeasuredWrite { write: target, .. }
            | Self::Explain {
                write: Some(target),
            }
            | Self::ExplainAnalyze {
                write: Some(target),
            }
            | Self::Probe {
                write: Some(target),
                ..
            } => Some(&target.output_path),
            Self::Probe { write: None, .. }
            | Self::Collect
            | Self::Explain { write: None }
            | Self::ExplainAnalyze { write: None } => None,
        }
    }

    /// The directory a measured write or a probe records the run under, if this action is one.
    #[must_use]
    pub const fn metrics_directory(&self) -> Option<&MetricsDirectory> {
        match self {
            Self::MeasuredWrite {
                metrics_directory, ..
            }
            | Self::Probe {
                metrics_directory, ..
            } => Some(metrics_directory),
            Self::Write(_) | Self::Collect | Self::Explain { .. } | Self::ExplainAnalyze { .. } => {
                None
            }
        }
    }

    /// Makes every refusal this action owes before dataset discovery, and hands back the action
    /// with what the checks produced: a measured write's or a probe's unrecorded run id, and a
    /// writing probe's vacant output path, refused if it holds the metrics directory. A write
    /// that executes, measured or analyzed, in `layout` of one file per partition is refused if
    /// anything exists at its output path.
    async fn check(self, ctx: &SessionContext, layout: OutputLayout) -> Result<CheckedAction> {
        Ok(match self {
            Self::Write(target) => CheckedAction::Write(written_in(ctx, target, layout).await?),
            Self::MeasuredWrite {
                write,
                metrics_directory,
                run_id,
            } => CheckedAction::MeasuredWrite(CheckedMeasuredWrite {
                run: metrics_directory.unrecorded(ctx, &run_id).await?,
                write: written_in(ctx, write, layout).await?,
            }),
            Self::Probe {
                write,
                metrics_directory,
                run_id,
                settings,
                kind,
            } => CheckedAction::Probe(Box::new(CheckedProbe {
                run: metrics_directory.unrecorded(ctx, &run_id).await?,
                write: probe_output(ctx, write, &metrics_directory).await?,
                settings,
                kind,
            })),
            Self::Collect => CheckedAction::Collect,
            Self::Explain { write } => CheckedAction::Explain {
                write,
                analyze: false,
            },
            Self::ExplainAnalyze { write } => CheckedAction::Explain {
                write: match write {
                    Some(write) => Some(written_in(ctx, write, layout).await?),
                    None => None,
                },
                analyze: true,
            },
        })
    }
}

/// One execution of a combiner against a dataset: the rows `inputs` plan, run through `action` on
/// `threads` threads.
pub struct CombinerRun {
    pub inputs: PlanInputs,
    pub action: Action,
    pub threads: NonZeroUsize,
}

impl CombinerRun {
    /// Executes the run to completion on its own runtimes.
    ///
    /// # Errors
    ///
    /// Returns an error if a path is on a store the pipeline cannot serve, or for any reason
    /// [`CombinerRun::execute_in`] gives.
    pub fn execute(self) -> Result<Outcome> {
        let started = Started::now();
        let options = PipelineOptions::for_paths(
            self.threads,
            [
                Some(self.inputs.input_path.as_str()),
                self.action.output_path(),
                self.action.metrics_directory().map(MetricsDirectory::path),
            ]
            .into_iter()
            .flatten(),
        )?;
        pipeline::run(
            move |ctx| async move { self.run(&ctx, started).await },
            options,
        )
    }

    /// Executes the run on `ctx`, inside a pipeline whose session serves every path the run
    /// names. A measured write's or a probe's run record times the run from this call.
    ///
    /// The caller builds the session, so the record describes the run rather than the session:
    /// its `run_ns` leaves out the runtime and session setup that [`CombinerRun::execute`]
    /// includes, and its `threads` is the run's thread count, whatever the session runs on.
    ///
    /// # Errors
    ///
    /// Returns an error if a measured write's or a probe's run id already has a run record under
    /// its metrics directory, a writing probe's output path already exists or holds its metrics
    /// directory, an executed write of one file per partition has something at its output path, the
    /// input dataset cannot be resolved, the formulation cannot be planned or executed, or the
    /// requested output or its sample annotation table cannot be written. A measured write or a
    /// probe that fails records nothing: the refusals of a repeated id and of an occupied output
    /// path come before dataset discovery, and the run is recorded only after the data write and
    /// its sample annotation table, or the probe, succeed. A probe also fails on a session the
    /// pipeline runner did not build, which has no IO runtime to sample on.
    pub async fn execute_in(self, ctx: &SessionContext) -> Result<Outcome> {
        self.run(ctx, Started::now()).await
    }

    async fn run(self, ctx: &SessionContext, started: Started) -> Result<Outcome> {
        let facts = RunFacts::new(started, &self.inputs, self.threads);
        self.action
            .check(ctx, self.inputs.formulation.output_layout())
            .await?
            .execute(ctx, &self.inputs, facts)
            .await
    }
}

/// An action whose refusals before dataset discovery have all passed. [`Action::check`] is the
/// only way to get one, and executing one is the only way a run discovers its dataset.
enum CheckedAction {
    Write(WriteTarget),
    MeasuredWrite(CheckedMeasuredWrite),
    Probe(Box<CheckedProbe>),
    Collect,
    /// An explain, or with `analyze` an explain analyze.
    Explain {
        write: Option<WriteTarget>,
        analyze: bool,
    },
}

impl CheckedAction {
    /// Plans the rows `inputs` describe and runs them through this action.
    ///
    /// An action that writes to an output path first removes any sample annotation table an
    /// earlier write left beside it, whether or not it writes one itself, so a table only ever
    /// marks the data the last write left there, and only once that write is complete. See
    /// ADR 0018.
    async fn execute(
        self,
        ctx: &SessionContext,
        inputs: &PlanInputs,
        facts: RunFacts,
    ) -> Result<Outcome> {
        let rows = inputs.plan(ctx).await?;
        if let Some(target) = self.written_target() {
            sample_annotation_table::remove(ctx, target, rows.ordered.layout).await?;
        }
        match self {
            Self::Write(target) => Ok(Outcome::RowsWritten(
                rows.write(ctx, &target).await?.rows_written,
            )),
            Self::MeasuredWrite(measured) => measured.execute(ctx, rows, facts).await,
            Self::Probe(probe) => probe.execute(ctx, rows, facts).await,
            Self::Collect => collect_rows(rows.ordered).await,
            Self::Explain { write, analyze } => {
                explain(sink_frame(rows.ordered, write)?, analyze).await
            }
        }
    }

    /// The target this action writes rows to when executed, if any. An explain writes only when
    /// it is analyzed.
    fn written_target(&self) -> Option<&WriteTarget> {
        match self {
            Self::Write(target)
            | Self::Explain {
                write: Some(target),
                analyze: true,
            } => Some(target),
            Self::MeasuredWrite(measured) => Some(&measured.write),
            Self::Probe(probe) => probe.write.as_ref().map(VacantTarget::target),
            Self::Collect | Self::Explain { .. } => None,
        }
    }
}

/// Hands back `write` for a write in `layout`, refused if the layout is one file per partition and
/// something exists at its output path.
async fn written_in(
    ctx: &SessionContext,
    write: WriteTarget,
    layout: OutputLayout,
) -> Result<WriteTarget> {
    if layout == OutputLayout::FilePerPartition {
        write.check_vacant_directory(ctx).await?;
    }
    Ok(write)
}

/// A measured write whose run id had no run record.
struct CheckedMeasuredWrite {
    write: WriteTarget,
    run: UnrecordedRun,
}

impl CheckedMeasuredWrite {
    /// Writes the rows and records the run.
    async fn execute(
        self,
        ctx: &SessionContext,
        rows: PlannedRows,
        facts: RunFacts,
    ) -> Result<Outcome> {
        let Self { write, run } = self;
        let coverage = rows.coverage;
        let executed = rows.write(ctx, &write).await?;
        let record = facts.record(
            run.run_id().to_string(),
            coverage,
            Some((&write).into()),
            executed.rows_written,
            executed.execute_ns,
            None,
        )?;
        Ok(Outcome::Measured {
            rows_written: executed.rows_written,
            unrecorded_metrics: run.record(ctx, &record, &executed.plan).await?,
        })
    }
}

/// A probe whose run id had no run record, and whose output path, if it writes, was vacant and
/// did not hold its metrics directory.
struct CheckedProbe {
    write: Option<VacantTarget>,
    run: UnrecordedRun,
    settings: ProbeSettings,
    kind: ProbeKind,
}

impl CheckedProbe {
    /// Probes the rows, writing or draining them, and records the run with its progress samples.
    async fn execute(
        self,
        ctx: &SessionContext,
        rows: PlannedRows,
        facts: RunFacts,
    ) -> Result<Outcome> {
        let Self {
            write,
            run,
            settings,
            kind,
        } = self;
        let probed = match &write {
            Some(target) => target.probe(rows.ordered, &settings, kind).await?,
            None => sink::probe(sink::drain(rows.ordered)?, &settings, kind).await?,
        };
        let record = facts.record(
            run.run_id().to_string(),
            rows.coverage,
            write.as_ref().map(|target| target.target().into()),
            probed.rows_received,
            probed.execute_ns,
            Some(ProbeRecord {
                settings,
                decision: probed.decision.clone(),
                first_partition_end_ns: probed.first_partition_end_ns,
                kind: probed.kind,
            }),
        )?;
        let unrecorded_metrics = run
            .record_probe(ctx, &record, &probed.plan, &probed.samples)
            .await?;
        Ok(Outcome::Probed {
            rows_received: probed.rows_received,
            steady_state_throughput: probed.decision.steady_state_throughput,
            stop_reason: probed.decision.stop_reason,
            unrecorded_metrics,
        })
    }
}

/// Collects the rows in memory.
async fn collect_rows(ordered: OrderedFrame) -> Result<Outcome> {
    let (frame, sink) = sink::collect(ordered)?;
    frame.collect().await?;
    Ok(Outcome::Batches(sink.take()))
}

/// When a run started, by the wall clock it records and the monotonic clock it times with.
#[derive(Clone, Copy)]
struct Started {
    at: SystemTime,
    instant: Instant,
}

impl Started {
    fn now() -> Self {
        Self {
            at: SystemTime::now(),
            instant: Instant::now(),
        }
    }
}

/// What a run record holds that a run knows before its action executes.
struct RunFacts {
    started: Started,
    formulation: FormulationRecord,
    row_ordering: String,
    dataset_path: String,
    input_format: String,
    threads: usize,
}

impl RunFacts {
    /// The facts of a run started at `started` on `threads` threads over the rows `inputs`
    /// describe.
    fn new(started: Started, inputs: &PlanInputs, threads: NonZeroUsize) -> Self {
        Self {
            started,
            formulation: (&inputs.formulation).into(),
            row_ordering: inputs.row_ordering.to_string(),
            dataset_path: inputs.input_path.clone(),
            input_format: inputs.input_format.name().to_string(),
            threads: threads.get(),
        }
    }

    /// The run record of `run_id`, a run over `coverage` that ends now. Its action wrote
    /// `write`, if any, wrote `rows_written` rows, or received them for a probe, and executed for
    /// `execute_ns`. A probe's settings and decision are `probe`.
    fn record(
        self,
        run_id: String,
        coverage: Coverage,
        write: Option<WriteRecord>,
        rows_written: u64,
        execute_ns: u64,
        probe: Option<ProbeRecord>,
    ) -> Result<RunRecord> {
        Ok(RunRecord {
            run_id,
            started_at: self.started.at,
            formulation: self.formulation,
            row_ordering: self.row_ordering,
            dataset_path: self.dataset_path,
            input_format: self.input_format,
            write,
            threads: self.threads,
            input_tables: coverage.input_tables,
            samples: coverage.samples,
            rows_written,
            run_ns: elapsed_ns(self.started.instant),
            execute_ns,
            peak_rss_bytes: process::peak_rss_bytes()?,
            probe,
        })
    }
}

/// What a run plans its rows from: the formulation's rows over the dataset at `input_path`,
/// declared in `row_ordering`, restricted to the named `input_tables` and limited to `row_limit`
/// rows when given.
///
/// The formulation merges under the whole row ordering, which must satisfy its required ordering.
pub struct PlanInputs {
    pub formulation: Formulation,
    pub row_ordering: RowOrdering,
    pub input_path: String,
    pub input_format: InputFormat,
    pub input_tables: Option<Vec<String>>,
    pub row_limit: Option<usize>,
}

impl PlanInputs {
    /// Discovers the dataset and plans the rows over it.
    async fn plan(&self, ctx: &SessionContext) -> Result<PlannedRows> {
        let dataset = Dataset::discover(
            ctx,
            ListingTableUrl::parse(&self.input_path)?,
            self.input_format.clone(),
            self.row_ordering.clone(),
            None,
        )
        .await?;
        let dataset = match &self.input_tables {
            Some(input_tables) => dataset.restrict_to(input_tables)?,
            None => dataset,
        };
        let coverage = Coverage {
            input_tables: dataset.input_tables().len(),
            samples: dataset.sample_set().len(),
        };
        let ordered = self.formulation.plan(ctx, &dataset).await?;
        let ordered = match self.row_limit {
            Some(limit) => ordered.limit(limit)?,
            None => ordered,
        };
        Ok(PlannedRows {
            ordered,
            coverage,
            sample_set: dataset.sample_set(),
            annotated: self.formulation.writes_sample_annotation_table(),
        })
    }
}

/// A run's planned rows, how much of the dataset they cover and its sample set, and whether a
/// write of them writes a sample annotation table.
struct PlannedRows {
    ordered: OrderedFrame,
    coverage: Coverage,
    sample_set: Vec<String>,
    annotated: bool,
}

/// How much of the dataset a run covers: the input tables it merges after narrowing, and the
/// size of their sample set.
#[derive(Clone, Copy)]
struct Coverage {
    input_tables: usize,
    samples: usize,
}

impl PlannedRows {
    /// Writes the rows to `target`, then, when annotated, the sample annotation table of the
    /// sample set beside them, and returns the data write's execution. A failed data write writes
    /// no table, so the table's presence marks the data complete. See ADR 0018.
    async fn write(self, ctx: &SessionContext, target: &WriteTarget) -> Result<ExecutedSink> {
        let layout = self.ordered.layout;
        let executed = target.write(self.ordered).await?;
        if self.annotated {
            sample_annotation_table::write(ctx, target, layout, &self.sample_set).await?;
        }
        Ok(executed)
    }
}

/// Wall-clock nanoseconds since `since`, saturating at `u64::MAX`.
fn elapsed_ns(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// The frame an explain renders: the write's file-sink frame in the formulation's output layout,
/// or a draining run without a write.
fn sink_frame(ordered: OrderedFrame, write: Option<WriteTarget>) -> Result<DataFrame> {
    match write {
        Some(target) => target.sink_frame(ordered),
        None => sink::drain(ordered),
    }
}

/// The output of a probe that writes to `write`, checked vacant, and refused if it holds
/// `metrics_directory`, whose records its removal would reach. `None` for a draining probe.
async fn probe_output(
    ctx: &SessionContext,
    write: Option<WriteTarget>,
    metrics_directory: &MetricsDirectory,
) -> Result<Option<VacantTarget>> {
    let Some(target) = write else {
        return Ok(None);
    };
    let target = target.vacant(ctx).await?;
    if target.contains(metrics_directory.path())? {
        return Err(DataFusionError::Configuration(format!(
            "the probe's metrics directory '{}' is under its output path '{}', everything under which the probe removes",
            metrics_directory.path(),
            target.target().output_path
        )));
    }
    Ok(Some(target))
}

async fn explain(frame: DataFrame, analyze: bool) -> Result<Outcome> {
    let batches = frame.explain(false, analyze)?.collect().await?;
    Ok(Outcome::Plan(pretty_format_batches(&batches)?.to_string()))
}

/// What a pipeline hands back to its caller.
#[derive(Debug)]
pub enum Outcome {
    /// The number of rows written to an output path.
    RowsWritten(u64),
    /// The number of rows a measured write wrote, and the names of the metrics its plan reported
    /// that the run metrics table has no column for, sorted and without repeats.
    Measured {
        rows_written: u64,
        unrecorded_metrics: Vec<String>,
    },
    /// The rows a probe's sink received before the stop, its steady-state throughput, why it
    /// stopped, and the names of the metrics its plan reported that the run metrics table has no
    /// column for, sorted and without repeats.
    Probed {
        rows_received: u64,
        steady_state_throughput: Option<f64>,
        stop_reason: StopReason,
        unrecorded_metrics: Vec<String>,
    },
    /// Record batches collected in memory.
    Batches(Vec<RecordBatch>),
    /// A plain or analyzed plan rendered as text.
    Plan(String),
}

impl Outcome {
    /// Renders the outcome for display: a measured write's row count is followed by one warning
    /// line per unrecorded metric, and a probe's by its steady-state throughput and stop reason
    /// before the warnings.
    ///
    /// # Errors
    ///
    /// Returns an error if a collected record batch cannot be formatted.
    pub fn render(&self) -> Result<String> {
        match self {
            Self::RowsWritten(count) => Ok(count.to_string()),
            Self::Measured {
                rows_written,
                unrecorded_metrics,
            } => Ok(lines_with_warnings(
                [rows_written.to_string()],
                unrecorded_metrics,
            )),
            Self::Probed {
                rows_received,
                steady_state_throughput,
                stop_reason,
                unrecorded_metrics,
            } => {
                let throughput = steady_state_throughput
                    .map_or_else(|| "none".to_string(), |rate| format!("{rate:.1} rows/s"));
                Ok(lines_with_warnings(
                    [
                        rows_received.to_string(),
                        format!("steady-state throughput: {throughput}"),
                        format!("stop reason: {}", stop_reason.name()),
                    ],
                    unrecorded_metrics,
                ))
            }
            Self::Batches(batches) => Ok(pretty_format_batches(batches)?.to_string()),
            Self::Plan(plan) => Ok(plan.clone()),
        }
    }
}

/// `lines`, then one warning line per name in `unrecorded_metrics`, joined by newlines.
fn lines_with_warnings(
    lines: impl IntoIterator<Item = String>,
    unrecorded_metrics: &[String],
) -> String {
    let warnings = unrecorded_metrics.iter().map(|name| {
        format!(
            "warning: metric '{name}' has no column in the run metrics table and was not recorded"
        )
    });
    lines
        .into_iter()
        .chain(warnings)
        .collect::<Vec<_>>()
        .join("\n")
}
