//! A resolved combiner run, the action it performs, and its outcome.

use crate::{
    format::InputFormat,
    formulation::Formulation,
    metrics_directory::MetricsDirectory,
    ordered_frame::OrderedFrame,
    pipeline::{self, PipelineOptions},
    process,
    run_metrics::{FormulationRecord, ProbeRecord, RunRecord, WriteRecord},
    sink,
    stored::dataset::Dataset,
    throughput_probe::{ProbeSettings, StopReason},
    write::WriteTarget,
};

use datafusion::{
    arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches},
    datasource::listing::ListingTableUrl,
    error::Result,
    prelude::{DataFrame, SessionContext},
};
use std::{
    num::NonZeroUsize,
    time::{Instant, SystemTime},
};

/// What to do with the combined rows.
///
/// Every action runs the rows into a sink that requires the formulation's ordering: the file
/// sink for a write, measured or not, a collecting sink for collect, and a draining sink for a
/// throughput probe and for an explain without a write. The run supplies the ordering to each.
/// See ADR 0014 for why the plan ends in a sink rather than a sort.
#[derive(Debug)]
pub enum Action {
    /// Write the rows to the target.
    Write(WriteTarget),
    /// Write the rows to the target and record the run as `run_id` under the metrics directory.
    /// See ADR 0016.
    MeasuredWrite {
        write: WriteTarget,
        metrics_directory: MetricsDirectory,
        run_id: String,
    },
    /// Drain the rows until the throughput probe's settings stop the run, and record it as
    /// `run_id` under the metrics directory with its progress samples. See ADR 0017.
    Probe {
        metrics_directory: MetricsDirectory,
        run_id: String,
        settings: ProbeSettings,
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
            } => Some(&target.output_path),
            Self::Probe { .. }
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
}

/// One execution of a combiner against a dataset.
pub struct CombinerRun {
    pub formulation: Formulation,
    pub input_path: String,
    pub input_format: InputFormat,
    pub action: Action,
    pub sample_set: Option<Vec<String>>,
    pub row_limit: Option<usize>,
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
                Some(self.input_path.as_str()),
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
    /// its metrics directory, the input dataset cannot be resolved, the formulation cannot be
    /// planned or executed, or the requested output cannot be written. A measured write or a
    /// probe that fails records nothing: the refusal of a repeated id comes before dataset
    /// discovery, and the run is recorded only after the data write or the probe succeeds. A
    /// probe also fails on a session the pipeline runner did not build, which has no IO runtime
    /// to sample on.
    pub async fn execute_in(self, ctx: &SessionContext) -> Result<Outcome> {
        self.run(ctx, Started::now()).await
    }

    async fn run(self, ctx: &SessionContext, started: Started) -> Result<Outcome> {
        let Self {
            formulation,
            input_path,
            input_format,
            action,
            sample_set,
            row_limit,
            threads,
        } = self;
        let facts = RunFacts {
            started,
            formulation: (&formulation).into(),
            dataset_path: input_path.clone(),
            input_format: input_format.name().to_string(),
            threads: threads.get(),
        };
        let rows = || {
            plan_rows(
                ctx,
                &formulation,
                &input_path,
                &input_format,
                sample_set.as_deref(),
                row_limit,
            )
        };

        match action {
            Action::Write(target) => Ok(Outcome::RowsWritten(
                target.write(rows().await?.0).await?.rows_written,
            )),
            Action::MeasuredWrite {
                write,
                metrics_directory,
                run_id,
            } => {
                let run = metrics_directory.unrecorded(ctx, &run_id).await?;
                let (ordered, samples) = rows().await?;
                let executed = write.write(ordered).await?;
                let record = facts.record(
                    run_id,
                    samples,
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
            Action::Probe {
                metrics_directory,
                run_id,
                settings,
            } => {
                let run = metrics_directory.unrecorded(ctx, &run_id).await?;
                let (ordered, samples) = rows().await?;
                let probed = sink::probe(sink::drain(ordered)?, &settings).await?;
                let record = facts.record(
                    run_id,
                    samples,
                    None,
                    probed.rows_received,
                    probed.execute_ns,
                    Some(ProbeRecord {
                        settings,
                        decision: probed.decision.clone(),
                        first_partition_end_ns: probed.first_partition_end_ns,
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
            Action::Collect => {
                let (frame, sink) = sink::collect(rows().await?.0)?;
                frame.collect().await?;
                Ok(Outcome::Batches(sink.take()))
            }
            Action::Explain { write } => explain(sink_frame(rows().await?.0, write)?, false).await,
            Action::ExplainAnalyze { write } => {
                explain(sink_frame(rows().await?.0, write)?, true).await
            }
        }
    }
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
    dataset_path: String,
    input_format: String,
    threads: usize,
}

impl RunFacts {
    /// The run record of `run_id`, a run over `samples` samples that ends now. Its action wrote
    /// `write`, if any, wrote `rows_written` rows, or received them for a probe, and executed for
    /// `execute_ns`. A probe's settings and decision are `probe`.
    fn record(
        self,
        run_id: String,
        samples: usize,
        write: Option<WriteRecord>,
        rows_written: u64,
        execute_ns: u64,
        probe: Option<ProbeRecord>,
    ) -> Result<RunRecord> {
        Ok(RunRecord {
            run_id,
            started_at: self.started.at,
            formulation: self.formulation,
            dataset_path: self.dataset_path,
            input_format: self.input_format,
            write,
            threads: self.threads,
            samples,
            rows_written,
            run_ns: elapsed_ns(self.started.instant),
            execute_ns,
            peak_rss_bytes: process::peak_rss_bytes()?,
            probe,
        })
    }
}

/// The formulation's rows over the dataset at `input_path`, restricted to `sample_set` and
/// limited to `row_limit` rows when given, and the size of the sample set they cover.
async fn plan_rows(
    ctx: &SessionContext,
    formulation: &Formulation,
    input_path: &str,
    input_format: &InputFormat,
    sample_set: Option<&[String]>,
    row_limit: Option<usize>,
) -> Result<(OrderedFrame, usize)> {
    let dataset = Dataset::discover(
        ctx,
        ListingTableUrl::parse(input_path)?,
        input_format.clone(),
        formulation.required_ordering(),
        None,
    )
    .await?;
    let dataset = match sample_set {
        Some(sample_set) => dataset.restrict_to(sample_set)?,
        None => dataset,
    };
    let samples = dataset.sample_set().len();
    let ordered = formulation.plan(ctx, &dataset).await?;
    let ordered = match row_limit {
        Some(limit) => ordered.limit(limit)?,
        None => ordered,
    };
    Ok((ordered, samples))
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
