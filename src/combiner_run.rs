//! A resolved combiner run, the action it performs, and its outcome.

use crate::{
    dataset::Dataset,
    format::{InputFormat, OutputFormat},
    formulation::Formulation,
    ordered_frame::OrderedFrame,
    pipeline::{self, PipelineOptions},
    process,
    run_metrics::{self, RunRecord},
    sink,
    write::WriteTarget,
};

use datafusion::{
    arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches},
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
    object_store::{self, ObjectStoreExt},
    physical_plan::ExecutionPlan,
    prelude::{DataFrame, SessionContext},
};
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Instant, SystemTime},
};

/// What to do with the combined rows.
///
/// Every action runs the rows into a sink that requires the formulation's ordering: the file
/// sink for a write, measured or not, a collecting sink for collect, and a draining sink for an
/// explain without a write. The run supplies the ordering to each. See ADR 0014 for why the plan
/// ends in a sink rather than a sort.
#[derive(Debug)]
pub enum Action {
    /// Write the rows to the target.
    Write(WriteTarget),
    /// Write the rows to the target and record the run: its run record at
    /// `<metrics_directory>/runs/<run_id>.parquet` and its run metrics at
    /// `<metrics_directory>/metrics/<run_id>.parquet`, both Parquet whatever the output format.
    /// See ADR 0016.
    MeasuredWrite {
        write: WriteTarget,
        metrics_directory: String,
        run_id: String,
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
            Self::Collect
            | Self::Explain { write: None }
            | Self::ExplainAnalyze { write: None } => None,
        }
    }

    /// The directory a measured write records the run under, if this action is one.
    #[must_use]
    pub fn metrics_directory(&self) -> Option<&str> {
        match self {
            Self::MeasuredWrite {
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
    /// Returns an error if a path is on a store the pipeline cannot serve, a measured write's run
    /// id already has a run record under its metrics directory, the input dataset cannot be
    /// resolved, the formulation cannot be planned or executed, or the requested output cannot be
    /// written. A measured write that fails records nothing: the refusal of a repeated id comes
    /// before dataset discovery, and the tables are written only after the data write succeeds.
    pub fn execute(self) -> Result<Outcome> {
        let started_at = SystemTime::now();
        let started = Instant::now();
        let Self {
            formulation,
            input_path,
            input_format,
            action,
            sample_set,
            row_limit,
            threads,
        } = self;
        let options = PipelineOptions::for_paths(
            threads,
            [
                Some(input_path.as_str()),
                action.output_path(),
                action.metrics_directory(),
            ]
            .into_iter()
            .flatten(),
        )?;

        pipeline::run(
            move |ctx| async move {
                if let Action::MeasuredWrite {
                    metrics_directory,
                    run_id,
                    ..
                } = &action
                {
                    refuse_recorded_run(&ctx, metrics_directory, run_id).await?;
                }
                let table_path = ListingTableUrl::parse(&input_path)?;
                let dataset = Dataset::discover(
                    &ctx,
                    table_path,
                    input_format.clone(),
                    formulation.required_ordering(),
                    None,
                )
                .await?;
                let dataset = match sample_set {
                    Some(sample_set) => dataset.restrict_to(&sample_set)?,
                    None => dataset,
                };
                let samples = dataset.sample_set().len();
                let ordered = formulation.plan(&ctx, &dataset).await?;
                let ordered = match row_limit {
                    Some(limit) => ordered.limit(limit)?,
                    None => ordered,
                };

                match action {
                    Action::Write(target) => Ok(Outcome::RowsWritten(
                        target.write(ordered).await?.rows_written,
                    )),
                    Action::MeasuredWrite {
                        write,
                        metrics_directory,
                        run_id,
                    } => {
                        let executed = write.write(ordered).await?;
                        let run_ns = elapsed_ns(started);
                        let peak_rss_bytes = process::peak_rss_bytes()?;
                        let record = RunRecord {
                            run_id,
                            started_at,
                            formulation: formulation.to_string(),
                            groups: formulation.groups().map(NonZeroUsize::get),
                            split_points: formulation.split_points().map(ToString::to_string),
                            dataset_path: input_path,
                            input_format: input_format.name().to_string(),
                            output_format: write.output_format.name().to_string(),
                            compression: write.output_format.compression().map(str::to_string),
                            threads: threads.get(),
                            samples,
                            output_path: write.output_path,
                            rows_written: executed.rows_written,
                            run_ns,
                            execute_ns: executed.execute_ns,
                            peak_rss_bytes,
                        };
                        record_run(&ctx, &metrics_directory, &record, &executed.plan).await
                    }
                    Action::Collect => {
                        let (frame, sink) = sink::collect(ordered)?;
                        frame.collect().await?;
                        Ok(Outcome::Batches(sink.take()))
                    }
                    Action::Explain { write } => explain(sink_frame(ordered, write)?, false).await,
                    Action::ExplainAnalyze { write } => {
                        explain(sink_frame(ordered, write)?, true).await
                    }
                }
            },
            options,
        )
    }
}

/// The subdirectory of a metrics directory holding the run record table.
const RUNS_TABLE: &str = "runs";
/// The subdirectory of a metrics directory holding the run metrics table.
const METRICS_TABLE: &str = "metrics";

/// Whether `run_id` already has a run record under `metrics_directory`, on whichever object
/// store the session serves the directory from.
///
/// The run record is what marks a run as recorded: a measured write writes it last, so a run
/// whose run metrics failed to write has no record, and its id may be retried.
///
/// # Errors
///
/// Returns an error if the metrics directory is on a store the session does not serve, or the
/// store cannot answer.
pub(crate) async fn run_is_recorded(
    ctx: &SessionContext,
    metrics_directory: &str,
    run_id: &str,
) -> Result<bool> {
    let url = ListingTableUrl::parse(run_table_path(metrics_directory, RUNS_TABLE, run_id))?;
    let store = ctx.runtime_env().object_store(&url)?;
    match store.head(url.prefix()).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Fails a measured write of `run_id` under `metrics_directory` when the id already has a run
/// record there, so that a repeated id cannot replace a recorded run. See ADR 0016.
async fn refuse_recorded_run(
    ctx: &SessionContext,
    metrics_directory: &str,
    run_id: &str,
) -> Result<()> {
    if run_is_recorded(ctx, metrics_directory, run_id).await? {
        return Err(DataFusionError::Configuration(format!(
            "run id '{run_id}' already has a run record at '{}'; a measured write does not replace a recorded run",
            run_table_path(metrics_directory, RUNS_TABLE, run_id)
        )));
    }
    Ok(())
}

/// Records a measured write: writes the run metrics of `plan` and then `record` under
/// `metrics_directory`, as Parquet, and hands back the outcome. The run record goes last because
/// its presence is what [`run_is_recorded`] checks: a failure between the two writes leaves run
/// metrics without a record, which a retry of the same id replaces rather than being refused.
async fn record_run(
    ctx: &SessionContext,
    metrics_directory: &str,
    record: &RunRecord,
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Outcome> {
    let metrics = run_metrics::run_metrics_batch(&record.run_id, plan)?;
    write_table(
        ctx,
        metrics.batch,
        &run_table_path(metrics_directory, METRICS_TABLE, &record.run_id),
    )
    .await?;
    write_table(
        ctx,
        run_metrics::run_record_batch(record)?,
        &run_table_path(metrics_directory, RUNS_TABLE, &record.run_id),
    )
    .await?;
    Ok(Outcome::Measured {
        rows_written: record.rows_written,
        unrecorded_metrics: metrics.unrecorded,
    })
}

/// The path of the run `run_id`'s file in the table `table` under `metrics_directory`.
fn run_table_path(metrics_directory: &str, table: &str, run_id: &str) -> String {
    format!(
        "{}/{table}/{run_id}.parquet",
        metrics_directory.trim_end_matches('/')
    )
}

/// Writes `batch` as one Parquet file at `path`.
async fn write_table(ctx: &SessionContext, batch: RecordBatch, path: &str) -> Result<()> {
    WriteTarget {
        output_path: path.to_string(),
        output_format: OutputFormat::PARQUET,
    }
    .write_unordered(ctx.read_batch(batch)?)
    .await?;
    Ok(())
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
    /// Record batches collected in memory.
    Batches(Vec<RecordBatch>),
    /// A plain or analyzed plan rendered as text.
    Plan(String),
}

impl Outcome {
    /// Renders the outcome for display: a measured write's row count is followed by one warning
    /// line per unrecorded metric.
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
            } => {
                let warnings = unrecorded_metrics.iter().map(|name| {
                    format!(
                        "warning: metric '{name}' has no column in the run metrics table and was not recorded"
                    )
                });
                Ok(std::iter::once(rows_written.to_string())
                    .chain(warnings)
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            Self::Batches(batches) => Ok(pretty_format_batches(batches)?.to_string()),
            Self::Plan(plan) => Ok(plan.clone()),
        }
    }
}
