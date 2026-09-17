//! A resolved combiner run, the action it performs, and its outcome.

use crate::{
    dataset::Dataset,
    format::{InputFormat, OutputFormat},
    formulation::Formulation,
    ordered_frame::OrderedFrame,
    pipeline::{self, PipelineOptions},
    sink,
};

use datafusion::{
    arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches},
    datasource::listing::ListingTableUrl,
    error::Result,
    prelude::DataFrame,
};
use std::num::NonZeroUsize;

/// Where a write puts its rows.
#[derive(Debug)]
pub struct WriteTarget {
    pub output_path: String,
    pub output_format: OutputFormat,
}

/// What to do with the combined rows.
///
/// Every action runs the rows into a sink that requires the formulation's ordering: the file
/// sink for a write, a collecting sink for collect, and a draining sink for an explain without a
/// write. The run supplies the ordering to each. See ADR 0014 for why the plan ends in a sink
/// rather than a sort.
#[derive(Debug)]
pub enum Action {
    /// Write the rows to the target.
    Write(WriteTarget),
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
    /// Returns an error if a path is on a store the pipeline cannot serve, the input dataset
    /// cannot be resolved, the formulation cannot be planned or executed, or the requested output
    /// cannot be written.
    pub fn execute(self) -> Result<Outcome> {
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
            [Some(input_path.as_str()), action.output_path()]
                .into_iter()
                .flatten(),
        )?;

        pipeline::run(
            move |ctx| async move {
                let table_path = ListingTableUrl::parse(input_path)?;
                let dataset = Dataset::discover(
                    &ctx,
                    table_path,
                    input_format,
                    formulation.required_ordering(),
                    None,
                )
                .await?;
                let dataset = match sample_set {
                    Some(sample_set) => dataset.restrict_to(&sample_set)?,
                    None => dataset,
                };
                let ordered = formulation.plan(&ctx, &dataset).await?;
                let ordered = match row_limit {
                    Some(limit) => ordered.limit(limit)?,
                    None => ordered,
                };

                match action {
                    Action::Write(target) => Ok(Outcome::RowsWritten(
                        target
                            .output_format
                            .write(ordered, &target.output_path)
                            .await?,
                    )),
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

/// The frame an explain renders: the write's file-sink frame in the formulation's output layout,
/// or a draining run without a write.
fn sink_frame(ordered: OrderedFrame, write: Option<WriteTarget>) -> Result<DataFrame> {
    match write {
        Some(target) => target
            .output_format
            .sink_frame(ordered, &target.output_path),
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
    /// Record batches collected in memory.
    Batches(Vec<RecordBatch>),
    /// A plain or analyzed plan rendered as text.
    Plan(String),
}

impl Outcome {
    /// Renders the outcome for display.
    ///
    /// # Errors
    ///
    /// Returns an error if a collected record batch cannot be formatted.
    pub fn render(&self) -> Result<String> {
        match self {
            Self::RowsWritten(count) => Ok(count.to_string()),
            Self::Batches(batches) => Ok(pretty_format_batches(batches)?.to_string()),
            Self::Plan(plan) => Ok(plan.clone()),
        }
    }
}
