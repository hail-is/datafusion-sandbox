//! A resolved combiner run, the action it performs, and its outcome.

use crate::{
    dataset::Dataset,
    format::{InputFormat, OutputFormat},
    formulation::Formulation,
    pipeline::{self, PipelineOptions},
};

use datafusion::{
    arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches},
    datasource::listing::ListingTableUrl,
    error::Result,
};

/// What to do with the combined rows.
#[derive(Debug)]
pub enum Action {
    /// Write the rows to a path using the given format.
    Write {
        output_path: String,
        output_format: OutputFormat,
    },
    /// Collect the rows in memory.
    Collect,
    /// Render the logical and physical plan without executing it.
    Explain,
    /// Execute the plan and render it with per-operator metrics.
    ExplainAnalyze,
}

/// One execution of a combiner against a dataset.
pub struct CombinerRun {
    pub formulation: Formulation,
    pub input_path: String,
    pub input_format: InputFormat,
    pub action: Action,
    pub sample_set: Option<Vec<String>>,
    pub row_limit: Option<usize>,
    pub threads: usize,
}

impl CombinerRun {
    /// Executes the run to completion on its own runtimes.
    ///
    /// # Errors
    ///
    /// Returns an error if the input dataset cannot be resolved, the formulation cannot be
    /// planned or executed, or the requested output cannot be written.
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
        let options = options_for(&input_path, &action, threads);

        pipeline::run(
            move |ctx| async move {
                let table_path = ListingTableUrl::parse(input_path)?;
                let layout = formulation.required_layout();
                let dataset =
                    Dataset::discover(&ctx, table_path, input_format, layout, None).await?;
                let dataset = match sample_set {
                    Some(sample_set) => dataset.restrict_to(&sample_set)?,
                    None => dataset,
                };
                let df = formulation.plan(&ctx, &dataset).await?;
                let df = match row_limit {
                    Some(limit) => df.limit(0, Some(limit))?,
                    None => df,
                };

                match action {
                    Action::Write {
                        output_path,
                        output_format,
                    } => Ok(Outcome::RowsWritten(
                        output_format.write(df, &output_path).await?,
                    )),
                    Action::Collect => Ok(Outcome::Batches(df.collect().await?)),
                    action @ (Action::Explain | Action::ExplainAnalyze) => {
                        let analyze = matches!(action, Action::ExplainAnalyze);
                        let batches = df.explain(false, analyze)?.collect().await?;
                        Ok(Outcome::Plan(pretty_format_batches(&batches)?.to_string()))
                    }
                }
            },
            options,
        )
    }
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

/// Pipeline options for a run. Object stores follow from the paths the run reads and writes.
fn options_for(input_path: &str, action: &Action, threads: usize) -> PipelineOptions {
    let output_path = match action {
        Action::Write { output_path, .. } => Some(output_path.as_str()),
        Action::Collect | Action::Explain | Action::ExplainAnalyze => None,
    };
    let mut object_stores = [Some(input_path), output_path]
        .into_iter()
        .flatten()
        .filter_map(object_store_base_url)
        .collect::<Vec<_>>();
    object_stores.dedup();
    PipelineOptions {
        threads,
        object_stores,
    }
}

/// The base URL of the object store `path` lives on, or `None` for a local path.
fn object_store_base_url(path: &str) -> Option<String> {
    let (scheme, rest) = path.split_once("://")?;
    if scheme == "file" {
        return None;
    }
    let authority = rest.split('/').next().unwrap_or("");
    Some(format!("{scheme}://{authority}"))
}

#[cfg(test)]
mod tests {
    //! These tests need private access to path-to-object-store derivation until it moves behind `PipelineOptions`.

    use super::*;

    #[test]
    fn registers_the_object_stores_of_both_input_and_output() {
        let action = Action::Write {
            output_path: "gs://bucket-b/out.vortex".to_string(),
            output_format: OutputFormat::VORTEX,
        };
        let options = options_for("gs://bucket-a/path", &action, 1);
        assert_eq!(options.object_stores, ["gs://bucket-a", "gs://bucket-b"]);
    }

    #[test]
    fn registers_a_shared_object_store_once() {
        let action = Action::Write {
            output_path: "gs://bucket/out.vortex".to_string(),
            output_format: OutputFormat::VORTEX,
        };
        let options = options_for("gs://bucket/path/", &action, 1);
        assert_eq!(options.object_stores, ["gs://bucket"]);
    }

    #[test]
    fn registers_no_object_stores_for_local_paths() {
        let action = Action::Write {
            output_path: "data/out.vortex".to_string(),
            output_format: OutputFormat::VORTEX,
        };
        let options = options_for("data/samples", &action, 1);
        assert_eq!(options.object_stores, Vec::<String>::new());
    }

    #[test]
    fn registers_only_the_input_store_when_there_is_no_output() {
        let options = options_for("gs://bucket-a/path", &Action::Collect, 1);
        assert_eq!(options.object_stores, ["gs://bucket-a"]);
    }
}
