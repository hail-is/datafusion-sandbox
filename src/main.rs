use clap::{Args, Parser, Subcommand};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::prelude::DataFrame;

use datafusion_sandbox::pipeline::{self, PipelineOptions};
use datafusion_sandbox::{
    Outcome, SAMPLES, combine_alleles, combine_refs, combiner_session_config, vortex_format, write,
    write_count,
};
use std::sync::Arc;
use vortex_datafusion::VortexFormatFactory;

const DEFAULT_SHOW_LIMIT: usize = 20;

#[derive(Parser)]
#[command(about = "Run hail-style pipelines built on datafusion")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Number of worker threads to execute on. 1 runs on a current-thread runtime, for timing
    /// single-threaded performance. Defaults to the number of available cores.
    #[arg(long, short = 'j', global = true)]
    threads: Option<usize>,
}

#[derive(Subcommand)]
enum Command {
    /// Combine the reference data of all samples under PATH.
    CombineRefs(CombinerArgs),
    /// Combine the alleles of all samples under PATH.
    CombineAlleles(CombinerArgs),
}

#[derive(Args)]
struct CombinerArgs {
    path: String,
    #[command(flatten)]
    ending: EndingArgs,
    /// Return at most ROWS combined rows. Defaults to 20 with --show and unlimited otherwise.
    #[arg(long, value_name = "ROWS")]
    limit: Option<usize>,
}

#[derive(Args)]
#[group(required = true, multiple = false)]
struct EndingArgs {
    /// Write the combined rows to PATH.
    #[arg(long, value_name = "PATH")]
    write: Option<String>,
    /// Print the combined rows.
    #[arg(long)]
    show: bool,
    /// Print the logical and physical plan without executing it.
    #[arg(long)]
    explain: bool,
    /// Execute the plan and print it with per-operator metrics.
    #[arg(long)]
    explain_analyze: bool,
}

enum Ending {
    Write(String),
    Show,
    Explain,
    ExplainAnalyze,
}

impl Ending {
    fn row_limit(&self, explicit_limit: Option<usize>) -> Option<usize> {
        explicit_limit.or_else(|| matches!(self, Self::Show).then_some(DEFAULT_SHOW_LIMIT))
    }

    fn output_path(&self) -> Option<&str> {
        match self {
            Self::Write(path) => Some(path),
            Self::Show | Self::Explain | Self::ExplainAnalyze => None,
        }
    }
}

impl From<EndingArgs> for Ending {
    fn from(args: EndingArgs) -> Self {
        match args {
            EndingArgs {
                write: Some(path), ..
            } => Self::Write(path),
            EndingArgs { show: true, .. } => Self::Show,
            EndingArgs { explain: true, .. } => Self::Explain,
            EndingArgs {
                explain_analyze: true,
                ..
            } => Self::ExplainAnalyze,
            _ => unreachable!("clap requires exactly one ending"),
        }
    }
}

enum Combiner {
    Refs,
    Alleles,
}

fn main() -> Result<()> {
    let Cli { command, threads } = Cli::parse();

    let (combiner, args) = match command {
        Command::CombineRefs(args) => (Combiner::Refs, args),
        Command::CombineAlleles(args) => (Combiner::Alleles, args),
    };
    let CombinerArgs {
        path,
        ending,
        limit,
    } = args;
    let ending = Ending::from(ending);
    let limit = ending.row_limit(limit);
    let options = options_for(&path, ending.output_path(), threads);
    let outcome = pipeline::run(
        move |ctx| async move {
            let df = match combiner {
                Combiner::Refs => combine_refs::plan(&ctx, &path, SAMPLES, vortex_format()).await?,
                Combiner::Alleles => {
                    combine_alleles::plan(&ctx, &path, SAMPLES, vortex_format()).await?
                }
            };
            let df = match limit {
                Some(limit) => df.limit(0, Some(limit))?,
                None => df,
            };
            produce_outcome(df, ending).await
        },
        options,
    )?;
    println!("{outcome}");
    Ok(())
}

async fn produce_outcome(df: DataFrame, ending: Ending) -> Result<Outcome> {
    match ending {
        Ending::Write(output) => {
            let write_result = write(df, &output, Arc::new(VortexFormatFactory::new())).await?;
            Ok(Outcome::RowsWritten(write_count(&write_result)?))
        }
        Ending::Show => Ok(Outcome::Batches(df.collect().await?)),
        Ending::Explain => {
            let batches = df.explain(false, false)?.collect().await?;
            Ok(Outcome::Plan(pretty_format_batches(&batches)?.to_string()))
        }
        Ending::ExplainAnalyze => {
            let batches = df.explain(false, true)?.collect().await?;
            Ok(Outcome::Plan(pretty_format_batches(&batches)?.to_string()))
        }
    }
}

/// Pipeline options for a combiner reading from `input_path` and optionally writing to
/// `output_path`: the object stores to register are the ones those paths live on, and local paths
/// need none at all.
fn options_for(
    input_path: &str,
    output_path: Option<&str>,
    threads: Option<usize>,
) -> PipelineOptions {
    let mut options = PipelineOptions {
        session_config: combiner_session_config(),
        ..Default::default()
    };
    if let Some(threads) = threads {
        options.threads = threads;
    }
    options.object_stores = [Some(input_path), output_path]
        .into_iter()
        .flatten()
        .filter_map(object_store_base_url)
        .collect();
    options.object_stores.dedup();
    options
}

/// The base URL of the object store `path` lives on, e.g. "gs://my-bucket", or None for a
/// local path.
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
    use super::*;

    #[test]
    fn registers_the_object_stores_of_both_input_and_output() {
        let options = options_for("gs://bucket-a/path", Some("gs://bucket-b/out.vortex"), None);
        assert_eq!(options.object_stores, ["gs://bucket-a", "gs://bucket-b"]);
    }

    #[test]
    fn registers_a_shared_object_store_once() {
        let options = options_for("gs://bucket/path/", Some("gs://bucket/out.vortex"), None);
        assert_eq!(options.object_stores, ["gs://bucket"]);
    }

    #[test]
    fn registers_no_object_stores_for_local_paths() {
        let options = options_for("data/samples", Some("data/out.vortex"), None);
        assert!(options.object_stores.is_empty());
    }

    #[test]
    fn registers_only_the_input_store_when_there_is_no_output() {
        let options = options_for("gs://bucket-a/path", None, None);
        assert_eq!(options.object_stores, ["gs://bucket-a"]);
    }
}
