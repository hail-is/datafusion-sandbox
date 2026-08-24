use clap::{Args, Parser, Subcommand, ValueEnum};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::DataFrame;

use datafusion_sandbox::format::{InputFormat, OutputFormat};
use datafusion_sandbox::pipeline::{self, PipelineOptions};
use datafusion_sandbox::{
    Outcome, SAMPLES, combine_alleles, combine_refs, combine_refs_one_scan,
    combiner_session_config, write,
};
use std::path::Path;

const DEFAULT_SHOW_LIMIT: usize = 20;

#[derive(Parser)]
#[command(about = "Run hail-style pipelines built on datafusion")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Number of worker threads to execute on.
    ///
    /// Defaults to available parallelism. Does not affect the number of partitions the plan is
    /// built with, which each combiner sets for itself.
    #[arg(long, short = 'j', global = true, value_parser = parse_thread_count)]
    threads: Option<usize>,
}

/// Rejects 0, which `Builder::worker_threads` panics on, with a message rather than clap's
/// generated `0 is not in 1..18446744073709551615`.
fn parse_thread_count(value: &str) -> std::result::Result<usize, String> {
    match value.parse::<usize>() {
        Ok(0) => Err("thread count must be at least 1".to_string()),
        Ok(threads) => Ok(threads),
        Err(e) => Err(e.to_string()),
    }
}

#[derive(Subcommand)]
enum Command {
    /// Combine the reference data of all samples under PATH.
    CombineRefs(CombinerArgs),
    /// Combine reference data with one shared scan of all samples under PATH.
    CombineRefsOneScan(CombinerArgs),
    /// Combine the alleles of all samples under PATH.
    CombineAlleles(CombinerArgs),
}

#[derive(Args)]
struct CombinerArgs {
    path: String,
    #[command(flatten)]
    formats: FormatArgs,
    #[command(flatten)]
    ending: EndingArgs,
    /// Compression to use when writing.
    ///
    /// Parquet accepts uncompressed, snappy, gzip(LEVEL), brotli(LEVEL), lz4, zstd(LEVEL), or lz4_raw.
    /// Vortex accepts standard or compact.
    #[arg(
        long,
        value_name = "COMPRESSION",
        requires = "write",
        conflicts_with_all = ["show", "explain", "explain_analyze"]
    )]
    compression: Option<String>,
    /// Return at most ROWS combined rows. Defaults to 20 with --show and unlimited otherwise.
    #[arg(long, value_name = "ROWS")]
    limit: Option<usize>,
}

#[derive(Args)]
struct FormatArgs {
    /// Format of the input tables.
    #[arg(long, value_enum, default_value = "vortex")]
    input_format: InputFormatArg,
    /// Format to write. Defaults to the input format.
    #[arg(long, value_enum)]
    output_format: Option<OutputFormatArg>,
}

impl FormatArgs {
    fn output_format(&self) -> OutputFormatArg {
        self.output_format
            .unwrap_or_else(|| self.input_format.default_output_format())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum InputFormatArg {
    Parquet,
    Vortex,
}

impl InputFormatArg {
    fn format(self) -> InputFormat {
        match self {
            Self::Parquet => InputFormat::PARQUET,
            Self::Vortex => InputFormat::VORTEX,
        }
    }

    fn default_output_format(self) -> OutputFormatArg {
        match self {
            Self::Parquet => OutputFormatArg::Parquet,
            Self::Vortex => OutputFormatArg::Vortex,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormatArg {
    Parquet,
    Vortex,
}

impl OutputFormatArg {
    fn format(self) -> OutputFormat {
        match self {
            Self::Parquet => OutputFormat::PARQUET,
            Self::Vortex => OutputFormat::VORTEX,
        }
    }
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

#[derive(Clone, Copy)]
enum Combiner {
    Refs,
    RefsOneScan,
    Alleles,
}

fn main() -> Result<()> {
    let Cli { command, threads } = Cli::parse();

    let (combiner, args) = match command {
        Command::CombineRefs(args) => (Combiner::Refs, args),
        Command::CombineRefsOneScan(args) => (Combiner::RefsOneScan, args),
        Command::CombineAlleles(args) => (Combiner::Alleles, args),
    };
    let CombinerArgs {
        path,
        formats,
        ending,
        compression,
        limit,
    } = args;
    let ending = Ending::from(ending);
    let input_format = formats.input_format.format();
    let output_format = formats.output_format().format();
    validate_output_extension(ending.output_path(), &output_format)?;
    let output_format = match compression {
        Some(compression) => output_format.with_compression(&compression)?,
        None => output_format,
    };
    let limit = ending.row_limit(limit);
    let session_config = match combiner {
        Combiner::RefsOneScan => combine_refs_one_scan::session_config(),
        Combiner::Refs | Combiner::Alleles => combiner_session_config(),
    };
    let options = options_for(&path, ending.output_path(), threads, session_config);
    let outcome = pipeline::run(
        move |ctx| async move {
            let df = match combiner {
                Combiner::Refs => combine_refs::plan(&ctx, &path, SAMPLES, input_format).await?,
                Combiner::RefsOneScan => {
                    combine_refs_one_scan::plan(&ctx, &path, input_format).await?
                }
                Combiner::Alleles => {
                    combine_alleles::plan(&ctx, &path, SAMPLES, input_format).await?
                }
            };
            let df = match limit {
                Some(limit) => df.limit(0, Some(limit))?,
                None => df,
            };
            produce_outcome(df, ending, output_format).await
        },
        options,
    )?;
    println!("{outcome}");
    Ok(())
}

async fn produce_outcome(
    df: DataFrame,
    ending: Ending,
    output_format: OutputFormat,
) -> Result<Outcome> {
    match ending {
        Ending::Write(output) => Ok(Outcome::RowsWritten(
            write(df, &output, &output_format).await?,
        )),
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

fn validate_output_extension(
    output_path: Option<&str>,
    output_format: &OutputFormat,
) -> Result<()> {
    let Some(output_path) = output_path else {
        return Ok(());
    };
    let Some(extension) = Path::new(output_path)
        .extension()
        .and_then(|ext| ext.to_str())
    else {
        return Ok(());
    };
    if extension != output_format.extension() {
        return Err(datafusion::error::DataFusionError::Configuration(format!(
            "output path '{output_path}' has extension '.{extension}', which contradicts output format '{}'",
            output_format
        )));
    }
    Ok(())
}

/// Pipeline options for a combiner reading from `input_path` and optionally writing to
/// `output_path`: the object stores to register are the ones those paths live on, and local paths
/// need none at all.
fn options_for(
    input_path: &str,
    output_path: Option<&str>,
    threads: Option<usize>,
    session_config: SessionConfig,
) -> PipelineOptions {
    let mut options = PipelineOptions {
        session_config,
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
    fn rejects_a_thread_count_of_zero() {
        assert_eq!(parse_thread_count("1"), Ok(1));
        assert_eq!(parse_thread_count("8"), Ok(8));

        let err = parse_thread_count("0").unwrap_err();
        assert!(err.contains("at least 1"), "got: {err}");

        assert!(parse_thread_count("banana").is_err());
    }

    #[test]
    fn registers_the_object_stores_of_both_input_and_output() {
        let options = options_for(
            "gs://bucket-a/path",
            Some("gs://bucket-b/out.vortex"),
            None,
            combiner_session_config(),
        );
        assert_eq!(options.object_stores, ["gs://bucket-a", "gs://bucket-b"]);
    }

    #[test]
    fn registers_a_shared_object_store_once() {
        let options = options_for(
            "gs://bucket/path/",
            Some("gs://bucket/out.vortex"),
            None,
            combiner_session_config(),
        );
        assert_eq!(options.object_stores, ["gs://bucket"]);
    }

    #[test]
    fn registers_no_object_stores_for_local_paths() {
        let options = options_for(
            "data/samples",
            Some("data/out.vortex"),
            None,
            combiner_session_config(),
        );
        assert!(options.object_stores.is_empty());
    }

    #[test]
    fn registers_only_the_input_store_when_there_is_no_output() {
        let options = options_for("gs://bucket-a/path", None, None, combiner_session_config());
        assert_eq!(options.object_stores, ["gs://bucket-a"]);
    }
}
