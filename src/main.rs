use clap::{Args, Parser, Subcommand, ValueEnum};
use datafusion::error::Result;

use datafusion_sandbox::combiner_run::{Action, CombinerRun};
use datafusion_sandbox::format::{InputFormat, OutputFormat};
use datafusion_sandbox::formulation::Formulation;
use std::{path::Path, thread::available_parallelism};

const DEFAULT_SHOW_LIMIT: usize = 20;

#[derive(Parser)]
#[command(about = "Run hail-style pipelines built on datafusion")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Number of worker threads to execute on.
    ///
    /// Defaults to available parallelism. Does not affect the number of partitions the plan is
    /// built with, which each formulation settles for itself.
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
    /// Combine reference data for the dataset's sample set under PATH.
    CombineRefs(CombineRefsArgs),
    /// Combine alleles for the dataset's sample set under PATH.
    CombineAlleles(CombinerArgs),
}

#[derive(Args)]
struct CombineRefsArgs {
    #[command(flatten)]
    combiner: CombinerArgs,
    /// How to build the reference combiner's plan.
    #[arg(long, value_enum, default_value = "union")]
    formulation: CombineRefsFormulationArg,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CombineRefsFormulationArg {
    Union,
}

impl CombineRefsFormulationArg {
    fn formulation(self) -> Formulation {
        match self {
            Self::Union => Formulation::CombineRefsUnion,
        }
    }
}

#[derive(Args)]
struct CombinerArgs {
    path: String,
    #[command(flatten)]
    formats: FormatArgs,
    #[command(flatten)]
    action: ActionArgs,
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
    /// Restrict the dataset's sample set to comma-separated sample ids.
    #[arg(long = "samples", value_delimiter = ',', value_name = "SAMPLE,...")]
    sample_set: Vec<String>,
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
struct ActionArgs {
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

enum CliAction {
    Write(String),
    Show,
    Explain,
    ExplainAnalyze,
}

impl CliAction {
    fn row_limit(&self, explicit_limit: Option<usize>) -> Option<usize> {
        explicit_limit.or_else(|| matches!(self, Self::Show).then_some(DEFAULT_SHOW_LIMIT))
    }
}

impl From<ActionArgs> for CliAction {
    fn from(args: ActionArgs) -> Self {
        match args {
            ActionArgs {
                write: Some(path), ..
            } => Self::Write(path),
            ActionArgs { show: true, .. } => Self::Show,
            ActionArgs { explain: true, .. } => Self::Explain,
            ActionArgs {
                explain_analyze: true,
                ..
            } => Self::ExplainAnalyze,
            _ => unreachable!("clap requires exactly one action"),
        }
    }
}

fn main() -> Result<()> {
    let Cli { command, threads } = Cli::parse();

    let (formulation, args) = match command {
        Command::CombineRefs(args) => (args.formulation.formulation(), args.combiner),
        Command::CombineAlleles(args) => (Formulation::CombineAllelesUnion, args),
    };
    let CombinerArgs {
        path,
        formats,
        action,
        compression,
        limit,
        sample_set,
    } = args;
    let cli_action = CliAction::from(action);
    let input_format = formats.input_format.format();
    let limit = cli_action.row_limit(limit);
    let action = match cli_action {
        CliAction::Write(output_path) => {
            let output_format = formats.output_format().format();
            validate_output_extension(&output_path, &output_format)?;
            let output_format = match compression {
                Some(compression) => output_format.with_compression(&compression)?,
                None => output_format,
            };
            Action::Write {
                output_path,
                output_format,
            }
        }
        CliAction::Show => Action::Collect,
        CliAction::Explain => Action::Explain,
        CliAction::ExplainAnalyze => Action::ExplainAnalyze,
    };
    let sample_set = (!sample_set.is_empty()).then_some(sample_set);
    let threads = threads.unwrap_or_else(|| available_parallelism().map(|n| n.get()).unwrap_or(1));
    println!("formulation: {formulation}");
    let outcome = CombinerRun {
        formulation,
        input_path: path,
        input_format,
        action,
        sample_set,
        row_limit: limit,
        threads,
    }
    .execute()?;
    println!("{outcome}");
    Ok(())
}

fn validate_output_extension(output_path: &str, output_format: &OutputFormat) -> Result<()> {
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
}
