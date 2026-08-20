use clap::{Args, Parser, Subcommand, ValueEnum};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::file_options::parquet_writer; //::parse_compression_string;
use datafusion::datasource::file_format::{
    FileFormat, FileFormatFactory,
    parquet::{ParquetFormat, ParquetFormatFactory},
};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;

use datafusion_sandbox::pipeline::{self, PipelineOptions};
use datafusion_sandbox::{
    Outcome, SAMPLES, combine_alleles, combine_refs, combiner_session_config, vortex_format, write,
    write_count,
};
use std::{collections::HashMap, path::Path, sync::Arc};
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
    formats: FormatArgs,
    #[command(flatten)]
    ending: EndingArgs,
    /// Compression to use when writing.
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
    input_format: Format,
    /// Format to write. Defaults to the input format.
    #[arg(long, value_enum)]
    output_format: Option<Format>,
}

impl FormatArgs {
    /// When gVCF becomes an input format, change its output default here rather
    /// than giving the input and output arguments separate enums.
    fn output_format(&self) -> Format {
        self.output_format.unwrap_or(self.input_format)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Format {
    Parquet,
    Vortex,
}

impl Format {
    fn read_format(self) -> Arc<dyn FileFormat> {
        match self {
            Self::Parquet => Arc::new(ParquetFormat::default()),
            Self::Vortex => vortex_format(),
        }
    }

    fn output_factory(self) -> Arc<dyn FileFormatFactory> {
        match self {
            Self::Parquet => Arc::new(ParquetFormatFactory::new()),
            Self::Vortex => Arc::new(VortexFormatFactory::new()),
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Vortex => "vortex",
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
        formats,
        ending,
        compression,
        limit,
    } = args;
    let ending = Ending::from(ending);
    let input_format = formats.input_format;
    let output_format = formats.output_format();
    validate_output_extension(ending.output_path(), output_format)?;
    let format_options = compression_options(compression.as_deref(), output_format)?;
    let limit = ending.row_limit(limit);
    let options = options_for(&path, ending.output_path(), threads);
    let outcome = pipeline::run(
        move |ctx| async move {
            let input_format = input_format.read_format();
            let df = match combiner {
                Combiner::Refs => combine_refs::plan(&ctx, &path, SAMPLES, input_format).await?,
                Combiner::Alleles => {
                    combine_alleles::plan(&ctx, &path, SAMPLES, input_format).await?
                }
            };
            let df = match limit {
                Some(limit) => df.limit(0, Some(limit))?,
                None => df,
            };
            produce_outcome(df, ending, output_format, format_options).await
        },
        options,
    )?;
    println!("{outcome}");
    Ok(())
}

async fn produce_outcome(
    df: DataFrame,
    ending: Ending,
    output_format: Format,
    format_options: HashMap<String, String>,
) -> Result<Outcome> {
    match ending {
        Ending::Write(output) => {
            let write_result =
                write(df, &output, output_format.output_factory(), format_options).await?;
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

fn validate_output_extension(output_path: Option<&str>, output_format: Format) -> Result<()> {
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
            output_format.extension()
        )));
    }
    Ok(())
}

fn compression_options(
    compression: Option<&str>,
    output_format: Format,
) -> Result<HashMap<String, String>> {
    let Some(compression) = compression else {
        return Ok(HashMap::new());
    };
    match output_format {
        Format::Parquet => {
            // DataFusion 55's parser assumes anything after `(` ends with `)` and removes the
            // final byte with `&rh[..rh.len() - 1]`. An input such as `gzip(` leaves `rh` empty,
            // so that subtraction panics instead of returning a configuration error.
            if !has_parquet_compression_syntax(compression) {
                return Err(unrecognized_compression(compression, output_format));
            }
            parquet_writer::parse_compression_string(compression)
                .map_err(|_| unrecognized_compression(compression, output_format))?;
        }
        Format::Vortex => {
            return Err(unrecognized_compression(compression, output_format));
        }
    };
    Ok(HashMap::from([(
        "format.compression".to_string(),
        compression.to_string(),
    )]))
}

fn has_parquet_compression_syntax(compression: &str) -> bool {
    let compression = compression.to_ascii_lowercase();
    if ["uncompressed", "snappy", "lz4", "lz4_raw"].contains(&compression.as_str()) {
        return true;
    }
    ["gzip", "brotli", "zstd"].into_iter().any(|codec| {
        compression
            .strip_prefix(codec)
            .and_then(|suffix| suffix.strip_prefix('('))
            .and_then(|level| level.strip_suffix(')'))
            .is_some_and(|level| !level.is_empty() && level.chars().all(|c| c.is_ascii_digit()))
    })
}

fn unrecognized_compression(compression: &str, output_format: Format) -> DataFusionError {
    DataFusionError::Configuration(format!(
        "compression '{compression}' is not recognized for output format '{}'",
        output_format.extension()
    ))
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
    fn maps_parquet_compression_to_a_format_option() {
        for compression in [
            "uncompressed",
            "snappy",
            "gzip(6)",
            "brotli(5)",
            "lz4",
            "zstd(7)",
            "lz4_raw",
        ] {
            assert_eq!(
                compression_options(Some(compression), Format::Parquet).unwrap(),
                std::collections::HashMap::from([(
                    "format.compression".to_string(),
                    compression.to_string(),
                )]),
            );
        }
    }

    #[test]
    fn omits_compression_option_when_the_flag_is_absent() {
        assert!(
            compression_options(None, Format::Parquet)
                .unwrap()
                .is_empty()
        );
        assert!(
            compression_options(None, Format::Vortex)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn rejects_invalid_parquet_compression() {
        let error = compression_options(Some("brotli"), Format::Parquet).unwrap_err();

        assert!(error.to_string().contains("parquet"), "got: {error}");
    }

    #[test]
    fn rejects_malformed_parquet_compression_without_panicking() {
        let error = compression_options(Some("gzip("), Format::Parquet).unwrap_err();

        assert!(error.to_string().contains("parquet"), "got: {error}");
    }

    #[test]
    fn rejects_compression_that_vortex_does_not_recognize() {
        let error = compression_options(Some("zstd(7)"), Format::Vortex).unwrap_err();

        assert!(error.to_string().contains("vortex"), "got: {error}");
    }

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
