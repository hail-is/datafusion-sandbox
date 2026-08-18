use clap::{Parser, Subcommand};
use datafusion::error::Result;

use datafusion_sandbox::pipeline::{self, PipelineOptions};
use datafusion_sandbox::{
    SAMPLES, combine_alleles, combine_refs, combiner_session_config, write, write_count,
};
use std::sync::Arc;
use vortex_datafusion::VortexFormatFactory;

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
    CombineRefs {
        path: String,
        #[arg(long, short, default_value = "data/combined.vortex")]
        output: String,
    },
    /// Combine the alleles of all samples under PATH.
    CombineAlleles {
        path: String,
        #[arg(long, short, default_value = "data/combined_alleles.vortex")]
        output: String,
    },
}

fn main() -> Result<()> {
    let Cli { command, threads } = Cli::parse();

    let write_result = match command {
        Command::CombineRefs { path, output } => {
            let options = options_for(&path, &output, threads);
            pipeline::run(
                move |ctx| async move {
                    let df = combine_refs::plan(&ctx, &path, SAMPLES).await?;
                    write(df, &output, Arc::new(VortexFormatFactory::new())).await
                },
                options,
            )?
        }
        Command::CombineAlleles { path, output } => {
            let options = options_for(&path, &output, threads);
            pipeline::run(
                move |ctx| async move {
                    let df = combine_alleles::plan(&ctx, &path, SAMPLES).await?;
                    write(df, &output, Arc::new(VortexFormatFactory::new())).await
                },
                options,
            )?
        }
    };
    println!("{}", write_count(&write_result)?);
    Ok(())
}

/// Pipeline options for a combiner reading from `input_path` and writing to `output_path`: the
/// object stores to register are the ones those paths live on, and local paths need none at all.
fn options_for(input_path: &str, output_path: &str, threads: Option<usize>) -> PipelineOptions {
    let mut options = PipelineOptions {
        session_config: combiner_session_config(),
        ..Default::default()
    };
    if let Some(threads) = threads {
        options.threads = threads;
    }
    options.object_stores = [input_path, output_path]
        .into_iter()
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
        let options = options_for("gs://bucket-a/path", "gs://bucket-b/out.vortex", None);
        assert_eq!(options.object_stores, ["gs://bucket-a", "gs://bucket-b"]);
    }

    #[test]
    fn registers_a_shared_object_store_once() {
        let options = options_for("gs://bucket/path/", "gs://bucket/out.vortex", None);
        assert_eq!(options.object_stores, ["gs://bucket"]);
    }

    #[test]
    fn registers_no_object_stores_for_local_paths() {
        let options = options_for("data/samples", "data/out.vortex", None);
        assert!(options.object_stores.is_empty());
    }
}
