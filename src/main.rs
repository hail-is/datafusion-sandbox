use clap::{Parser, Subcommand};
use datafusion::error::Result;

use datafusion_sandbox::{combine_alleles, combine_refs};

use std::thread::available_parallelism;

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
    let cli = Cli::parse();
    let threads = match cli.threads {
        Some(n) => n,
        None => available_parallelism().map(|n| n.get()).unwrap_or(1),
    };

    // The runtime built here is the IO runtime: `combine_refs` moves the query itself onto a
    // separate `CpuRuntime`, so that IO and CPU-bound work don't contend for the same threads.
    let mut builder = if threads == 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(threads);
        builder
    };
    let runtime = builder.enable_all().build()?;

    runtime.block_on(async {
        match cli.command {
            Command::CombineRefs { path, output } => {
                combine_refs::run(&path, &output, threads).await
            }
            Command::CombineAlleles { path, output } => {
                combine_alleles::run(&path, &output, threads).await
            }
        }
    })
}
