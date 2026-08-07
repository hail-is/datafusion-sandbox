use datafusion_sandbox::{VortexReadOptions, read_vortex, write_vortex};

use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::prelude::*;

// Combines all vortex files in a directory. Assumes each file is a single sample, with the sample id
// provided by a parent directory of the form "s=HG123456". Assumes all files have the same schema.
// Reads them as a single table, with one partition per input file. By declaring the input files
// to be sorted, this generates a physical plan with a single `SortPreservingMergeExec`.
#[tokio::main(flavor = "current_thread")] // for timing single-threaded performance
// #[tokio::main()]
async fn main() -> Result<()> {
    // Need at least as many partitions as files to avoid sorting. Merging is done via SortPreservingMergeExec,
    // which merges many partitions into one. If there are more files than partitions, DataSourceExec will
    // need to group (fragments of) multiple files into one partition, which will then no longer be ordered.
    let mut config = SessionConfig::new().with_target_partitions(50);
    config.options_mut().optimizer.preserve_file_partitions = 1;
    let ctx = SessionContext::new_with_config(config);

    let read_opts = VortexReadOptions {
        file_sort_order: vec![vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ]],
        schema: None,
        table_partition_cols: vec![
            ("contig".to_string(), DataType::Utf8),
            ("s".to_string(), DataType::Utf8),
        ],
    };
    let df = read_vortex(&ctx, "data/vortices_chr22/", read_opts).await?;

    let df = df.sort_by(vec![col("contig"), col("position")])?;
    // df.limit(50, Some(100))?.show().await?;
    // df.explain(false, false)?.show().await?;
    write_vortex(df, "data/combined.vortex", None).await?;
    Ok(())
}
