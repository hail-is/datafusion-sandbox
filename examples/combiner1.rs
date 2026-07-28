use datafusion_sandbox::write_vortex;

use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;

use std::sync::Arc;

use vortex::VortexSessionDefault;
use vortex::session::VortexSession;

use vortex_datafusion::VortexFormat;

// Combines all vortex files in a directory. Assumes each file is a single sample, with the sample id
// provided by a parent directory of the form "s=HG123456". Assumes all files have the same schema.
// Reads them as a single table, with one partition per input file. By declaring the input files
// to be sorted, this generates a physical plan with a single `SortPreservingMergeExec`.
#[tokio::main]
async fn main() -> Result<()> {
    // Need at least as many partitions as files to avoid sorting. Merging is done via SortPreservingMergeExec,
    // which merges many partitions into one. If there are more files than partitions, DataSourceExec will
    // need to group (fragments of) multiple files into one partition, which will then no longer be ordered.
    let mut config = SessionConfig::new().with_target_partitions(50);
    config.options_mut().optimizer.preserve_file_partitions = 1;
    let ctx = SessionContext::new_with_config(config);
    let contig = col(Column::from_name("contig"));
    let position = col(Column::from_name("position"));
    let vortex_session = VortexSession::default();
    let format = Arc::new(VortexFormat::new(vortex_session));
    let vortex_opts = ListingOptions::new(format)
        .with_file_extension(".vortex")
        .with_file_sort_order(vec![vec![
            contig.clone().sort(true, false),
            position.clone().sort(true, false),
        ]])
        .with_table_partition_cols(vec![
            ("contig".to_string(), DataType::Utf8),
            ("s".to_string(), DataType::Utf8),
        ])
        .with_session_config_options(ctx.state().config());
    let _parquet_opts = ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_file_sort_order(vec![vec![position.clone().sort(true, false)]])
        .with_table_partition_cols(vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ])
        .with_session_config_options(ctx.state().config());
    ctx.register_listing_table("ref", "data/vortices_chr22/", vortex_opts, None, None)
        .await?;
    let df = ctx.table("ref").await?;
    let df = df.sort_by(vec![contig, position])?;
    // df.limit(50, Some(100))?.show().await?;
    // df.explain(false, false)?.show().await?;
    write_vortex(df, "data/combined.vortex", None).await?;
    Ok(())
}
