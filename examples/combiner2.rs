use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::logical_plan::Union;
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
// Reads each as a separate table, then unions.
//
// The generated physical plan still has a `SortPreservingMergeExec` doing the main work. The difference from combiner1
// is only that now many `DataSourceExec`s feed into a `UnionExec`, which still feeds `SortPreservingMergeExec` with one
// partition per input sample. Seems to have about the same performance.
#[tokio::main(flavor = "current_thread")] // for timing single-threaded performance
// #[tokio::main]
async fn main() -> Result<()> {
    let samples = &[
        "HG00308", "HG00592", "HG02230", "NA18534", "NA20760", "NA18530", "HG03805", "HG02223",
        "HG00637", "NA12249", "HG02224", "NA21099", "NA11830", "HG01378", "HG00187", "HG01356",
        "HG02188", "NA20769", "HG00190", "NA18618", "NA18507", "HG03363", "NA21123", "HG03088",
        "NA21122", "HG00373", "HG01058", "HG00524", "NA18969", "HG03833", "HG04158", "HG03578",
        "HG00339", "HG00313", "NA20317", "HG00553", "HG01357", "NA19747", "NA18609", "HG01377",
        "NA19456", "HG00590", "HG01383", "HG00320", "HG04001", "NA20796", "HG00323", "HG01384",
        "NA18613", "NA20802",
    ];

    // Forces one partition per input scan. There will still be one partition per input going into the `SortPreservingMergeExec`.
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    let contig = col("contig");
    let position = col("position");
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
    ctx.register_listing_table("ref", "data/vortices_chr22/", vortex_opts, None, None)
        .await?;
    let df = ctx.table("ref").await?;
    let lps = samples
        .iter()
        .map(|s| {
            Ok(Arc::new(
                df.clone()
                    .filter(col("s").eq(lit(*s)))?
                    .into_unoptimized_plan(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let lp = LogicalPlan::Union(Union::try_new(lps)?);
    let df = DataFrame::new(ctx.state(), lp);
    let df = df.sort_by(vec![contig, position])?;
    // df.limit(50, Some(100))?.show().await?;
    // df.explain(true, false)?.show().await?;
    write_vortex(df, "data/combined.vortex", None).await?;
    Ok(())
}
