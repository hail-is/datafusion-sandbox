use datafusion::functions_window::rank::rank;
use datafusion_sandbox::{VortexReadOptions, read_vortex_with_schema, write_vortex};

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::logical_plan::Union;
use datafusion::prelude::*;

use std::sync::Arc;

use vortex::VortexSessionDefault;
use vortex::session::VortexSession;

use vortex_datafusion::VortexFormat;

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

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    // Note: leaving the schema to be inferred infers Utf8View for "alleles", which runs into what
    // I suspect is a bug, the effect of which is the query planner doesn't think the input file groups
    // are sorted.
    let read_opts = VortexReadOptions {
        file_sort_order: vec![vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
            col("alleles").sort(true, false),
        ]],
        schema: Some(Arc::new(Schema::new(vec![
            Field::new("position", DataType::Int32, false),
            Field::new("alleles", DataType::Utf8, false),
        ]))),
        table_partition_cols: vec![("contig".to_string(), DataType::Utf8)],
    };

    let lps = samples
        .iter()
        .map(|s| {
            let df = read_vortex_with_schema(
                &ctx,
                format!("data/vortices_alleles_chr22/s={}", s),
                read_opts.clone(),
            )?;
            Ok(Arc::new(df.into_unoptimized_plan()))
        })
        .collect::<Result<Vec<_>>>()?;
    let lp = LogicalPlan::Union(Union::try_new(lps)?);
    let df = DataFrame::new(ctx.state(), lp);
    let df = df.sort_by(vec![col("contig"), col("position"), col("alleles")])?;
    let df = df.distinct()?;
    let df = df.window(vec![
        rank()
            .order_by(vec![
                col("contig").sort(true, false),
                col("position").sort(true, false),
                col("alleles").sort(true, false),
            ])
            .partition_by(vec![col("contig"), col("position")])
            .build()?,
    ])?;
    // df.limit(0, Some(100))?.show().await?;
    // df.explain(false, false)?.show().await?;
    write_vortex(df, "data/combined_alleles.vortex", None).await?;
    Ok(())
}
