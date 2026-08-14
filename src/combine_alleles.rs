use crate::{VortexReadOptions, read_vortex};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    error::Result,
    functions_window::rank::rank,
    logical_expr::{LogicalPlan, SortExpr, logical_plan::Union},
    prelude::*,
};

use std::sync::Arc;

/// The locus ordering declared on the input files and requested in the query: contig, position,
/// then alleles. Declaring all three identically is what keeps the plan mergeable rather than
/// re-sorting.
fn locus_ordering() -> Vec<SortExpr> {
    vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
        col("alleles").sort(true, false),
    ]
}

/// Builds the plan combining the alleles of all `samples` under `table_path`, which is expected
/// to contain one directory per sample, of the form "s=HG123456". Produces the distinct set of
/// alleles at each locus, ranked within the locus.
pub async fn plan(ctx: &SessionContext, table_path: &str, samples: &[&str]) -> Result<DataFrame> {
    let table_path = table_path.trim_end_matches('/');

    // Note: leaving the schema to be inferred infers Utf8View for "alleles", which runs into what
    // I suspect is a bug, the effect of which is the query planner doesn't think the input file groups
    // are sorted.
    let read_opts = VortexReadOptions {
        file_sort_order: vec![locus_ordering()],
        schema: Some(Arc::new(Schema::new(vec![
            Field::new("position", DataType::Int32, false),
            Field::new("alleles", DataType::Utf8, false),
        ]))),
        table_partition_cols: vec![("contig".to_string(), DataType::Utf8)],
    };

    let mut lps = Vec::with_capacity(samples.len());
    for s in samples {
        let df = read_vortex(ctx, format!("{table_path}/s={s}/"), read_opts.clone()).await?;
        lps.push(Arc::new(df.into_unoptimized_plan()));
    }
    let lp = LogicalPlan::Union(Union::try_new(lps)?);
    let df = DataFrame::new(ctx.state(), lp);
    let df = df.sort(locus_ordering())?;
    let df = df.distinct()?;
    df.window(vec![
        rank()
            .order_by(locus_ordering())
            .partition_by(vec![col("contig"), col("position")])
            .build()?,
    ])
}
