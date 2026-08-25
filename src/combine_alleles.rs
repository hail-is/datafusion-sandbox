use crate::{Dataset, derived_session, read, union_sample_plans};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    datasource::listing::ListingOptions,
    error::Result,
    functions_window::rank::rank,
    logical_expr::SortExpr,
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

/// Builds the union-of-per-sample-scans formulation under the session its plan
/// shape depends on. Produces the distinct set of alleles at each locus,
/// ranked within the locus.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let ctx = derived_session(ctx, |options| {
        options.execution.target_partitions = 1;
    });
    let listing_options = ListingOptions::new(dataset.input_format().read_format())
        .with_file_sort_order(vec![locus_ordering()])
        .with_table_partition_cols(vec![("contig".to_string(), DataType::Utf8)]);

    // Note: leaving the schema to be inferred infers Utf8View for "alleles", which runs into what
    // I suspect is a bug, the effect of which is the query planner doesn't think the input file groups
    // are sorted.
    let schema = Arc::new(Schema::new(vec![
        Field::new("position", DataType::Int32, false),
        Field::new("alleles", DataType::Utf8, false),
    ]));

    let mut plans = Vec::with_capacity(dataset.sample_set().len());
    for sample in dataset.sample_set() {
        let sample_path = dataset.sample_path(sample)?;
        let df = read(
            &ctx,
            sample_path.as_str(),
            listing_options.clone(),
            Some(Arc::clone(&schema)),
        )
        .await?;
        plans.push(Arc::new(df.into_unoptimized_plan()));
    }
    let df = DataFrame::new(ctx.state(), union_sample_plans(plans)?);
    let df = df.sort(locus_ordering())?;
    let df = df.distinct()?;
    df.window(vec![
        rank()
            .order_by(locus_ordering())
            .partition_by(vec![col("contig"), col("position")])
            .build()?,
    ])
}
