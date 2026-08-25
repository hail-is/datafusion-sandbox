use crate::{Dataset, derived_session, read, union_sample_plans};

use datafusion::{
    arrow::datatypes::DataType, datasource::listing::ListingOptions, error::Result,
    logical_expr::SortExpr, prelude::*,
};

use std::sync::Arc;

/// The locus ordering declared on the input files and requested in the query: contig, then
/// position. Declaring both identically is what keeps the plan mergeable rather than re-sorting.
fn locus_ordering() -> Vec<SortExpr> {
    vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
    ]
}

/// Builds the union-of-per-sample-scans formulation under the session its plan
/// shape depends on.
///
/// The generated physical plan still has a `SortPreservingMergeExec` doing the main work. The
/// difference from the one-scan formulation is that many `DataSourceExec`s feed
/// a `UnionExec`, which still feeds `SortPreservingMergeExec` with one partition
/// per input sample.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let ctx = derived_session(ctx, |options| {
        options.execution.target_partitions = 1;
    });
    let listing_options = ListingOptions::new(dataset.input_format().read_format())
        .with_file_sort_order(vec![locus_ordering()])
        .with_table_partition_cols(vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ]);
    let df = read(&ctx, dataset.table_path().as_str(), listing_options, None).await?;

    let plans = dataset
        .sample_set()
        .iter()
        .map(|sample| {
            Ok(Arc::new(
                df.clone()
                    .filter(col("s").eq(lit(sample.as_str())))?
                    .into_unoptimized_plan(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let df = DataFrame::new(ctx.state(), union_sample_plans(plans)?);
    df.sort(locus_ordering())
}
