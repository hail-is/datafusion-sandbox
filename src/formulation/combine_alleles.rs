use super::union_sample_plans;
use crate::dataset::{Dataset, DatasetLayout};

use datafusion::{error::Result, functions_window::rank::rank, logical_expr::SortExpr, prelude::*};

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

pub(crate) fn required_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: locus_ordering(),
        schema: None,
    }
}

/// Builds the union-of-per-sample-scans formulation. Produces the distinct set
/// of alleles at each locus, ranked within the locus.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let mut plans = Vec::with_capacity(dataset.sample_set().len());
    for sample in dataset.sample_set() {
        let df = dataset.read_sample(ctx, sample).await?;
        plans.push(Arc::new(df.into_unoptimized_plan()));
    }
    let df = DataFrame::new(ctx.state(), union_sample_plans(plans)?);
    let df = df.select_columns(&["position", "alleles", "contig"])?;
    let df = df.distinct()?;
    let df = df.window(vec![
        rank()
            .order_by(locus_ordering())
            .partition_by(vec![col("contig"), col("position")])
            .build()?,
    ])?;
    df.sort(locus_ordering())
}
