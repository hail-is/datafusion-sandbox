use super::union_sample_plans;
use crate::dataset::{Dataset, DatasetLayout};
use crate::locus::LocusRepresentation;

use datafusion::{error::Result, functions_window::rank::rank, logical_expr::SortExpr, prelude::*};

use std::sync::Arc;

/// The locus ordering declared on the input files and requested in the query: contig, position,
/// then alleles. Declaring all three identically is what keeps the plan mergeable rather than
/// re-sorting.
fn locus_ordering(representation: LocusRepresentation) -> Vec<SortExpr> {
    let mut ordering = representation.ordering();
    ordering.push(col("alleles").sort(true, false));
    ordering
}

pub(crate) fn required_layout(representation: LocusRepresentation) -> DatasetLayout {
    DatasetLayout {
        locus_ordering: locus_ordering(representation),
    }
}

/// Builds the union-of-per-sample-scans formulation. Produces the distinct set
/// of alleles at each locus, ranked within the locus.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let representation = dataset.locus_representation();
    let mut plans = Vec::with_capacity(dataset.sample_set().len());
    for sample in dataset.sample_set() {
        let df = dataset.read_sample(ctx, sample).await?;
        plans.push(Arc::new(df.into_unoptimized_plan()));
    }
    let df = DataFrame::new(ctx.state(), union_sample_plans(plans)?);
    let mut projection = representation.projection_columns().to_vec();
    projection.push("alleles");
    let df = df.select_columns(&projection)?;
    let df = df.distinct()?;
    let df = df.window(vec![
        rank()
            .order_by(locus_ordering(representation))
            .partition_by(representation.window_partition())
            .build()?,
    ])?;
    df.sort(locus_ordering(representation))
}
