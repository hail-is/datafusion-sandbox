use super::union_sample_plans;
use crate::dataset::{Dataset, DatasetLayout};
use crate::locus::LocusOrdering;

use datafusion::{error::Result, functions_window::rank::rank, prelude::*};

use std::sync::Arc;

pub(crate) fn required_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: LocusOrdering::locus_then_alleles(),
    }
}

/// Builds the union-of-per-sample-scans formulation. Produces the distinct set
/// of alleles at each locus, ranked within the locus.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let representation = dataset.locus_representation();
    let query_ordering = dataset.query_ordering(&LocusOrdering::locus_then_alleles())?;
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
            .order_by(query_ordering.clone())
            .partition_by(representation.window_partition())
            .build()?,
    ])?;
    df.sort(query_ordering)
}
