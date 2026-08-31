use super::union_sample_plans;
use crate::dataset::Dataset;

use datafusion::{error::Result, prelude::*};

use std::sync::Arc;

/// Builds the union-of-per-sample-scans formulation.
///
/// Many `DataSourceExec`s feed a `UnionExec`, which feeds a
/// `SortPreservingMergeExec` with one partition per input sample.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let locus_ordering = dataset.locus_representation().ordering();
    let mut plans = Vec::with_capacity(dataset.sample_set().len());
    for sample in dataset.sample_set() {
        let df = dataset.read_sample(ctx, sample).await?;
        plans.push(Arc::new(df.into_unoptimized_plan()));
    }
    let df = DataFrame::new(ctx.state(), union_sample_plans(plans)?);
    df.sort(locus_ordering)
}
