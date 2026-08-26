use super::{derived_session, reference_layout, union_sample_plans};
use crate::dataset::Dataset;

use datafusion::{error::Result, prelude::*};

use std::sync::Arc;

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
    let locus_ordering = reference_layout().locus_ordering;
    let mut plans = Vec::with_capacity(dataset.sample_set().len());
    for sample in dataset.sample_set() {
        let df = dataset.read_sample(&ctx, sample).await?;
        plans.push(Arc::new(df.into_unoptimized_plan()));
    }
    let df = DataFrame::new(ctx.state(), union_sample_plans(plans)?);
    df.sort(locus_ordering)
}
