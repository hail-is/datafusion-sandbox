//! The one-shared-scan reference combiner formulation preserves one input
//! partition per file for a sort-preserving merge.

use super::{derived_session, reference_layout};
use crate::dataset::Dataset;

use datafusion::{error::Result, prelude::*};

/// Builds the one-shared-scan formulation under the session its plan shape
/// depends on.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let sample_count = dataset.sample_set().len();
    let ctx = derived_session(ctx, |options| {
        options.execution.target_partitions = sample_count;
        options.optimizer.preserve_file_partitions = 1;
    });
    let locus_ordering = reference_layout().locus_ordering;
    dataset.read(&ctx).await?.sort(locus_ordering)
}
