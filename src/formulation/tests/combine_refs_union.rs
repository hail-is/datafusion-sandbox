use crate::formulation::combine_refs_union;
use crate::locus::RowOrdering;
use crate::ordered_frame::OrderedFrame;
use crate::stored::dataset::Dataset;
use crate::testing::fixture::SAMPLES;
use crate::tests::{
    formulation_plans::{FORMATS, PlanBuilder, REPRESENTATIONS, dataset, physical_plan},
    plan_shape::PlanShape,
};

use datafusion::{error::Result, prelude::SessionContext};
use futures::{FutureExt, future::BoxFuture};

/// The union formulation's plan builder.
#[derive(Clone)]
struct Union;

impl PlanBuilder for Union {
    fn plan<'a>(
        &'a self,
        ctx: &'a SessionContext,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<OrderedFrame>> {
        combine_refs_union::plan(ctx, dataset).boxed()
    }
}

#[test]
fn requires_the_locus_ordering() {
    assert_eq!(
        combine_refs_union::required_ordering(),
        RowOrdering::locus()
    );
}

/// Both readers preserve the declared row ordering through the union, so the requested ordering
/// needs a merge over one partition per sample but no re-sort.
#[test]
fn merges_one_partition_per_sample_without_re_sorting() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let plan = physical_plan(&Union, &dataset(format, representation));
            PlanShape::of(&plan).assert_merge_tree(&[SAMPLES.len()]);
        }
    }
}
