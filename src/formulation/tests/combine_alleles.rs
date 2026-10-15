use crate::formulation::combine_alleles;
use crate::locus::LocusRepresentation;
use crate::ordered_frame::OrderedFrame;
use crate::row_ordering::RowOrdering;
use crate::stored::dataset::Dataset;
use crate::testing::fixture::{SAMPLES, block_on};
use crate::tests::{
    formulation_plans::{
        FORMATS, PlanBuilder, REPRESENTATIONS, dataset, mixed_dataset, physical_plan,
    },
    plan_shape::PlanShape,
};

use datafusion::{common::DataFusionError, error::Result, prelude::SessionContext};
use futures::{FutureExt, future::BoxFuture};

/// The allele combiner's plan builder.
#[derive(Clone)]
struct Alleles;

impl PlanBuilder for Alleles {
    fn plan<'a>(
        &'a self,
        ctx: &'a SessionContext,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<OrderedFrame>> {
        combine_alleles::plan(ctx, dataset).boxed()
    }
}

#[test]
fn requires_the_locus_then_alleles_ordering() {
    assert_eq!(
        combine_alleles::required_ordering(),
        RowOrdering::locus_then_alleles()
    );
}

/// Both readers preserve the declared row ordering through the union, so the requested ordering
/// needs a merge over one partition per sample, and the de-duplication and ranking stacked on top
/// don't reintroduce a sort.
#[test]
fn merges_one_partition_per_sample_without_re_sorting() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let plan = physical_plan(&Alleles, &dataset(format, representation));
            PlanShape::of(&plan).assert_merge_tree(&[SAMPLES.len()]);
        }
    }
}

#[test]
fn rejects_a_dataset_with_a_multi_sample_input_table() {
    for format in FORMATS {
        let dataset = mixed_dataset(format, LocusRepresentation::ContigPosition);
        let ctx = SessionContext::new();
        dataset.fixture.register(&ctx);

        let error = block_on(Alleles.plan(&ctx, &dataset.dataset))
            .expect_err("the allele combiner reads only single-sample input tables");

        assert!(matches!(error, DataFusionError::Plan(_)));
        let message = error.to_string();
        assert!(
            message.contains("multi-sample"),
            "unexpected error: {message}"
        );
        assert!(message.contains("'g0'"), "unexpected error: {message}");
    }
}
