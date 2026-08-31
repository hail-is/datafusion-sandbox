//! The supported combiner formulations and their shared plan-building helpers.

mod combine_alleles;
mod combine_refs_union;

use crate::dataset::{Dataset, DatasetLayout};
use crate::locus::LocusRepresentation;

use datafusion::{
    error::Result,
    logical_expr::{LogicalPlan, logical_plan::Union},
    prelude::*,
};
use std::{fmt, sync::Arc};

/// A supported way to build one of the combiners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Formulation {
    CombineAllelesUnion,
    CombineRefsUnion,
}

impl fmt::Display for Formulation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CombineAllelesUnion | Self::CombineRefsUnion => formatter.write_str("union"),
        }
    }
}

impl Formulation {
    pub fn required_layout(self, representation: LocusRepresentation) -> DatasetLayout {
        match self {
            Self::CombineAllelesUnion => combine_alleles::required_layout(representation),
            Self::CombineRefsUnion => reference_layout(representation),
        }
    }

    /// Builds this formulation's plan over `dataset`.
    pub async fn plan(self, ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
        let required_layout = self.required_layout(dataset.locus_representation());
        dataset.check_ordering(&required_layout.locus_ordering)?;
        match self {
            Self::CombineAllelesUnion => combine_alleles::plan(ctx, dataset).await,
            Self::CombineRefsUnion => combine_refs_union::plan(ctx, dataset).await,
        }
    }
}

fn reference_layout(representation: LocusRepresentation) -> DatasetLayout {
    DatasetLayout {
        locus_ordering: representation.ordering(),
    }
}

fn union_sample_plans(mut plans: Vec<Arc<LogicalPlan>>) -> Result<LogicalPlan> {
    if plans.len() == 1 {
        Ok((*plans.pop().expect("a dataset has at least one sample")).clone())
    } else {
        Ok(LogicalPlan::Union(Union::try_new(plans)?))
    }
}
