//! The supported combiner formulations and their shared plan-building helpers.

mod combine_alleles;
mod combine_refs_union;

use crate::dataset::{Dataset, DatasetLayout};

use datafusion::{error::Result, prelude::*};
use std::fmt;

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
    pub fn required_layout(self) -> DatasetLayout {
        let locus_ordering = match self {
            Self::CombineAllelesUnion => combine_alleles::required_ordering(),
            Self::CombineRefsUnion => combine_refs_union::required_ordering(),
        };
        DatasetLayout { locus_ordering }
    }

    /// Builds this formulation's plan over `dataset`.
    pub async fn plan(self, ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
        match self {
            Self::CombineAllelesUnion => combine_alleles::plan(ctx, dataset).await,
            Self::CombineRefsUnion => combine_refs_union::plan(ctx, dataset).await,
        }
    }
}
