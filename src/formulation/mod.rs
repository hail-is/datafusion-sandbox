//! The supported combiner formulations and their shared plan-building helpers.

mod combine_alleles;
mod combine_refs_union;

use crate::dataset::{Dataset, DatasetLayout};

use datafusion::{
    arrow::datatypes::DataType,
    common::config::ConfigOptions,
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
    pub fn required_layout(self) -> DatasetLayout {
        match self {
            Self::CombineAllelesUnion => combine_alleles::required_layout(),
            Self::CombineRefsUnion => reference_layout(),
        }
    }

    /// Builds this formulation's plan over `dataset`.
    pub async fn plan(self, ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
        let required_layout = self.required_layout();
        dataset.check_ordering(&required_layout.locus_ordering)?;
        match self {
            Self::CombineAllelesUnion => combine_alleles::plan(ctx, dataset).await,
            Self::CombineRefsUnion => combine_refs_union::plan(ctx, dataset).await,
        }
    }
}

fn reference_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ],
        partition_columns: vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ],
        schema: None,
    }
}

/// Derives a session from `ctx` with `overrides` applied to its config.
///
/// [`SessionContext::state`] hands back an owned clone that shares the caller's
/// `Arc<RuntimeEnv>` and catalog list, so registered object stores and the
/// file-statistics cache carry over. The caller's `SessionConfig` still holds a
/// reference to the same `Arc<ConfigOptions>`, so `options_mut` copies rather
/// than mutating in place and the overrides cannot reach the caller.
fn derived_session(
    ctx: &SessionContext,
    overrides: impl FnOnce(&mut ConfigOptions),
) -> SessionContext {
    let mut state = ctx.state();
    overrides(state.config_mut().options_mut());
    SessionContext::new_with_state(state)
}

fn union_sample_plans(mut plans: Vec<Arc<LogicalPlan>>) -> Result<LogicalPlan> {
    if plans.len() == 1 {
        Ok((*plans.pop().expect("a dataset has at least one sample")).clone())
    } else {
        Ok(LogicalPlan::Union(Union::try_new(plans)?))
    }
}
