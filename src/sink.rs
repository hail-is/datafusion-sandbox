//! Sinks: the operator every action runs its frame into, and the ordering it requires.
//!
//! Every action ends in a `DataSinkExec` whose required input ordering is the formulation's
//! ordering. The requirement is what keeps a merge per sample group beneath the final merge; a
//! logical sort in the same place destroys it. The file sink is the format's; the two sinks here
//! stand in for it when the action collects rows or only analyzes the plan. See
//! [ADR 0014](../docs/adr/0014-hold-the-merge-tree-with-the-sinks-ordering-requirement.md).

use crate::locus::StoredOrdering;

use datafusion::{
    arrow::{compute::SortOptions, datatypes::SchemaRef, record_batch::RecordBatch},
    catalog::{Session, TableProvider},
    common::DFSchema,
    datasource::{
        DefaultTableSource,
        sink::{DataSink, DataSinkExec},
    },
    error::{DataFusionError, Result},
    execution::TaskContext,
    logical_expr::{LogicalPlanBuilder, SortExpr, TableType, dml::InsertOp},
    physical_expr::{LexRequirement, PhysicalSortExpr},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream},
    prelude::{DataFrame, Expr},
};
use futures_util::TryStreamExt;

use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
};

/// What an insert plans to once its input and the ordering requirement are known.
#[async_trait::async_trait]
pub trait SinkTarget: fmt::Debug + Send + Sync {
    /// The execution plan that writes `input` into this target, requiring `ordering` of it.
    ///
    /// # Errors
    ///
    /// Returns an error if the target's plan cannot be built.
    async fn plan(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// The frame that runs `df` into `target` when executed, requiring `ordering` of the rows.
///
/// `None` places no requirement. The planner reaches the target through the frame's table
/// source; nothing is registered on the session.
///
/// # Errors
///
/// Returns an error if the insert plan cannot be built.
pub fn run_into(
    df: DataFrame,
    name: &str,
    ordering: Option<&StoredOrdering>,
    target: Arc<dyn SinkTarget>,
) -> Result<DataFrame> {
    let provider = Arc::new(SinkProvider {
        schema: Arc::clone(df.schema().inner()),
        ordering: ordering.map(StoredOrdering::sort_expressions),
        target,
    });
    let (state, plan) = df.into_parts();
    let plan = LogicalPlanBuilder::insert_into(
        plan,
        name,
        Arc::new(DefaultTableSource::new(provider)),
        InsertOp::Append,
    )?
    .build()?;
    Ok(DataFrame::new(state, plan))
}

/// The frame that runs `df` into a sink keeping every batch it receives, in arrival order.
///
/// Executing the frame yields the row count; the batches are taken from the returned sink
/// afterwards.
///
/// # Errors
///
/// Returns an error if the sink plan cannot be built.
pub fn collect(
    df: DataFrame,
    ordering: &StoredOrdering,
) -> Result<(DataFrame, Arc<CollectingSink>)> {
    let sink = Arc::new(CollectingSink {
        schema: Arc::clone(df.schema().inner()),
        batches: Mutex::new(Vec::new()),
    });
    let target = Arc::new(DataSinkTarget(sink.clone()));
    let frame = run_into(df, "collect", Some(ordering), target)?;
    Ok((frame, sink))
}

/// The frame that runs `df` into a sink counting its rows and dropping them.
///
/// # Errors
///
/// Returns an error if the sink plan cannot be built.
pub fn drain(df: DataFrame, ordering: &StoredOrdering) -> Result<DataFrame> {
    let sink = Arc::new(DrainingSink {
        schema: Arc::clone(df.schema().inner()),
    });
    run_into(df, "drain", Some(ordering), Arc::new(DataSinkTarget(sink)))
}

/// A sink that keeps the batches written to it.
#[derive(Debug)]
pub struct CollectingSink {
    schema: SchemaRef,
    batches: Mutex<Vec<RecordBatch>>,
}

impl CollectingSink {
    /// Takes the batches written so far, leaving the sink empty.
    pub fn take(&self) -> Vec<RecordBatch> {
        std::mem::take(&mut *self.batches.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl DisplayAs for CollectingSink {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CollectingSink")
    }
}

#[async_trait::async_trait]
impl DataSink for CollectingSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _: &Arc<TaskContext>,
    ) -> Result<u64> {
        count_rows(data, |batch| {
            self.batches
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(batch);
        })
        .await
    }
}

/// A sink that counts the rows written to it and keeps nothing.
#[derive(Debug)]
pub struct DrainingSink {
    schema: SchemaRef,
}

impl DisplayAs for DrainingSink {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DrainingSink")
    }
}

#[async_trait::async_trait]
impl DataSink for DrainingSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _: &Arc<TaskContext>,
    ) -> Result<u64> {
        count_rows(data, drop).await
    }
}

/// Consumes `data`, handing each batch to `on_batch`, and returns the number of rows seen.
async fn count_rows(
    mut data: SendableRecordBatchStream,
    mut on_batch: impl FnMut(RecordBatch) + Send,
) -> Result<u64> {
    let mut rows = 0_u64;
    while let Some(batch) = data.try_next().await? {
        rows = rows.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        on_batch(batch);
    }
    Ok(rows)
}

/// A target that is a `DataSink` behind `DataFusion`'s own `DataSinkExec`.
#[derive(Debug)]
struct DataSinkTarget(Arc<dyn DataSink>);

#[async_trait::async_trait]
impl SinkTarget for DataSinkTarget {
    async fn plan(
        &self,
        _: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        ordering: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::clone(&self.0),
            ordering,
        )))
    }
}

/// The table an insert writes to: a target and the ordering its input must arrive in.
#[derive(Debug)]
struct SinkProvider {
    schema: SchemaRef,
    ordering: Option<Vec<SortExpr>>,
    target: Arc<dyn SinkTarget>,
}

#[async_trait::async_trait]
impl TableProvider for SinkProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        _: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "a sink only accepts rows; it cannot be scanned".to_string(),
        ))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let requirement = match &self.ordering {
            Some(ordering) => {
                let input_schema = DFSchema::try_from(input.schema())?;
                let sort_exprs = ordering
                    .iter()
                    .map(|sort| {
                        let expr = state.create_physical_expr(sort.expr.clone(), &input_schema)?;
                        Ok(PhysicalSortExpr::new(
                            expr,
                            SortOptions::new(!sort.asc, sort.nulls_first),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                LexRequirement::new(sort_exprs.into_iter().map(Into::into))
            }
            None => None,
        };
        self.target.plan(state, input, requirement).await
    }
}
