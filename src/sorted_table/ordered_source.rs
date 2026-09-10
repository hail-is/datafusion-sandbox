//! Keeps the recovered ordering through data source rewrites. See ADR 0012.

use datafusion::{
    common::{Result, Statistics, config::ConfigOptions, tree_node::TreeNodeRecursion},
    datasource::{
        physical_plan::FileScanConfig,
        source::{DataSource, OpenArgs},
    },
    execution::TaskContext,
    logical_expr::Operator,
    physical_expr::{
        EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
        expressions::BinaryExpr, projection::ProjectionExprs, split_conjunction,
        utils::reassign_expr_columns,
    },
    physical_plan::{
        DisplayFormatType, Partitioning, SendableRecordBatchStream, SortOrderPushdownResult,
        execution_plan::SchedulingType, filter_pushdown::FilterPushdownPropagation,
        metrics::ExecutionPlanMetricsSet,
    },
};
use std::{any::Any, fmt, sync::Arc};

#[derive(Clone, Debug)]
pub(super) struct OrderedSource {
    inner: Arc<dyn DataSource>,
    declared: EquivalenceProperties,
}

impl OrderedSource {
    pub(super) fn new(config: FileScanConfig) -> Result<Self> {
        let schema = config.file_source().table_schema().table_schema();
        let mut declared = EquivalenceProperties::new_with_orderings(
            Arc::clone(schema),
            config.output_ordering.clone(),
        )
        .with_constraints(config.constraints.clone());
        // The format may have installed a filter before returning the config.
        // Its equalities must participate before any ordering column is projected out.
        if let Some(filter) = config.file_source().filter() {
            for predicate in split_conjunction(&filter) {
                let predicate = reassign_expr_columns(Arc::clone(predicate), schema)?;
                if let Some(binary) = predicate.downcast_ref::<BinaryExpr>()
                    && binary.op() == &Operator::Eq
                {
                    declared.add_equal_conditions(
                        Arc::clone(binary.left()),
                        Arc::clone(binary.right()),
                    )?;
                }
            }
        }
        if let Some(projection) = config.file_source().projection() {
            declared = declared.project(
                &projection.projection_mapping(schema)?,
                Arc::new(projection.project_schema(schema)?),
            );
        }
        Ok(Self {
            inner: Arc::new(config),
            declared,
        })
    }

    fn rewrap(&self, inner: Arc<dyn DataSource>) -> Arc<dyn DataSource> {
        Arc::new(Self {
            inner,
            declared: self.declared.clone(),
        })
    }
}

impl DataSource for OrderedSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.open(partition, context)
    }

    fn open_with_args(&self, args: OpenArgs) -> Result<SendableRecordBatchStream> {
        self.inner.open_with_args(args)
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        self.inner
            .repartitioned(
                target_partitions,
                repartition_file_min_size,
                output_ordering,
            )
            .map(|source| source.map(|source| self.rewrap(source)))
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        // Keep the format's filter equivalences and constraints, but bypass the
        // composed-row ordering proof that cannot establish general file cuts.
        let mut properties = self.inner.eq_properties();
        properties.add_orderings(self.declared.oeq_class().iter().cloned());
        properties
    }

    fn scheduling_type(&self) -> SchedulingType {
        self.inner.scheduling_type()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        self.inner
            .with_fetch(limit)
            .map(|source| self.rewrap(source))
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        self.inner
            .try_swapping_with_projection(projection)?
            .map(|inner| {
                // Filters can make an omitted ordering column constant or equivalent
                // to a projected column. Preserve that evidence before projection.
                let properties = self.eq_properties();
                let schema = properties.schema();
                let declared = properties.project(
                    &projection.projection_mapping(schema)?,
                    Arc::new(projection.project_schema(schema)?),
                );
                let source: Arc<dyn DataSource> = Arc::new(Self { inner, declared });
                Ok(source)
            })
            .transpose()
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        let result = self.inner.try_pushdown_filters(filters, config)?;
        Ok(FilterPushdownPropagation {
            filters: result.filters,
            updated_node: result.updated_node.map(|source| self.rewrap(source)),
        })
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn DataSource>>> {
        if self
            .eq_properties()
            .ordering_satisfy(order.iter().cloned())?
        {
            return Ok(SortOrderPushdownResult::Exact {
                inner: Arc::new(self.clone()),
            });
        }
        // A different requested order can reorder files or row groups. It must
        // not inherit our original declaration.
        self.inner.try_pushdown_sort(order)
    }

    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn DataSource>> {
        self.inner
            .with_preserve_order(preserve_order)
            .map(|source| self.rewrap(source))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn DataSource>> {
        self.inner
            .with_new_state(state)
            .map(|source| self.rewrap(source))
    }

    fn create_sibling_state(&self, config: &ConfigOptions) -> Option<Arc<dyn Any + Send + Sync>> {
        self.inner.create_sibling_state(config)
    }
}
