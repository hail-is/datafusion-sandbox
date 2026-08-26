use datafusion::{
    arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::streaming::StreamingTable,
    common::{DataFusionError, config::ConfigOptions},
    datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        helpers::{describe_partition, list_partitions},
    },
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    functions_aggregate::count::count_all,
    logical_expr::{LogicalPlan, SortExpr, col, logical_plan::Union},
    physical_plan::{stream::RecordBatchStreamAdapter, streaming::PartitionStream},
    prelude::*,
};
use object_store::ObjectStore;
use std::{collections::BTreeSet, fmt, sync::Arc};

use crate::format::InputFormat;

pub mod combine_alleles;
pub mod combine_refs_one_scan;
pub mod combine_refs_union;
pub mod combiner_run;
pub mod cpu_runtime;
pub mod format;
pub mod pipeline;

/// How a dataset's rows and files are arranged on disk.
#[derive(Clone, Debug)]
pub struct DatasetLayout {
    pub locus_ordering: Vec<SortExpr>,
    pub partition_columns: Vec<(String, DataType)>,
    pub schema: Option<SchemaRef>,
}

/// A directory of per-sample tables, its format, layout, and discovered sample set.
#[derive(Debug)]
pub struct Dataset {
    table_path: ListingTableUrl,
    input_format: InputFormat,
    layout: DatasetLayout,
    sample_set: Vec<String>,
}

impl Dataset {
    /// Discovers the sample directories immediately below `table_path` with one
    /// object-store listing request.
    pub async fn discover(
        store: &dyn ObjectStore,
        mut table_path: ListingTableUrl,
        input_format: InputFormat,
        layout: DatasetLayout,
    ) -> Result<Self> {
        if !table_path.is_collection() {
            let path = <ListingTableUrl as AsRef<str>>::as_ref(&table_path);
            table_path = ListingTableUrl::parse(format!("{}/", path.trim_end_matches('/')))?;
        }
        let partitions = list_partitions(store, &table_path, 0, None).await?;
        let mut sample_set = partitions
            .iter()
            .filter_map(|partition| {
                let (path, depth, _) = describe_partition(partition);
                (depth == 1)
                    .then(|| path.trim_end_matches('/').rsplit('/').next())
                    .flatten()
                    .and_then(|directory| directory.strip_prefix("s="))
                    .map(str::to_string)
            })
            .collect::<Vec<_>>();
        sample_set.sort();
        if sample_set.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "dataset '{}' contains no samples",
                <ListingTableUrl as AsRef<str>>::as_ref(&table_path)
            )));
        }

        Ok(Self {
            table_path,
            input_format,
            layout,
            sample_set,
        })
    }

    pub fn sample_set(&self) -> &[String] {
        &self.sample_set
    }

    /// Reads the dataset's sample set with its declared ordering, partitions, and schema.
    pub async fn read(&self, ctx: &SessionContext) -> Result<DataFrame> {
        self.read_path(
            ctx,
            self.table_path.clone(),
            self.layout.partition_columns.clone(),
        )
        .await?
        .filter(
            col("s").in_list(
                self.sample_set
                    .iter()
                    .map(|sample| lit(sample.as_str()))
                    .collect(),
                false,
            ),
        )
    }

    /// Reads one sample directory, dropping the `s` partition column above it.
    pub async fn read_sample(&self, ctx: &SessionContext, sample: &str) -> Result<DataFrame> {
        let sample_path =
            ListingTableUrl::parse(format!("{}s={sample}/", self.table_path.as_str()))?;
        let partition_columns = self
            .layout
            .partition_columns
            .iter()
            .filter(|(name, _)| name != "s")
            .cloned()
            .collect();
        self.read_path(ctx, sample_path, partition_columns).await
    }

    /// Checks that the dataset's locus ordering starts with every required expression.
    pub fn check_ordering(&self, required: &[SortExpr]) -> Result<()> {
        if self.layout.locus_ordering.starts_with(required) {
            return Ok(());
        }

        Err(DataFusionError::Execution(format!(
            "dataset locus ordering {:?} does not satisfy required ordering {:?}",
            self.layout.locus_ordering, required
        )))
    }

    async fn read_path(
        &self,
        ctx: &SessionContext,
        table_path: ListingTableUrl,
        partition_columns: Vec<(String, DataType)>,
    ) -> Result<DataFrame> {
        let listing_options = ListingOptions::new(self.input_format.read_format())
            .with_file_sort_order(vec![self.layout.locus_ordering.clone()])
            .with_table_partition_cols(partition_columns);
        let config = ListingTableConfig::new(table_path).with_listing_options(listing_options);
        let config = match &self.layout.schema {
            Some(schema) => config.with_schema(Arc::clone(schema)),
            None => config.infer_schema(&ctx.state()).await?,
        };
        let table = ListingTable::try_new(config)?
            .with_cache(ctx.runtime_env().cache_manager.get_file_statistic_cache());
        ctx.read_table(Arc::new(table))
    }

    /// Restricts this dataset to a nonempty requested sample set, rejecting ids
    /// that are not present rather than silently intersecting the two sets.
    pub fn restrict_to(mut self, requested_sample_set: &[String]) -> Result<Self> {
        if requested_sample_set.is_empty() {
            return Err(DataFusionError::Execution(
                "requested sample set contains no samples".to_string(),
            ));
        }
        let available = self
            .sample_set
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let missing = requested_sample_set
            .iter()
            .map(String::as_str)
            .filter(|sample| !available.contains(sample))
            .collect::<BTreeSet<_>>();
        if !missing.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "samples not found in dataset: {}",
                missing.into_iter().collect::<Vec<_>>().join(", ")
            )));
        }

        let requested_sample_set = requested_sample_set
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        self.sample_set
            .retain(|sample| requested_sample_set.contains(sample.as_str()));
        Ok(self)
    }
}

/// A supported way to build one of the combiners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Formulation {
    CombineAllelesUnion,
    CombineRefsUnion,
    CombineRefsOneScan,
}

impl fmt::Display for Formulation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CombineAllelesUnion | Self::CombineRefsUnion => formatter.write_str("union"),
            Self::CombineRefsOneScan => formatter.write_str("one-scan"),
        }
    }
}

impl Formulation {
    pub fn required_layout(self) -> DatasetLayout {
        match self {
            Self::CombineAllelesUnion => combine_alleles::required_layout(),
            Self::CombineRefsUnion | Self::CombineRefsOneScan => reference_layout(),
        }
    }

    /// Builds this formulation's plan over `dataset`.
    pub async fn plan(self, ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
        let required_layout = self.required_layout();
        dataset.check_ordering(&required_layout.locus_ordering)?;
        match self {
            Self::CombineAllelesUnion => combine_alleles::plan(ctx, dataset).await,
            Self::CombineRefsUnion => combine_refs_union::plan(ctx, dataset).await,
            Self::CombineRefsOneScan => combine_refs_one_scan::plan(ctx, dataset).await,
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

#[derive(Debug)]
struct IntRangeStream {
    schema: SchemaRef,
    start: i32,
    end: i32,
    batch_size: i32,
}

impl PartitionStream for IntRangeStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let end = self.end;
        let batch_size = self.batch_size;

        // `unfold` produces one item per poll — that's the laziness.
        let stream = futures::stream::unfold(self.start, move |next| {
            let schema = Arc::clone(&schema);
            async move {
                if next > end {
                    return None;
                }
                let last = (next + batch_size - 1).min(end);
                let array = Int32Array::from_iter_values(next..=last);
                let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])
                    .map_err(DataFusionError::from);
                Some((batch, last + 1))
            }
        });

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

pub fn make_range_table_source(start: i32, end: i32, batch_size: u32) -> Result<StreamingTable> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)]));

    let partition: Arc<dyn PartitionStream> = Arc::new(IntRangeStream {
        schema: Arc::clone(&schema),
        start,
        end,
        batch_size: batch_size as i32,
    });

    Ok(
        StreamingTable::try_new(schema, vec![partition])?.with_sort_order(vec![SortExpr::new(
            col("idx"),
            true,
            true,
        )]),
    )
}

pub fn make_range_table(ctx: &SessionContext, n_rows: u32, batch_size: u32) -> Result<DataFrame> {
    let range_provider = Arc::new(make_range_table_source(1, n_rows as i32, batch_size)?);
    ctx.read_table(range_provider)
}

pub fn make_table_group_by_aggregate_sorted(
    ctx: &SessionContext,
    batch_size: u32,
    n_rows: u32,
) -> Result<DataFrame> {
    let df = make_range_table(ctx, n_rows, batch_size)?;
    df.aggregate(
        vec![(col("idx") / lit(1000)).alias("group")],
        vec![count_all()],
    )
}

pub fn make_table_range_join(
    ctx: &SessionContext,
    m: u32,
    n: u32,
    batch_size: u32,
) -> Result<DataFrame> {
    let left = make_range_table(ctx, m, batch_size)?;
    let right = make_range_table(ctx, n, batch_size)?.select(vec![col("idx").alias("idx2")])?;
    left.join(right, JoinType::Inner, &["idx"], &["idx2"], None)
}
