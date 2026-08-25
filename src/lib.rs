use datafusion::{
    arrow::{
        array::{Array, Int32Array, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::streaming::StreamingTable,
    common::{DataFusionError, config::ConfigOptions},
    datasource::{
        file_format::format_as_file_type,
        listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
            helpers::{describe_partition, list_partitions},
        },
    },
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext, context::DataFilePaths},
    functions_aggregate::count::count_all,
    logical_expr::{
        LogicalPlan, SortExpr, col,
        logical_plan::{LogicalPlanBuilder, Union},
    },
    physical_plan::{stream::RecordBatchStreamAdapter, streaming::PartitionStream},
    prelude::*,
};
use object_store::ObjectStore;
use std::{collections::BTreeSet, fmt, sync::Arc};

use crate::format::{InputFormat, OutputFormat};

pub mod combine_alleles;
pub mod combine_refs_one_scan;
pub mod combine_refs_union;
pub mod combiner_run;
pub mod cpu_runtime;
pub mod format;
pub mod pipeline;

/// A directory of per-sample tables, its format, and its sample set.
#[derive(Debug)]
pub struct Dataset {
    table_path: ListingTableUrl,
    input_format: InputFormat,
    sample_set: Vec<String>,
}

impl Dataset {
    /// Discovers the sample directories immediately below `table_path` with one
    /// object-store listing request.
    pub async fn discover(
        store: &dyn ObjectStore,
        mut table_path: ListingTableUrl,
        input_format: InputFormat,
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
            sample_set,
        })
    }

    /// The root every sample directory hangs off, normalised by [`discover`] to
    /// a collection URL and so always ending in a delimiter.
    ///
    /// [`discover`]: Self::discover
    pub fn table_path(&self) -> &ListingTableUrl {
        &self.table_path
    }

    pub fn input_format(&self) -> &InputFormat {
        &self.input_format
    }

    pub fn sample_set(&self) -> &[String] {
        &self.sample_set
    }

    /// The directory holding one sample's files, which is the only place the
    /// `s=<sample>` layout is written down for a formulation that reads each
    /// sample from its own path rather than through a partition column.
    pub fn sample_path(&self, sample: &str) -> Result<ListingTableUrl> {
        ListingTableUrl::parse(format!("{}s={sample}/", self.table_path.as_str()))
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
    /// Builds this formulation's plan over `dataset`.
    pub async fn plan(self, ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
        match self {
            Self::CombineAllelesUnion => combine_alleles::plan(ctx, dataset).await,
            Self::CombineRefsUnion => combine_refs_union::plan(ctx, dataset).await,
            Self::CombineRefsOneScan => combine_refs_one_scan::plan(ctx, dataset).await,
        }
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

/// Reads one or more file collections as a single table.
///
/// Takes the same listing options and optional schema that
/// [`SessionContext::register_listing_table`] does: `listing_options` carries
/// the file format along with the sort order and partition columns declared on
/// the files, and `schema` is inferred from the files when `None`.
///
/// This exists because DataFusion keeps its generic read entry point private,
/// exposing only per-format wrappers. It carries over that entry point's
/// file-statistics cache, so benchmark numbers stay comparable with
/// DataFusion's own readers rather than running uncached. It drops the
/// file-extension check, which DataFusion skips for collection paths anyway,
/// and every path here is a collection.
pub async fn read<P: DataFilePaths>(
    ctx: &SessionContext,
    table_paths: P,
    listing_options: ListingOptions,
    schema: Option<SchemaRef>,
) -> Result<DataFrame> {
    let table_paths = table_paths.to_urls()?;
    if table_paths.is_empty() {
        return Err(DataFusionError::Execution(
            "No table paths were provided".to_string(),
        ));
    }
    let config =
        ListingTableConfig::new_with_multi_paths(table_paths).with_listing_options(listing_options);
    let config = match schema {
        Some(schema) => config.with_schema(schema),
        None => config.infer_schema(&ctx.state()).await?,
    };
    let table = ListingTable::try_new(config)?
        .with_cache(ctx.runtime_env().cache_manager.get_file_statistic_cache());

    ctx.read_table(Arc::new(table))
}

/// Writes all rows in `df` to `path` using one of this project's output
/// formats, returning the number of rows written.
///
/// DataFusion exposes no generic DataFrame-level write-with-format entry point;
/// its public generic seam is [`LogicalPlanBuilder::copy_to`], one layer below
/// `DataFrame`, so this uses that supported route.
pub async fn write(df: DataFrame, path: &str, format: &OutputFormat) -> Result<u64> {
    let file_type = format_as_file_type(format.output_factory());
    let (session_state, plan) = df.into_parts();
    let plan = LogicalPlanBuilder::copy_to(
        plan,
        path.into(),
        file_type,
        format.format_options(),
        vec![],
    )?
    .build()?;
    let batches = DataFrame::new(session_state, plan).collect().await?;
    decode_row_count(&batches)
}

/// Decodes the number of rows produced by a DataFusion copy-to write.
///
/// Copy-to returns exactly one batch of one non-null `UInt64` value; anything
/// else means that contract moved underneath us.
fn decode_row_count(batches: &[RecordBatch]) -> Result<u64> {
    match batches {
        [batch] if batch.num_columns() == 1 && batch.num_rows() == 1 => batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .filter(|counts| !counts.is_null(0))
            .map(|counts| counts.value(0)),
        _ => None,
    }
    .ok_or_else(|| {
        DataFusionError::Internal(format!(
            "expected one batch with a single non-null count: UInt64 row from copy-to, got {batches:?}"
        ))
    })
}
