//! Stored datasets and their shared layouts.

use crate::{
    format::InputFormat,
    locus::{LocusOrdering, LocusRepresentation, StoredOrdering},
    sorted_table::{AttachedScalar, SortedTable},
};

use datafusion::{
    arrow::datatypes::{DataType, Field, SchemaRef},
    common::{DataFusionError, ScalarValue},
    datasource::listing::{
        ListingTableUrl, PartitionedFile,
        helpers::{describe_partition, list_partitions},
    },
    error::Result,
    logical_expr::{LogicalPlan, logical_plan::Union},
    prelude::*,
};
use futures_util::{StreamExt, TryStreamExt};
use std::{collections::BTreeSet, num::NonZeroUsize, sync::Arc};

/// How [`Dataset::read`] arranges the per-sample scans beneath the frame it returns.
///
/// The formulation chooses the shape; the dataset knows nothing about why.
#[derive(Clone, Debug)]
pub enum ScanShape {
    /// The union of every sample's scan, or the one scan of a single sample.
    Flat,
    /// The sample set split into `groups` sample groups by [`Dataset::sample_groups`], each
    /// group's scans unioned and sorted by `ordering`, and the groups unioned. A group of one
    /// sample is its scan, and one group is the flat shape.
    SampleGroups {
        groups: NonZeroUsize,
        ordering: StoredOrdering,
    },
}

/// How a dataset's rows and files are arranged on disk.
#[derive(Clone, Debug)]
pub struct DatasetLayout {
    pub locus_ordering: LocusOrdering,
}

impl DatasetLayout {
    fn stored_ordering(
        &self,
        representation: LocusRepresentation,
        schema: &SchemaRef,
    ) -> Result<StoredOrdering> {
        let stored_ordering = self.locus_ordering.expand(representation);
        for column in stored_ordering.column_names() {
            if schema.field_with_name(column).is_err() {
                return Err(DataFusionError::Plan(format!(
                    "locus ordering column '{column}' is missing from the dataset schema"
                )));
            }
        }
        Ok(stored_ordering)
    }
}

/// A directory of per-sample tables, its format, layout, and discovered sample set.
#[derive(Debug)]
pub struct Dataset {
    table_path: ListingTableUrl,
    input_format: InputFormat,
    layout: DatasetLayout,
    schema: SchemaRef,
    sample_set: Vec<String>,
    locus_representation: LocusRepresentation,
}

impl Dataset {
    /// Constructs a dataset from already resolved schema and sample-set data.
    ///
    /// # Errors
    ///
    /// Returns an error if the table path cannot be normalized, the sample set is empty, the
    /// locus representation cannot be detected, or the schema lacks an ordering column.
    pub fn new(
        table_path: ListingTableUrl,
        input_format: InputFormat,
        layout: DatasetLayout,
        schema: SchemaRef,
        sample_set: Vec<String>,
    ) -> Result<Self> {
        let table_path = normalize_table_path(table_path)?;
        if sample_set.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "dataset '{}' contains no samples",
                <ListingTableUrl as AsRef<str>>::as_ref(&table_path)
            )));
        }
        let locus_representation = LocusRepresentation::detect(&schema)?;
        layout.stored_ordering(locus_representation, &schema)?;

        Ok(Self {
            table_path,
            input_format,
            layout,
            schema,
            sample_set,
            locus_representation,
        })
    }

    /// Discovers the sample directories immediately below `table_path` with one
    /// object-store listing request, and resolves the dataset schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store cannot be read, the dataset has no samples or input
    /// files, schema inference fails, or the resolved dataset is invalid.
    pub async fn discover(
        ctx: &SessionContext,
        table_path: ListingTableUrl,
        input_format: InputFormat,
        layout: DatasetLayout,
        schema: Option<SchemaRef>,
    ) -> Result<Self> {
        let table_path = normalize_table_path(table_path)?;
        let state = ctx.state();
        let store = ctx.runtime_env().object_store(&table_path)?;
        let partitions = list_partitions(store.as_ref(), &table_path, 0, None).await?;
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
            return Err(DataFusionError::Plan(format!(
                "dataset '{}' contains no samples",
                <ListingTableUrl as AsRef<str>>::as_ref(&table_path)
            )));
        }
        let schema = if let Some(schema) = schema {
            schema
        } else {
            let format = input_format.read_format();
            let extension = format.get_ext();
            let mut files = table_path
                .list_all_files(&state, store.as_ref(), &extension)
                .await?;
            let input_file = loop {
                match files.next().await.transpose()? {
                    Some(file) if file.size > 0 => break file,
                    Some(_) => {}
                    None => {
                        return Err(DataFusionError::Plan(format!(
                            "no input files found in dataset '{}'",
                            table_path.as_str()
                        )));
                    }
                }
            };
            format.infer_schema(&state, &store, &[input_file]).await?
        };
        Self::new(table_path, input_format, layout, schema, sample_set)
    }

    #[must_use]
    pub fn sample_set(&self) -> &[String] {
        &self.sample_set
    }

    #[must_use]
    pub const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Expands the required query ordering after checking it against the layout.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset's stored ordering does not start with `required`.
    pub fn query_ordering(&self, required: &LocusOrdering) -> Result<StoredOrdering> {
        self.check_ordering(required)?;
        Ok(required.expand(self.locus_representation))
    }

    /// Splits the sample set into at most `groups` sample groups: contiguous slices of the
    /// sorted sample set whose sizes differ by at most one, larger groups first. A group count
    /// above the sample count clamps to one sample per group.
    #[must_use]
    pub fn sample_groups(&self, groups: NonZeroUsize) -> Vec<&[String]> {
        // A nonempty sample set keeps the clamped count nonzero, so the divisions cannot fail.
        let groups = groups.get().min(self.sample_set.len());
        let quotient = self.sample_set.len().checked_div(groups).unwrap_or(0);
        let remainder = self.sample_set.len().checked_rem(groups).unwrap_or(0);
        let mut rest = self.sample_set.as_slice();
        (0..groups)
            .map(|index| {
                let size = quotient.saturating_add(usize::from(index < remainder));
                let (group, tail) = rest.split_at(size);
                rest = tail;
                group
            })
            .collect()
    }

    /// Reads the dataset's whole sample set into one frame arranged as `shape`.
    ///
    /// # Errors
    ///
    /// Returns an error if a sample cannot be read or the sample plans cannot be combined.
    pub async fn read(&self, ctx: &SessionContext, shape: &ScanShape) -> Result<DataFrame> {
        let (sample_groups, ordering) = match shape {
            ScanShape::Flat => return self.read_sample_group(ctx, &self.sample_set).await,
            ScanShape::SampleGroups { groups, ordering } => (self.sample_groups(*groups), ordering),
        };
        if sample_groups.len() == 1 {
            return self.read_sample_group(ctx, &self.sample_set).await;
        }
        let mut plans = Vec::with_capacity(sample_groups.len());
        for group in sample_groups {
            let frame = self.read_sample_group(ctx, group).await?;
            let frame = if group.len() > 1 {
                frame.sort(ordering.sort_expressions())?
            } else {
                frame
            };
            plans.push(frame.into_unoptimized_plan());
        }
        union_plans(ctx, plans)
    }

    /// Reads a nonempty group of samples into one frame: the union of their scans, or the one
    /// scan of a single sample.
    async fn read_sample_group(&self, ctx: &SessionContext, group: &[String]) -> Result<DataFrame> {
        let mut plans = Vec::with_capacity(group.len());
        for sample in group {
            let frame = self.read_sample(ctx, sample).await?;
            plans.push(frame.into_unoptimized_plan());
        }
        union_plans(ctx, plans)
    }

    /// Reads one sample directory as a sorted table and attaches its sample id.
    async fn read_sample(&self, ctx: &SessionContext, sample: &str) -> Result<DataFrame> {
        let sample_path =
            ListingTableUrl::parse(format!("{}s={sample}/", self.table_path.as_str()))?;
        let state = ctx.state();
        let store = ctx.runtime_env().object_store(&sample_path)?;
        let format = self.input_format.read_format();
        let extension = format.get_ext();
        let files = sample_path
            .list_all_files(&state, store.as_ref(), &extension)
            .await?
            .map_ok(PartitionedFile::new_from_meta)
            .try_collect()
            .await?;
        let table = SortedTable::new(
            sample_path.object_store(),
            format,
            files,
            Arc::clone(&self.schema),
            self.layout
                .stored_ordering(self.locus_representation, &self.schema)?
                .sort_expressions(),
            Some(AttachedScalar {
                field: Arc::new(Field::new("s", DataType::Utf8, false)),
                value: ScalarValue::Utf8(Some(sample.to_string())),
            }),
        );
        ctx.read_table(Arc::new(table))
    }

    /// Checks that the dataset's locus ordering starts with the required ordering.
    fn check_ordering(&self, required: &LocusOrdering) -> Result<()> {
        if required.is_prefix_of(&self.layout.locus_ordering) {
            return Ok(());
        }

        Err(DataFusionError::Plan(format!(
            "dataset locus ordering {:?} does not satisfy required ordering {:?}",
            self.layout.locus_ordering, required
        )))
    }

    /// Restricts this dataset to a nonempty requested sample set, rejecting ids
    /// that are not present rather than silently intersecting the two sets.
    ///
    /// # Errors
    ///
    /// Returns an error if `requested_sample_set` is empty or contains an unknown sample.
    pub fn restrict_to(mut self, requested_sample_set: &[String]) -> Result<Self> {
        if requested_sample_set.is_empty() {
            return Err(DataFusionError::Plan(
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
            return Err(DataFusionError::Plan(format!(
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

/// The union of nonempty `plans`, or the one plan itself.
fn union_plans(ctx: &SessionContext, mut plans: Vec<LogicalPlan>) -> Result<DataFrame> {
    let plan = if plans.len() == 1 {
        plans.pop().ok_or_else(|| {
            DataFusionError::Internal("a non-empty sample group produced no plans".to_string())
        })?
    } else {
        LogicalPlan::Union(Union::try_new(plans.into_iter().map(Arc::new).collect())?)
    };
    Ok(DataFrame::new(ctx.state(), plan))
}

fn normalize_table_path(mut table_path: ListingTableUrl) -> Result<ListingTableUrl> {
    if !table_path.is_collection() {
        let path = <ListingTableUrl as AsRef<str>>::as_ref(&table_path);
        table_path = ListingTableUrl::parse(format!("{}/", path.trim_end_matches('/')))?;
    }
    Ok(table_path)
}
