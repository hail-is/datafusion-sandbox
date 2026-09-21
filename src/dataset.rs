//! Stored tables where a declared locus ordering meets their encoded files, including datasets
//! with sample sets and standalone sorted tables.

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
use object_store::ObjectMeta;
use std::{collections::BTreeSet, sync::Arc};

/// A directory of per-sample tables, its format, declared locus ordering, and sample set.
#[derive(Clone, Debug)]
pub struct Dataset {
    table_path: ListingTableUrl,
    input_format: InputFormat,
    locus_ordering: LocusOrdering,
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
        locus_ordering: LocusOrdering,
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
        let dataset = Self {
            table_path,
            input_format,
            locus_ordering,
            schema,
            sample_set,
            locus_representation,
        };
        dataset.stored_ordering()?;
        Ok(dataset)
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
        locus_ordering: LocusOrdering,
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
        Self::new(table_path, input_format, locus_ordering, schema, sample_set)
    }

    #[must_use]
    pub fn sample_set(&self) -> &[String] {
        &self.sample_set
    }

    #[must_use]
    pub const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// How the dataset's rows record their locus.
    #[must_use]
    pub const fn locus_representation(&self) -> LocusRepresentation {
        self.locus_representation
    }

    /// Expands the required query ordering after checking it against the dataset's locus ordering.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset's stored ordering does not start with `required`.
    pub fn query_ordering(&self, required: &LocusOrdering) -> Result<StoredOrdering> {
        self.check_ordering(required)?;
        Ok(required.expand(self.locus_representation))
    }

    /// Reads the dataset's whole sample set into one flat frame: the union of its per-sample
    /// scans, or the one scan of a single-sample dataset.
    ///
    /// # Errors
    ///
    /// Returns an error if a sample cannot be read or the sample plans cannot be combined.
    pub async fn read(&self, ctx: &SessionContext) -> Result<DataFrame> {
        let mut plans = Vec::with_capacity(self.sample_set.len());
        for sample in &self.sample_set {
            let frame = self.read_sample(ctx, sample).await?;
            plans.push(frame.into_unoptimized_plan());
        }
        union_or_single(ctx, plans)
    }

    /// Reads one sample directory as a sorted table and attaches its sample id.
    async fn read_sample(&self, ctx: &SessionContext, sample: &str) -> Result<DataFrame> {
        let sample_path =
            ListingTableUrl::parse(format!("{}s={sample}/", self.table_path.as_str()))?;
        let format = self.input_format.read_format();
        let files = list_files_by_extension(ctx, &sample_path, &self.input_format)
            .await?
            .into_iter()
            .map(PartitionedFile::new_from_meta)
            .collect();
        let table = SortedTable::new(
            sample_path.object_store(),
            format,
            files,
            Arc::clone(&self.schema),
            self.stored_ordering()?.sort_expressions(),
            Some(AttachedScalar {
                field: Arc::new(Field::new("s", DataType::Utf8, false)),
                value: ScalarValue::Utf8(Some(sample.to_string())),
            }),
        );
        ctx.read_table(Arc::new(table))
    }

    fn stored_ordering(&self) -> Result<StoredOrdering> {
        let stored_ordering = self.locus_ordering.expand(self.locus_representation);
        for column in stored_ordering.column_names() {
            if self.schema.field_with_name(&column).is_err() {
                return Err(DataFusionError::Plan(format!(
                    "locus ordering column '{column}' is missing from the dataset schema"
                )));
            }
        }
        Ok(stored_ordering)
    }

    /// Checks that the dataset's locus ordering starts with the required ordering.
    fn check_ordering(&self, required: &LocusOrdering) -> Result<()> {
        if required.is_prefix_of(&self.locus_ordering) {
            return Ok(());
        }

        Err(DataFusionError::Plan(format!(
            "dataset locus ordering {:?} does not satisfy required ordering {:?}",
            self.locus_ordering, required
        )))
    }

    /// Restricts this dataset to a nonempty requested sample set, rejecting ids
    /// that are not present rather than silently intersecting the two sets.
    ///
    /// # Errors
    ///
    /// Returns an error if `requested_sample_set` is empty or contains an unknown sample.
    pub fn restrict_to(&self, requested_sample_set: &[String]) -> Result<Self> {
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
        let mut restricted = self.clone();
        restricted
            .sample_set
            .retain(|sample| requested_sample_set.contains(sample.as_str()));
        Ok(restricted)
    }
}

/// Reads one file or one directory of files as a single sorted table under `locus_ordering`.
///
/// Unlike [`Dataset`], this function does not discover a sample set or attach a sample column. It
/// trusts the files to form one sorted table and lets [`SortedTable`] recover their order from
/// statistics.
///
/// # Errors
///
/// Returns an error if the path cannot be listed, contains no nonempty file of `input_format`, its
/// schema has no supported locus representation, an ordering field is absent, or the table cannot
/// be constructed.
pub async fn read_sorted_table(
    ctx: &SessionContext,
    table_path: ListingTableUrl,
    input_format: InputFormat,
    locus_ordering: LocusOrdering,
) -> Result<DataFrame> {
    let state = ctx.state();
    let store = ctx.runtime_env().object_store(&table_path)?;
    let format = input_format.read_format();
    let files = list_files_by_extension(ctx, &table_path, &input_format).await?;
    let input_file = first_nonempty_file(&files).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "no input files found in sorted table '{}'",
            table_path.as_str()
        ))
    })?;
    let schema = format
        .infer_schema(&state, &store, std::slice::from_ref(input_file))
        .await?;
    let representation = LocusRepresentation::detect(&schema)?;
    let stored_ordering = locus_ordering.expand(representation);
    for column in stored_ordering.column_names() {
        if schema.field_with_name(&column).is_err() {
            return Err(DataFusionError::Plan(format!(
                "locus ordering column '{column}' is missing from the sorted table schema"
            )));
        }
    }
    let table = SortedTable::new(
        table_path.object_store(),
        format,
        files
            .into_iter()
            .map(PartitionedFile::new_from_meta)
            .collect(),
        schema,
        stored_ordering.sort_expressions(),
        None,
    );
    ctx.read_table(Arc::new(table))
}

async fn list_files_by_extension(
    ctx: &SessionContext,
    table_path: &ListingTableUrl,
    input_format: &InputFormat,
) -> Result<Vec<ObjectMeta>> {
    let state = ctx.state();
    let store = ctx.runtime_env().object_store(table_path)?;
    let extension = input_format.read_format().get_ext();
    table_path
        .list_all_files(&state, store.as_ref(), &extension)
        .await?
        .try_collect()
        .await
}

fn first_nonempty_file(files: &[ObjectMeta]) -> Option<&ObjectMeta> {
    files.iter().find(|file| file.size > 0)
}

/// The union of nonempty `plans`, or the one plan itself.
///
/// # Errors
///
/// Returns an error if `plans` is empty or `DataFusion` cannot construct the union.
pub fn union_or_single(ctx: &SessionContext, mut plans: Vec<LogicalPlan>) -> Result<DataFrame> {
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
