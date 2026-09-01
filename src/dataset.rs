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
use std::{collections::BTreeSet, sync::Arc};

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
        let schema = match schema {
            Some(schema) => schema,
            None => {
                let format = input_format.read_format();
                let extension = format.get_ext();
                let mut files = table_path
                    .list_all_files(&state, store.as_ref(), &extension)
                    .await?;
                let input_file = loop {
                    match files.next().await.transpose()? {
                        Some(file) if file.size > 0 => break file,
                        Some(_) => continue,
                        None => {
                            return Err(DataFusionError::Plan(format!(
                                "no input files found in dataset '{}'",
                                table_path.as_str()
                            )));
                        }
                    }
                };
                format.infer_schema(&state, &store, &[input_file]).await?
            }
        };
        Self::new(table_path, input_format, layout, schema, sample_set)
    }

    pub fn sample_set(&self) -> &[String] {
        &self.sample_set
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Expands the required query ordering after checking it against the layout.
    pub fn query_ordering(&self, required: &LocusOrdering) -> Result<StoredOrdering> {
        self.check_ordering(required)?;
        Ok(required.expand(self.locus_representation))
    }

    /// Reads the dataset's whole sample set into one frame.
    ///
    /// The formulation chooses the scan shape. If another shape is added, it should
    /// become an argument here rather than a branch owned by the dataset.
    pub async fn read(&self, ctx: &SessionContext) -> Result<DataFrame> {
        let mut plans = Vec::with_capacity(self.sample_set.len());
        for sample in &self.sample_set {
            let frame = self.read_sample(ctx, sample).await?;
            plans.push(Arc::new(frame.into_unoptimized_plan()));
        }
        let plan = if plans.len() == 1 {
            (*plans.pop().expect("a dataset has at least one sample")).clone()
        } else {
            LogicalPlan::Union(Union::try_new(plans)?)
        };
        Ok(DataFrame::new(ctx.state(), plan))
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

fn normalize_table_path(mut table_path: ListingTableUrl) -> Result<ListingTableUrl> {
    if !table_path.is_collection() {
        let path = <ListingTableUrl as AsRef<str>>::as_ref(&table_path);
        table_path = ListingTableUrl::parse(format!("{}/", path.trim_end_matches('/')))?;
    }
    Ok(table_path)
}
