//! Stored datasets and their shared layouts.

use crate::format::InputFormat;

use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    common::DataFusionError,
    datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        helpers::{describe_partition, list_partitions},
    },
    error::Result,
    logical_expr::SortExpr,
    prelude::*,
};
use futures_util::StreamExt;
use std::{collections::BTreeSet, sync::Arc};

/// How a dataset's rows and files are arranged on disk.
#[derive(Clone, Debug)]
pub struct DatasetLayout {
    pub locus_ordering: Vec<SortExpr>,
    pub partition_columns: Vec<(String, DataType)>,
    pub schema: Option<SchemaRef>,
}

impl DatasetLayout {
    fn check_locus_ordering_columns(&self, schema: &SchemaRef) -> Result<()> {
        for column in self
            .locus_ordering
            .iter()
            .flat_map(|ordering| ordering.expr.column_refs())
        {
            let is_partition_column = self
                .partition_columns
                .iter()
                .any(|(name, _)| name == &column.name);
            if schema.field_with_name(&column.name).is_err() && !is_partition_column {
                return Err(DataFusionError::Plan(format!(
                    "locus ordering column '{}' is missing from the dataset schema",
                    column.name
                )));
            }
        }
        Ok(())
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
}

impl Dataset {
    /// Discovers the sample directories immediately below `table_path` with one
    /// object-store listing request, and resolves the dataset schema.
    pub async fn discover(
        ctx: &SessionContext,
        mut table_path: ListingTableUrl,
        input_format: InputFormat,
        layout: DatasetLayout,
    ) -> Result<Self> {
        if !table_path.is_collection() {
            let path = <ListingTableUrl as AsRef<str>>::as_ref(&table_path);
            table_path = ListingTableUrl::parse(format!("{}/", path.trim_end_matches('/')))?;
        }
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
            return Err(DataFusionError::Execution(format!(
                "dataset '{}' contains no samples",
                <ListingTableUrl as AsRef<str>>::as_ref(&table_path)
            )));
        }
        let schema = match &layout.schema {
            Some(schema) => Arc::clone(schema),
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
        layout.check_locus_ordering_columns(&schema)?;

        Ok(Self {
            table_path,
            input_format,
            layout,
            schema,
            sample_set,
        })
    }

    pub fn sample_set(&self) -> &[String] {
        &self.sample_set
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
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
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(Arc::clone(&self.schema));
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
