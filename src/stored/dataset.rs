//! Datasets: stored input tables under a declared locus ordering, each with its sample set.

use super::{list_files_by_extension, normalize_table_path};
use crate::{
    format::InputFormat,
    locus::{LocusOrdering, LocusRepresentation, StoredOrdering},
    sorted_table::{AttachedScalar, SortedTable},
};

use datafusion::{
    arrow::datatypes::{DataType, Field, FieldRef, SchemaRef},
    common::{DataFusionError, ScalarValue},
    datasource::listing::{
        ListingTableUrl, PartitionedFile,
        helpers::{describe_partition, list_partitions},
    },
    error::Result,
    logical_expr::{LogicalPlan, logical_plan::Union},
    prelude::*,
};
use futures_util::StreamExt;
use std::{collections::BTreeSet, sync::Arc};

/// The column every row read from a dataset carries its sample id in: a non-null `s` of the view
/// string type, which is how both formats read a stored string column back.
#[must_use]
pub fn sample_field() -> FieldRef {
    Arc::new(Field::new("s", DataType::Utf8View, false))
}

/// One locus-sorted table a dataset holds, named by its entry in the dataset root, with the
/// samples it covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputTable {
    name: String,
    sample_set: Vec<String>,
}

impl InputTable {
    /// The single-sample input table stored as the directory `s=<sample>/`.
    #[must_use]
    pub fn single_sample(sample: &str) -> Self {
        Self {
            name: format!("s={sample}"),
            sample_set: vec![sample.to_string()],
        }
    }

    /// The table's entry in the dataset root, such as `s=HG00308`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The samples the table covers, in sorted order.
    #[must_use]
    pub fn sample_set(&self) -> &[String] {
        &self.sample_set
    }
}

/// A directory of input tables, its format, and declared locus ordering.
#[derive(Clone, Debug)]
pub struct Dataset {
    table_path: ListingTableUrl,
    input_format: InputFormat,
    locus_ordering: LocusOrdering,
    schema: SchemaRef,
    input_tables: Vec<InputTable>,
    locus_representation: LocusRepresentation,
}

impl Dataset {
    /// Constructs a dataset from an already resolved schema and input tables, which it holds in
    /// name order.
    ///
    /// # Errors
    ///
    /// Returns an error if the table path cannot be normalized, there are no input tables, two
    /// input tables share a name, the locus representation cannot be detected, or the schema
    /// lacks an ordering column.
    pub fn new(
        table_path: ListingTableUrl,
        input_format: InputFormat,
        locus_ordering: LocusOrdering,
        schema: SchemaRef,
        mut input_tables: Vec<InputTable>,
    ) -> Result<Self> {
        let table_path = normalize_table_path(table_path)?;
        input_tables.sort_by(|left, right| left.name.cmp(&right.name));
        if input_tables.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "dataset '{}' contains no samples",
                <ListingTableUrl as AsRef<str>>::as_ref(&table_path)
            )));
        }
        if let Some([duplicate, _]) = input_tables
            .array_windows()
            .find(|[left, right]| left.name == right.name)
        {
            return Err(DataFusionError::Plan(format!(
                "dataset '{}' holds more than one input table named '{}'",
                <ListingTableUrl as AsRef<str>>::as_ref(&table_path),
                duplicate.name
            )));
        }
        let (locus_representation, _) = locus_ordering.validate_against(&schema)?;
        Ok(Self {
            table_path,
            input_format,
            locus_ordering,
            schema,
            input_tables,
            locus_representation,
        })
    }

    /// Discovers the single-sample input tables, the `s=<id>/` directories immediately below
    /// `table_path`, with one object-store listing request, and resolves the dataset schema.
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
        let input_tables = partitions
            .iter()
            .filter_map(|partition| {
                let (path, depth, _) = describe_partition(partition);
                (depth == 1)
                    .then(|| path.trim_end_matches('/').rsplit('/').next())
                    .flatten()
                    .and_then(|directory| directory.strip_prefix("s="))
                    .map(InputTable::single_sample)
            })
            .collect::<Vec<_>>();
        if input_tables.is_empty() {
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
        Self::new(
            table_path,
            input_format,
            locus_ordering,
            schema,
            input_tables,
        )
    }

    /// The dataset's input tables, in name order.
    #[must_use]
    pub fn input_tables(&self) -> &[InputTable] {
        &self.input_tables
    }

    /// The samples the dataset covers, the union of its input tables' sample sets, in sorted
    /// order.
    #[must_use]
    pub fn sample_set(&self) -> Vec<String> {
        let mut sample_set = self
            .input_tables
            .iter()
            .flat_map(|table| table.sample_set.iter().cloned())
            .collect::<Vec<_>>();
        sample_set.sort();
        sample_set
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

    /// Reads the dataset's input tables into one flat frame: the union of their scans, or the
    /// one scan of a dataset with one input table.
    ///
    /// # Errors
    ///
    /// Returns an error if an input table cannot be read or the plans cannot be combined.
    pub async fn read(&self, ctx: &SessionContext) -> Result<DataFrame> {
        let mut plans = Vec::with_capacity(self.input_tables.len());
        for input_table in &self.input_tables {
            let frame = self.read_input_table(ctx, input_table).await?;
            plans.push(frame.into_unoptimized_plan());
        }
        union_or_single(ctx, plans)
    }

    /// Reads one single-sample input table as a sorted table and attaches its sample id.
    async fn read_input_table(
        &self,
        ctx: &SessionContext,
        input_table: &InputTable,
    ) -> Result<DataFrame> {
        let [sample] = input_table.sample_set() else {
            return Err(DataFusionError::Internal(format!(
                "single-sample input table '{}' covers {} samples",
                input_table.name(),
                input_table.sample_set().len()
            )));
        };
        let table_path = ListingTableUrl::parse(format!(
            "{}{}/",
            self.table_path.as_str(),
            input_table.name()
        ))?;
        let format = self.input_format.read_format();
        let files = list_files_by_extension(ctx, &table_path, &self.input_format)
            .await?
            .into_iter()
            .map(PartitionedFile::new_from_meta)
            .collect();
        let table = SortedTable::new(
            table_path.object_store(),
            format,
            files,
            Arc::clone(&self.schema),
            self.locus_ordering
                .expand(self.locus_representation)
                .sort_expressions(),
            Some(AttachedScalar {
                field: sample_field(),
                value: ScalarValue::Utf8View(Some(sample.clone())),
            }),
        );
        ctx.read_table(Arc::new(table))
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

    /// Restricts this dataset to a nonempty request of input table names, keeping name order and
    /// rejecting names that are not present rather than silently intersecting the two.
    ///
    /// # Errors
    ///
    /// Returns an error if `requested_input_tables` is empty or names an unknown input table.
    pub fn restrict_to(&self, requested_input_tables: &[String]) -> Result<Self> {
        if requested_input_tables.is_empty() {
            return Err(DataFusionError::Plan(
                "no input tables requested".to_string(),
            ));
        }
        let available = self
            .input_tables
            .iter()
            .map(InputTable::name)
            .collect::<BTreeSet<_>>();
        let missing = requested_input_tables
            .iter()
            .map(String::as_str)
            .filter(|name| !available.contains(name))
            .collect::<BTreeSet<_>>();
        if !missing.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "input tables not found in dataset: {}",
                missing.into_iter().collect::<Vec<_>>().join(", ")
            )));
        }

        let requested_input_tables = requested_input_tables
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut restricted = self.clone();
        restricted
            .input_tables
            .retain(|table| requested_input_tables.contains(table.name()));
        Ok(restricted)
    }
}

/// The union of nonempty `plans`, or the one plan itself.
///
/// # Errors
///
/// Returns an error if `plans` is empty or `DataFusion` cannot construct the union.
pub fn union_or_single(ctx: &SessionContext, mut plans: Vec<LogicalPlan>) -> Result<DataFrame> {
    let plan = if plans.len() == 1 {
        plans.pop().ok_or_else(|| {
            DataFusionError::Internal("a non-empty input group produced no plans".to_string())
        })?
    } else {
        LogicalPlan::Union(Union::try_new(plans.into_iter().map(Arc::new).collect())?)
    };
    Ok(DataFrame::new(ctx.state(), plan))
}
