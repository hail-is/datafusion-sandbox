//! Datasets: stored input tables under a declared locus ordering, each with its sample set, which
//! a multi-sample input table declares in its sample annotation table (ADR 0018).

use super::{first_nonempty_file, list_files_by_extension, normalize_table_path};
use crate::{
    format::InputFormat,
    locus::{LocusOrdering, LocusRepresentation, StoredOrdering},
    sorted_table::{AttachedScalar, SortedTable},
};

use datafusion::{
    arrow::{
        array::{Array, AsArray},
        compute::cast,
        datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
    },
    common::{DataFusionError, ScalarValue},
    datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
    },
    error::Result,
    logical_expr::{LogicalPlan, logical_plan::Union},
    prelude::*,
};
use object_store::{ListResult, ObjectMeta};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// The column every row read from a dataset carries its sample id in: a non-null `s` of the view
/// string type, which is how both formats read a stored string column back.
#[must_use]
pub fn sample_field() -> FieldRef {
    Arc::new(Field::new("s", DataType::Utf8View, false))
}

/// How an input table is stored in its dataset's root, which decides how its rows come to carry
/// their sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputTableKind {
    /// The directory `s=<id>/`, whose rows are given its one sample as they are read.
    SingleSample,
    /// The file `<name>.<ext>`, whose rows carry their sample.
    MultiSampleFile,
    /// The directory `<name>/`, whose files' rows carry their sample.
    MultiSampleDirectory,
}

/// One locus-sorted table a dataset holds, named by its entry in the dataset root without the
/// format's extension, with the samples it covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputTable {
    name: String,
    kind: InputTableKind,
    sample_set: Vec<String>,
}

impl InputTable {
    /// The single-sample input table stored as the directory `s=<sample>/`.
    #[must_use]
    pub fn single_sample(sample: &str) -> Self {
        Self {
            name: format!("s={sample}"),
            kind: InputTableKind::SingleSample,
            sample_set: vec![sample.to_string()],
        }
    }

    /// The multi-sample input table stored as the file `<name>.<ext>`, covering `sample_set`.
    #[must_use]
    pub fn multi_sample_file(name: &str, sample_set: Vec<String>) -> Self {
        Self::multi_sample(name, InputTableKind::MultiSampleFile, sample_set)
    }

    /// The multi-sample input table stored as the directory `<name>/`, covering `sample_set`.
    #[must_use]
    pub fn multi_sample_directory(name: &str, sample_set: Vec<String>) -> Self {
        Self::multi_sample(name, InputTableKind::MultiSampleDirectory, sample_set)
    }

    fn multi_sample(name: &str, kind: InputTableKind, mut sample_set: Vec<String>) -> Self {
        sample_set.sort();
        Self {
            name: name.to_string(),
            kind,
            sample_set,
        }
    }

    /// The table's entry in the dataset root without the format's extension, such as
    /// `s=HG00308` or `g0`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// How the table is stored.
    #[must_use]
    pub const fn kind(&self) -> InputTableKind {
        self.kind
    }

    /// Whether the table's rows carry their sample.
    #[must_use]
    pub const fn is_multi_sample(&self) -> bool {
        !matches!(self.kind, InputTableKind::SingleSample)
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
        let mut tables_by_sample = BTreeMap::new();
        for table in &input_tables {
            for sample in &table.sample_set {
                if let Some(first) = tables_by_sample.insert(sample, &table.name) {
                    return Err(DataFusionError::Plan(format!(
                        "sample '{sample}' is in both input tables '{first}' and '{}' of dataset \
                         '{}'",
                        table.name,
                        <ListingTableUrl as AsRef<str>>::as_ref(&table_path)
                    )));
                }
            }
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

    /// Discovers the input tables in the root `table_path` with one object-store listing request,
    /// reads each multi-sample input table's sample set from its sample annotation table, and
    /// resolves the dataset schema, the rows' schema without their sample.
    ///
    /// Each root entry must be an `s=<id>/` directory, a sample annotation table
    /// `<stem>.samples.<ext>`, or the file `<stem>.<ext>` or directory `<stem>/` it is beside,
    /// where `<ext>` is the input format's extension. See ADR 0018.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store cannot be read, a root entry is none of the above,
    /// multi-sample data has no sample annotation table or one has no data, an annotation table
    /// cannot be read or has no non-null string `s` column, the dataset has no samples or input
    /// files, schema inference fails, or the resolved dataset is invalid.
    pub async fn discover(
        ctx: &SessionContext,
        table_path: ListingTableUrl,
        input_format: InputFormat,
        locus_ordering: LocusOrdering,
        schema: Option<SchemaRef>,
    ) -> Result<Self> {
        let table_path = normalize_table_path(table_path)?;
        let store = ctx.runtime_env().object_store(&table_path)?;
        let root = store.list_with_delimiter(Some(table_path.prefix())).await?;
        let entries = RootEntries::classify(&table_path, &input_format, &root)?;
        let mut input_tables = entries
            .single_samples
            .iter()
            .map(|sample| InputTable::single_sample(sample))
            .collect::<Vec<_>>();
        for (name, kind) in entries.multi_samples {
            let annotation_table = format!(
                "{}{name}.samples.{}",
                table_path.as_str(),
                input_format.name()
            );
            let sample_set = read_sample_set(ctx, &annotation_table, &input_format).await?;
            input_tables.push(InputTable::multi_sample(&name, kind, sample_set));
        }
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
            let input_file = first_input_file(ctx, &table_path, &input_format, &input_tables)
                .await?
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "no input files found in dataset '{}'",
                        table_path.as_str()
                    ))
                })?;
            let schema = format
                .infer_schema(&ctx.state(), &store, &[input_file])
                .await?;
            let fields = schema
                .fields()
                .iter()
                .filter(|field| field.name() != sample_field().name())
                .cloned()
                .collect::<Vec<_>>();
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
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

    /// Reads one input table as a sorted table: a single-sample table's sample id attached to its
    /// rows, or a multi-sample table's sample read from its files.
    async fn read_input_table(
        &self,
        ctx: &SessionContext,
        input_table: &InputTable,
    ) -> Result<DataFrame> {
        let table_path = input_table_path(&self.table_path, &self.input_format, input_table)?;
        let files = list_files_by_extension(ctx, &table_path, &self.input_format)
            .await?
            .into_iter()
            .map(PartitionedFile::new_from_meta)
            .collect();
        let (file_schema, attached_scalar) = match (input_table.kind, input_table.sample_set()) {
            (InputTableKind::SingleSample, [sample]) => (
                Arc::clone(&self.schema),
                Some(AttachedScalar {
                    field: sample_field(),
                    value: ScalarValue::Utf8View(Some(sample.clone())),
                }),
            ),
            (InputTableKind::SingleSample, sample_set) => {
                return Err(DataFusionError::Internal(format!(
                    "single-sample input table '{}' covers {} samples",
                    input_table.name(),
                    sample_set.len()
                )));
            }
            (InputTableKind::MultiSampleFile | InputTableKind::MultiSampleDirectory, _) => {
                let mut fields = self.schema.fields().to_vec();
                fields.push(sample_field());
                let schema = Schema::new_with_metadata(fields, self.schema.metadata().clone());
                (Arc::new(schema), None)
            }
        };
        let table = SortedTable::new(
            table_path.object_store(),
            self.input_format.read_format(),
            files,
            file_schema,
            self.locus_ordering
                .expand(self.locus_representation)
                .sort_expressions(),
            attached_scalar,
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

/// A dataset root's entries, classified and paired.
struct RootEntries {
    /// The sample of each `s=<id>/` directory.
    single_samples: Vec<String>,
    /// The name and kind of each multi-sample input table paired with its annotation table.
    multi_samples: Vec<(String, InputTableKind)>,
}

impl RootEntries {
    /// Classifies the entries of `root`, the listing of `table_path`, and pairs multi-sample data
    /// with its sample annotation table.
    fn classify(
        table_path: &ListingTableUrl,
        input_format: &InputFormat,
        root: &ListResult,
    ) -> Result<Self> {
        let extension = format!(".{}", input_format.name());
        let annotation_suffix = format!(".samples{extension}");
        let entry_url = |entry: &str| format!("{}{entry}", table_path.as_str());
        let mut single_samples = Vec::new();
        let mut data = BTreeMap::new();
        let mut annotated = BTreeSet::new();
        let mut directories = root
            .common_prefixes
            .iter()
            .filter_map(|prefix| prefix.filename())
            .collect::<Vec<_>>();
        directories.sort_unstable();
        for directory in directories {
            match directory.strip_prefix("s=") {
                Some(sample) => single_samples.push(sample.to_string()),
                None => {
                    data.insert(
                        directory.to_string(),
                        (
                            InputTableKind::MultiSampleDirectory,
                            format!("{directory}/"),
                        ),
                    );
                }
            }
        }
        let mut objects = root
            .objects
            .iter()
            // A store may list a directory marker as an object at the root itself.
            .filter(|object| &object.location != table_path.prefix())
            .filter_map(|object| object.location.filename())
            .collect::<Vec<_>>();
        objects.sort_unstable();
        for object in objects {
            if let Some(stem) = object.strip_suffix(&annotation_suffix) {
                annotated.insert(stem);
            } else if let Some(stem) = object.strip_suffix(&extension) {
                if data
                    .insert(
                        stem.to_string(),
                        (InputTableKind::MultiSampleFile, object.to_string()),
                    )
                    .is_some()
                {
                    return Err(DataFusionError::Plan(format!(
                        "dataset entries '{}' and '{}' would be one input table '{stem}'",
                        entry_url(object),
                        entry_url(&format!("{stem}/"))
                    )));
                }
            } else {
                return Err(DataFusionError::Plan(format!(
                    "dataset entry '{}' is neither an input table nor a sample annotation table \
                     of format {}",
                    entry_url(object),
                    input_format.name()
                )));
            }
        }
        if let Some((_, (_, entry))) = data
            .iter()
            .find(|(stem, _)| !annotated.contains(stem.as_str()))
        {
            return Err(DataFusionError::Plan(format!(
                "multi-sample input table '{}' has no sample annotation table, as an incomplete \
                 write leaves it",
                entry_url(entry)
            )));
        }
        if let Some(stem) = annotated.iter().find(|stem| !data.contains_key(**stem)) {
            return Err(DataFusionError::Plan(format!(
                "sample annotation table '{}' has no data beside it",
                entry_url(&format!("{stem}{annotation_suffix}"))
            )));
        }
        Ok(Self {
            single_samples,
            multi_samples: data
                .into_iter()
                .map(|(stem, (kind, _))| (stem, kind))
                .collect(),
        })
    }
}

/// The location of `input_table` in the dataset root `table_path`: a collection for a directory,
/// or the one file of a multi-sample file.
fn input_table_path(
    table_path: &ListingTableUrl,
    input_format: &InputFormat,
    input_table: &InputTable,
) -> Result<ListingTableUrl> {
    let entry = match input_table.kind {
        InputTableKind::SingleSample | InputTableKind::MultiSampleDirectory => {
            format!("{}/", input_table.name)
        }
        InputTableKind::MultiSampleFile => {
            format!("{}.{}", input_table.name, input_format.name())
        }
    };
    ListingTableUrl::parse(format!("{}{entry}", table_path.as_str()))
}

/// The first nonempty file of the first input table, in name order, that has one.
async fn first_input_file(
    ctx: &SessionContext,
    table_path: &ListingTableUrl,
    input_format: &InputFormat,
    input_tables: &[InputTable],
) -> Result<Option<ObjectMeta>> {
    for input_table in input_tables {
        let path = input_table_path(table_path, input_format, input_table)?;
        let files = list_files_by_extension(ctx, &path, input_format).await?;
        if let Some(file) = first_nonempty_file(&files) {
            return Ok(Some(file.clone()));
        }
    }
    Ok(None)
}

/// The sample set declared by the sample annotation table at `path`: its non-null string `s`
/// column, sorted. Other columns are ignored, and the rows of the table it declares are not read.
async fn read_sample_set(
    ctx: &SessionContext,
    path: &str,
    input_format: &InputFormat,
) -> Result<Vec<String>> {
    let config = ListingTableConfig::new(ListingTableUrl::parse(path)?)
        .with_listing_options(ListingOptions::new(input_format.read_format()))
        .infer_schema(&ctx.state())
        .await?;
    let sample = sample_field();
    let invalid = |problem: &str| {
        DataFusionError::Plan(format!(
            "sample annotation table '{path}' {problem}; it needs a non-null string column '{}'",
            sample.name()
        ))
    };
    let schema = config
        .file_schema
        .as_ref()
        .ok_or_else(|| invalid("has no schema"))?;
    match schema.field_with_name(sample.name()) {
        Ok(field)
            if matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) => {}
        Ok(field) => {
            return Err(invalid(&format!(
                "has a column '{}' of type {}",
                sample.name(),
                field.data_type()
            )));
        }
        Err(_) => {
            return Err(invalid(&format!("has no column '{}'", sample.name())));
        }
    }
    let batches = ctx
        .read_table(Arc::new(ListingTable::try_new(config)?))?
        .select_columns(&[sample.name()])?
        .collect()
        .await?;
    let mut sample_set = Vec::new();
    for batch in &batches {
        let column = cast(batch.column(0), &DataType::Utf8)?;
        if column.null_count() > 0 {
            return Err(invalid("has a null sample"));
        }
        sample_set.extend(column.as_string::<i32>().iter().flatten().map(String::from));
    }
    sample_set.sort();
    if let Some([duplicate, _]) = sample_set
        .array_windows()
        .find(|[left, right]| left == right)
    {
        return Err(DataFusionError::Plan(format!(
            "sample annotation table '{path}' declares sample '{duplicate}' more than once"
        )));
    }
    if sample_set.is_empty() {
        return Err(invalid("declares no samples"));
    }
    Ok(sample_set)
}
