//! Locus-sorted tables: one file or one directory of files read as a single sorted table under a
//! locus ordering.
//!
//! Construction classifies the path against its object store, infers the schema from the first
//! nonempty file of the input format, and validates the locus ordering against that schema.

use super::{first_nonempty_file, list_files_by_extension, normalize_table_path};
use crate::{
    format::InputFormat,
    locus::{LocusOrdering, LocusRepresentation, StoredOrdering},
    sorted_table::SortedTable,
};

use datafusion::{
    common::DataFusionError,
    datasource::{
        TableProvider,
        listing::{ListingTableUrl, PartitionedFile},
    },
    error::Result,
    prelude::{DataFrame, SessionContext},
};
use object_store::ObjectStoreExt;
use std::sync::Arc;

/// One file or one directory of files, its locus representation, and its stored ordering.
///
/// Unlike a dataset, it has no sample set and attaches no sample column. It trusts its files to
/// form one sorted table and lets [`SortedTable`] recover their order from statistics.
#[derive(Debug)]
pub struct LocusSortedTable {
    table: Arc<dyn TableProvider>,
    locus_representation: LocusRepresentation,
    stored_ordering: StoredOrdering,
}

impl LocusSortedTable {
    /// Opens `table_path` as one locus-sorted table. An object at the path is one file; otherwise
    /// the table is every file of `input_format` beneath the path.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store cannot be read, the path holds no nonempty file of
    /// `input_format`, schema inference fails, the schema has no supported locus representation,
    /// or it lacks a stored ordering column.
    pub async fn open(
        ctx: &SessionContext,
        table_path: ListingTableUrl,
        input_format: InputFormat,
        locus_ordering: LocusOrdering,
    ) -> Result<Self> {
        let store = ctx.runtime_env().object_store(&table_path)?;
        let table_path = if table_path.is_collection() {
            table_path
        } else {
            match store.head(table_path.prefix()).await {
                Ok(_) => table_path,
                Err(object_store::Error::NotFound { .. }) => normalize_table_path(table_path)?,
                Err(error) => return Err(error.into()),
            }
        };
        let format = input_format.read_format();
        let files = list_files_by_extension(ctx, &table_path, &input_format).await?;
        let input_file = first_nonempty_file(&files).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "no input files found in locus-sorted table '{}'",
                table_path.as_str()
            ))
        })?;
        let schema = format
            .infer_schema(&ctx.state(), &store, std::slice::from_ref(input_file))
            .await?;
        let (locus_representation, stored_ordering) = locus_ordering.validate_against(&schema)?;
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
        Ok(Self {
            table: Arc::new(table),
            locus_representation,
            stored_ordering,
        })
    }

    /// Reads the table as one frame in its stored ordering.
    ///
    /// # Errors
    ///
    /// Returns an error if `DataFusion` cannot build a frame over the table.
    pub fn read(&self, ctx: &SessionContext) -> Result<DataFrame> {
        ctx.read_table(Arc::clone(&self.table))
    }

    /// How the table's rows record their locus.
    #[must_use]
    pub const fn locus_representation(&self) -> LocusRepresentation {
        self.locus_representation
    }

    /// The table's locus ordering expanded into its stored fields.
    #[must_use]
    pub const fn stored_ordering(&self) -> &StoredOrdering {
        &self.stored_ordering
    }
}
