//! Stored tables read under a declared row ordering, validated against their files at
//! construction.
//!
//! The one table this module writes is a sample annotation table, around a data write that
//! [`crate::write`] performs, since the table's write and its read share one contract. Writing
//! the stored tables themselves stays with [`crate::write`] until an input table's data and its
//! annotation table share one directory, as ADR 0018 expects, and their layout becomes this
//! module's.
//!
//! - [`dataset`] reads a dataset's input tables, each with its sample set.
//! - [`locus_sorted_table`] reads one file or one directory of files as a single locus-sorted
//!   table.

pub mod dataset;
pub mod locus_sorted_table;

#[cfg(test)]
mod tests;

use crate::format::InputFormat;

use datafusion::{datasource::listing::ListingTableUrl, error::Result, prelude::SessionContext};
use futures_util::TryStreamExt;
use object_store::ObjectMeta;

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

fn normalize_table_path(mut table_path: ListingTableUrl) -> Result<ListingTableUrl> {
    if !table_path.is_collection() {
        let path = <ListingTableUrl as AsRef<str>>::as_ref(&table_path);
        table_path = ListingTableUrl::parse(format!("{}/", path.trim_end_matches('/')))?;
    }
    Ok(table_path)
}
