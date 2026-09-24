//! Stored tables read under a declared locus ordering, validated against their files at
//! construction.
//!
//! - [`dataset`] reads a dataset's per-sample tables under its sample set.
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
