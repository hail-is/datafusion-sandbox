//! Sample annotation tables: the table beside a multi-sample input table that declares its sample
//! set, one row per sample. See ADR 0018.

use crate::{ordered_frame::OutputLayout, stored::dataset::sample_field, write::WriteTarget};

use datafusion::{
    arrow::{
        array::StringViewArray,
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::listing::ListingTableUrl,
    error::Result,
    object_store::{self, ObjectStoreExt},
    prelude::SessionContext,
};
use std::sync::Arc;

/// The path of the sample annotation table beside a write to `target` in `layout`.
///
/// The path is `<stem>.samples.<ext>` with the target format's extension. The stem of one file is
/// its path without that extension, and the stem of a directory of files is its path.
#[must_use]
pub fn path(target: &WriteTarget, layout: OutputLayout) -> String {
    let extension = target.output_format.extension();
    let stem = match layout {
        OutputLayout::SingleFile => target
            .output_path
            .strip_suffix(&format!(".{extension}"))
            .unwrap_or(&target.output_path),
        OutputLayout::FilePerPartition => target.output_path.trim_end_matches('/'),
    };
    format!("{stem}.samples.{extension}")
}

/// The schema of a sample annotation table: one non-null column `s` of sample ids, typed as a
/// dataset's rows carry it.
#[must_use]
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![sample_field()]))
}

/// Writes the sample annotation table of `sample_set` beside a write to `target` in `layout`,
/// in the target's format with one row per sample in sorted order, replacing any file there.
///
/// # Errors
///
/// Returns an error if the table cannot be written.
pub async fn write(
    ctx: &SessionContext,
    target: &WriteTarget,
    layout: OutputLayout,
    sample_set: &[String],
) -> Result<()> {
    let mut samples = sample_set.iter().map(String::as_str).collect::<Vec<_>>();
    samples.sort_unstable();
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(StringViewArray::from(samples))])?;
    WriteTarget {
        output_path: path(target, layout),
        output_format: target.output_format.clone(),
    }
    .write_unordered(ctx.read_batch(batch)?)
    .await?;
    Ok(())
}

/// Removes the sample annotation table beside a write to `target` in `layout`, if one is there.
///
/// Every write removes an earlier write's table before its data, so neither a write that then
/// fails nor one that writes no table leaves a table marking its data complete.
///
/// # Errors
///
/// Returns an error if the path is on a store the session does not serve, or the store fails to
/// delete a table that is there.
pub async fn remove(
    ctx: &SessionContext,
    target: &WriteTarget,
    layout: OutputLayout,
) -> Result<()> {
    let url = ListingTableUrl::parse(path(target, layout))?;
    let store = ctx.runtime_env().object_store(&url)?;
    match store.delete(url.prefix()).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
