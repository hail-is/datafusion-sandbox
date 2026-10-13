//! Sample annotation tables: the table beside a multi-sample input table that declares its sample
//! set. See ADR 0018.
//!
//! A table has one row per sample, and its presence marks the input table complete. This module
//! owns the table's naming, schema, reading, and writing, and the order a write of the data
//! beside it keeps.

use super::{check_sample_column, sample_field};
use crate::{format::InputFormat, ordered_frame::OutputLayout, write::WriteTarget};

use datafusion::{
    arrow::{
        array::{Array, AsArray, StringViewArray},
        compute::cast,
        datatypes::{DataType, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    common::DataFusionError,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    error::Result,
    object_store::{self, ObjectStoreExt},
    prelude::SessionContext,
};
use std::{future::Future, sync::Arc};

/// Runs `data_write`, a write to `target` in `layout`, and returns what it returned.
///
/// It first removes any sample annotation table an earlier write left beside the target, and
/// afterwards, given a `sample_set`, writes the table of that sample set there. So a table only
/// ever marks the data the last write left, and only once that write is complete: a data write
/// that fails leaves no table, and one with no sample set leaves none either. The data write does
/// not start until the earlier table is gone.
///
/// # Errors
///
/// Returns the data write's error, or an error if the path is on a store the session does not
/// serve, or the store fails to delete or write the table.
pub async fn write_marked<T>(
    ctx: &SessionContext,
    target: &WriteTarget,
    layout: OutputLayout,
    sample_set: Option<&[String]>,
    data_write: impl Future<Output = Result<T>>,
) -> Result<T> {
    remove(ctx, target, layout).await?;
    let written = data_write.await?;
    if let Some(sample_set) = sample_set {
        write(ctx, target, layout, sample_set).await?;
    }
    Ok(written)
}

/// The stem of the input table that the file `file_name` is the sample annotation table of, if it
/// is one in the format with `extension`: `<stem>` for `<stem>.samples.<extension>`.
#[must_use]
pub fn annotated_stem<'a>(file_name: &'a str, extension: &str) -> Option<&'a str> {
    file_name
        .strip_suffix(extension)?
        .strip_suffix('.')?
        .strip_suffix(".samples")
}

/// The path of the sample annotation table of the input table named `input_table` in the dataset
/// root `root`.
pub(super) fn path(
    root: &ListingTableUrl,
    input_table: &str,
    input_format: &InputFormat,
) -> String {
    format!(
        "{}{}",
        root.as_str(),
        file_name(input_table, input_format.name())
    )
}

/// The sample set that the sample annotation table of the input table named `input_table` in the
/// dataset root `root` declares: its non-null string `s` column, sorted. Other columns are
/// ignored, and the rows of the input table are not read.
///
/// # Errors
///
/// Returns an error if the table cannot be read, has no non-null string `s` column, or declares a
/// null sample, a sample more than once, or no samples.
pub(super) async fn read(
    ctx: &SessionContext,
    root: &ListingTableUrl,
    input_table: &str,
    input_format: &InputFormat,
) -> Result<Vec<String>> {
    let path = path(root, input_table, input_format);
    let config = ListingTableConfig::new(ListingTableUrl::parse(&path)?)
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
    check_sample_column(schema).map_err(|problem| {
        DataFusionError::Plan(format!("sample annotation table '{path}' {problem}"))
    })?;
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

/// The file name of the sample annotation table of the input table with stem `stem`, in the
/// format with `extension`.
fn file_name(stem: &str, extension: &str) -> String {
    format!("{stem}.samples.{extension}")
}

/// The path of the sample annotation table beside a write to `target` in `layout`.
///
/// The path is `<stem>.samples.<ext>` with the target format's extension. The stem of one file is
/// its path without that extension, and the stem of a directory of files is its path.
fn beside(target: &WriteTarget, layout: OutputLayout) -> String {
    let extension = target.output_format.extension();
    let stem = match layout {
        OutputLayout::SingleFile => target
            .output_path
            .strip_suffix(&format!(".{extension}"))
            .unwrap_or(&target.output_path),
        OutputLayout::FilePerPartition => target.output_path.trim_end_matches('/'),
    };
    file_name(stem, extension)
}

/// The schema of a sample annotation table: one non-null column `s` of sample ids, typed as a
/// dataset's rows carry it.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![sample_field()]))
}

/// Writes the sample annotation table of `sample_set` beside a write to `target` in `layout`,
/// in the target's format with one row per sample in sorted order, replacing any file there.
async fn write(
    ctx: &SessionContext,
    target: &WriteTarget,
    layout: OutputLayout,
    sample_set: &[String],
) -> Result<()> {
    let mut samples = sample_set.iter().map(String::as_str).collect::<Vec<_>>();
    samples.sort_unstable();
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(StringViewArray::from(samples))])?;
    WriteTarget {
        output_path: beside(target, layout),
        output_format: target.output_format.clone(),
    }
    .write_unordered(ctx.read_batch(batch)?)
    .await?;
    Ok(())
}

/// Removes the sample annotation table beside a write to `target` in `layout`, if one is there.
async fn remove(ctx: &SessionContext, target: &WriteTarget, layout: OutputLayout) -> Result<()> {
    let url = ListingTableUrl::parse(beside(target, layout))?;
    let store = ctx.runtime_env().object_store(&url)?;
    match store.delete(url.prefix()).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
