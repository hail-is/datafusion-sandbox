//! The progress samples table: one row per progress sample of a throughput probe.
//!
//! [`progress_samples_batch`] fills it from a probe's samples, and [`read_progress_samples`]
//! reads them back.

use super::{column_as, to_u64};
use crate::throughput_probe::ProgressSample;

use datafusion::{
    arrow::{
        array::{ArrayRef, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    common::cast::as_uint64_array,
    error::Result,
};

use std::sync::Arc;

/// The schema of the progress samples table: one row per progress sample of a throughput probe.
///
/// `sample_index` is the sample's position in the order the probe took them, from 0.
#[must_use]
pub(crate) fn progress_samples_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("sample_index", DataType::UInt64, false),
        Field::new("elapsed_ns", DataType::UInt64, false),
        Field::new("rows", DataType::UInt64, false),
    ]))
}

/// The batch of the progress samples table holding `samples`, in order, of the run `run_id`.
///
/// # Errors
///
/// Returns an error if the batch cannot be assembled.
pub(super) fn progress_samples_batch(
    run_id: &str,
    samples: &[ProgressSample],
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![run_id; samples.len()])),
        Arc::new(UInt64Array::from_iter_values(
            (0..samples.len()).map(to_u64),
        )),
        Arc::new(UInt64Array::from_iter_values(
            samples.iter().map(|sample| sample.elapsed_ns),
        )),
        Arc::new(UInt64Array::from_iter_values(
            samples.iter().map(|sample| sample.rows),
        )),
    ];
    Ok(RecordBatch::try_new(progress_samples_schema(), columns)?)
}

/// The progress samples in `batches` of the progress samples table, in their rows' order.
///
/// # Errors
///
/// Returns an error if a batch lacks the `elapsed_ns` or `rows` column, or holds one of another
/// type.
pub(super) fn read_progress_samples(batches: &[RecordBatch]) -> Result<Vec<ProgressSample>> {
    let mut samples = Vec::new();
    for batch in batches {
        let elapsed = column_as(batch, "elapsed_ns", &DataType::UInt64)?;
        let rows = column_as(batch, "rows", &DataType::UInt64)?;
        samples.extend(
            as_uint64_array(&elapsed)?
                .values()
                .iter()
                .zip(as_uint64_array(&rows)?.values())
                .map(|(&elapsed_ns, &rows)| ProgressSample { elapsed_ns, rows }),
        );
    }
    Ok(samples)
}
