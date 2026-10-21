//! Readers of a record batch's columns as plain values, and an assertion on a schema's columns.

use datafusion::arrow::{
    array::{Float64Array, TimestampNanosecondArray, UInt64Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};

/// The values of the string column `name`, rendered, a null rendering as the empty string. A
/// string column read back from Parquet is a view array, so this renders rather than downcasts.
///
/// # Panics
///
/// Panics if `batch` has no column `name` or a value cannot be rendered.
#[must_use]
pub(crate) fn string_values(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    (0..column.len())
        .map(|row| array_value_to_string(column, row).expect("a rendered cell"))
        .collect()
}

/// The values of the `UInt64` column `name`, null included.
///
/// # Panics
///
/// Panics if `batch` has no column `name` or it is not a `UInt64` column.
#[must_use]
pub(crate) fn u64_values(batch: &RecordBatch, name: &str) -> Vec<Option<u64>> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| panic!("{name} is not a UInt64 column: {column:?}"))
        .iter()
        .collect()
}

/// The values of the `Float64` column `name`, null included.
///
/// # Panics
///
/// Panics if `batch` has no column `name` or it is not a `Float64` column.
#[must_use]
pub(crate) fn f64_values(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    column
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("{name} is not a Float64 column: {column:?}"))
        .iter()
        .collect()
}

/// Asserts that `schema` has exactly `expected` columns, in order, each with its name, type, and
/// nullability.
///
/// # Panics
///
/// Panics if the columns differ.
pub(crate) fn assert_columns(schema: &Schema, expected: &[(&str, DataType, bool)]) {
    let columns: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().as_str(),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect();
    assert_eq!(columns, expected);
}

/// The values of the nanosecond timestamp column `name`, null included.
///
/// # Panics
///
/// Panics if `batch` has no column `name` or it is not a nanosecond timestamp column.
#[must_use]
pub(crate) fn timestamp_values(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let column = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    column
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap_or_else(|| panic!("{name} is not a nanosecond timestamp column: {column:?}"))
        .iter()
        .collect()
}
