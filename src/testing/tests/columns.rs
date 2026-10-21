//! Reading a record batch's columns as plain values, and asserting a schema's columns.

use crate::testing::columns::{
    assert_columns, f64_values, string_values, timestamp_values, u64_values,
};

use datafusion::arrow::{
    array::{
        ArrayRef, Float64Array, Int64Array, StringArray, StringViewArray, TimestampNanosecondArray,
        UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use std::sync::Arc;

/// A batch of three rows holding one nullable column of each type the readers read, and a
/// non-nullable `Int64` column `n` none of them reads.
fn batch() -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "utf8",
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ),
        (
            "view",
            Arc::new(StringViewArray::from(vec![Some("x"), Some("y"), None])),
        ),
        (
            "u64",
            Arc::new(UInt64Array::from(vec![Some(1), None, Some(u64::MAX)])),
        ),
        (
            "f64",
            Arc::new(Float64Array::from(vec![None, Some(0.5), Some(-2.0)])),
        ),
        (
            "timestamp",
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(-1),
                Some(7),
                None,
            ])),
        ),
        ("n", Arc::new(Int64Array::from(vec![1, 2, 3]))),
    ];
    RecordBatch::try_from_iter_with_nullable(
        columns
            .into_iter()
            .map(|(name, column)| (name, column, name != "n")),
    )
    .unwrap()
}

#[test]
fn string_values_renders_plain_and_view_strings_alike_with_null_as_empty() {
    let batch = batch();

    assert_eq!(string_values(&batch, "utf8"), ["a", "", "c"]);
    assert_eq!(string_values(&batch, "view"), ["x", "y", ""]);
}

#[test]
fn the_typed_readers_read_every_value_in_row_order_null_included() {
    let batch = batch();

    assert_eq!(u64_values(&batch, "u64"), [Some(1), None, Some(u64::MAX)]);
    assert_eq!(f64_values(&batch, "f64"), [None, Some(0.5), Some(-2.0)]);
    assert_eq!(
        timestamp_values(&batch, "timestamp"),
        [Some(-1), Some(7), None]
    );
}

#[test]
#[should_panic(expected = "no column missing")]
fn string_values_panics_naming_a_missing_column() {
    drop(string_values(&batch(), "missing"));
}

#[test]
#[should_panic(expected = "no column missing")]
fn u64_values_panics_naming_a_missing_column() {
    drop(u64_values(&batch(), "missing"));
}

#[test]
#[should_panic(expected = "no column missing")]
fn f64_values_panics_naming_a_missing_column() {
    drop(f64_values(&batch(), "missing"));
}

#[test]
#[should_panic(expected = "no column missing")]
fn timestamp_values_panics_naming_a_missing_column() {
    drop(timestamp_values(&batch(), "missing"));
}

#[test]
#[should_panic(expected = "n is not a UInt64 column")]
fn u64_values_panics_on_a_column_of_another_type() {
    drop(u64_values(&batch(), "n"));
}

#[test]
#[should_panic(expected = "u64 is not a Float64 column")]
fn f64_values_panics_on_a_column_of_another_type() {
    drop(f64_values(&batch(), "u64"));
}

#[test]
#[should_panic(expected = "n is not a nanosecond timestamp column")]
fn timestamp_values_panics_on_a_column_of_another_type() {
    drop(timestamp_values(&batch(), "n"));
}

/// The schema `assert_columns` checks: a non-nullable string and a nullable timestamp.
fn schema() -> Schema {
    Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ])
}

#[test]
fn assert_columns_accepts_the_schemas_columns_in_order() {
    assert_columns(
        &schema(),
        &[
            ("name", DataType::Utf8, false),
            (
                "at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ],
    );
}

#[test]
#[should_panic(expected = "assertion")]
fn assert_columns_rejects_another_order() {
    assert_columns(
        &schema(),
        &[
            (
                "at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            ("name", DataType::Utf8, false),
        ],
    );
}

#[test]
#[should_panic(expected = "assertion")]
fn assert_columns_rejects_another_type() {
    assert_columns(
        &schema(),
        &[
            ("name", DataType::Utf8View, false),
            (
                "at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ],
    );
}

#[test]
#[should_panic(expected = "assertion")]
fn assert_columns_rejects_another_nullability() {
    assert_columns(
        &schema(),
        &[
            ("name", DataType::Utf8, true),
            (
                "at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ],
    );
}

#[test]
#[should_panic(expected = "assertion")]
fn assert_columns_rejects_a_missing_column() {
    assert_columns(&schema(), &[("name", DataType::Utf8, false)]);
}
