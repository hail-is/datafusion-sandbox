//! The sample field: the column that carries each row's sample id, and the check that a stored
//! schema carries it.

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use std::sync::Arc;

/// The column every row read from a dataset carries its sample id in: a non-null `s` of the view
/// string type, which is how both formats read a stored string column back.
#[must_use]
pub fn sample_field() -> FieldRef {
    Arc::new(Field::new("s", DataType::Utf8View, false))
}

/// Checks that `schema` has a non-null string `s`, of any of Arrow's string types.
///
/// # Errors
///
/// Returns a description of the problem, ending with what the column needs to be, if `schema` has
/// no `s`, or its `s` is nullable or not a string. Callers prefix it with what they checked.
pub fn check_sample_column(schema: &Schema) -> std::result::Result<(), String> {
    let sample = sample_field();
    let problem = match schema.field_with_name(sample.name()) {
        Ok(field)
            if matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) && !field.is_nullable() =>
        {
            return Ok(());
        }
        Ok(field) if field.is_nullable() => {
            format!("has a nullable column '{}'", sample.name())
        }
        Ok(field) => format!(
            "has a column '{}' of type {}",
            sample.name(),
            field.data_type()
        ),
        Err(_) => format!("has no column '{}'", sample.name()),
    };
    Err(format!(
        "{problem}; it needs a non-null string column '{}'",
        sample.name()
    ))
}
