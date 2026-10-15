use crate::sample::{check_sample_column, sample_field};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

#[test]
fn the_sample_field_is_a_non_null_view_string_named_s() {
    let field = sample_field();

    assert_eq!(field.name(), "s");
    assert_eq!(field.data_type(), &DataType::Utf8View);
    assert!(!field.is_nullable());
}

fn schema_with(field: Field) -> Schema {
    Schema::new(vec![Field::new("x", DataType::Int32, false), field])
}

#[test]
fn a_schema_with_a_non_null_string_sample_column_passes_the_check() {
    for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let schema = schema_with(Field::new("s", data_type, false));

        assert_eq!(check_sample_column(&schema), Ok(()));
    }
}

#[test]
fn a_schema_without_a_sample_column_fails_the_check() {
    let schema = schema_with(Field::new("t", DataType::Utf8, false));

    assert_eq!(
        check_sample_column(&schema),
        Err("has no column 's'; it needs a non-null string column 's'".to_string())
    );
}

#[test]
fn a_schema_with_a_nullable_sample_column_fails_the_check() {
    let schema = schema_with(Field::new("s", DataType::Utf8, true));

    assert_eq!(
        check_sample_column(&schema),
        Err("has a nullable column 's'; it needs a non-null string column 's'".to_string())
    );
}

#[test]
fn a_schema_with_a_non_string_sample_column_fails_the_check() {
    let schema = schema_with(Field::new("s", DataType::Int64, false));

    assert_eq!(
        check_sample_column(&schema),
        Err("has a column 's' of type Int64; it needs a non-null string column 's'".to_string())
    );
}
