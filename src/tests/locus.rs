use crate::locus::{LocusOrdering, LocusRepresentation};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    common::DataFusionError,
    prelude::col,
};
use std::sync::Arc;

#[test]
fn accepts_an_exact_locus_ordering_prefix() {
    assert!(LocusOrdering::locus().is_prefix_of(&LocusOrdering::locus()));
}

#[test]
fn accepts_a_finer_locus_ordering_prefix() {
    assert!(LocusOrdering::locus().is_prefix_of(&LocusOrdering::locus_then_alleles()));
}

#[test]
fn rejects_an_insufficient_locus_ordering_prefix() {
    assert!(!LocusOrdering::locus_then_alleles().is_prefix_of(&LocusOrdering::locus()));
}

#[test]
fn expands_a_contig_position_locus_ordering() {
    assert_eq!(
        LocusOrdering::locus()
            .expand(LocusRepresentation::ContigPosition)
            .sort_expressions(),
        vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ]
    );
}

#[test]
fn expands_a_packed_locus_then_alleles_ordering() {
    assert_eq!(
        LocusOrdering::locus_then_alleles()
            .expand(LocusRepresentation::Packed)
            .sort_expressions(),
        vec![
            col("locus").sort(true, false),
            col("alleles").sort(true, false),
        ]
    );
}

#[test]
fn contig_position_stored_ordering_exposes_its_columns_and_locus_prefix() {
    let ordering = LocusOrdering::locus_then_alleles().expand(LocusRepresentation::ContigPosition);

    assert_eq!(ordering.column_names(), ["contig", "position", "alleles"]);
    let locus_prefix = ordering.locus_prefix();
    assert_eq!(
        locus_prefix.sort_expressions(),
        vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ]
    );
    assert_eq!(
        locus_prefix.partition_expressions(),
        vec![col("contig"), col("position")]
    );
}

#[test]
fn packed_stored_ordering_exposes_its_columns_and_locus_prefix() {
    let ordering = LocusOrdering::locus_then_alleles().expand(LocusRepresentation::Packed);

    assert_eq!(ordering.column_names(), ["locus", "alleles"]);
    let locus_prefix = ordering.locus_prefix();
    assert_eq!(
        locus_prefix.sort_expressions(),
        vec![col("locus").sort(true, false)]
    );
    assert_eq!(locus_prefix.partition_expressions(), vec![col("locus")]);
}

#[test]
fn rejects_contig_without_position_as_a_locus_representation() {
    let error =
        LocusRepresentation::detect(&schema(vec![Field::new("contig", DataType::Utf8, false)]))
            .expect_err("contig-position requires both fields");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("position"));
}

#[test]
fn detects_a_contig_position_locus_representation() {
    let representation = LocusRepresentation::detect(&schema(vec![
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
    ]))
    .unwrap();

    assert_eq!(representation, LocusRepresentation::ContigPosition);
}

#[test]
fn detects_an_int64_packed_locus_representation() {
    let representation =
        LocusRepresentation::detect(&schema(vec![Field::new("locus", DataType::Int64, false)]))
            .unwrap();

    assert_eq!(representation, LocusRepresentation::Packed);
}

#[test]
fn rejects_both_locus_representations() {
    let error = LocusRepresentation::detect(&schema(vec![
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
        Field::new("locus", DataType::Int64, false),
    ]))
    .expect_err("a schema must have exactly one locus representation");

    assert!(matches!(error, DataFusionError::Plan(_)));
    let message = error.to_string();
    assert!(message.contains("both"), "unexpected error: {message}");
    assert!(message.contains("contig"), "unexpected error: {message}");
    assert!(message.contains("locus"), "unexpected error: {message}");
}

#[test]
fn rejects_neither_locus_representation() {
    let error =
        LocusRepresentation::detect(&schema(vec![Field::new("alleles", DataType::Utf8, false)]))
            .expect_err("a schema must have one locus representation");

    assert!(matches!(error, DataFusionError::Plan(_)));
    let message = error.to_string();
    assert!(message.contains("neither"), "unexpected error: {message}");
    assert!(message.contains("contig"), "unexpected error: {message}");
    assert!(message.contains("locus"), "unexpected error: {message}");
}

#[test]
fn rejects_a_non_int64_packed_locus_representation() {
    let error =
        LocusRepresentation::detect(&schema(vec![Field::new("locus", DataType::Utf8, false)]))
            .expect_err("a packed locus must be Int64");

    assert!(matches!(error, DataFusionError::Plan(_)));
    let message = error.to_string();
    assert!(message.contains("locus"), "unexpected error: {message}");
    assert!(message.contains("Int64"), "unexpected error: {message}");
    assert!(message.contains("Utf8"), "unexpected error: {message}");
}

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}
