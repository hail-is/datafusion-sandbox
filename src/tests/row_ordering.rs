use crate::{
    locus::LocusRepresentation,
    row_ordering::{RowOrdering, SortColumn},
};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    common::DataFusionError,
    prelude::col,
};
use std::sync::Arc;

#[test]
fn accepts_an_exact_row_ordering_prefix() {
    assert!(RowOrdering::locus().is_prefix_of(&RowOrdering::locus()));
}

#[test]
fn accepts_a_finer_row_ordering_prefix() {
    assert!(RowOrdering::locus().is_prefix_of(&RowOrdering::locus_then_alleles()));
}

#[test]
fn rejects_an_insufficient_row_ordering_prefix() {
    assert!(!RowOrdering::locus_then_alleles().is_prefix_of(&RowOrdering::locus()));
}

#[test]
fn a_locus_then_sample_ordering_satisfies_a_locus_requirement_only() {
    let sample = RowOrdering::locus_then_sample();
    let alleles = RowOrdering::locus_then_alleles();

    assert!(RowOrdering::locus().is_prefix_of(&sample));
    assert!(!alleles.is_prefix_of(&sample));
    assert!(!sample.is_prefix_of(&alleles));
}

#[test]
fn expands_a_contig_position_locus_then_sample_ordering_with_the_sample_last() {
    assert_eq!(
        RowOrdering::locus_then_sample()
            .expand(LocusRepresentation::ContigPosition)
            .column_names(),
        ["contig", "position", "s"]
    );
}

#[test]
fn expands_a_contig_position_row_ordering() {
    assert_eq!(
        RowOrdering::locus()
            .expand(LocusRepresentation::ContigPosition)
            .sort_expressions(),
        vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ]
    );
}

#[test]
fn a_stored_ordering_is_a_plain_sort_list_ascending_with_nulls_last() {
    assert_eq!(
        RowOrdering::locus_then_alleles()
            .expand(LocusRepresentation::ContigPosition)
            .sort_columns(),
        ["contig", "position", "alleles"].map(|column| SortColumn {
            column: column.to_string(),
            ascending: true,
            nulls_first: false,
        })
    );
}

#[test]
fn a_sort_column_sorts_by_its_column_in_its_direction_and_null_placement() {
    let column = SortColumn {
        column: "position".to_string(),
        ascending: false,
        nulls_first: true,
    };

    assert_eq!(column.sort_expression(), col("position").sort(false, true));
}

#[test]
fn expands_a_packed_locus_then_alleles_ordering() {
    assert_eq!(
        RowOrdering::locus_then_alleles()
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
    let ordering = RowOrdering::locus_then_alleles().expand(LocusRepresentation::ContigPosition);

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
    let ordering = RowOrdering::locus_then_alleles().expand(LocusRepresentation::Packed);

    assert_eq!(ordering.column_names(), ["locus", "alleles"]);
    let locus_prefix = ordering.locus_prefix();
    assert_eq!(
        locus_prefix.sort_expressions(),
        vec![col("locus").sort(true, false)]
    );
    assert_eq!(locus_prefix.partition_expressions(), vec![col("locus")]);
}

#[test]
fn validating_a_row_ordering_detects_the_representation_and_expands_under_it() {
    let (representation, stored_ordering) = RowOrdering::locus_then_alleles()
        .validate_against(&schema(vec![
            Field::new("locus", DataType::Int64, false),
            Field::new("alleles", DataType::Utf8, false),
        ]))
        .unwrap();

    assert_eq!(representation, LocusRepresentation::Packed);
    assert_eq!(stored_ordering.column_names(), ["locus", "alleles"]);
}

#[test]
fn validating_a_row_ordering_rejects_a_schema_missing_an_ordering_column() {
    let error = RowOrdering::locus_then_alleles()
        .validate_against(&schema(vec![
            Field::new("contig", DataType::Utf8View, false),
            Field::new("position", DataType::Int32, false),
        ]))
        .expect_err("the schema has no alleles column");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("'alleles'"), "{error}");
}

#[test]
fn validating_a_row_ordering_surfaces_a_representation_error() {
    let error = RowOrdering::locus()
        .validate_against(&schema(vec![Field::new("alleles", DataType::Utf8, false)]))
        .expect_err("the schema has no locus representation");

    assert!(error.to_string().contains("neither"), "{error}");
}

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}
