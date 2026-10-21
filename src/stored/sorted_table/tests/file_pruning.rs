use crate::stored::sorted_table::{AttachedScalar, file_pruning::FilePruning};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::{ColumnStatistics, ScalarValue, Statistics, stats::Precision},
    datasource::{listing::PartitionedFile, table_schema::TableSchema},
    logical_expr::Expr,
    prelude::{SessionContext, col, lit},
};
use std::sync::Arc;

fn attached() -> AttachedScalar {
    AttachedScalar {
        field: Arc::new(Field::new("source", DataType::Utf8, true)),
        value: ScalarValue::Utf8(Some("NA18534".to_string())),
    }
}

/// `position` in the files, followed by the attached scalar's column if there is one.
fn table_schema(attached: Option<&AttachedScalar>) -> TableSchema {
    TableSchema::builder(Arc::new(Schema::new(vec![Field::new(
        "position",
        DataType::Int32,
        false,
    )])))
    .with_table_partition_cols(
        attached
            .iter()
            .map(|attached| Arc::clone(&attached.field))
            .collect::<Vec<_>>(),
    )
    .build()
}

fn int(value: i32) -> ScalarValue {
    ScalarValue::Int32(Some(value))
}

/// A one-row file whose `position` bounds are `min` and `max`.
fn file_with_bounds(
    path: &str,
    min: Precision<ScalarValue>,
    max: Precision<ScalarValue>,
) -> PartitionedFile {
    PartitionedFile::new(path, 8).with_statistics(Arc::new(Statistics {
        num_rows: Precision::Exact(1),
        total_byte_size: Precision::Absent,
        column_statistics: vec![ColumnStatistics {
            null_count: Precision::Exact(0),
            min_value: min,
            max_value: max,
            ..Default::default()
        }],
    }))
}

/// A one-row file with `position` exactly in `min..=max`.
fn file(path: &str, min: i32, max: i32) -> PartitionedFile {
    file_with_bounds(path, Precision::Exact(int(min)), Precision::Exact(int(max)))
}

fn pruning(attached: Option<&AttachedScalar>, filters: Vec<Expr>) -> Option<FilePruning> {
    FilePruning::new(
        &SessionContext::new().state(),
        &table_schema(attached),
        attached,
        filters,
    )
    .unwrap()
}

/// The paths of the files `filters` retain, in retained order.
fn retained(
    attached: Option<&AttachedScalar>,
    filters: Vec<Expr>,
    files: Vec<PartitionedFile>,
) -> Vec<String> {
    pruning(attached, filters)
        .expect("filters build a pruning")
        .retain(files)
        .unwrap()
        .iter()
        .map(|file| file.object_meta.location.to_string())
        .collect()
}

#[test]
fn no_filters_build_no_pruning() {
    assert!(pruning(None, vec![]).is_none());
    assert!(pruning(Some(&attached()), vec![]).is_none());
}

#[test]
fn exact_bounds_that_exclude_the_filter_remove_the_file_and_any_other_bounds_keep_it() {
    let files = vec![
        file("excluded", 1, 10),
        file("overlapping", 15, 30),
        file_with_bounds(
            "inexact",
            Precision::Inexact(int(1)),
            Precision::Inexact(int(10)),
        ),
        file_with_bounds("absent", Precision::Absent, Precision::Absent),
        PartitionedFile::new("no-statistics", 8),
    ];

    let retained = retained(None, vec![col("position").gt(lit(20))], files);

    assert_eq!(
        retained,
        ["overlapping", "inexact", "absent", "no-statistics"]
    );
}

#[test]
fn a_predicate_the_pruner_cannot_rewrite_keeps_every_file() {
    // No `position` in 1..=10 has a remainder of 100, but the pruner cannot reason about `%`.
    let filter = (col("position") % lit(7)).eq(lit(100));

    let retained = retained(
        None,
        vec![filter],
        vec![file("low", 1, 10), file("high", 11, 20)],
    );

    assert_eq!(retained, ["low", "high"]);
}

#[test]
fn the_attached_scalar_is_folded_into_the_predicate_as_a_literal() {
    let attached = attached();
    let files = || vec![file("low", 1, 10), file("high", 21, 30)];
    let filter = |source: &str| {
        col("source")
            .eq(lit(source))
            .or(col("position").gt(lit(20)))
    };

    // Folded, the other source's disjunct is false and only the position bound decides.
    assert_eq!(
        retained(Some(&attached), vec![filter("NA12878")], files()),
        ["high"]
    );
    // Folded, the file's own source makes the predicate true for every file.
    assert_eq!(
        retained(Some(&attached), vec![filter("NA18534")], files()),
        ["low", "high"]
    );
}

#[test]
fn retained_files_keep_their_input_order() {
    let files = vec![
        file("e", 41, 50),
        file("x", 1, 5),
        file("d", 21, 30),
        file("y", 6, 10),
        file("c", 31, 40),
    ];

    let retained = retained(None, vec![col("position").gt(lit(20))], files);

    assert_eq!(retained, ["e", "d", "c"]);
}

#[test]
fn every_filter_must_admit_a_retained_file() {
    let files = vec![
        file("low", 1, 4),
        file("middle", 8, 12),
        file("high", 25, 30),
    ];

    let retained = retained(
        None,
        vec![col("position").gt(lit(5)), col("position").lt(lit(20))],
        files,
    );

    assert_eq!(retained, ["middle"]);
}

#[test]
fn a_literal_of_another_type_is_compared_as_the_column_type() {
    for literal in [lit(20_i64), lit(20.5_f64), lit("20")] {
        let files = vec![file("low", 1, 10), file("high", 21, 30)];

        let retained = retained(None, vec![col("position").gt(literal.clone())], files);

        assert_eq!(retained, ["high"], "{literal}");
    }
}
