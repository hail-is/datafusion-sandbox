use crate::{
    row_ordering::SortColumn,
    testing::assertion_marker::{asserted, marker, wheel_literal},
};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::ScalarValue,
    datasource::MemTable,
    logical_expr::{Expr, LogicalPlan},
    prelude::SessionContext,
};
use std::sync::Arc;

fn sort_column(column: &str, ascending: bool, nulls_first: bool) -> SortColumn {
    SortColumn {
        column: column.to_string(),
        ascending,
        nulls_first,
    }
}

#[test]
fn a_marker_alternates_each_column_with_its_direction() {
    let marker = marker(&[
        sort_column("a", true, false),
        sort_column("b", true, true),
        sort_column("c", false, false),
        sort_column("d", false, true),
    ]);

    assert_eq!(
        marker.to_string(),
        "assert_ordered(a, Utf8View(\"ASC NULLS LAST\"), b, Utf8View(\"ASC NULLS FIRST\"), c, \
         Utf8View(\"DESC NULLS LAST\"), d, Utf8View(\"DESC NULLS FIRST\"))"
    );
}

#[test]
fn a_literal_is_a_utf8_view_as_the_wheel_writes_it() {
    assert_eq!(
        wheel_literal("x"),
        Expr::Literal(ScalarValue::Utf8View(Some("x".to_string())), None)
    );
}

#[test]
fn an_asserted_frame_is_its_frame_beneath_a_filter_of_the_marker() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let table = MemTable::try_new(schema, vec![vec![]]).unwrap();
    let frame = SessionContext::new().read_table(Arc::new(table)).unwrap();
    let ordering = [sort_column("a", true, false)];

    let plan = asserted(frame, &ordering).into_unoptimized_plan();

    let LogicalPlan::Filter(filter) = &plan else {
        panic!("expected a filter:\n{plan}");
    };
    assert_eq!(
        filter.predicate.to_string(),
        "assert_ordered(?table?.a, Utf8View(\"ASC NULLS LAST\"))"
    );
    assert!(matches!(filter.input.as_ref(), LogicalPlan::TableScan(_)));
}
