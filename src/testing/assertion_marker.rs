//! Builds the marker of an ordering assertion as a frontend lowers it, so a suite can place one in
//! a plan in Rust.
//!
//! The strings here are the marker's wire form, written out rather than shared with the backend's
//! reading of them, so a suite using this adapter holds that form in place.

use crate::{ordering_assertion, row_ordering::SortColumn};

use datafusion::{
    common::ScalarValue,
    logical_expr::Expr,
    prelude::{DataFrame, col},
};

/// `frame` with the marker asserting `ordering` above it.
pub(crate) fn asserted(frame: DataFrame, ordering: &[SortColumn]) -> DataFrame {
    frame.filter(marker(ordering)).unwrap()
}

/// The marker's predicate asserting `ordering`: each column followed by its direction as a string
/// literal, which the wheel's `lit` writes as a `Utf8View`.
pub(crate) fn marker(ordering: &[SortColumn]) -> Expr {
    ordering_assertion::assert_ordered_udf().call(
        ordering
            .iter()
            .flat_map(|column| [col(column.column.as_str()), direction(column)])
            .collect(),
    )
}

/// A string literal as the wheel's `lit` writes it.
pub(crate) fn wheel_literal(value: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8View(Some(value.to_string())), None)
}

fn direction(column: &SortColumn) -> Expr {
    wheel_literal(match (column.ascending, column.nulls_first) {
        (true, false) => "ASC NULLS LAST",
        (true, true) => "ASC NULLS FIRST",
        (false, false) => "DESC NULLS LAST",
        (false, true) => "DESC NULLS FIRST",
    })
}
