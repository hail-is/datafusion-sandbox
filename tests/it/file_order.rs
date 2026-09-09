use datafusion::common::{ScalarValue, stats::Precision};
use datafusion_sandbox::file_order::{Bounds, Direction, FileOrderError, recover_file_order};

fn exact(min: i32, max: i32) -> Bounds {
    Bounds {
        min: Precision::Exact(ScalarValue::Int32(Some(min))),
        max: Precision::Exact(ScalarValue::Int32(Some(max))),
    }
}

fn ascending(columns: usize) -> Vec<Direction> {
    vec![Direction::Ascending; columns]
}

/// Sorting by the row of per-column minimums puts the file holding `(1, 5), (2, 0)`, whose
/// composed minimum is `(1, 0)`, before the file constant at `1` with minimum `(1, 1)`. Every
/// sorted layout has the constant file first.
#[test]
fn a_file_spanning_the_leading_value_follows_the_files_constant_on_it() {
    let files = vec![
        vec![exact(1, 2), exact(0, 5)],
        vec![exact(1, 1), exact(1, 3)],
        vec![exact(3, 3), exact(0, 0)],
    ];

    let order = recover_file_order(&files, &ascending(2)).unwrap();

    assert_eq!(order, vec![1, 0, 2]);
}

#[test]
fn a_minimum_above_its_maximum_is_unusable() {
    let files = vec![vec![exact(5, 1)], vec![exact(6, 9)]];

    let error = recover_file_order(&files, &ascending(1)).unwrap_err();

    assert_eq!(
        error,
        FileOrderError::UnusableStatistics { file: 0, column: 0 }
    );
}

#[test]
fn files_constant_on_the_leading_value_are_ordered_by_the_next_column_before_the_one_extending_past_it()
 {
    let files = vec![
        vec![exact(7, 9), exact(0, 100)],
        vec![exact(7, 7), exact(30, 40)],
        vec![exact(7, 7), exact(10, 20)],
    ];

    let order = recover_file_order(&files, &ascending(2)).unwrap();

    assert_eq!(order, vec![2, 1, 0]);
}

#[test]
fn two_files_extending_past_a_shared_leading_value_refute_the_order_naming_both() {
    let files = vec![
        vec![exact(7, 7), exact(1, 1)],
        vec![exact(7, 8), exact(1, 1)],
        vec![exact(7, 9), exact(1, 1)],
    ];

    let error = recover_file_order(&files, &ascending(2)).unwrap_err();

    assert_eq!(
        error,
        FileOrderError::Refuted {
            first: 1,
            second: 2
        }
    );
}

#[test]
fn overlapping_leading_columns_refute_the_order_naming_both_files() {
    let files = vec![vec![exact(1, 10)], vec![exact(5, 15)]];

    let error = recover_file_order(&files, &ascending(1)).unwrap_err();

    assert_eq!(
        error,
        FileOrderError::Refuted {
            first: 0,
            second: 1
        }
    );
}

#[test]
fn identical_constant_files_are_accepted_in_either_order() {
    let files = vec![
        vec![exact(3, 3), exact(1, 1)],
        vec![exact(3, 3), exact(1, 1)],
    ];

    let order = recover_file_order(&files, &ascending(2)).unwrap();

    assert!(order == vec![0, 1] || order == vec![1, 0], "{order:?}");
}

#[test]
fn touching_ranges_on_the_leading_column_are_accepted() {
    let files = vec![vec![exact(10, 20)], vec![exact(1, 10)]];

    let order = recover_file_order(&files, &ascending(1)).unwrap();

    assert_eq!(order, vec![1, 0]);
}

#[test]
fn a_descending_column_swaps_the_roles_of_the_bounds() {
    let files = vec![vec![exact(1, 4)], vec![exact(5, 9)]];

    let order = recover_file_order(&files, &[Direction::Descending]).unwrap();

    assert_eq!(order, vec![1, 0]);
}

#[test]
fn directions_apply_per_column() {
    let files = vec![
        vec![exact(1, 1), exact(1, 2)],
        vec![exact(1, 1), exact(3, 4)],
        vec![exact(0, 0), exact(9, 9)],
    ];

    let order = recover_file_order(&files, &[Direction::Ascending, Direction::Descending]).unwrap();

    assert_eq!(order, vec![2, 1, 0]);
}

#[test]
fn an_inexact_bound_on_a_compared_column_is_rejected_naming_the_file_and_column() {
    let files = vec![
        vec![exact(1, 1), exact(1, 2)],
        vec![
            exact(1, 1),
            Bounds {
                min: Precision::Exact(ScalarValue::Int32(Some(3))),
                max: Precision::Inexact(ScalarValue::Int32(Some(4))),
            },
        ],
    ];

    let error = recover_file_order(&files, &ascending(2)).unwrap_err();

    assert_eq!(
        error,
        FileOrderError::UnusableStatistics { file: 1, column: 1 }
    );
}

#[test]
fn an_inexact_bound_on_a_column_never_compared_is_ignored() {
    let files = vec![
        vec![
            exact(1, 2),
            Bounds {
                min: Precision::Inexact(ScalarValue::Int32(Some(0))),
                max: Precision::Absent,
            },
        ],
        vec![
            exact(3, 4),
            Bounds {
                min: Precision::Absent,
                max: Precision::Absent,
            },
        ],
    ];

    let order = recover_file_order(&files, &ascending(2)).unwrap();

    assert_eq!(order, vec![0, 1]);
}
