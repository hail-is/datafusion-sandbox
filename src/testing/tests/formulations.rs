//! Formulations and split points built from compact test notation.

use crate::{
    formulation::Formulation,
    locus::{Locus, SplitPoints},
    testing::formulations::{grouped_merge, interval_merge, split_points},
};

use std::num::NonZeroUsize;

#[test]
fn grouped_merge_has_the_given_number_of_groups() {
    for groups in [1, 3] {
        assert_eq!(
            grouped_merge(groups),
            Formulation::CombineRefsGroupedMerge {
                groups: NonZeroUsize::new(groups).unwrap()
            }
        );
    }
}

#[test]
#[should_panic(expected = "at least one group")]
fn grouped_merge_panics_on_zero_groups() {
    drop(grouped_merge(0));
}

#[test]
fn split_points_reads_comma_separated_contig_positions_in_order() {
    assert_eq!(
        split_points("1:100,2:50"),
        SplitPoints::new(vec![
            Locus::new(1, 100).unwrap(),
            Locus::new(2, 50).unwrap()
        ])
        .unwrap()
    );
}

#[test]
fn an_empty_string_is_no_split_points_and_so_one_locus_interval() {
    assert_eq!(split_points(""), SplitPoints::new(Vec::new()).unwrap());
    assert_eq!(split_points("").intervals().len(), 1);
}

#[test]
#[should_panic(expected = "test split points are valid")]
fn split_points_panics_on_points_that_do_not_increase() {
    drop(split_points("2:50,1:100"));
}

#[test]
#[should_panic(expected = "test split points are valid")]
fn split_points_panics_on_an_invalid_point() {
    drop(split_points("chr1:100"));
}

#[test]
fn interval_merge_splits_at_the_given_points() {
    for notation in ["", "1:100,2:50"] {
        assert_eq!(
            interval_merge(notation),
            Formulation::CombineRefsIntervalMerge {
                split_points: split_points(notation)
            },
            "{notation:?}"
        );
    }
}

#[test]
#[should_panic(expected = "test split points are valid")]
fn interval_merge_panics_on_invalid_split_points() {
    drop(interval_merge("1:100,1:100"));
}
