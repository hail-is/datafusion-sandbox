//! Recovers the order of a sorted table's files from their ordering statistics.
//!
//! The files are assumed to hold one sorted table. Per ordering column, two files compare as
//! intervals: a file is at or before another when its trailing bound is at most the other's
//! leading bound, two files are equivalent when that holds both ways (so both are constant at
//! the same value), and otherwise they are incomparable. The file order is the lexicographic
//! order over the ordering columns built from that comparison, and an incomparable pair at any
//! column refutes the assumption. See ADR 0011.
//!
//! This module knows nothing about files, paths, or formats. Files are indices into the caller's
//! list, and the caller turns errors into its own.

use datafusion::common::{ScalarValue, stats::Precision};
use std::{cmp::Ordering, error::Error, fmt};

/// One file's minimum and maximum on one ordering column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bounds {
    pub min: Precision<ScalarValue>,
    pub max: Precision<ScalarValue>,
}

/// The direction of one ordering column. The nulls-first flag plays no role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    Ascending,
    Descending,
}

/// Why the files' statistics did not yield an order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileOrderError {
    /// A bound the algorithm had to compare was absent, inexact, or null, the file's minimum lay
    /// above its maximum, or its bounds had no order against another file's because their types
    /// disagree.
    UnusableStatistics { file: usize, column: usize },
    /// Neither file can be placed at or before the other, so no sorted order exists.
    Refuted { first: usize, second: usize },
}

impl fmt::Display for FileOrderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnusableStatistics { file, column } => write!(
                formatter,
                "file {file} has no exact bounds on ordering column {column}"
            ),
            Self::Refuted { first, second } => write!(
                formatter,
                "files {first} and {second} cannot both be placed before the other"
            ),
        }
    }
}

impl Error for FileOrderError {}

/// Recovers the only file order compatible with the files holding one sorted table.
///
/// `files` holds, per file, one [`Bounds`] per ordering column, and `directions` holds one
/// direction per ordering column. The result is a permutation of file indices. Files that are
/// equivalent on every column may appear in either order.
///
/// A file's bounds on a column are compared only when the file is constant on every earlier
/// column, and only compared bounds must be exact. The leading column is compared for every
/// file.
///
/// # Errors
///
/// Returns [`FileOrderError::UnusableStatistics`] when a compared bound is absent, inexact, or
/// null, and [`FileOrderError::Refuted`] when two files cannot both be placed before the other.
pub fn recover_file_order(
    files: &[Vec<Bounds>],
    directions: &[Direction],
) -> Result<Vec<usize>, FileOrderError> {
    let keys = files
        .iter()
        .enumerate()
        .map(|(file, bounds)| sort_key(file, bounds, directions))
        .collect::<Result<Vec<_>, _>>()?;

    // The interval comparison is only a total order when a sorted order exists, and the standard
    // sort may panic on a comparator that is not one. So sort by a total proxy key and verify
    // each adjacent pair afterwards. Transitivity of the interval relation makes a consistent
    // chain of adjacent pairs a consistent whole.
    let key_of = |file: usize| keys.get(file).map_or_default(Vec::as_slice);
    let mut order: Vec<usize> = (0..files.len()).collect();
    order.sort_by(|&a, &b| compare_keys(key_of(a), key_of(b)));

    for (&first, &second) in order.iter().zip(order.iter().skip(1)) {
        check_adjacent(first, key_of(first), second, key_of(second))?;
    }
    Ok(order)
}

impl Direction {
    /// Compares two values in this direction. `None` means the values have no order, which only
    /// happens when their types disagree.
    fn compare(self, a: &ScalarValue, b: &ScalarValue) -> Option<Ordering> {
        let ordering = a.partial_cmp(b)?;
        Some(match self {
            Self::Ascending => ordering,
            Self::Descending => ordering.reverse(),
        })
    }
}

/// One file's bounds on one column, oriented so that `lead` is the edge the ordering reaches
/// first. For an ascending column that is the minimum, for a descending column the maximum.
#[derive(Debug)]
struct Interval<'a> {
    lead: &'a ScalarValue,
    trail: &'a ScalarValue,
    direction: Direction,
}

impl Interval<'_> {
    fn is_constant(&self) -> bool {
        self.lead == self.trail
    }
}

/// The intervals a file is placed by: each column's bounds up to and including the first
/// column the file is not constant on. Exactness is required exactly for these bounds.
fn sort_key<'a>(
    file: usize,
    bounds: &'a [Bounds],
    directions: &[Direction],
) -> Result<Vec<Interval<'a>>, FileOrderError> {
    let mut key = Vec::new();
    for (column, &direction) in directions.iter().enumerate() {
        let interval = bounds
            .get(column)
            .and_then(|bounds| usable_interval(bounds, direction))
            .ok_or(FileOrderError::UnusableStatistics { file, column })?;
        let constant = interval.is_constant();
        key.push(interval);
        if !constant {
            break;
        }
    }
    Ok(key)
}

fn usable_interval(bounds: &Bounds, direction: Direction) -> Option<Interval<'_>> {
    let (Precision::Exact(min), Precision::Exact(max)) = (&bounds.min, &bounds.max) else {
        return None;
    };
    if min.is_null() || max.is_null() || min.partial_cmp(max)? == Ordering::Greater {
        return None;
    }
    let (lead, trail) = match direction {
        Direction::Ascending => (min, max),
        Direction::Descending => (max, min),
    };
    Some(Interval {
        lead,
        trail,
        direction,
    })
}

/// Lexicographic order over `(lead, trail)` per column. Because a key stops at the first
/// non-constant column, two keys that agree on a prefix stop at the same column, so this is a
/// total preorder. Values with no order compare equal here and are caught by the adjacent check.
fn compare_keys(a: &[Interval<'_>], b: &[Interval<'_>]) -> Ordering {
    for (interval_a, interval_b) in a.iter().zip(b) {
        let direction = interval_a.direction;
        let ordering = direction
            .compare(interval_a.lead, interval_b.lead)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                direction
                    .compare(interval_a.trail, interval_b.trail)
                    .unwrap_or(Ordering::Equal)
            });
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    a.len().cmp(&b.len())
}

enum Relation {
    Before,
    Equivalent,
    Incomparable,
}

/// The three-way interval comparison on one column.
fn relate(a: &Interval<'_>, b: &Interval<'_>) -> Option<Relation> {
    // Proof mode: change both `is_le` to `is_lt`. A constant file next to a non-constant one at
    // the same value, and two touching ranges, then become incomparable, and the recovered
    // order is again the proof ADR 0007 described.
    let a_before_b = a.direction.compare(a.trail, b.lead)?.is_le();
    let b_before_a = a.direction.compare(b.trail, a.lead)?.is_le();
    Some(match (a_before_b, b_before_a) {
        (true, true) => Relation::Equivalent,
        (true, false) => Relation::Before,
        (false, _) => Relation::Incomparable,
    })
}

/// Checks that `first` may be placed at or before `second`, column by column. Equivalent on
/// every column means two identical single-key files, accepted in either order.
fn check_adjacent(
    first: usize,
    key_first: &[Interval<'_>],
    second: usize,
    key_second: &[Interval<'_>],
) -> Result<(), FileOrderError> {
    for (column, (interval_first, interval_second)) in key_first.iter().zip(key_second).enumerate()
    {
        let relation =
            relate(interval_first, interval_second).ok_or(FileOrderError::UnusableStatistics {
                file: second,
                column,
            })?;
        match relation {
            Relation::Before => return Ok(()),
            Relation::Equivalent => {}
            Relation::Incomparable => return Err(FileOrderError::Refuted { first, second }),
        }
    }
    Ok(())
}
