//! A formulation's deferred rows and the requirements that travel with them.

use crate::{
    ordering_assertion::{self, AssertionScope},
    row_ordering::{SortColumn, StoredOrdering},
};

use datafusion::{error::Result, prelude::DataFrame};

/// What a plan builder returns: deferred rows, the ordering a run holds them in, and the output
/// layout a write gives them.
#[derive(Debug)]
pub struct OrderedFrame {
    pub frame: DataFrame,
    pub ordering: StoredOrdering,
    pub layout: OutputLayout,
}

impl OrderedFrame {
    /// Applies a row limit while keeping the ordering and output layout.
    ///
    /// # Errors
    ///
    /// Returns an error if `DataFusion` cannot build the limit plan.
    pub fn limit(self, n: usize) -> Result<Self> {
        Ok(Self {
            frame: self.frame.limit(0, Some(n))?,
            ordering: self.ordering,
            layout: self.layout,
        })
    }

    /// The frame held in the ordering over `scope` by an ordering assertion at its root, with
    /// every sort the plan builder wrote an established order, and the ordering as the plain sort
    /// list a sink records. See [`ordering_assertion::hold`].
    ///
    /// # Errors
    ///
    /// Returns an error if the assertion cannot be placed.
    pub fn hold(self, scope: AssertionScope) -> Result<(DataFrame, Vec<SortColumn>)> {
        let ordering = self.ordering.sort_columns();
        let frame = ordering_assertion::hold(self.frame, &ordering, scope)?;
        Ok((frame, ordering))
    }
}

/// How a write lays a formulation's rows out at the output path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputLayout {
    /// One file at the path, its rows in the frame's ordering.
    SingleFile,
    /// A directory at the path holding one file per partition of the written frame, named by
    /// the partition's zero-padded index with the format's extension, each file's rows in the
    /// frame's ordering. An empty partition writes an empty file.
    FilePerPartition,
}

impl OutputLayout {
    /// Where a write in this layout holds the frame's ordering: over the one partition its file
    /// is written from, or within each partition, each written to its own file.
    #[must_use]
    pub const fn assertion_scope(self) -> AssertionScope {
        match self {
            Self::SingleFile => AssertionScope::OnePartition,
            Self::FilePerPartition => AssertionScope::EachPartition,
        }
    }
}
