//! A formulation's deferred rows and the requirements that travel with them.

use crate::locus::StoredOrdering;

use datafusion::{error::Result, prelude::DataFrame};

/// What a plan builder returns: deferred rows, the ordering a sink must require, and the output
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
}

/// How a write lays a formulation's rows out at the output path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputLayout {
    /// One file at the path, its rows in the sink's required order.
    SingleFile,
    /// A directory at the path holding one file per partition of the written frame, named by
    /// the partition's zero-padded index with the format's extension, each file's rows in the
    /// sink's required order. An empty partition writes an empty file.
    FilePerPartition,
}
