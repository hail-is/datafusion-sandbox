//! A session config hostile to plans that rely on a scan's partitioning.

use crate::pipeline;
use datafusion::prelude::SessionConfig;

/// The shared session settings, plus permission to split a file scan of any size across
/// `target_partitions` partitions wherever the plan lets the optimizer do so.
#[must_use]
pub(crate) fn hostile_config(target_partitions: usize) -> SessionConfig {
    let mut config = pipeline::session_config().with_target_partitions(target_partitions);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    config
}
