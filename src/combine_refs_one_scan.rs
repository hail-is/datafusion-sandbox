//! The earlier reference combiner variant: reads every sample through one scan
//! and preserves one input partition per file for a sort-preserving merge.

use crate::{VortexReadOptions, read_vortex};

use datafusion::{arrow::datatypes::DataType, error::Result, prelude::*};

/// The session configuration this plan shape depends on. There must be at
/// least as many target partitions as input files, and file partitions must be
/// preserved, or the scan groups files into partitions whose locus ordering is
/// no longer known.
pub fn session_config() -> SessionConfig {
    let mut config = SessionConfig::new().with_target_partitions(50);
    config.options_mut().optimizer.preserve_file_partitions = 1;
    config
}

/// Builds the earlier reference combiner plan over every sample under
/// `table_path` in one shared scan.
pub async fn plan(ctx: &SessionContext, table_path: &str) -> Result<DataFrame> {
    let locus_ordering = vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
    ];
    let read_options = VortexReadOptions {
        file_sort_order: vec![locus_ordering.clone()],
        schema: None,
        table_partition_cols: vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ],
    };
    let df = read_vortex(ctx, table_path, read_options).await?;

    df.sort(locus_ordering)
}
