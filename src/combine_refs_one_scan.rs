//! The earlier reference combiner variant: reads every sample through one scan
//! and preserves one input partition per file for a sort-preserving merge.

use crate::{format::InputFormat, read};

use datafusion::{
    arrow::datatypes::DataType, datasource::listing::ListingOptions, error::Result, prelude::*,
};

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
pub async fn plan(
    ctx: &SessionContext,
    table_path: &str,
    input_format: InputFormat,
) -> Result<DataFrame> {
    let locus_ordering = vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
    ];
    let listing_options = ListingOptions::new(input_format.read_format())
        .with_file_sort_order(vec![locus_ordering.clone()])
        .with_table_partition_cols(vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ]);
    let df = read(ctx, table_path, listing_options, None).await?;

    df.sort(locus_ordering)
}
