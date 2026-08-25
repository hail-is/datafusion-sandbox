//! The one-shared-scan reference combiner formulation preserves one input
//! partition per file for a sort-preserving merge.

use crate::{Dataset, derived_session, read};

use datafusion::{
    arrow::datatypes::DataType, datasource::listing::ListingOptions, error::Result, prelude::*,
};

/// Builds the one-shared-scan formulation under the session its plan shape
/// depends on.
pub async fn plan(ctx: &SessionContext, dataset: &Dataset) -> Result<DataFrame> {
    let sample_count = dataset.sample_set().len();
    let ctx = derived_session(ctx, |options| {
        options.execution.target_partitions = sample_count;
        options.optimizer.preserve_file_partitions = 1;
    });
    let locus_ordering = vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
    ];
    let listing_options = ListingOptions::new(dataset.input_format().read_format())
        .with_file_sort_order(vec![locus_ordering.clone()])
        .with_table_partition_cols(vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ]);
    let df = read(&ctx, dataset.table_path().as_str(), listing_options, None)
        .await?
        .filter(
            col("s").in_list(
                dataset
                    .sample_set()
                    .iter()
                    .map(|sample| lit(sample.as_str()))
                    .collect(),
                false,
            ),
        )?;

    df.sort(locus_ordering)
}
