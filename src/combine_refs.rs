use crate::{VortexReadOptions, read_vortex};

use datafusion::{
    arrow::datatypes::DataType,
    error::Result,
    logical_expr::{LogicalPlan, SortExpr, logical_plan::Union},
    prelude::*,
};

use std::sync::Arc;

/// The locus ordering declared on the input files and requested in the query: contig, then
/// position. Declaring both identically is what keeps the plan mergeable rather than re-sorting.
fn locus_ordering() -> Vec<SortExpr> {
    vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
    ]
}

/// Builds the plan combining the reference data of all `samples` under `table_path`. Assumes
/// each file is a single sample, with the sample id provided by a parent directory of the form
/// "s=HG123456". Assumes all files have the same schema. Reads each as a separate table, then
/// unions.
///
/// The generated physical plan still has a `SortPreservingMergeExec` doing the main work. The
/// difference from combiner1 is only that now many `DataSourceExec`s feed into a `UnionExec`,
/// which still feeds `SortPreservingMergeExec` with one partition per input sample. Seems to
/// have about the same performance.
pub async fn plan(ctx: &SessionContext, table_path: &str, samples: &[&str]) -> Result<DataFrame> {
    let table_path = format!("{}/", table_path.trim_end_matches('/'));

    let read_opts = VortexReadOptions {
        file_sort_order: vec![locus_ordering()],
        schema: None,
        table_partition_cols: vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ],
    };
    let df = read_vortex(ctx, &table_path, read_opts).await?;

    let lps = samples
        .iter()
        .map(|s| {
            Ok(Arc::new(
                df.clone()
                    .filter(col("s").eq(lit(*s)))?
                    .into_unoptimized_plan(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let lp = LogicalPlan::Union(Union::try_new(lps)?);
    let df = DataFrame::new(ctx.state(), lp);
    df.sort(locus_ordering())
}
