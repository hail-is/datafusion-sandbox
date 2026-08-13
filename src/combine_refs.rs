use crate::{
    SAMPLES, VortexReadOptions, cpu_runtime::CpuRuntime, drain_join_set, read_vortex, write_vortex,
};

use datafusion::{
    arrow::datatypes::DataType,
    common::runtime::JoinSet,
    error::Result,
    execution::object_store::ObjectStoreUrl,
    logical_expr::{LogicalPlan, logical_plan::Union},
    object_store::{client::SpawnedReqwestConnector, gcp::GoogleCloudStorageBuilder},
    prelude::*,
};

use tokio::runtime::Handle;

use std::sync::Arc;

// Combines all vortex files in a directory. Assumes each file is a single sample, with the sample id
// provided by a parent directory of the form "s=HG123456". Assumes all files have the same schema.
// Reads each as a separate table, then unions.
//
// The generated physical plan still has a `SortPreservingMergeExec` doing the main work. The difference from combiner1
// is only that now many `DataSourceExec`s feed into a `UnionExec`, which still feeds `SortPreservingMergeExec` with one
// partition per input sample. Seems to have about the same performance.
pub async fn run(table_path: &str, output_path: &str, threads: usize) -> Result<()> {
    let table_path = table_path.to_string();
    let output_path = output_path.to_string();

    let cpu_runtime = CpuRuntime::try_new(threads)?;
    let io_handle = Handle::current();

    // Forces one partition per input scan. There will still be one partition per input going into the `SortPreservingMergeExec`.
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    let os = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name("hail-common")
        // Use the io_runtime to run the HTTP requests. Without this line,
        // you will see an error such as:
        // A Tokio 1.x context was found, but IO is disabled.
        .with_http_connector(SpawnedReqwestConnector::new(io_handle))
        .build()?;
    let url = ObjectStoreUrl::parse("gs://hail-common")?;
    ctx.register_object_store(url.as_ref(), Arc::new(os));

    let driver_task = async move {
        let read_opts = VortexReadOptions {
            file_sort_order: vec![vec![
                col("contig").sort(true, false),
                col("position").sort(true, false),
            ]],
            schema: None,
            table_partition_cols: vec![
                ("s".to_string(), DataType::Utf8),
                ("contig".to_string(), DataType::Utf8),
            ],
        };
        let df = read_vortex(&ctx, &table_path, read_opts).await?;

        let lps = SAMPLES
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
        let df = df.sort_by(vec![col("contig"), col("position")])?;
        // df.limit(50, Some(100))?.show().await?;
        // df.explain(true, false)?.show().await?;
        write_vortex(df, &output_path, None).await?;
        Ok(()) as Result<()>
    };

    let mut join_set = JoinSet::new();
    join_set.spawn_on(driver_task, cpu_runtime.handle());
    drain_join_set(join_set).await;

    Ok(())
}
