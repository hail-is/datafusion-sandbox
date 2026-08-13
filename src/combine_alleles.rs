use crate::{
    SAMPLES, VortexReadOptions, cpu_runtime::CpuRuntime, drain_join_set, read_vortex_with_schema,
    write_vortex,
};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::runtime::JoinSet,
    error::Result,
    execution::object_store::ObjectStoreUrl,
    functions_window::rank::rank,
    logical_expr::{LogicalPlan, logical_plan::Union},
    object_store::{client::SpawnedReqwestConnector, gcp::GoogleCloudStorageBuilder},
    prelude::*,
};

use tokio::runtime::Handle;

use std::sync::Arc;

// Combines the alleles of all samples under `table_path`, which is expected to contain one
// directory per sample, of the form "s=HG123456". Produces the distinct set of alleles at each
// locus, ranked within the locus.
pub async fn run(table_path: &str, output_path: &str, threads: usize) -> Result<()> {
    let table_path = table_path.trim_end_matches('/').to_string();
    let output_path = output_path.to_string();

    let cpu_runtime = CpuRuntime::try_new(threads)?;
    let io_handle = Handle::current();

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
        // Note: leaving the schema to be inferred infers Utf8View for "alleles", which runs into what
        // I suspect is a bug, the effect of which is the query planner doesn't think the input file groups
        // are sorted.
        let read_opts = VortexReadOptions {
            file_sort_order: vec![vec![
                col("contig").sort(true, false),
                col("position").sort(true, false),
                col("alleles").sort(true, false),
            ]],
            schema: Some(Arc::new(Schema::new(vec![
                Field::new("position", DataType::Int32, false),
                Field::new("alleles", DataType::Utf8, false),
            ]))),
            table_partition_cols: vec![("contig".to_string(), DataType::Utf8)],
        };

        let lps = SAMPLES
            .iter()
            .map(|s| {
                let df = read_vortex_with_schema(
                    &ctx,
                    format!("{}/s={}", &table_path, s),
                    read_opts.clone(),
                )?;
                Ok(Arc::new(df.into_unoptimized_plan()))
            })
            .collect::<Result<Vec<_>>>()?;
        let lp = LogicalPlan::Union(Union::try_new(lps)?);
        let df = DataFrame::new(ctx.state(), lp);
        let df = df.sort_by(vec![col("contig"), col("position"), col("alleles")])?;
        let df = df.distinct()?;
        let df = df.window(vec![
            rank()
                .order_by(vec![
                    col("contig").sort(true, false),
                    col("position").sort(true, false),
                    col("alleles").sort(true, false),
                ])
                .partition_by(vec![col("contig"), col("position")])
                .build()?,
        ])?;
        // df.limit(0, Some(100))?.show().await?;
        // df.explain(false, false)?.show().await?;
        write_vortex(df, &output_path, None).await?;
        Ok(()) as Result<()>
    };

    let mut join_set = JoinSet::new();
    join_set.spawn_on(driver_task, cpu_runtime.handle());
    drain_join_set(join_set).await;

    Ok(())
}
