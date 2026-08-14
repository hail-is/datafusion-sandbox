use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::*;
use datafusion_sandbox::pipeline::PlanBuilder;
use datafusion_sandbox::*;
use divan::Bencher;
use divan::counter::ItemsCount;
use futures_util::stream::StreamExt;
use std::sync::Arc;
use tokio::runtime::LocalRuntime as Runtime;

fn main() {
    // Run registered benchmarks.
    divan::main();
}

fn deep_copy_plan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let new_children: Vec<Arc<dyn ExecutionPlan>> =
        plan.children().into_iter().map(deep_copy_plan).collect();
    Arc::clone(plan).with_new_children(new_children).unwrap()
}

fn session_context() -> SessionContext {
    // run benchmarks with single core
    let config = SessionConfig::new().with_target_partitions(1);
    SessionContext::new_with_config(config)
}

const BATCH_SIZES: &[u32] = &[1, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192];
const N_ROWS: u32 = 2 ^ 18;

fn run_benchmark(bencher: Bencher, plan_builder: impl PlanBuilder) {
    // using a LocalRuntime, again because we're running with single core
    let rt = Runtime::new().unwrap();
    let ctx = session_context();

    let physical_plan = rt.block_on(async {
        plan_builder
            .build(ctx.clone())
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    });

    bencher
        // physical plans store execution state, so we need a fresh copy for each run
        .with_inputs(|| deep_copy_plan(&physical_plan))
        .bench_local_values(|plan| -> usize {
            rt.block_on(async {
                // only counts the number of record batches, but I think this is enough to prevent optimizing away any of the pipeline
                execute_stream(plan, ctx.task_ctx()).unwrap().count().await
            })
        });
}

#[divan::bench(args = BATCH_SIZES, counter = ItemsCount::new(N_ROWS))]
fn range_table(bencher: Bencher, batch_size: u32) {
    run_benchmark(bencher, move |ctx: SessionContext| async move {
        make_range_table(&ctx, N_ROWS, batch_size)
    });
}

#[divan::bench(args = BATCH_SIZES, counter = ItemsCount::new(N_ROWS))]
fn table_group_by_aggregate_sorted(bencher: Bencher, batch_size: u32) {
    run_benchmark(bencher, move |ctx: SessionContext| async move {
        make_table_group_by_aggregate_sorted(&ctx, batch_size, N_ROWS)
    });
}
