//! Run records for tests that need one but assert on none of its facts.

use crate::{
    metrics_directory::MetricsDirectory,
    pipeline::{self, PipelineOptions},
    testing::{
        columns::string_values, fixture, generated::make_range_table, memory_store::MemoryStore,
        run_records::run_record,
    },
};

#[test]
fn run_record_names_the_given_run() {
    for run_id in ["run-a", "another run"] {
        assert_eq!(run_record(run_id).run_id, run_id);
    }
}

/// The record is one a test can record: a metrics directory takes it for its run, and it reads
/// back as that run's record.
#[test]
fn a_metrics_directory_records_the_run_record_under_its_run_id() {
    let store = MemoryStore::new("run-records");
    let directory = MetricsDirectory::new(&format!("{}benchmarks", store.url().as_str()));

    let recorded = pipeline::run(
        move |ctx| async move {
            store.register(&ctx);
            let plan = make_range_table(&ctx, 10, 4)?
                .create_physical_plan()
                .await?;
            datafusion::physical_plan::collect(std::sync::Arc::clone(&plan), ctx.task_ctx())
                .await?;
            directory
                .unrecorded(&ctx, "run-a")
                .await?
                .record(&ctx, &run_record("run-a"), &plan)
                .await?;
            fixture::read_recorded_run(&ctx, &directory, "run-a").await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    let record = recorded.record.unwrap();
    assert_eq!(string_values(&record, "run_id"), ["run-a"]);
}
