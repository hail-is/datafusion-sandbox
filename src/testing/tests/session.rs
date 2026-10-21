//! The hostile session config, which lets the optimizer split a file scan of any size.

use crate::{
    locus::LocusRepresentation,
    pipeline,
    testing::{
        fixture::{self, FixtureFormat, block_on},
        plan_shape::PlanShape,
        session::hostile_config,
    },
};

use datafusion::{
    datasource::source::DataSourceExec,
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};
use std::{collections::BTreeMap, sync::Arc};

/// Every option `config` sets, by key.
fn options(config: &SessionConfig) -> BTreeMap<String, Option<String>> {
    config
        .options()
        .entries()
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect()
}

#[test]
fn hostile_config_is_the_shared_config_but_for_target_partitions_and_the_file_split_size() {
    let mut hostile = options(&hostile_config(5));
    let mut shared = options(&pipeline::session_config());

    // The shared target partitions default to the CPU count, which may equal the hostile one, so
    // the two keys are set aside rather than required to differ.
    for key in [
        "datafusion.execution.target_partitions",
        "datafusion.optimizer.repartition_file_min_size",
    ] {
        hostile.remove(key);
        shared.remove(key);
    }
    assert_eq!(hostile, shared);
    let config = hostile_config(5);
    assert_eq!(config.target_partitions(), 5);
    assert_eq!(config.options().optimizer.repartition_file_min_size, 0);
}

/// The scan of one of the fixture's Parquet files, a file of a few kilobytes.
fn scan_of_one_small_file() -> Arc<dyn ExecutionPlan> {
    let dataset = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let ctx = SessionContext::new();
    dataset.register(&ctx);
    block_on(async {
        let file = dataset.sample_files(fixture::SAMPLES[0]).await.remove(0);
        let path = format!(
            "{}{}",
            dataset.table_path().object_store().as_str(),
            file.location
        );
        ctx.read_parquet(path, ParquetReadOptions::default())
            .await?
            .create_physical_plan()
            .await
    })
    .unwrap()
}

/// The optimizer splits a scan where the plan lets it by asking the scan to repartition itself
/// under the session's options; a scan that declines stays as it is. The optimizer only asks
/// where it judges more partitions beneficial, which a scan of a few rows never is, so this asks
/// the scan directly.
#[test]
fn hostile_config_lets_a_small_file_scan_split_across_its_target_partitions() {
    let scan = scan_of_one_small_file();
    assert!(scan.is::<DataSourceExec>(), "{}", PlanShape::of(&scan));
    let shared = pipeline::session_config();
    let hostile = hostile_config(4);

    let declined = scan.repartitioned(4, shared.options()).unwrap();
    let split = scan.repartitioned(4, hostile.options()).unwrap().unwrap();

    assert!(declined.is_none());
    assert_eq!(split.output_partitioning().partition_count(), 4);
}
