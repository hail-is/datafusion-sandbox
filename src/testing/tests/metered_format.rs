use crate::locus::LocusRepresentation;
use crate::testing::{
    fixture::{self, FixtureFormat, block_on},
    memory_store::MemoryStore,
    metered_format::{Gate, MeteredFormat},
};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    common::{Statistics, stats::Precision},
    datasource::{
        file_format::{FileFormat, FileMeta, parquet::ParquetFormat},
        listing::PartitionedFile,
    },
    execution::SessionState,
    prelude::SessionContext,
};
use futures::future::join_all;
use object_store::ObjectMeta;
use std::{sync::Arc, time::Duration};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "position",
        DataType::Int32,
        false,
    )]))
}

fn object(path: &str) -> ObjectMeta {
    PartitionedFile::new(path, 8).object_meta
}

/// Statistics reporting `rows` rows, so a test can tell which script answered.
fn rows(rows: usize) -> Statistics {
    Statistics {
        num_rows: Precision::Exact(rows),
        ..Statistics::new_unknown(&schema())
    }
}

fn script(path: &str, statistics: Statistics) -> (&str, FileMeta) {
    (path, FileMeta::new(statistics))
}

/// The statistics `format` infers for `path` against an empty store.
async fn infer(format: &MeteredFormat, state: &SessionState, path: &str) -> Statistics {
    let store = MemoryStore::new("metered-format");
    format
        .infer_stats_and_ordering(state, store.store(), schema(), &object(path))
        .await
        .unwrap()
        .statistics
}

#[tokio::test]
async fn scripted_inference_answers_by_path_and_records_starts_and_completions() {
    let state = SessionContext::new().state();
    let format =
        MeteredFormat::scripted(vec![script("a", rows(1)), script("b", rows(2))], Gate::Open);

    let b = infer(&format, &state, "b").await;
    let a = infer(&format, &state, "a").await;

    assert_eq!(b, rows(2));
    assert_eq!(a, rows(1));
    assert_eq!(format.started(), ["b", "a"]);
    assert_eq!(format.completed(), ["b", "a"]);
    assert_eq!(format.peak_in_flight(), 1);
}

#[test]
fn delegating_inference_answers_as_the_wrapped_format_does() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    block_on(async {
        let state = SessionContext::new().state();
        let object = fixture.sample_files(fixture::SAMPLES[0]).await.remove(0);
        let parquet = ParquetFormat::default();
        let schema = parquet
            .infer_schema(&state, fixture.store(), std::slice::from_ref(&object))
            .await
            .unwrap();
        let format = MeteredFormat::delegating(Arc::new(ParquetFormat::default()));

        let metered = format
            .infer_stats_and_ordering(&state, fixture.store(), Arc::clone(&schema), &object)
            .await
            .unwrap();

        let direct = parquet
            .infer_stats_and_ordering(&state, fixture.store(), schema, &object)
            .await
            .unwrap();
        assert_eq!(metered.statistics, direct.statistics);
        assert_eq!(metered.ordering, direct.ordering);
        assert_eq!(format.started(), [object.location.to_string()]);
    });
}

#[tokio::test]
#[should_panic(expected = "must be inferred together with ordering")]
async fn inferring_statistics_alone_panics() {
    let state = SessionContext::new().state();
    let format = MeteredFormat::scripted(vec![script("a", rows(1))], Gate::Open);
    let store = MemoryStore::new("statistics-alone");
    let _statistics = format
        .infer_stats(&state, store.store(), schema(), &object("a"))
        .await;
}

#[tokio::test]
#[should_panic(expected = "must be inferred together with statistics")]
async fn inferring_ordering_alone_panics() {
    let state = SessionContext::new().state();
    let format = MeteredFormat::scripted(vec![script("a", rows(1))], Gate::Open);
    let store = MemoryStore::new("ordering-alone");
    let _ordering = format
        .infer_ordering(&state, store.store(), schema(), &object("a"))
        .await;
}

#[tokio::test]
#[should_panic(expected = "no scripted metadata for b")]
async fn inferring_an_unscripted_path_panics() {
    let state = SessionContext::new().state();
    let format = MeteredFormat::scripted(vec![script("a", rows(1))], Gate::Open);
    infer(&format, &state, "b").await;
}

#[tokio::test]
async fn an_until_started_gate_holds_each_inference_until_that_many_run_at_once() {
    let state = SessionContext::new().state();
    let format = || {
        MeteredFormat::scripted(
            vec![
                script("a", rows(1)),
                script("b", rows(2)),
                script("c", rows(3)),
            ],
            Gate::UntilStarted { inferences: 3 },
        )
    };

    let held = format();
    let alone = tokio::time::timeout(Duration::from_millis(50), infer(&held, &state, "a")).await;
    assert!(alone.is_err(), "one inference alone must be held");
    assert_eq!(held.started(), ["a"]);
    assert_eq!(held.completed(), Vec::<String>::new());

    let format = format();
    let metas = join_all(["a", "b", "c"].map(|path| infer(&format, &state, path))).await;

    assert_eq!(metas, [rows(1), rows(2), rows(3)]);
    assert_eq!(format.peak_in_flight(), 3);
    let mut completed = format.completed();
    completed.sort();
    assert_eq!(completed, ["a", "b", "c"]);
}

#[tokio::test]
async fn a_complete_in_order_gate_completes_inferences_in_its_order() {
    let state = SessionContext::new().state();
    let format = MeteredFormat::scripted(
        vec![
            script("a", rows(1)),
            script("b", rows(2)),
            script("c", rows(3)),
        ],
        Gate::CompleteInOrder(vec!["c".to_string(), "a".to_string(), "b".to_string()]),
    );

    let metas = join_all(["a", "b", "c"].map(|path| infer(&format, &state, path))).await;

    assert_eq!(metas, [rows(1), rows(2), rows(3)]);
    assert_eq!(format.started(), ["a", "b", "c"]);
    assert_eq!(format.completed(), ["c", "a", "b"]);
}
