//! Cancellation: runs held open at a gated store while a test cancels them by id or through their
//! handle, or lets them go on with no handle.

use super::{
    MemoryStores, sorted_union,
    writes::{locations, measured, recorded},
};
use crate::{
    backend::{Action, Backend, ErrorCode, Outcome, Run, Settings, Versions},
    locus::LocusRepresentation,
    metrics_directory::MetricsDirectory,
    row_ordering::{RowOrdering, SortColumn},
    testing::{
        columns::string_values,
        fixture::{self, DatasetFixture, FixtureFormat, RecordedRun},
        memory_store::{MemoryStore, ReadGate},
    },
};

use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

fn mixed() -> &'static Arc<DatasetFixture> {
    fixture::mixed_dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::ContigPosition)
}

/// A backend whose reads of `fixture`'s dataset are held at the returned gate, and which writes to
/// `output`, with runs checking their rows' order at run time.
fn gated_backend(
    fixture: &'static Arc<DatasetFixture>,
    output: &MemoryStore,
) -> (Backend, ReadGate) {
    let (gated, gate) = fixture.with_gated_reads();
    let backend = Backend::new(Arc::new(MemoryStores(vec![
        (gated.url().clone(), Arc::clone(gated.store())),
        (output.url().clone(), Arc::clone(output.store())),
    ])))
    .verifying_order();
    (backend, gate)
}

fn start(backend: &Backend, bytes: &[u8], action: Action, run_id: &str) -> Run {
    backend
        .start(
            bytes,
            action,
            &Settings::new(),
            Some(run_id.to_string()),
            Versions::backend(),
        )
        .unwrap()
}

fn collect(ordering: &[SortColumn]) -> Action {
    Action::Collect {
        ordering: ordering.to_vec(),
    }
}

/// What `metrics` on `store` holds for `run_id` once it holds a run record: the wait for a run
/// no handle is left to wait for.
fn recorded_eventually(
    store: &MemoryStore,
    metrics: &MetricsDirectory,
    run_id: &str,
) -> RecordedRun {
    let started = Instant::now();
    loop {
        let recorded = recorded(store, metrics, run_id);
        if recorded.record.is_some() {
            return recorded;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "{run_id} recorded nothing"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

/// A run cancelled by id while its reads are held ends in the run-cancelled code once they are
/// released, with no Outcome.
#[test]
fn a_run_cancelled_by_id_while_its_reads_are_held_ends_in_the_run_cancelled_code() {
    let fixture = mixed();
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (backend, gate) = gated_backend(fixture, &MemoryStore::new("cancel-collect"));
    let run = start(&backend, &bytes, collect(&ordering), "run-a");

    fixture::block_on(gate.reached());
    backend.cancel("run-a");
    gate.release();

    let error = run.wait().unwrap_err();
    assert_eq!(error.code(), ErrorCode::RunCancelled, "{error}");
    assert!(error.to_string().starts_with("run_cancelled"), "{error}");
}

/// A measured write cancelled that way leaves a run record saying so, with none of the
/// measurements it never made, no run metrics and no output.
#[test]
fn a_measured_write_cancelled_by_id_leaves_a_cancelled_run_record_and_no_output() {
    let fixture = mixed();
    let output = MemoryStore::new("cancel-measured");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (action, metrics) = measured(&output, &ordering, true);
    let (backend, gate) = gated_backend(fixture, &output);
    let run = start(&backend, &bytes, action, "run-a");

    fixture::block_on(gate.reached());
    backend.cancel("run-a");
    gate.release();

    let error = run.wait().unwrap_err();
    assert_eq!(error.code(), ErrorCode::RunCancelled, "{error}");
    let RecordedRun {
        record,
        metrics: run_metrics,
        ..
    } = recorded(&output, &metrics, "run-a");
    let record = record.unwrap();
    for (column, expected) in [
        ("run_id", "run-a"),
        ("status", "cancelled"),
        ("formulation", "grouped-merge"),
        ("dataset_path", fixture.table_path().as_str()),
        ("rows_written", ""),
        ("execute_ns", ""),
    ] {
        assert_eq!(string_values(&record, column), [expected], "{column}");
    }
    assert!(run_metrics.is_none(), "{run_metrics:?}");
    assert_eq!(locations(&output), Vec::<String>::new());
}

/// However soon a measured write is cancelled, even before its pipeline starts, a campaign still
/// sees the run it started.
#[test]
fn a_measured_write_cancelled_as_soon_as_it_starts_still_leaves_a_cancelled_run_record() {
    let fixture = mixed();
    let output = MemoryStore::new("cancel-measured-at-once");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (action, metrics) = measured(&output, &ordering, false);
    // Never released until the run is cancelled, so the run can't complete before its cancel.
    let (backend, gate) = gated_backend(fixture, &output);
    let run = start(&backend, &bytes, action, "run-a");

    backend.cancel("run-a");
    gate.release();

    let error = run.wait().unwrap_err();
    assert_eq!(error.code(), ErrorCode::RunCancelled, "{error}");
    let record = recorded(&output, &metrics, "run-a").record.unwrap();
    assert_eq!(string_values(&record, "status"), ["cancelled"]);
}

/// Cancelling an id no run has is not an error, and leaves the runs that are going alone.
#[test]
fn cancelling_an_unknown_run_id_is_not_an_error_and_cancels_nothing() {
    let fixture = mixed();
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (backend, gate) = gated_backend(fixture, &MemoryStore::new("cancel-unknown"));
    let run = start(&backend, &bytes, collect(&ordering), "run-a");

    fixture::block_on(gate.reached());
    backend.cancel("no-such-run");
    gate.release();

    assert!(matches!(run.wait(), Ok(Outcome::Rows { .. })));
}

/// Cancelling a finished run is not an error: its Outcome stands, and a later run under its id is
/// not cancelled by it.
#[test]
fn cancelling_a_finished_run_id_is_not_an_error_and_leaves_its_outcome() {
    let fixture = mixed();
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (backend, gate) = gated_backend(fixture, &MemoryStore::new("cancel-finished"));
    gate.release();
    let finished = start(&backend, &bytes, collect(&ordering), "run-a");
    assert!(matches!(finished.wait(), Ok(Outcome::Rows { .. })));

    backend.cancel("run-a");

    assert!(matches!(finished.wait(), Ok(Outcome::Rows { .. })));
    let later = start(&backend, &bytes, collect(&ordering), "run-a");
    assert!(matches!(later.wait(), Ok(Outcome::Rows { .. })));
}

/// Dropping a handle leaves its run going, to the end it would have had.
#[test]
fn dropping_a_handle_leaves_its_run_going_to_completion() {
    let fixture = mixed();
    let output = MemoryStore::new("cancel-dropped-completes");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (action, metrics) = measured(&output, &ordering, false);
    let (backend, gate) = gated_backend(fixture, &output);
    let run = start(&backend, &bytes, action, "run-a");

    fixture::block_on(gate.reached());
    drop(run);
    gate.release();

    let record = recorded_eventually(&output, &metrics, "run-a")
        .record
        .unwrap();
    assert_eq!(string_values(&record, "status"), ["completed"]);
    assert_eq!(locations(&output), ["out/g.parquet"]);
}

/// With its handle dropped, a run can still be cancelled by id.
#[test]
fn cancelling_by_id_reaches_a_run_whose_handle_was_dropped() {
    let fixture = mixed();
    let output = MemoryStore::new("cancel-dropped");
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (action, metrics) = measured(&output, &ordering, false);
    let (backend, gate) = gated_backend(fixture, &output);
    let run = start(&backend, &bytes, action, "run-a");

    fixture::block_on(gate.reached());
    drop(run);
    backend.cancel("run-a");
    gate.release();

    let record = recorded_eventually(&output, &metrics, "run-a")
        .record
        .unwrap();
    assert_eq!(string_values(&record, "status"), ["cancelled"]);
    assert_eq!(locations(&output), Vec::<String>::new());
}

/// A run id names one going run, so a cancel by id reaches exactly it. The refused start leaves
/// the id's run reachable, and once that run ends the id is free again.
#[test]
fn starting_a_run_under_the_id_of_a_going_run_raises_invalid_action() {
    let fixture = mixed();
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (backend, gate) = gated_backend(fixture, &MemoryStore::new("cancel-repeated-id"));
    let going = start(&backend, &bytes, collect(&ordering), "run-a");

    let refused = backend
        .start(
            &bytes,
            collect(&ordering),
            &Settings::new(),
            Some("run-a".to_string()),
            Versions::backend(),
        )
        .unwrap_err();

    assert_eq!(refused.code(), ErrorCode::InvalidAction, "{refused}");
    assert!(refused.message().contains("run-a"), "{refused}");
    fixture::block_on(gate.reached());
    backend.cancel("run-a");
    gate.release();
    assert_eq!(going.wait().unwrap_err().code(), ErrorCode::RunCancelled);
    let later = start(&backend, &bytes, collect(&ordering), "run-a");
    assert!(matches!(later.wait(), Ok(Outcome::Rows { .. })));
}

/// A handle cancels its own run, not a later one that took the id once its own run ended.
#[test]
fn a_handle_cancels_its_own_run_and_not_a_later_run_under_its_id() {
    let fixture = mixed();
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (backend, gate) = gated_backend(fixture, &MemoryStore::new("cancel-handle"));
    let cancelled = start(&backend, &bytes, collect(&ordering), "run-a");

    fixture::block_on(gate.reached());
    cancelled.cancel();
    gate.release();
    assert_eq!(
        cancelled.wait().unwrap_err().code(),
        ErrorCode::RunCancelled
    );

    let later = start(&backend, &bytes, collect(&ordering), "run-a");
    cancelled.cancel();
    assert!(matches!(later.wait(), Ok(Outcome::Rows { .. })));
}

/// Waiting a while for a run that goes on longer hands back nothing and leaves it going; once the
/// run has ended, every wait of either kind hands back how it ended.
#[test]
fn waiting_a_while_hands_back_nothing_until_the_run_ends_and_then_how_it_ended() {
    let fixture = mixed();
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let (backend, gate) = gated_backend(fixture, &MemoryStore::new("cancel-wait-a-while"));
    let run = start(&backend, &bytes, collect(&ordering), "run-a");

    fixture::block_on(gate.reached());
    assert!(run.wait_timeout(Duration::from_millis(1)).is_none());
    gate.release();

    assert!(matches!(
        run.wait_timeout(Duration::from_secs(60)),
        Some(Ok(Outcome::Rows { .. }))
    ));
    assert!(matches!(
        run.wait_timeout(Duration::ZERO),
        Some(Ok(Outcome::Rows { .. }))
    ));
    assert!(matches!(run.wait(), Ok(Outcome::Rows { .. })));
}
