//! The progress samples table, filled from a probe's samples and read back.

use crate::{
    metrics_directory::progress_samples,
    tests::support::{assert_columns, string_values, u64_values},
    throughput_probe::ProgressSample,
};

use datafusion::arrow::datatypes::DataType;

#[test]
fn progress_samples_read_back_in_order() {
    let samples = [(3_000, 0), (100_004_000, 512), (200_001_000, 1_536)]
        .map(|(elapsed_ns, rows)| ProgressSample { elapsed_ns, rows });
    let (first, rest) = samples.split_at(1);
    let batches = [
        progress_samples::progress_samples_batch("run-1", first).unwrap(),
        progress_samples::progress_samples_batch("run-1", rest).unwrap(),
    ];

    assert_eq!(
        progress_samples::read_progress_samples(&batches).unwrap(),
        samples
    );
}

/// The progress samples table holds one row per sample, in the order taken, each with its index
/// in that order.
#[test]
fn progress_samples_fill_one_row_each_in_order() {
    let samples = [(3_000, 0), (100_004_000, 512), (200_001_000, 1_536)]
        .map(|(elapsed_ns, rows)| ProgressSample { elapsed_ns, rows });

    let batch = progress_samples::progress_samples_batch("run-1", &samples).unwrap();

    assert_columns(
        &batch.schema(),
        &[
            ("run_id", DataType::Utf8, false),
            ("sample_index", DataType::UInt64, false),
            ("elapsed_ns", DataType::UInt64, false),
            ("rows", DataType::UInt64, false),
        ],
    );
    assert_eq!(string_values(&batch, "run_id"), ["run-1"; 3]);
    assert_eq!(
        u64_values(&batch, "sample_index"),
        [Some(0), Some(1), Some(2)]
    );
    assert_eq!(
        u64_values(&batch, "elapsed_ns"),
        [Some(3_000), Some(100_004_000), Some(200_001_000)]
    );
    assert_eq!(
        u64_values(&batch, "rows"),
        [Some(0), Some(512), Some(1_536)]
    );
}
