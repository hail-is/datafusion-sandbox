//! Run records for tests that need one but assert on none of its facts.

use crate::metrics_directory::{FormulationRecord, RunRecord, WriteRecord};
use std::time::{Duration, SystemTime};

/// A run record of `run_id` whose other fields hold plausible settings and measurements of a
/// grouped-merge write, for tests that need a record but assert on none of its facts.
#[must_use]
pub(crate) fn run_record(run_id: &str) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        started_at: SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(1_700_000_000))
            .expect("a start time after the epoch"),
        formulation: FormulationRecord {
            name: "grouped-merge".to_string(),
            groups: Some(2),
            split_points: None,
        },
        row_ordering: "locus".to_string(),
        dataset_path: "gs://bucket/refs".to_string(),
        input_format: "vortex".to_string(),
        write: Some(WriteRecord {
            output_path: "gs://bucket/combined.parquet".to_string(),
            output_format: "parquet".to_string(),
            compression: Some("snappy".to_string()),
        }),
        threads: 1,
        input_tables: 4,
        samples: 4,
        rows_written: 32,
        run_ns: 2_000,
        execute_ns: 1_000,
        peak_rss_bytes: 1 << 20,
        probe: None,
    }
}
