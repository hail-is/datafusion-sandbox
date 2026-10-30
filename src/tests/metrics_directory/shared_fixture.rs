//! The shared metrics directory fixture, which the probe viewer's and the sweep report's suites
//! read: the tables the writers write from fixed synthetic inputs, checked in under
//! `python/tests/fixtures/metrics_directory`
//! ([ADR 0021](../../../docs/adr/0021-hold-the-rust-python-contracts-with-shared-fixtures.md)).

use super::{directory, eager_settings, executed_plan, on, shadow_record};
use crate::{
    metrics_directory::{FormulationRecord, Grid, RunRecord, RunStatus, WriteRecord},
    testing::{memory_store::MemoryStore, probe_series::noisy_series},
};

use bytes::Bytes;
use datafusion::{
    arrow::{
        array::RecordBatchReader, compute::concat_batches, datatypes::Schema, error::ArrowError,
        record_batch::RecordBatch, util::pretty::pretty_format_batches,
    },
    error::Result,
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
};
use object_store::ObjectStoreExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    num::NonZeroU32,
    path::Path,
    time::{Duration, SystemTime},
};

/// The fixture's directory, under the crate root.
const FIXTURE: &str = "python/tests/fixtures/metrics_directory";

/// The environment variable that, set to 1, makes the freshness test rewrite the fixture.
const REWRITE: &str = "DATAFUSION_SANDBOX_REWRITE_METRICS_FIXTURE";

/// The tables the fixture holds: every table the Python readers read. The run metrics are left
/// out, since no reader reads them and they hold the timings of the plan the test executes.
const TABLES: [&str; 5] = ["runs", "progress", "checks", "replay-baselines", "replays"];

/// The run id of the fixture's measured write, named `<cell>-<runner>` as a sweep's runner names
/// its cells' runs.
const WRITE: &str = "write-r1";

/// The run id of the fixture's shadow probe.
const SHADOW: &str = "shadow-r1";

/// The run id of the fixture's measured write that failed.
const FAILED: &str = "failed-r1";

/// The run record of a measured write of `run_id`: 1,000,000 rows from 50 input tables in 2 s on
/// 2 threads.
fn fixture_record(run_id: &str) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        status: RunStatus::Completed,
        started_at: SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(1_700_000_000))
            .unwrap(),
        formulation: FormulationRecord {
            name: "grouped-merge".to_string(),
            groups: Some(4),
            split_points: None,
        },
        row_ordering: "locus,s".to_string(),
        dataset_path: "gs://bucket/refs".to_string(),
        input_format: "vortex".to_string(),
        write: Some(WriteRecord {
            output_path: "gs://bucket/combined.parquet".to_string(),
            output_format: "parquet".to_string(),
            compression: Some("zstd(3)".to_string()),
        }),
        threads: 2,
        input_tables: Some(50),
        samples: Some(200),
        rows_written: Some(1_000_000),
        run_ns: 2_000_000_000,
        execute_ns: Some(1_500_000_000),
        peak_rss_bytes: 1 << 30,
        probe: None,
    }
}

/// The grid the fixture's shadow probe is replayed under: two pairs, one at its recorded settings,
/// and two values of each stop setting, one of them recorded.
fn fixture_grid() -> Grid {
    let recorded = eager_settings();
    Grid {
        batch_durations: vec![recorded.batch_duration, Duration::from_secs(1)],
        window_groups: vec![recorded.window_groups],
        precisions: vec![0.05, recorded.precision],
        consecutive_checks: vec![recorded.consecutive_checks, NonZeroU32::new(3).unwrap()],
        min_durations: vec![recorded.min_duration, Duration::from_secs(10)],
    }
}

/// The fixture's files as the writers write them, by path within the metrics directory: a
/// measured write's run record, the run record and progress samples of a drained shadow probe
/// over a noisy series, replayed under [`fixture_grid`], and the run record of a measured write
/// that failed after 0.5 s. The other runs' facts are the first measured write's, but those a
/// run that did not complete has not learned.
fn written_fixture() -> BTreeMap<String, Bytes> {
    let store = MemoryStore::new("shared-fixture");
    let directory = directory(&store);
    let settings = eager_settings();
    let samples = noisy_series(2, 4, 0.05);
    let first_partition_end_ns = samples.get(150).map(|sample| sample.elapsed_ns);
    let last = samples.last().unwrap();
    let shadow = RunRecord {
        write: None,
        rows_written: Some(last.rows),
        run_ns: last.elapsed_ns,
        execute_ns: Some(last.elapsed_ns),
        probe: shadow_record(SHADOW, &settings, &samples, first_partition_end_ns).probe,
        ..fixture_record(SHADOW)
    };
    let failed = RunRecord {
        status: RunStatus::Failed,
        rows_written: None,
        run_ns: 500_000_000,
        execute_ns: None,
        ..fixture_record(FAILED)
    };
    let held = store.clone();
    on(&store, move |ctx| async move {
        let plan = executed_plan(&ctx, false).await?;
        let run = directory.unrecorded(&ctx, WRITE).await?;
        run.record(&ctx, &fixture_record(WRITE), &plan).await?;
        let run = directory.unrecorded(&ctx, SHADOW).await?;
        run.record_probe(&ctx, &shadow, &plan, &samples).await?;
        let run = directory.unrecorded(&ctx, FAILED).await?;
        run.record_unfinished(&ctx, &failed).await?;
        directory.replay(&ctx, &fixture_grid()).await?;
        let mut files = BTreeMap::new();
        for table in TABLES {
            for location in held.locations_under(&format!("benchmarks/{table}")).await {
                let bytes = held.store().get(&location).await?.bytes().await?;
                let file = location.as_ref().trim_start_matches("benchmarks/");
                files.insert(file.to_string(), bytes);
            }
        }
        Ok(files)
    })
}

/// The checked-in fixture's files under [`TABLES`], by path within the metrics directory.
fn checked_in_fixture(fixture: &Path) -> BTreeMap<String, Bytes> {
    let mut files = BTreeMap::new();
    for table in TABLES {
        let Ok(entries) = fs::read_dir(fixture.join(table)) else {
            continue;
        };
        for entry in entries {
            let entry = entry.unwrap();
            let file = format!("{table}/{}", entry.file_name().to_string_lossy());
            files.insert(file, Bytes::from(fs::read(entry.path()).unwrap()));
        }
    }
    files
}

/// Replaces the fixture's tables with `written`.
fn rewrite(fixture: &Path, written: &BTreeMap<String, Bytes>) {
    for table in TABLES {
        let directory = fixture.join(table);
        if directory.exists() {
            fs::remove_dir_all(&directory).unwrap();
        }
        fs::create_dir_all(&directory).unwrap();
    }
    for (file, bytes) in written {
        fs::write(fixture.join(file), bytes).unwrap();
    }
}

/// The rows of a Parquet file, in its schema as decoded.
fn decoded(bytes: Bytes) -> Result<RecordBatch> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, ArrowError>>()?;
    Ok(concat_batches(&schema, &batches)?)
}

/// The fields and metadata of `schema`, one field to a line.
fn schema_lines(schema: &Schema) -> String {
    let fields: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            let nullable = if field.is_nullable() {
                ", nullable"
            } else {
                ""
            };
            format!("  {}: {}{nullable}", field.name(), field.data_type())
        })
        .collect();
    format!("{}\n  metadata: {:?}", fields.join("\n"), schema.metadata())
}

/// How the file the writers write differs from the checked-in one, or `None` if their decoded
/// schemas and rows are equal.
fn difference(written: Bytes, checked_in: Bytes) -> Option<String> {
    let written = decoded(written).unwrap();
    let checked_in = match decoded(checked_in) {
        Ok(checked_in) => checked_in,
        Err(error) => return Some(format!("the checked-in file does not decode: {error}")),
    };
    if written.schema() != checked_in.schema() {
        return Some(format!(
            "the writers write the schema\n{}\nbut the fixture holds\n{}",
            schema_lines(&written.schema()),
            schema_lines(&checked_in.schema())
        ));
    }
    if written.num_rows() != checked_in.num_rows() {
        return Some(format!(
            "the writers write {} rows but the fixture holds {}",
            written.num_rows(),
            checked_in.num_rows()
        ));
    }
    let row =
        (0..written.num_rows()).find(|&row| written.slice(row, 1) != checked_in.slice(row, 1))?;
    Some(format!(
        "row {row} differs: the writers write\n{}\nbut the fixture holds\n{}",
        pretty_format_batches(&[written.slice(row, 1)]).unwrap(),
        pretty_format_batches(&[checked_in.slice(row, 1)]).unwrap()
    ))
}

/// The checked-in fixture holds the tables the writers write, compared by decoded schema and
/// rows, since a Parquet file's footer records the writer's version. When it is stale, the
/// failure names each stale file and the environment variable that rewrites the fixture.
#[test]
fn the_shared_metrics_directory_fixture_is_current() {
    let written = written_fixture();
    // The fixture is read, and on request rewritten, on local disk: the Python suites read it
    // there without a Rust build, and only this test can say whether it is what the writers
    // write.
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    if std::env::var(REWRITE).as_deref() == Ok("1") {
        rewrite(&fixture, &written);
        return;
    }
    let mut checked_in = checked_in_fixture(&fixture);

    let files: BTreeSet<String> = written.keys().chain(checked_in.keys()).cloned().collect();
    let stale: Vec<String> = files
        .into_iter()
        .filter_map(|file| {
            let difference = match (written.get(&file), checked_in.remove(&file)) {
                (Some(written), Some(checked_in)) => difference(written.clone(), checked_in)?,
                (Some(_), None) => "the writers write it, but the fixture lacks it".to_string(),
                (None, _) => "the fixture holds it, but the writers no longer write it".to_string(),
            };
            Some(format!("{file}: {difference}"))
        })
        .collect();
    assert!(
        stale.is_empty(),
        "{FIXTURE} is stale; rerun this test with {REWRITE}=1 to rewrite it.\n\n{}",
        stale.join("\n\n")
    );
}
