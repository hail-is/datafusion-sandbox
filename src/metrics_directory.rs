//! The metrics directory a measured write records runs under.
//!
//! A metrics directory holds two tables, each a directory of one Parquet file per run: the run
//! record at `runs/<run_id>.parquet` and the run metrics at `metrics/<run_id>.parquet`. This
//! module owns that layout and the two guarantees
//! [ADR 0016](../docs/adr/0016-record-run-metrics-as-wide-parquet-tables.md) gives the history a
//! directory accumulates. A run id that already has a run record is refused: checking an id is
//! the only way to get an [`UnrecordedRun`], and only an unrecorded run can be recorded. And the
//! run record is written last, so its presence marks a recorded run: a failure between the two
//! writes leaves run metrics without a record, which a retry of the id replaces.
//!
//! The check is not a lock. Two runs sharing an id that check before either records both pass,
//! and the later one's files replace the earlier one's.

use crate::{
    format::OutputFormat,
    run_metrics::{self, RunRecord},
    write::WriteTarget,
};

use datafusion::{
    arrow::record_batch::RecordBatch,
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
    object_store::{self, ObjectStoreExt},
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};
use std::sync::Arc;

/// The subdirectory of a metrics directory holding the run record table.
const RUNS_TABLE: &str = "runs";
/// The subdirectory of a metrics directory holding the run metrics table.
const METRICS_TABLE: &str = "metrics";

/// A directory a measured write records runs under, on any store the session serves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsDirectory {
    /// The path as given, without a trailing slash.
    path: String,
}

impl MetricsDirectory {
    /// The metrics directory at `path`, a local path or a URL, with or without a trailing slash.
    #[must_use]
    pub fn new(path: &str) -> Self {
        Self {
            path: path.trim_end_matches('/').to_string(),
        }
    }

    /// The directory's path, without a trailing slash.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The path of the run record file of `run_id`.
    #[must_use]
    pub fn run_record_path(&self, run_id: &str) -> String {
        self.table_path(RUNS_TABLE, run_id)
    }

    /// The path of the run metrics file of `run_id`.
    #[must_use]
    pub fn run_metrics_path(&self, run_id: &str) -> String {
        self.table_path(METRICS_TABLE, run_id)
    }

    fn table_path(&self, table: &str, run_id: &str) -> String {
        format!("{}/{table}/{run_id}.parquet", self.path)
    }

    /// Checks that `run_id` has no run record in this directory, and hands back the run that may
    /// now be recorded under it. Run metrics without a record do not make an id recorded.
    ///
    /// # Errors
    ///
    /// Returns a configuration error naming the run record's path if `run_id` already has one,
    /// or an error if the directory is on a store the session does not serve or the store cannot
    /// answer.
    pub async fn unrecorded(&self, ctx: &SessionContext, run_id: &str) -> Result<UnrecordedRun> {
        let record_path = self.run_record_path(run_id);
        let url = ListingTableUrl::parse(&record_path)?;
        let store = ctx.runtime_env().object_store(&url)?;
        match store.head(url.prefix()).await {
            Ok(_) => Err(DataFusionError::Configuration(format!(
                "run id '{run_id}' already has a run record at '{record_path}'; a measured write does not replace a recorded run"
            ))),
            Err(object_store::Error::NotFound { .. }) => Ok(UnrecordedRun {
                directory: self.clone(),
                run_id: run_id.to_string(),
            }),
            Err(error) => Err(error.into()),
        }
    }
}

/// A run id that had no run record in a metrics directory when it was checked, and so may be
/// recorded there. [`MetricsDirectory::unrecorded`] is the only way to get one.
#[derive(Debug)]
pub struct UnrecordedRun {
    directory: MetricsDirectory,
    run_id: String,
}

impl UnrecordedRun {
    /// The run id checked.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Records the run: writes the run metrics of `plan`, an executed plan, and then `record`,
    /// and hands back the names of the metrics `plan` reported that the run metrics table has no
    /// column for, sorted and without repeats.
    ///
    /// # Errors
    ///
    /// Returns an error if `record` names another run id, or if either table cannot be filled or
    /// written. A failed run metrics write leaves the run unrecorded; so does a failed run record
    /// write, which leaves the run metrics behind for a retry to replace.
    pub async fn record(
        self,
        ctx: &SessionContext,
        record: &RunRecord,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<String>> {
        if record.run_id != self.run_id {
            return Err(DataFusionError::Internal(format!(
                "the run '{}' was checked, but the record names run '{}'",
                self.run_id, record.run_id
            )));
        }
        let metrics = run_metrics::run_metrics_batch(&self.run_id, plan)?;
        write_table(
            ctx,
            metrics.batch,
            self.directory.run_metrics_path(&self.run_id),
        )
        .await?;
        write_table(
            ctx,
            run_metrics::run_record_batch(record)?,
            self.directory.run_record_path(&self.run_id),
        )
        .await?;
        Ok(metrics.unrecorded)
    }
}

/// Writes `batch` as one Parquet file at `path`, replacing any file there.
async fn write_table(ctx: &SessionContext, batch: RecordBatch, path: String) -> Result<()> {
    WriteTarget {
        output_path: path,
        output_format: OutputFormat::PARQUET,
    }
    .write_unordered(ctx.read_batch(batch)?)
    .await?;
    Ok(())
}
