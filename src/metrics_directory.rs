//! The metrics directory measured writes and throughput probes record runs under.
//!
//! A metrics directory holds three tables, each a directory of one Parquet file per run: the run
//! record at `runs/<run_id>.parquet`, the run metrics at `metrics/<run_id>.parquet`, and a
//! throughput probe's progress samples at `progress/<run_id>.parquet`. A [replay](crate::replay)
//! of its shadow probes adds three more, again one file per run, which each replay replaces: the
//! tightness checks at `checks/<run_id>.parquet`, the end-of-run decisions at
//! `replay-baselines/<run_id>.parquet`, and each combination's stop at
//! `replays/<run_id>.parquet`. This module owns that layout and the two guarantees
//! [ADR 0016](../docs/adr/0016-record-run-metrics-as-wide-parquet-tables.md) gives the history a
//! directory accumulates. A run id that already has a run record is refused: checking an id is
//! the only way to get an [`UnrecordedRun`], and only an unrecorded run can be recorded. And the
//! run record is written last, so its presence marks a recorded run: a failure between the
//! writes leaves other tables' files without a record, which a retry of the id replaces.
//!
//! The check is not a lock. Two runs sharing an id that check before either records both pass,
//! and the later one's files replace the earlier one's.

use crate::{
    format::OutputFormat,
    replay::{self, Grid, ShadowRun},
    run_metrics::{self, RunRecord},
    throughput_probe::{ProbeKind, ProgressSample},
    write::WriteTarget,
};

use datafusion::{
    arrow::record_batch::RecordBatch,
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
    object_store::{self, ObjectStoreExt},
    physical_plan::ExecutionPlan,
    prelude::{ParquetReadOptions, SessionContext, col},
};
use std::{collections::BTreeSet, sync::Arc};

/// The subdirectory of a metrics directory holding the run record table.
const RUNS_TABLE: &str = "runs";
/// The subdirectory of a metrics directory holding the run metrics table.
const METRICS_TABLE: &str = "metrics";
/// The subdirectory of a metrics directory holding the progress samples table.
const PROGRESS_TABLE: &str = "progress";
/// The subdirectory of a metrics directory holding a replay's tightness checks.
const CHECKS_TABLE: &str = "checks";
/// The subdirectory of a metrics directory holding a replay's end-of-run decisions.
const REPLAY_BASELINES_TABLE: &str = "replay-baselines";
/// The subdirectory of a metrics directory holding a replay's stops.
const REPLAYS_TABLE: &str = "replays";

/// A directory measured writes and throughput probes record runs under, on any store the session
/// serves.
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

    /// The path of the progress samples file of `run_id`.
    #[must_use]
    pub fn progress_samples_path(&self, run_id: &str) -> String {
        self.table_path(PROGRESS_TABLE, run_id)
    }

    /// The path of the replayed tightness checks file of `run_id`.
    #[must_use]
    pub fn checks_path(&self, run_id: &str) -> String {
        self.table_path(CHECKS_TABLE, run_id)
    }

    /// The path of the replay baselines file of `run_id`.
    #[must_use]
    pub fn replay_baselines_path(&self, run_id: &str) -> String {
        self.table_path(REPLAY_BASELINES_TABLE, run_id)
    }

    /// The path of the replays file of `run_id`.
    #[must_use]
    pub fn replays_path(&self, run_id: &str) -> String {
        self.table_path(REPLAYS_TABLE, run_id)
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
                "run id '{run_id}' already has a run record at '{record_path}'; a recorded run is never replaced"
            ))),
            Err(object_store::Error::NotFound { .. }) => Ok(UnrecordedRun {
                directory: self.clone(),
                run_id: run_id.to_string(),
            }),
            Err(error) => Err(error.into()),
        }
    }

    /// Replays every shadow probe recorded here under every combination of `grid`, and writes
    /// its checks, replay baselines and replays, replacing any an earlier replay wrote. Runs that
    /// are not shadow probes are skipped. Once every shadow probe is replayed, the replay tables'
    /// files of any other run, such as one whose run record was removed, are deleted, so the
    /// tables hold the current runs alone.
    ///
    /// # Errors
    ///
    /// Returns an error if the run records or a shadow probe's progress samples cannot be read, if
    /// a shadow probe's run record lacks a setting, or if a replay table cannot be written, or a
    /// stale file in one listed or deleted. A failure leaves the stale files in place.
    pub async fn replay(&self, ctx: &SessionContext, grid: &Grid) -> Result<ReplayCount> {
        let records = ctx
            .read_parquet(
                format!("{}/{RUNS_TABLE}/", self.path),
                ParquetReadOptions::default().schema(&run_metrics::run_record_schema()),
            )
            .await?
            .collect()
            .await?;
        let mut count = ReplayCount::default();
        let mut replayed_runs = BTreeSet::new();
        for record in &records {
            for row in 0..record.num_rows() {
                let Some(probe) = run_metrics::recorded_probe(record, row)?
                    .filter(|probe| probe.kind == ProbeKind::Shadow)
                else {
                    count.skipped = count.skipped.saturating_add(1);
                    continue;
                };
                let run = ShadowRun {
                    samples: self.progress_samples(ctx, &probe.run_id).await?,
                    run_id: probe.run_id,
                    settings: probe.settings,
                    first_partition_end_ns: probe.first_partition_end_ns,
                };
                let replayed = replay::replay(grid, &run);
                write_table(ctx, replayed.checks_batch()?, self.checks_path(&run.run_id)).await?;
                write_table(
                    ctx,
                    replayed.replay_baselines_batch()?,
                    self.replay_baselines_path(&run.run_id),
                )
                .await?;
                write_table(
                    ctx,
                    replayed.replays_batch()?,
                    self.replays_path(&run.run_id),
                )
                .await?;
                count.replayed = count.replayed.saturating_add(1);
                replayed_runs.insert(run.run_id);
            }
        }
        count.removed = self.remove_stale_replays(ctx, &replayed_runs).await?;
        Ok(count)
    }

    /// Deletes every file of the replay tables whose run is not among `replayed_runs`, and hands
    /// back how many runs had one. Other files in the tables' directories are left alone.
    async fn remove_stale_replays(
        &self,
        ctx: &SessionContext,
        replayed_runs: &BTreeSet<String>,
    ) -> Result<usize> {
        let mut stale_runs = BTreeSet::new();
        for table in [CHECKS_TABLE, REPLAY_BASELINES_TABLE, REPLAYS_TABLE] {
            let url = ListingTableUrl::parse(format!("{}/{table}/", self.path))?;
            let store = ctx.runtime_env().object_store(&url)?;
            let objects = match store.list_with_delimiter(Some(url.prefix())).await {
                Ok(listing) => listing.objects,
                Err(object_store::Error::NotFound { .. }) => Vec::new(),
                Err(error) => return Err(error.into()),
            };
            for object in objects {
                let Some(run_id) = object
                    .location
                    .filename()
                    .and_then(|name| name.strip_suffix(".parquet"))
                    .filter(|&run_id| !replayed_runs.contains(run_id))
                else {
                    continue;
                };
                match store.delete(&object.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => return Err(error.into()),
                }
                stale_runs.insert(run_id.to_string());
            }
        }
        Ok(stale_runs.len())
    }

    /// The progress samples recorded for `run_id`, in the order taken.
    async fn progress_samples(
        &self,
        ctx: &SessionContext,
        run_id: &str,
    ) -> Result<Vec<ProgressSample>> {
        let batches = ctx
            .read_parquet(
                self.progress_samples_path(run_id),
                ParquetReadOptions::default().schema(&run_metrics::progress_samples_schema()),
            )
            .await?
            .sort(vec![col("sample_index").sort(true, false)])?
            .collect()
            .await?;
        run_metrics::progress_samples(&batches)
    }
}

/// How many runs [`MetricsDirectory::replay`] replayed, how many it skipped as not shadow
/// probes, and how many other runs' stale replay files it removed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplayCount {
    pub replayed: usize,
    pub skipped: usize,
    pub removed: usize,
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
        self.check(record)?;
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

    /// Records a throughput probe: writes its progress `samples` first, and then records it as
    /// [`UnrecordedRun::record`] does.
    ///
    /// # Errors
    ///
    /// Returns an error for any reason [`UnrecordedRun::record`] gives, or if the progress
    /// samples cannot be filled or written, which leaves the run unrecorded.
    pub async fn record_probe(
        self,
        ctx: &SessionContext,
        record: &RunRecord,
        plan: &Arc<dyn ExecutionPlan>,
        samples: &[ProgressSample],
    ) -> Result<Vec<String>> {
        self.check(record)?;
        write_table(
            ctx,
            run_metrics::progress_samples_batch(&self.run_id, samples)?,
            self.directory.progress_samples_path(&self.run_id),
        )
        .await?;
        self.record(ctx, record, plan).await
    }

    /// Refuses `record` if it names another run than the one checked.
    fn check(&self, record: &RunRecord) -> Result<()> {
        if record.run_id == self.run_id {
            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "the run '{}' was checked, but the record names run '{}'",
                self.run_id, record.run_id
            )))
        }
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
