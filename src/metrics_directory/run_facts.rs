//! Run facts: what a run knows before its action executes, and the run record they become when it
//! ends.
//!
//! A run gathers [`RunFacts`] as it starts. [`RunFacts::completed`] makes the record of a run that
//! completed, and [`RunFacts::record_stopped`] records one that a failure or a cancellation
//! stopped, so every runner states how a run ended the same way.

use super::{
    FormulationRecord, ProbeRecord, RunRecord, RunStatus, UnrecordedRun, WriteRecord, process,
};
use crate::{pipeline, sink::ExecutedSink};

use datafusion::{
    error::{DataFusionError, Result},
    prelude::SessionContext,
};
use std::{
    future::Future,
    time::{Instant, SystemTime},
};

/// When a run started, by the wall clock it records and the monotonic clock it times with.
#[derive(Clone, Copy, Debug)]
pub struct Started {
    at: SystemTime,
    instant: Instant,
}

impl Started {
    #[must_use]
    pub fn now() -> Self {
        Self {
            at: SystemTime::now(),
            instant: Instant::now(),
        }
    }
}

/// How much of the dataset a run covers: the input tables it reads, and the size of their sample
/// set.
#[derive(Clone, Copy, Debug)]
pub struct Coverage {
    pub input_tables: usize,
    pub samples: usize,
}

/// How a measured write that completed went: the rows it wrote, and the names of the metrics its
/// plan reported that the run metrics table has no column for, sorted and without repeats.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeasuredRows {
    pub rows_written: u64,
    pub unrecorded_metrics: Vec<String>,
}

/// What a run record holds that a run knows before its action executes.
#[derive(Debug)]
pub struct RunFacts {
    pub started: Started,
    pub formulation: FormulationRecord,
    pub row_ordering: String,
    pub dataset_path: String,
    pub input_format: String,
    pub threads: usize,
}

impl RunFacts {
    /// Plans a measured write's rows with `plan`, writes them to `write` with `execute`, until the
    /// run is cancelled, and records the run on `run`: as completed with its run metrics, or, if
    /// planning or writing stopped it, by a run record alone before the error is returned.
    ///
    /// `plan` hands back how much of the dataset the rows cover along with them, so a run stopped
    /// once it planned records that too. Recording happens here, inside the pipeline, so before the
    /// run's runtimes are torn down. A failure to record a stopped run too is not reported over
    /// the error that stopped it.
    ///
    /// # Errors
    ///
    /// Returns the error that stopped the run, or an error if a completed run can't be recorded, as
    /// [`UnrecordedRun::record`] says.
    pub async fn measure<Rows, Plan, Execute, Executing>(
        self,
        ctx: &SessionContext,
        run: UnrecordedRun,
        write: WriteRecord,
        plan: Plan,
        execute: Execute,
    ) -> Result<MeasuredRows>
    where
        Plan: Future<Output = Result<(Coverage, Rows)>>,
        Execute: FnOnce(Rows) -> Executing,
        Executing: Future<Output = Result<ExecutedSink>>,
    {
        let mut planned = None;
        let written = pipeline::until_cancelled(ctx, async {
            let (coverage, rows) = plan.await?;
            planned = Some(coverage);
            Ok((coverage, execute(rows).await?))
        })
        .await;
        let (coverage, executed) = match written {
            Ok(written) => written,
            Err(error) => {
                self.record_stopped(ctx, run, &error, planned, write).await;
                return Err(error);
            }
        };
        let record = self.completed(
            run.run_id().to_string(),
            coverage,
            Some(write),
            executed.rows_written,
            executed.execute_ns,
            None,
        )?;
        Ok(MeasuredRows {
            rows_written: executed.rows_written,
            unrecorded_metrics: run.record(ctx, &record, &executed.plan).await?,
        })
    }

    /// The run record of `run_id`, a completed run over `coverage` that ends now. Its action
    /// wrote `write`, if any, wrote `rows_written` rows, or received them for a probe, and
    /// executed for `execute_ns`. A probe's settings and decision are `probe`.
    ///
    /// # Errors
    ///
    /// Returns an error if the process's peak resident set size cannot be read.
    pub fn completed(
        self,
        run_id: String,
        coverage: Coverage,
        write: Option<WriteRecord>,
        rows_written: u64,
        execute_ns: u64,
        probe: Option<ProbeRecord>,
    ) -> Result<RunRecord> {
        Ok(RunRecord {
            rows_written: Some(rows_written),
            execute_ns: Some(execute_ns),
            probe,
            ..self.ended(run_id, RunStatus::Completed, Some(coverage), write)?
        })
    }

    /// Records on `run` a run that `error` stopped, by a run record alone, with the status
    /// `cancelled` if the error is the run's cancellation and `failed` otherwise, over `coverage`
    /// if it planned its rows. The record holds none of the facts only a completed run has.
    ///
    /// `error` is what the caller needs, so a failure to record the run too is dropped.
    async fn record_stopped(
        self,
        ctx: &SessionContext,
        run: UnrecordedRun,
        error: &DataFusionError,
        coverage: Option<Coverage>,
        write: WriteRecord,
    ) {
        let status = if pipeline::is_cancelled(error) {
            RunStatus::Cancelled
        } else {
            RunStatus::Failed
        };
        if let Ok(record) = self.ended(run.run_id().to_string(), status, coverage, Some(write)) {
            drop(run.record_unfinished(ctx, &record).await);
        }
    }

    /// The run record of `run_id`, a run that ends now with `status`.
    fn ended(
        self,
        run_id: String,
        status: RunStatus,
        coverage: Option<Coverage>,
        write: Option<WriteRecord>,
    ) -> Result<RunRecord> {
        Ok(RunRecord {
            run_id,
            status,
            started_at: self.started.at,
            formulation: self.formulation,
            row_ordering: self.row_ordering,
            dataset_path: self.dataset_path,
            input_format: self.input_format,
            write,
            threads: self.threads,
            input_tables: coverage.map(|coverage| coverage.input_tables),
            samples: coverage.map(|coverage| coverage.samples),
            rows_written: None,
            run_ns: elapsed_ns(self.started.instant),
            execute_ns: None,
            peak_rss_bytes: process::peak_rss_bytes()?,
            probe: None,
        })
    }
}

/// Wall-clock nanoseconds since `since`, saturating at `u64::MAX`.
fn elapsed_ns(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
