//! The backend: the Rust side that takes what a frontend sends and runs it. See ADR 0022.
//!
//! A [`Backend`] opens datasets for a frontend to plan against. Opening one hands out a scan
//! description per input table, each in a schema-only table provider the frontend's plans read in
//! place of the input table, and the dataset's stored ordering as plain sort columns. Plan bytes a
//! frontend encodes with the backend's codec hold those descriptions, which the backend reads back.
//!
//! [`Backend::start`] runs plan bytes with an [`Action`] and a settings map, and the [`Run`] it
//! hands back waits for the run's [`Outcome`]. Decoding leaves each scan a placeholder holding its
//! description; the run reopens each as the sorted-table scan the Rust formulations read, then
//! holds the plan in the Action's ordering with an ordering assertion at its root, every sort the
//! frontend wrote an established order, before the logical optimizer sees it.
//!
//! The backend reaches object stores only through the [`ObjectStoreResolver`] it is built with:
//! [`ResolveFromUrls`] in production, or a suite's in-memory stores. Every call that touches a
//! store runs through the pipeline runner.
//!
//! Every call carries the [`Versions`] the frontend speaks and checks them against the backend's
//! own. Every failure is a [`BackendError`] with a stable [`ErrorCode`].
//!
//! - [`codec`] is the logical extension codec scan descriptions cross in.
//! - [`scan_description`] holds scan descriptions and the schema-only provider that holds one.
//! - [`scan_reopening`] reopens a decoded plan's described scans.

pub mod codec;
pub mod scan_description;
pub mod scan_reopening;

#[cfg(test)]
mod tests;

use crate::{
    format::InputFormat,
    ordering_assertion::{self, AssertionScope, OrderingAssertionFailed},
    pipeline::{self, PipelineOptions},
    row_ordering::{RowOrdering, SortColumn},
    sink,
    stored::dataset::Dataset,
};
use codec::Codec;
use scan_description::{DescribedScan, ScanDescription};

use datafusion::{
    DATAFUSION_VERSION,
    arrow::{
        array::StringArray, datatypes::SchemaRef, record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    },
    catalog::TableProvider,
    common::{DataFusionError, TableReference},
    datasource::listing::ListingTableUrl,
    error::Result,
    execution::TaskContext,
    logical_expr::{AggregateUDF, Extension, LogicalPlan, ScalarUDF, WindowUDF},
    prelude::{DataFrame, SessionContext},
};
use datafusion_proto::{
    bytes::logical_plan_from_bytes_with_extension_codec, logical_plan::LogicalExtensionCodec,
};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt, mem,
    num::NonZeroUsize,
    slice,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};
use uuid::Uuid;

/// Where a backend's runs find the object stores the paths they read and write live on, and the
/// credentials to reach them.
pub trait ObjectStoreResolver: fmt::Debug + Send + Sync {
    /// Options for a run with `threads` threads per runtime that reads and writes `paths`, which
    /// register the stores those paths live on.
    ///
    /// # Errors
    ///
    /// Returns an error naming a path on a store the resolver cannot serve.
    fn pipeline_options(&self, threads: NonZeroUsize, paths: &[&str]) -> Result<PipelineOptions>;
}

/// The production resolver: a store for each path's URL, with credentials from the environment,
/// as [`PipelineOptions::for_paths`] resolves them.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResolveFromUrls;

impl ObjectStoreResolver for ResolveFromUrls {
    fn pipeline_options(&self, threads: NonZeroUsize, paths: &[&str]) -> Result<PipelineOptions> {
        PipelineOptions::for_paths(threads, paths.iter().copied())
    }
}

/// The in-process backend.
#[derive(Debug)]
pub struct Backend {
    object_stores: Arc<dyn ObjectStoreResolver>,
    /// Whether runs check at run time that rows arrive in the order every ordering assertion and
    /// established order holds.
    verify_order: bool,
}

/// A dataset a frontend plans against, as [`Backend::open_dataset`] hands it out.
#[derive(Debug)]
pub struct OpenedDataset {
    /// The dataset's input tables, in name order.
    pub input_tables: Vec<InputTableScan>,
    /// The schema every scan reads: the dataset's rows with their sample.
    pub schema: SchemaRef,
    /// The order the dataset's rows are stored in, under the row ordering it was opened with.
    pub ordering: Vec<SortColumn>,
}

/// One input table of an opened dataset.
#[derive(Debug)]
pub struct InputTableScan {
    /// The samples the input table covers, in sorted order.
    pub sample_set: Vec<String>,
    /// The table provider a frontend plan reads in place of the input table.
    pub scan: Arc<DescribedScan>,
}

impl Backend {
    #[must_use]
    pub fn new(object_stores: Arc<dyn ObjectStoreResolver>) -> Self {
        Self {
            object_stores,
            verify_order: false,
        }
    }

    /// This backend, with every run also checking at run time that rows arrive in the order each
    /// ordering assertion and established order holds, failing at the first that doesn't, as
    /// [`ordering_assertion::verify_order`] does. It catches an order the optimizer believed in but
    /// the rows lack, which no planning check can (issue #325), at the cost of comparing every
    /// row's keys.
    #[must_use]
    pub fn verifying_order(self) -> Self {
        Self {
            verify_order: true,
            ..self
        }
    }

    /// Opens the dataset at `path`, discovering it as [`Dataset::discover`] does, stored in
    /// `input_format` and declared in `row_ordering`.
    ///
    /// Each scan description it hands out names `path` as given, so a backend reopens it the way
    /// it was opened.
    ///
    /// # Errors
    ///
    /// Returns an [`ErrorCode::InvalidAction`] error if `versions` are not the backend's, and an
    /// [`ErrorCode::Backend`] error if the resolver cannot serve `path`, discovery fails, an input
    /// table cannot be read, or the input tables' scans read different schemas.
    pub fn open_dataset(
        &self,
        path: &str,
        input_format: InputFormat,
        row_ordering: RowOrdering,
        versions: Versions,
    ) -> Result<OpenedDataset, BackendError> {
        versions.check()?;
        let options = self
            .object_stores
            .pipeline_options(pipeline::default_thread_count(), &[path])?;
        let path = path.to_string();
        pipeline::run(
            move |ctx| async move {
                let dataset = Dataset::discover(
                    &ctx,
                    ListingTableUrl::parse(&path)?,
                    input_format.clone(),
                    row_ordering.clone(),
                    None,
                )
                .await?;
                let ordering = dataset.query_ordering(&row_ordering)?.sort_columns();
                let mut input_tables = Vec::with_capacity(dataset.input_tables().len());
                for input_table in dataset.input_tables() {
                    let name = input_table.name().to_string();
                    let frame = dataset
                        .restrict_to(slice::from_ref(&name))?
                        .read(&ctx)
                        .await?;
                    let description = ScanDescription {
                        dataset_path: path.clone(),
                        input_format: input_format.clone(),
                        row_ordering: row_ordering.clone(),
                        input_table: name,
                    };
                    input_tables.push(InputTableScan {
                        sample_set: input_table.sample_set().to_vec(),
                        scan: Arc::new(DescribedScan::new(
                            description,
                            Arc::clone(frame.schema().inner()),
                        )),
                    });
                }
                let schema = scan_schema(&path, &input_tables)?;
                Ok(OpenedDataset {
                    input_tables,
                    schema,
                    ordering,
                })
            },
            options,
        )
        .map_err(BackendError::from)
    }

    /// The scan descriptions `plan_bytes` read, each once, in the order the plan first reads them.
    /// The bytes are a logical plan a frontend encoded with the backend's [codec].
    ///
    /// # Errors
    ///
    /// Returns an [`ErrorCode::InvalidAction`] error if `versions` are not the backend's or the
    /// plan carries a payload the codec refuses, and an [`ErrorCode::Backend`] error if the bytes
    /// are not otherwise such a plan, or it reads a table that is not a scan description.
    pub fn scan_descriptions(
        &self,
        plan_bytes: &[u8],
        versions: Versions,
    ) -> Result<Vec<ScanDescription>, BackendError> {
        versions.check()?;
        Ok(scan_reopening::descriptions(&decode(plan_bytes)?)?)
    }

    /// Starts a run of `plan_bytes`, a logical plan a frontend encoded with the backend's
    /// [codec], that performs `action` under `settings`, as `run_id` or under a generated id, and
    /// hands back the run. The run goes on in the background until it ends; dropping the handle
    /// doesn't stop it.
    ///
    /// The plan's functions resolve by name from the backend's registry. The run reopens each scan
    /// as the sorted-table scan of its input table, listing the input table's files when it does,
    /// holds the plan in the Action's ordering, and runs it through the pipeline runner.
    ///
    /// # Errors
    ///
    /// Returns an [`ErrorCode::InvalidAction`] error, before the run starts, if `versions` are not
    /// the backend's, a setting is unknown or malformed, the Action's ordering is empty, or the
    /// plan carries a payload framed at another wire version or an inlined Python function; and
    /// an [`ErrorCode::Backend`] error if the bytes are not otherwise a plan the backend can
    /// decode, the resolver can't serve the datasets the plan reads, or the run can't start.
    pub fn start(
        &self,
        plan_bytes: &[u8],
        action: Action,
        settings: &Settings,
        run_id: Option<String>,
        versions: Versions,
    ) -> Result<Run, BackendError> {
        versions.check()?;
        let settings = RunSettings::parse(settings)?;
        action.check()?;
        let plan = decode(plan_bytes)?;
        let descriptions = scan_reopening::descriptions(&plan)?;
        let paths = descriptions
            .iter()
            .map(|description| description.dataset_path.as_str())
            .collect::<Vec<_>>();
        let options = self
            .object_stores
            .pipeline_options(settings.threads, &paths)?;
        let run_id = run_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let verify_order = self.verify_order;
        let thread =
            thread::Builder::new()
                .name(format!("hail_df run {run_id}"))
                .spawn(move || {
                    pipeline::run(
                        move |ctx| async move {
                            execute(ctx, plan, action, settings, verify_order).await
                        },
                        options,
                    )
                })
                .map_err(|error| BackendError::from(DataFusionError::External(Box::new(error))))?;
        Ok(Run { run_id, thread })
    }
}

/// The plan `plan_bytes` encode, decoded with the backend's [codec] on a session of the shared
/// configuration, whose registry the plan's functions resolve from. Every scan is a placeholder.
///
/// The decoder flattens some of the codec's errors into text, so a failed decoding fails with the
/// codec's first [`ErrorCode::InvalidAction`] error, if it had one.
fn decode(plan_bytes: &[u8]) -> Result<LogicalPlan> {
    let ctx = SessionContext::new_with_config(pipeline::session_config());
    let codec = KeepingRefusal::default();
    logical_plan_from_bytes_with_extension_codec(plan_bytes, &ctx.task_ctx(), &codec)
        .map_err(|error| codec.take().unwrap_or(error))
}

/// The backend's [codec], keeping aside the first error it decodes with that fails a call as
/// [`ErrorCode::InvalidAction`].
#[derive(Debug, Default)]
struct KeepingRefusal {
    refusal: Mutex<Option<DataFusionError>>,
}

impl KeepingRefusal {
    /// `result`, its error kept aside first if it is the first refusal.
    fn kept<T>(&self, result: Result<T>) -> Result<T> {
        result.inspect_err(|error| {
            if ErrorCode::of(error) == ErrorCode::InvalidAction
                && let Ok(mut refusal) = self.refusal.lock()
                && refusal.is_none()
            {
                *refusal = Some(match error.find_root() {
                    DataFusionError::External(source) => invalid_action(source.to_string()),
                    root => invalid_action(root.to_string()),
                });
            }
        })
    }

    fn take(&self) -> Option<DataFusionError> {
        self.refusal.lock().ok()?.take()
    }
}

impl LogicalExtensionCodec for KeepingRefusal {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.kept(Codec.try_decode(buf, inputs, ctx))
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        Codec.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        self.kept(Codec.try_decode_table_provider(buf, table_ref, schema, ctx))
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        Codec.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.kept(Codec.try_decode_udf(name, buf))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        Codec.try_encode_udf(node, buf)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.kept(Codec.try_decode_udaf(name, buf))
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.kept(Codec.try_decode_udwf(name, buf))
    }
}

/// Runs `plan` on `ctx` as `action` asks, under `settings`, checking the order of its rows at run
/// time if it should `verify_order`.
async fn execute(
    ctx: SessionContext,
    plan: LogicalPlan,
    action: Action,
    settings: RunSettings,
    verify_order: bool,
) -> Result<Outcome> {
    settings.apply(&ctx, verify_order);
    let frontend_plan = plan.display_indent().to_string();
    let plan = pipeline::until_cancelled(&ctx, scan_reopening::reopen(&ctx, plan)).await?;
    let frame = DataFrame::new(ctx.state(), plan);
    match action {
        Action::Collect { ordering } => {
            let schema = Arc::clone(frame.schema().inner());
            let held = ordering_assertion::hold(frame, &ordering, AssertionScope::OnePartition)?;
            let (frame, sink) = sink::collect(held)?;
            pipeline::until_cancelled(&ctx, frame.collect()).await?;
            Ok(Outcome::Rows {
                schema,
                batches: sink.take(),
            })
        }
        Action::Explain { ordering } => {
            let held = ordering_assertion::hold(frame, &ordering, AssertionScope::OnePartition)?;
            explain(&frontend_plan, sink::drain(held)?).await
        }
    }
}

/// The plan text of `frame`: `frontend_plan`, then its logical and physical plans. The physical
/// plan is built first, so a plan that fails planning fails the explain rather than rendering the
/// failure.
async fn explain(frontend_plan: &str, frame: DataFrame) -> Result<Outcome> {
    frame.clone().create_physical_plan().await?;
    let explained = frame.explain(false, false)?.collect().await?;
    let schema = explained.first().map_or_else(
        || {
            Err(DataFusionError::Internal(
                "an explain returned no batches".to_string(),
            ))
        },
        |batch| Ok(batch.schema()),
    )?;
    let frontend = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["frontend_plan"])),
            Arc::new(StringArray::from(vec![frontend_plan])),
        ],
    )?;
    let batches = [vec![frontend], explained].concat();
    Ok(Outcome::Plan(pretty_format_batches(&batches)?.to_string()))
}

/// What a run does with the plan's rows.
///
/// Every Action holds the rows in its ordering, plain sort columns, with an ordering assertion at
/// the plan's root over one partition, and every sort the frontend wrote becomes an established
/// order. A plan that could meet the assertion only by a
/// sort the backend inserted fails before it executes, with
/// [`ErrorCode::OrderingAssertionFailed`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    /// Collect the rows, in `ordering`, into the Outcome.
    Collect { ordering: Vec<SortColumn> },
    /// Render the plan of a run that drains the rows in `ordering`, without running it: the
    /// frontend plan as decoded, then the logical and physical plans.
    Explain { ordering: Vec<SortColumn> },
}

impl Action {
    /// Checks what the Action asks for before a run starts.
    fn check(&self) -> Result<(), BackendError> {
        let (Self::Collect { ordering } | Self::Explain { ordering }) = self;
        if ordering.is_empty() {
            return Err(BackendError::invalid_action(
                "the Action's ordering has no sort columns, and a run holds its rows in at least one",
            ));
        }
        Ok(())
    }
}

/// What a run hands back once it ends.
#[derive(Debug)]
pub enum Outcome {
    /// A collect's rows, in its ordering, and their schema.
    Rows {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
    /// An explain's plan text.
    Plan(String),
}

/// A run's settings: string values by key, every key in the `hail_df.` namespace.
///
/// - `hail_df.threads`: the threads of each of the run's runtimes; default, available parallelism.
/// - `hail_df.target_partitions`: the session's target partitions; default, the shared session's.
/// - `hail_df.repartition_file_min_size`: the least file size, in bytes, the optimizer splits a
///   scan of across partitions; default, the shared session's.
///
/// Every other key is an [`ErrorCode::InvalidAction`] error. The session otherwise keeps the
/// shared configuration of ADR 0003.
pub type Settings = BTreeMap<String, String>;

/// A run's settings, parsed.
#[derive(Clone, Copy, Debug)]
struct RunSettings {
    threads: NonZeroUsize,
    target_partitions: Option<NonZeroUsize>,
    repartition_file_min_size: Option<usize>,
}

impl RunSettings {
    fn parse(settings: &Settings) -> Result<Self, BackendError> {
        let mut parsed = Self {
            threads: pipeline::default_thread_count(),
            target_partitions: None,
            repartition_file_min_size: None,
        };
        for (key, value) in settings {
            match key.as_str() {
                "hail_df.threads" => parsed.threads = setting(key, value)?,
                "hail_df.target_partitions" => {
                    parsed.target_partitions = Some(setting(key, value)?);
                }
                "hail_df.repartition_file_min_size" => {
                    parsed.repartition_file_min_size = Some(setting(key, value)?);
                }
                _ => {
                    return Err(BackendError::invalid_action(&format!(
                        "unknown setting '{key}': a run takes hail_df.threads, \
                         hail_df.target_partitions and hail_df.repartition_file_min_size"
                    )));
                }
            }
        }
        Ok(parsed)
    }

    /// Sets the session options among these settings on `ctx`'s session, and the run-time order
    /// check if it should `verify_order`.
    fn apply(self, ctx: &SessionContext, verify_order: bool) {
        let state = ctx.state_ref();
        let mut state = state.write();
        let config = state.config_mut();
        if verify_order {
            *config = ordering_assertion::verify_order(mem::take(config));
        }
        let options = config.options_mut();
        if let Some(target_partitions) = self.target_partitions {
            options.execution.target_partitions = target_partitions.get();
        }
        if let Some(size) = self.repartition_file_min_size {
            options.optimizer.repartition_file_min_size = size;
        }
        drop(state);
    }
}

/// The value of setting `key`, parsed from `value`.
fn setting<T: std::str::FromStr>(key: &str, value: &str) -> Result<T, BackendError>
where
    T::Err: fmt::Display,
{
    value.parse().map_err(|error| {
        BackendError::invalid_action(&format!("setting '{key}' = '{value}' is invalid: {error}"))
    })
}

/// A started run: its id, and the run itself, going on in the background.
#[derive(Debug)]
pub struct Run {
    run_id: String,
    thread: JoinHandle<Result<Outcome>>,
}

impl Run {
    /// The run's id, as supplied to [`Backend::start`] or generated there.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Blocks until the run ends, and hands back its Outcome.
    ///
    /// # Errors
    ///
    /// Returns the run's failure: [`ErrorCode::OrderingAssertionFailed`] if the plan could meet an
    /// assertion only by a sort the backend inserted, [`ErrorCode::RunCancelled`] if the run was
    /// cancelled, and otherwise [`ErrorCode::Backend`].
    pub fn wait(self) -> Result<Outcome, BackendError> {
        match self.thread.join() {
            Ok(outcome) => Ok(outcome?),
            Err(panic) => Err(BackendError {
                code: ErrorCode::Backend,
                message: format!(
                    "run {} panicked: {}",
                    self.run_id,
                    panic
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                        .unwrap_or("with no message")
                ),
            }),
        }
    }
}

/// The versions a frontend speaks, which every call carries.
///
/// They are the `DataFusion` major its plans are built on, and the version of the `DFPYCHN` frame
/// its `datafusion` wheel wraps payloads in. A call fails with [`ErrorCode::InvalidAction`] unless
/// they are the backend's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Versions {
    pub datafusion_major: u64,
    pub wire_version: u8,
}

impl Versions {
    /// The backend's own versions: its `DataFusion` major and the frame version its codec reads.
    #[must_use]
    pub fn backend() -> Self {
        Self {
            datafusion_major: DATAFUSION_VERSION
                .split('.')
                .next()
                .and_then(|major| major.parse().ok())
                .unwrap_or_default(),
            wire_version: codec::WIRE_VERSION,
        }
    }

    fn check(self) -> Result<(), BackendError> {
        let backend = Self::backend();
        if self.datafusion_major != backend.datafusion_major {
            return Err(BackendError::invalid_action(&format!(
                "the frontend builds plans on DataFusion {}, and the backend runs DataFusion {}: \
                 align the datafusion wheel with the backend",
                self.datafusion_major, backend.datafusion_major
            )));
        }
        if self.wire_version != backend.wire_version {
            return Err(BackendError::invalid_action(&format!(
                "the frontend frames payloads at DFPYCHN version {}, and the backend reads \
                 version {}: align the datafusion wheel with the backend",
                self.wire_version, backend.wire_version
            )));
        }
        Ok(())
    }
}

/// The kind of a backend failure, as a stable code a frontend maps to its own error.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorCode {
    /// The call asked for something the backend doesn't take: versions other than its own,
    /// an unknown setting, a malformed Action, or a plan carrying a payload it refuses.
    InvalidAction,
    /// The plan could meet an ordering assertion only by a sort the backend inserted.
    OrderingAssertionFailed,
    /// The run was cancelled.
    RunCancelled,
    /// Any other failure.
    Backend,
}

impl ErrorCode {
    /// The code's stable name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::InvalidAction => "invalid_action",
            Self::OrderingAssertionFailed => "ordering_assertion_failed",
            Self::RunCancelled => "run_cancelled",
            Self::Backend => "backend_error",
        }
    }

    /// The code of `error`, from what lies beneath any context it was wrapped in.
    #[must_use]
    pub fn of(error: &DataFusionError) -> Self {
        if OrderingAssertionFailed::find(error).is_some() {
            Self::OrderingAssertionFailed
        } else if pipeline::is_cancelled(error) {
            Self::RunCancelled
        } else if matches!(
            error.find_root(),
            DataFusionError::External(source) if source.is::<InvalidAction>()
        ) {
            Self::InvalidAction
        } else {
            Self::Backend
        }
    }
}

/// A failure of a backend call or run: its code and its message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendError {
    code: ErrorCode,
    message: String,
}

impl BackendError {
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    fn invalid_action(message: &str) -> Self {
        Self {
            code: ErrorCode::InvalidAction,
            message: message.to_string(),
        }
    }
}

impl From<DataFusionError> for BackendError {
    fn from(error: DataFusionError) -> Self {
        Self {
            code: ErrorCode::of(&error),
            message: error.to_string(),
        }
    }
}

impl fmt::Display for BackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code.name(), self.message)
    }
}

impl Error for BackendError {}

/// The source of an error a call fails with as [`ErrorCode::InvalidAction`], from beneath the
/// decoder or wherever else it arises.
#[derive(Debug)]
struct InvalidAction(String);

impl fmt::Display for InvalidAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for InvalidAction {}

/// An error that fails its call as [`ErrorCode::InvalidAction`], with `message`.
fn invalid_action(message: String) -> DataFusionError {
    DataFusionError::External(Box::new(InvalidAction(message)))
}

/// The schema every one of `input_tables`' scans reads.
fn scan_schema(path: &str, input_tables: &[InputTableScan]) -> Result<SchemaRef> {
    let mut schemas = input_tables.iter().map(|table| table.scan.schema_ref());
    let Some(schema) = schemas.next() else {
        return Err(DataFusionError::Internal(format!(
            "dataset '{path}' was opened with no input tables"
        )));
    };
    if let Some(other) = schemas.find(|other| *other != schema) {
        return Err(DataFusionError::Internal(format!(
            "the input tables of dataset '{path}' read different schemas: {schema:?} and {other:?}"
        )));
    }
    Ok(Arc::clone(schema))
}
