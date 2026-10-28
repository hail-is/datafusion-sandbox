//! PROTOTYPE, throwaway, for hail-is/datafusion-sandbox#321: can a grouped-merge plan built in
//! Python with the stock `datafusion` wheel reach the backend and plan as the Rust plan builder's?
//!
//! Both halves of the seam live in this one cdylib, as they would in the shipped client:
//!
//! Frontend pieces the wheel loads:
//! - `ScanTable`: one input table as a schema-only table provider holding its scan description.
//!   It can be planned against but never scanned.
//! - `SeamCodec`: a logical codec that writes a `ScanTable`'s description as its payload.
//!
//! The in-process backend:
//! - `Backend.open_dataset`: discovers a dataset and returns a `ScanTable` per input table, and
//!   the ordering its rows are read in.
//! - `Backend.execute`: decodes plan bytes, reopens every described scan as the crate's real
//!   sorted table, runs the plan into the Action's sink through `pipeline::run`, and returns the
//!   physical plan.
//! - `Backend.rust_grouped_merge`: the oracle, the same Action over the Rust grouped-merge plan
//!   builder's frame.
//!
//! Every backend call releases the GIL around `pipeline::run`.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, TableReference, exec_err, not_impl_err};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{TableType, provider_as_source, source_as_provider};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, TableScan};
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_proto::bytes::logical_plan_from_bytes_with_extension_codec;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_sandbox::format::{InputFormat, OutputFormat};
use datafusion_sandbox::formulation::Formulation;
use datafusion_sandbox::ordered_frame::{OrderedFrame, OutputLayout};
use datafusion_sandbox::pipeline::{self, PipelineOptions};
use datafusion_sandbox::row_ordering::{RowOrdering, StoredOrdering};
use datafusion_sandbox::sink;
use datafusion_sandbox::stored::dataset::Dataset;
use datafusion_sandbox::testing::fixture::{self, DiskDatasetFixture, FixtureFormat};
use datafusion_sandbox::write::WriteTarget;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule};

const CODEC_ID: &str = "hail.seam";
const SCAN_MAGIC: &str = "HAILSCAN1";

// ---------------------------------------------------------------------------------------------
// The scan description: what crosses the seam for each scan, and what any backend reopens.

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ScanDescription {
    table_path: String,
    format: String,
    row_ordering: String,
    input_table: String,
}

impl ScanDescription {
    fn encode(&self) -> Vec<u8> {
        [
            SCAN_MAGIC,
            &self.table_path,
            &self.format,
            &self.row_ordering,
            &self.input_table,
        ]
        .join("\n")
        .into_bytes()
    }

    fn decode(blob: &[u8]) -> Result<Self> {
        let text = std::str::from_utf8(blob).map_err(|e| DataFusionError::External(e.into()))?;
        match text.split('\n').collect::<Vec<_>>().as_slice() {
            [SCAN_MAGIC, table_path, format, row_ordering, input_table] => Ok(Self {
                table_path: (*table_path).to_string(),
                format: (*format).to_string(),
                row_ordering: (*row_ordering).to_string(),
                input_table: (*input_table).to_string(),
            }),
            _ => exec_err!("not a Hail scan description"),
        }
    }

    /// The dataset this description's input table belongs to, without the input-table selection.
    fn dataset_key(&self) -> (String, String, String) {
        (
            self.table_path.clone(),
            self.format.clone(),
            self.row_ordering.clone(),
        )
    }
}

/// A table provider that only knows its description and schema. The frontend plans against it;
/// the backend decodes into it and then swaps in the real sorted table.
#[derive(Debug)]
struct DescribedScan {
    description: ScanDescription,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for DescribedScan {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        exec_err!(
            "the scan of input table '{}' is a description; only the backend opens it",
            self.description.input_table
        )
    }
}

// ---------------------------------------------------------------------------------------------
// The codec. One impl serves both sides: the frontend encodes, the backend decodes.

#[derive(Debug)]
struct SeamCodecImpl;

impl LogicalExtensionCodec for SeamCodecImpl {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> Result<Extension> {
        not_impl_err!("frontend plans hold stock nodes only")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("frontend plans hold stock nodes only")
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let description = ScanDescription::decode(strip_envelope(buf)?)?;
        Ok(Arc::new(DescribedScan {
            description,
            schema,
        }))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let Some(scan) = node.downcast_ref::<DescribedScan>() else {
            return exec_err!("not a Hail scan");
        };
        buf.extend_from_slice(&scan.description.encode());
        Ok(())
    }
}

/// datafusion-python 55 wraps every payload an installed codec writes as
/// `DFPYCHN | version: u8 | id_len: u32 LE | id | blob`.
fn strip_envelope(buf: &[u8]) -> Result<&[u8]> {
    let Some(rest) = buf.strip_prefix(b"DFPYCHN".as_slice()) else {
        return exec_err!("payload has no DFPYCHN envelope");
    };
    let [1, a, b, c, d, rest @ ..] = rest else {
        return exec_err!("unknown or truncated DFPYCHN envelope");
    };
    let len = u32::from_le_bytes([*a, *b, *c, *d]) as usize;
    if rest.len() < len {
        return exec_err!("truncated DFPYCHN envelope");
    }
    let (id, blob) = rest.split_at(len);
    if id != CODEC_ID.as_bytes() {
        return exec_err!(
            "payload written by codec '{}', not '{CODEC_ID}'",
            String::from_utf8_lossy(id)
        );
    }
    Ok(blob)
}

// ---------------------------------------------------------------------------------------------
// Backend helpers.

fn row_ordering(name: &str) -> Result<RowOrdering> {
    match name {
        "locus" => Ok(RowOrdering::locus()),
        "locus_then_alleles" => Ok(RowOrdering::locus_then_alleles()),
        "locus_then_sample" => Ok(RowOrdering::locus_then_sample()),
        _ => exec_err!("unknown row ordering '{name}'"),
    }
}

fn input_format(name: &str) -> Result<InputFormat> {
    match name {
        "parquet" => Ok(InputFormat::PARQUET),
        "vortex" => Ok(InputFormat::VORTEX),
        _ => exec_err!("unknown input format '{name}'"),
    }
}

fn output_format(name: &str) -> Result<OutputFormat> {
    match name {
        "parquet" => Ok(OutputFormat::PARQUET),
        "vortex" => Ok(OutputFormat::VORTEX),
        _ => exec_err!("unknown output format '{name}'"),
    }
}

async fn discover(ctx: &SessionContext, path: &str, format: &str, ordering: &str) -> Result<Dataset> {
    Dataset::discover(
        ctx,
        ListingTableUrl::parse(path)?,
        input_format(format)?,
        row_ordering(ordering)?,
        None,
    )
    .await
}

/// The source of the one table scan in `frame`: the crate's sorted table for one input table.
fn only_scan_provider(frame: &DataFrame) -> Result<Arc<dyn TableProvider>> {
    let mut found = None;
    frame.logical_plan().apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            found = Some(source_as_provider(&scan.source));
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    found.unwrap_or_else(|| exec_err!("an input table read holds no table scan"))
}

/// Replaces every described scan in `plan` with the real sorted table it describes. The codec
/// cannot do this itself: it decodes synchronously, and opening an input table lists its files.
async fn reopen_scans(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> Result<(LogicalPlan, Vec<(ScanDescription, Dataset)>)> {
    let mut descriptions = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(described) = source_as_provider(&scan.source)?.downcast_ref::<DescribedScan>()
        {
            descriptions.push(described.description.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut datasets = BTreeMap::new();
    let mut providers = BTreeMap::new();
    let mut opened = Vec::new();
    for description in descriptions {
        if providers.contains_key(&description) {
            continue;
        }
        let key = description.dataset_key();
        if !datasets.contains_key(&key) {
            let dataset = discover(ctx, &key.0, &key.1, &key.2).await?;
            datasets.insert(key.clone(), dataset);
        }
        let dataset = &datasets[&key];
        let frame = dataset
            .restrict_to(std::slice::from_ref(&description.input_table))?
            .read(ctx)
            .await?;
        providers.insert(description.clone(), only_scan_provider(&frame)?);
        opened.push((description, dataset.clone()));
    }

    let plan = plan
        .transform_up(|node| {
            let LogicalPlan::TableScan(scan) = node else {
                return Ok(Transformed::no(node));
            };
            let Some(described) = source_as_provider(&scan.source)?
                .downcast_ref::<DescribedScan>()
                .map(|d| d.description.clone())
            else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            let real = Arc::clone(&providers[&described]);
            if real.schema() != scan.source.schema() {
                return exec_err!(
                    "input table '{}' reopened with a different schema",
                    described.input_table
                );
            }
            Ok(Transformed::yes(LogicalPlan::TableScan(TableScan {
                source: provider_as_source(real),
                ..scan
            })))
        })?
        .data;
    Ok((plan, opened))
}

/// The stored ordering the Action's ordering names, checked against the reopened datasets. The
/// crate's sinks take a `StoredOrdering`, not a sort list, so the backend has to recover one.
fn stored_ordering(
    action_ordering: &[(String, bool, bool)],
    opened: &[(ScanDescription, Dataset)],
) -> Result<StoredOrdering> {
    let Some((description, dataset)) = opened.first() else {
        return exec_err!("the plan scans no Hail dataset");
    };
    let stored = dataset.query_ordering(&row_ordering(&description.row_ordering)?)?;
    let expected = stored
        .sort_expressions()
        .into_iter()
        .map(|sort| (sort.expr.to_string(), sort.asc, sort.nulls_first))
        .collect::<Vec<_>>();
    if expected != action_ordering {
        return exec_err!(
            "the Action's ordering {action_ordering:?} is not the datasets' {expected:?}"
        );
    }
    Ok(stored)
}

fn layout(name: &str) -> Result<OutputLayout> {
    match name {
        "single_file" => Ok(OutputLayout::SingleFile),
        "file_per_partition" => Ok(OutputLayout::FilePerPartition),
        _ => exec_err!("unknown output layout '{name}'"),
    }
}

/// Runs `ordered` into the Action's sink: planned only for `explain`, executed for `write`.
async fn run_action(
    ordered: OrderedFrame,
    kind: &str,
    output_path: &str,
    output_format_name: &str,
) -> Result<(String, Option<u64>)> {
    let target = WriteTarget {
        output_path: output_path.to_string(),
        output_format: output_format(output_format_name)?,
    };
    let frame = target.sink_frame(ordered)?;
    match kind {
        "explain" => {
            let plan = frame.create_physical_plan().await?;
            Ok((displayable(plan.as_ref()).indent(true).to_string(), None))
        }
        "write" => {
            let executed = sink::execute_and_retain(frame).await?;
            Ok((
                displayable(executed.plan.as_ref()).indent(true).to_string(),
                Some(executed.rows_written),
            ))
        }
        _ => exec_err!("unknown Action '{kind}'"),
    }
}

/// Applies the Action's settings map to the backend's session. Unknown keys are errors.
fn apply_settings(ctx: &SessionContext, settings: &[(String, String)]) -> Result<()> {
    let state = ctx.state_ref();
    let mut state = state.write();
    for (key, value) in settings {
        state.config_mut().options_mut().set(key, value)?;
    }
    Ok(())
}

fn py_err(e: DataFusionError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ---------------------------------------------------------------------------------------------
// Python classes.

#[pyclass(name = "ScanTable", frozen)]
struct PyScanTable {
    scan: Arc<DescribedScan>,
}

#[pymethods]
impl PyScanTable {
    #[getter]
    fn description(&self) -> String {
        format!("{:?}", self.scan.description)
    }

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let capsule = session.call_method0("__datafusion_logical_extension_codec__")?;
        let capsule = capsule.cast::<PyCapsule>()?;
        let codec = capsule
            .pointer_checked(Some(c"datafusion_logical_extension_codec"))?
            .cast::<FFI_LogicalExtensionCodec>();
        // SAFETY: the capsule's name says it holds an FFI_LogicalExtensionCodec, and the session
        // keeps it alive for this call; we clone it rather than keep the pointer.
        let codec = unsafe { codec.as_ref() }.clone();
        let provider: Arc<dyn TableProvider> = self.scan.clone();
        let ffi = FFI_TableProvider::new_with_ffi_codec(provider, false, None, codec);
        PyCapsule::new_with_value(py, ffi, c"datafusion_table_provider")
    }
}

#[pyclass(name = "SeamCodec", frozen)]
struct PySeamCodec;

#[pymethods]
impl PySeamCodec {
    #[new]
    fn new() -> Self {
        Self
    }

    #[classattr]
    fn __datafusion_codec_id__() -> &'static str {
        CODEC_ID
    }

    #[pyo3(signature = (session=None))]
    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let Some(session) = session else {
            return Err(PyValueError::new_err("the seam codec needs the session"));
        };
        let capsule = session.call_method0("__datafusion_task_context_provider__")?;
        let capsule = capsule.cast::<PyCapsule>()?;
        let provider = capsule
            .pointer_checked(Some(c"datafusion_task_context_provider"))?
            .cast::<FFI_TaskContextProvider>();
        // SAFETY: as for the codec capsule above.
        let provider = unsafe { provider.as_ref() }.clone();
        let codec = FFI_LogicalExtensionCodec::new(Arc::new(SeamCodecImpl), None, provider);
        PyCapsule::new_with_value(py, codec, c"datafusion_logical_extension_codec")
    }
}

/// A dataset fixture on local disk, alive as long as this object.
#[pyclass(name = "Fixture", frozen)]
struct PyFixture {
    fixture: DiskDatasetFixture,
}

#[pymethods]
impl PyFixture {
    #[getter]
    fn path(&self) -> String {
        self.fixture.table_path().to_string()
    }
}

#[pyfunction]
fn make_fixture(py: Python<'_>, format: &str) -> PyResult<PyFixture> {
    let format = match format {
        "parquet" => FixtureFormat::Parquet,
        "vortex" => FixtureFormat::Vortex,
        _ => return Err(PyValueError::new_err(format!("unknown format '{format}'"))),
    };
    let fixture = py.detach(|| fixture::contig_position_disk_fixture(format));
    Ok(PyFixture { fixture })
}

type Ordering = Vec<(String, bool, bool)>;

#[pyclass(name = "Backend", frozen)]
struct PyBackend {
    threads: NonZeroUsize,
}

#[pymethods]
impl PyBackend {
    #[new]
    fn new(threads: usize) -> PyResult<Self> {
        let threads =
            NonZeroUsize::new(threads).ok_or_else(|| PyValueError::new_err("threads must be > 0"))?;
        Ok(Self { threads })
    }

    /// Opens a dataset: its input tables as scan tables, in name order, and the ordering their
    /// rows are read in, as `(column, ascending, nulls_first)`.
    fn open_dataset(
        &self,
        py: Python<'_>,
        path: String,
        format: String,
        row_ordering: String,
    ) -> PyResult<(Vec<(String, PyScanTable)>, Ordering)> {
        let threads = self.threads;
        let (tables, ordering) = py
            .detach(move || {
                let options = PipelineOptions::for_paths(threads, [path.as_str()])?;
                pipeline::run(
                    move |ctx| async move {
                        let dataset = discover(&ctx, &path, &format, &row_ordering).await?;
                        let mut tables = Vec::new();
                        for input_table in dataset.input_tables() {
                            let name = input_table.name().to_string();
                            let frame = dataset
                                .restrict_to(std::slice::from_ref(&name))?
                                .read(&ctx)
                                .await?;
                            let schema = Arc::clone(frame.schema().inner());
                            let description = ScanDescription {
                                table_path: path.clone(),
                                format: format.clone(),
                                row_ordering: row_ordering.clone(),
                                input_table: name.clone(),
                            };
                            tables.push((name, Arc::new(DescribedScan { description, schema })));
                        }
                        let ordering = dataset
                            .query_ordering(&self::row_ordering(&row_ordering)?)?
                            .sort_expressions()
                            .into_iter()
                            .map(|sort| (sort.expr.to_string(), sort.asc, sort.nulls_first))
                            .collect::<Vec<_>>();
                        Ok((tables, ordering))
                    },
                    options,
                )
            })
            .map_err(py_err)?;
        let tables = tables
            .into_iter()
            .map(|(name, scan)| (name, PyScanTable { scan }))
            .collect();
        Ok((tables, ordering))
    }

    /// Runs frontend plan bytes under an Action: `kind` is `explain` or `write`. Returns the
    /// physical plan and, for a write, the rows written.
    #[expect(clippy::too_many_arguments, reason = "prototype: the Action, spelled out")]
    #[pyo3(signature = (plan_bytes, *, kind, output_path, output_format, ordering, layout, settings))]
    fn execute(
        &self,
        py: Python<'_>,
        plan_bytes: Bound<'_, PyBytes>,
        kind: String,
        output_path: String,
        output_format: String,
        ordering: Ordering,
        layout: String,
        settings: Vec<(String, String)>,
    ) -> PyResult<(String, Option<u64>, String)> {
        let bytes = plan_bytes.as_bytes().to_vec();
        let threads = self.threads;
        py.detach(move || {
            let options = PipelineOptions::for_paths(threads, [output_path.as_str()])?;
            pipeline::run(
                move |ctx| async move {
                    apply_settings(&ctx, &settings)?;
                    let decoded = logical_plan_from_bytes_with_extension_codec(
                        &bytes,
                        &ctx.task_ctx(),
                        &SeamCodecImpl,
                    )?;
                    let decoded_text = decoded.display_indent().to_string();
                    let (plan, opened) = reopen_scans(&ctx, decoded).await?;
                    let ordered = OrderedFrame {
                        frame: DataFrame::new(ctx.state(), plan),
                        ordering: stored_ordering(&ordering, &opened)?,
                        layout: self::layout(&layout)?,
                    };
                    let (physical, rows) =
                        run_action(ordered, &kind, &output_path, &output_format).await?;
                    Ok((physical, rows, decoded_text))
                },
                options,
            )
        })
        .map_err(py_err)
    }

    /// The oracle: the same Action over the Rust grouped-merge plan builder's frame.
    #[expect(clippy::too_many_arguments, reason = "prototype")]
    #[pyo3(signature = (path, format, row_ordering, groups, *, kind, output_path, output_format, settings))]
    fn rust_grouped_merge(
        &self,
        py: Python<'_>,
        path: String,
        format: String,
        row_ordering: String,
        groups: usize,
        kind: String,
        output_path: String,
        output_format: String,
        settings: Vec<(String, String)>,
    ) -> PyResult<(String, Option<u64>)> {
        let threads = self.threads;
        let groups =
            NonZeroUsize::new(groups).ok_or_else(|| PyValueError::new_err("groups must be > 0"))?;
        py.detach(move || {
            let options =
                PipelineOptions::for_paths(threads, [path.as_str(), output_path.as_str()])?;
            pipeline::run(
                move |ctx| async move {
                    apply_settings(&ctx, &settings)?;
                    let dataset = discover(&ctx, &path, &format, &row_ordering).await?;
                    let ordered = Formulation::CombineRefsGroupedMerge { groups }
                        .plan(&ctx, &dataset)
                        .await?;
                    run_action(ordered, &kind, &output_path, &output_format).await
                },
                options,
            )
        })
        .map_err(py_err)
    }
}

#[pymodule(gil_used = false)]
fn hail_proto(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyScanTable>()?;
    m.add_class::<PySeamCodec>()?;
    m.add_class::<PyBackend>()?;
    m.add_class::<PyFixture>()?;
    m.add_function(wrap_pyfunction!(make_fixture, m)?)?;
    Ok(())
}
