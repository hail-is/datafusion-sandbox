//! `hail_df._native`: the Python bindings over the backend in `datafusion_sandbox::backend`.
//!
//! The bindings only marshal: values between Python and the backend, the FFI capsules the
//! `datafusion` wheel loads (the codec and each input table's schema-only scan), a collect's rows
//! through the Arrow C stream interface, and the backend's error codes as `BackendError` and its
//! subclasses. The Python package around them is the interface; see `docs/hail_df.md`.

use datafusion::{
    arrow::{
        datatypes::SchemaRef,
        ffi::FFI_ArrowSchema,
        ffi_stream::FFI_ArrowArrayStream,
        record_batch::{RecordBatch, RecordBatchIterator},
    },
    catalog::TableProvider,
    error::DataFusionError,
};
use datafusion_ffi::{
    execution::FFI_TaskContextProvider, proto::logical_extension_codec::FFI_LogicalExtensionCodec,
    table_provider::FFI_TableProvider, udf::FFI_ScalarUDF,
};
use datafusion_sandbox::{
    backend::{
        self, Action, ErrorCode, InputTableScan, OpenedDataset, Outcome, ResolveFromUrls, Run,
        Settings, Versions, Write,
        codec::{CODEC_ID, Codec},
        scan_description::{DescribedScan, ScanDescription},
    },
    metrics_directory::{FormulationRecord, MetricsDirectory},
    ordered_frame::OutputLayout,
    ordering_assertion::assert_ordered_udf,
    row_ordering::SortColumn,
};
use pyo3::{
    IntoPyObjectExt, create_exception,
    exceptions::PyException,
    prelude::*,
    types::{PyBytes, PyCapsule},
};
use std::{ffi::CStr, sync::Arc, time::Duration};

create_exception!(
    hail_df,
    BackendError,
    PyException,
    "A failure the backend reports."
);
create_exception!(
    hail_df,
    InvalidAction,
    BackendError,
    "A call the backend doesn't take: versions other than its own, an unknown setting, a \
     malformed Action, or a plan carrying a payload it refuses or an ordering assertion's marker \
     it can't read."
);

create_exception!(
    hail_df,
    OrderingAssertionFailed,
    BackendError,
    "A plan that could hold an ordering assertion only by a sort the backend inserted, or through \
     a node that keeps no input's order. Raised before the plan executes."
);

create_exception!(
    hail_df,
    RunCancelled,
    BackendError,
    "A run that was cancelled. It has no Outcome."
);

fn backend_error(error: &impl ToString) -> PyErr {
    BackendError::new_err(error.to_string())
}

/// The exception for `error`, by its code.
fn raised(error: &backend::BackendError) -> PyErr {
    match error.code() {
        ErrorCode::InvalidAction => InvalidAction::new_err(error.to_string()),
        ErrorCode::RunCancelled => RunCancelled::new_err(error.to_string()),
        ErrorCode::OrderingAssertionFailed => OrderingAssertionFailed::new_err(error.to_string()),
        ErrorCode::Backend => BackendError::new_err(error.to_string()),
    }
}

/// The in-process backend, resolving object stores and credentials from URLs.
#[pyclass(name = "Backend", frozen)]
struct PyBackend(backend::Backend);

#[pymethods]
impl PyBackend {
    #[new]
    fn new() -> Self {
        Self(backend::Backend::new(Arc::new(ResolveFromUrls)))
    }

    /// Opens the dataset at `path`, stored in `input_format` and declared in `row_ordering`, for a
    /// frontend on `DataFusion` major `datafusion_major` whose wheel frames payloads at
    /// `wire_version`.
    fn open_dataset(
        &self,
        py: Python<'_>,
        path: &str,
        input_format: &str,
        row_ordering: &str,
        datafusion_major: u64,
        wire_version: u8,
    ) -> PyResult<PyOpenedDataset> {
        let input_format = input_format.parse().map_err(|e| backend_error(&e))?;
        let row_ordering = row_ordering.parse().map_err(|e| backend_error(&e))?;
        let versions = Versions {
            datafusion_major,
            wire_version,
        };
        py.detach(|| {
            self.0
                .open_dataset(path, input_format, row_ordering, versions)
        })
        .map(PyOpenedDataset)
        .map_err(|e| raised(&e))
    }

    /// The scan descriptions `plan_bytes` read, each once, in the order the plan first reads them,
    /// for a frontend on `DataFusion` major `datafusion_major` whose wheel frames payloads at
    /// `wire_version`.
    fn scan_descriptions(
        &self,
        py: Python<'_>,
        plan_bytes: &Bound<'_, PyBytes>,
        datafusion_major: u64,
        wire_version: u8,
    ) -> PyResult<Vec<PyScanDescription>> {
        let plan_bytes = plan_bytes.as_bytes();
        let versions = Versions {
            datafusion_major,
            wire_version,
        };
        py.detach(|| self.0.scan_descriptions(plan_bytes, versions))
            .map(|descriptions| descriptions.into_iter().map(PyScanDescription).collect())
            .map_err(|e| raised(&e))
    }

    /// The plan bytes of the built-in formulation with the run record's `name`, `groups` and
    /// `split_points` over `dataset`, for a frontend on `DataFusion` major `datafusion_major` whose
    /// wheel frames payloads at `wire_version`.
    #[expect(
        clippy::too_many_arguments,
        reason = "Python's call carries each value plainly, as the formulation record holds them"
    )]
    fn builtin_plan<'py>(
        &self,
        py: Python<'py>,
        dataset: &PyOpenedDataset,
        name: String,
        groups: Option<usize>,
        split_points: Option<String>,
        datafusion_major: u64,
        wire_version: u8,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let formulation = FormulationRecord {
            name,
            groups,
            split_points,
        };
        let versions = Versions {
            datafusion_major,
            wire_version,
        };
        let bytes = py
            .detach(|| self.0.builtin_plan(&dataset.0, &formulation, versions))
            .map_err(|e| raised(&e))?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Starts a run of `plan_bytes` that performs `action` under `settings`, as `run_id` or under a
    /// generated id, for a frontend on `DataFusion` major `datafusion_major` whose wheel frames
    /// payloads at `wire_version`.
    #[expect(
        clippy::too_many_arguments,
        reason = "Python's call carries each value plainly, as `Backend::start` takes them"
    )]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "pyo3 extracts `settings` by value"
    )]
    #[pyo3(signature = (plan_bytes, action, settings, run_id, datafusion_major, wire_version))]
    fn start(
        &self,
        py: Python<'_>,
        plan_bytes: &Bound<'_, PyBytes>,
        action: &PyAction,
        settings: Settings,
        run_id: Option<String>,
        datafusion_major: u64,
        wire_version: u8,
    ) -> PyResult<PyRun> {
        let plan_bytes = plan_bytes.as_bytes();
        let action = action.0.clone();
        let versions = Versions {
            datafusion_major,
            wire_version,
        };
        py.detach(|| {
            self.0
                .start(plan_bytes, action, &settings, run_id, versions)
        })
        .map(PyRun)
        .map_err(|e| raised(&e))
    }

    /// Cancels the going run named `run_id`. An id no run has, or one whose run has ended, is not
    /// an error.
    fn cancel(&self, run_id: &str) {
        self.0.cancel(run_id);
    }
}

/// What a run does with the plan's rows, as the backend takes it.
#[pyclass(name = "Action", frozen)]
struct PyAction(Action);

#[pymethods]
impl PyAction {
    /// Collect the rows, in `ordering`, a list of `(column, ascending, nulls_first)` triples.
    #[staticmethod]
    fn collect(ordering: Vec<(String, bool, bool)>) -> Self {
        Self(Action::Collect {
            ordering: sort_columns(ordering),
        })
    }

    /// Render the plan of a run that drains the rows in `ordering`, or performs `write`, without
    /// running it.
    #[staticmethod]
    #[pyo3(signature = (ordering, write=None))]
    fn explain(ordering: Vec<(String, bool, bool)>, write: Option<&PyWrite>) -> Self {
        Self(Action::Explain {
            ordering: sort_columns(ordering),
            write: write.map(|write| write.0.clone()),
        })
    }

    /// Run the plan, draining the rows in `ordering` or performing `write`, and render it with
    /// per-operator metrics.
    #[staticmethod]
    #[pyo3(signature = (ordering, write=None))]
    fn explain_analyze(ordering: Vec<(String, bool, bool)>, write: Option<&PyWrite>) -> Self {
        Self(Action::ExplainAnalyze {
            ordering: sort_columns(ordering),
            write: write.map(|write| write.0.clone()),
        })
    }

    /// Perform `write`.
    #[staticmethod]
    fn write(write: &PyWrite) -> Self {
        Self(Action::Write(write.0.clone()))
    }

    /// Perform `write`, and record the run under `metrics_directory` with the formulation record of
    /// `name`, `groups` and `split_points`.
    #[staticmethod]
    fn measured_write(
        write: &PyWrite,
        metrics_directory: &str,
        name: String,
        groups: Option<usize>,
        split_points: Option<String>,
    ) -> Self {
        Self(Action::MeasuredWrite {
            write: write.0.clone(),
            metrics_directory: MetricsDirectory::new(metrics_directory),
            formulation: FormulationRecord {
                name,
                groups,
                split_points,
            },
        })
    }
}

/// A write of a plan's rows, as the backend takes it.
#[pyclass(name = "Write", frozen)]
struct PyWrite(Write);

#[pymethods]
impl PyWrite {
    /// A write to `output_path` in the format named `output_format`, with `compression` or the
    /// format's default, recording `ordering`, in the layout named `layout`, with a sample
    /// annotation table beside the file if it should `annotate_samples`.
    #[new]
    fn new(
        output_path: String,
        output_format: &str,
        compression: Option<&str>,
        ordering: Vec<(String, bool, bool)>,
        layout: &str,
        annotate_samples: bool,
    ) -> PyResult<Self> {
        let layout = match layout {
            "single_file" => OutputLayout::SingleFile,
            "file_per_partition" => OutputLayout::FilePerPartition,
            _ => {
                return Err(InvalidAction::new_err(format!(
                    "unknown output layout '{layout}'"
                )));
            }
        };
        Write::new(
            output_path,
            output_format,
            compression,
            sort_columns(ordering),
            layout,
            annotate_samples,
        )
        .map(Self)
        .map_err(|e| raised(&e))
    }
}

/// `ordering`, a list of `(column, ascending, nulls_first)` triples, as the backend takes it.
fn sort_columns(ordering: Vec<(String, bool, bool)>) -> Vec<SortColumn> {
    ordering
        .into_iter()
        .map(|(column, ascending, nulls_first)| SortColumn {
            column,
            ascending,
            nulls_first,
        })
        .collect()
}

/// How long a wait runs with the interpreter released before it next checks for signals: about as
/// long as Ctrl-C can take to reach a run.
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(50);

/// A started run. Every `wait` hands back the same.
#[pyclass(name = "Run", frozen)]
struct PyRun(Run);

#[pymethods]
impl PyRun {
    #[getter]
    fn run_id(&self) -> &str {
        self.0.run_id()
    }

    /// Blocks until the run ends, with the interpreter released, and hands back its Outcome: the
    /// rows of a collect, the plan text of an explain as a string, the rows a write wrote as an
    /// integer, or the rows a measured write wrote and the names of the metrics it could not record
    /// as a tuple.
    ///
    /// It checks for signals while it waits. When a signal's handler raises, as Ctrl-C's does, it
    /// cancels the run and raises that once the run has ended, so that a cancelled measured write
    /// has recorded itself by the time a script dies of it. A second such exception stops it
    /// waiting for that.
    fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut interrupted = None;
        let ended = loop {
            if let Err(signalled) = py.check_signals() {
                self.0.cancel();
                if interrupted.is_some() {
                    return Err(signalled);
                }
                interrupted = Some(signalled);
            }
            if let Some(ended) = py.detach(|| self.0.wait_timeout(SIGNAL_CHECK_INTERVAL)) {
                break ended;
            }
        };
        if let Some(signalled) = interrupted {
            return Err(signalled);
        }
        match ended {
            Ok(Outcome::Rows { schema, batches }) => {
                PyRows { schema, batches }.into_bound_py_any(py)
            }
            Ok(Outcome::Plan(plan)) => plan.into_bound_py_any(py),
            Ok(Outcome::RowsWritten(rows_written)) => rows_written.into_bound_py_any(py),
            Ok(Outcome::Measured {
                rows_written,
                unrecorded_metrics,
            }) => (rows_written, unrecorded_metrics).into_bound_py_any(py),
            Err(error) => Err(raised(&error)),
        }
    }
}

/// A collect's rows, exported through the Arrow C stream interface.
#[pyclass(name = "Rows", frozen)]
struct PyRows {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[pymethods]
impl PyRows {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        // A consumer may ask for another schema; these rows are in this one.
        let _ = requested_schema;
        let reader = RecordBatchIterator::new(
            self.batches.clone().into_iter().map(Ok),
            Arc::clone(&self.schema),
        );
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        PyCapsule::new_with_value(py, stream, c"arrow_array_stream")
    }
}

/// A dataset as the backend opened it.
#[pyclass(name = "OpenedDataset", frozen)]
struct PyOpenedDataset(OpenedDataset);

#[pymethods]
impl PyOpenedDataset {
    #[getter]
    fn input_tables(&self) -> Vec<PyInputTableScan> {
        self.0.input_tables.iter().map(Into::into).collect()
    }

    #[getter]
    fn schema(&self) -> PySchema {
        PySchema(Arc::clone(&self.0.schema))
    }

    /// The stored ordering as `(column, ascending, nulls_first)` triples.
    #[getter]
    fn ordering(&self) -> Vec<(String, bool, bool)> {
        self.0
            .ordering
            .iter()
            .map(|sort| (sort.column.clone(), sort.ascending, sort.nulls_first))
            .collect()
    }
}

/// One input table of an opened dataset, and the schema-only scan a plan reads in its place.
#[pyclass(name = "InputTableScan", frozen)]
struct PyInputTableScan {
    sample_set: Vec<String>,
    scan: Arc<DescribedScan>,
}

impl From<&InputTableScan> for PyInputTableScan {
    fn from(table: &InputTableScan) -> Self {
        Self {
            sample_set: table.sample_set.clone(),
            scan: Arc::clone(&table.scan),
        }
    }
}

#[pymethods]
impl PyInputTableScan {
    #[getter]
    fn name(&self) -> &str {
        &self.scan.description().input_table
    }

    #[getter]
    fn sample_set(&self) -> Vec<String> {
        self.sample_set.clone()
    }

    #[getter]
    fn description(&self) -> PyScanDescription {
        PyScanDescription(self.scan.description().clone())
    }

    /// The scan as an FFI table provider for `session`, encoding with the session's codec.
    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let capsule = session.call_method0("__datafusion_logical_extension_codec__")?;
        let codec = capsule_value::<FFI_LogicalExtensionCodec>(
            &capsule,
            c"datafusion_logical_extension_codec",
        )?;
        let provider: Arc<dyn TableProvider> = self.scan.clone();
        let provider = FFI_TableProvider::new_with_ffi_codec(provider, false, None, codec);
        PyCapsule::new_with_value(py, provider, c"datafusion_table_provider")
    }
}

/// What a frontend plan holds in place of a scan.
#[pyclass(name = "ScanDescription", frozen, eq, hash)]
#[derive(PartialEq, Eq, Hash)]
struct PyScanDescription(ScanDescription);

#[pymethods]
impl PyScanDescription {
    #[getter]
    fn dataset_path(&self) -> &str {
        &self.0.dataset_path
    }

    #[getter]
    const fn input_format(&self) -> &'static str {
        self.0.input_format.name()
    }

    #[getter]
    fn row_ordering(&self) -> String {
        self.0.row_ordering.to_string()
    }

    #[getter]
    fn input_table(&self) -> &str {
        &self.0.input_table
    }

    fn __repr__(&self) -> String {
        format!(
            "ScanDescription(dataset_path={:?}, input_format={:?}, row_ordering={:?}, \
             input_table={:?})",
            self.0.dataset_path,
            self.0.input_format.name(),
            self.0.row_ordering.to_string(),
            self.0.input_table
        )
    }
}

/// An Arrow schema, exported through the Arrow `PyCapsule` interface.
#[pyclass(name = "Schema", frozen)]
struct PySchema(SchemaRef);

#[pymethods]
impl PySchema {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_schema__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        // A consumer may ask for another schema; this one is all there is.
        let _ = requested_schema;
        let schema = FFI_ArrowSchema::try_from(self.0.as_ref())
            .map_err(|e| backend_error(&DataFusionError::ArrowError(Box::new(e), None)))?;
        PyCapsule::new_with_value(py, schema, c"arrow_schema")
    }
}

/// The backend's codec, for a frontend session to install with `with_logical_extension_codec`.
#[pyclass(name = "Codec", frozen)]
struct PyCodec;

#[pymethods]
impl PyCodec {
    #[new]
    const fn new() -> Self {
        Self
    }

    #[classattr]
    const fn __datafusion_codec_id__() -> &'static str {
        CODEC_ID
    }

    /// The codec as an FFI codec for `session`, which provides its task context.
    #[expect(clippy::unused_self, reason = "Python calls it as a method")]
    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let capsule = session.call_method0("__datafusion_task_context_provider__")?;
        let provider = capsule_value::<FFI_TaskContextProvider>(
            &capsule,
            c"datafusion_task_context_provider",
        )?;
        let codec = FFI_LogicalExtensionCodec::new(Arc::new(Codec), None, provider);
        PyCapsule::new_with_value(py, codec, c"datafusion_logical_extension_codec")
    }
}

/// The function of an ordering assertion's marker, for `datafusion`'s `ScalarUDF.from_pycapsule`.
#[pyclass(name = "AssertOrdered", frozen)]
struct PyAssertOrdered;

#[pymethods]
impl PyAssertOrdered {
    #[new]
    const fn new() -> Self {
        Self
    }

    /// The function as an FFI function, which crosses plan bytes by name.
    #[expect(clippy::unused_self, reason = "Python calls it as a method")]
    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        PyCapsule::new_with_value(
            py,
            FFI_ScalarUDF::from(Arc::new(assert_ordered_udf())),
            c"datafusion_scalar_udf",
        )
    }
}

/// A clone of the `T` that `capsule`, named `name`, holds.
fn capsule_value<T: Clone>(capsule: &Bound<'_, PyAny>, name: &'static CStr) -> PyResult<T> {
    let capsule = capsule.cast::<PyCapsule>()?;
    let value = capsule.pointer_checked(Some(name))?.cast::<T>();
    // SAFETY: a capsule named `name` holds a `T` by the `datafusion` FFI protocol, and the capsule,
    // bound for this call, keeps it alive while it is cloned.
    Ok(unsafe { value.as_ref() }.clone())
}

#[pymodule(gil_used = false, name = "_native")]
fn native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("BackendError", m.py().get_type::<BackendError>())?;
    m.add("InvalidAction", m.py().get_type::<InvalidAction>())?;
    m.add(
        "OrderingAssertionFailed",
        m.py().get_type::<OrderingAssertionFailed>(),
    )?;
    m.add("RunCancelled", m.py().get_type::<RunCancelled>())?;
    m.add_class::<PyAction>()?;
    m.add_class::<PyAssertOrdered>()?;
    m.add_class::<PyBackend>()?;
    m.add_class::<PyCodec>()?;
    m.add_class::<PyInputTableScan>()?;
    m.add_class::<PyOpenedDataset>()?;
    m.add_class::<PyRows>()?;
    m.add_class::<PyRun>()?;
    m.add_class::<PyScanDescription>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyWrite>()?;
    Ok(())
}
