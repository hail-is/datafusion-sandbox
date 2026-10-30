//! `hail_df._native`: the Python bindings over the backend in `datafusion_sandbox::backend`.
//!
//! The bindings only marshal: values between Python and the backend, the FFI capsules the
//! `datafusion` wheel loads (the codec and each input table's schema-only scan), and the backend's
//! errors as `BackendError`. The Python package around them is the interface; see
//! `docs/hail_df.md`.

use datafusion::{
    arrow::{datatypes::SchemaRef, ffi::FFI_ArrowSchema},
    catalog::TableProvider,
    error::DataFusionError,
};
use datafusion_ffi::{
    execution::FFI_TaskContextProvider, proto::logical_extension_codec::FFI_LogicalExtensionCodec,
    table_provider::FFI_TableProvider,
};
use datafusion_sandbox::backend::{
    self, InputTableScan, OpenedDataset, ResolveFromUrls,
    codec::{CODEC_ID, Codec},
    scan_description::{DescribedScan, ScanDescription},
};
use pyo3::{
    create_exception,
    exceptions::PyException,
    prelude::*,
    types::{PyBytes, PyCapsule},
};
use std::{ffi::CStr, sync::Arc};

create_exception!(
    hail_df,
    BackendError,
    PyException,
    "A failure the backend reports."
);

fn backend_error(error: &DataFusionError) -> PyErr {
    BackendError::new_err(error.to_string())
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

    /// Opens the dataset at `path`, stored in `input_format` and declared in `row_ordering`.
    fn open_dataset(
        &self,
        py: Python<'_>,
        path: &str,
        input_format: &str,
        row_ordering: &str,
    ) -> PyResult<PyOpenedDataset> {
        let input_format = input_format.parse().map_err(|e| backend_error(&e))?;
        let row_ordering = row_ordering.parse().map_err(|e| backend_error(&e))?;
        py.detach(|| self.0.open_dataset(path, input_format, row_ordering))
            .map(PyOpenedDataset)
            .map_err(|e| backend_error(&e))
    }

    /// The scan descriptions `plan_bytes` read, each once, in the order the plan first reads them.
    fn scan_descriptions(
        &self,
        py: Python<'_>,
        plan_bytes: &Bound<'_, PyBytes>,
    ) -> PyResult<Vec<PyScanDescription>> {
        let plan_bytes = plan_bytes.as_bytes();
        py.detach(|| self.0.scan_descriptions(plan_bytes))
            .map(|descriptions| descriptions.into_iter().map(PyScanDescription).collect())
            .map_err(|e| backend_error(&e))
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
    m.add_class::<PyBackend>()?;
    m.add_class::<PyCodec>()?;
    m.add_class::<PyInputTableScan>()?;
    m.add_class::<PyOpenedDataset>()?;
    m.add_class::<PyScanDescription>()?;
    m.add_class::<PySchema>()?;
    Ok(())
}
