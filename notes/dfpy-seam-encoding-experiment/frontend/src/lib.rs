//! Throwaway frontend cdylib for notes/dfpy-seam-encoding.md.
//!
//! Exports, as PyCapsules the stock `datafusion` wheel accepts:
//! - `SchemaOnlyTable(path)`: a table provider that can be planned against but never scanned;
//! - `AssertOrdered()`: the `assert_ordered` marker UDF;
//! - `SeamCodec()`: a logical codec that writes our own payload for `SchemaOnlyTable` and encodes
//!   every UDF by name.
//!
//! `seen()` returns what the codec was handed, so the driver can check whether
//! it saw our own types or FFI wrappers.

use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, ScalarValue, TableReference, exec_err, not_impl_err};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    ColumnarValue, Expr, Extension, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

pub const SCAN_MAGIC: &[u8] = b"HAILSCAN";

static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn note(event: String) {
    SEEN.lock().unwrap().push(event);
}

#[derive(Debug)]
struct ScanDescription {
    path: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for ScanDescription {
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
        exec_err!("schema-only provider for {} is never scanned", self.path)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct AssertOrderedUdf {
    signature: Signature,
}

impl ScalarUDFImpl for AssertOrderedUdf {
    fn name(&self) -> &str {
        "assert_ordered"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
    }
}

#[derive(Debug)]
struct SeamCodecImpl;

impl LogicalExtensionCodec for SeamCodecImpl {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> Result<Extension> {
        not_impl_err!("no extension nodes")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("no extension nodes")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        not_impl_err!("the frontend never decodes")
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        match node.downcast_ref::<ScanDescription>() {
            Some(desc) => {
                note(format!(
                    "encode_table_provider({table_ref}): own ScanDescription path={}",
                    desc.path
                ));
                buf.extend_from_slice(SCAN_MAGIC);
                buf.extend_from_slice(desc.path.as_bytes());
                Ok(())
            }
            None => {
                note(format!(
                    "encode_table_provider({table_ref}): not ours, declined"
                ));
                exec_err!("not a Hail scan description")
            }
        }
    }

    fn try_encode_udf(&self, node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        let own = node.inner().downcast_ref::<AssertOrderedUdf>().is_some();
        note(format!(
            "encode_udf({}): own AssertOrderedUdf={own}, writing nothing",
            node.name()
        ));
        Ok(())
    }
}

fn session_logical_codec(session: &Bound<'_, PyAny>) -> PyResult<FFI_LogicalExtensionCodec> {
    let capsule = session.call_method0("__datafusion_logical_extension_codec__")?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let codec = capsule
        .pointer_checked(Some(c"datafusion_logical_extension_codec"))?
        .cast::<FFI_LogicalExtensionCodec>();
    Ok(unsafe { codec.as_ref() }.clone())
}

fn session_task_ctx_provider(session: &Bound<'_, PyAny>) -> PyResult<FFI_TaskContextProvider> {
    let capsule = session.call_method0("__datafusion_task_context_provider__")?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let provider = capsule
        .pointer_checked(Some(c"datafusion_task_context_provider"))?
        .cast::<FFI_TaskContextProvider>();
    Ok(unsafe { provider.as_ref() }.clone())
}

#[pyclass(name = "SchemaOnlyTable")]
struct PySchemaOnlyTable {
    path: String,
}

#[pymethods]
impl PySchemaOnlyTable {
    #[new]
    fn new(path: String) -> Self {
        Self { path }
    }

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("contig", DataType::Utf8, false),
            Field::new("position", DataType::Int32, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let provider = Arc::new(ScanDescription {
            path: self.path.clone(),
            schema,
        });
        let codec = session_logical_codec(&session)?;
        let ffi = FFI_TableProvider::new_with_ffi_codec(provider, false, None, codec);
        PyCapsule::new_with_value(py, ffi, c"datafusion_table_provider")
    }
}

#[pyclass(name = "AssertOrdered")]
struct PyAssertOrdered;

#[pymethods]
impl PyAssertOrdered {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(ScalarUDF::new_from_impl(AssertOrderedUdf {
            signature: Signature::variadic_any(Volatility::Immutable),
        }));
        PyCapsule::new_with_value(py, FFI_ScalarUDF::from(udf), c"datafusion_scalar_udf")
    }
}

#[pyclass(name = "SeamCodec")]
struct PySeamCodec;

#[pymethods]
impl PySeamCodec {
    #[new]
    fn new() -> Self {
        Self
    }

    #[classattr]
    fn __datafusion_codec_id__() -> &'static str {
        "hail.seam"
    }

    #[pyo3(signature = (session=None))]
    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
        session: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let Some(session) = session else {
            return Err(pyo3::exceptions::PyValueError::new_err("needs the session"));
        };
        let provider = session_task_ctx_provider(&session)?;
        let codec = FFI_LogicalExtensionCodec::new(Arc::new(SeamCodecImpl), None, provider);
        PyCapsule::new_with_value(py, codec, c"datafusion_logical_extension_codec")
    }
}

#[pyfunction]
fn seen() -> Vec<String> {
    std::mem::take(&mut *SEEN.lock().unwrap())
}

#[pymodule]
fn seam_probe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySchemaOnlyTable>()?;
    m.add_class::<PyAssertOrdered>()?;
    m.add_class::<PySeamCodec>()?;
    m.add_function(wrap_pyfunction!(seen, m)?)?;
    Ok(())
}
