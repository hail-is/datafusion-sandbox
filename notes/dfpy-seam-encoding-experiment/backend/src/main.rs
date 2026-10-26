//! Throwaway backend for notes/dfpy-seam-encoding.md.
//!
//! Decodes plan bytes written by `LogicalPlan.to_bytes(ctx)` in the stock
//! `datafusion` wheel, using this DataFusion copy (55.1) and a codec that
//! knows nothing of datafusion-python except its chained-payload envelope.
//! Registers its own `assert_ordered` so the name resolves here.
//!
//! Usage: seam-backend <plan.bin>...

use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
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
use datafusion::prelude::SessionContext;
use datafusion_proto::bytes::logical_plan_from_bytes_with_extension_codec;
use datafusion_proto::logical_plan::LogicalExtensionCodec;

const CODEC_ID: &str = "hail.seam";
const SCAN_MAGIC: &[u8] = b"HAILSCAN";

/// datafusion-python 55.0.0 wraps every payload an installed codec writes as
/// `DFPYCHN | version: u8 | id_len: u32 LE | id | blob` (crates/core/src/codec.rs).
fn strip_envelope(buf: &[u8]) -> Result<&[u8]> {
    let Some(rest) = buf.strip_prefix(b"DFPYCHN".as_slice()) else {
        return exec_err!("payload has no DFPYCHN envelope");
    };
    let [version, a, b, c, d, rest @ ..] = rest else {
        return exec_err!("truncated envelope");
    };
    if *version != 1 {
        return exec_err!("envelope version {version}");
    }
    let len = u32::from_le_bytes([*a, *b, *c, *d]) as usize;
    let (id, blob) = rest.split_at(len);
    let id = std::str::from_utf8(id).unwrap_or("<non-utf8>");
    if id != CODEC_ID {
        return exec_err!("payload written by codec '{id}', not '{CODEC_ID}'");
    }
    Ok(blob)
}

#[derive(Debug)]
struct ReopenedScan {
    path: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for ReopenedScan {
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
        exec_err!("experiment backend does not scan {}", self.path)
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
struct BackendCodec;

impl LogicalExtensionCodec for BackendCodec {
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
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let blob = strip_envelope(buf)?;
        let Some(path) = blob.strip_prefix(SCAN_MAGIC) else {
            return exec_err!("not a Hail scan description");
        };
        let path = String::from_utf8_lossy(path).into_owned();
        println!("  decode_table_provider({table_ref}): path={path} schema={schema:?}");
        Ok(Arc::new(ReopenedScan { path, schema }))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        not_impl_err!("the backend never encodes")
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        println!(
            "  decode_udf({name}): {} payload bytes, prefix {:?}",
            buf.len(),
            { String::from_utf8_lossy(&buf[..buf.len().min(10)]) }
        );
        if let Some(header) = buf.strip_prefix(b"DFPYUDF".as_slice()) {
            return exec_err!(
                "'{name}' is an inline Python UDF (wire v{}, Python {}.{}); this backend has no Python",
                header[0],
                header[1],
                header[2]
            );
        }
        not_impl_err!("no codec-encoded UDFs here")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::new_from_impl(AssertOrderedUdf {
        signature: Signature::variadic_any(Volatility::Immutable),
    }));
    let task_ctx = ctx.task_ctx();
    for path in std::env::args().skip(1) {
        println!("== {path}");
        let bytes = std::fs::read(&path).expect("read plan bytes");
        match logical_plan_from_bytes_with_extension_codec(&bytes, &task_ctx, &BackendCodec) {
            Ok(plan) => {
                println!("decoded:\n{}", plan.display_indent());
                match ctx.state().optimize(&plan) {
                    Ok(optimized) => println!("backend-optimized:\n{}", optimized.display_indent()),
                    Err(e) => println!("backend optimize failed: {e}"),
                }
            }
            Err(e) => println!("decode failed: {e}"),
        }
    }
    Ok(())
}
