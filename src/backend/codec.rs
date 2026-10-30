//! The backend's logical extension codec: how scan descriptions cross in plan bytes.
//!
//! A frontend installs it on its `datafusion` session, under the id [`CODEC_ID`], so that encoding
//! a plan writes each described scan's [description](ScanDescription::to_bytes) as its payload.
//! The wheel frames every payload an installed codec writes as `DFPYCHN | version: u8 | id_len:
//! u32 LE | id | payload`; the codec writes its payloads unframed and reads them back only from
//! that frame, at version 1 and under its own id.
//!
//! Frontend plans hold stock logical nodes only, so the codec refuses every extension node. It
//! writes no payload for a function, which crosses by name and resolves from the decoding
//! session's registry. It refuses a function the wheel inlined as a Python payload, `DFPYUDF`,
//! `DFPYUDA` or `DFPYUDW`, as Python functions are not part of the MVP (ADR 0022).
//!
//! A payload outside the frame, framed at another version or by another codec, or carrying an
//! inlined Python function fails decoding as [`ErrorCode::InvalidAction`](super::ErrorCode).

use crate::backend::{
    invalid_action,
    scan_description::{DescribedScan, ScanDescription},
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::TableProvider,
    common::{DataFusionError, TableReference},
    error::Result,
    execution::TaskContext,
    logical_expr::{AggregateUDF, Extension, LogicalPlan, ScalarUDF, WindowUDF},
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use std::sync::Arc;

/// The id the codec is installed under on a frontend session, its `__datafusion_codec_id__`, which
/// names it in every payload frame. Stable: plan bytes name it.
pub const CODEC_ID: &str = "hail_df";

/// The bytes every payload frame the wheel writes starts with.
const FRAME_MAGIC: &[u8] = b"DFPYCHN";

/// The payload frame version the codec reads: the `DFPYCHN` wire version a frontend's wheel must
/// write.
pub const WIRE_VERSION: u8 = 1;

/// The bytes every payload the wheel writes for an inlined Python function starts with, followed
/// by `F`, `A` or `W` for a scalar, aggregate or window function.
const INLINED_PYTHON_FUNCTION: &[u8] = b"DFPYUD";

/// The codec. See the module docs.
#[derive(Clone, Copy, Debug, Default)]
pub struct Codec;

impl LogicalExtensionCodec for Codec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> Result<Extension> {
        Err(no_extension_nodes())
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        Err(no_extension_nodes())
    }

    /// Reads a framed scan description back into a described scan with `schema`.
    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let description = ScanDescription::from_bytes(unframe(buf)?)?;
        Ok(Arc::new(DescribedScan::new(description, schema)))
    }

    /// Writes a described scan's description; fails on any other provider.
    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let Some(scan) = node.downcast_ref::<DescribedScan>() else {
            return Err(DataFusionError::Plan(format!(
                "table '{table_ref}' is not a scan description, the only table a frontend plan \
                 may read"
            )));
        };
        buf.extend_from_slice(&scan.description().to_bytes());
        Ok(())
    }

    /// Writes nothing, so the function crosses by name.
    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    /// Fails: a function the backend's registry lacks, or one with a payload, has no definition
    /// here.
    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        Err(no_function(name, buf))
    }

    /// Fails as [`Codec::try_decode_udf`] does.
    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        Err(no_function(name, buf))
    }

    /// Fails as [`Codec::try_decode_udf`] does.
    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        Err(no_function(name, buf))
    }
}

/// Why function `name`, with payload `buf`, can't be decoded: it was inlined as a Python
/// function, it carries a payload no codec of the backend's wrote, or the backend's registry lacks
/// it.
fn no_function(name: &str, buf: &[u8]) -> DataFusionError {
    if buf.starts_with(INLINED_PYTHON_FUNCTION) {
        invalid_action(format!(
            "function '{name}' is a Python function inlined into the plan, and the backend runs \
             no Python functions: register it as a Rust function on both sides, or leave it out"
        ))
    } else if buf.is_empty() {
        DataFusionError::Plan(format!(
            "function '{name}' is not registered on the backend"
        ))
    } else {
        invalid_action(format!(
            "function '{name}' carries a payload, and functions cross plan bytes by name only"
        ))
    }
}

/// The payload inside the wheel's frame around `buf`.
fn unframe(buf: &[u8]) -> Result<&[u8]> {
    let Some(rest) = buf.strip_prefix(FRAME_MAGIC) else {
        return Err(invalid_action(
            "a table payload has no DFPYCHN frame naming the codec that wrote it".to_string(),
        ));
    };
    let Some((&version, rest)) = rest.split_first() else {
        return Err(truncated_frame());
    };
    if version != WIRE_VERSION {
        return Err(invalid_action(format!(
            "a table payload is framed at DFPYCHN version {version}, and this backend reads \
             version {WIRE_VERSION}: align the datafusion wheel with the backend"
        )));
    }
    let Some((id_length, rest)) = rest.split_first_chunk::<4>() else {
        return Err(truncated_frame());
    };
    let Some((id, payload)) = usize::try_from(u32::from_le_bytes(*id_length))
        .ok()
        .and_then(|id_length| rest.split_at_checked(id_length))
    else {
        return Err(truncated_frame());
    };
    if id != CODEC_ID.as_bytes() {
        return Err(invalid_action(format!(
            "a table payload was written by codec '{}', not by '{CODEC_ID}'",
            String::from_utf8_lossy(id)
        )));
    }
    Ok(payload)
}

fn truncated_frame() -> DataFusionError {
    invalid_action("a table payload's DFPYCHN frame is cut off".to_string())
}

fn no_extension_nodes() -> DataFusionError {
    DataFusionError::Plan(
        "frontend plans hold stock logical nodes only, and this one holds an extension node"
            .to_string(),
    )
}
