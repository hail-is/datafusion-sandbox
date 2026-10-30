//! The backend's logical extension codec: how scan descriptions cross in plan bytes.
//!
//! A frontend installs it on its `datafusion` session, under the id [`CODEC_ID`], so that encoding
//! a plan writes each described scan's [description](ScanDescription::to_bytes) as its payload.
//! The wheel frames every payload an installed codec writes as `DFPYCHN | version: u8 | id_len:
//! u32 LE | id | payload`; the codec writes its payloads unframed and reads them back only from
//! that frame, at version 1 and under its own id.
//!
//! Frontend plans hold stock logical nodes only, so the codec refuses every extension node. It
//! writes no payload for a function, which crosses by name.

use crate::backend::scan_description::{DescribedScan, ScanDescription};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::TableProvider,
    common::{DataFusionError, TableReference},
    error::Result,
    execution::TaskContext,
    logical_expr::{Extension, LogicalPlan, ScalarUDF},
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use std::sync::Arc;

/// The id the codec is installed under on a frontend session, its `__datafusion_codec_id__`, which
/// names it in every payload frame. Stable: plan bytes name it.
pub const CODEC_ID: &str = "hail_df";

/// The bytes every payload frame the wheel writes starts with.
const FRAME_MAGIC: &[u8] = b"DFPYCHN";

/// The payload frame version the codec reads.
const FRAME_VERSION: u8 = 1;

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
}

/// The payload inside the wheel's frame around `buf`.
fn unframe(buf: &[u8]) -> Result<&[u8]> {
    let Some(rest) = buf.strip_prefix(FRAME_MAGIC) else {
        return Err(DataFusionError::Plan(
            "a table payload has no DFPYCHN frame naming the codec that wrote it".to_string(),
        ));
    };
    let Some((&version, rest)) = rest.split_first() else {
        return Err(truncated_frame());
    };
    if version != FRAME_VERSION {
        return Err(DataFusionError::Plan(format!(
            "a table payload is framed at DFPYCHN version {version}, and this backend reads \
             version {FRAME_VERSION}: align the datafusion wheel with the backend"
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
        return Err(DataFusionError::Plan(format!(
            "a table payload was written by codec '{}', not by '{CODEC_ID}'",
            String::from_utf8_lossy(id)
        )));
    }
    Ok(payload)
}

fn truncated_frame() -> DataFusionError {
    DataFusionError::Plan("a table payload's DFPYCHN frame is cut off".to_string())
}

fn no_extension_nodes() -> DataFusionError {
    DataFusionError::Plan(
        "frontend plans hold stock logical nodes only, and this one holds an extension node"
            .to_string(),
    )
}
