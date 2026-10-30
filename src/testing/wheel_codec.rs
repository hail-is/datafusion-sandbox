//! A logical codec adapter that frames another codec's payloads as the `datafusion` Python wheel
//! does, so a suite can build in Rust the plan bytes the wheel writes.
//!
//! The wheel installs a session's extension codecs in a chain and wraps each payload one of them
//! writes as `DFPYCHN | version: u8 | id_len: u32 LE | id | payload`, version 1, naming the codec
//! by its id. An empty payload, which a codec writes for a function that crosses by name, it leaves
//! empty. This adapter does the same for the one codec it wraps.

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::TableProvider,
    common::TableReference,
    error::Result,
    execution::TaskContext,
    logical_expr::{Extension, LogicalPlan, ScalarUDF},
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use std::sync::Arc;

/// `codec`, installed on a wheel's session under `id`.
#[derive(Debug)]
pub(crate) struct WheelCodec {
    codec: Arc<dyn LogicalExtensionCodec>,
    id: String,
}

impl WheelCodec {
    pub(crate) fn new(codec: Arc<dyn LogicalExtensionCodec>, id: &str) -> Self {
        Self {
            codec,
            id: id.to_string(),
        }
    }

    /// Runs `encode` into a fresh payload and appends it to `buf`, framed under this codec's id
    /// unless it is empty.
    fn encode_framed(
        &self,
        buf: &mut Vec<u8>,
        encode: impl FnOnce(&mut Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let mut payload = Vec::new();
        encode(&mut payload)?;
        if payload.is_empty() {
            return Ok(());
        }
        buf.extend_from_slice(b"DFPYCHN");
        buf.push(1);
        let id_length = u32::try_from(self.id.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&id_length.to_le_bytes());
        buf.extend_from_slice(self.id.as_bytes());
        buf.extend_from_slice(&payload);
        Ok(())
    }
}

/// Encodes through the wrapped codec and frames what it writes; decodes through it unchanged, as
/// the backend's own codec decodes what the wheel wrote.
impl LogicalExtensionCodec for WheelCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.codec.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_framed(buf, |payload| self.codec.try_encode(node, payload))
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        self.codec
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.encode_framed(buf, |payload| {
            self.codec
                .try_encode_table_provider(table_ref, node, payload)
        })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.codec.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_framed(buf, |payload| self.codec.try_encode_udf(node, payload))
    }
}
