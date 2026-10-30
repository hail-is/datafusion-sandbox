use crate::testing::wheel_codec::WheelCodec;

use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    catalog::TableProvider,
    common::{TableReference, not_impl_err},
    datasource::empty::EmptyTable,
    error::Result,
    execution::TaskContext,
    functions::math::abs,
    logical_expr::{Extension, LogicalPlan, ScalarUDF},
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use std::sync::Arc;

#[test]
fn frames_a_payload_under_the_codec_s_id_as_the_wheel_does() {
    let mut buf = Vec::new();

    WheelCodec::new(Arc::new(Fixed(b"blob")), "lib")
        .try_encode_table_provider(&TableReference::bare("t"), empty_table(), &mut buf)
        .unwrap();

    assert_eq!(buf, b"DFPYCHN\x01\x03\x00\x00\x00libblob");
}

#[test]
fn leaves_an_empty_payload_unframed() {
    let mut buf = Vec::new();

    WheelCodec::new(Arc::new(Fixed(b"")), "lib")
        .try_encode_udf(&abs(), &mut buf)
        .unwrap();

    assert_eq!(buf, b"");
}

/// A codec that writes the same payload for every table provider and function.
#[derive(Debug)]
struct Fixed(&'static [u8]);

impl LogicalExtensionCodec for Fixed {
    fn try_decode(&self, _: &[u8], _: &[LogicalPlan], _: &TaskContext) -> Result<Extension> {
        not_impl_err!("no extension nodes")
    }

    fn try_encode(&self, _: &Extension, _: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("no extension nodes")
    }

    fn try_decode_table_provider(
        &self,
        _: &[u8],
        _: &TableReference,
        _: SchemaRef,
        _: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        not_impl_err!("encodes only")
    }

    fn try_encode_table_provider(
        &self,
        _: &TableReference,
        _: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        buf.extend_from_slice(self.0);
        Ok(())
    }

    fn try_encode_udf(&self, _: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(self.0);
        Ok(())
    }
}

fn empty_table() -> Arc<dyn TableProvider> {
    Arc::new(EmptyTable::new(Arc::new(Schema::empty())))
}
