use crate::{
    backend::{
        codec::{CODEC_ID, Codec},
        scan_description::{DescribedScan, ScanDescription},
    },
    format::InputFormat,
    row_ordering::RowOrdering,
    testing::wheel_codec::WheelCodec,
};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    catalog::TableProvider,
    common::TableReference,
    datasource::empty::EmptyTable,
    functions::math::abs,
    prelude::SessionContext,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use std::sync::Arc;

#[test]
fn writes_a_described_scan_s_description_as_its_payload() {
    let mut payload = Vec::new();

    Codec
        .try_encode_table_provider(&table(), described_scan(), &mut payload)
        .unwrap();

    assert_eq!(payload, description().to_bytes());
}

#[test]
fn refuses_to_write_a_provider_it_does_not_own() {
    let mut payload = Vec::new();
    let provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(schema()));

    let error = Codec
        .try_encode_table_provider(&table(), provider, &mut payload)
        .unwrap_err();

    assert!(error.to_string().contains("scan description"), "{error}");
    assert_eq!(payload, b"");
}

/// A function crosses by name: the codec writes nothing for it, so the backend resolves it from
/// its own registry.
#[test]
fn writes_no_payload_for_a_function() {
    let mut payload = Vec::new();

    Codec.try_encode_udf(&abs(), &mut payload).unwrap();

    assert_eq!(payload, b"");
}

#[test]
fn reads_a_payload_the_wheel_framed_back_into_a_described_scan_with_the_scan_s_schema() {
    let payload = wheel_payload(CODEC_ID);
    let ctx = SessionContext::new();

    let provider = Codec
        .try_decode_table_provider(&payload, &table(), schema(), &ctx.task_ctx())
        .unwrap();

    let scan = provider.downcast_ref::<DescribedScan>().unwrap();
    assert_eq!(scan.description(), &description());
    assert_eq!(provider.schema(), schema());
}

#[test]
fn refuses_a_payload_another_codec_wrote() {
    let payload = wheel_payload("another_library");
    let ctx = SessionContext::new();

    let error = Codec
        .try_decode_table_provider(&payload, &table(), schema(), &ctx.task_ctx())
        .unwrap_err();

    assert!(error.to_string().contains("'another_library'"), "{error}");
}

#[test]
fn refuses_a_payload_the_wheel_did_not_frame() {
    let ctx = SessionContext::new();

    let error = Codec
        .try_decode_table_provider(
            &description().to_bytes(),
            &table(),
            schema(),
            &ctx.task_ctx(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("DFPYCHN"), "{error}");
}

/// The wheel's framing has a version of its own, the byte after `DFPYCHN`, which the codec reads
/// only at the version it knows.
#[test]
fn refuses_a_payload_framed_at_another_wire_version() {
    let mut payload = wheel_payload(CODEC_ID);
    payload[7] = 2;
    let ctx = SessionContext::new();

    let error = Codec
        .try_decode_table_provider(&payload, &table(), schema(), &ctx.task_ctx())
        .unwrap_err();

    assert!(error.to_string().contains("version 2"), "{error}");
}

/// What the wheel writes for a described scan when the codec is installed on its session under
/// `codec_id`.
fn wheel_payload(codec_id: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    WheelCodec::new(Arc::new(Codec), codec_id)
        .try_encode_table_provider(&table(), described_scan(), &mut payload)
        .unwrap();
    payload
}

fn described_scan() -> Arc<dyn TableProvider> {
    Arc::new(DescribedScan::new(description(), schema()))
}

fn description() -> ScanDescription {
    ScanDescription {
        dataset_path: "memory://fixture/samples".to_string(),
        input_format: InputFormat::VORTEX,
        row_ordering: RowOrdering::locus(),
        input_table: "g0".to_string(),
    }
}

fn table() -> TableReference {
    TableReference::bare("g0")
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "locus",
        DataType::Int64,
        false,
    )]))
}
