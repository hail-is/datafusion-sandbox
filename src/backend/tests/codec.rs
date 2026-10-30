use crate::{
    backend::{
        ErrorCode,
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
    assert_eq!(ErrorCode::of(&error), ErrorCode::InvalidAction);
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
    assert_eq!(ErrorCode::of(&error), ErrorCode::InvalidAction);
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
    assert_eq!(ErrorCode::of(&error), ErrorCode::InvalidAction);
}

/// The wheel inlines a Python function as a payload of its own, `DFPYUDF`, `DFPYUDA` or
/// `DFPYUDW` and then a pickled callable, which the backend refuses rather than runs.
#[test]
fn refuses_a_python_function_the_wheel_inlined_in_every_family() {
    let inlined = |family: &[u8]| [b"DFPYUD".as_slice(), family, &[1, 3, 13], b"pickled"].concat();

    let errors = [
        Codec.try_decode_udf("double", &inlined(b"F")).unwrap_err(),
        Codec.try_decode_udaf("double", &inlined(b"A")).unwrap_err(),
        Codec.try_decode_udwf("double", &inlined(b"W")).unwrap_err(),
    ];

    for error in errors {
        assert_eq!(ErrorCode::of(&error), ErrorCode::InvalidAction, "{error}");
        assert!(error.to_string().contains("'double'"), "{error}");
        assert!(error.to_string().contains("Python function"), "{error}");
    }
}

/// Functions cross by name only, so any other payload is refused too.
#[test]
fn refuses_a_function_with_any_other_payload() {
    let error = Codec
        .try_decode_udf("double", b"DFPYCHN\x01 framed")
        .unwrap_err();

    assert_eq!(ErrorCode::of(&error), ErrorCode::InvalidAction, "{error}");
    assert!(error.to_string().contains("by name"), "{error}");
}

/// The decoder asks the codec for a function the registry lacks, with no payload; the codec has
/// none to give.
#[test]
fn has_no_function_the_backend_s_registry_lacks() {
    let error = Codec.try_decode_udf("double", b"").unwrap_err();

    assert_eq!(ErrorCode::of(&error), ErrorCode::Backend, "{error}");
    assert!(
        error.to_string().contains("'double' is not registered"),
        "{error}"
    );
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
