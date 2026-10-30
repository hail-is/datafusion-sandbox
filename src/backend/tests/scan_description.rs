use crate::{
    backend::scan_description::{DescribedScan, ScanDescription},
    format::InputFormat,
    row_ordering::RowOrdering,
    testing::fixture::block_on,
};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    catalog::TableProvider,
    datasource::TableType,
    prelude::SessionContext,
};
use std::sync::Arc;

#[test]
fn a_scan_description_reads_back_from_its_bytes() {
    for (input_format, row_ordering) in [
        (InputFormat::PARQUET, RowOrdering::locus()),
        (InputFormat::VORTEX, RowOrdering::locus_then_alleles()),
        (InputFormat::PARQUET, RowOrdering::locus_then_sample()),
    ] {
        let description = ScanDescription {
            dataset_path: "gs://bucket/data set/samples".to_string(),
            input_format,
            row_ordering,
            input_table: "s=HG00308".to_string(),
        };

        assert_eq!(
            ScanDescription::from_bytes(&description.to_bytes()).unwrap(),
            description
        );
    }
}

/// A path or name holding the separator characters a text encoding might use still reads back
/// whole.
#[test]
fn a_scan_description_reads_back_paths_and_names_holding_any_characters() {
    let description = ScanDescription {
        dataset_path: "/data/a\nb,c\0d".to_string(),
        input_format: InputFormat::VORTEX,
        row_ordering: RowOrdering::locus(),
        input_table: "g0\n".to_string(),
    };

    assert_eq!(
        ScanDescription::from_bytes(&description.to_bytes()).unwrap(),
        description
    );
}

#[test]
fn bytes_that_are_no_scan_description_fail_to_read() {
    let bytes = description().to_bytes();
    let truncated = &bytes[..bytes.len() - 1];
    let mut trailing = bytes.clone();
    trailing.push(0);
    let mut other_version = bytes.clone();
    other_version[8] = 99;

    for (case, bytes) in [
        ("empty", &[][..]),
        ("another payload", b"DFPYUDF1 not ours".as_slice()),
        ("truncated", truncated),
        ("trailing bytes", &trailing),
        ("another version", &other_version),
    ] {
        let message = ScanDescription::from_bytes(bytes).map_or_else(
            |error| error.to_string(),
            |description| format!("read {description:?}"),
        );

        assert!(message.contains("scan description"), "{case}: {message}");
    }
}

#[test]
fn a_scan_description_naming_an_unknown_format_or_ordering_fails_to_read() {
    for (format, ordering, unknown) in [
        ("csv", "locus", "'csv'"),
        ("parquet", "alleles,locus", "'alleles,locus'"),
    ] {
        let bytes = replace_field(&description().to_bytes(), 1, format.as_bytes());
        let bytes = replace_field(&bytes, 2, ordering.as_bytes());

        let message = ScanDescription::from_bytes(&bytes).unwrap_err().to_string();

        assert!(message.contains(unknown), "{message}");
    }
}

#[test]
fn a_described_scan_plans_with_its_schema_as_a_base_table() {
    let scan = DescribedScan::new(description(), schema());

    assert_eq!(scan.description(), &description());
    assert_eq!(scan.schema(), schema());
    assert_eq!(scan.table_type(), TableType::Base);

    let ctx = SessionContext::new();
    let frame = ctx.read_table(Arc::new(scan)).unwrap();
    assert_eq!(frame.schema().inner(), &schema());
}

#[test]
fn a_described_scan_refuses_to_scan_naming_its_input_table() {
    let ctx = SessionContext::new();
    let frame = ctx
        .read_table(Arc::new(DescribedScan::new(description(), schema())))
        .unwrap();

    let message = block_on(frame.create_physical_plan())
        .unwrap_err()
        .to_string();

    assert!(message.contains("'s=HG00308'"), "{message}");
    assert!(message.contains("scan description"), "{message}");
}

fn description() -> ScanDescription {
    ScanDescription {
        dataset_path: "memory://fixture/samples".to_string(),
        input_format: InputFormat::PARQUET,
        row_ordering: RowOrdering::locus_then_alleles(),
        input_table: "s=HG00308".to_string(),
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("contig", DataType::Utf8View, false),
        Field::new("position", DataType::Int32, false),
    ]))
}

/// `bytes`, a scan description's, with its field `index` replaced by `value`, in the layout
/// `ScanDescription::to_bytes` documents.
fn replace_field(bytes: &[u8], index: usize, value: &[u8]) -> Vec<u8> {
    let (header, mut rest) = bytes.split_at(9);
    let mut out = header.to_vec();
    for field in 0..4 {
        let (length, tail) = rest.split_at(4);
        let length = usize::try_from(u32::from_le_bytes(length.try_into().unwrap())).unwrap();
        let (content, tail) = tail.split_at(length);
        let content = if field == index { value } else { content };
        out.extend_from_slice(&u32::try_from(content.len()).unwrap().to_le_bytes());
        out.extend_from_slice(content);
        rest = tail;
    }
    out
}
