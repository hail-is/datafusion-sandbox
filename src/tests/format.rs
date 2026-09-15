#![expect(
    clippy::as_conversions,
    reason = "the in-memory object's test-controlled length fits in u64"
)]

use crate::{
    format::OutputFormat,
    generated::make_range_table,
    pipeline::{self, PipelineOptions},
};
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

use std::sync::Arc;

#[test]
fn parquet_writes_rows_and_returns_their_count() {
    assert_writes_rows(&OutputFormat::PARQUET, "parquet");
}

#[test]
fn vortex_writes_rows_and_returns_their_count() {
    assert_writes_rows(&OutputFormat::VORTEX, "vortex");
}

fn assert_writes_rows(format: &'static OutputFormat, extension: &'static str) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let object_path = Path::from(format!("rows.{extension}"));

    let (rows_written, metadata, bytes) = pipeline::run(
        move |ctx: SessionContext| async move {
            let store_url = ObjectStoreUrl::parse("memory://out")?;
            ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));
            let df = make_range_table(&ctx, 1000, 128)?;
            let rows_written = format
                .write(df, &format!("memory://out/rows.{extension}"), None)
                .await?;
            let metadata = store.head(&object_path).await?;
            let bytes = store.get(&object_path).await?.bytes().await?;
            Ok((rows_written, metadata, bytes))
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(rows_written, 1000);
    assert!(metadata.size > 0);
    assert_eq!(bytes.len() as u64, metadata.size);
}

#[test]
fn parquet_accepts_its_compression_modes() {
    for compression in [
        "uncompressed",
        "snappy",
        "gzip(6)",
        "brotli(5)",
        "lz4",
        "zstd(7)",
        "lz4_raw",
    ] {
        OutputFormat::PARQUET
            .with_compression(compression)
            .unwrap_or_else(|error| panic!("parquet rejected {compression}: {error}"));
    }
}

#[test]
fn vortex_accepts_its_compression_modes() {
    for compression in ["standard", "compact"] {
        OutputFormat::VORTEX
            .with_compression(compression)
            .unwrap_or_else(|error| panic!("vortex rejected {compression}: {error}"));
    }
}

#[test]
fn parquet_rejects_an_incomplete_compression_mode() {
    assert_rejects_compression(OutputFormat::PARQUET, "brotli", "parquet");
}

#[test]
fn parquet_rejects_a_malformed_compression_mode_without_panicking() {
    assert_rejects_compression(OutputFormat::PARQUET, "gzip(", "parquet");
}

#[test]
fn vortex_rejects_a_parquet_compression_mode() {
    assert_rejects_compression(OutputFormat::VORTEX, "zstd(7)", "vortex");
}

fn assert_rejects_compression(format: OutputFormat, compression: &str, format_name: &str) {
    let error = format.with_compression(compression).unwrap_err();
    let message = error.to_string();

    assert!(message.contains(compression), "got: {message}");
    assert!(message.contains(format_name), "got: {message}");
}
