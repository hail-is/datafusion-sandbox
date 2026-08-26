use datafusion::prelude::SessionContext;
use datafusion_sandbox::{format::OutputFormat, make_range_table};

use std::future::Future;

#[test]
fn parquet_writes_rows_and_returns_their_count() {
    assert_writes_rows(OutputFormat::PARQUET, "parquet");
}

#[test]
fn vortex_writes_rows_and_returns_their_count() {
    assert_writes_rows(OutputFormat::VORTEX, "vortex");
}

fn assert_writes_rows(format: OutputFormat, extension: &str) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("rows.{extension}"));
    let path = path.to_str().unwrap();

    let rows_written = block_on(async {
        let ctx = SessionContext::new();
        let df = make_range_table(&ctx, 1000, 128).unwrap();
        format.write(df, path).await.unwrap()
    });

    assert_eq!(rows_written, 1000);
    assert!(std::path::Path::new(path).exists());
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

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn assert_rejects_compression(format: OutputFormat, compression: &str, format_name: &str) {
    let error = format.with_compression(compression).unwrap_err();
    let message = error.to_string();

    assert!(message.contains(compression), "got: {message}");
    assert!(message.contains(format_name), "got: {message}");
}
