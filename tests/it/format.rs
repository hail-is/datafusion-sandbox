use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};
use datafusion_sandbox::{format::OutputFormat, synthetic::make_range_table};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

use std::{future::Future, sync::Arc};

#[test]
fn parquet_writes_rows_and_returns_their_count() {
    assert_writes_rows(&OutputFormat::PARQUET, "parquet");
}

#[test]
fn vortex_writes_rows_and_returns_their_count() {
    assert_writes_rows(&OutputFormat::VORTEX, "vortex");
}

fn assert_writes_rows(format: &OutputFormat, extension: &str) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let object_path = Path::from(format!("rows.{extension}"));

    let (rows_written, metadata, bytes) = block_on(async {
        let ctx = SessionContext::new();
        let store_url = ObjectStoreUrl::parse("memory://out").unwrap();
        ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));
        let df = make_range_table(&ctx, 1000, 128).unwrap();
        let rows_written = format
            .write(df, &format!("memory://out/rows.{extension}"))
            .await
            .unwrap();
        let metadata = store.head(&object_path).await.unwrap();
        let bytes = store
            .get(&object_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        (rows_written, metadata, bytes)
    });

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
