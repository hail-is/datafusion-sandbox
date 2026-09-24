use crate::{
    fixture::{self, FixtureFormat, MemoryStore},
    format::OutputFormat,
    formulation::Formulation,
    generated::make_range_table,
    locus::LocusRepresentation,
    pipeline::{self, PipelineOptions},
    stored::dataset::Dataset,
    write::WriteTarget,
};

use object_store::{ObjectStoreExt, path::Path};
use std::sync::Arc;

#[test]
fn uncompressed_parquet_is_larger_than_zstd_parquet() {
    let uncompressed = written_size(
        OutputFormat::PARQUET
            .with_compression("uncompressed")
            .unwrap(),
        "parquet-uncompressed",
    );
    let compressed = written_size(
        OutputFormat::PARQUET.with_compression("zstd(7)").unwrap(),
        "parquet-zstd",
    );

    assert!(uncompressed > compressed, "{uncompressed} <= {compressed}");
}

#[test]
fn compact_and_standard_vortex_have_different_file_sizes() {
    let standard = combined_refs_size(
        OutputFormat::VORTEX.with_compression("standard").unwrap(),
        "vortex-standard",
    );
    let compact = combined_refs_size(
        OutputFormat::VORTEX.with_compression("compact").unwrap(),
        "vortex-compact",
    );

    assert_ne!(standard, compact, "standard and compact file sizes match");
}

fn combined_refs_size(output_format: OutputFormat, store_name: &str) -> u64 {
    let input = Arc::clone(fixture::dataset_fixture(
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    ));
    let output = MemoryStore::new(store_name);
    let target = WriteTarget {
        output_path: format!("{}combined.vortex", output.url().as_str()),
        output_format,
    };

    pipeline::run(
        move |ctx| async move {
            input.register(&ctx);
            output.register(&ctx);
            let formulation = Formulation::CombineRefsUnion;
            let dataset = Dataset::discover(
                &ctx,
                input.table_path().clone(),
                input.input_format(),
                formulation.required_ordering(),
                None,
            )
            .await?;
            let executed = target
                .write(formulation.plan(&ctx, &dataset).await?)
                .await?;
            assert_eq!(executed.rows_written, 32);
            Ok(output
                .store()
                .head(&Path::from("combined.vortex"))
                .await?
                .size)
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

fn written_size(output_format: OutputFormat, store_name: &str) -> u64 {
    let store = MemoryStore::new(store_name);
    let extension = output_format.extension();
    let object_path = Path::from(format!("rows.{extension}"));
    let target = WriteTarget {
        output_path: format!("{}rows.{extension}", store.url().as_str()),
        output_format,
    };

    pipeline::run(
        move |ctx| async move {
            store.register(&ctx);
            let executed = target
                .write_unordered(make_range_table(&ctx, 10_000, 128)?)
                .await?;
            assert_eq!(executed.rows_written, 10_000);
            Ok(store.store().head(&object_path).await?.size)
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
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
