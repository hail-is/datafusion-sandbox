use datafusion::prelude::*;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    datasource::{
        file_format::parquet::{ParquetFormat, ParquetFormatFactory},
        listing::ListingOptions,
    },
};
use datafusion_sandbox::pipeline::{self, PipelineOptions};
use datafusion_sandbox::{make_range_table, read, vortex_format, write};

use std::{future::Future, sync::Arc};
use vortex_datafusion::VortexFormatFactory;

const N_ROWS: u32 = 1000;

/// Writes a small vortex file and returns its path, rooted in `dir`.
fn write_vortex_fixture(dir: &tempfile::TempDir) -> String {
    let output = dir.path().join("fixture.vortex");
    let output_path = output.to_str().unwrap().to_string();
    let write_path = output_path.clone();
    pipeline::run(
        move |ctx| async move {
            let df = make_range_table(&ctx, N_ROWS, 128)?;
            write(df, &write_path, Arc::new(VortexFormatFactory::new())).await
        },
        PipelineOptions::default(),
    )
    .unwrap();
    output_path
}

fn write_parquet_fixture(dir: &tempfile::TempDir, filename: &str) -> String {
    let output_path = dir.path().join(filename).to_str().unwrap().to_string();
    let write_path = output_path.clone();
    block_on(async {
        let ctx = SessionContext::new();
        let df = make_range_table(&ctx, N_ROWS, 128).unwrap();
        write(df, &write_path, Arc::new(ParquetFormatFactory::new()))
            .await
            .unwrap();
    });
    output_path
}

#[test]
fn reads_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet_fixture(&dir, "fixture.parquet");

    block_on(async {
        let ctx = SessionContext::new();
        let df = read(&ctx, &path, parquet_listing_options(), None)
            .await
            .unwrap();

        assert!(df.schema().has_column_with_unqualified_name("idx"));
        assert_eq!(df.count().await.unwrap(), N_ROWS as usize);
    });
}

#[test]
fn reads_multiple_paths_as_one_table() {
    let dir = tempfile::tempdir().unwrap();
    let first = write_parquet_fixture(&dir, "first.parquet");
    let second = write_parquet_fixture(&dir, "second.parquet");

    block_on(async {
        let ctx = SessionContext::new();
        let df = read(&ctx, vec![first, second], parquet_listing_options(), None)
            .await
            .unwrap();

        assert_eq!(df.count().await.unwrap(), 2 * N_ROWS as usize);
    });
}

/// The read helper serves both cases: without a schema it infers one from the
/// file, with a schema it uses the one given.
#[test]
fn reads_with_inferred_or_given_schema() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_vortex_fixture(&dir);

    block_on(async {
        let ctx = SessionContext::new();

        let inferred = read(&ctx, &path, vortex_listing_options(), None)
            .await
            .unwrap();
        assert!(inferred.schema().has_column_with_unqualified_name("idx"));
        assert_eq!(inferred.count().await.unwrap(), N_ROWS as usize);

        let schema = Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, true)]));
        let given = read(&ctx, &path, vortex_listing_options(), Some(schema))
            .await
            .unwrap();
        assert_eq!(given.count().await.unwrap(), N_ROWS as usize);
    });
}

#[test]
fn rejects_an_empty_path_list() {
    block_on(async {
        let ctx = SessionContext::new();
        let err = read(&ctx, Vec::<String>::new(), parquet_listing_options(), None)
            .await
            .expect_err("an empty path list has no table to read");

        assert!(
            err.to_string().contains("No table paths"),
            "unexpected error: {err}"
        );
    });
}

fn parquet_listing_options() -> ListingOptions {
    ListingOptions::new(Arc::new(ParquetFormat::default()))
}

fn vortex_listing_options() -> ListingOptions {
    ListingOptions::new(vortex_format())
}

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
