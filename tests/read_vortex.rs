use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_sandbox::pipeline::{self, PipelineOptions};
use datafusion_sandbox::{VortexReadOptions, make_range_table, read_vortex};

use std::sync::Arc;

const N_ROWS: u32 = 1000;

/// Writes a small vortex file and returns its path, rooted in `dir`.
fn write_fixture(dir: &tempfile::TempDir) -> String {
    let output = dir.path().join("fixture.vortex");
    let output_path = output.to_str().unwrap().to_string();
    pipeline::run(
        move |ctx| async move { make_range_table(&ctx, N_ROWS, 128) },
        &output_path,
        PipelineOptions::default(),
    )
    .unwrap();
    output_path
}

/// The one vortex read function serves both cases: without a schema it infers
/// one from the file, with a schema it uses the one given.
#[test]
fn reads_with_inferred_or_given_schema() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_fixture(&dir);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let ctx = SessionContext::new();

        let inferred = read_vortex(&ctx, &path, VortexReadOptions::default())
            .await
            .unwrap();
        assert!(inferred.schema().has_column_with_unqualified_name("idx"));
        assert_eq!(inferred.count().await.unwrap(), N_ROWS as usize);

        let schema = Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, true)]));
        let given = read_vortex(
            &ctx,
            &path,
            VortexReadOptions {
                schema: Some(schema),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(given.count().await.unwrap(), N_ROWS as usize);
    });
}
