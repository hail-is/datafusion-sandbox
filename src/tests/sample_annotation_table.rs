//! Sample annotation table paths, and writes over in-memory object stores.

use crate::{
    fixture::{self, MemoryStore},
    format::{InputFormat, OutputFormat},
    ordered_frame::OutputLayout,
    pipeline::{self, PipelineOptions},
    sample_annotation_table,
    write::WriteTarget,
};

use datafusion::arrow::datatypes::DataType;
use object_store::path::Path;

/// The table sits beside the data: one file's stem drops the format's extension, if the path has
/// it, and a directory's stem is its path without a trailing slash. The table takes the format's
/// extension either way.
#[test]
fn the_table_takes_the_stem_of_the_output_path_and_the_formats_extension() {
    for (output_path, output_format, layout, expected) in [
        (
            "out/g0.parquet",
            OutputFormat::PARQUET,
            OutputLayout::SingleFile,
            "out/g0.samples.parquet",
        ),
        (
            "gs://bucket/out/g0.vortex",
            OutputFormat::VORTEX,
            OutputLayout::SingleFile,
            "gs://bucket/out/g0.samples.vortex",
        ),
        (
            "out/g0",
            OutputFormat::PARQUET,
            OutputLayout::SingleFile,
            "out/g0.samples.parquet",
        ),
        (
            "out/g1",
            OutputFormat::PARQUET,
            OutputLayout::FilePerPartition,
            "out/g1.samples.parquet",
        ),
        (
            "out/g1/",
            OutputFormat::VORTEX,
            OutputLayout::FilePerPartition,
            "out/g1.samples.vortex",
        ),
    ] {
        let target = WriteTarget {
            output_path: output_path.to_string(),
            output_format,
        };
        assert_eq!(
            sample_annotation_table::path(&target, layout),
            expected,
            "{target:?} {layout:?}"
        );
    }
}

/// A written table reads back in either format with the declared schema, a non-null view string
/// `s`, and holds the sample set sorted, whatever order the caller gave it in.
#[test]
fn a_written_table_reads_back_sorted_in_its_schema() {
    for (output_format, input_format) in [
        (OutputFormat::PARQUET, InputFormat::PARQUET),
        (OutputFormat::VORTEX, InputFormat::VORTEX),
    ] {
        let extension = output_format.extension();
        let store = MemoryStore::new("annotations");
        let target = WriteTarget {
            output_path: format!("{}out/g0.{extension}", store.url().as_str()),
            output_format,
        };
        let path = sample_annotation_table::path(&target, OutputLayout::SingleFile);
        let read_store = store.clone();

        let (batches, locations) = pipeline::run(
            move |ctx| async move {
                store.register(&ctx);
                let sample_set = ["NA18534", "HG00308", "HG02230"].map(ToString::to_string);
                sample_annotation_table::write(
                    &ctx,
                    &target,
                    OutputLayout::SingleFile,
                    &sample_set,
                )
                .await?;
                let batches = fixture::read_file(&ctx, &path, &input_format, None).await?;
                Ok((batches, read_store.locations_under("out").await))
            },
            PipelineOptions::single_threaded(),
        )
        .unwrap();

        assert_eq!(
            locations,
            [Path::from(format!("out/g0.samples.{extension}"))],
            "{extension}"
        );
        let schema = batches[0].schema();
        assert_eq!(schema, sample_annotation_table::schema(), "{extension}");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8View);
        assert!(!schema.field(0).is_nullable());
        let samples = batches
            .iter()
            .flat_map(|batch| fixture::string_column(batch, "s"))
            .collect::<Vec<_>>();
        assert_eq!(samples, ["HG00308", "HG02230", "NA18534"], "{extension}");
    }
}
