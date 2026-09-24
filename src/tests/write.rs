//! File writes through a write target, over in-memory object stores.

use crate::{
    dataset::Dataset,
    fixture::{self, FixtureFormat, MemoryStore},
    format::OutputFormat,
    formulation::Formulation,
    generated::make_range_table,
    locus::LocusRepresentation,
    pipeline::{self, PipelineOptions},
    run_metrics::WriteRecord,
    write::WriteTarget,
};

use datafusion::datasource::listing::ListingTableUrl;
use futures_util::TryStreamExt;
use object_store::{ObjectStoreExt, path::Path};
use std::sync::Arc;

/// A write target describes its recorded settings: its output path, its format's name, and its
/// compression as the caller spelled it, or none at the format's default.
#[test]
fn a_write_target_describes_its_recorded_settings() {
    let record = |output_path: &str, output_format: &str, compression: Option<&str>| WriteRecord {
        output_path: output_path.to_string(),
        output_format: output_format.to_string(),
        compression: compression.map(ToString::to_string),
    };

    for (output_path, output_format, expected) in [
        (
            "gs://bucket/out.parquet",
            OutputFormat::PARQUET,
            record("gs://bucket/out.parquet", "parquet", None),
        ),
        (
            "out.parquet",
            OutputFormat::PARQUET.with_compression("zstd(3)").unwrap(),
            record("out.parquet", "parquet", Some("zstd(3)")),
        ),
        (
            "out.vortex",
            OutputFormat::VORTEX,
            record("out.vortex", "vortex", None),
        ),
        (
            "out.vortex",
            OutputFormat::VORTEX.with_compression("compact").unwrap(),
            record("out.vortex", "vortex", Some("compact")),
        ),
    ] {
        let target = WriteTarget {
            output_path: output_path.to_string(),
            output_format,
        };
        assert_eq!(WriteRecord::from(&target), expected, "{target:?}");
    }
}

#[test]
fn an_ordered_write_uses_its_frames_single_file_layout() {
    let input = Arc::clone(fixture::dataset_fixture(
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    ));
    let output = MemoryStore::new("write-single");
    let output_path = format!("{}combined.parquet", output.url().as_str());
    let target = WriteTarget {
        output_path: output_path.clone(),
        output_format: OutputFormat::PARQUET,
    };

    let (executed, metadata) = pipeline::run(
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
            let ordered = formulation.plan(&ctx, &dataset).await?;
            let executed = target.write(ordered).await?;
            let metadata = output.store().head(&Path::from("combined.parquet")).await?;
            Ok((executed, metadata))
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    assert_eq!(executed.rows_written, 32);
    assert!(executed.execute_ns > 0);
    assert!(executed.plan.metrics().is_some());
    assert!(metadata.size > 0, "{output_path}");
}

#[test]
fn an_ordered_write_uses_its_frames_file_per_partition_layout() {
    let input = Arc::clone(fixture::dataset_fixture(
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    ));
    let output = MemoryStore::new("write-partitioned");
    let directory = format!("{}combined", output.url().as_str());
    let output_format = OutputFormat::PARQUET;
    let predicted: Vec<Path> = (0..3)
        .map(|index| {
            let path = output_format.partition_file_path(&directory, index, 3);
            ListingTableUrl::parse(path).unwrap().prefix().clone()
        })
        .collect();
    let target = WriteTarget {
        output_path: directory,
        output_format,
    };

    let (executed, mut written) = pipeline::run(
        move |ctx| async move {
            input.register(&ctx);
            output.register(&ctx);
            let formulation = Formulation::CombineRefsIntervalMerge {
                split_points: "1:3,2:2".parse()?,
            };
            let dataset = Dataset::discover(
                &ctx,
                input.table_path().clone(),
                input.input_format(),
                formulation.required_ordering(),
                None,
            )
            .await?;
            let ordered = formulation.plan(&ctx, &dataset).await?;
            let executed = target.write(ordered).await?;
            let written: Vec<Path> = output
                .store()
                .list(Some(&Path::from("combined")))
                .map_ok(|metadata| metadata.location)
                .try_collect()
                .await?;
            Ok((executed, written))
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    written.sort();
    assert_eq!(executed.rows_written, 32);
    assert!(executed.execute_ns > 0);
    assert!(executed.plan.metrics().is_some());
    assert_eq!(written, predicted);
}

#[test]
fn unordered_writes_return_what_they_executed() {
    for (output_format, extension) in [
        (OutputFormat::PARQUET, "parquet"),
        (OutputFormat::VORTEX, "vortex"),
    ] {
        let store = MemoryStore::new(extension);
        let object_path = Path::from(format!("rows.{extension}"));
        let target = WriteTarget {
            output_path: format!("{}rows.{extension}", store.url().as_str()),
            output_format,
        };

        let (executed, metadata, bytes) = pipeline::run(
            move |ctx| async move {
                store.register(&ctx);
                let frame = make_range_table(&ctx, 1000, 128)?;
                let executed = target.write_unordered(frame).await?;
                let metadata = store.store().head(&object_path).await?;
                let bytes = store.store().get(&object_path).await?.bytes().await?;
                Ok((executed, metadata, bytes))
            },
            PipelineOptions::single_threaded(),
        )
        .unwrap();

        assert_eq!(executed.rows_written, 1000, "{extension}");
        assert!(executed.execute_ns > 0, "{extension}");
        assert!(executed.plan.metrics().is_some(), "{extension}");
        assert!(metadata.size > 0, "{extension}");
        assert_eq!(
            u64::try_from(bytes.len()).unwrap(),
            metadata.size,
            "{extension}"
        );
    }
}
