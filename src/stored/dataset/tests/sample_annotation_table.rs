//! Sample annotation tables written around a data write, named, and read back, over in-memory
//! object stores.

#![expect(
    clippy::as_conversions,
    reason = "the test controls the concrete array types erased behind ArrayRef"
)]

use crate::{
    fixture::{self, MemoryStore},
    format::{InputFormat, OutputFormat},
    ordered_frame::OutputLayout,
    pipeline::{self, PipelineOptions},
    stored::dataset::sample_annotation_table::{self, annotated_stem, write_marked},
    write::WriteTarget,
};

use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    common::DataFusionError,
    datasource::listing::ListingTableUrl,
    error::Result,
    prelude::SessionContext,
};
use object_store::{ObjectStoreExt, path::Path};

use std::{future::Future, sync::Arc};

/// The table sits beside the data: one file's stem drops the format's extension, if the path has
/// it, and a directory's stem is its path without a trailing slash. The table takes the format's
/// extension either way, and its name is one [`annotated_stem`] reads that stem back from.
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
            "out/g0.vortex",
            OutputFormat::VORTEX,
            OutputLayout::SingleFile,
            "out/g0.samples.vortex",
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
        let extension = output_format.extension();
        let store = MemoryStore::new("annotations");
        let target = WriteTarget {
            output_path: format!("{}{output_path}", store.url().as_str()),
            output_format,
        };
        let description = format!("{output_path} {layout:?}");

        let locations = on_store(&store, move |ctx, store| async move {
            write_marked(&ctx, &target, layout, Some(&samples(&["a"])), async {
                Ok(())
            })
            .await?;
            Ok(store.locations_under("out").await)
        });

        assert_eq!(locations, [Path::from(expected)], "{description}");
        let name = locations[0].filename().unwrap();
        let stem = output_path
            .trim_start_matches("out/")
            .trim_end_matches('/')
            .trim_end_matches(&format!(".{extension}"));
        assert_eq!(annotated_stem(name, extension), Some(stem), "{description}");
    }
}

/// A name is an annotation table's only with `.samples` and then the given extension after its
/// stem, which may itself hold dots or be empty.
#[test]
fn annotated_stem_names_the_input_table_a_table_annotates() {
    for (file_name, extension, expected) in [
        ("g0.samples.parquet", "parquet", Some("g0")),
        ("run.2.samples.vortex", "vortex", Some("run.2")),
        (".samples.parquet", "parquet", Some("")),
        ("g0.parquet", "parquet", None),
        ("g0.samples.vortex", "parquet", None),
        ("g0.samples", "parquet", None),
        ("g0samples.parquet", "parquet", None),
        ("g0.samples.parquet.tmp", "parquet", None),
    ] {
        assert_eq!(
            annotated_stem(file_name, extension),
            expected,
            "{file_name} {extension}"
        );
    }
}

/// A written table holds the sample set sorted, whatever order the caller gave it in, and reads
/// back in either format as that set. Its one column is the dataset's sample column: a non-null
/// view string `s`.
#[test]
fn a_written_table_reads_back_sorted_in_its_schema() {
    for (output_format, input_format) in [
        (OutputFormat::PARQUET, InputFormat::PARQUET),
        (OutputFormat::VORTEX, InputFormat::VORTEX),
    ] {
        let extension = output_format.extension();
        let store = MemoryStore::new("annotations");
        let root = format!("{}out/", store.url().as_str());
        let target = WriteTarget {
            output_path: format!("{root}g0.{extension}"),
            output_format,
        };
        let path = format!("{root}g0.samples.{extension}");

        let (sample_set, batches) = on_store(&store, move |ctx, _| async move {
            let sample_set = samples(&["NA18534", "HG00308", "HG02230"]);
            write_marked(
                &ctx,
                &target,
                OutputLayout::SingleFile,
                Some(&sample_set),
                async { Ok(()) },
            )
            .await?;
            let root = ListingTableUrl::parse(&root)?;
            Ok((
                sample_annotation_table::read(&ctx, &root, "g0", &input_format).await?,
                fixture::read_file(&ctx, &path, &input_format, None).await?,
            ))
        });

        assert_eq!(sample_set, ["HG00308", "HG02230", "NA18534"], "{extension}");
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 1, "{extension}");
        assert_eq!(schema.field(0).name(), "s", "{extension}");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8View);
        assert!(!schema.field(0).is_nullable(), "{extension}");
        let rows = batches
            .iter()
            .flat_map(|batch| fixture::string_column(batch, "s"))
            .collect::<Vec<_>>();
        assert_eq!(rows, ["HG00308", "HG02230", "NA18534"], "{extension}");
    }
}

/// An earlier write's table is gone before the data write starts, so no table marks data while it
/// is being replaced, and the new table is there once the data write is done.
#[test]
fn an_earlier_table_is_removed_before_the_data_write_starts() {
    let store = MemoryStore::new("annotations");
    let target = target_on(&store);

    let (during, after) = on_store(&store, move |ctx, store| async move {
        put_earlier_table(&store).await?;
        let listed = store.clone();
        let during = write_marked(
            &ctx,
            &target,
            OutputLayout::SingleFile,
            Some(&samples(&["a"])),
            async move { Ok(listed.locations_under("out").await) },
        )
        .await?;
        Ok((during, store.locations_under("out").await))
    });

    assert_eq!(during, Vec::<Path>::new());
    assert_eq!(after, [Path::from("out/g0.samples.parquet")]);
}

/// A write with no sample set only removes an earlier write's table.
#[test]
fn a_write_without_a_sample_set_leaves_no_table() {
    let store = MemoryStore::new("annotations");
    let target = target_on(&store);

    let (written, after) = on_store(&store, move |ctx, store| async move {
        put_earlier_table(&store).await?;
        let written = write_marked(&ctx, &target, OutputLayout::SingleFile, None, async {
            Ok(7)
        })
        .await?;
        Ok((written, store.locations_under("out").await))
    });

    assert_eq!(written, 7);
    assert_eq!(after, Vec::<Path>::new());
}

/// A data write that fails returns its error and leaves no table, neither its own nor an earlier
/// write's, which would mark the partial data complete.
#[test]
fn a_failed_data_write_returns_its_error_and_leaves_no_table() {
    let store = MemoryStore::new("annotations");
    let target = target_on(&store);

    let (failed, after) = on_store(&store, move |ctx, store| async move {
        put_earlier_table(&store).await?;
        let failed = write_marked(
            &ctx,
            &target,
            OutputLayout::SingleFile,
            Some(&samples(&["a"])),
            async { Err::<(), _>(DataFusionError::Execution("the data write failed".into())) },
        )
        .await;
        Ok((failed, store.locations_under("out").await))
    });

    let message = failed.unwrap_err().to_string();
    assert!(message.contains("the data write failed"), "{message}");
    assert_eq!(after, Vec::<Path>::new());
}

/// Reading requires a non-null string `s`, whatever other columns the table holds, and names the
/// table when it has none.
#[test]
fn rejects_a_table_without_a_non_null_string_sample_column_naming_it() {
    for (case, invalid_sample) in [
        ("missing", vec![]),
        (
            "nullable",
            vec![(
                Field::new("s", DataType::Utf8, true),
                Arc::new(StringArray::from(vec!["sample-b"])) as ArrayRef,
            )],
        ),
        (
            "not a string",
            vec![(
                Field::new("s", DataType::Int32, false),
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            )],
        ),
    ] {
        let note = (
            Field::new("note", DataType::Utf8, false),
            Arc::new(StringArray::from(vec!["an ignored column"])) as ArrayRef,
        );

        let error = read_written(invalid_sample.into_iter().chain([note]).collect())
            .expect_err("a table without a valid `s` must not read");

        assert!(matches!(error, DataFusionError::Plan(_)), "{case}: {error}");
        let message = error.to_string();
        assert!(
            message.contains("out/g0.samples.vortex") && message.contains("'s'"),
            "{case}: unexpected error: {message}"
        );
    }
}

/// A sample set holds each sample once, so a table that declares one twice is rejected, naming
/// the table and the sample.
#[test]
fn rejects_a_table_that_declares_a_sample_twice() {
    let error = read_written(vec![sample_column(&["HG00308", "NA18534", "HG00308"])])
        .expect_err("a duplicate sample must not read");

    let message = error.to_string();
    assert!(
        message.contains("out/g0.samples.vortex") && message.contains("'HG00308' more than once"),
        "unexpected error: {message}"
    );
}

/// A table with no rows declares no samples, which no input table may have.
#[test]
fn rejects_a_table_that_declares_no_samples() {
    let error = read_written(vec![sample_column(&[])]).expect_err("no samples must not read");

    let message = error.to_string();
    assert!(
        message.contains("out/g0.samples.vortex") && message.contains("declares no samples"),
        "unexpected error: {message}"
    );
}

fn samples(ids: &[&str]) -> Vec<String> {
    ids.iter().map(ToString::to_string).collect()
}

/// A one-file Parquet write to `out/g0.parquet` on `store`.
fn target_on(store: &MemoryStore) -> WriteTarget {
    WriteTarget {
        output_path: format!("{}out/g0.parquet", store.url().as_str()),
        output_format: OutputFormat::PARQUET,
    }
}

/// Puts an earlier write's table beside [`target_on`]'s data.
async fn put_earlier_table(store: &MemoryStore) -> Result<()> {
    store
        .store()
        .put(&Path::from("out/g0.samples.parquet"), "earlier".into())
        .await?;
    Ok(())
}

fn sample_column(ids: &[&str]) -> (Field, ArrayRef) {
    (
        Field::new("s", DataType::Utf8, false),
        Arc::new(StringArray::from(ids.to_vec())),
    )
}

/// Reads the sample set of input table `g0` under `out/` after writing `columns` as its Vortex
/// annotation table, without the module's writer.
fn read_written(columns: Vec<(Field, ArrayRef)>) -> Result<Vec<String>> {
    let store = MemoryStore::new("annotations");
    let root = format!("{}out/", store.url().as_str());
    let (fields, arrays) = columns.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    on_store(&store, move |ctx, _| async move {
        WriteTarget {
            output_path: format!("{root}g0.samples.vortex"),
            output_format: OutputFormat::VORTEX,
        }
        .write_unordered(ctx.read_batch(batch)?)
        .await?;
        Ok(sample_annotation_table::read(
            &ctx,
            &ListingTableUrl::parse(&root)?,
            "g0",
            &InputFormat::VORTEX,
        )
        .await)
    })
}

/// Runs `body` in a pipeline whose session has `store` registered, and returns its result.
fn on_store<T, Fut>(
    store: &MemoryStore,
    body: impl FnOnce(SessionContext, MemoryStore) -> Fut + Send + 'static,
) -> T
where
    T: Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    let store = store.clone();
    pipeline::run(
        move |ctx| {
            store.register(&ctx);
            body(ctx, store)
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}
