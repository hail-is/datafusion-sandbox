use crate::{
    fixture::{self, FixtureFormat, SortedTableFixture},
    format::InputFormat,
    locus::{Locus, LocusRepresentation, SplitPoints},
    pipeline::{self, PipelineOptions},
    split_points,
};

use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array, StringViewArray},
        record_batch::RecordBatch,
    },
    common::DataFusionError,
    datasource::listing::ListingTableUrl,
    error::Result,
    execution::object_store::ObjectStoreUrl,
    parquet::arrow::ArrowWriter,
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::{num::NonZeroUsize, sync::Arc};

#[test]
fn row_balanced_chooses_the_loci_at_floor_indices() {
    let loci = (1..=10)
        .map(|position| Locus::new(1, position).unwrap())
        .collect::<Vec<_>>();
    for (format, format_name) in [
        (FixtureFormat::Parquet, "parquet"),
        (FixtureFormat::Vortex, "vortex"),
    ] {
        for (representation, representation_name) in [
            (LocusRepresentation::ContigPosition, "contig-position"),
            (LocusRepresentation::Packed, "packed"),
        ] {
            for (layout, files) in [
                ("file", vec![loci.clone()]),
                (
                    "directory",
                    vec![loci[0..4].to_vec(), loci[4..7].to_vec(), loci[7..].to_vec()],
                ),
            ] {
                let fixture = fixture::sorted_table_fixture(
                    &format!("row-balanced-{format_name}-{representation_name}-{layout}"),
                    format,
                    representation,
                    files,
                );
                let path = if layout == "file" {
                    fixture.file_path(0).clone()
                } else {
                    fixture.table_path().clone()
                };
                let input_format = fixture.input_format();

                let points = row_balanced(fixture, path, input_format, 4).unwrap();

                assert_eq!(
                    points.to_string(),
                    "1:3,1:6,1:8",
                    "{format_name} {representation_name} {layout}"
                );
            }
        }
    }
}

#[test]
fn row_balanced_handles_several_row_and_interval_counts() {
    for (name, row_count, intervals, expected) in [
        ("five-rows-two-intervals", 5, 2, "1:3"),
        ("nine-rows-three-intervals", 9, 3, "1:4,1:7"),
        ("ten-rows-four-intervals", 10, 4, "1:3,1:6,1:8"),
    ] {
        let loci = (1..=row_count)
            .map(|position| Locus::new(1, position).unwrap())
            .collect::<Vec<_>>();
        let fixture = fixture::sorted_table_fixture(
            &format!("row-balanced-counts-{name}"),
            FixtureFormat::Vortex,
            LocusRepresentation::Packed,
            vec![loci],
        );

        let points = row_balanced_over_table(fixture, intervals).unwrap();

        assert_eq!(points.to_string(), expected, "{name}");
    }
}

#[test]
fn row_balanced_rejects_a_table_with_no_rows() {
    let fixture = fixture::sorted_table_fixture(
        "row-balanced-empty",
        FixtureFormat::Vortex,
        LocusRepresentation::Packed,
        vec![Vec::new()],
    );

    let error =
        row_balanced_over_table(fixture, 2).expect_err("an empty table has no split-point row");

    assert!(matches!(error, DataFusionError::Configuration(_)));
    assert!(error.to_string().contains("table has no rows"), "{error}");
}

#[test]
fn row_balanced_rejects_target_rows_on_the_same_locus() {
    let repeated = Locus::new(1, 3).unwrap();
    let loci = vec![
        Locus::new(1, 1).unwrap(),
        Locus::new(1, 2).unwrap(),
        repeated,
        repeated,
        repeated,
        repeated,
        repeated,
        Locus::new(1, 8).unwrap(),
        Locus::new(1, 9).unwrap(),
        Locus::new(1, 10).unwrap(),
    ];
    let fixture = fixture::sorted_table_fixture(
        "row-balanced-shared-locus",
        FixtureFormat::Parquet,
        LocusRepresentation::ContigPosition,
        vec![loci],
    );

    let error = row_balanced_over_table(fixture, 4)
        .expect_err("two target rows at one locus cannot define distinct intervals");

    assert!(matches!(error, DataFusionError::Configuration(_)));
    let message = error.to_string();
    assert!(
        message.contains("cannot be balanced into 4 intervals"),
        "{message}"
    );
    assert!(message.contains("1:3 does not follow 1:3"), "{message}");
}

#[test]
fn row_balanced_rejects_when_two_targets_name_the_same_row() {
    let fixture = fixture::sorted_table_fixture(
        "row-balanced-repeated-target-row",
        FixtureFormat::Vortex,
        LocusRepresentation::Packed,
        vec![vec![Locus::new(1, 1).unwrap(), Locus::new(1, 2).unwrap()]],
    );

    let error = row_balanced_over_table(fixture, 4)
        .expect_err("one row cannot supply two strictly increasing split points");

    assert!(matches!(error, DataFusionError::Configuration(_)));
    let message = error.to_string();
    assert!(
        message.contains("cannot be balanced into 4 intervals"),
        "{message}"
    );
    assert!(message.contains("1:2 does not follow 1:2"), "{message}");
}

#[test]
fn row_balanced_rejects_a_vortex_path_read_as_parquet_before_computing() {
    let fixture = fixture::sorted_table_fixture(
        "row-balanced-format-mismatch",
        FixtureFormat::Vortex,
        LocusRepresentation::Packed,
        vec![vec![Locus::new(1, 1).unwrap(), Locus::new(1, 2).unwrap()]],
    );
    let path = fixture.file_path(0).clone();
    assert!(path.as_str().ends_with(".vortex"), "{}", path.as_str());

    let error = row_balanced(fixture, path, InputFormat::PARQUET, 2)
        .expect_err("a Vortex file is not a Parquet table");

    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert!(
        error.to_string().contains("no input files found"),
        "{error}"
    );
}

#[test]
fn row_balanced_rejects_fewer_than_two_intervals() {
    let fixture = fixture::sorted_table_fixture(
        "row-balanced-one-interval",
        FixtureFormat::Vortex,
        LocusRepresentation::Packed,
        vec![vec![Locus::new(1, 1).unwrap(), Locus::new(1, 2).unwrap()]],
    );

    let error = row_balanced_over_table(fixture, 1).expect_err("one interval has no split point");

    assert!(matches!(error, DataFusionError::Configuration(_)));
    assert!(error.to_string().contains("at least 2"), "{error}");
}

#[test]
fn row_balanced_rejects_a_contig_name_without_an_ordinal() {
    let error = pipeline::run(
        |ctx| async move {
            let store = Arc::new(InMemory::new());
            ctx.register_object_store(ObjectStoreUrl::parse("memory://")?.as_ref(), store.clone());
            let contigs: ArrayRef = Arc::new(StringViewArray::from(vec!["chr01", "chrX"]));
            let positions: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
            let batch =
                RecordBatch::try_from_iter(vec![("contig", contigs), ("position", positions)])?;
            let mut bytes = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None)?;
            writer.write(&batch)?;
            writer.close()?;
            store
                .put(&Path::from("foreign-contig.parquet"), bytes.into())
                .await?;
            let table_path = ListingTableUrl::parse("memory:///foreign-contig.parquet")?;
            let intervals = NonZeroUsize::new(2).unwrap();
            split_points::row_balanced(&ctx, table_path, InputFormat::PARQUET, intervals).await
        },
        PipelineOptions::single_threaded(),
    )
    .expect_err("a selected foreign contig name must be rejected");

    assert!(matches!(error, DataFusionError::Configuration(_)));
    assert!(
        error.to_string().contains("invalid contig name 'chrX'"),
        "{error}"
    );
}

/// Computes split points over the fixture's whole table directory under its own format.
fn row_balanced_over_table(fixture: SortedTableFixture, intervals: usize) -> Result<SplitPoints> {
    let path = fixture.table_path().clone();
    let input_format = fixture.input_format();
    row_balanced(fixture, path, input_format, intervals)
}

fn row_balanced(
    fixture: SortedTableFixture,
    path: ListingTableUrl,
    input_format: InputFormat,
    intervals: usize,
) -> Result<SplitPoints> {
    let intervals = NonZeroUsize::new(intervals).unwrap();
    pipeline::run(
        move |ctx| {
            fixture.register(&ctx);
            async move { split_points::row_balanced(&ctx, path, input_format, intervals).await }
        },
        PipelineOptions::single_threaded(),
    )
}
