//! The per-test growing dataset fixture, to which a test adds a file.

use super::{FORMATS, extension, listed_beneath};
use crate::{
    locus::LocusRepresentation,
    pipeline::{self, PipelineOptions},
    row_ordering::RowOrdering,
    stored::dataset::Dataset,
    testing::fixture::{self, GrowingDatasetFixture},
};

#[test]
fn a_growing_dataset_holds_file_d_of_its_one_input_table_until_the_test_adds_c() {
    for format in FORMATS {
        let fixture = fixture::growing_dataset_fixture(
            &format!("growing-{}", extension(format)),
            format,
            LocusRepresentation::Packed,
        );
        let ext = extension(format);
        let listed = || listed_beneath(fixture.store().store(), fixture.table_path().prefix());

        assert_eq!(listed(), [format!("s=HG00308/d.{ext}")]);

        fixture.add_next_file();

        assert_eq!(
            listed(),
            [format!("s=HG00308/c.{ext}"), format!("s=HG00308/d.{ext}")]
        );
    }
}

#[test]
fn a_growing_dataset_discovers_as_one_single_sample_input_table() {
    let fixture = fixture::growing_dataset_fixture(
        "growing-discovered",
        fixture::FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    );
    let store = fixture.store().clone();
    let table_path = fixture.table_path().clone();
    let input_format = fixture.input_format();

    let names = pipeline::run(
        move |ctx| async move {
            store.register(&ctx);
            let dataset =
                Dataset::discover(&ctx, table_path, input_format, RowOrdering::locus(), None)
                    .await?;
            Ok(dataset
                .input_tables()
                .iter()
                .map(|table| table.name().to_string())
                .collect::<Vec<_>>())
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    assert_eq!(names, [format!("s={}", GrowingDatasetFixture::SAMPLE)]);
}

#[test]
#[should_panic(expected = "added already")]
fn adding_the_next_file_twice_panics() {
    let fixture = fixture::growing_dataset_fixture(
        "growing-twice",
        fixture::FixtureFormat::Parquet,
        LocusRepresentation::Packed,
    );
    fixture.add_next_file();
    fixture.add_next_file();
}
