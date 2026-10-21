//! The per-test locus-sorted table fixture, with caller-chosen files of loci.

use super::{
    FORMATS, REPRESENTATIONS, column_names, extension, listed_beneath, locus_columns, read_back,
};
use crate::{
    locus::{Locus, LocusRepresentation},
    testing::fixture::{self, FixtureFormat, block_on},
};

use std::sync::Arc;

fn loci(points: &[(u32, i32)]) -> Vec<Locus> {
    points
        .iter()
        .map(|&(contig, position)| Locus::new(contig, position).unwrap())
        .collect()
}

#[test]
fn a_sorted_table_fixture_names_its_files_in_reverse_of_locus_order() {
    for format in FORMATS {
        let fixture = fixture::sorted_table_fixture(
            &format!("sorted-names-{}", extension(format)),
            format,
            LocusRepresentation::Packed,
            vec![loci(&[(1, 1)]), loci(&[(1, 2)]), loci(&[(2, 1)])],
        );
        let ext = extension(format);

        let listed = listed_beneath(fixture.store(), fixture.table_path().prefix());
        let file_names: Vec<&str> = (0..3)
            .map(|index| fixture.file_path(index).prefix().filename().unwrap())
            .collect();

        assert_eq!(
            listed,
            [
                format!("0001.{ext}"),
                format!("0002.{ext}"),
                format!("0003.{ext}")
            ]
        );
        assert_eq!(
            file_names,
            [
                format!("0003.{ext}"),
                format!("0002.{ext}"),
                format!("0001.{ext}")
            ]
        );
        assert!(fixture.table_path().is_collection());
    }
}

#[test]
fn each_file_of_a_sorted_table_fixture_holds_its_loci_in_its_representation() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let files = vec![loci(&[(1, 1), (1, 1), (1, 5)]), loci(&[(1, 5), (3, 2)])];
            let fixture = Arc::new(fixture::sorted_table_fixture(
                &format!("sorted-loci-{}-{representation}", extension(format)),
                format,
                representation,
                files.clone(),
            ));

            for (index, expected) in files.into_iter().enumerate() {
                let registered = Arc::clone(&fixture);
                let batches = read_back(
                    fixture.file_path(index).as_str().to_string(),
                    fixture.input_format(),
                    move |ctx| registered.register(ctx),
                );

                let context = format!("{format:?} {representation:?} file {index}");
                assert_eq!(
                    column_names(&batches),
                    locus_columns(representation),
                    "{context}"
                );
                let read: Vec<Locus> = batches
                    .iter()
                    .flat_map(|batch| fixture::decode_loci(batch, representation))
                    .collect();
                assert_eq!(read, expected, "{context}");
            }
            assert_eq!(fixture.representation(), representation);
        }
    }
}

#[test]
#[should_panic(expected = "sorted-table fixture has no file 1")]
fn file_path_panics_past_the_last_file() {
    let fixture = fixture::sorted_table_fixture(
        "sorted-past-the-end",
        FixtureFormat::Parquet,
        LocusRepresentation::Packed,
        vec![loci(&[(1, 1)])],
    );

    let _past_the_end = fixture.file_path(1);
}

#[test]
#[should_panic(expected = "a sorted-table fixture needs at least one file")]
fn a_sorted_table_fixture_of_no_files_panics() {
    drop(fixture::sorted_table_fixture(
        "sorted-empty",
        FixtureFormat::Parquet,
        LocusRepresentation::Packed,
        Vec::new(),
    ));
}

#[test]
#[should_panic(expected = "build the sorted-table fixture before calling pipeline::run")]
fn building_a_sorted_table_fixture_inside_a_runtime_panics() {
    block_on(async {
        drop(fixture::sorted_table_fixture(
            "sorted-in-a-runtime",
            FixtureFormat::Parquet,
            LocusRepresentation::Packed,
            vec![loci(&[(1, 1)])],
        ));
    });
}
