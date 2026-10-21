//! The per-test dataset fixtures on local disk.
//!
//! These tests read and list the local filesystem, which ADR 0010 otherwise keeps tests off: a
//! disk fixture's contract is to put a dataset on local disk for the tests that need one, and to
//! remove it when dropped, so only the filesystem shows whether it keeps that contract.

use super::{DOCUMENTED_FILES, FORMATS, documented_rows, extension, loci_and_alleles, read_back};
use crate::{
    locus::LocusRepresentation,
    testing::fixture::{self, DiskDatasetFixture, FixtureFormat, SAMPLES, block_on},
};

use std::path::Path;

/// The disk fixtures of `format`, each with the representation it writes.
fn disk_fixtures(format: FixtureFormat) -> [(DiskDatasetFixture, LocusRepresentation); 2] {
    [
        (
            fixture::contig_position_disk_fixture(format),
            LocusRepresentation::ContigPosition,
        ),
        (
            fixture::packed_disk_fixture(format),
            LocusRepresentation::Packed,
        ),
    ]
}

/// The names of the entries of the directory `path`, sorted.
fn entries(path: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[test]
fn a_disk_fixture_holds_the_four_documented_files_of_every_sample_on_local_disk() {
    for format in FORMATS {
        for (fixture, representation) in disk_fixtures(format) {
            let root = Path::new(fixture.table_path());
            let ext = extension(format);
            let context = format!("{format:?} {representation:?}");

            let samples: Vec<String> = SAMPLES.iter().map(|sample| format!("s={sample}")).collect();
            assert_eq!(entries(root), samples, "{context}");
            for sample in &samples {
                assert_eq!(
                    entries(&root.join(sample)),
                    ["a", "b", "c", "d"].map(|stem| format!("{stem}.{ext}")),
                    "{context}"
                );
            }
            for sample in &samples {
                for (stem, _) in DOCUMENTED_FILES {
                    let path = root.join(sample).join(format!("{stem}.{ext}"));
                    let batches = read_back(
                        path.to_str().unwrap().to_string(),
                        fixture.input_format(),
                        |_| {},
                    );
                    assert_eq!(
                        loci_and_alleles(&batches, representation),
                        documented_rows(stem),
                        "{context} {sample} {stem}"
                    );
                }
            }
        }
    }
}

#[test]
fn dropping_a_disk_fixture_removes_its_directory() {
    // Retaining fixtures for debugging keeps them on purpose.
    if std::env::var("DATAFUSION_SANDBOX_KEEP_FIXTURES").as_deref() == Ok("1") {
        return;
    }
    for (fixture, _) in disk_fixtures(FixtureFormat::Parquet) {
        let root = Path::new(fixture.table_path()).to_path_buf();
        assert!(root.is_dir(), "{}", root.display());

        drop(fixture);

        assert!(!root.exists(), "{}", root.display());
    }
}

#[test]
#[should_panic(expected = "build the dataset fixture before calling pipeline::run")]
fn building_a_disk_fixture_inside_a_runtime_panics() {
    block_on(async {
        drop(fixture::packed_disk_fixture(FixtureFormat::Parquet));
    });
}
