//! The shared in-memory dataset fixtures: one per format and representation, the mixed ones, and
//! the one without alleles.

use super::{
    DOCUMENTED_FILES, FORMATS, REPRESENTATIONS, column_names, documented_rows, extension,
    listed_beneath, loci_and_alleles, locus_columns, read_back,
};
use crate::{
    formulation::Formulation,
    locus::{Locus, LocusRepresentation},
    pipeline::{self, PipelineOptions},
    row_ordering::RowOrdering,
    stored::dataset::{Dataset, InputTable},
    testing::fixture::{
        self, DatasetFixture, FixtureFormat, INPUT_TABLES, MIXED_INPUT_TABLES, MIXED_SAMPLE_SETS,
        SAMPLES, block_on,
    },
};

use datafusion::{datasource::listing::ListingTableUrl, prelude::SessionContext};
use object_store::ObjectStoreExt;
use std::sync::Arc;

#[test]
fn the_input_tables_are_one_single_sample_table_per_sample_in_order() {
    let named: Vec<String> = SAMPLES.iter().map(|sample| format!("s={sample}")).collect();

    assert_eq!(INPUT_TABLES, named);
}

#[test]
fn the_mixed_input_tables_split_the_samples_in_order_ending_in_a_single_sample_table() {
    assert_eq!(MIXED_SAMPLE_SETS.len(), MIXED_INPUT_TABLES.len());
    assert_eq!(MIXED_SAMPLE_SETS.concat(), SAMPLES);
    assert_eq!(
        MIXED_INPUT_TABLES.last(),
        Some(&format!("s={}", SAMPLES.last().unwrap()).as_str())
    );
    assert!(MIXED_INPUT_TABLES.is_sorted());
}

/// The dataset `fixture` holds, discovered under `ordering`. Discovery reads multi-sample tables'
/// sample annotation tables, which executes plans, so it runs in the pipeline runner.
fn discovered(fixture: &Arc<DatasetFixture>, ordering: RowOrdering) -> Dataset {
    let fixture = Arc::clone(fixture);
    pipeline::run(
        move |ctx| async move {
            fixture.register(&ctx);
            Dataset::discover(
                &ctx,
                fixture.table_path().clone(),
                fixture.input_format(),
                ordering,
                None,
            )
            .await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

/// The fixture's files, by path below its dataset root.
fn listed(fixture: &DatasetFixture) -> Vec<String> {
    listed_beneath(fixture.store(), fixture.table_path().prefix())
}

/// The files of a single-sample input table of `sample`, by path below the dataset root.
fn sample_table_files(sample: &str, format: FixtureFormat) -> Vec<String> {
    ["a", "b", "c", "d"]
        .map(|stem| format!("s={sample}/{stem}.{}", extension(format)))
        .to_vec()
}

#[test]
fn a_dataset_fixture_holds_the_four_documented_files_of_every_sample() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let fixture = fixture::dataset_fixture(format, representation);

            let expected: Vec<String> = SAMPLES
                .iter()
                .flat_map(|sample| sample_table_files(sample, format))
                .collect();
            assert_eq!(listed(fixture), expected, "{format:?} {representation:?}");
            assert_eq!(fixture.representation(), representation);
        }
    }
}

#[test]
fn each_file_of_a_dataset_fixture_holds_its_documented_rows_in_its_representation() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let fixture = Arc::clone(fixture::dataset_fixture(format, representation));
            for (stem, _) in DOCUMENTED_FILES {
                let context = format!("{format:?} {representation:?} {stem}");
                let path = format!(
                    "{}s={}/{stem}.{}",
                    fixture.table_path().as_str(),
                    SAMPLES[0],
                    extension(format)
                );
                let registered = Arc::clone(&fixture);

                let batches = read_back(path, fixture.input_format(), move |ctx| {
                    registered.register(ctx);
                });

                let mut columns = locus_columns(representation);
                columns.push("alleles".to_string());
                assert_eq!(column_names(&batches), columns, "{context}");
                assert_eq!(
                    loci_and_alleles(&batches, representation),
                    documented_rows(stem),
                    "{context}"
                );
            }
        }
    }
}

#[test]
fn a_dataset_fixture_discovers_as_its_input_tables() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for (fixture, input_tables, sample_sets) in [
                (
                    fixture::dataset_fixture(format, representation),
                    INPUT_TABLES,
                    SAMPLES
                        .iter()
                        .map(|sample| vec![*sample])
                        .collect::<Vec<_>>(),
                ),
                (
                    fixture::mixed_dataset_fixture(format, representation),
                    MIXED_INPUT_TABLES,
                    MIXED_SAMPLE_SETS.iter().map(|set| set.to_vec()).collect(),
                ),
            ] {
                let dataset = discovered(
                    fixture,
                    Formulation::CombineAllelesUnion.required_ordering(),
                );

                let context = format!("{format:?} {representation:?} {input_tables:?}");
                let names: Vec<&str> = dataset
                    .input_tables()
                    .iter()
                    .map(InputTable::name)
                    .collect();
                let sets: Vec<Vec<&str>> = dataset
                    .input_tables()
                    .iter()
                    .map(|table| table.sample_set().iter().map(String::as_str).collect())
                    .collect();
                assert_eq!(names, input_tables, "{context}");
                assert_eq!(sets, sample_sets, "{context}");
                assert_eq!(dataset.locus_representation(), representation, "{context}");
            }
        }
    }
}

#[test]
fn each_format_and_representation_shares_one_fixture_on_its_own_store() {
    let mut urls = Vec::new();
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for fixture in [fixture::dataset_fixture, fixture::mixed_dataset_fixture] {
                assert!(Arc::ptr_eq(
                    fixture(format, representation),
                    fixture(format, representation)
                ));
                urls.push(
                    fixture(format, representation)
                        .table_path()
                        .object_store()
                        .as_str()
                        .to_string(),
                );
            }
        }
    }
    urls.push(
        fixture::vortex_without_alleles_fixture()
            .table_path()
            .object_store()
            .as_str()
            .to_string(),
    );

    let count = urls.len();
    urls.sort();
    urls.dedup();
    assert_eq!(urls.len(), count, "{urls:?}");
}

#[test]
fn the_output_root_is_a_directory_beside_the_dataset_on_its_registered_store() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let ctx = SessionContext::new();
    fixture.register(&ctx);

    let output_root = ListingTableUrl::parse(fixture.output_root()).unwrap();

    assert_eq!(
        output_root.object_store(),
        fixture.table_path().object_store()
    );
    assert!(
        !output_root
            .prefix()
            .prefix_matches(fixture.table_path().prefix()),
        "{output_root}"
    );
    assert!(
        !fixture
            .table_path()
            .prefix()
            .prefix_matches(output_root.prefix()),
        "{output_root}"
    );
    ctx.runtime_env().object_store(&output_root).unwrap();
}

/// A fixture registered with gated reads holds the reads of its dataset's files on the session
/// until the gate is released.
#[test]
fn a_fixture_registered_with_gated_reads_holds_reads_of_its_dataset_until_released() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let ctx = SessionContext::new();
    let gate = fixture.register_with_gated_reads(&ctx);
    let store = ctx
        .runtime_env()
        .object_store(fixture.table_path())
        .unwrap();
    let file = object_store::path::Path::from(format!(
        "{}/{}",
        fixture.table_path().prefix(),
        sample_table_files(SAMPLES[0], FixtureFormat::Parquet)[0]
    ));

    let held = block_on(async {
        tokio::select! {
            biased;
            read = store.get(&file) => {
                read.unwrap();
                false
            }
            () = gate.reached() => true,
        }
    });
    gate.release();
    let released = block_on(async { store.get(&file).await?.bytes().await }).unwrap();

    assert!(held);
    assert!(!released.is_empty());
}

#[test]
fn sample_files_lists_a_samples_files_in_path_order_the_reverse_of_locus_order() {
    for format in FORMATS {
        let representation = LocusRepresentation::Packed;
        let fixture = Arc::clone(fixture::dataset_fixture(format, representation));

        let files = block_on(fixture.sample_files(SAMPLES[1]));

        let first_loci: Vec<Locus> = files
            .iter()
            .map(|file| {
                let path = format!(
                    "{}{}",
                    fixture.table_path().object_store().as_str(),
                    file.location
                );
                let registered = Arc::clone(&fixture);
                let batches = read_back(path, fixture.input_format(), move |ctx| {
                    registered.register(ctx);
                });
                loci_and_alleles(&batches, representation)[0].0
            })
            .collect();
        let expected: Vec<String> = sample_table_files(SAMPLES[1], format)
            .into_iter()
            .map(|file| format!("{}/{file}", fixture.table_path().prefix()))
            .collect();
        let locations: Vec<String> = files.iter().map(|file| file.location.to_string()).collect();
        assert_eq!(locations, expected, "{format:?}");
        assert!(
            first_loci.is_sorted_by(|earlier, later| earlier > later),
            "{format:?}: {first_loci:?}"
        );
    }
}

#[test]
#[should_panic(expected = "fixture sample nobody has no files")]
fn sample_files_panics_on_a_sample_the_fixture_lacks() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);

    drop(block_on(fixture.sample_files("nobody")));
}

#[test]
fn a_mixed_fixture_holds_two_annotated_multi_sample_tables_and_a_single_sample_table() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let fixture = fixture::mixed_dataset_fixture(format, representation);
            let ext = extension(format);

            let mut expected = vec![
                format!("g0.{ext}"),
                format!("g0.samples.{ext}"),
                format!("g1.samples.{ext}"),
            ];
            expected.extend(["a", "b", "c", "d"].map(|stem| format!("g1/{stem}.{ext}")));
            expected.extend(sample_table_files(SAMPLES[3], format));
            expected.sort();
            assert_eq!(listed(fixture), expected, "{format:?} {representation:?}");
            assert_eq!(fixture.representation(), representation);
        }
    }
}

/// The rows of the file at `path` below the mixed fixture's root, as loci, alleles, and samples.
fn mixed_rows(
    format: FixtureFormat,
    representation: LocusRepresentation,
    path: &str,
) -> Vec<fixture::Row> {
    let fixture = Arc::clone(fixture::mixed_dataset_fixture(format, representation));
    let path = format!("{}{path}", fixture.table_path().as_str());
    let registered = Arc::clone(&fixture);
    read_back(path, fixture.input_format(), move |ctx| {
        registered.register(ctx);
    })
    .iter()
    .flat_map(|batch| fixture::decode_rows(batch, representation))
    .collect()
}

#[test]
fn the_mixed_g0_file_holds_the_first_two_samples_rows_in_locus_then_sample_order() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let mut expected: Vec<fixture::Row> = fixture::sample_rows()
                .into_iter()
                .flat_map(|(locus, alleles)| {
                    MIXED_SAMPLE_SETS[0]
                        .iter()
                        .map(move |sample| (locus, alleles.to_string(), (*sample).to_string()))
                })
                .collect();
            // Stable, so each sample's rows at a locus stay in alleles order.
            expected.sort_by(|left, right| (left.0, &left.2).cmp(&(right.0, &right.2)));

            let rows = mixed_rows(format, representation, &format!("g0.{}", extension(format)));

            assert_eq!(rows, expected, "{format:?} {representation:?}");
        }
    }
}

#[test]
fn each_mixed_g1_file_holds_its_documented_rows_carrying_the_third_sample() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for (stem, _) in DOCUMENTED_FILES {
                let expected: Vec<fixture::Row> = documented_rows(stem)
                    .into_iter()
                    .map(|(locus, alleles)| (locus, alleles, SAMPLES[2].to_string()))
                    .collect();

                let rows = mixed_rows(
                    format,
                    representation,
                    &format!("g1/{stem}.{}", extension(format)),
                );

                assert_eq!(rows, expected, "{format:?} {representation:?} {stem}");
            }
        }
    }
}

#[test]
fn the_fixture_without_alleles_is_one_vortex_file_of_one_contig_position_locus() {
    let fixture = Arc::clone(fixture::vortex_without_alleles_fixture());
    let registered = Arc::clone(&fixture);

    let listed = listed(&fixture);
    let batches = read_back(
        format!("{}s=sample-a/a.vortex", fixture.table_path().as_str()),
        fixture.input_format(),
        move |ctx| registered.register(ctx),
    );

    assert_eq!(listed, ["s=sample-a/a.vortex"]);
    assert_eq!(
        fixture.representation(),
        LocusRepresentation::ContigPosition
    );
    assert_eq!(
        column_names(&batches),
        locus_columns(LocusRepresentation::ContigPosition)
    );
    let loci: Vec<Locus> = batches
        .iter()
        .flat_map(|batch| fixture::decode_loci(batch, LocusRepresentation::ContigPosition))
        .collect();
    assert_eq!(loci, [Locus::new(1, 1).unwrap()]);
}

#[test]
fn the_fixture_without_alleles_discovers_as_one_sample_under_the_locus_ordering() {
    let dataset = discovered(
        fixture::vortex_without_alleles_fixture(),
        RowOrdering::locus(),
    );

    assert_eq!(dataset.sample_set(), ["sample-a"]);
}
