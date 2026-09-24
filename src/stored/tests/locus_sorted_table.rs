use crate::{
    fixture::{self, FixtureFormat},
    format::InputFormat,
    locus::{Locus, LocusOrdering, LocusRepresentation},
    pipeline::{self, PipelineOptions},
    stored::locus_sorted_table::LocusSortedTable,
};

use datafusion::{
    common::DataFusionError,
    datasource::listing::ListingTableUrl,
    error::Result,
    prelude::{DataFrame, SessionContext},
};
use object_store::{ObjectStoreExt, path::Path};

#[test]
fn reads_a_file_and_a_directory_in_locus_order() {
    for (format, format_name) in [
        (FixtureFormat::Parquet, "parquet"),
        (FixtureFormat::Vortex, "vortex"),
    ] {
        for (representation, representation_name) in [
            (LocusRepresentation::ContigPosition, "contig-position"),
            (LocusRepresentation::Packed, "packed"),
        ] {
            let first = vec![Locus::new(1, 1).unwrap(), Locus::new(1, 2).unwrap()];
            let second = vec![Locus::new(1, 3).unwrap(), Locus::new(2, 1).unwrap()];
            // The fixture names its files in reverse locus order.
            let fixture = fixture::sorted_table_fixture(
                &format!("locus-sorted-table-{format_name}-{representation_name}"),
                format,
                representation,
                vec![first.clone(), second.clone()],
            );

            pipeline::run(
                move |ctx| {
                    fixture.register(&ctx);
                    async move {
                        let directory = fixture.table_path().as_str();
                        let unslashed_directory =
                            ListingTableUrl::parse(directory.trim_end_matches('/'))?;
                        for (path, expected) in [
                            (fixture.file_path(0).clone(), first.clone()),
                            (fixture.table_path().clone(), [&first[..], &second].concat()),
                            (unslashed_directory, [&first[..], &second].concat()),
                        ] {
                            let table = LocusSortedTable::open(
                                &ctx,
                                path.clone(),
                                fixture.input_format(),
                                LocusOrdering::locus(),
                            )
                            .await?;
                            let actual = loci(table.read(&ctx)?, representation).await?;
                            assert_eq!(
                                actual,
                                expected,
                                "{format_name} {representation_name} {}",
                                path.as_str()
                            );
                        }
                        Ok(())
                    }
                },
                PipelineOptions::single_threaded(),
            )
            .unwrap();
        }
    }
}

#[test]
fn exposes_the_representation_and_stored_ordering_of_its_files() {
    for (representation, columns) in [
        (
            LocusRepresentation::ContigPosition,
            vec!["contig", "position"],
        ),
        (LocusRepresentation::Packed, vec!["locus"]),
    ] {
        let fixture = fixture::sorted_table_fixture(
            &format!("locus-sorted-table-exposes-{representation}"),
            FixtureFormat::Vortex,
            representation,
            vec![vec![Locus::new(1, 1).unwrap()]],
        );

        let table = open(&fixture, fixture.table_path(), fixture.input_format()).unwrap();

        assert_eq!(table.locus_representation(), representation);
        assert_eq!(table.stored_ordering().column_names(), columns);
    }
}

#[test]
fn ignores_files_with_another_formats_extension() {
    let expected = vec![Locus::new(1, 1).unwrap(), Locus::new(1, 2).unwrap()];
    let fixture = fixture::sorted_table_fixture(
        "locus-sorted-table-extension-filter",
        FixtureFormat::Vortex,
        LocusRepresentation::Packed,
        vec![expected.clone()],
    );

    pipeline::run(
        move |ctx| {
            fixture.register(&ctx);
            async move {
                let ignored =
                    Path::from(format!("{}/ignored.parquet", fixture.table_path().prefix()));
                fixture
                    .store()
                    .put(&ignored, b"not parquet".to_vec().into())
                    .await?;
                let table = LocusSortedTable::open(
                    &ctx,
                    fixture.table_path().clone(),
                    fixture.input_format(),
                    LocusOrdering::locus(),
                )
                .await?;
                let actual = loci(table.read(&ctx)?, fixture.representation()).await?;
                assert_eq!(actual, expected);
                Ok(())
            }
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();
}

#[test]
fn rejects_a_missing_ordering_column_at_construction() {
    let fixture = fixture::sorted_table_fixture(
        "locus-sorted-table-missing-alleles",
        FixtureFormat::Parquet,
        LocusRepresentation::Packed,
        vec![vec![Locus::new(1, 1).unwrap()]],
    );

    let error = pipeline::run(
        move |ctx| {
            fixture.register(&ctx);
            async move {
                LocusSortedTable::open(
                    &ctx,
                    fixture.table_path().clone(),
                    fixture.input_format(),
                    LocusOrdering::locus_then_alleles(),
                )
                .await
            }
        },
        PipelineOptions::single_threaded(),
    )
    .expect_err("the fixture's files have no alleles column");

    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert!(error.to_string().contains("'alleles'"), "{error}");
}

#[test]
fn rejects_a_path_with_no_nonempty_file_of_the_format() {
    let fixture = fixture::sorted_table_fixture(
        "locus-sorted-table-no-parquet",
        FixtureFormat::Vortex,
        LocusRepresentation::Packed,
        vec![vec![Locus::new(1, 1).unwrap()]],
    );

    let error = pipeline::run(
        move |ctx| {
            fixture.register(&ctx);
            async move {
                let empty = Path::from(format!("{}/empty.parquet", fixture.table_path().prefix()));
                fixture.store().put(&empty, Vec::new().into()).await?;
                LocusSortedTable::open(
                    &ctx,
                    fixture.table_path().clone(),
                    InputFormat::PARQUET,
                    LocusOrdering::locus(),
                )
                .await
            }
        },
        PipelineOptions::single_threaded(),
    )
    .expect_err("the directory holds Vortex files and one empty Parquet file");

    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert!(
        error.to_string().contains("no input files found"),
        "{error}"
    );
}

fn open(
    fixture: &fixture::SortedTableFixture,
    path: &ListingTableUrl,
    input_format: InputFormat,
) -> Result<LocusSortedTable> {
    let ctx = SessionContext::new();
    fixture.register(&ctx);
    fixture::block_on(LocusSortedTable::open(
        &ctx,
        path.clone(),
        input_format,
        LocusOrdering::locus(),
    ))
}

async fn loci(frame: DataFrame, representation: LocusRepresentation) -> Result<Vec<Locus>> {
    Ok(frame
        .collect()
        .await?
        .iter()
        .map(|batch| representation.loci(batch))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect())
}
