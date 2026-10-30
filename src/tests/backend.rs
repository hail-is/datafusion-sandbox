//! The backend's suite runs on the shared dataset fixtures' in-memory stores, which it hands the
//! backend through its object-store resolver.
//!
//! The tests here open datasets and read back plan bytes' scan descriptions; `runs` starts plan
//! bytes and waits for their Outcomes.

mod runs;

use crate::{
    backend::{
        Backend, ErrorCode, ObjectStoreResolver, OpenedDataset, ResolveFromUrls, Versions,
        codec::{CODEC_ID, Codec},
        scan_description::ScanDescription,
    },
    locus::LocusRepresentation,
    pipeline::PipelineOptions,
    row_ordering::{RowOrdering, SortColumn},
    testing::{
        fixture::{
            self, DatasetFixture, FixtureFormat, INPUT_TABLES, MIXED_INPUT_TABLES,
            MIXED_SAMPLE_SETS, SAMPLES,
        },
        wheel_codec::WheelCodec,
    },
};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    catalog::TableProvider,
    error::Result,
    execution::object_store::ObjectStoreUrl,
    logical_expr::LogicalPlanBuilder,
    prelude::{SessionContext, col},
};
use datafusion_proto::bytes::logical_plan_to_bytes_with_extension_codec;
use object_store::ObjectStore;
use std::{num::NonZeroUsize, sync::Arc};

const FORMATS: [FixtureFormat; 2] = [FixtureFormat::Parquet, FixtureFormat::Vortex];
const REPRESENTATIONS: [LocusRepresentation; 2] = [
    LocusRepresentation::ContigPosition,
    LocusRepresentation::Packed,
];

#[test]
fn opening_a_dataset_hands_out_a_scan_description_per_input_table_in_name_order() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let fixture = fixture::dataset_fixture(format, representation);

            let opened = open(fixture, RowOrdering::locus_then_alleles());

            assert_eq!(
                names(&opened),
                INPUT_TABLES,
                "{format:?} {representation:?}"
            );
            for table in &opened.input_tables {
                assert_eq!(
                    table.scan.description(),
                    &ScanDescription {
                        dataset_path: fixture.table_path().to_string(),
                        input_format: fixture.input_format(),
                        row_ordering: RowOrdering::locus_then_alleles(),
                        input_table: table.scan.description().input_table.clone(),
                    }
                );
            }
        }
    }
}

#[test]
fn each_input_table_comes_with_its_sample_set() {
    for format in FORMATS {
        let single = open(
            fixture::dataset_fixture(format, LocusRepresentation::ContigPosition),
            RowOrdering::locus(),
        );
        let mixed = open(
            fixture::mixed_dataset_fixture(format, LocusRepresentation::Packed),
            RowOrdering::locus(),
        );

        assert_eq!(
            sample_sets(&single),
            SAMPLES
                .iter()
                .map(|sample| vec![*sample])
                .collect::<Vec<_>>()
        );
        assert_eq!(
            sample_sets(&mixed),
            MIXED_SAMPLE_SETS
                .iter()
                .map(|set| set.to_vec())
                .collect::<Vec<_>>()
        );
        assert_eq!(names(&mixed), MIXED_INPUT_TABLES);
    }
}

/// Every scan reads the dataset's rows with their sample, single-sample and multi-sample input
/// tables alike: the fixture's columns, non-null, with strings read as string views.
#[test]
fn every_scan_has_the_dataset_s_schema_with_the_sample() {
    let string = || DataType::Utf8View;
    for (representation, columns) in [
        (
            LocusRepresentation::ContigPosition,
            vec![
                ("contig", string()),
                ("position", DataType::Int32),
                ("alleles", string()),
                ("s", string()),
            ],
        ),
        (
            LocusRepresentation::Packed,
            vec![
                ("locus", DataType::Int64),
                ("alleles", string()),
                ("s", string()),
            ],
        ),
    ] {
        let expected = Schema::new(
            columns
                .into_iter()
                .map(|(name, data_type)| Field::new(name, data_type, false))
                .collect::<Vec<_>>(),
        );
        for format in FORMATS {
            let opened = open(
                fixture::mixed_dataset_fixture(format, representation),
                RowOrdering::locus(),
            );

            assert_eq!(
                opened.schema.as_ref(),
                &expected,
                "{format:?} {representation:?}"
            );
            for table in &opened.input_tables {
                assert_eq!(table.scan.schema_ref(), &opened.schema);
            }
        }
    }
}

#[test]
fn the_stored_ordering_comes_as_plain_sort_columns_ascending_with_nulls_last() {
    for (representation, row_ordering, columns) in [
        (
            LocusRepresentation::ContigPosition,
            RowOrdering::locus_then_alleles(),
            &["contig", "position", "alleles"][..],
        ),
        (
            LocusRepresentation::Packed,
            RowOrdering::locus_then_alleles(),
            &["locus", "alleles"],
        ),
        (
            LocusRepresentation::ContigPosition,
            RowOrdering::locus(),
            &["contig", "position"],
        ),
    ] {
        let opened = open(
            fixture::dataset_fixture(FixtureFormat::Vortex, representation),
            row_ordering,
        );

        let expected = columns
            .iter()
            .map(|column| SortColumn {
                column: (*column).to_string(),
                ascending: true,
                nulls_first: false,
            })
            .collect::<Vec<_>>();
        assert_eq!(opened.ordering, expected);
    }
}

#[test]
fn opening_a_dataset_whose_rows_lack_a_column_of_the_row_ordering_fails() {
    let fixture = fixture::vortex_without_alleles_fixture();

    let error = backend_over(fixture)
        .open_dataset(
            fixture.table_path().as_str(),
            fixture.input_format(),
            RowOrdering::locus_then_alleles(),
            Versions::backend(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("'alleles'"), "{error}");
}

/// The backend reaches a dataset only through the stores its resolver hands it.
#[test]
fn opening_a_dataset_on_a_store_the_resolver_does_not_serve_fails() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let backend = Backend::new(Arc::new(NoStores));

    let error = backend
        .open_dataset(
            fixture.table_path().as_str(),
            fixture.input_format(),
            RowOrdering::locus(),
            Versions::backend(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("memory://"), "{error}");
}

/// What the frontend's plan bytes say each scan reads comes back as the descriptions the dataset
/// handed out, each once, in the order the plan first reads them.
#[test]
fn the_scan_descriptions_of_plan_bytes_are_those_their_scans_were_opened_with() {
    let fixture =
        fixture::mixed_dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let backend = backend_over(fixture);
    let opened = open(fixture, RowOrdering::locus());
    let [g0, g1, single] = opened.input_tables.as_slice() else {
        panic!("the mixed fixture has three input tables");
    };
    let ctx = SessionContext::new();
    let scan = |table: &crate::backend::InputTableScan| {
        let provider: Arc<dyn TableProvider> = table.scan.clone();
        ctx.read_table(provider).unwrap().into_unoptimized_plan()
    };
    let plan = LogicalPlanBuilder::from(scan(single))
        .union(scan(g0))
        .unwrap()
        .union(scan(single))
        .unwrap()
        .sort(opened.ordering.iter().map(SortColumn::sort_expression))
        .unwrap()
        .filter(col("alleles").is_not_null())
        .unwrap()
        .build()
        .unwrap();
    let bytes = wheel_bytes(&plan);

    let descriptions = backend
        .scan_descriptions(&bytes, Versions::backend())
        .unwrap();

    assert_eq!(
        descriptions,
        [single, g0]
            .map(|table| table.scan.description().clone())
            .to_vec()
    );
    assert!(!descriptions.contains(g1.scan.description()));
}

#[test]
fn bytes_that_are_no_plan_have_no_scan_descriptions() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);

    let error = backend_over(fixture)
        .scan_descriptions(b"not a plan", Versions::backend())
        .unwrap_err();

    assert!(error.to_string().contains("plan"), "{error}");
}

/// Opening a dataset, and reading back plan bytes, check the frontend's versions as a run does.
#[test]
fn every_call_refuses_versions_other_than_the_backend_s_with_the_invalid_action_code() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let backend = backend_over(fixture);
    let other = Versions {
        datafusion_major: Versions::backend().datafusion_major.saturating_add(1),
        ..Versions::backend()
    };

    let opening = backend
        .open_dataset(
            fixture.table_path().as_str(),
            fixture.input_format(),
            RowOrdering::locus(),
            other,
        )
        .unwrap_err();
    let reading_back = backend.scan_descriptions(b"", other).unwrap_err();

    for error in [opening, reading_back] {
        assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
        assert!(error.message().contains("DataFusion"), "{error}");
    }
}

/// The production resolver serves what the pipeline runner can: local paths and `gs://` URLs.
#[test]
fn resolving_from_urls_refuses_a_scheme_the_pipeline_cannot_serve() {
    let error = ResolveFromUrls
        .pipeline_options(NonZeroUsize::MIN, &["s3://bucket/samples"])
        .unwrap_err();

    assert!(error.to_string().contains("s3://bucket/samples"), "{error}");
}

/// The adapter at the backend's object-store seam: in-memory stores by URL, whatever the paths.
#[derive(Debug)]
pub(super) struct MemoryStores(Vec<(ObjectStoreUrl, Arc<dyn ObjectStore>)>);

impl ObjectStoreResolver for MemoryStores {
    fn pipeline_options(&self, threads: NonZeroUsize, _paths: &[&str]) -> Result<PipelineOptions> {
        Ok(self
            .0
            .iter()
            .fold(PipelineOptions::new(threads), |options, (url, store)| {
                options.with_object_store(url, Arc::clone(store))
            }))
    }
}

/// A resolver that serves no store.
#[derive(Debug)]
struct NoStores;

impl ObjectStoreResolver for NoStores {
    fn pipeline_options(&self, threads: NonZeroUsize, _paths: &[&str]) -> Result<PipelineOptions> {
        Ok(PipelineOptions::new(threads))
    }
}

/// A backend over `fixture`'s store whose runs check their rows' order at run time, as ADR 0022
/// asks of the backend's tests.
fn backend_over(fixture: &'static Arc<DatasetFixture>) -> Backend {
    Backend::new(Arc::new(MemoryStores(vec![(
        fixture.table_path().object_store(),
        Arc::clone(fixture.store()),
    )])))
    .verifying_order()
}

fn open(fixture: &'static Arc<DatasetFixture>, row_ordering: RowOrdering) -> OpenedDataset {
    backend_over(fixture)
        .open_dataset(
            fixture.table_path().as_str(),
            fixture.input_format(),
            row_ordering,
            Versions::backend(),
        )
        .unwrap()
}

fn names(opened: &OpenedDataset) -> Vec<&str> {
    opened
        .input_tables
        .iter()
        .map(|table| table.scan.description().input_table.as_str())
        .collect()
}

fn sample_sets(opened: &OpenedDataset) -> Vec<Vec<&str>> {
    opened
        .input_tables
        .iter()
        .map(|table| table.sample_set.iter().map(String::as_str).collect())
        .collect()
}

/// `plan`'s bytes as the wheel writes them, with the backend's codec installed on its session.
fn wheel_bytes(plan: &datafusion::logical_expr::LogicalPlan) -> Vec<u8> {
    logical_plan_to_bytes_with_extension_codec(plan, &WheelCodec::new(Arc::new(Codec), CODEC_ID))
        .unwrap()
        .to_vec()
}
