use crate::{
    backend::{
        scan_description::{DescribedScan, ScanDescription},
        scan_reopening,
    },
    format::InputFormat,
    locus::{Locus, LocusRepresentation},
    pipeline::{self, PipelineOptions},
    row_ordering::RowOrdering,
    stored::dataset::Dataset,
    testing::fixture::{self, FixtureFormat, GrowingDatasetFixture},
};

use datafusion::{
    arrow::{
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::TableProvider,
    datasource::{empty::EmptyTable, listing::ListingTableUrl},
    error::Result,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::{DataFrame, SessionContext},
};
use std::{slice, sync::Arc};

/// Registers a test's dataset store on a session.
type Register = Arc<dyn Fn(&SessionContext) + Send + Sync>;

#[test]
fn the_descriptions_a_plan_reads_come_each_once_in_the_order_it_first_reads_them() {
    let [g0, g1] = ["g0", "g1"]
        .map(|name| description("memory://fixture/samples/", InputFormat::VORTEX, name));
    let plan = LogicalPlanBuilder::from(placeholder(&g1, locus_schema()))
        .union(placeholder(&g0, locus_schema()))
        .unwrap()
        .union(placeholder(&g1, locus_schema()))
        .unwrap()
        .build()
        .unwrap();

    assert_eq!(scan_reopening::descriptions(&plan).unwrap(), [g1, g0]);
}

#[test]
fn a_plan_reading_a_table_that_is_not_a_scan_description_has_no_descriptions() {
    let provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(locus_schema()));
    let plan = SessionContext::new()
        .read_table(provider)
        .unwrap()
        .into_unoptimized_plan();

    let error = scan_reopening::descriptions(&plan).unwrap_err();

    assert!(
        error.to_string().contains("not a scan description"),
        "{error}"
    );
}

/// A placeholder refuses to scan, so a reopened plan that runs has had every scan reopened, each
/// reading its own input table's rows.
#[test]
fn reopening_turns_every_described_scan_into_a_scan_of_its_input_table() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        let fixture = fixture::dataset_fixture(format, LocusRepresentation::Packed);
        let register: Register = Arc::new(|ctx| fixture.register(ctx));
        let [first, second] = ["s=HG02230", "s=HG00592"]
            .map(|name| description(fixture.table_path().as_str(), fixture.input_format(), name))
            .map(|description| described(&register, description));
        let plan = LogicalPlanBuilder::from(first)
            .union(second)
            .unwrap()
            .build()
            .unwrap();

        let batches = reopened(&register, plan).unwrap();

        let mut samples = batches
            .iter()
            .flat_map(|batch| fixture::string_column(batch, "s"))
            .collect::<Vec<_>>();
        let rows = samples.len();
        samples.sort();
        samples.dedup();
        assert_eq!(samples, ["HG00592", "HG02230"], "{format:?}");
        assert_eq!(rows, 2 * fixture::sample_rows().len(), "{format:?}");
    }
}

/// Reopening lists the input table's files then, so it reads a file added after the scan was
/// described.
#[test]
fn reopening_reads_a_file_added_after_the_scan_was_described() {
    let fixture = fixture::growing_dataset_fixture(
        "reopened-relisted",
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    );
    let store = fixture.store().clone();
    let register: Register = Arc::new(move |ctx| store.register(ctx));
    let plan = described(
        &register,
        description(
            fixture.table_path().as_str(),
            fixture.input_format(),
            &format!("s={}", GrowingDatasetFixture::SAMPLE),
        ),
    );

    fixture.add_next_file();
    let batches = reopened(&register, plan).unwrap();

    assert_eq!(
        batches
            .iter()
            .flat_map(|batch| fixture::decode_loci(batch, LocusRepresentation::ContigPosition))
            .collect::<Vec<_>>(),
        [(1, 1), (1, 2), (1, 2), (1, 3)]
            .map(|(contig, position)| Locus::new(contig, position).unwrap())
    );
}

/// A scan's description was handed out with its schema; reopening it under another fails rather
/// than plan against columns the scan no longer reads.
#[test]
fn reopening_a_scan_whose_input_table_now_reads_another_schema_fails() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let register: Register = Arc::new(|ctx| fixture.register(ctx));
    let plan = placeholder(
        &description(
            fixture.table_path().as_str(),
            fixture.input_format(),
            "s=HG00308",
        ),
        locus_schema(),
    );

    let error = reopened(&register, plan).unwrap_err();

    assert!(error.to_string().contains("schema"), "{error}");
    assert!(error.to_string().contains("'s=HG00308'"), "{error}");
}

#[test]
fn reopening_a_scan_of_an_input_table_the_dataset_no_longer_has_fails() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let register: Register = Arc::new(|ctx| fixture.register(ctx));
    let plan = placeholder(
        &description(
            fixture.table_path().as_str(),
            fixture.input_format(),
            "s=NA00000",
        ),
        locus_schema(),
    );

    let error = reopened(&register, plan).unwrap_err();

    assert!(error.to_string().contains("s=NA00000"), "{error}");
}

/// The scan of a placeholder holding `description` and `schema`, as a decoded plan holds it.
fn placeholder(description: &ScanDescription, schema: SchemaRef) -> LogicalPlan {
    let provider: Arc<dyn TableProvider> =
        Arc::new(DescribedScan::new(description.clone(), schema));
    SessionContext::new()
        .read_table(provider)
        .unwrap()
        .into_unoptimized_plan()
}

/// The placeholder scan of `description`, with the schema its input table reads now.
fn described(register: &Register, description: ScanDescription) -> LogicalPlan {
    let register = Arc::clone(register);
    let schema = pipeline::run(
        move |ctx| async move {
            register(&ctx);
            let dataset = Dataset::discover(
                &ctx,
                ListingTableUrl::parse(&description.dataset_path)?,
                description.input_format.clone(),
                description.row_ordering.clone(),
                None,
            )
            .await?;
            let frame = dataset
                .restrict_to(slice::from_ref(&description.input_table))?
                .read(&ctx)
                .await?;
            Ok((description, Arc::clone(frame.schema().inner())))
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();
    placeholder(&schema.0, schema.1)
}

/// The rows of `plan`, reopened.
fn reopened(register: &Register, plan: LogicalPlan) -> Result<Vec<RecordBatch>> {
    let register = Arc::clone(register);
    pipeline::run(
        move |ctx| async move {
            register(&ctx);
            let plan = scan_reopening::reopen(&ctx, plan).await?;
            DataFrame::new(ctx.state(), plan).collect().await
        },
        PipelineOptions::single_threaded(),
    )
}

/// The description of input table `name` of the dataset at `dataset_path`, in `input_format`,
/// under the locus ordering.
fn description(dataset_path: &str, input_format: InputFormat, name: &str) -> ScanDescription {
    ScanDescription {
        dataset_path: dataset_path.to_string(),
        input_format,
        row_ordering: RowOrdering::locus(),
        input_table: name.to_string(),
    }
}

fn locus_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "locus",
        DataType::Int64,
        false,
    )]))
}
