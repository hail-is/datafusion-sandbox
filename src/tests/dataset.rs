#![expect(
    clippy::as_conversions,
    reason = "the test controls the concrete array types erased behind ArrayRef"
)]

use crate::fixture::{self, block_on};

use crate::{
    dataset::{Dataset, DatasetLayout},
    format::{InputFormat, OutputFormat},
    locus::{LocusOrdering, LocusRepresentation},
    pipeline::{self, PipelineOptions},
};
use datafusion::{
    arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    common::DataFusionError,
    datasource::{listing::ListingTableUrl, source::DataSourceExec},
    error::Result,
    execution::object_store::ObjectStoreUrl,
    physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable, union::UnionExec},
    prelude::{SessionContext, col, lit},
};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

use std::sync::Arc;

#[test]
fn reads_one_sample_in_locus_then_alleles_order_with_its_sample_id_attached() {
    for format in [
        fixture::FixtureFormat::Parquet,
        fixture::FixtureFormat::Vortex,
    ] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let fixture = Arc::clone(fixture::dataset_fixture(format, representation));
            let sample_id = fixture::SAMPLES[0].to_string();

            pipeline::run(
                move |ctx| {
                    fixture.register(&ctx);
                    async move {
                        let dataset = Dataset::discover(
                            &ctx,
                            fixture.table_path().clone(),
                            fixture.input_format(),
                            allele_layout(),
                            None,
                        )
                        .await?
                        .restrict_to(std::slice::from_ref(&sample_id))?;
                        let df = dataset.read(&ctx).await?;
                        let columns = match representation {
                            LocusRepresentation::ContigPosition => {
                                vec!["contig", "position", "alleles", "s"]
                            }
                            LocusRepresentation::Packed => vec!["locus", "alleles", "s"],
                        };
                        // Do not sort here: the dataset read must preserve file and row order.
                        let batches = df.select_columns(&columns)?.collect().await?;
                        let rows = batches
                            .iter()
                            .flat_map(|batch| {
                                (0..batch.num_rows()).map(|row| {
                                    batch
                                        .columns()
                                        .iter()
                                        .map(|column| array_value_to_string(column, row).unwrap())
                                        .collect::<Vec<_>>()
                                })
                            })
                            .collect::<Vec<_>>();
                        let expected = match representation {
                            LocusRepresentation::ContigPosition => vec![
                                vec!["chr1", "1", "A,G"],
                                vec!["chr1", "2", "A,C"],
                                vec!["chr1", "2", "A,G"],
                                vec!["chr1", "3", "A,C"],
                                vec!["chr1", "4", "A,G"],
                                vec!["chr2", "1", "A,C"],
                                vec!["chr2", "2", "A,G"],
                                vec!["chr2", "3", "A,C"],
                            ],
                            LocusRepresentation::Packed => vec![
                                vec!["4294967297", "A,G"],
                                vec!["4294967298", "A,C"],
                                vec!["4294967298", "A,G"],
                                vec!["4294967299", "A,C"],
                                vec!["4294967300", "A,G"],
                                vec!["8589934593", "A,C"],
                                vec!["8589934594", "A,G"],
                                vec!["8589934595", "A,C"],
                            ],
                        }
                        .into_iter()
                        .map(|mut row| {
                            row.push(&sample_id);
                            row
                        })
                        .collect::<Vec<_>>();
                        assert_eq!(rows, expected);
                        Ok(())
                    }
                },
                PipelineOptions {
                    threads: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        }
    }
}

#[test]
fn reading_a_dataset_unions_one_single_partition_input_per_sample() {
    for format in [
        fixture::FixtureFormat::Parquet,
        fixture::FixtureFormat::Vortex,
    ] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let fixture = fixture::dataset_fixture(format, representation);

            block_on(async {
                let ctx = SessionContext::new();
                fixture.register(&ctx);
                let dataset = Dataset::discover(
                    &ctx,
                    fixture.table_path().clone(),
                    fixture.input_format(),
                    allele_layout(),
                    None,
                )
                .await
                .unwrap();
                let plan = dataset
                    .read(&ctx)
                    .await
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap();
                let unions = nodes_of::<UnionExec>(&plan);

                assert_eq!(unions.len(), 1);
                assert_eq!(unions[0].children().len(), fixture::SAMPLES.len());
                assert!(
                    unions[0]
                        .children()
                        .iter()
                        .all(|input| input.output_partitioning().partition_count() == 1)
                );
            });
        }
    }
}

#[test]
fn filtering_the_attached_sample_column_composes_with_the_dataset_sample_set() {
    for format in [
        fixture::FixtureFormat::Parquet,
        fixture::FixtureFormat::Vortex,
    ] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let fixture = Arc::clone(fixture::dataset_fixture(format, representation));

            pipeline::run(
                move |ctx| {
                    fixture.register(&ctx);
                    async move {
                        let dataset = Dataset::discover(
                            &ctx,
                            fixture.table_path().clone(),
                            fixture.input_format(),
                            allele_layout(),
                            None,
                        )
                        .await?
                        .restrict_to(&[
                            fixture::SAMPLES[0].to_string(),
                            fixture::SAMPLES[1].to_string(),
                        ])?;
                        let mut plans = Vec::new();
                        for (sample, expected_rows, expected_scans) in [
                            (fixture::SAMPLES[0], 8, 1),
                            // This sample exists on storage but is outside the dataset's sample set.
                            (fixture::SAMPLES[2], 0, 0),
                        ] {
                            let df = dataset
                                .read(&ctx)
                                .await?
                                .filter(col("s").eq(lit(sample)))?
                                .select_columns(&["s"])?;
                            let plan = df.clone().create_physical_plan().await?;
                            let batches = df.collect().await?;
                            let samples = batches
                                .iter()
                                .flat_map(|batch| {
                                    (0..batch.num_rows()).map(|row| {
                                        array_value_to_string(batch.column(0), row).unwrap()
                                    })
                                })
                                .collect::<Vec<_>>();
                            assert_eq!(samples, vec![sample; expected_rows]);
                            plans.push((sample, expected_scans, plan));
                        }

                        for (sample, expected_scans, plan) in plans {
                            assert_eq!(
                                nodes_of::<DataSourceExec>(&plan).len(),
                                expected_scans,
                                "filtering s = {sample} must remove irrelevant per-sample file scans:\n{}",
                                displayable(plan.as_ref()).indent(true),
                            );
                        }
                        Ok(())
                    }
                },
                PipelineOptions {
                    threads: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        }
    }
}

#[test]
fn rejects_an_inferred_schema_missing_a_required_ordering_column() {
    let fixture = fixture::vortex_without_alleles_fixture();

    let error = block_on(async {
        let ctx = SessionContext::new();
        fixture.register(&ctx);
        Dataset::discover(
            &ctx,
            fixture.table_path().clone(),
            fixture.input_format(),
            allele_layout(),
            None,
        )
        .await
        .expect_err("the inferred schema must contain every required ordering column")
    });

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(
        error.to_string().contains("alleles"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_a_resolved_schema_missing_a_required_ordering_column() {
    let error = Dataset::new(
        ListingTableUrl::parse("memory:///samples").unwrap(),
        InputFormat::VORTEX,
        allele_layout(),
        contig_position_schema(false),
        vec!["sample-a".to_string()],
    )
    .expect_err("the resolved schema must contain every required ordering column");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(
        error.to_string().contains("alleles"),
        "unexpected error: {error}"
    );
}

#[test]
fn infers_the_schema_from_one_input_file() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let table_path = ListingTableUrl::parse("memory://schema-inference/samples/").unwrap();
    let store_url = table_path.object_store();
    let root = table_path.as_str().trim_end_matches('/').to_string();

    pipeline::run(
        move |ctx| {
            ctx.register_object_store(store_url.as_ref(), store);
            async move {
                let int_batch = RecordBatch::try_from_iter(vec![
                    ("contig", Arc::new(StringArray::from(vec!["chr1"])) as _),
                    ("position", Arc::new(Int32Array::from(vec![1])) as _),
                ])?;
                let string_batch = RecordBatch::try_from_iter(vec![
                    ("contig", Arc::new(StringArray::from(vec!["chr1"])) as _),
                    ("position", Arc::new(StringArray::from(vec!["one"])) as _),
                ])?;
                for (name, batch) in [("a.vortex", int_batch), ("b.vortex", string_batch)] {
                    let path = format!("{root}/s=sample-a/{name}");
                    let df = ctx.read_batch(batch)?;
                    OutputFormat::VORTEX.write(df, &path).await?;
                }

                let dataset =
                    Dataset::discover(&ctx, table_path, InputFormat::VORTEX, locus_layout(), None)
                        .await
                        .expect("incompatible schemas in later files must not be merged");

                dataset
                    .schema()
                    .field_with_name("position")
                    .expect("the discovered schema must contain the position field");
                Ok(())
            }
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap();
}

#[test]
fn uses_a_pinned_schema_without_inference() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = contig_position_schema(false);

    let dataset = block_on(async {
        store
            .put(
                &Path::from("samples/s=sample-a/invalid.vortex"),
                b"not a vortex file".to_vec().into(),
            )
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let store_url = ObjectStoreUrl::parse("memory://").unwrap();
        ctx.register_object_store(store_url.as_ref(), store);
        Dataset::discover(
            &ctx,
            ListingTableUrl::parse("memory:///samples").unwrap(),
            InputFormat::VORTEX,
            locus_layout(),
            Some(Arc::clone(&schema)),
        )
        .await
    })
    .expect("a pinned schema must bypass inference");

    assert_eq!(dataset.schema(), &schema);
}

#[test]
fn discovers_the_dataset_sample_set_in_memory() {
    let dataset = block_on(discover_in_memory(&["sample-b", "sample-a"])).unwrap();

    assert_eq!(dataset.sample_set(), ["sample-a", "sample-b"]);
}

#[test]
fn discovery_surfaces_a_locus_representation_error() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "locus",
        DataType::Utf8,
        false,
    )]));
    let error = block_on(discover_in_memory_with_schema(&["sample-a"], schema))
        .expect_err("discovery must reject a non-Int64 packed locus");

    assert!(matches!(error, DataFusionError::Plan(_)));
    let message = error.to_string();
    assert!(message.contains("locus"), "unexpected error: {message}");
    assert!(message.contains("Int64"), "unexpected error: {message}");
    assert!(message.contains("Utf8"), "unexpected error: {message}");
}

#[test]
fn rejects_a_dataset_with_no_samples_in_memory() {
    let error = block_on(discover_in_memory(&[]))
        .expect_err("an object store with no sample paths is not a dataset");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(
        error.to_string().contains("no samples"),
        "unexpected error: {error}"
    );
}

#[test]
fn narrows_the_dataset_sample_set() {
    let dataset = dataset_from_data(&["sample-a", "sample-b"])
        .restrict_to(&["sample-b".to_string()])
        .unwrap();

    assert_eq!(dataset.sample_set(), ["sample-b"]);
}

#[test]
fn rejects_an_empty_requested_sample_set() {
    let error = dataset_from_data(&["sample-a"])
        .restrict_to(&[])
        .expect_err("a dataset must retain at least one sample");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert_eq!(
        error.to_string(),
        "Error during planning: requested sample set contains no samples"
    );
}

#[test]
fn rejects_requested_samples_that_are_not_in_the_dataset() {
    let error = dataset_from_data(&["sample-a"])
        .restrict_to(&["missing-b".to_string(), "missing-a".to_string()])
        .expect_err("unknown sample ids must fail");

    assert!(matches!(error, DataFusionError::Plan(_)));
    let message = error.to_string();
    assert!(message.contains("missing-a"), "unexpected error: {message}");
    assert!(message.contains("missing-b"), "unexpected error: {message}");
}

fn dataset_from_data(sample_set: &[&str]) -> Dataset {
    Dataset::new(
        ListingTableUrl::parse("memory:///samples").unwrap(),
        InputFormat::VORTEX,
        allele_layout(),
        contig_position_schema(true),
        sample_set.iter().map(ToString::to_string).collect(),
    )
    .unwrap()
}

async fn discover_in_memory(sample_set: &[&str]) -> Result<Dataset> {
    discover_in_memory_with_schema(sample_set, contig_position_schema(false)).await
}

async fn discover_in_memory_with_schema(sample_set: &[&str], schema: SchemaRef) -> Result<Dataset> {
    let ctx = SessionContext::new();
    let store = Arc::new(InMemory::new());
    for sample in sample_set {
        store
            .put(
                &Path::from(format!("samples/s={sample}/marker")),
                Vec::<u8>::new().into(),
            )
            .await?;
    }
    let store_url = ObjectStoreUrl::parse("memory://")?;
    ctx.register_object_store(store_url.as_ref(), store);
    Dataset::discover(
        &ctx,
        ListingTableUrl::parse("memory:///samples")?,
        InputFormat::VORTEX,
        locus_layout(),
        Some(schema),
    )
    .await
}

fn contig_position_schema(include_alleles: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
    ];
    if include_alleles {
        fields.push(Field::new("alleles", DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

fn locus_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: LocusOrdering::locus(),
    }
}

fn allele_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: LocusOrdering::locus_then_alleles(),
    }
}

fn nodes_of<T: ExecutionPlan>(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut found = Vec::new();
    if plan.downcast_ref::<T>().is_some() {
        found.push(Arc::clone(plan));
    }
    for child in plan.children() {
        found.extend(nodes_of::<T>(child));
    }
    found
}
