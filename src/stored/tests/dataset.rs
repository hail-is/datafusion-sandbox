#![expect(
    clippy::as_conversions,
    reason = "the test controls the concrete array types erased behind ArrayRef"
)]

use crate::fixture::{self, block_on};
use crate::tests::plan_shape::PlanShape;

use crate::{
    format::{InputFormat, OutputFormat},
    locus::{Locus, LocusRepresentation, RowOrdering},
    pipeline::{self, PipelineOptions},
    stored::dataset::{Dataset, InputTable, InputTableKind},
    write::WriteTarget,
};
use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    common::DataFusionError,
    datasource::{listing::ListingTableUrl, source::DataSourceExec},
    error::Result,
    execution::object_store::ObjectStoreUrl,
    prelude::{SessionContext, col, lit},
};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

use std::sync::Arc;

#[test]
fn reads_one_sample_in_locus_then_alleles_order_with_its_sample_id_attached_as_a_view_string() {
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
            let input_table = fixture::INPUT_TABLES[0].to_string();

            pipeline::run(
                move |ctx| {
                    fixture.register(&ctx);
                    async move {
                        let dataset = Dataset::discover(
                            &ctx,
                            fixture.table_path().clone(),
                            fixture.input_format(),
                            RowOrdering::locus_then_alleles(),
                            None,
                        )
                        .await?
                        .restrict_to(std::slice::from_ref(&input_table))?;
                        let df = dataset.read(&ctx).await?;
                        let sample = df.schema().field_with_unqualified_name("s")?;
                        assert_eq!(sample.data_type(), &DataType::Utf8View);
                        assert!(!sample.is_nullable());
                        let mut columns = RowOrdering::locus_then_alleles()
                            .expand(representation)
                            .column_names();
                        columns.push("s".to_string());
                        let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
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
                        let expected = [
                            (Locus::new(1, 1).unwrap(), "A,G"),
                            (Locus::new(1, 2).unwrap(), "A,C"),
                            (Locus::new(1, 2).unwrap(), "A,G"),
                            (Locus::new(1, 3).unwrap(), "A,C"),
                            (Locus::new(1, 4).unwrap(), "A,G"),
                            (Locus::new(2, 1).unwrap(), "A,C"),
                            (Locus::new(2, 2).unwrap(), "A,G"),
                            (Locus::new(2, 3).unwrap(), "A,C"),
                        ]
                        .into_iter()
                        .map(|(locus, alleles)| {
                            let mut row = fixture::locus_cells(locus, representation);
                            row.push(alleles.to_string());
                            row.push(sample_id.clone());
                            row
                        })
                        .collect::<Vec<_>>();
                        assert_eq!(rows, expected);
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
                    RowOrdering::locus_then_alleles(),
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

                PlanShape::of(&plan)
                    .assert_one_ordered_partition_per_sample(fixture::SAMPLES.len());
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
                            RowOrdering::locus_then_alleles(),
                            None,
                        )
                        .await?
                        .restrict_to(&[
                            fixture::INPUT_TABLES[0].to_string(),
                            fixture::INPUT_TABLES[1].to_string(),
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
                            let shape = PlanShape::of(&plan);
                            assert_eq!(
                                shape.nodes_of::<DataSourceExec>().len(),
                                expected_scans,
                                "filtering s = {sample} must remove irrelevant per-sample file scans:\n{shape}",
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
fn rejects_an_inferred_schema_missing_a_required_ordering_column() {
    let fixture = fixture::vortex_without_alleles_fixture();

    let error = block_on(async {
        let ctx = SessionContext::new();
        fixture.register(&ctx);
        Dataset::discover(
            &ctx,
            fixture.table_path().clone(),
            fixture.input_format(),
            RowOrdering::locus_then_alleles(),
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
        RowOrdering::locus_then_alleles(),
        contig_position_schema(false),
        vec![InputTable::single_sample("sample-a")],
    )
    .expect_err("the resolved schema must contain every required ordering column");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(
        error.to_string().contains("alleles"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_two_input_tables_with_one_name() {
    let error = Dataset::new(
        ListingTableUrl::parse("memory:///samples").unwrap(),
        InputFormat::VORTEX,
        RowOrdering::locus_then_alleles(),
        contig_position_schema(true),
        ["sample-b", "sample-a", "sample-b"]
            .map(InputTable::single_sample)
            .to_vec(),
    )
    .expect_err("a name identifies one entry in the dataset root");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert_eq!(
        error.to_string(),
        "Error during planning: dataset 'memory:///samples/' holds more than one input table \
         named 's=sample-b'"
    );
}

#[test]
fn inferred_schema_locus_fields_match_the_representation_fields() {
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
                    RowOrdering::locus(),
                    None,
                )
                .await
                .unwrap();

                for expected in representation.fields() {
                    let actual = dataset.schema().field_with_name(expected.name()).unwrap();
                    assert_eq!(
                        actual.data_type(),
                        expected.data_type(),
                        "{format:?} {representation:?} field {}",
                        expected.name()
                    );
                }
            });
        }
    }
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
                    WriteTarget {
                        output_path: path,
                        output_format: OutputFormat::VORTEX,
                    }
                    .write_unordered(df)
                    .await?;
                }

                let dataset = Dataset::discover(
                    &ctx,
                    table_path,
                    InputFormat::VORTEX,
                    RowOrdering::locus(),
                    None,
                )
                .await
                .expect("incompatible schemas in later files must not be merged");

                dataset
                    .schema()
                    .field_with_name("position")
                    .expect("the discovered schema must contain the position field");
                Ok(())
            }
        },
        PipelineOptions::single_threaded(),
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
            RowOrdering::locus(),
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
fn discovers_one_single_sample_input_table_per_sample_directory_in_name_order() {
    let dataset = block_on(discover_in_memory(&["sample-b", "sample-a"])).unwrap();

    let input_tables = dataset
        .input_tables()
        .iter()
        .map(|table| (table.name(), table.sample_set()))
        .collect::<Vec<_>>();
    assert_eq!(
        input_tables,
        [
            ("s=sample-a", &["sample-a".to_string()][..]),
            ("s=sample-b", &["sample-b".to_string()][..]),
        ]
    );
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
fn narrows_the_dataset_to_the_named_input_tables() {
    let dataset = dataset_from_data(&["sample-a", "sample-b", "sample-c"])
        .restrict_to(&["s=sample-c".to_string(), "s=sample-a".to_string()])
        .unwrap();

    let names = dataset
        .input_tables()
        .iter()
        .map(InputTable::name)
        .collect::<Vec<_>>();
    assert_eq!(names, ["s=sample-a", "s=sample-c"]);
    assert_eq!(dataset.sample_set(), ["sample-a", "sample-c"]);
}

#[test]
fn rejects_an_empty_input_table_request() {
    let error = dataset_from_data(&["sample-a"])
        .restrict_to(&[])
        .expect_err("a dataset must retain at least one input table");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert_eq!(
        error.to_string(),
        "Error during planning: no input tables requested"
    );
}

#[test]
fn rejects_requested_input_tables_that_are_not_in_the_dataset() {
    let error = dataset_from_data(&["sample-a"])
        .restrict_to(&["s=missing-b".to_string(), "s=missing-a".to_string()])
        .expect_err("unknown input table names must fail");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert_eq!(
        error.to_string(),
        "Error during planning: input tables not found in dataset: s=missing-a, s=missing-b"
    );
}

#[test]
fn rejects_a_sample_id_in_place_of_its_input_table_name() {
    let error = dataset_from_data(&["sample-a"])
        .restrict_to(&["sample-a".to_string()])
        .expect_err("a sample id does not name an input table");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(
        error.to_string().contains("sample-a"),
        "unexpected error: {error}"
    );
}

fn dataset_from_data(sample_set: &[&str]) -> Dataset {
    Dataset::new(
        ListingTableUrl::parse("memory:///samples").unwrap(),
        InputFormat::VORTEX,
        RowOrdering::locus_then_alleles(),
        contig_position_schema(true),
        sample_set
            .iter()
            .copied()
            .map(InputTable::single_sample)
            .collect(),
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
        RowOrdering::locus(),
        Some(schema),
    )
    .await
}

fn contig_position_schema(include_alleles: bool) -> SchemaRef {
    let mut fields = LocusRepresentation::ContigPosition.fields();
    if include_alleles {
        fields.push(Field::new("alleles", DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

#[test]
fn discovers_input_tables_of_each_kind_named_by_their_root_entries_with_their_sample_sets() {
    for format in [
        fixture::FixtureFormat::Parquet,
        fixture::FixtureFormat::Vortex,
    ] {
        let dataset = discover_mixed(format, LocusRepresentation::ContigPosition);

        let input_tables = dataset
            .input_tables()
            .iter()
            .map(|table| (table.name(), table.kind(), table.sample_set()))
            .collect::<Vec<_>>();
        let sample_sets = fixture::MIXED_SAMPLE_SETS
            .iter()
            .map(|samples| samples.iter().map(ToString::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            input_tables,
            [
                ("g0", InputTableKind::MultiSampleFile, &sample_sets[0][..]),
                (
                    "g1",
                    InputTableKind::MultiSampleDirectory,
                    &sample_sets[1][..]
                ),
                (
                    "s=NA18534",
                    InputTableKind::SingleSample,
                    &sample_sets[2][..]
                ),
            ]
        );
        assert_eq!(dataset.sample_set(), fixture::SAMPLES);
        assert_eq!(
            dataset.schema().field_with_name("s").ok(),
            None,
            "the dataset schema is the rows' schema without their sample"
        );
    }
}

#[test]
fn reads_a_mixed_dataset_with_one_view_string_sample_column_and_each_tables_rows() {
    for format in [
        fixture::FixtureFormat::Parquet,
        fixture::FixtureFormat::Vortex,
    ] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let fixture = Arc::clone(fixture::mixed_dataset_fixture(format, representation));
            let mut rows = pipeline::run(
                move |ctx| {
                    fixture.register(&ctx);
                    async move {
                        let dataset = Dataset::discover(
                            &ctx,
                            fixture.table_path().clone(),
                            fixture.input_format(),
                            RowOrdering::locus_then_alleles(),
                            None,
                        )
                        .await?;
                        let df = dataset.read(&ctx).await?;
                        let fields = df.schema().fields();
                        let sample = fields.last().unwrap();
                        assert_eq!(sample.name(), "s");
                        assert_eq!(sample.data_type(), &DataType::Utf8View);
                        assert!(!sample.is_nullable());
                        let batches = df.collect().await?;
                        Ok(batches
                            .iter()
                            .flat_map(|batch| fixture::decode_rows(batch, representation))
                            .collect::<Vec<_>>())
                    }
                },
                PipelineOptions::single_threaded(),
            )
            .unwrap();

            let mut expected = fixture::SAMPLES
                .iter()
                .flat_map(|sample| {
                    fixture::sample_rows()
                        .into_iter()
                        .map(|(locus, alleles)| (locus, alleles.to_string(), sample.to_string()))
                })
                .collect::<Vec<_>>();
            rows.sort();
            expected.sort();
            assert_eq!(rows, expected);
        }
    }
}

#[test]
fn reading_a_mixed_dataset_unions_one_single_partition_input_per_input_table() {
    for format in [
        fixture::FixtureFormat::Parquet,
        fixture::FixtureFormat::Vortex,
    ] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let fixture = fixture::mixed_dataset_fixture(format, representation);

            block_on(async {
                let ctx = SessionContext::new();
                fixture.register(&ctx);
                let dataset = Dataset::discover(
                    &ctx,
                    fixture.table_path().clone(),
                    fixture.input_format(),
                    RowOrdering::locus_then_alleles(),
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

                PlanShape::of(&plan)
                    .assert_one_ordered_partition_per_sample(fixture::MIXED_INPUT_TABLES.len());
            });
        }
    }
}

#[test]
fn narrowing_a_mixed_dataset_selects_whole_multi_sample_input_tables() {
    let dataset = discover_mixed(
        fixture::FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    )
    .restrict_to(&["s=NA18534".to_string(), "g0".to_string()])
    .unwrap();

    let names = dataset
        .input_tables()
        .iter()
        .map(InputTable::name)
        .collect::<Vec<_>>();
    assert_eq!(names, ["g0", "s=NA18534"]);
    assert_eq!(dataset.sample_set(), ["HG00308", "HG00592", "NA18534"]);
}

#[test]
fn rejects_a_root_entry_that_is_no_input_table_naming_it() {
    for (entries, rejected) in [
        (&["notes.txt"][..], "notes.txt"),
        (&["g0.parquet", "g0.samples.parquet"][..], "g0.parquet"),
        (&["g0.samples.txt"][..], "g0.samples.txt"),
    ] {
        let error = block_on(discover_entries_in_memory(entries))
            .expect_err("a stray root entry must fail discovery");

        assert!(matches!(error, DataFusionError::Plan(_)));
        let message = error.to_string();
        assert!(
            message.contains(&format!("memory:///samples/{rejected}")),
            "{entries:?}: unexpected error: {message}"
        );
    }
}

#[test]
fn rejects_multi_sample_data_without_its_sample_annotation_table_naming_it() {
    for (entries, rejected) in [
        (&["g0.vortex"][..], "g0.vortex"),
        (&["g1/0.vortex", "g1/1.vortex"][..], "g1/"),
    ] {
        let error = block_on(discover_entries_in_memory(entries))
            .expect_err("data without its annotation table must fail discovery");

        assert!(matches!(error, DataFusionError::Plan(_)));
        let message = error.to_string();
        assert!(
            message.contains(&format!("memory:///samples/{rejected}")),
            "{entries:?}: unexpected error: {message}"
        );
        assert!(
            message.contains("no sample annotation table"),
            "{entries:?}: unexpected error: {message}"
        );
    }
}

#[test]
fn rejects_a_sample_annotation_table_without_its_data_naming_it() {
    let error = block_on(discover_entries_in_memory(&["g0.samples.vortex"]))
        .expect_err("an annotation table without data must fail discovery");

    assert!(matches!(error, DataFusionError::Plan(_)));
    let message = error.to_string();
    assert!(
        message.contains("memory:///samples/g0.samples.vortex"),
        "unexpected error: {message}"
    );
    assert!(message.contains("no data"), "unexpected error: {message}");
}

#[test]
fn rejects_a_sample_in_two_input_tables_naming_both() {
    let error = Dataset::new(
        ListingTableUrl::parse("memory:///samples").unwrap(),
        InputFormat::VORTEX,
        RowOrdering::locus_then_alleles(),
        contig_position_schema(true),
        vec![
            InputTable::single_sample("sample-b"),
            InputTable::multi_sample_file(
                "g0",
                vec!["sample-b".to_string(), "sample-a".to_string()],
            ),
        ],
    )
    .expect_err("a sample in two input tables must fail");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert_eq!(
        error.to_string(),
        "Error during planning: sample 'sample-b' is in both input tables 'g0' and 's=sample-b' \
         of dataset 'memory:///samples/'"
    );
}

#[test]
fn rejects_a_sample_in_two_discovered_input_tables_naming_both() {
    for (first, second, names) in [
        ("s=sample-a/a.vortex", "g0.vortex", "'g0' and 's=sample-a'"),
        (
            "s=sample-a/a.vortex",
            "g0/a.vortex",
            "'g0' and 's=sample-a'",
        ),
        ("g1.vortex", "g0/a.vortex", "'g0' and 'g1'"),
    ] {
        let mut entries = vec![
            (first, vec![contig(), position(), sample("sample-a")]),
            (second, vec![contig(), position(), sample("sample-a")]),
            ("g0.samples.vortex", vec![sample("sample-a")]),
        ];
        if first.starts_with("g1") {
            entries.push(("g1.samples.vortex", vec![sample("sample-a")]));
        }
        let error = discover_written(entries)
            .expect_err("a sample in two input tables must fail discovery");

        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "{second}: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains(&format!(
                "sample 'sample-a' is in both input tables {names}"
            )),
            "{first}, {second}: unexpected error: {message}"
        );
    }
}

#[test]
fn rejects_an_input_table_whose_columns_differ_from_the_datasets_naming_it() {
    let differences: [(Vec<(Field, ArrayRef)>, &str); 3] = [
        (vec![], "it lacks the dataset's [MIN_DP: Int32]"),
        (
            vec![(
                Field::new("MIN_DP", DataType::Int64, false),
                Arc::new(Int64Array::from(vec![1])),
            )],
            "it lacks the dataset's [MIN_DP: Int32] and the dataset lacks its [MIN_DP: Int64]",
        ),
        (
            vec![(
                Field::new("MIN_DP", DataType::Int32, true),
                Arc::new(Int32Array::from(vec![1])),
            )],
            "it lacks the dataset's [MIN_DP: Int32] and the dataset lacks its [MIN_DP: Int32?]",
        ),
    ];
    for (data, annotation, table) in [
        ("s=sample-b/a.vortex", None, "s=sample-b"),
        ("g0.vortex", Some("g0.samples.vortex"), "g0"),
        ("g0/a.vortex", Some("g0.samples.vortex"), "g0"),
    ] {
        for (columns, difference) in differences.clone() {
            let mut entries = vec![
                ("s=sample-a/a.vortex", vec![contig(), position(), min_dp()]),
                (
                    data,
                    [contig(), position(), sample("sample-b")]
                        .into_iter()
                        .chain(columns)
                        .collect(),
                ),
            ];
            entries.extend(annotation.map(|path| (path, vec![sample("sample-b")])));
            let error = discover_written(entries)
                .expect_err("an input table without the dataset's columns must fail discovery");

            assert!(matches!(error, DataFusionError::Plan(_)), "{data}: {error}");
            let message = error.to_string();
            assert!(
                message.contains(&format!("input table '{table}'")) && message.contains(difference),
                "{data}: unexpected error: {message}"
            );
        }
    }
}

#[test]
fn rejects_a_multi_sample_input_table_without_a_non_null_string_sample_column_naming_it() {
    for data in ["g0.vortex", "g0/a.vortex"] {
        for (case, invalid_sample) in invalid_sample_columns() {
            let error = discover_written(vec![
                ("s=sample-a/a.vortex", vec![contig(), position()]),
                (
                    data,
                    [contig(), position()]
                        .into_iter()
                        .chain(invalid_sample)
                        .collect(),
                ),
                ("g0.samples.vortex", vec![sample("sample-b")]),
            ])
            .expect_err("a multi-sample input table without a valid `s` must fail discovery");

            assert!(
                matches!(error, DataFusionError::Plan(_)),
                "{data}, {case}: {error}"
            );
            let message = error.to_string();
            assert!(
                message.contains("input table 'g0'") && message.contains("'s'"),
                "{data}, {case}: unexpected error: {message}"
            );
        }
    }
}

#[test]
fn rejects_a_sample_annotation_table_without_a_non_null_string_sample_column_naming_it() {
    for data in ["g0.vortex", "g0/a.vortex"] {
        for (case, invalid_sample) in invalid_sample_columns() {
            let note = (
                Field::new("note", DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["an ignored column"])) as ArrayRef,
            );
            let error = discover_written(vec![
                ("s=sample-a/a.vortex", vec![contig(), position()]),
                (data, vec![contig(), position(), sample("sample-b")]),
                (
                    "g0.samples.vortex",
                    invalid_sample.into_iter().chain([note]).collect(),
                ),
            ])
            .expect_err("an annotation table without a valid `s` must fail discovery");

            assert!(
                matches!(error, DataFusionError::Plan(_)),
                "{data}, {case}: {error}"
            );
            let message = error.to_string();
            assert!(
                message.contains("samples/g0.samples.vortex") && message.contains("'s'"),
                "{data}, {case}: unexpected error: {message}"
            );
        }
    }
}

#[test]
fn discovers_input_tables_with_the_datasets_columns_and_a_non_null_string_sample_column() {
    for data in ["g0.vortex", "g0/a.vortex"] {
        let dataset = discover_written(vec![
            ("s=sample-a/a.vortex", vec![contig(), position(), min_dp()]),
            (
                data,
                vec![contig(), position(), sample("sample-b"), min_dp()],
            ),
            ("g0.samples.vortex", vec![sample("sample-b")]),
        ])
        .unwrap();

        assert_eq!(dataset.sample_set(), ["sample-a", "sample-b"], "{data}");
    }
}

fn contig() -> (Field, ArrayRef) {
    (
        Field::new("contig", DataType::Utf8, false),
        Arc::new(StringArray::from(vec!["chr1"])),
    )
}

fn position() -> (Field, ArrayRef) {
    (
        Field::new("position", DataType::Int32, false),
        Arc::new(Int32Array::from(vec![1])),
    )
}

fn min_dp() -> (Field, ArrayRef) {
    (
        Field::new("MIN_DP", DataType::Int32, false),
        Arc::new(Int32Array::from(vec![1])),
    )
}

fn sample(id: &str) -> (Field, ArrayRef) {
    (
        Field::new("s", DataType::Utf8, false),
        Arc::new(StringArray::from(vec![id])),
    )
}

/// Each way `s` can fail to be a non-null string, named, as the columns that stand in for it.
fn invalid_sample_columns() -> [(&'static str, Vec<(Field, ArrayRef)>); 3] {
    [
        ("missing", vec![]),
        (
            "nullable",
            vec![(
                Field::new("s", DataType::Utf8, true),
                Arc::new(StringArray::from(vec!["sample-b"])),
            )],
        ),
        (
            "not a string",
            vec![(
                Field::new("s", DataType::Int32, false),
                Arc::new(Int32Array::from(vec![1])),
            )],
        ),
    ]
}

/// Discovers, with an inferred schema, the Vortex dataset under `samples/` of an in-memory store
/// holding a one-row file at each of `entries`, a path under `samples/` and the file's columns.
fn discover_written(entries: Vec<(&str, Vec<(Field, ArrayRef)>)>) -> Result<Dataset> {
    let store = fixture::MemoryStore::new("written-entries");
    let root = format!("{}samples", store.url().as_str());
    let entries = entries
        .into_iter()
        .map(|(path, columns)| {
            let (fields, arrays) = columns.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
            Ok((
                format!("{root}/{path}"),
                RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    pipeline::run(
        move |ctx| {
            store.register(&ctx);
            async move {
                for (output_path, batch) in entries {
                    WriteTarget {
                        output_path,
                        output_format: OutputFormat::VORTEX,
                    }
                    .write_unordered(ctx.read_batch(batch)?)
                    .await?;
                }
                Dataset::discover(
                    &ctx,
                    ListingTableUrl::parse(&root)?,
                    InputFormat::VORTEX,
                    RowOrdering::locus(),
                    None,
                )
                .await
            }
        },
        PipelineOptions::single_threaded(),
    )
}

fn discover_mixed(format: fixture::FixtureFormat, representation: LocusRepresentation) -> Dataset {
    let fixture = fixture::mixed_dataset_fixture(format, representation);
    let ctx = SessionContext::new();
    fixture.register(&ctx);
    block_on(Dataset::discover(
        &ctx,
        fixture.table_path().clone(),
        fixture.input_format(),
        RowOrdering::locus_then_alleles(),
        None,
    ))
    .unwrap()
}

/// Discovers a Vortex dataset whose root holds a single-sample input table and empty objects at
/// `entries`, under a pinned schema. Every rejection of a root entry precedes reading one.
async fn discover_entries_in_memory(entries: &[&str]) -> Result<Dataset> {
    let ctx = SessionContext::new();
    let store = Arc::new(InMemory::new());
    for entry in std::iter::once(&"s=sample-a/marker").chain(entries) {
        store
            .put(
                &Path::from(format!("samples/{entry}")),
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
        RowOrdering::locus(),
        Some(contig_position_schema(false)),
    )
    .await
}
