//! Discovering the shared dataset fixtures, and planning a plan builder over them through a sink.
//!
//! The suite plans with a test adapter at the plan builder seam, which reads the dataset in a
//! known order and records the session settings it planned under.

use crate::{
    formulation::Formulation,
    locus::{Locus, LocusRepresentation},
    ordered_frame::{OrderedFrame, OutputLayout},
    pipeline::{self, PipelineOptions},
    row_ordering::{RowOrdering, StoredOrdering},
    stored::dataset::{Dataset, InputTable},
    testing::{
        columns::u64_values,
        fixture::{self, FixtureFormat, INPUT_TABLES, MIXED_INPUT_TABLES, Row, block_on},
        formulation_plans::{
            FORMATS, FixtureDataset, PlanBuilder, REPRESENTATIONS, collected_batches,
            collected_rows, dataset, dataset_with_ordering, drained_plan,
            drained_plan_and_ordering, file_sink_plan, loci, mixed_dataset, output_format,
            output_path, physical_plan, physical_plan_with_target, planned, shared_datasets,
            sink_plan,
        },
        plan_shape::PlanShape,
        session::hostile_config,
    },
};

use datafusion::{
    datasource::{sink::DataSinkExec, source::DataSourceExec},
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
    prelude::{SessionContext, col},
};
use futures::{FutureExt, future::BoxFuture};
use std::sync::{Arc, Mutex};

/// The settings of a session a plan was built in that this module chooses.
#[derive(Debug, PartialEq)]
struct Settings {
    target_partitions: usize,
    repartition_file_min_size: usize,
}

/// A plan builder that reads its dataset sorted by locus, sample, and alleles, so its rows have
/// one order, and records the settings of each session it plans in.
#[derive(Clone)]
struct ReadSorted {
    layout: OutputLayout,
    planned_in: Arc<Mutex<Vec<Settings>>>,
}

impl ReadSorted {
    fn new(layout: OutputLayout) -> Self {
        Self {
            layout,
            planned_in: Arc::default(),
        }
    }

    fn planned_in(&self) -> Vec<Settings> {
        std::mem::take(&mut self.planned_in.lock().unwrap())
    }
}

/// The ordering [`ReadSorted`] declares: the locus then the sample.
fn ordering(representation: LocusRepresentation) -> StoredOrdering {
    RowOrdering::locus_then_sample().expand(representation)
}

impl PlanBuilder for ReadSorted {
    fn plan<'a>(
        &'a self,
        ctx: &'a SessionContext,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<OrderedFrame>> {
        async move {
            let config = ctx.state().config().clone();
            self.planned_in.lock().unwrap().push(Settings {
                target_partitions: config.target_partitions(),
                repartition_file_min_size: config.options().optimizer.repartition_file_min_size,
            });
            let ordering = ordering(dataset.locus_representation());
            let mut sort = ordering.sort_expressions();
            sort.push(col("alleles").sort(true, true));
            Ok(OrderedFrame {
                frame: dataset.read(ctx).await?.sort(sort)?,
                ordering,
                layout: self.layout,
            })
        }
        .boxed()
    }
}

/// A plan builder that fails.
#[derive(Clone)]
struct Failing;

impl PlanBuilder for Failing {
    fn plan<'a>(
        &'a self,
        _ctx: &'a SessionContext,
        _dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<OrderedFrame>> {
        async { Err(DataFusionError::Plan("this builder fails".to_string())) }.boxed()
    }
}

/// Every row of the dataset fixture, as [`ReadSorted`] orders them.
fn every_row() -> Vec<Row> {
    let mut rows: Vec<Row> = fixture::sample_rows()
        .into_iter()
        .flat_map(|(locus, alleles)| {
            fixture::SAMPLES
                .iter()
                .map(move |sample| (locus, alleles.to_string(), (*sample).to_string()))
        })
        .collect();
    rows.sort_by(|left, right| (left.0, &left.2, &left.1).cmp(&(right.0, &right.2, &right.1)));
    rows
}

fn input_table_names(dataset: &Dataset) -> Vec<&str> {
    dataset
        .input_tables()
        .iter()
        .map(InputTable::name)
        .collect()
}

fn same_format(left: FixtureFormat, right: FixtureFormat) -> bool {
    format!("{left:?}") == format!("{right:?}")
}

// Datasets.

#[test]
fn the_loops_cover_both_fixture_formats_and_both_representations() {
    assert_eq!(format!("{FORMATS:?}"), "[Parquet, Vortex]");
    assert_eq!(format!("{REPRESENTATIONS:?}"), "[ContigPosition, Packed]");
}

#[test]
fn shared_datasets_are_the_dataset_fixture_then_the_mixed_one_as_discovered() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let [plain, mixed] = shared_datasets(format, representation);

            for (dataset, fixture, input_tables) in [
                (
                    &plain,
                    fixture::dataset_fixture(format, representation),
                    INPUT_TABLES,
                ),
                (
                    &mixed,
                    fixture::mixed_dataset_fixture(format, representation),
                    MIXED_INPUT_TABLES,
                ),
            ] {
                let context = format!("{format:?} {representation:?} {input_tables:?}");
                assert!(Arc::ptr_eq(dataset.fixture, fixture), "{context}");
                assert!(same_format(dataset.format, format), "{context}");
                assert_eq!(dataset.input_tables, input_tables, "{context}");
                assert_eq!(
                    input_table_names(&dataset.dataset),
                    input_tables,
                    "{context}"
                );
                assert_eq!(
                    dataset.dataset.locus_representation(),
                    representation,
                    "{context}"
                );
            }
        }
    }
}

#[test]
fn dataset_and_mixed_dataset_satisfy_the_allele_combiners_ordering() {
    let required = Formulation::CombineAllelesUnion.required_ordering();
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for dataset in [
                dataset(format, representation),
                mixed_dataset(format, representation),
            ] {
                assert_eq!(
                    dataset.dataset.query_ordering(&required).unwrap(),
                    required.expand(representation),
                    "{format:?} {representation:?}"
                );
            }
        }
    }
}

#[test]
fn dataset_with_ordering_declares_the_given_ordering() {
    let representation = LocusRepresentation::Packed;

    let dataset =
        dataset_with_ordering(FixtureFormat::Parquet, representation, RowOrdering::locus());

    assert_eq!(
        dataset
            .dataset
            .query_ordering(&RowOrdering::locus())
            .unwrap(),
        RowOrdering::locus().expand(representation)
    );
    dataset
        .dataset
        .query_ordering(&RowOrdering::locus_then_alleles())
        .unwrap_err();
}

#[test]
fn restricted_to_last_keeps_the_last_input_tables_in_the_inventory_and_the_dataset() {
    for (dataset, n, expected) in [
        (
            dataset(FixtureFormat::Parquet, LocusRepresentation::Packed),
            1,
            &["s=NA18534"][..],
        ),
        (
            mixed_dataset(FixtureFormat::Vortex, LocusRepresentation::ContigPosition),
            2,
            &["g1", "s=NA18534"][..],
        ),
    ] {
        let restricted = dataset.restricted_to_last(n);

        assert_eq!(restricted.input_tables, expected);
        assert_eq!(input_table_names(&restricted.dataset), expected);
    }
}

// Planning.

#[test]
fn planned_plans_in_a_session_of_the_config_with_the_fixtures_store_registered() {
    let builder = ReadSorted::new(OutputLayout::SingleFile);
    let dataset = dataset(FixtureFormat::Vortex, LocusRepresentation::Packed);
    let table_path = dataset.fixture.table_path().clone();

    let (ctx, ordered) = block_on(planned(&builder, &dataset, hostile_config(5))).unwrap();

    assert_eq!(
        builder.planned_in(),
        [Settings {
            target_partitions: 5,
            repartition_file_min_size: 0
        }]
    );
    assert_eq!(ctx.state().config().target_partitions(), 5);
    ctx.runtime_env().object_store(&table_path).unwrap();
    assert_eq!(ordered.ordering, ordering(LocusRepresentation::Packed));
}

#[test]
fn planned_returns_the_builders_error() {
    let dataset = dataset(FixtureFormat::Parquet, LocusRepresentation::Packed);

    let Err(error) = block_on(planned(&Failing, &dataset, hostile_config(5))) else {
        panic!("planning with a failing builder succeeded");
    };

    assert!(error.to_string().contains("this builder fails"), "{error}");
}

#[test]
fn sink_plan_ends_the_frame_in_a_draining_sink_requiring_its_ordering() {
    let builder = ReadSorted::new(OutputLayout::SingleFile);
    let dataset = dataset(FixtureFormat::Parquet, LocusRepresentation::ContigPosition);

    let plan = block_on(async {
        let (_, ordered) = planned(&builder, &dataset, hostile_config(2)).await?;
        sink_plan(ordered).await
    })
    .unwrap();

    let shape = PlanShape::of(&plan);
    shape.assert_ends_in_sink_requiring(&ordering(LocusRepresentation::ContigPosition));
    assert!(shape.to_string().contains("DrainingSink"), "{shape}");
}

#[test]
fn physical_plan_plans_under_the_hostile_config_of_eight_partitions_into_a_draining_sink() {
    let builder = ReadSorted::new(OutputLayout::SingleFile);
    let dataset = dataset(FixtureFormat::Parquet, LocusRepresentation::Packed);

    let plan = physical_plan(&builder, &dataset);
    let with_target = physical_plan_with_target(&builder, &dataset, 3);

    assert_eq!(
        builder.planned_in(),
        [
            Settings {
                target_partitions: 8,
                repartition_file_min_size: 0
            },
            Settings {
                target_partitions: 3,
                repartition_file_min_size: 0
            }
        ]
    );
    for plan in [plan, with_target] {
        let shape = PlanShape::of(&plan);
        shape.assert_ends_in_sink_requiring(&ordering(LocusRepresentation::Packed));
        assert!(shape.to_string().contains("DrainingSink"), "{shape}");
    }
}

/// The rows `plan`, a sink plan over the fixture of `dataset`, writes when executed.
fn rows_written(plan: Arc<dyn ExecutionPlan>, dataset: &FixtureDataset) -> u64 {
    let fixture = Arc::clone(dataset.fixture);
    let batches = pipeline::run(
        move |ctx| async move {
            fixture.register(&ctx);
            datafusion::physical_plan::collect(plan, ctx.task_ctx()).await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();
    batches
        .iter()
        .flat_map(|batch| u64_values(batch, "count"))
        .map(Option::unwrap)
        .sum()
}

#[test]
fn drained_plan_drains_the_builders_rows_up_to_the_limit_and_hands_back_their_ordering() {
    let builder = ReadSorted::new(OutputLayout::SingleFile);
    let dataset = dataset(FixtureFormat::Vortex, LocusRepresentation::ContigPosition);
    let all_rows = u64::try_from(every_row().len()).unwrap();

    for (limit, expected) in [(None, all_rows), (Some(3), 3)] {
        let (plan, ordering) =
            drained_plan_and_ordering(&builder, &dataset, hostile_config(4), limit);
        let drained = drained_plan(&builder, &dataset, hostile_config(4), limit);

        assert_eq!(
            ordering,
            self::ordering(LocusRepresentation::ContigPosition)
        );
        PlanShape::of(&plan).assert_ends_in_sink_requiring(&ordering);
        assert_eq!(rows_written(plan, &dataset), expected, "{limit:?}");
        assert_eq!(rows_written(drained, &dataset), expected, "{limit:?}");
    }
}

// Writes.

#[test]
fn output_format_writes_files_named_as_the_fixture_names_its_own() {
    for format in FORMATS {
        let fixture = fixture::dataset_fixture(format, LocusRepresentation::Packed);
        let extension = output_format(format).extension();

        for file in block_on(fixture.sample_files(fixture::SAMPLES[0])) {
            assert_eq!(file.location.extension(), Some(extension), "{format:?}");
        }
    }
}

#[test]
fn output_path_is_a_file_or_a_directory_beside_the_dataset_by_layout() {
    for format in FORMATS {
        let dataset = dataset(format, LocusRepresentation::Packed);
        let root = dataset.fixture.output_root().to_string();
        let extension = output_format(format).extension();

        for (layout, expected) in [
            (
                OutputLayout::SingleFile,
                format!("{root}/combined.{extension}"),
            ),
            (OutputLayout::FilePerPartition, format!("{root}/combined")),
        ] {
            let (_, ordered) = block_on(planned(
                &ReadSorted::new(layout),
                &dataset,
                hostile_config(1),
            ))
            .unwrap();

            assert_eq!(output_path(&ordered, &dataset), expected, "{format:?}");
        }
    }
}

#[test]
fn file_sink_plan_writes_the_fixture_format_requiring_the_frames_ordering_up_to_the_limit() {
    for format in FORMATS {
        for layout in [OutputLayout::SingleFile, OutputLayout::FilePerPartition] {
            let builder = ReadSorted::new(layout);
            let dataset = dataset(format, LocusRepresentation::Packed);

            let (plan, ordering) = file_sink_plan(&builder, &dataset, hostile_config(4), Some(3));

            let shape = PlanShape::of(&plan);
            let context = format!("{format:?} {layout:?}:\n{shape}");
            assert_eq!(ordering, self::ordering(LocusRepresentation::Packed));
            shape.assert_ends_in_sink_requiring(&ordering);
            let sink = match format {
                FixtureFormat::Parquet => "sink=ParquetSink",
                FixtureFormat::Vortex => "sink=VortexSink",
            };
            assert!(shape.to_string().contains(sink), "{context}");
            assert!(shape.to_string().contains("fetch=3"), "{context}");
            assert_eq!(builder.planned_in().len(), 1, "{context}");
        }
    }
}

// Collecting.

#[test]
fn collected_batches_executes_the_adjusted_frame_into_the_collecting_sink() {
    let builder = ReadSorted::new(OutputLayout::SingleFile);
    let dataset = dataset(FixtureFormat::Parquet, LocusRepresentation::Packed);

    let (plan, batches) = collected_batches(&builder, dataset, |frame| frame.limit(0, Some(5)));

    let rows: Vec<Row> = batches
        .iter()
        .flat_map(|batch| fixture::decode_rows(batch, LocusRepresentation::Packed))
        .collect();
    assert_eq!(rows, every_row()[..5]);
    let shape = PlanShape::of(&plan);
    shape.assert_ends_in_sink_requiring(&ordering(LocusRepresentation::Packed));
    assert!(shape.nodes_of::<DataSinkExec>().len() == 1, "{shape}");
    assert!(shape.output_rows_of::<DataSourceExec>() > 0, "{shape}");
    assert_eq!(
        builder.planned_in(),
        [Settings {
            target_partitions: 8,
            repartition_file_min_size: 0
        }]
    );
}

#[test]
fn collected_rows_decodes_the_builders_rows_in_order() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let rows = collected_rows(
                &ReadSorted::new(OutputLayout::SingleFile),
                format,
                representation,
            );

            assert_eq!(rows, every_row(), "{format:?} {representation:?}");
        }
    }
}

#[test]
fn loci_lists_each_rows_locus_in_order() {
    let [first, second] = [Locus::new(2, 1).unwrap(), Locus::new(1, 5).unwrap()];
    let rows: Vec<Row> = vec![
        (first, "A,G".to_string(), "HG00308".to_string()),
        (second, "A,C".to_string(), "HG00308".to_string()),
        (first, "A,C".to_string(), "NA18534".to_string()),
    ];

    assert_eq!(loci(&rows), [first, second, first]);
}
