use super::{plan_shape::PlanShape, support::hostile_config};
use crate::{
    dataset::{Dataset, read_sorted_table},
    fixture::{self, FixtureFormat},
    format::OutputFormat,
    formulation::Formulation,
    locus::{Locus, LocusOrdering, LocusRepresentation},
    pipeline::{self, PipelineOptions},
    split_points::{row_balanced_from_frame, row_balanced_plan},
};

use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array, StringViewArray},
        record_batch::RecordBatch,
    },
    common::DataFusionError,
    datasource::source::DataSourceExec,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, filter::FilterExec,
        repartition::RepartitionExec, sorts::sort::SortExec, windows::BoundedWindowAggExec,
    },
    prelude::SessionContext,
};
use std::{num::NonZeroUsize, sync::Arc};

#[test]
fn row_balanced_chooses_the_loci_at_floor_indices() {
    let loci = (1..=10)
        .map(|position| Locus::new(1, position).unwrap())
        .collect::<Vec<_>>();
    for (format, format_name) in [
        (FixtureFormat::Parquet, "parquet"),
        (FixtureFormat::Vortex, "vortex"),
    ] {
        for (representation, representation_name) in [
            (LocusRepresentation::ContigPosition, "contig-position"),
            (LocusRepresentation::Packed, "packed"),
        ] {
            for (layout, files) in [
                ("file", vec![loci.clone()]),
                (
                    "directory",
                    vec![loci[0..4].to_vec(), loci[4..7].to_vec(), loci[7..].to_vec()],
                ),
            ] {
                let fixture = fixture::sorted_table_fixture(
                    &format!("row-balanced-{format_name}-{representation_name}-{layout}"),
                    format,
                    representation,
                    files,
                );
                pipeline::run(
                    move |ctx| {
                        fixture.register(&ctx);
                        async move {
                            let path = if layout == "file" {
                                fixture.file_path(0).clone()
                            } else {
                                fixture.table_path().clone()
                            };
                            let frame = read_sorted_table(
                                &ctx,
                                path,
                                fixture.input_format(),
                                LocusOrdering::locus(),
                            )
                            .await?;
                            let points =
                                row_balanced_from_frame(frame, NonZeroUsize::new(4).unwrap())
                                    .await?;
                            assert_eq!(points.to_string(), "1:3,1:6,1:8");
                            Ok(())
                        }
                    },
                    PipelineOptions::single_threaded(),
                )
                .unwrap();
            }
        }
    }
}

#[test]
fn row_balanced_handles_several_row_and_interval_counts() {
    for (name, row_count, intervals, expected) in [
        ("five-rows-two-intervals", 5, 2, "1:3"),
        ("nine-rows-three-intervals", 9, 3, "1:4,1:7"),
        ("ten-rows-four-intervals", 10, 4, "1:3,1:6,1:8"),
    ] {
        let loci = (1..=row_count)
            .map(|position| Locus::new(1, position).unwrap())
            .collect::<Vec<_>>();
        let fixture = fixture::sorted_table_fixture(
            &format!("row-balanced-counts-{name}"),
            FixtureFormat::Vortex,
            LocusRepresentation::Packed,
            vec![loci],
        );

        pipeline::run(
            move |ctx| {
                fixture.register(&ctx);
                async move {
                    let frame = read_sorted_table(
                        &ctx,
                        fixture.file_path(0).clone(),
                        fixture.input_format(),
                        LocusOrdering::locus(),
                    )
                    .await?;
                    let points =
                        row_balanced_from_frame(frame, NonZeroUsize::new(intervals).unwrap())
                            .await?;
                    assert_eq!(points.to_string(), expected, "{name}");
                    Ok(())
                }
            },
            PipelineOptions::single_threaded(),
        )
        .unwrap();
    }
}

#[test]
fn row_balanced_rejects_a_table_with_no_rows() {
    let fixture = fixture::sorted_table_fixture(
        "row-balanced-empty",
        FixtureFormat::Vortex,
        LocusRepresentation::Packed,
        vec![Vec::new()],
    );

    let error = pipeline::run(
        move |ctx| {
            fixture.register(&ctx);
            async move {
                let frame = read_sorted_table(
                    &ctx,
                    fixture.file_path(0).clone(),
                    fixture.input_format(),
                    LocusOrdering::locus(),
                )
                .await?;
                row_balanced_from_frame(frame, NonZeroUsize::new(2).unwrap()).await
            }
        },
        PipelineOptions::single_threaded(),
    )
    .expect_err("an empty table has no split-point row");

    assert!(matches!(error, DataFusionError::Configuration(_)));
    assert!(error.to_string().contains("table has no rows"), "{error}");
}

#[test]
fn row_balanced_rejects_target_rows_on_the_same_locus() {
    let repeated = Locus::new(1, 3).unwrap();
    let loci = vec![
        Locus::new(1, 1).unwrap(),
        Locus::new(1, 2).unwrap(),
        repeated,
        repeated,
        repeated,
        repeated,
        repeated,
        Locus::new(1, 8).unwrap(),
        Locus::new(1, 9).unwrap(),
        Locus::new(1, 10).unwrap(),
    ];
    let fixture = fixture::sorted_table_fixture(
        "row-balanced-shared-locus",
        FixtureFormat::Parquet,
        LocusRepresentation::ContigPosition,
        vec![loci],
    );

    let error = pipeline::run(
        move |ctx| {
            fixture.register(&ctx);
            async move {
                let frame = read_sorted_table(
                    &ctx,
                    fixture.file_path(0).clone(),
                    fixture.input_format(),
                    LocusOrdering::locus(),
                )
                .await?;
                row_balanced_from_frame(frame, NonZeroUsize::new(4).unwrap()).await
            }
        },
        PipelineOptions::single_threaded(),
    )
    .expect_err("two target rows at one locus cannot define distinct intervals");

    assert!(matches!(error, DataFusionError::Configuration(_)));
    let message = error.to_string();
    assert!(
        message.contains("cannot be balanced into 4 intervals"),
        "{message}"
    );
    assert!(message.contains("1:3 does not follow 1:3"), "{message}");
}

#[test]
fn row_balanced_rejects_when_two_targets_name_the_same_row() {
    let fixture = fixture::sorted_table_fixture(
        "row-balanced-repeated-target-row",
        FixtureFormat::Vortex,
        LocusRepresentation::Packed,
        vec![vec![Locus::new(1, 1).unwrap(), Locus::new(1, 2).unwrap()]],
    );

    let error = pipeline::run(
        move |ctx| {
            fixture.register(&ctx);
            async move {
                let frame = read_sorted_table(
                    &ctx,
                    fixture.file_path(0).clone(),
                    fixture.input_format(),
                    LocusOrdering::locus(),
                )
                .await?;
                row_balanced_from_frame(frame, NonZeroUsize::new(4).unwrap()).await
            }
        },
        PipelineOptions::single_threaded(),
    )
    .expect_err("one row cannot supply two strictly increasing split points");

    assert!(matches!(error, DataFusionError::Configuration(_)));
    let message = error.to_string();
    assert!(
        message.contains("cannot be balanced into 4 intervals"),
        "{message}"
    );
    assert!(message.contains("1:2 does not follow 1:2"), "{message}");
}

#[test]
fn row_balanced_rejects_a_contig_name_without_an_ordinal() {
    let error = pipeline::run(
        |ctx| async move {
            let contigs: ArrayRef = Arc::new(StringViewArray::from(vec!["chr01", "chrX"]));
            let positions: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
            let batch =
                RecordBatch::try_from_iter(vec![("contig", contigs), ("position", positions)])?;
            let frame = ctx.read_batch(batch)?;
            row_balanced_from_frame(frame, NonZeroUsize::new(2).unwrap()).await
        },
        PipelineOptions::single_threaded(),
    )
    .expect_err("a selected foreign contig name must be rejected");

    assert!(matches!(error, DataFusionError::Configuration(_)));
    assert!(
        error.to_string().contains("invalid contig name 'chrX'"),
        "{error}"
    );
}

#[test]
fn row_balanced_plan_is_one_ordered_scan_window_and_filter() {
    let fixture = Arc::clone(fixture::dataset_fixture(
        FixtureFormat::Parquet,
        LocusRepresentation::Packed,
    ));

    pipeline::run(
        move |_| async move {
            let ctx = SessionContext::new_with_config(hostile_config(8));
            fixture.register(&ctx);
            let dataset = Dataset::discover(
                &ctx,
                fixture.table_path().clone(),
                fixture.input_format(),
                LocusOrdering::locus_then_alleles(),
                None,
            )
            .await?;
            let formulation = Formulation::CombineRefsIntervalMerge {
                split_points: "1:3,2:2".parse()?,
            };
            let directory = format!("{}row-balanced-plan-input", fixture.table_path().as_str());
            let ordered = formulation.plan(&ctx, &dataset).await?;
            OutputFormat::PARQUET.write(ordered, &directory).await?;
            let frame = read_sorted_table(
                &ctx,
                datafusion::datasource::listing::ListingTableUrl::parse(directory)?,
                fixture.input_format(),
                LocusOrdering::locus(),
            )
            .await?;
            let (_, _, selected) = row_balanced_plan(frame, NonZeroUsize::new(4).unwrap()).await?;
            let plan = selected.create_physical_plan().await?;
            let shape = PlanShape::of(&plan);

            assert_eq!(shape.nodes_of::<DataSourceExec>().len(), 1, "{shape}");
            assert_eq!(shape.nodes_of::<BoundedWindowAggExec>().len(), 1, "{shape}");
            assert_eq!(shape.nodes_of::<FilterExec>().len(), 1, "{shape}");
            assert!(shape.nodes_of::<SortExec>().is_empty(), "{shape}");
            assert!(
                shape.nodes_of::<CoalescePartitionsExec>().is_empty(),
                "{shape}"
            );
            assert!(shape.nodes_of::<RepartitionExec>().is_empty(), "{shape}");
            Ok::<_, DataFusionError>(())
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();
}
