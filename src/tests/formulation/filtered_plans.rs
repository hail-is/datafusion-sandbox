//! Formulations under an ordinary filter, as a combiner run caller would restrict a run to a
//! contig or a locus interval.
//!
//! A filter must not change how a plan gets to its result. The sorted table prunes each sample's
//! files by their ordering statistics and leaves the residual to the format, and the shared
//! session lets both formats apply that residual inside the scan. Without that, Parquet keeps a
//! filter operator and a repartition above every sample's scan that Vortex does not, and the
//! plans this repo exists to compare stop being comparable. See ADR 0003.
//!
//! The tests here check that the filter stays inside every sample's scan, which files each scan
//! kept, that Parquet and Vortex plans have the same operators, and that the rows that come back
//! are the filter's rows. Every plan is observed through a sink, as the parent module explains,
//! and rows are collected through the collecting sink so grouped-merge returns them in locus
//! order. The reference combiner's filtered plan is its unfiltered plan. The
//! allele combiner's parallel operators repartition above the union with or without a filter,
//! and under a filter the optimizer adds one more order-preserving round-robin there because a
//! filtered scan's row count is inexact. It does so in both formats alike, so that plan is
//! compared across formats rather than against its unfiltered shape.

use super::{
    FORMATS, FORMULATIONS, FixtureDataset, REPRESENTATIONS, assert_merge_tree, dataset, displayed,
    expected_groups, hostile_config, nodes_of, operators, ordering, sink_plan,
};
use crate::fixture::{self, FixtureFormat, SAMPLES, SampleRow, block_on};
use crate::formulation::Formulation;
use crate::locus::{Locus, LocusInterval, LocusRepresentation};
use crate::pipeline::{self, PipelineOptions};
use crate::sink;

use datafusion::{
    arrow::record_batch::RecordBatch,
    datasource::source::DataSourceExec,
    error::Result,
    logical_expr::Expr,
    physical_plan::{
        ExecutionPlan, filter::FilterExec, repartition::RepartitionExec, union::UnionExec,
    },
    prelude::{DataFrame, SessionContext},
};

use std::{ops::Range, sync::Arc};

/// A locus restriction a caller might place on a run, with what the fixtures say it should
/// select. Distinct from restricting the sample set.
#[derive(Clone, Copy, Debug)]
enum LocusRestriction {
    /// Every row on one contig.
    Contig,
    /// The rows of one half-open locus interval within a contig.
    LocusInterval,
}

impl LocusRestriction {
    const ALL: [Self; 2] = [Self::Contig, Self::LocusInterval];
    const CONTIG: &'static str = "chr2";
    const INTERVAL_CONTIG: &'static str = "chr1";
    const INTERVAL: Range<i32> = 3..5;

    fn filter(self, representation: LocusRepresentation) -> Expr {
        match self {
            Self::Contig => fixture::contig_filter(representation, Self::CONTIG),
            Self::LocusInterval => LocusInterval::new(
                Some(Locus::from_contig_name(Self::INTERVAL_CONTIG, Self::INTERVAL.start).unwrap()),
                Some(Locus::from_contig_name(Self::INTERVAL_CONTIG, Self::INTERVAL.end).unwrap()),
            )
            .unwrap()
            .filter(representation)
            .expect("a bounded interval has a filter"),
        }
    }

    /// Whether a fixture row satisfies the restriction, judged independently of any plan.
    fn keeps(self, (contig, position, _): SampleRow) -> bool {
        match self {
            Self::Contig => contig == Self::CONTIG,
            Self::LocusInterval => {
                contig == Self::INTERVAL_CONTIG && Self::INTERVAL.contains(&position)
            }
        }
    }

    /// The files of one sample whose ordering statistics admit a matching row, in locus order.
    /// Files d and c are constant on chr1, b spans the contig boundary, and a is constant on chr2.
    /// File c holds chr1:3 and b starts at chr1:4.
    const fn expected_stems(self) -> [&'static str; 2] {
        match self {
            Self::Contig => ["b", "a"],
            Self::LocusInterval => ["c", "b"],
        }
    }

    /// One sample's rows that satisfy the restriction, in locus-then-alleles order.
    fn expected_rows(self) -> Vec<(String, i32, String)> {
        fixture::sample_rows()
            .into_iter()
            .filter(|&row| self.keeps(row))
            .map(|(contig, position, alleles)| (contig.to_string(), position, alleles.to_string()))
            .collect()
    }
}

#[test]
fn filtered_plans_keep_the_filter_inside_the_scans_in_both_formats_and_representations() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            for formulation in FORMULATIONS {
                for restriction in LocusRestriction::ALL {
                    let plan = filtered_physical_plan(
                        formulation,
                        &dataset,
                        restriction.filter(representation),
                    );
                    assert_filter_stays_inside_the_scans(
                        &plan,
                        &expected_groups(formulation, SAMPLES.len()),
                    );
                    assert_filter_reaches_every_scan(&plan, SAMPLES.len(), representation);
                }
            }
        }
    }
}

/// A comparison across formats must measure the format, not the optimizer's reaction to it.
#[test]
fn parquet_and_vortex_filtered_plans_have_the_same_operators() {
    for representation in REPRESENTATIONS {
        let parquet = dataset(FixtureFormat::Parquet, representation);
        let vortex = dataset(FixtureFormat::Vortex, representation);
        for formulation in FORMULATIONS {
            for restriction in LocusRestriction::ALL {
                let parquet_plan = filtered_physical_plan(
                    formulation,
                    &parquet,
                    restriction.filter(representation),
                );
                let vortex_plan = filtered_physical_plan(
                    formulation,
                    &vortex,
                    restriction.filter(representation),
                );
                assert_eq!(
                    operators(&parquet_plan),
                    operators(&vortex_plan),
                    "{representation:?} {formulation:?} {restriction:?}:\n{}\n{}",
                    displayed(&parquet_plan),
                    displayed(&vortex_plan),
                );
            }
        }
    }
}

/// The reference combiner's formulations among the shared list.
const REFERENCE_FORMULATIONS: [Formulation; 2] = [FORMULATIONS[0], FORMULATIONS[1]];

/// The reference combiner has no parallel operators between its unions and its merges, so a
/// filter leaves the operators of each of its formulations exactly as they are without one.
#[test]
fn filtering_the_reference_combiner_leaves_its_operators_unchanged() {
    for formulation in REFERENCE_FORMULATIONS {
        assert!(
            !matches!(formulation, Formulation::CombineAllelesUnion),
            "{formulation:?} is not a reference combiner formulation"
        );
    }
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            for formulation in REFERENCE_FORMULATIONS {
                let unfiltered = super::physical_plan(formulation, &dataset);
                for restriction in LocusRestriction::ALL {
                    let filtered = filtered_physical_plan(
                        formulation,
                        &dataset,
                        restriction.filter(representation),
                    );
                    assert_eq!(
                        operators(&filtered),
                        operators(&unfiltered),
                        "{format:?} {representation:?} {formulation:?} {restriction:?}:\n{}\n{}",
                        displayed(&filtered),
                        displayed(&unfiltered),
                    );
                }
            }
        }
    }
}

#[test]
fn a_contig_restriction_selects_its_files_and_returns_its_rows_from_every_formulation() {
    assert_selects_files_and_returns_rows(LocusRestriction::Contig);
}

#[test]
fn a_locus_interval_restriction_selects_its_files_and_returns_its_rows_from_every_formulation() {
    assert_selects_files_and_returns_rows(LocusRestriction::LocusInterval);
}

/// Runs every formulation under `restriction` in both formats and representations. Each sample's
/// scan keeps only the files that can hold a matching row, and the collected rows are the
/// restriction's rows: once per sample from the reference combiner, in locus order across
/// samples, and once in all from the allele combiner.
fn assert_selects_files_and_returns_rows(restriction: LocusRestriction) {
    let expected_rows = restriction.expected_rows();
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            for formulation in FORMULATIONS {
                let context =
                    format!("{format:?} {representation:?} {formulation:?} {restriction:?}");
                let (plan, batches) = run_filtered(
                    formulation,
                    dataset(format, representation),
                    restriction.filter(representation),
                );
                assert_filter_stays_inside_the_scans(
                    &plan,
                    &expected_groups(formulation, SAMPLES.len()),
                );
                assert_filter_reaches_every_scan(&plan, SAMPLES.len(), representation);
                assert_eq!(
                    scanned_stems(&plan),
                    vec![restriction.expected_stems().to_vec(); SAMPLES.len()],
                    "{context}:\n{}",
                    displayed(&plan),
                );

                let rows: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| rows(batch, representation))
                    .collect();
                match formulation {
                    Formulation::CombineRefsUnion | Formulation::CombineRefsGroupedMerge { .. } => {
                        let samples: Vec<String> = batches
                            .iter()
                            .flat_map(|batch| fixture::string_column(batch, "s"))
                            .collect();
                        assert_eq!(rows.len(), samples.len(), "{context}");
                        assert_eq!(
                            rows.len(),
                            expected_rows.len().checked_mul(SAMPLES.len()).unwrap(),
                            "{context}: {rows:?}"
                        );
                        let loci: Vec<_> = rows
                            .iter()
                            .map(|(contig, position, _)| (contig, position))
                            .collect();
                        assert!(
                            loci.is_sorted(),
                            "{context}: rows are not in locus order: {loci:?}"
                        );
                        for sample in SAMPLES {
                            let sample_rows: Vec<_> = rows
                                .iter()
                                .zip(&samples)
                                .filter(|(_, s)| s == sample)
                                .map(|(row, _)| row.clone())
                                .collect();
                            assert_eq!(sample_rows, expected_rows, "{context}: rows of {sample}");
                        }
                    }
                    Formulation::CombineAllelesUnion => {
                        assert_eq!(rows, expected_rows, "{context}");
                    }
                }
            }
        }
    }
}

/// Builds `formulation` over `dataset` and applies `filter` above it, as a caller would, before
/// the run's sink.
async fn plan_filtered(
    ctx: &SessionContext,
    formulation: Formulation,
    dataset: &FixtureDataset,
    filter: Expr,
) -> Result<DataFrame> {
    dataset.fixture.register(ctx);
    formulation
        .plan(ctx, &dataset.dataset)
        .await?
        .filter(filter)
}

/// Plans `formulation` under `filter` on a hostile session, through a sink, without executing it.
fn filtered_physical_plan(
    formulation: Formulation,
    dataset: &FixtureDataset,
    filter: Expr,
) -> Arc<dyn ExecutionPlan> {
    block_on(async {
        let ctx = SessionContext::new_with_config(hostile_config(8));
        let frame = plan_filtered(&ctx, formulation, dataset, filter)
            .await
            .unwrap();
        sink_plan(formulation, frame, dataset).await.unwrap()
    })
}

/// Plans `formulation` under `filter` on a hostile session and executes it through the pipeline
/// runner into a collecting sink, returning the plan alongside the rows the sink received.
fn run_filtered(
    formulation: Formulation,
    dataset: FixtureDataset,
    filter: Expr,
) -> (Arc<dyn ExecutionPlan>, Vec<RecordBatch>) {
    pipeline::run(
        move |_| async move {
            let ctx = SessionContext::new_with_config(hostile_config(8));
            let frame = plan_filtered(&ctx, formulation, &dataset, filter).await?;
            let (frame, collected) = sink::collect(frame, &ordering(formulation, &dataset))?;
            let plan = frame.create_physical_plan().await?;
            datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await?;
            Ok((plan, collected.take()))
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap()
}

/// The filter changed nothing between the scans and the merges: no filter operator, the merge
/// tree over `groups` with one ordered partition into every union input and no repartition
/// beneath any union, and no sort. Operators above the outermost union may repartition under the
/// hostile session's target partitions, as long as every repartition preserves its input order.
fn assert_filter_stays_inside_the_scans(plan: &Arc<dyn ExecutionPlan>, groups: &[usize]) {
    assert_merge_tree(plan, groups);
    assert!(
        nodes_of::<FilterExec>(plan).is_empty(),
        "expected the filter to be applied inside every scan, but the plan contains a FilterExec:\n{}",
        displayed(plan),
    );
    for union in nodes_of::<UnionExec>(plan) {
        for input in union.children() {
            assert!(
                nodes_of::<RepartitionExec>(input).is_empty(),
                "expected no repartition beneath a union:\n{}",
                displayed(plan),
            );
        }
    }
    for repartition in nodes_of::<RepartitionExec>(plan) {
        assert!(
            repartition.maintains_input_order().iter().all(|&kept| kept),
            "expected every repartition above the union to preserve its input order:\n{}",
            displayed(plan),
        );
    }
}

/// Every sample's scan displays a predicate over the representation's locus column. `EXPLAIN` is
/// the public observation of a pushed filter, and each format names it differently. That the
/// predicate is the whole filter is shown by the results: with no filter operator anywhere in
/// the plan, only the scans could have narrowed the rows to the restriction's rows.
fn assert_filter_reaches_every_scan(
    plan: &Arc<dyn ExecutionPlan>,
    n_samples: usize,
    representation: LocusRepresentation,
) {
    let locus_column = match representation {
        LocusRepresentation::ContigPosition => "contig",
        LocusRepresentation::Packed => "locus",
    };
    let scans = nodes_of::<DataSourceExec>(plan);
    assert_eq!(scans.len(), n_samples, "{}", displayed(plan));
    for scan in &scans {
        let text = displayed(scan);
        let predicate = text
            .split_once("predicate=")
            .or_else(|| text.split_once("predicate:"))
            .map(|(_, predicate)| predicate);
        assert!(
            predicate.is_some_and(|predicate| predicate.contains(locus_column)),
            "expected a filter on {locus_column} to reach the scan: {text}"
        );
    }
}

/// The file stems each scan displays, one scan per sample in union order.
fn scanned_stems(plan: &Arc<dyn ExecutionPlan>) -> Vec<Vec<&'static str>> {
    nodes_of::<DataSourceExec>(plan)
        .iter()
        .map(|scan| {
            let text = displayed(scan);
            let (_, group) = text
                .split_once("file_groups={1 group: [[")
                .unwrap_or_else(|| panic!("{text}"));
            let (paths, _) = group.split_once("]]").unwrap();
            paths.split(", ").map(stem).collect()
        })
        .collect()
}

/// The fixture file stem `path` names, as one of the fixture's own stems.
fn stem(path: &str) -> &'static str {
    let (stem, _) = path.rsplit('/').next().unwrap().split_once('.').unwrap();
    ["a", "b", "c", "d"]
        .into_iter()
        .find(|known| *known == stem)
        .unwrap_or_else(|| panic!("{path} is not a fixture file"))
}

/// Each row's contig, position, and alleles, in result order.
fn rows(batch: &RecordBatch, representation: LocusRepresentation) -> Vec<(String, i32, String)> {
    fixture::decode_loci(batch, representation)
        .into_iter()
        .zip(fixture::string_column(batch, "alleles"))
        .map(|((contig, position), alleles)| (contig, position, alleles))
        .collect()
}
