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
    FORMATS, FixtureDataset, REPRESENTATIONS, collected_batches, dataset, expected_groups,
    formulations, planned, sink_plan,
};
use crate::fixture::{self, FixtureFormat, SAMPLES, SampleRow, block_on};
use crate::formulation::Formulation;
use crate::locus::{Locus, LocusInterval, LocusRepresentation};
use crate::ordered_frame::OrderedFrame;
use crate::tests::{plan_shape::PlanShape, support::hostile_config};

use datafusion::{logical_expr::Expr, physical_plan::ExecutionPlan};

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
    const CONTIG: &'static str = "chr02";
    const INTERVAL_CONTIG: &'static str = "chr01";
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
    fn keeps(self, (locus, _): SampleRow) -> bool {
        match self {
            Self::Contig => locus.contig_name() == Self::CONTIG,
            Self::LocusInterval => {
                locus.contig_name() == Self::INTERVAL_CONTIG
                    && Self::INTERVAL.contains(&locus.position())
            }
        }
    }

    /// The files of one sample whose ordering statistics admit a matching row, in locus order.
    /// Files d and c are constant on chr01, b spans the contig boundary, and a is constant on chr02.
    /// File c holds chr01:3 and b starts at chr01:4.
    const fn expected_stems(self) -> [&'static str; 2] {
        match self {
            Self::Contig => ["b", "a"],
            Self::LocusInterval => ["c", "b"],
        }
    }

    /// One sample's rows that satisfy the restriction, in locus-then-alleles order.
    fn expected_rows(self) -> Vec<(Locus, String)> {
        fixture::sample_rows()
            .into_iter()
            .filter(|&row| self.keeps(row))
            .map(|(locus, alleles)| (locus, alleles.to_string()))
            .collect()
    }
}

#[test]
fn filtered_plans_keep_the_filter_inside_the_scans_in_both_formats_and_representations() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            for formulation in &formulations() {
                for restriction in LocusRestriction::ALL {
                    let plan = filtered_physical_plan(
                        formulation,
                        &dataset,
                        restriction.filter(representation),
                    );
                    let shape = PlanShape::of(&plan);
                    shape.assert_filter_stays_inside_the_scans(&expected_groups(
                        formulation,
                        SAMPLES.len(),
                    ));
                    shape.assert_filter_reaches_every_scan(SAMPLES.len(), representation);
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
        for formulation in &formulations() {
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
                let parquet_shape = PlanShape::of(&parquet_plan);
                let vortex_shape = PlanShape::of(&vortex_plan);
                assert_eq!(
                    parquet_shape.operators(),
                    vortex_shape.operators(),
                    "{representation:?} {formulation:?} {restriction:?}:\n{parquet_shape}\n{vortex_shape}",
                );
            }
        }
    }
}

/// The reference combiner's formulations among the shared list.
fn reference_formulations() -> [Formulation; 2] {
    let [union, grouped_merge, _] = formulations();
    [union, grouped_merge]
}

/// The reference combiner has no parallel operators between its unions and its merges, so a
/// filter leaves the operators of each of its formulations exactly as they are without one.
#[test]
fn filtering_the_reference_combiner_leaves_its_operators_unchanged() {
    for formulation in &reference_formulations() {
        assert!(
            !matches!(formulation, Formulation::CombineAllelesUnion),
            "{formulation:?} is not a reference combiner formulation"
        );
    }
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let dataset = dataset(format, representation);
            for formulation in &reference_formulations() {
                let unfiltered = super::physical_plan(formulation, &dataset);
                for restriction in LocusRestriction::ALL {
                    let filtered = filtered_physical_plan(
                        formulation,
                        &dataset,
                        restriction.filter(representation),
                    );
                    let filtered_shape = PlanShape::of(&filtered);
                    let unfiltered_shape = PlanShape::of(&unfiltered);
                    assert_eq!(
                        filtered_shape.operators(),
                        unfiltered_shape.operators(),
                        "{format:?} {representation:?} {formulation:?} {restriction:?}:\n{filtered_shape}\n{unfiltered_shape}",
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
            for formulation in &formulations() {
                let context =
                    format!("{format:?} {representation:?} {formulation:?} {restriction:?}");
                let filter = restriction.filter(representation);
                let (plan, batches) =
                    collected_batches(formulation, dataset(format, representation), move |frame| {
                        frame.filter(filter)
                    });
                let shape = PlanShape::of(&plan);
                shape.assert_filter_stays_inside_the_scans(&expected_groups(
                    formulation,
                    SAMPLES.len(),
                ));
                shape.assert_filter_reaches_every_scan(SAMPLES.len(), representation);
                let scanned_files = shape.scanned_files();
                let scanned_stems: Vec<_> = scanned_files
                    .iter()
                    .map(|paths| fixture::file_stems(paths))
                    .collect();
                assert_eq!(
                    scanned_stems,
                    vec![restriction.expected_stems().to_vec(); SAMPLES.len()],
                    "{context}:\n{shape}",
                );

                match formulation {
                    Formulation::CombineRefsUnion
                    | Formulation::CombineRefsGroupedMerge { .. }
                    | Formulation::CombineRefsIntervalMerge { .. } => {
                        let rows: Vec<fixture::Row> = batches
                            .iter()
                            .flat_map(|batch| fixture::decode_rows(batch, representation))
                            .collect();
                        assert_eq!(
                            rows.len(),
                            expected_rows.len().checked_mul(SAMPLES.len()).unwrap(),
                            "{context}: {rows:?}"
                        );
                        let loci: Vec<_> = rows.iter().map(|(locus, _, _)| *locus).collect();
                        assert!(
                            loci.is_sorted(),
                            "{context}: rows are not in locus order: {loci:?}"
                        );
                        for sample in SAMPLES {
                            let sample_rows: Vec<_> = rows
                                .iter()
                                .filter(|(_, _, row_sample)| row_sample == sample)
                                .map(|(locus, alleles, _)| (*locus, alleles.clone()))
                                .collect();
                            assert_eq!(sample_rows, expected_rows, "{context}: rows of {sample}");
                        }
                    }
                    Formulation::CombineAllelesUnion => {
                        let rows: Vec<_> = batches
                            .iter()
                            .flat_map(|batch| {
                                fixture::decode_loci(batch, representation)
                                    .into_iter()
                                    .zip(fixture::string_column(batch, "alleles"))
                            })
                            .collect();
                        assert_eq!(rows, expected_rows, "{context}");
                    }
                }
            }
        }
    }
}

/// Plans `formulation` under `filter` on a hostile session, through a sink, without executing it.
fn filtered_physical_plan(
    formulation: &Formulation,
    dataset: &FixtureDataset,
    filter: Expr,
) -> Arc<dyn ExecutionPlan> {
    block_on(async {
        let (_, ordered) = planned(formulation, dataset, hostile_config(8))
            .await
            .unwrap();
        let OrderedFrame {
            frame,
            ordering,
            layout,
        } = ordered;
        let ordered = OrderedFrame {
            frame: frame.filter(filter).unwrap(),
            ordering,
            layout,
        };
        sink_plan(ordered).await.unwrap()
    })
}
