//! PROTOTYPE for wayfinder ticket #137. Throwaway; not for main.
//!
//! Does a half-open locus interval predicate that crosses a contig boundary prune files and push
//! down exactly in both formats and both representations? Under contig-position the interval is
//! the compound
//!
//! ```text
//! (contig > c1 OR (contig = c1 AND position >= p1))
//! AND (contig < c2 OR (contig = c2 AND position < p2))
//! ```
//!
//! with either conjunct dropped when its bound is absent. Under packed it is a plain range on
//! `locus`. Prints every plan under `--nocapture` so the residual each format displays can be
//! read off.

use super::{
    assert_filter_reaches_every_scan, assert_filter_stays_inside_the_scans, filtered_physical_plan,
    operators, rows, run_filtered, scanned_stems,
};
use super::super::{FORMATS, FORMULATIONS, REPRESENTATIONS, dataset, displayed};
use crate::fixture::{self, FixtureFormat, SAMPLES, SampleRow};
use crate::formulation::Formulation;
use crate::locus::LocusRepresentation;

use datafusion::{
    logical_expr::Expr,
    prelude::{col, lit},
};

type Point = (&'static str, i32);

/// A half-open locus interval: includes `start`, excludes `end`; `None` is unbounded.
#[derive(Clone, Copy, Debug)]
struct HalfOpen {
    name: &'static str,
    start: Option<Point>,
    end: Option<Point>,
    /// Files whose ordering statistics admit a row of the interval, in locus order.
    /// d = chr1:1..2, c = chr1:2..3, b = chr1:4..chr2:1, a = chr2:2..3.
    expected_stems: &'static [&'static str],
}

const CASES: [HalfOpen; 6] = [
    HalfOpen {
        name: "unbounded above past a straddling file's rows [chr2:2, ..)",
        start: Some(("chr2", 2)),
        end: None,
        expected_stems: &["a"],
    },
    HalfOpen {
        name: "cross-contig [chr1:3, chr2:2)",
        start: Some(("chr1", 3)),
        end: Some(("chr2", 2)),
        expected_stems: &["c", "b"],
    },
    HalfOpen {
        name: "unbounded below [.., chr1:3)",
        start: None,
        end: Some(("chr1", 3)),
        expected_stems: &["d", "c"],
    },
    HalfOpen {
        name: "unbounded above [chr2:1, ..)",
        start: Some(("chr2", 1)),
        end: None,
        expected_stems: &["b", "a"],
    },
    HalfOpen {
        name: "ends at a contig start [chr1:4, chr2:1)",
        start: Some(("chr1", 4)),
        end: Some(("chr2", 1)),
        expected_stems: &["b"],
    },
    HalfOpen {
        name: "starts at a contig start [chr2:1, chr2:3)",
        start: Some(("chr2", 1)),
        end: Some(("chr2", 3)),
        expected_stems: &["b", "a"],
    },
];

fn packed(contig: &str, position: i32) -> i64 {
    let ordinal = contig.strip_prefix("chr").unwrap().parse::<i64>().unwrap();
    (ordinal << 32) | i64::from(position)
}

impl HalfOpen {
    fn filter(self, representation: LocusRepresentation) -> Expr {
        let lower = self.start.map(|(c, p)| match representation {
            LocusRepresentation::ContigPosition => col("contig")
                .gt(lit(c))
                .or(col("contig").eq(lit(c)).and(col("position").gt_eq(lit(p)))),
            LocusRepresentation::Packed => col("locus").gt_eq(lit(packed(c, p))),
        });
        let upper = self.end.map(|(c, p)| match representation {
            LocusRepresentation::ContigPosition => col("contig")
                .lt(lit(c))
                .or(col("contig").eq(lit(c)).and(col("position").lt(lit(p)))),
            LocusRepresentation::Packed => col("locus").lt(lit(packed(c, p))),
        });
        match (lower, upper) {
            (Some(lower), Some(upper)) => lower.and(upper),
            (Some(one), None) | (None, Some(one)) => one,
            (None, None) => unreachable!("every case has a bound"),
        }
    }

    fn keeps(self, (contig, position, _): SampleRow) -> bool {
        let point = (contig, position);
        self.start.is_none_or(|start| point >= start) && self.end.is_none_or(|end| point < end)
    }

    fn expected_rows(self) -> Vec<(String, i32, String)> {
        fixture::sample_rows()
            .into_iter()
            .filter(|&row| self.keeps(row))
            .map(|(contig, position, alleles)| (contig.to_string(), position, alleles.to_string()))
            .collect()
    }
}

#[test]
fn prototype_cross_contig_intervals_prune_push_down_and_return_exact_rows() {
    let mut failures = Vec::new();
    for case in CASES {
        let expected_rows = case.expected_rows();
        assert!(!expected_rows.is_empty(), "{}: case selects nothing", case.name);
        for format in FORMATS {
            for representation in REPRESENTATIONS {
                for formulation in FORMULATIONS {
                    let context = format!("{format:?} {representation:?} {formulation:?} {}", case.name);
                    let (plan, batches) = run_filtered(
                        formulation,
                        dataset(format, representation),
                        case.filter(representation),
                    );
                    eprintln!("=== {context}\n{}", displayed(&plan));
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        assert_filter_stays_inside_the_scans(&plan, SAMPLES.len());
                        assert_filter_reaches_every_scan(&plan, SAMPLES.len(), representation);
                        let got: Vec<_> = batches
                            .iter()
                            .flat_map(|batch| rows(batch, representation))
                            .collect();
                        match formulation {
                            Formulation::CombineRefsUnion => {
                                let samples: Vec<String> = batches
                                    .iter()
                                    .flat_map(|batch| fixture::string_column(batch, "s"))
                                    .collect();
                                assert_eq!(got.len(), expected_rows.len() * SAMPLES.len(), "{context}: {got:?}");
                                let loci: Vec<_> = got.iter().map(|(c, p, _)| (c, p)).collect();
                                assert!(loci.is_sorted(), "{context}: not in locus order: {loci:?}");
                                for sample in SAMPLES {
                                    let sample_rows: Vec<_> = got
                                        .iter()
                                        .zip(&samples)
                                        .filter(|(_, s)| s == sample)
                                        .map(|(row, _)| row.clone())
                                        .collect();
                                    assert_eq!(sample_rows, expected_rows, "{context}: rows of {sample}");
                                }
                            }
                            Formulation::CombineAllelesUnion => {
                                assert_eq!(got, expected_rows, "{context}");
                            }
                        }
                        eprintln!("--- rows exact: {context}");
                        assert_eq!(
                            scanned_stems(&plan),
                            vec![case.expected_stems.to_vec(); SAMPLES.len()],
                            "{context}: pruned files"
                        );
                    }));
                    if let Err(payload) = result {
                        let message = payload
                            .downcast_ref::<String>()
                            .cloned()
                            .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
                            .unwrap_or_default();
                        eprintln!("!!! FAILED {context}\n{message}");
                        failures.push(context);
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "failures:\n{}", failures.join("\n"));
}

#[test]
fn prototype_parquet_and_vortex_have_the_same_operators_under_cross_contig_intervals() {
    for case in CASES {
        for representation in REPRESENTATIONS {
            let parquet = dataset(FixtureFormat::Parquet, representation);
            let vortex = dataset(FixtureFormat::Vortex, representation);
            for formulation in FORMULATIONS {
                let p = filtered_physical_plan(formulation, &parquet, case.filter(representation));
                let v = filtered_physical_plan(formulation, &vortex, case.filter(representation));
                assert_eq!(
                    operators(&p),
                    operators(&v),
                    "{representation:?} {formulation:?} {}:\n{}\n{}",
                    case.name,
                    displayed(&p),
                    displayed(&v),
                );
            }
        }
    }
}
