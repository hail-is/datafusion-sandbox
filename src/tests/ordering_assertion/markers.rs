//! The marker: a filter of `assert_ordered` that `hold` replaces with an assertion where the plan
//! wrote it.

use super::{
    ascending, assertion_failure, dataset, flat_read, held_plan, key_order, planned_then_executed,
    sessions,
};
use crate::{
    locus::LocusRepresentation,
    ordering_assertion::{self, AssertionScope, OrderingAssertionFailed},
    pipeline::{self, PipelineOptions},
    row_ordering::SortColumn,
    testing::{
        assertion_marker::{asserted, marker, wheel_literal},
        fixture::{self, FixtureFormat, block_on},
        formulation_plans::FORMATS,
        plan_shape::PlanShape,
        session::hostile_config,
    },
};

use datafusion::logical_expr::expr_fn::scalar_subquery;
use datafusion::{
    common::ScalarValue,
    error::DataFusionError,
    functions_aggregate::expr_fn::max,
    logical_expr::Expr,
    physical_plan::sorts::sort::SortExec,
    prelude::{col, lit},
};
use std::sync::Arc;

/// A marker over a union of ordered scans holds the union where the plan wrote it: the merge tree
/// sits beneath it, with a projection between it and the root, and no sort, under either session.
#[test]
fn a_mid_plan_marker_holds_a_merge_tree_beneath_it_without_a_sort() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        // The marker holds alleles too, so the projection dropping it stays above the marker.
        let kept = &ordering[..ordering.len() - 1];
        let kept_columns: Vec<_> = kept.iter().map(|sort| col(sort.column.as_str())).collect();
        for (session, config) in sessions() {
            let plan = block_on(async {
                let frame = asserted(flat_read(&dataset, config).await, &ordering)
                    .select(kept_columns.clone())
                    .unwrap();
                held_plan(frame, kept).await.unwrap()
            });

            let shape = PlanShape::of(&plan);
            let operators = shape.operators();
            let tree = [
                "OrderingAssertionExec",
                "  ProjectionExec",
                "    OrderingAssertionExec",
                "      SortPreservingMergeExec",
                "        UnionExec",
            ];
            assert_eq!(
                operators[..tree.len()],
                tree,
                "{format:?}, {session}:\n{shape}"
            );
            shape.assert_no_sorts();
        }
    }
}

/// A marker of an order the rows lack fails before execution, with the same failure as the root
/// assertion's, whatever the root asserts above it.
#[test]
fn a_marker_of_an_order_the_data_lacks_fails_before_execution() {
    for format in FORMATS {
        let dataset = dataset(format);
        let by_sample = [ascending("s")];
        for (session, config) in sessions() {
            let frame = asserted(block_on(flat_read(&dataset, config)), &by_sample);

            let failure = assertion_failure(frame, &by_sample);
            assert!(
                failure.contains("by sorting"),
                "{format:?}, {session}: {failure}"
            );
            assert!(failure.contains("s@"), "{format:?}, {session}: {failure}");
        }
    }
}

/// A marker over a key the plan computes fails before execution: nothing states the computed
/// column's order, so only a sort could meet the marker.
#[test]
fn a_marker_on_a_computed_key_fails_before_execution() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        for (session, config) in sessions() {
            let computed = block_on(flat_read(&dataset, config))
                .select(vec![
                    col("contig"),
                    (col("position") + lit(1)).alias("position"),
                    col("alleles"),
                ])
                .unwrap();

            let failure = assertion_failure(asserted(computed, &ordering), &ordering);
            assert!(
                failure.contains("by sorting"),
                "{format:?}, {session}: {failure}"
            );
        }
    }
}

/// A marker above a frontend sort into the order the scans already have holds it, and keeps the
/// merge tree the sort becomes.
#[test]
fn a_marker_above_a_sort_into_the_scans_order_keeps_its_merge_tree() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        let sorts: Vec<_> = ordering.iter().map(SortColumn::sort_expression).collect();
        for (session, config) in sessions() {
            let plan = block_on(async {
                let sorted = flat_read(&dataset, config)
                    .await
                    .sort(sorts.clone())
                    .unwrap();
                held_plan(asserted(sorted, &ordering), &ordering)
                    .await
                    .unwrap()
            });

            let shape = PlanShape::of(&plan);
            let operators = shape.operators();
            let tree = [
                "OrderingAssertionExec",
                "  OrderingAssertionExec",
                "    EstablishedOrderExec",
                "      SortPreservingMergeExec",
            ];
            assert_eq!(
                operators[..tree.len()],
                tree,
                "{format:?}, {session}:\n{shape}"
            );
            shape.assert_no_sorts();
        }
    }
}

/// A marker above a frontend sort into an order the rows lack holds too: the sort beneath it is the
/// one the plan wrote.
#[test]
fn a_marker_above_a_sort_the_plan_wrote_accepts_that_sort() {
    for format in FORMATS {
        let dataset = dataset(format);
        let mut ordering = vec![ascending("s")];
        ordering.extend(key_order(&dataset));
        let sorts: Vec<_> = ordering.iter().map(SortColumn::sort_expression).collect();
        for (session, config) in sessions() {
            let plan = block_on(async {
                let sorted = flat_read(&dataset, config)
                    .await
                    .sort(sorts.clone())
                    .unwrap();
                held_plan(asserted(sorted, &ordering), &ordering)
                    .await
                    .unwrap()
            });

            let shape = PlanShape::of(&plan);
            assert!(
                !shape.nodes_of::<SortExec>().is_empty(),
                "{format:?}, {session}: expected the plan's sort:\n{shape}"
            );
        }
    }
}

/// Each direction a marker states reads as the sort options it names, shown by the order a failed
/// assertion names.
#[test]
fn a_marker_holds_the_direction_it_states() {
    let dataset = dataset(FixtureFormat::Parquet);
    let cases = [
        ((true, false), "s@3 ASC NULLS LAST"),
        ((true, true), "s@3 ASC]"),
        ((false, false), "s@3 DESC NULLS LAST"),
        ((false, true), "s@3 DESC]"),
    ];
    for ((ascending, nulls_first), named) in cases {
        let by_sample = [SortColumn {
            column: "s".to_string(),
            ascending,
            nulls_first,
        }];
        let frame = asserted(
            block_on(flat_read(&dataset, pipeline::session_config())),
            &by_sample,
        );

        let failure = assertion_failure(frame, &by_sample);
        assert!(failure.contains(named), "{named}: {failure}");
    }
}

/// A marker's rows run through unchanged, in the order it asserts, with the run-time check on.
#[test]
fn a_held_marker_passes_every_row_on_in_its_order() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        let batches = pipeline::run(
            move |_| async move {
                let frame = flat_read(
                    &dataset,
                    ordering_assertion::verify_order(hostile_config(8)),
                )
                .await;
                let expected = dataset.input_tables.len();
                let batches =
                    planned_then_executed(asserted(frame, &ordering), &ordering).await??;
                Ok((batches, expected))
            },
            PipelineOptions::single_threaded(),
        )
        .unwrap();

        let (batches, input_tables) = batches;
        let loci: Vec<_> = batches
            .iter()
            .flat_map(|batch| fixture::decode_loci(batch, LocusRepresentation::ContigPosition))
            .collect();
        let mut sorted = loci.clone();
        sorted.sort();
        assert_eq!(loci, sorted, "{format:?}");
        assert_eq!(
            loci.len(),
            input_tables * fixture::sample_rows().len(),
            "{format:?}"
        );
    }
}

/// Every marker `hold` can't place fails with the same planning error `check_markers` gives,
/// naming what is wrong, rather than leaving the marker to hold nothing.
#[test]
fn a_marker_that_is_not_a_filter_s_whole_valid_predicate_is_refused() {
    let udf = ordering_assertion::assert_ordered_udf;
    let contig_ascending = || vec![col("contig"), wheel_literal("ASC NULLS LAST")];

    let refused: Vec<(&str, Expr, &str)> = vec![
        (
            "part of a larger predicate",
            udf()
                .call(contig_ascending())
                .and(col("position").gt(lit(0))),
            "whole predicate",
        ),
        ("no arguments", udf().call(vec![]), "pairs"),
        (
            "a key with no direction",
            udf().call(vec![col("contig")]),
            "pairs",
        ),
        (
            "a last key with no direction",
            udf().call(vec![
                col("contig"),
                wheel_literal("ASC NULLS LAST"),
                col("position"),
            ]),
            "pairs",
        ),
        (
            "a computed key",
            udf().call(vec![
                col("position") + lit(1),
                wheel_literal("ASC NULLS LAST"),
            ]),
            "key columns",
        ),
        (
            "an unknown direction",
            udf().call(vec![col("contig"), wheel_literal("UP")]),
            "direction",
        ),
        (
            "a direction that is no literal",
            udf().call(vec![col("contig"), col("alleles")]),
            "direction",
        ),
        (
            "a null direction",
            udf().call(vec![
                col("contig"),
                Expr::Literal(ScalarValue::Utf8(None), None),
            ]),
            "direction",
        ),
    ];
    let dataset = dataset(FixtureFormat::Parquet);
    let ordering = key_order(&dataset);
    for (case, predicate, message) in refused {
        let frame = block_on(flat_read(&dataset, pipeline::session_config()))
            .filter(predicate)
            .unwrap();

        let error = ordering_assertion::check_markers(frame.logical_plan()).unwrap_err();
        assert!(matches!(error, DataFusionError::Plan(_)), "{case}: {error}");
        assert!(error.to_string().contains(message), "{case}: {error}");
        let held = ordering_assertion::hold(frame, &ordering, AssertionScope::OnePartition);
        assert!(
            matches!(held, Err(DataFusionError::Plan(_))),
            "{case}: hold should refuse it too"
        );
    }
}

/// A marker that isn't a filter's predicate is refused wherever the plan holds it.
#[test]
fn a_marker_in_a_projection_is_refused() {
    let dataset = dataset(FixtureFormat::Parquet);
    let ordering = key_order(&dataset);
    let frame = block_on(flat_read(&dataset, pipeline::session_config()))
        .select(vec![col("contig"), marker(&ordering).alias("kept")])
        .unwrap();

    let error = ordering_assertion::check_markers(frame.logical_plan()).unwrap_err();
    assert!(error.to_string().contains("whole predicate"), "{error}");
}

/// A marker in a scalar subquery holds where the plan wrote it, as one anywhere else does.
#[test]
fn a_marker_in_a_subquery_is_held_where_the_plan_wrote_it() {
    for format in FORMATS {
        let dataset = dataset(format);
        let ordering = key_order(&dataset);
        let plan = block_on(async {
            let outer = flat_read(&dataset, pipeline::session_config()).await;
            let inner = asserted(
                flat_read(&dataset, pipeline::session_config()).await,
                &ordering,
            )
            .aggregate(vec![], vec![max(col("position")).alias("largest")])
            .unwrap()
            .into_unoptimized_plan();
            let frame = outer
                .filter(col("position").lt_eq(scalar_subquery(Arc::new(inner))))
                .unwrap();
            ordering_assertion::check_markers(frame.logical_plan()).unwrap();
            held_plan(frame, &ordering).await.unwrap()
        });

        let shape = PlanShape::of(&plan);
        let assertions = shape
            .operators()
            .iter()
            .filter(|operator| operator.trim() == "OrderingAssertionExec")
            .count();
        assert_eq!(assertions, 2, "{format:?}:\n{shape}");
    }
}

/// A marker that isn't a filter's predicate is refused inside a subquery too.
#[test]
fn a_misplaced_marker_in_a_subquery_is_refused() {
    let dataset = dataset(FixtureFormat::Parquet);
    let ordering = key_order(&dataset);
    let frame = block_on(async {
        let outer = flat_read(&dataset, pipeline::session_config()).await;
        let inner = flat_read(&dataset, pipeline::session_config())
            .await
            .filter(marker(&ordering).and(col("position").gt(lit(0))))
            .unwrap()
            .aggregate(vec![], vec![max(col("position")).alias("largest")])
            .unwrap()
            .into_unoptimized_plan();
        outer
            .filter(col("position").lt_eq(scalar_subquery(Arc::new(inner))))
            .unwrap()
    });

    let error = ordering_assertion::check_markers(frame.logical_plan()).unwrap_err();
    assert!(error.to_string().contains("whole predicate"), "{error}");
}

/// A marker has nothing to compute: outside `hold`, evaluating it fails rather than pass rows the
/// assertion didn't check.
#[test]
fn evaluating_a_marker_fails() {
    let dataset = dataset(FixtureFormat::Parquet);
    let ordering = key_order(&dataset);
    let result = pipeline::run(
        move |_| async move {
            let frame = flat_read(&dataset, pipeline::session_config()).await;
            frame
                .select(vec![marker(&ordering).alias("kept")])?
                .collect()
                .await
        },
        PipelineOptions::single_threaded(),
    );

    let error = result.unwrap_err();
    assert!(error.to_string().contains("computes nothing"), "{error}");
}

/// Expected failure while the gap in the guard apache/datafusion#11678 added remains, not yet
/// reported upstream: a marker of (position, contig, alleles) beneath a root asserting (contig,
/// position, alleles). The sort the backend inserts should fail the root assertion, but the two
/// orders agree on alleles, so `pushdown_sorts` pushes it beneath the marker's assertion, and
/// `SanityCheckPlan` rejects the plan with its own error first. The plan still fails before
/// execution. Tracked in issue #325.
#[test]
fn expected_failure_a_sort_pushed_beneath_a_marker_fails_unclearly() {
    let dataset = dataset(FixtureFormat::Parquet);
    let ordering = key_order(&dataset);
    let position_first = ["position", "contig", "alleles"].map(ascending);
    let frame = asserted(
        block_on(flat_read(&dataset, pipeline::session_config())),
        &position_first,
    );

    let result = block_on(held_plan(frame, &ordering));

    let unfixed = matches!(
        &result,
        Err(error) if OrderingAssertionFailed::find(error).is_none()
            && error.to_string().contains("does not satisfy order requirements")
    );
    assert!(
        unfixed,
        "the sort pushdown past a marker's assertion no longer reproduces: remove this test, \
         check its box on issue #325, and expect a failed ordering assertion here instead. \
         Result: {result:?}"
    );
}
