//! Ordering assertions in the frontend's plan: plan bytes carrying `assert_ordered` markers, built
//! in Rust with the backend's codec, framed as the wheel frames them, started with an Action and
//! waited for. Every run checks its rows' order at run time.

use super::{
    FORMATS, backend_over, collected_rows, expected_keys, hostile, keys, open, wheel_bytes,
};
use crate::{
    backend::{Action, ErrorCode, Outcome, Settings, Versions},
    locus::LocusRepresentation,
    row_ordering::{RowOrdering, SortColumn},
    testing::{
        assertion_marker::{marker, wheel_literal},
        fixture::{self, DatasetFixture, FixtureFormat},
    },
};

use datafusion::{
    catalog::TableProvider,
    logical_expr::{Expr, LogicalPlanBuilder},
    prelude::{SessionContext, col, lit},
};
use std::sync::Arc;

/// The union of every input table's scan of `fixture`, opened in locus-then-alleles order, as a
/// plan builder to go on from, and that ordering.
fn union_of_scans(fixture: &'static Arc<DatasetFixture>) -> (LogicalPlanBuilder, Vec<SortColumn>) {
    let opened = open(fixture, RowOrdering::locus_then_alleles());
    let ctx = SessionContext::new();
    let mut scans = opened.input_tables.iter().map(|table| {
        let provider: Arc<dyn TableProvider> = table.scan.clone();
        ctx.read_table(provider).unwrap().into_unoptimized_plan()
    });
    let first = LogicalPlanBuilder::from(scans.next().unwrap());
    let union = scans.fold(first, |union, scan| union.union(scan).unwrap());
    (union, opened.ordering)
}

fn fixture(format: FixtureFormat) -> &'static Arc<DatasetFixture> {
    fixture::dataset_fixture(format, LocusRepresentation::ContigPosition)
}

fn ascending(column: &str) -> SortColumn {
    SortColumn {
        column: column.to_string(),
        ascending: true,
        nulls_first: false,
    }
}

/// The section of `plan`'s rendering named `name`, without the table's padding.
fn section(plan: &str, name: &str) -> String {
    let lines = plan
        .lines()
        .skip_while(|line| !line.starts_with(&format!("| {name}")));
    let mut section = Vec::new();
    for (index, line) in lines.enumerate() {
        let is_next_section = index > 0 && !line.starts_with("|  ");
        if is_next_section {
            break;
        }
        section.push(line);
    }
    section.join("\n")
}

/// What explaining `bytes` in `ordering` under `settings` renders.
fn explained(
    fixture: &'static Arc<DatasetFixture>,
    bytes: &[u8],
    ordering: Vec<SortColumn>,
    settings: &Settings,
) -> String {
    let outcome = backend_over(fixture)
        .start(
            bytes,
            Action::Explain {
                ordering,
                write: None,
            },
            settings,
            None,
            Versions::backend(),
        )
        .unwrap()
        .wait()
        .unwrap();
    let Outcome::Plan(text) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    text
}

/// The marker crosses plan bytes by name and decodes as written, a filter over the union, and the
/// ordering pass replaces it in the plan the optimizer gets.
#[test]
fn a_marker_survives_plan_bytes_and_decoding_as_written() {
    for format in FORMATS {
        let fixture = fixture(format);
        let (union, ordering) = union_of_scans(fixture);
        let plan = union.filter(marker(&ordering)).unwrap().build().unwrap();

        let plan_text = explained(fixture, &wheel_bytes(&plan), ordering, &Settings::new());

        let frontend = section(&plan_text, "frontend_plan");
        assert!(
            frontend.contains("Filter: assert_ordered(") && frontend.contains("Union"),
            "{format:?}:\n{frontend}"
        );
        assert!(
            frontend.contains(r#"Utf8View("ASC NULLS LAST")"#),
            "{format:?}:\n{frontend}"
        );
        let logical = section(&plan_text, "logical_plan");
        assert!(
            !logical.contains("assert_ordered"),
            "{format:?}:\n{logical}"
        );
        assert!(
            logical.contains("OrderingAssertion"),
            "{format:?}:\n{logical}"
        );
    }
}

/// A marker mid-plan holds a merge tree beneath it, with no sort, under the default and hostile
/// settings, and collects every row in its order.
#[test]
fn a_mid_plan_marker_holds_a_merge_tree_with_no_sort_under_any_settings() {
    for format in FORMATS {
        let fixture = fixture(format);
        let (union, ordering) = union_of_scans(fixture);
        let kept = &ordering[..ordering.len() - 1];
        let kept_columns: Vec<_> = kept.iter().map(|sort| col(sort.column.as_str())).collect();
        let plan = union
            .filter(marker(&ordering))
            .unwrap()
            .project(kept_columns)
            .unwrap()
            .build()
            .unwrap();
        let bytes = wheel_bytes(&plan);

        for settings in [Settings::new(), hostile()] {
            let plan_text = explained(fixture, &bytes, kept.to_vec(), &settings);

            let physical = section(&plan_text, "physical_plan");
            let at = |operator: &str| {
                physical
                    .lines()
                    .position(|line| line.contains(operator))
                    .unwrap_or_else(|| panic!("{format:?}: no {operator}:\n{physical}"))
            };
            let merge = at("SortPreservingMergeExec");
            assert!(at("ProjectionExec") < merge, "{format:?}:\n{physical}");
            assert!(
                physical
                    .lines()
                    .filter(|line| line.contains("OrderingAssertionExec"))
                    .count()
                    == 2,
                "{format:?}:\n{physical}"
            );
            assert!(!physical.contains("SortExec"), "{format:?}:\n{physical}");
        }
    }
}

/// A marker's rows come back in its order under each session, every one of them.
#[test]
fn collecting_through_a_marker_returns_every_row_in_its_order() {
    for format in FORMATS {
        let fixture = fixture(format);
        let (union, ordering) = union_of_scans(fixture);
        let plan = union.filter(marker(&ordering)).unwrap().build().unwrap();
        let bytes = wheel_bytes(&plan);

        for settings in [Settings::new(), hostile()] {
            let outcome = backend_over(fixture)
                .start(
                    &bytes,
                    Action::Collect {
                        ordering: ordering.clone(),
                    },
                    &settings,
                    None,
                    Versions::backend(),
                )
                .unwrap()
                .wait()
                .unwrap();

            let rows = collected_rows(outcome, LocusRepresentation::ContigPosition);
            assert_eq!(keys(&rows), expected_keys(), "{format:?}");
        }
    }
}

/// A marker the rows can't meet without a sort fails with the ordering assertion's code, from
/// `wait`: on an order the data lacks, and on a key the plan computes, under either session. Each
/// fails explaining too, which never executes the plan.
#[test]
fn a_marker_only_a_sort_could_meet_fails_with_the_ordering_assertion_code_before_execution() {
    for format in FORMATS {
        let fixture = fixture(format);

        let (union, ordering) = union_of_scans(fixture);
        let position_first: Vec<_> = ["position", "contig", "alleles"].map(ascending).to_vec();
        let lacking = union
            .filter(marker(&position_first))
            .unwrap()
            .build()
            .unwrap();

        let (union, _) = union_of_scans(fixture);
        let computed = union
            .project(vec![
                col("contig"),
                (col("position") + lit(1)).alias("position"),
                col("alleles"),
                col("s"),
            ])
            .unwrap()
            .filter(marker(&ordering))
            .unwrap()
            .build()
            .unwrap();

        let cases = [
            (wheel_bytes(&lacking), position_first),
            (wheel_bytes(&computed), ordering),
        ];
        for (bytes, ordering) in cases {
            for settings in [Settings::new(), hostile()] {
                for action in [
                    Action::Collect {
                        ordering: ordering.clone(),
                    },
                    Action::Explain {
                        ordering: ordering.clone(),
                        write: None,
                    },
                ] {
                    let error = backend_over(fixture)
                        .start(&bytes, action, &settings, None, Versions::backend())
                        .unwrap()
                        .wait()
                        .unwrap_err();

                    assert_eq!(error.code(), ErrorCode::OrderingAssertionFailed, "{error}");
                    assert!(error.message().contains("by sorting"), "{error}");
                }
            }
        }
    }
}

/// A marker above a sort the frontend wrote holds, and keeps the merge tree that sort becomes.
#[test]
fn a_marker_above_a_frontend_sort_is_accepted() {
    for format in FORMATS {
        let fixture = fixture(format);
        let (union, ordering) = union_of_scans(fixture);
        let plan = union
            .sort(ordering.iter().map(SortColumn::sort_expression))
            .unwrap()
            .filter(marker(&ordering))
            .unwrap()
            .build()
            .unwrap();
        let bytes = wheel_bytes(&plan);

        for settings in [Settings::new(), hostile()] {
            let plan_text = explained(fixture, &bytes, ordering.clone(), &settings);

            let physical = section(&plan_text, "physical_plan");
            assert!(physical.contains("EstablishedOrderExec"), "{physical}");
            assert!(physical.contains("SortPreservingMergeExec"), "{physical}");
            assert!(!physical.contains("SortExec"), "{physical}");
        }
    }
}

/// A marker the ordering pass can't read fails the call that starts its run, as `InvalidAction`,
/// so no run begins to find out.
#[test]
fn starting_a_plan_with_a_misplaced_or_malformed_marker_fails_with_invalid_action() {
    let fixture = fixture(FixtureFormat::Parquet);
    let (_, ordering) = union_of_scans(fixture);
    let udf = crate::ordering_assertion::assert_ordered_udf;
    let predicates: Vec<(Expr, &str)> = vec![
        (
            marker(&ordering).and(col("position").gt(lit(0))),
            "whole predicate",
        ),
        (udf().call(vec![col("contig")]), "pairs"),
        (
            udf().call(vec![col("contig"), wheel_literal("UP")]),
            "direction",
        ),
    ];
    for (predicate, message) in predicates {
        let (union, ordering) = union_of_scans(fixture);
        let plan = union.filter(predicate).unwrap().build().unwrap();

        let started = backend_over(fixture).start(
            &wheel_bytes(&plan),
            Action::Collect { ordering },
            &Settings::new(),
            None,
            Versions::backend(),
        );

        let Err(error) = started else {
            panic!("start should refuse the marker");
        };
        assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
        assert!(error.message().contains(message), "{error}");
    }
}

/// Expected failure while the gap in the guard apache/datafusion#11678 added remains: a marker
/// contradicting the Action's ordering fails before execution, but with `SanityCheckPlan`'s error
/// rather than the ordering assertion's code. Tracked in issue #325.
#[test]
fn expected_failure_a_marker_contradicting_the_action_s_ordering_has_the_plain_code() {
    let fixture = fixture(FixtureFormat::Parquet);
    let (union, ordering) = union_of_scans(fixture);
    let position_first: Vec<_> = ["position", "contig", "alleles"].map(ascending).to_vec();
    let plan = union
        .filter(marker(&position_first))
        .unwrap()
        .build()
        .unwrap();

    let error = backend_over(fixture)
        .start(
            &wheel_bytes(&plan),
            Action::Collect { ordering },
            &Settings::new(),
            None,
            Versions::backend(),
        )
        .unwrap()
        .wait()
        .unwrap_err();

    assert!(
        error.code() == ErrorCode::Backend
            && error
                .message()
                .contains("does not satisfy order requirements"),
        "the sort pushdown past a marker's assertion no longer reproduces: remove this test, \
         check its box on issue #325, and expect a failed ordering assertion here instead. \
         Error: {error}"
    );
}
