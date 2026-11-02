//! Built-in plans: a Rust formulation's plan bytes, with every scan a scan description, started
//! through the same `start` as a frontend's.

use super::{
    FORMATS, NoStores, REPRESENTATIONS, backend_over, collected_rows, expected_keys, expected_rows,
    hostile, keys, open, sorted, sorted_union, wheel_bytes,
};
use crate::{
    backend::{Action, Backend, ErrorCode, Outcome, Settings, Versions},
    locus::LocusRepresentation,
    metrics_directory::FormulationRecord,
    row_ordering::{RowOrdering, SortColumn},
    testing::{
        fixture::{self, DatasetFixture, FixtureFormat},
        plan_shape::exec_names,
    },
};

use datafusion::{
    catalog::TableProvider,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::SessionContext,
};
use std::sync::Arc;

fn union() -> FormulationRecord {
    FormulationRecord {
        name: "union".to_string(),
        groups: None,
        split_points: None,
    }
}

fn grouped_merge(groups: usize) -> FormulationRecord {
    FormulationRecord {
        name: "grouped-merge".to_string(),
        groups: Some(groups),
        split_points: None,
    }
}

/// Each group count below, equal to and above the mixed fixture's three input tables, with the
/// sizes of the input groups it splits them into: contiguous, larger groups first, and clamped to
/// one input table per group.
const GROUP_SIZES: [(usize, &[usize]); 4] =
    [(1, &[3]), (2, &[2, 1]), (3, &[1, 1, 1]), (8, &[1, 1, 1])];

/// The plan bytes of a frontend's grouped-merge over `fixture`, opened under `row_ordering`: the
/// union of input groups of `sizes`, each its scans' union sorted by the stored ordering, in input
/// table order with no sort above them, as the wheel's left-nested unions write it.
fn sorted_groups(
    fixture: &'static Arc<DatasetFixture>,
    row_ordering: RowOrdering,
    sizes: &[usize],
) -> Vec<u8> {
    fn union_of(plans: impl IntoIterator<Item = LogicalPlan>) -> LogicalPlanBuilder {
        let mut plans = plans.into_iter();
        let first = LogicalPlanBuilder::from(plans.next().unwrap());
        plans.fold(first, |union, plan| union.union(plan).unwrap())
    }

    let opened = open(fixture, row_ordering);
    let ctx = SessionContext::new();
    let mut scans = opened.input_tables.iter().map(|table| {
        let provider: Arc<dyn TableProvider> = table.scan.clone();
        ctx.read_table(provider).unwrap().into_unoptimized_plan()
    });
    let groups = sizes
        .iter()
        .map(|size| {
            union_of(scans.by_ref().take(*size).collect::<Vec<_>>())
                .sort(opened.ordering.iter().map(SortColumn::sort_expression))
                .unwrap()
                .build()
                .unwrap()
        })
        .collect::<Vec<_>>();
    wheel_bytes(&union_of(groups).build().unwrap())
}

/// The physical plan section of `plan`'s text, each line stripped of the table's padding, which
/// the widest line of any section sets.
fn physical_plan(plan: &str) -> String {
    plan.lines()
        .skip_while(|line| !line.starts_with("| physical_plan"))
        .map(|line| line.trim_end_matches(['|', ' ']))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The Outcome of running `bytes` as `action` under `settings` on a backend over `fixture`.
fn run(
    fixture: &'static Arc<DatasetFixture>,
    bytes: &[u8],
    action: Action,
    settings: &Settings,
) -> Outcome {
    backend_over(fixture)
        .start(bytes, action, settings, None, Versions::backend())
        .unwrap()
        .wait()
        .unwrap()
}

/// The settings map of a one-partition session.
fn one_partition() -> Settings {
    Settings::from([("hail_df.target_partitions".to_string(), "1".to_string())])
}

/// Explains `builtin` and `frontend` under `settings` on a backend over `fixture`, and checks that
/// they plan alike and that the plan is a merge tree with no sort.
fn assert_explains_alike(
    fixture: &'static Arc<DatasetFixture>,
    builtin: &[u8],
    frontend: &[u8],
    ordering: &[SortColumn],
    settings: &Settings,
    context: &str,
) {
    let [builtin, frontend] = [builtin, frontend].map(|bytes| {
        let action = Action::Explain {
            ordering: ordering.to_vec(),
        };
        let outcome = run(fixture, bytes, action, settings);
        let Outcome::Plan(plan) = outcome else {
            panic!("expected a plan, got {outcome:?}");
        };
        plan
    });

    assert!(
        exec_names(&builtin).contains(&"SortPreservingMergeExec"),
        "{context}:\n{builtin}"
    );
    assert!(!exec_names(&builtin).contains(&"SortExec"), "{context}");
    assert_eq!(
        physical_plan(&builtin),
        physical_plan(&frontend),
        "{context}"
    );
}

/// The built-in union's plan bytes over `fixture` opened under `row_ordering`, and the ordering it
/// states.
fn builtin_union(
    fixture: &'static Arc<DatasetFixture>,
    row_ordering: RowOrdering,
) -> (Vec<u8>, Vec<SortColumn>) {
    let opened = open(fixture, row_ordering);
    let bytes = backend_over(fixture)
        .builtin_plan(&opened, &union(), Versions::backend())
        .unwrap();
    (bytes, opened.ordering)
}

#[test]
fn the_built_in_union_reads_every_input_table_as_its_scan_description() {
    for format in FORMATS {
        let fixture = fixture::mixed_dataset_fixture(format, LocusRepresentation::Packed);
        let opened = open(fixture, RowOrdering::locus_then_alleles());

        let backend = backend_over(fixture);
        let bytes = backend
            .builtin_plan(&opened, &union(), Versions::backend())
            .unwrap();

        assert_eq!(
            backend
                .scan_descriptions(&bytes, Versions::backend())
                .unwrap(),
            opened
                .input_tables
                .iter()
                .map(|table| table.scan.description().clone())
                .collect::<Vec<_>>(),
            "{format:?}"
        );
    }
}

#[test]
fn the_built_in_union_reads_only_the_input_tables_of_the_dataset_it_is_given() {
    let fixture =
        fixture::mixed_dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let mut opened = open(fixture, RowOrdering::locus());
    opened.input_tables.truncate(1);
    let backend = backend_over(fixture);

    let bytes = backend
        .builtin_plan(&opened, &union(), Versions::backend())
        .unwrap();

    assert_eq!(
        backend
            .scan_descriptions(&bytes, Versions::backend())
            .unwrap(),
        [opened.input_tables[0].scan.description().clone()]
    );
}

#[test]
fn collecting_the_built_in_union_returns_every_row_in_the_dataset_s_ordering() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let fixture = fixture::dataset_fixture(format, representation);
            let (bytes, ordering) = builtin_union(fixture, RowOrdering::locus_then_alleles());

            for settings in [Settings::new(), hostile()] {
                let outcome = run(
                    fixture,
                    &bytes,
                    Action::Collect {
                        ordering: ordering.clone(),
                    },
                    &settings,
                );

                let rows = collected_rows(outcome, representation);
                let context = format!("{format:?} {representation:?} {settings:?}");
                assert_eq!(keys(&rows), expected_keys(), "{context}");
                assert_eq!(sorted(rows), expected_rows(), "{context}");
            }
        }
    }
}

/// The union a frontend writes and the built-in one plan alike, under whatever row ordering the
/// dataset was opened with and whichever settings, and neither needs a sort.
#[test]
fn the_built_in_union_explains_as_the_union_a_frontend_writes() {
    for format in FORMATS {
        for row_ordering in [RowOrdering::locus(), RowOrdering::locus_then_alleles()] {
            let fixture = fixture::mixed_dataset_fixture(format, LocusRepresentation::Packed);
            let (builtin, ordering) = builtin_union(fixture, row_ordering.clone());
            let (frontend, frontend_ordering) = sorted_union(fixture, row_ordering);
            assert_eq!(ordering, frontend_ordering);

            for settings in [Settings::new(), one_partition(), hostile()] {
                let context = format!("{format:?} {settings:?}");
                assert_explains_alike(fixture, &builtin, &frontend, &ordering, &settings, &context);
            }
        }
    }
}

#[test]
fn the_built_in_grouped_merge_reads_every_input_table_as_its_scan_description() {
    let fixture =
        fixture::mixed_dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let opened = open(fixture, RowOrdering::locus());
    let backend = backend_over(fixture);

    for (groups, _) in GROUP_SIZES {
        let bytes = backend
            .builtin_plan(&opened, &grouped_merge(groups), Versions::backend())
            .unwrap();

        let described = backend
            .scan_descriptions(&bytes, Versions::backend())
            .unwrap();

        let expected = opened
            .input_tables
            .iter()
            .map(|table| table.scan.description().clone())
            .collect::<Vec<_>>();
        assert_eq!(described, expected, "{groups} groups");
    }
}

#[test]
fn collecting_the_built_in_grouped_merge_returns_every_row_in_the_dataset_s_ordering() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let fixture = fixture::dataset_fixture(format, representation);
            let opened = open(fixture, RowOrdering::locus_then_alleles());
            for (groups, _) in GROUP_SIZES {
                let bytes = backend_over(fixture)
                    .builtin_plan(&opened, &grouped_merge(groups), Versions::backend())
                    .unwrap();

                for settings in [Settings::new(), hostile()] {
                    let action = Action::Collect {
                        ordering: opened.ordering.clone(),
                    };
                    let rows =
                        collected_rows(run(fixture, &bytes, action, &settings), representation);

                    let context = format!("{format:?} {representation:?} {groups} {settings:?}");
                    assert_eq!(keys(&rows), expected_keys(), "{context}");
                    assert_eq!(sorted(rows), expected_rows(), "{context}");
                }
            }
        }
    }
}

/// The grouped-merge a frontend writes and the built-in one plan alike, whichever way the frontend
/// nests its unions, and neither needs a sort: each group's sort is an established order, and the
/// root assertion merges the groups.
#[test]
fn the_built_in_grouped_merge_explains_as_the_input_groups_a_frontend_writes() {
    for format in FORMATS {
        for row_ordering in [RowOrdering::locus(), RowOrdering::locus_then_alleles()] {
            let fixture = fixture::mixed_dataset_fixture(format, LocusRepresentation::Packed);
            let opened = open(fixture, row_ordering.clone());
            for (groups, sizes) in GROUP_SIZES {
                let builtin = backend_over(fixture)
                    .builtin_plan(&opened, &grouped_merge(groups), Versions::backend())
                    .unwrap();
                let frontend = sorted_groups(fixture, row_ordering.clone(), sizes);

                for settings in [Settings::new(), one_partition(), hostile()] {
                    let context = format!("{format:?} {groups} {settings:?}");
                    assert_explains_alike(
                        fixture,
                        &builtin,
                        &frontend,
                        &opened.ordering,
                        &settings,
                        &context,
                    );
                }
            }
        }
    }
}

#[test]
fn every_other_formulation_has_no_built_in_plan() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let opened = open(fixture, RowOrdering::locus());

    for formulation in [
        grouped_merge(0),
        FormulationRecord {
            groups: None,
            ..grouped_merge(2)
        },
        FormulationRecord {
            split_points: Some("chr01:100".to_string()),
            ..grouped_merge(2)
        },
        FormulationRecord {
            name: "interval-merge".to_string(),
            groups: None,
            split_points: Some("chr01:100".to_string()),
        },
        FormulationRecord {
            name: "no such formulation".to_string(),
            groups: None,
            split_points: None,
        },
        FormulationRecord {
            groups: Some(2),
            ..union()
        },
    ] {
        let error = backend_over(fixture)
            .builtin_plan(&opened, &formulation, Versions::backend())
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
        assert!(error.message().contains(&formulation.name), "{error}");
    }
}

#[test]
fn a_dataset_with_no_input_tables_has_no_built_in_plan() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let mut opened = open(fixture, RowOrdering::locus());
    opened.input_tables.clear();

    let error = backend_over(fixture)
        .builtin_plan(&opened, &union(), Versions::backend())
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
}

#[test]
fn a_built_in_plan_refuses_versions_other_than_the_backend_s() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let opened = open(fixture, RowOrdering::locus());
    let other = Versions {
        wire_version: Versions::backend().wire_version.saturating_add(1),
        ..Versions::backend()
    };

    let error = backend_over(fixture)
        .builtin_plan(&opened, &union(), other)
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
    assert!(error.message().contains("DFPYCHN"), "{error}");
}

/// The backend builds the formulation over the dataset as it stands, through the stores its
/// resolver hands it.
#[test]
fn a_built_in_plan_on_a_store_the_resolver_does_not_serve_fails() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let opened = open(fixture, RowOrdering::locus());

    let error = Backend::new(Arc::new(NoStores))
        .builtin_plan(&opened, &union(), Versions::backend())
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::Backend, "{error}");
    assert!(error.message().contains("memory://"), "{error}");
}
