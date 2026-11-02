//! Runs: plan bytes built in Rust with the backend's codec, framed as the wheel frames them,
//! started with an Action and waited for.

use super::{
    FORMATS, MemoryStores, REPRESENTATIONS, backend_over, collected_rows, expected_keys,
    expected_rows, hostile, keys, sorted, sorted_union, wheel_bytes,
};
use crate::{
    backend::{
        Action, Backend, ErrorCode, ObjectStoreResolver, Outcome, Settings, Versions,
        codec::{CODEC_ID, Codec},
    },
    locus::LocusRepresentation,
    pipeline::PipelineOptions,
    row_ordering::{RowOrdering, SortColumn},
    testing::{
        fixture::{self, FixtureFormat, GrowingDatasetFixture},
        plan_shape::exec_names,
        wheel_codec::WheelCodec,
    },
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::TableProvider,
    common::TableReference,
    error::Result,
    execution::TaskContext,
    functions::math::expr_fn::abs,
    logical_expr::{Expr, Extension, LogicalPlan, LogicalPlanBuilder, ScalarUDF},
    prelude::{SessionContext, lit},
};
use datafusion_proto::{
    bytes::{
        logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
    },
    logical_plan::LogicalExtensionCodec,
};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    thread::available_parallelism,
};

/// The union formulation, written as a frontend writes it: every input table's scan, unioned and
/// sorted by the dataset's ordering, collects every row in that ordering.
#[test]
fn collecting_a_sorted_union_of_every_scan_returns_every_row_in_the_dataset_s_ordering() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let fixture = fixture::dataset_fixture(format, representation);
            let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus_then_alleles());

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

                let rows = collected_rows(outcome, representation);
                let context = format!("{format:?} {representation:?} {settings:?}");
                assert_eq!(keys(&rows), expected_keys(), "{context}");
                assert_eq!(sorted(rows), expected_rows(), "{context}");
            }
        }
    }
}

/// Under the hostile session, the frontend's sort of a union of every scan explains as a merge tree
/// held by the root assertion, with no sort inserted, beside the frontend plan as decoded.
#[test]
fn explaining_a_sorted_union_under_the_hostile_session_shows_a_merge_tree_and_no_sort() {
    for format in FORMATS {
        for representation in REPRESENTATIONS {
            let fixture = fixture::dataset_fixture(format, representation);
            let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus_then_alleles());

            let outcome = backend_over(fixture)
                .start(
                    &bytes,
                    Action::Explain {
                        ordering,
                        write: None,
                    },
                    &hostile(),
                    None,
                    Versions::backend(),
                )
                .unwrap()
                .wait()
                .unwrap();

            let Outcome::Plan(plan) = outcome else {
                panic!("expected a plan, got {outcome:?}");
            };
            for section in ["frontend_plan", "logical_plan", "physical_plan"] {
                assert!(plan.contains(section), "{section}:\n{plan}");
            }
            let names = exec_names(&plan);
            assert!(!names.contains(&"SortExec"), "{plan}");
            assert!(
                plan.contains("OrderingAssertionExec: order=[") && plan.contains(", verify_order"),
                "a backend verifying order checks it at the assertion:\n{plan}"
            );
            let beneath_assertion = names
                .iter()
                .skip_while(|name| **name != "OrderingAssertionExec")
                .skip(1)
                .filter(|name| **name != "EstablishedOrderExec")
                .copied()
                .collect::<Vec<_>>();
            assert_eq!(
                beneath_assertion.first(),
                Some(&"SortPreservingMergeExec"),
                "{plan}"
            );
            assert!(beneath_assertion.contains(&"UnionExec"), "{plan}");
            assert!(
                plan.contains(&format!("Sort: {}", sort_keys(representation))),
                "the frontend plan keeps its sort:\n{plan}"
            );
        }
    }
}

/// An analyze runs the plan, draining its rows, and renders each operator's metrics beside the
/// frontend, logical and physical plans; a plain explain has no metrics, since it runs nothing.
#[test]
fn analyzing_a_plan_runs_it_and_shows_per_operator_metrics_beside_every_plan_section() {
    for format in FORMATS {
        let fixture = fixture::dataset_fixture(format, LocusRepresentation::Packed);
        let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus_then_alleles());
        let backend = backend_over(fixture);
        let plan_of = |action| {
            let outcome = backend
                .start(&bytes, action, &hostile(), None, Versions::backend())
                .unwrap()
                .wait()
                .unwrap();
            let Outcome::Plan(plan) = outcome else {
                panic!("expected a plan, got {outcome:?}");
            };
            plan
        };

        let analyzed = plan_of(Action::ExplainAnalyze {
            ordering: ordering.clone(),
            write: None,
        });
        let explained = plan_of(Action::Explain {
            ordering,
            write: None,
        });

        for section in ["frontend_plan", "logical_plan", "physical_plan"] {
            assert!(analyzed.contains(section), "{section}:\n{analyzed}");
        }
        let every_row = expected_rows().len();
        assert!(analyzed.contains("Plan with Metrics"), "{analyzed}");
        assert!(
            analyzed.contains(&format!("output_rows={every_row}")),
            "the root operators report every row:\n{analyzed}"
        );
        assert!(analyzed.contains("elapsed_compute="), "{analyzed}");
        assert!(!explained.contains("Plan with Metrics"), "{explained}");
        assert!(!explained.contains("output_rows="), "{explained}");
    }
}

/// An analyze fails on what a run would fail on, rather than rendering the failure as a plan.
#[test]
fn analyzing_a_plan_the_root_assertion_holds_only_by_a_sort_fails_with_the_ordering_assertion_code()
{
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let (bytes, _) = sorted_union(fixture, RowOrdering::locus_then_alleles());

    let error = backend_over(fixture)
        .start(
            &bytes,
            Action::ExplainAnalyze {
                ordering: vec![SortColumn {
                    column: "alleles".to_string(),
                    ascending: true,
                    nulls_first: false,
                }],
                write: None,
            },
            &Settings::new(),
            None,
            Versions::backend(),
        )
        .unwrap()
        .wait()
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::OrderingAssertionFailed, "{error}");
}

#[test]
fn every_action_with_an_empty_ordering_fails_to_start_with_the_invalid_action_code() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let (bytes, _) = sorted_union(fixture, RowOrdering::locus());

    for action in [
        Action::Collect { ordering: vec![] },
        Action::Explain {
            ordering: vec![],
            write: None,
        },
        Action::ExplainAnalyze {
            ordering: vec![],
            write: None,
        },
    ] {
        let error = backend_over(fixture)
            .start(
                &bytes,
                action.clone(),
                &Settings::new(),
                None,
                Versions::backend(),
            )
            .unwrap_err();

        assert_eq!(
            error.code(),
            ErrorCode::InvalidAction,
            "{action:?}: {error}"
        );
    }
}

/// The threads a run is started with, by default and under `hail_df.threads`, reach the options
/// its runtimes are built from.
#[test]
fn the_threads_setting_sets_the_run_s_thread_count_and_defaults_to_available_parallelism() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let threads = Arc::new(RecordingThreads {
        stores: MemoryStores(vec![(
            fixture.table_path().object_store(),
            Arc::clone(fixture.store()),
        )]),
        threads: Mutex::default(),
    });
    let backend = Backend::new(threads.clone());
    let default = available_parallelism().unwrap();

    for settings in [
        Settings::new(),
        Settings::from([("hail_df.threads".to_string(), "3".to_string())]),
    ] {
        backend
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
    }

    assert_eq!(
        *threads.threads.lock().unwrap(),
        [default, NonZeroUsize::new(3).unwrap()]
    );
}

/// The adapter at the backend's object-store seam that records the thread count each run asks for.
#[derive(Debug)]
struct RecordingThreads {
    stores: MemoryStores,
    threads: Mutex<Vec<NonZeroUsize>>,
}

impl ObjectStoreResolver for RecordingThreads {
    fn pipeline_options(&self, threads: NonZeroUsize, paths: &[&str]) -> Result<PipelineOptions> {
        self.threads.lock().unwrap().push(threads);
        self.stores.pipeline_options(threads, paths)
    }
}

/// A run reads the files an input table holds when it runs, not when its scan was described.
#[test]
fn a_file_added_to_an_input_table_after_its_plan_was_encoded_is_read() {
    for format in FORMATS {
        let fixture = fixture::growing_dataset_fixture(
            &format!("relisted-{format:?}").to_lowercase(),
            format,
            LocusRepresentation::Packed,
        );
        let backend = Backend::new(Arc::new(MemoryStores(vec![(
            fixture.store().url().clone(),
            Arc::clone(fixture.store().store()),
        )])))
        .verifying_order();
        let opened = backend
            .open_dataset(
                fixture.table_path().as_str(),
                fixture.input_format(),
                RowOrdering::locus_then_alleles(),
                Versions::backend(),
            )
            .unwrap();
        let [table] = opened.input_tables.as_slice() else {
            panic!("a growing dataset has one input table");
        };
        let provider: Arc<dyn TableProvider> = table.scan.clone();
        let plan = SessionContext::new()
            .read_table(provider)
            .unwrap()
            .sort(
                opened
                    .ordering
                    .iter()
                    .map(SortColumn::sort_expression)
                    .collect(),
            )
            .unwrap()
            .into_unoptimized_plan();
        let bytes = wheel_bytes(&plan);

        fixture.add_next_file();
        let outcome = backend
            .start(
                &bytes,
                Action::Collect {
                    ordering: opened.ordering.clone(),
                },
                &Settings::new(),
                None,
                Versions::backend(),
            )
            .unwrap()
            .wait()
            .unwrap();

        let expected = fixture::sample_rows()
            .into_iter()
            .take(4)
            .map(|(locus, alleles)| {
                (
                    locus,
                    alleles.to_string(),
                    GrowingDatasetFixture::SAMPLE.to_string(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            collected_rows(outcome, LocusRepresentation::Packed),
            expected,
            "{format:?}"
        );
    }
}

/// A plan whose root assertion holds only if the backend sorts fails before it runs.
#[test]
fn a_plan_the_root_assertion_holds_only_by_a_sort_fails_with_the_ordering_assertion_code() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let (bytes, _) = sorted_union(fixture, RowOrdering::locus_then_alleles());
    let by_alleles = vec![SortColumn {
        column: "alleles".to_string(),
        ascending: true,
        nulls_first: false,
    }];

    let error = backend_over(fixture)
        .start(
            &bytes,
            Action::Collect {
                ordering: by_alleles,
            },
            &Settings::new(),
            None,
            Versions::backend(),
        )
        .unwrap()
        .wait()
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::OrderingAssertionFailed, "{error}");
    assert!(error.message().contains("alleles"), "{error}");
}

/// A frontend on another `DataFusion` major, or framing payloads at another wire version, is
/// refused before anything runs.
#[test]
fn versions_other_than_the_backend_s_fail_to_start_with_the_invalid_action_code() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Vortex, LocusRepresentation::Packed);
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let backend = Versions::backend();

    for (versions, mentioned) in [
        (
            Versions {
                datafusion_major: backend.datafusion_major.saturating_add(1),
                ..backend
            },
            "DataFusion",
        ),
        (
            Versions {
                wire_version: backend.wire_version.saturating_add(1),
                ..backend
            },
            "DFPYCHN",
        ),
    ] {
        let error = backend_over(fixture)
            .start(
                &bytes,
                Action::Collect {
                    ordering: ordering.clone(),
                },
                &Settings::new(),
                None,
                versions,
            )
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
        assert!(error.message().contains(mentioned), "{error}");
    }
}

/// A scan's payload framed at another `DFPYCHN` version is refused before anything runs, even
/// when the frontend states the backend's versions.
#[test]
fn a_payload_framed_at_another_wire_version_fails_to_start_with_the_invalid_action_code() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Vortex, LocusRepresentation::Packed);
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let reframed = at_wire_version(&bytes, 2);
    assert_ne!(reframed, bytes);

    let error = backend_over(fixture)
        .start(
            &reframed,
            Action::Collect { ordering },
            &Settings::new(),
            None,
            Versions::backend(),
        )
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
    assert!(error.message().contains("version 2"), "{error}");
}

/// A function the wheel inlined as a Python payload is refused, whichever family of Python
/// function its payload names, while a function with no payload resolves by name from the
/// backend's registry.
#[test]
fn an_inlined_python_function_fails_to_start_with_the_invalid_action_code() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let plan = with_function_call(&bytes);
    let backend = backend_over(fixture);
    let start = |bytes: &[u8]| {
        backend.start(
            bytes,
            Action::Collect {
                ordering: ordering.clone(),
            },
            &Settings::new(),
            None,
            Versions::backend(),
        )
    };

    let by_name = start(&wheel_bytes(&plan)).unwrap().wait().unwrap();
    assert!(matches!(by_name, Outcome::Rows { .. }), "{by_name:?}");
    for family in *b"FAW" {
        let mut payload = b"DFPYUD".to_vec();
        payload.extend_from_slice(&[family, 1, 3, 13]);
        payload.extend_from_slice(b"a cloudpickled callable");
        let inlined =
            logical_plan_to_bytes_with_extension_codec(&plan, &InliningPythonFunctions(payload))
                .unwrap();

        let error = start(&inlined).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidAction, "{error}");
        assert!(error.message().contains("'abs'"), "{error}");
        assert!(error.message().contains("Python"), "{error}");
    }
}

#[test]
fn a_setting_outside_the_three_the_backend_takes_fails_to_start_with_the_invalid_action_code() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());

    for (key, value) in [
        ("hail_df.partitions", "8"),
        ("datafusion.execution.target_partitions", "8"),
        ("hail_df.threads", "many"),
        ("hail_df.target_partitions", "0"),
    ] {
        let error = backend_over(fixture)
            .start(
                &bytes,
                Action::Collect {
                    ordering: ordering.clone(),
                },
                &Settings::from([(key.to_string(), value.to_string())]),
                None,
                Versions::backend(),
            )
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidAction, "{key}: {error}");
        assert!(error.message().contains(key), "{error}");
    }
}

#[test]
fn a_run_keeps_the_id_it_is_given_and_otherwise_gets_one_of_its_own() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::Packed);
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let backend = backend_over(fixture);
    let start = |run_id: Option<&str>| {
        backend
            .start(
                &bytes,
                Action::Collect {
                    ordering: ordering.clone(),
                },
                &Settings::new(),
                run_id.map(ToString::to_string),
                Versions::backend(),
            )
            .unwrap()
    };

    let given = start(Some("combine-7"));
    let first = start(None);
    let second = start(None);

    assert_eq!(given.run_id(), "combine-7");
    assert_ne!(first.run_id(), "");
    assert_ne!(first.run_id(), second.run_id());
    for run in [given, first, second] {
        run.wait().unwrap();
    }
}

/// Each error code's name is part of the interface a frontend maps exceptions from.
#[test]
fn every_error_code_has_a_stable_name() {
    assert_eq!(
        [
            ErrorCode::InvalidAction,
            ErrorCode::OrderingAssertionFailed,
            ErrorCode::RunCancelled,
            ErrorCode::Backend,
        ]
        .map(ErrorCode::name),
        [
            "invalid_action",
            "ordering_assertion_failed",
            "run_cancelled",
            "backend_error",
        ]
    );
}

/// The run-time order check is the backend's choice, off unless it is built verifying order.
#[test]
fn only_a_backend_verifying_order_checks_its_rows_order_at_run_time() {
    let fixture = fixture::dataset_fixture(FixtureFormat::Vortex, LocusRepresentation::Packed);
    let (bytes, ordering) = sorted_union(fixture, RowOrdering::locus());
    let resolver = || {
        Arc::new(MemoryStores(vec![(
            fixture.table_path().object_store(),
            Arc::clone(fixture.store()),
        )]))
    };

    let [plain, verifying] = [
        Backend::new(resolver()),
        Backend::new(resolver()).verifying_order(),
    ]
    .map(|backend| {
        let outcome = backend
            .start(
                &bytes,
                Action::Explain {
                    ordering: ordering.clone(),
                    write: None,
                },
                &Settings::new(),
                None,
                Versions::backend(),
            )
            .unwrap()
            .wait()
            .unwrap();
        let Outcome::Plan(plan) = outcome else {
            panic!("expected a plan, got {outcome:?}");
        };
        plan
    });

    assert!(!plain.contains("verify_order"), "{plain}");
    assert!(verifying.contains("verify_order"), "{verifying}");
}

/// The plan `bytes` encode with a call of the registered function `abs` added to a projection
/// above it, keeping its columns.
fn with_function_call(bytes: &[u8]) -> LogicalPlan {
    let plan = logical_plan_from_bytes_with_extension_codec(
        bytes,
        &SessionContext::new().task_ctx(),
        &Codec,
    )
    .unwrap();
    let columns = plan.schema().columns().into_iter().map(Expr::Column);
    LogicalPlanBuilder::from(plan)
        .project(columns.chain([abs(lit(-1_i64)).alias("one")]))
        .unwrap()
        .build()
        .unwrap()
}

/// The wheel's codec chain with Python-function inlining on: every function as `payload`,
/// unframed, ahead of the backend's codec, framed, for everything else.
#[derive(Debug)]
struct InliningPythonFunctions(Vec<u8>);

impl LogicalExtensionCodec for InliningPythonFunctions {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        wheel_codec().try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        wheel_codec().try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        wheel_codec().try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        wheel_codec().try_encode_table_provider(table_ref, node, buf)
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(&self.0);
        Ok(())
    }
}

fn wheel_codec() -> WheelCodec {
    WheelCodec::new(Arc::new(Codec), CODEC_ID)
}

/// `bytes` with every `DFPYCHN` frame in them at wire version `version`.
fn at_wire_version(bytes: &[u8], version: u8) -> Vec<u8> {
    let frames = bytes
        .windows(b"DFPYCHN".len())
        .enumerate()
        .filter(|(_, window)| window == b"DFPYCHN")
        .map(|(start, window)| start.checked_add(window.len()).unwrap())
        .collect::<Vec<_>>();
    let mut reframed = bytes.to_vec();
    for at in frames {
        reframed[at] = version;
    }
    reframed
}

/// The sort keys of the locus-then-alleles ordering in `representation`, as a logical plan
/// renders them.
fn sort_keys(representation: LocusRepresentation) -> String {
    RowOrdering::locus_then_alleles()
        .expand(representation)
        .column_names()
        .iter()
        .map(|column| format!("{column} ASC NULLS LAST"))
        .collect::<Vec<_>>()
        .join(", ")
}
