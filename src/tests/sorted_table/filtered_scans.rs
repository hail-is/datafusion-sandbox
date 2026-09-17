//! Scans under inexact filters: file pruning by statistics, the order and partitioning of what
//! remains, and the reads planning and execution make.

use super::{
    column_statistics, displayed_file_paths, file, file_with_rows, file_with_statistics,
    metadata_collection::MeteredFormat, multi_column_table, sample_table, sample_table_with_format,
    scalar_table, table,
};
use crate::fixture::{self, DatasetFixture, FixtureFormat, MemoryStore, block_on};
use crate::locus::{Locus, LocusInterval, LocusRepresentation};
use crate::pipeline::{self, PipelineOptions};

use async_trait::async_trait;
use datafusion::{
    arrow::record_batch::RecordBatch,
    catalog::TableProvider,
    common::{ColumnStatistics, ScalarValue, stats::Precision},
    datasource::file_format::FileFormat,
    logical_expr::Expr,
    physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning, displayable},
    prelude::{SessionContext, col, lit},
};
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

/// Plans one fixture sample under `filter` on a hostile session and returns the plan.
async fn filtered_plan(
    ctx: &SessionContext,
    fixture: &DatasetFixture,
    filter: Option<Expr>,
) -> Arc<dyn ExecutionPlan> {
    let table = sample_table(ctx, fixture, fixture::SAMPLES[0]).await;
    let mut df = ctx.read_table(Arc::new(table)).unwrap();
    if let Some(filter) = filter {
        df = df.filter(filter).unwrap();
    }
    df.create_physical_plan().await.unwrap()
}

/// The shared session, plus permission to split or re-sort a scan wherever the table lets it.
fn hostile_session(fixture: &DatasetFixture) -> SessionContext {
    let mut config = pipeline::session_config().with_target_partitions(8);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    let ctx = SessionContext::new_with_config(config);
    fixture.register(&ctx);
    ctx
}

/// The file stems of `paths`, in order.
fn stems(paths: &[String]) -> Vec<&str> {
    paths
        .iter()
        .map(|path| {
            path.rsplit('/')
                .next()
                .and_then(|name| name.split_once('.'))
                .map(|(stem, _)| stem)
                .unwrap()
        })
        .collect()
}

#[test]
fn a_contig_filter_prunes_files_constant_on_another_contig_in_both_formats() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        let fixture = fixture::dataset_fixture(format, LocusRepresentation::ContigPosition);
        let paths = block_on(async {
            let ctx = hostile_session(fixture);
            let plan = filtered_plan(&ctx, fixture, Some(col("contig").eq(lit("chr2")))).await;
            displayed_file_paths(plan.as_ref())
        });
        // d and c are constant on chr1; b spans the contig boundary and a is constant on chr2.
        assert_eq!(stems(&paths), ["b", "a"], "{format:?}");
    }
}

/// Locus intervals prune by ordering statistics alone, in both representations and formats,
/// and the rows that come back are the filter's rows in locus order.
#[test]
fn a_locus_interval_filter_prunes_files_outside_it_and_keeps_the_scan_ordered() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            let fixture = Arc::clone(fixture::dataset_fixture(format, representation));
            let (paths, batches) = pipeline::run(
                move |_| async move {
                    let ctx = hostile_session(&fixture);
                    let interval = LocusInterval::new(
                        Some(Locus::new(1, 3).unwrap()),
                        Some(Locus::new(1, 5).unwrap()),
                    )
                    .unwrap();
                    let plan = filtered_plan(&ctx, &fixture, interval.filter(representation)).await;
                    assert!(plan.output_ordering().is_some(), "{plan:?}");
                    assert!(matches!(
                        plan.output_partitioning(),
                        Partitioning::UnknownPartitioning(1)
                    ));
                    let text = displayable(plan.as_ref()).indent(true).to_string();
                    assert!(!text.contains("SortExec"), "{text}");
                    assert!(!text.contains("RepartitionExec"), "{text}");
                    let paths = displayed_file_paths(plan.as_ref());
                    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
                    Ok((paths, batches))
                },
                PipelineOptions {
                    threads: 1,
                    ..Default::default()
                },
            )
            .unwrap();
            // d ends at chr1:2 and a starts at chr2:2; c holds chr1:3 and b starts at chr1:4.
            assert_eq!(stems(&paths), ["c", "b"], "{format:?} {representation:?}");
            let loci: Vec<_> = batches
                .iter()
                .flat_map(|batch| fixture::decode_loci(batch, representation))
                .collect();
            assert_eq!(
                loci,
                [Locus::new(1, 3).unwrap(), Locus::new(1, 4).unwrap()],
                "{format:?} {representation:?}"
            );
        }
    }
}

/// A filter that excludes every file yields an empty result over the projected schema.
#[test]
fn a_filter_excluding_every_file_returns_an_empty_result_with_the_projected_schema() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        let fixture = Arc::clone(fixture::dataset_fixture(
            format,
            LocusRepresentation::ContigPosition,
        ));
        pipeline::run(
            move |_| async move {
                let ctx = hostile_session(&fixture);
                let table = sample_table(&ctx, &fixture, fixture::SAMPLES[0]).await;
                let df = ctx
                    .read_table(Arc::new(table))?
                    .filter(col("contig").eq(lit("chr3")))?
                    .select(vec![col("position")])?;
                let plan = df.create_physical_plan().await?;
                let text = displayable(plan.as_ref()).indent(true).to_string();
                assert!(text.contains("file_groups={1 group: [[]]}"), "{text}");
                assert!(!text.contains("SortExec"), "{text}");
                assert_eq!(plan.schema().fields().len(), 1, "{text}");
                assert_eq!(plan.schema().field(0).name(), "position");
                let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
                Ok(())
            },
            PipelineOptions {
                threads: 1,
                ..Default::default()
            },
        )
        .unwrap();
    }
}

/// Retained files keep the relative order the unfiltered scan gives them, and the scan's
/// statistics describe only the retained files.
#[test]
fn retained_files_keep_their_recovered_order_and_the_scan_statistics_describe_them() {
    let fixture =
        fixture::dataset_fixture(FixtureFormat::Parquet, LocusRepresentation::ContigPosition);
    block_on(async {
        let ctx = hostile_session(fixture);
        let unfiltered = filtered_plan(&ctx, fixture, None).await;
        let filtered = filtered_plan(
            &ctx,
            fixture,
            Some(
                col("contig")
                    .eq(lit("chr1"))
                    .and(col("position").gt_eq(lit(2))),
            ),
        )
        .await;
        let all = displayed_file_paths(unfiltered.as_ref());
        let kept = displayed_file_paths(filtered.as_ref());
        // d, c, and b all reach chr1:2 or later on chr1; a is constant on chr2.
        assert_eq!(stems(&kept), ["d", "c", "b"]);
        let mut expected = all.iter().filter(|path| kept.contains(path));
        assert!(kept.iter().all(|path| expected.next() == Some(path)));
        // The pushed contig filter makes contig a constant, so the filtered scan may state its
        // ordering without it. It must still satisfy the full declared ordering.
        let declared = unfiltered.output_ordering().unwrap();
        assert!(
            filtered
                .equivalence_properties()
                .ordering_satisfy(declared.iter().cloned())
                .unwrap(),
            "{:?}",
            filtered.output_ordering()
        );

        // The pushed filter marks the scan's row count inexact; its value is the retained files'.
        let statistics = super::statistics(filtered.as_ref(), None);
        assert_eq!(statistics.num_rows.get_value(), Some(&6));
        assert_eq!(
            super::statistics(unfiltered.as_ref(), None).num_rows,
            Precision::Exact(8)
        );
    });
}

/// Pruning removes only files whose exact bounds prove the filter false. Absent and inexact
/// bounds are unknown to the pruner, so their files stay.
#[tokio::test]
async fn absent_and_inexact_bounds_keep_a_file_and_exact_bounds_that_exclude_the_filter_remove_it()
{
    let store = MemoryStore::new("conservative-pruning");
    let inexact = |min: i32, max: i32| ColumnStatistics {
        null_count: Precision::Exact(0),
        min_value: Precision::Inexact(ScalarValue::Int32(Some(min))),
        max_value: Precision::Inexact(ScalarValue::Int32(Some(max))),
        ..Default::default()
    };
    let files = || {
        vec![
            file_with_statistics(
                "proven.parquet",
                vec![
                    column_statistics(Some(1), Some(1)),
                    column_statistics(Some(1), Some(3)),
                ],
            ),
            // Not constant on major, so order recovery never compares minor.
            file_with_statistics(
                "inexact.parquet",
                vec![column_statistics(Some(2), Some(3)), inexact(1, 3)],
            ),
            file_with_statistics(
                "absent.parquet",
                vec![
                    column_statistics(Some(4), Some(5)),
                    column_statistics(None, None),
                ],
            ),
        ]
    };
    let ctx = SessionContext::new();
    store.register(&ctx);
    for (filter, expected) in [
        (
            col("minor").gt(lit(10)),
            vec!["inexact.parquet", "absent.parquet"],
        ),
        (
            col("minor").lt_eq(lit(3)),
            vec!["proven.parquet", "inexact.parquet", "absent.parquet"],
        ),
        (col("major").gt_eq(lit(4)), vec!["absent.parquet"]),
        // An expression the pruner cannot rewrite keeps every file.
        (
            (col("major") % lit(2)).eq(lit(0)),
            vec!["proven.parquet", "inexact.parquet", "absent.parquet"],
        ),
    ] {
        let table = multi_column_table(&store, files());
        let plan = table
            .scan(&ctx.state(), None, std::slice::from_ref(&filter), None)
            .await
            .unwrap();
        assert_eq!(displayed_file_paths(plan.as_ref()), expected, "{filter}");
    }
}

/// Pruning runs before order recovery, so a filter that excludes a file with unusable ordering
/// bounds rescues the scan. A filter that cannot exclude it leaves the error in place.
#[tokio::test]
async fn a_filter_rescues_a_scan_by_removing_a_file_with_unusable_ordering_bounds() {
    let store = MemoryStore::new("rescued-scan");
    let files = || {
        vec![
            file_with_statistics(
                "usable.parquet",
                vec![
                    column_statistics(Some(1), Some(1)),
                    column_statistics(Some(1), Some(3)),
                ],
            ),
            // Constant on major, so order recovery needs minor bounds it does not have.
            file_with_statistics(
                "unusable.parquet",
                vec![
                    column_statistics(Some(2), Some(2)),
                    column_statistics(None, None),
                ],
            ),
        ]
    };
    let ctx = SessionContext::new();
    store.register(&ctx);
    let error = multi_column_table(&store, files())
        .scan(&ctx.state(), None, &[], None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unusable.parquet"), "{error}");
    let error = multi_column_table(&store, files())
        .scan(&ctx.state(), None, &[col("minor").gt(lit(0))], None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unusable.parquet"), "{error}");

    let plan = multi_column_table(&store, files())
        .scan(&ctx.state(), None, &[col("major").eq(lit(1))], None)
        .await
        .unwrap();
    assert_eq!(displayed_file_paths(plan.as_ref()), ["usable.parquet"]);
}

/// A file with exactly zero rows is dropped even when its bounds satisfy the filter, so the
/// zero-row rule and not pruning removes it.
#[tokio::test]
async fn zero_row_files_are_still_dropped_under_a_filter_their_bounds_satisfy() {
    let store = MemoryStore::new("zero-rows-under-filter");
    let ctx = SessionContext::new();
    store.register(&ctx);
    let plan = table(
        &store,
        vec![
            file_with_rows(
                "empty.parquet",
                0,
                vec![column_statistics(Some(1), Some(10))],
            ),
            file("rows.parquet", Some(1), Some(10)),
        ],
    )
    .scan(&ctx.state(), None, &[col("position").gt(lit(0))], None)
    .await
    .unwrap();
    assert_eq!(displayed_file_paths(plan.as_ref()), ["rows.parquet"]);
}

/// The attached scalar is folded into the pruning predicate, so a filter mixing it with a
/// stored column prunes by the scalar's known value.
#[tokio::test]
async fn filters_mixing_the_attached_scalar_with_stored_columns_prune_by_the_scalar_value() {
    let store = MemoryStore::new("mixed-scalar-filters");
    let ctx = SessionContext::new();
    store.register(&ctx);
    let files = || {
        vec![
            file("later.parquet", Some(11), Some(20)),
            file("earlier.parquet", Some(1), Some(10)),
        ]
    };
    let source_is = |sample: &str| col("source").eq(lit(sample));
    for (filter, expected) in [
        (
            source_is("sample-1").and(col("position").gt(lit(15))),
            vec!["later.parquet"],
        ),
        (
            source_is("sample-2").or(col("position").gt(lit(15))),
            vec!["later.parquet"],
        ),
        (
            source_is("sample-1").or(col("position").gt(lit(15))),
            vec!["earlier.parquet", "later.parquet"],
        ),
        (
            source_is("sample-2").and(col("position").gt(lit(15))),
            vec![],
        ),
    ] {
        let table = scalar_table(&store, files(), ScalarValue::from("sample-1"));
        let plan = table
            .scan(&ctx.state(), None, std::slice::from_ref(&filter), None)
            .await
            .unwrap();
        assert!(plan.output_ordering().is_some(), "{filter}");
        let text = displayable(plan.as_ref()).indent(true).to_string();
        let paths = if expected.is_empty() {
            assert!(text.contains("file_groups={1 group: [[]]}"), "{text}");
            vec![]
        } else {
            displayed_file_paths(plan.as_ref())
        };
        assert_eq!(paths, expected, "{filter}");
    }
}

/// A filter the pruner cannot use keeps every file and still returns the right rows.
#[test]
fn an_unsupported_pruning_expression_keeps_every_file_and_filters_the_rows() {
    let fixture = Arc::clone(fixture::dataset_fixture(
        FixtureFormat::Parquet,
        LocusRepresentation::ContigPosition,
    ));
    let (paths, loci) = pipeline::run(
        move |_| async move {
            let ctx = hostile_session(&fixture);
            let plan =
                filtered_plan(&ctx, &fixture, Some((col("position") % lit(2)).eq(lit(1)))).await;
            let paths = displayed_file_paths(plan.as_ref());
            let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
            let loci: Vec<_> = batches
                .iter()
                .flat_map(|batch| fixture::decode_loci(batch, LocusRepresentation::ContigPosition))
                .collect();
            Ok((paths, loci))
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(stems(&paths), ["d", "c", "b", "a"]);
    let expected = [
        Locus::new(1, 1).unwrap(),
        Locus::new(1, 3).unwrap(),
        Locus::new(2, 1).unwrap(),
        Locus::new(2, 3).unwrap(),
    ];
    assert_eq!(loci, expected);
}

/// An object store that records the location of every read it serves.
#[derive(Debug)]
struct RecordingStore {
    inner: Arc<dyn ObjectStore>,
    reads: Mutex<Vec<String>>,
}

impl RecordingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            reads: Mutex::new(Vec::new()),
        }
    }

    /// The distinct file stems read so far, then forgets them.
    fn take_read_stems(&self) -> BTreeSet<String> {
        let reads = std::mem::take(&mut *self.reads.lock().unwrap());
        stems(&reads).into_iter().map(ToString::to_string).collect()
    }
}

impl std::fmt::Display for RecordingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "RecordingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.reads.lock().unwrap().push(location.to_string());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// No constant column's value is known before its footer is read, so planning a filtered scan
/// still reads every footer once, and pruning happens afterwards. Execution then opens only the
/// files pruning retained.
#[test]
fn planning_reads_every_footer_once_and_execution_opens_only_retained_files() {
    for format in [FixtureFormat::Parquet, FixtureFormat::Vortex] {
        let fixture = Arc::clone(fixture::dataset_fixture(
            format,
            LocusRepresentation::ContigPosition,
        ));
        pipeline::run(
            move |ctx| async move {
                let store = Arc::new(RecordingStore::new(Arc::clone(fixture.store())));
                let registered: Arc<dyn ObjectStore> = store.clone();
                ctx.register_object_store(fixture.table_path().object_store().as_ref(), registered);
                let metered = Arc::new(MeteredFormat::delegating(
                    fixture.input_format().read_format(),
                ));
                let metered_format: Arc<dyn FileFormat> = metered.clone();
                // Schema inference on another session, so its footer reads do not warm this
                // session's file metadata cache and hide planning's reads from the store.
                let table = sample_table_with_format(
                    &SessionContext::new(),
                    &fixture,
                    fixture::SAMPLES[0],
                    metered_format,
                )
                .await;
                assert!(store.take_read_stems().is_empty());

                let plan = ctx
                    .read_table(Arc::new(table))?
                    .filter(col("contig").eq(lit("chr2")))?
                    .create_physical_plan()
                    .await?;

                let all: BTreeSet<String> = ["a", "b", "c", "d"]
                    .into_iter()
                    .map(ToString::to_string)
                    .collect();
                let mut inferred = metered.started();
                inferred.sort();
                assert_eq!(inferred.len(), 4, "{format:?}: {inferred:?}");
                assert_eq!(store.take_read_stems(), all, "{format:?}");
                assert_eq!(stems(&displayed_file_paths(plan.as_ref())), ["b", "a"]);

                let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
                let opened = store.take_read_stems();
                let retained: BTreeSet<String> =
                    ["a", "b"].into_iter().map(ToString::to_string).collect();
                assert_eq!(opened, retained, "{format:?}");
                assert_eq!(metered.started().len(), 4, "{format:?}");
                let loci: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        fixture::decode_loci(batch, LocusRepresentation::ContigPosition)
                    })
                    .collect();
                assert!(
                    loci.iter().all(|locus| locus.contig_ordinal() == 2) && loci.len() == 3,
                    "{format:?}: {loci:?}"
                );

                // The cached statistics serve a second filtered plan without any read.
                let table = sample_table_with_format(
                    &SessionContext::new(),
                    &fixture,
                    fixture::SAMPLES[0],
                    metered.clone(),
                )
                .await;
                store.take_read_stems();
                let plan = ctx
                    .read_table(Arc::new(table))?
                    .filter(col("contig").eq(lit("chr2")))?
                    .create_physical_plan()
                    .await?;
                assert_eq!(stems(&displayed_file_paths(plan.as_ref())), ["b", "a"]);
                assert!(store.take_read_stems().is_empty(), "{format:?}");
                assert_eq!(metered.started().len(), 4, "{format:?}");
                Ok(())
            },
            PipelineOptions {
                threads: 1,
                ..Default::default()
            },
        )
        .unwrap();
    }
}
