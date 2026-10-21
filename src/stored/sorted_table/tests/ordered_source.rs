use crate::locus::{Locus, LocusRepresentation};
use crate::pipeline::{self, PipelineOptions};
use crate::stored::sorted_table::ordered_source::OrderedSource;
use crate::testing::fixture::{self, FixtureFormat, SortedTableFixture, block_on};

use datafusion::{
    arrow::{datatypes::Schema, record_batch::RecordBatch},
    common::{
        DFSchema, DataFusionError, ScalarValue, Statistics, config::ConfigOptions, stats::Precision,
    },
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource},
        source::{DataSource, DataSourceExec},
        table_schema::TableSchema,
    },
    logical_expr::Expr,
    physical_expr::{
        LexOrdering, PhysicalExpr, PhysicalSortExpr, expressions::Column,
        projection::ProjectionExprs,
    },
    physical_plan::{ExecutionPlan, SortOrderPushdownResult, displayable},
    prelude::{SessionContext, col, lit},
};
use futures::TryStreamExt;
use object_store::ObjectStoreExt as _;
use std::sync::Arc;

/// Two Parquet files of one locus-sorted table cut inside `chr01:2`. Their bounds touch, so the
/// file scan alone cannot prove that reading them in order keeps the table sorted.
fn touching_files() -> SortedTableFixture {
    let locus = |position| Locus::new(1, position).unwrap();
    fixture::sorted_table_fixture(
        "ordered-source",
        FixtureFormat::Parquet,
        LocusRepresentation::ContigPosition,
        vec![vec![locus(1), locus(2)], vec![locus(2), locus(3)]],
    )
}

fn table_schema() -> TableSchema {
    TableSchema::from(Arc::new(Schema::new(
        LocusRepresentation::ContigPosition.fields(),
    )))
}

/// `contig, position`, the order the files were written in.
fn declared() -> LexOrdering {
    LexOrdering::new(vec![
        PhysicalSortExpr::new_default(Arc::new(Column::new("contig", 0))),
        PhysicalSortExpr::new_default(Arc::new(Column::new("position", 1))),
    ])
    .unwrap()
}

/// A scan of the fixture's files in locus order, declaring [`declared`] and four rows, through
/// `source`, projected to `projection`.
async fn config_with(
    fixture: &SortedTableFixture,
    source: ParquetSource,
    projection: Option<Vec<usize>>,
) -> FileScanConfig {
    let mut files = Vec::new();
    for index in 0..2 {
        let meta = fixture
            .store()
            .head(fixture.file_path(index).prefix())
            .await
            .unwrap();
        files.push(PartitionedFile::from(meta));
    }
    let schema = table_schema();
    let statistics = Statistics {
        num_rows: Precision::Exact(4),
        ..Statistics::new_unknown(schema.table_schema())
    };
    FileScanConfigBuilder::new(fixture.table_path().object_store(), Arc::new(source))
        .with_file_group(FileGroup::new(files))
        .with_statistics(statistics)
        .with_projection_indices(projection)
        .unwrap()
        .with_output_ordering(vec![declared()])
        .build()
}

async fn config(fixture: &SortedTableFixture) -> FileScanConfig {
    config_with(fixture, ParquetSource::new(table_schema()), None).await
}

/// The output ordering a plan over `source` reports, as text.
fn ordering_of(source: Arc<dyn DataSource>) -> Option<String> {
    DataSourceExec::new(source)
        .properties()
        .output_ordering()
        .map(ToString::to_string)
}

fn satisfies(source: &dyn DataSource, ordering: &LexOrdering) -> bool {
    source
        .eq_properties()
        .ordering_satisfy(ordering.iter().cloned())
        .unwrap()
}

/// `filter` as a physical expression over the rows `source` produces.
fn predicate(source: &dyn DataSource, filter: Expr) -> Arc<dyn PhysicalExpr> {
    let schema = DFSchema::try_from(Arc::clone(source.eq_properties().schema())).unwrap();
    SessionContext::new()
        .create_physical_expr(filter, &schema)
        .unwrap()
}

/// Session options under which a Parquet scan accepts pushed-down filters.
fn pushdown_options() -> ConfigOptions {
    let mut options = ConfigOptions::new();
    options.execution.parquet.pushdown_filters = true;
    options
}

#[test]
fn the_declared_ordering_holds_where_the_wrapped_scan_cannot_prove_it() {
    let fixture = touching_files();
    block_on(async {
        let config = config(&fixture).await;
        assert!(
            !satisfies(&config, &declared()),
            "the fixture must be one the scan alone cannot prove ordered"
        );

        let source = OrderedSource::new(config).unwrap();

        assert!(satisfies(&source, &declared()));
    });
}

#[test]
fn the_declared_ordering_survives_fetch_filter_pushdown_and_state_rewrites() {
    let fixture = touching_files();
    block_on(async {
        let source = OrderedSource::new(config(&fixture).await).unwrap();

        let fetched = source.with_fetch(Some(1)).unwrap();
        assert_eq!(fetched.fetch(), Some(1));
        assert!(satisfies(fetched.as_ref(), &declared()));

        let filter = predicate(&source, col("position").gt(lit(1)));
        let filtered = source
            .try_pushdown_filters(vec![filter], &pushdown_options())
            .unwrap()
            .updated_node
            .expect("a Parquet scan takes a pushed-down filter");
        assert!(satisfies(filtered.as_ref(), &declared()));

        // A file scan takes no execution state through `with_new_state`, so its only state
        // rewrite is whether to preserve order.
        for preserve_order in [true, false] {
            let rewritten = source.with_preserve_order(preserve_order).unwrap();
            assert!(
                satisfies(rewritten.as_ref(), &declared()),
                "{preserve_order}"
            );
        }
    });
}

#[test]
fn a_projection_keeps_the_equivalences_that_still_apply() {
    let fixture = touching_files();
    block_on(async {
        let source = OrderedSource::new(config(&fixture).await).unwrap();
        let project = |source: &dyn DataSource, indices: &[usize]| {
            let schema = Arc::clone(source.eq_properties().schema());
            source
                .try_swapping_with_projection(&ProjectionExprs::from_indices(indices, &schema))
                .unwrap()
                .expect("a Parquet scan takes a projection")
        };

        assert_eq!(
            ordering_of(project(&source, &[1, 0])).as_deref(),
            Some("contig@1 ASC, position@0 ASC")
        );
        assert_eq!(
            ordering_of(project(&source, &[0])).as_deref(),
            Some("contig@0 ASC")
        );
        assert_eq!(ordering_of(project(&source, &[1])), None);

        // A filter that makes `contig` constant leaves `position` ordered without it.
        let filter = predicate(
            &source,
            col("contig").eq(lit(ScalarValue::Utf8View(Some("chr01".to_string())))),
        );
        let filtered = source
            .try_pushdown_filters(vec![filter], &pushdown_options())
            .unwrap()
            .updated_node
            .expect("a Parquet scan takes a pushed-down filter");
        assert_eq!(
            ordering_of(project(filtered.as_ref(), &[1])).as_deref(),
            Some("position@0 ASC")
        );
    });
}

/// A format may return its config with a filter and projection already installed. The filter's
/// equalities count before the projection drops a column of the declared ordering.
#[test]
fn a_config_already_filtered_and_projected_declares_what_its_filter_preserves() {
    let fixture = touching_files();
    block_on(async {
        let unfiltered = config(&fixture).await;
        let filter = predicate(
            &unfiltered,
            col("contig").eq(lit(ScalarValue::Utf8View(Some("chr01".to_string())))),
        );
        let parquet = ParquetSource::new(table_schema()).with_predicate(filter);
        let config = config_with(&fixture, parquet, Some(vec![1])).await;

        let source = OrderedSource::new(config).unwrap();

        assert_eq!(
            ordering_of(Arc::new(source)).as_deref(),
            Some("position@0 ASC")
        );
    });
}

#[test]
fn a_sort_request_that_does_not_match_drops_the_declaration() {
    let fixture = touching_files();
    block_on(async {
        let source = OrderedSource::new(config(&fixture).await).unwrap();
        let declared = declared();

        for request in [declared.to_vec(), vec![declared[0].clone()]] {
            let SortOrderPushdownResult::Exact { inner } =
                source.try_pushdown_sort(&request).unwrap()
            else {
                panic!("a prefix of the declared ordering must push down exactly: {request:?}");
            };
            assert!(satisfies(inner.as_ref(), &declared));
        }

        for request in [
            vec![declared[1].clone()],
            vec![declared[0].reverse(), declared[1].reverse()],
        ] {
            match source.try_pushdown_sort(&request).unwrap() {
                SortOrderPushdownResult::Exact { .. } => {
                    panic!("{request:?} must not push down exactly")
                }
                SortOrderPushdownResult::Inexact { inner } => {
                    assert!(!satisfies(inner.as_ref(), &declared), "{request:?}");
                }
                SortOrderPushdownResult::Unsupported => {}
            }
        }
    });
}

#[test]
fn statistics_partitioning_and_display_are_the_wrapped_scans() {
    let fixture = touching_files();
    block_on(async {
        let config = config(&fixture).await;
        let source = OrderedSource::new(config.clone()).unwrap();

        for partition in [None, Some(0)] {
            assert_eq!(
                source.partition_statistics(partition).unwrap(),
                config.partition_statistics(partition).unwrap(),
                "{partition:?}"
            );
        }
        // Partitioning equality treats unknown partitionings as unequal, so compare their text.
        assert_eq!(
            source.output_partitioning().to_string(),
            config.output_partitioning().to_string()
        );
        let display = |source: Arc<dyn DataSource>| {
            displayable(&DataSourceExec::new(source))
                .one_line()
                .to_string()
        };
        assert_eq!(display(Arc::new(source)), display(Arc::new(config)));
    });
}

#[test]
fn execution_reads_the_wrapped_scans_rows() {
    let fixture = Arc::new(touching_files());
    let (wrapped, direct) = pipeline::run(
        move |ctx| async move {
            fixture.register(&ctx);
            let config = config(&fixture).await;
            let source = OrderedSource::new(config.clone())?;
            let rows = |source: Arc<dyn DataSource>| {
                let task = ctx.task_ctx();
                async move {
                    let batches: Vec<RecordBatch> = source.open(0, task)?.try_collect().await?;
                    Ok::<_, DataFusionError>(batches)
                }
            };
            Ok((rows(Arc::new(source)).await?, rows(Arc::new(config)).await?))
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap();

    assert_eq!(wrapped, direct);
    assert_eq!(wrapped.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
}
