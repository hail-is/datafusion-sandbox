use crate::fixture;

use datafusion::{
    arrow::datatypes::DataType, datasource::listing::ListingTableUrl, logical_expr::col,
    object_store::local::LocalFileSystem, prelude::SessionContext,
};
use datafusion_sandbox::{
    dataset::{Dataset, DatasetLayout},
    format::InputFormat,
};

use std::future::Future;

#[test]
fn accepts_an_exact_locus_ordering() {
    let dataset = dataset_with_ordering(vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
    ]);

    dataset
        .check_ordering(&[
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ])
        .unwrap();
}

#[test]
fn accepts_a_finer_locus_ordering() {
    let dataset = dataset_with_ordering(vec![
        col("contig").sort(true, false),
        col("position").sort(true, false),
        col("alleles").sort(true, false),
    ]);

    dataset
        .check_ordering(&[
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ])
        .unwrap();
}

#[test]
fn rejects_an_insufficient_locus_ordering() {
    let dataset = dataset_with_ordering(vec![col("contig").sort(true, false)]);

    let error = dataset
        .check_ordering(&[
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ])
        .expect_err("the dataset does not satisfy the required ordering");

    let message = error.to_string();
    assert!(
        message.contains("locus ordering"),
        "unexpected error: {message}"
    );
    assert!(message.contains("position"), "unexpected error: {message}");
}

fn dataset_with_ordering(locus_ordering: Vec<datafusion::logical_expr::SortExpr>) -> Dataset {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-a"]);
    let table_path = ListingTableUrl::parse(&root).unwrap();
    block_on(Dataset::discover(
        &LocalFileSystem::new(),
        table_path,
        InputFormat::VORTEX,
        DatasetLayout {
            locus_ordering,
            partition_columns: vec![],
            schema: None,
        },
    ))
    .unwrap()
}

#[test]
fn reads_a_whole_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_parquet_sample_tables(dir.path(), &["sample-a", "sample-b"]);

    block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        let dataset = Dataset::discover(
            &LocalFileSystem::new(),
            table_path,
            InputFormat::PARQUET,
            reference_layout(),
        )
        .await
        .unwrap();
        let df = dataset.read(&SessionContext::new()).await.unwrap();

        assert!(df.schema().has_column_with_unqualified_name("s"));
        assert!(df.schema().has_column_with_unqualified_name("contig"));
        assert_eq!(df.count().await.unwrap(), 16);
    });
}

#[test]
fn reads_only_the_restricted_sample_set() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_parquet_sample_tables(dir.path(), &["sample-a", "sample-b"]);

    block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        let dataset = Dataset::discover(
            &LocalFileSystem::new(),
            table_path,
            InputFormat::PARQUET,
            reference_layout(),
        )
        .await
        .unwrap()
        .restrict_to(&["sample-b".to_string()])
        .unwrap();
        let df = dataset.read(&SessionContext::new()).await.unwrap();

        assert_eq!(df.count().await.unwrap(), 8);
    });
}

#[test]
fn reads_one_sample_without_the_sample_partition_column() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-a", "sample-b"]);

    block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        let dataset = Dataset::discover(
            &LocalFileSystem::new(),
            table_path,
            InputFormat::VORTEX,
            allele_layout(),
        )
        .await
        .unwrap();
        let df = dataset
            .read_sample(&SessionContext::new(), "sample-b")
            .await
            .unwrap();

        assert!(!df.schema().has_column_with_unqualified_name("s"));
        assert!(df.schema().has_column_with_unqualified_name("contig"));
        assert!(df.schema().has_column_with_unqualified_name("alleles"));
        assert_eq!(df.count().await.unwrap(), 8);
    });
}

#[test]
fn discovers_the_dataset_sample_set() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-b", "sample-a"]);

    let dataset = block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        Dataset::discover(
            &LocalFileSystem::new(),
            table_path,
            InputFormat::VORTEX,
            empty_layout(),
        )
        .await
        .unwrap()
    });

    assert_eq!(dataset.sample_set(), ["sample-a", "sample-b"]);
}

#[test]
fn rejects_a_dataset_with_no_samples() {
    let dir = tempfile::tempdir().unwrap();

    let error = block_on(async {
        let table_path = ListingTableUrl::parse(dir.path().to_str().unwrap()).unwrap();
        Dataset::discover(
            &LocalFileSystem::new(),
            table_path,
            InputFormat::VORTEX,
            empty_layout(),
        )
        .await
        .expect_err("an empty directory is not a dataset")
    });

    assert!(
        error.to_string().contains("no samples"),
        "unexpected error: {error}"
    );
}

#[test]
fn narrows_the_dataset_sample_set() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-a", "sample-b"]);

    let dataset = block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        Dataset::discover(
            &LocalFileSystem::new(),
            table_path,
            InputFormat::VORTEX,
            empty_layout(),
        )
        .await
        .unwrap()
        .restrict_to(&["sample-b".to_string()])
        .unwrap()
    });

    assert_eq!(dataset.sample_set(), ["sample-b"]);
}

#[test]
fn rejects_an_empty_requested_sample_set() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-a"]);

    let error = block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        Dataset::discover(
            &LocalFileSystem::new(),
            table_path,
            InputFormat::VORTEX,
            empty_layout(),
        )
        .await
        .unwrap()
        .restrict_to(&[])
        .expect_err("a dataset must retain at least one sample")
    });

    assert_eq!(
        error.to_string(),
        "Execution error: requested sample set contains no samples"
    );
}

#[test]
fn rejects_requested_samples_that_are_not_in_the_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-a"]);

    let error = block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        Dataset::discover(
            &LocalFileSystem::new(),
            table_path,
            InputFormat::VORTEX,
            empty_layout(),
        )
        .await
        .unwrap()
        .restrict_to(&["missing-b".to_string(), "missing-a".to_string()])
        .expect_err("unknown sample ids must fail")
    });

    let message = error.to_string();
    assert!(message.contains("missing-a"), "unexpected error: {message}");
    assert!(message.contains("missing-b"), "unexpected error: {message}");
}

fn reference_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ],
        partition_columns: vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ],
        schema: None,
    }
}

fn allele_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
            col("alleles").sort(true, false),
        ],
        partition_columns: vec![
            ("s".to_string(), DataType::Utf8),
            ("contig".to_string(), DataType::Utf8),
        ],
        schema: None,
    }
}

fn empty_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: vec![],
        partition_columns: vec![],
        schema: None,
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
