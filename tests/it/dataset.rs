use crate::fixture;

use datafusion::{
    arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    common::DataFusionError,
    datasource::listing::ListingTableUrl,
    logical_expr::col,
    prelude::SessionContext,
};
use datafusion_sandbox::{
    dataset::{Dataset, DatasetLayout},
    format::{InputFormat, OutputFormat},
};

use std::{future::Future, sync::Arc};

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
        &SessionContext::new(),
        table_path,
        InputFormat::VORTEX,
        DatasetLayout {
            locus_ordering,
            schema: None,
        },
    ))
    .unwrap()
}

#[test]
fn reads_one_sample_with_its_sample_id_attached() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-a", "sample-b"]);

    block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        let dataset = Dataset::discover(
            &SessionContext::new(),
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

        assert!(df.schema().has_column_with_unqualified_name("s"));
        assert!(df.schema().has_column_with_unqualified_name("contig"));
        assert!(df.schema().has_column_with_unqualified_name("alleles"));
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 8);
        for batch in batches {
            let sample_ids = batch
                .column_by_name("s")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!((0..batch.num_rows()).all(|row| sample_ids.value(row) == "sample-b"));
        }
    });
}

#[test]
fn rejects_an_inferred_schema_missing_a_locus_ordering_column() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-a"]);

    let error = block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        Dataset::discover(
            &SessionContext::new(),
            table_path,
            InputFormat::VORTEX,
            DatasetLayout {
                locus_ordering: vec![col("missing").sort(true, false)],
                schema: None,
            },
        )
        .await
        .expect_err("the inferred schema must contain every locus ordering column")
    });

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(
        error.to_string().contains("missing"),
        "unexpected error: {error}"
    );
}

#[test]
fn infers_the_schema_from_one_input_file() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("samples");
    let sample_dir = root.join("s=sample-a");
    std::fs::create_dir_all(&sample_dir).unwrap();

    block_on(async {
        let ctx = SessionContext::new();
        let int_batch = RecordBatch::try_from_iter(vec![(
            "position",
            Arc::new(Int32Array::from(vec![1])) as _,
        )])
        .unwrap();
        let string_batch = RecordBatch::try_from_iter(vec![(
            "position",
            Arc::new(StringArray::from(vec!["one"])) as _,
        )])
        .unwrap();
        for (name, batch) in [("a.vortex", int_batch), ("b.vortex", string_batch)] {
            let path = sample_dir.join(name);
            let df = ctx.read_batch(batch).unwrap();
            OutputFormat::VORTEX
                .write(df, path.to_str().unwrap())
                .await
                .unwrap();
        }

        let table_path = ListingTableUrl::parse(root.to_str().unwrap()).unwrap();
        let dataset = Dataset::discover(
            &ctx,
            table_path,
            InputFormat::VORTEX,
            DatasetLayout {
                locus_ordering: vec![col("position").sort(true, false)],
                schema: None,
            },
        )
        .await
        .expect("incompatible schemas in later files must not be merged");

        assert!(dataset.schema().field_with_name("position").is_ok());
    });
}

#[test]
fn uses_a_pinned_schema_without_inference() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("samples");
    let sample_dir = root.join("s=sample-a");
    std::fs::create_dir_all(&sample_dir).unwrap();
    std::fs::write(sample_dir.join("invalid.vortex"), b"not a vortex file").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "position",
        DataType::Int32,
        false,
    )]));

    let dataset = block_on(async {
        let table_path = ListingTableUrl::parse(root.to_str().unwrap()).unwrap();
        Dataset::discover(
            &SessionContext::new(),
            table_path,
            InputFormat::VORTEX,
            DatasetLayout {
                locus_ordering: vec![col("position").sort(true, false)],
                schema: Some(Arc::clone(&schema)),
            },
        )
        .await
        .expect("a pinned schema must bypass inference")
    });

    assert_eq!(dataset.schema(), &schema);
}

#[test]
fn discovers_the_dataset_sample_set() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-b", "sample-a"]);

    let dataset = block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        Dataset::discover(
            &SessionContext::new(),
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
            &SessionContext::new(),
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
            &SessionContext::new(),
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
            &SessionContext::new(),
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
            &SessionContext::new(),
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

fn allele_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
            col("alleles").sort(true, false),
        ],
        schema: None,
    }
}

fn empty_layout() -> DatasetLayout {
    DatasetLayout {
        locus_ordering: vec![],
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
