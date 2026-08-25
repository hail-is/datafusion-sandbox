mod fixture;

use datafusion::{datasource::listing::ListingTableUrl, object_store::local::LocalFileSystem};
use datafusion_sandbox::{Dataset, format::InputFormat};

use std::future::Future;

#[test]
fn discovers_the_dataset_sample_set() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-b", "sample-a"]);

    let dataset = block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        Dataset::discover(&LocalFileSystem::new(), table_path, InputFormat::VORTEX)
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
        Dataset::discover(&LocalFileSystem::new(), table_path, InputFormat::VORTEX)
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
        Dataset::discover(&LocalFileSystem::new(), table_path, InputFormat::VORTEX)
            .await
            .unwrap()
            .restrict_to(&["sample-b".to_string()])
            .unwrap()
    });

    assert_eq!(dataset.sample_set(), ["sample-b"]);
}

#[test]
fn rejects_requested_samples_that_are_not_in_the_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let root = fixture::write_sample_tables(dir.path(), &["sample-a"]);

    let error = block_on(async {
        let table_path = ListingTableUrl::parse(&root).unwrap();
        Dataset::discover(&LocalFileSystem::new(), table_path, InputFormat::VORTEX)
            .await
            .unwrap()
            .restrict_to(&["missing-b".to_string(), "missing-a".to_string()])
            .expect_err("unknown sample ids must fail")
    });

    let message = error.to_string();
    assert!(message.contains("missing-a"), "unexpected error: {message}");
    assert!(message.contains("missing-b"), "unexpected error: {message}");
}

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
