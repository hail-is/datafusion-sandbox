//! The store that fails every write under one prefix and serves everything else.

use crate::testing::{fixture::block_on, memory_store::failing_writes::FailingWrites};

use futures::{StreamExt, TryStreamExt};
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart, memory::InMemory, path::Path,
};
use std::sync::Arc;

const PREFIX: &str = "metrics/runs";

/// Locations beneath [`PREFIX`]: the prefix itself, a child, and a deeper descendant.
const BENEATH: [&str; 3] = [PREFIX, "metrics/runs/run-a", "metrics/runs/deeper/run-a"];

/// Locations outside [`PREFIX`], including ones that only share its leading characters.
const OUTSIDE: [&str; 4] = [
    "metrics/runs-old/run-a",
    "metrics/run",
    "metrics/progress/run-a",
    "elsewhere",
];

/// A failing store over a fresh inner store, and the inner store, to observe what was stored.
fn stores() -> (FailingWrites, Arc<dyn ObjectStore>) {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    (
        FailingWrites::new(Arc::clone(&inner), Path::from(PREFIX)),
        inner,
    )
}

fn stored(inner: &Arc<dyn ObjectStore>) -> Vec<Path> {
    let mut locations: Vec<Path> =
        block_on(inner.list(None).map_ok(|meta| meta.location).try_collect()).unwrap();
    locations.sort();
    locations
}

/// The writes the store checks.
#[derive(Clone, Copy, Debug)]
enum Write {
    Put,
    Multipart,
    /// A copy from the inner store's `source`.
    Copy,
}

const WRITES: [Write; 3] = [Write::Put, Write::Multipart, Write::Copy];

impl Write {
    /// Writes to `location` through `store`.
    async fn to(self, store: &FailingWrites, location: &Path) -> object_store::Result<()> {
        match self {
            Self::Put => store
                .put(location, PutPayload::from_static(b"put"))
                .await
                .map(drop),
            Self::Multipart => {
                let mut upload = WriteMultipart::new(store.put_multipart(location).await?);
                upload.write(b"multipart");
                upload.finish().await.map(drop)
            }
            Self::Copy => store.copy(&Path::from("source"), location).await,
        }
    }
}

#[test]
fn every_write_beneath_the_prefix_fails_naming_it_and_stores_nothing() {
    for kind in WRITES {
        for location in BENEATH {
            let (store, inner) = stores();
            block_on(inner.put(&Path::from("source"), PutPayload::from_static(b"source"))).unwrap();

            let error = block_on(kind.to(&store, &Path::from(location))).unwrap_err();

            let message = error.to_string();
            assert!(message.contains(PREFIX), "{kind:?} {location}: {message}");
            assert!(message.contains(location), "{kind:?} {location}: {message}");
            assert_eq!(
                stored(&inner),
                [Path::from("source")],
                "{kind:?} {location}"
            );
        }
    }
}

#[test]
fn every_write_outside_the_prefix_reaches_the_inner_store() {
    for kind in WRITES {
        for location in OUTSIDE {
            let (store, inner) = stores();
            block_on(inner.put(&Path::from("source"), PutPayload::from_static(b"source"))).unwrap();

            block_on(kind.to(&store, &Path::from(location))).unwrap();

            assert!(
                stored(&inner).contains(&Path::from(location)),
                "{kind:?} {location}"
            );
        }
    }
}

#[test]
fn a_copy_from_beneath_the_prefix_to_outside_it_succeeds() {
    let (store, inner) = stores();
    let source = Path::from("metrics/runs/run-a");
    block_on(inner.put(&source, PutPayload::from_static(b"a"))).unwrap();

    block_on(store.copy(&source, &Path::from("copied"))).unwrap();

    assert_eq!(stored(&inner), [Path::from("copied"), source]);
}

#[test]
fn reads_lists_and_deletes_beneath_the_prefix_reach_the_inner_store() {
    let (store, inner) = stores();
    let location = Path::from("metrics/runs/run-a");
    block_on(inner.put(&location, PutPayload::from_static(b"a"))).unwrap();

    let read = block_on(async { store.get(&location).await?.bytes().await }).unwrap();
    let listed: Vec<Path> = block_on(
        store
            .list(Some(&Path::from(PREFIX)))
            .map_ok(|meta| meta.location)
            .try_collect(),
    )
    .unwrap();
    let delimited = block_on(store.list_with_delimiter(Some(&Path::from("metrics")))).unwrap();
    let deleted: Vec<Path> = block_on(
        store
            .delete_stream(futures::stream::iter([Ok(location.clone())]).boxed())
            .try_collect(),
    )
    .unwrap();

    assert_eq!(read.as_ref(), b"a");
    assert_eq!(listed, std::slice::from_ref(&location));
    assert_eq!(delimited.common_prefixes, [Path::from(PREFIX)]);
    assert_eq!(deleted, [location]);
    assert_eq!(stored(&inner), Vec::<Path>::new());
}
