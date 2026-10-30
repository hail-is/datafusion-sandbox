//! The store that holds every read under one prefix at a gate and serves everything else.

use crate::testing::{
    fixture::block_on,
    memory_store::gated_reads::{GatedReads, ReadGate},
};

use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
use std::sync::Arc;

const PREFIX: &str = "data/table";

/// Locations beneath [`PREFIX`]: the prefix itself, a child, and a deeper descendant.
const BENEATH: [&str; 3] = [PREFIX, "data/table/a", "data/table/s=a/b"];

/// Locations outside [`PREFIX`], including ones that only share its leading characters.
const OUTSIDE: [&str; 3] = ["data/table-old/a", "data/tab", "elsewhere"];

/// A gated store over a fresh inner store holding one object at each of [`BENEATH`] and
/// [`OUTSIDE`], its gate, and the inner store.
fn stores() -> (GatedReads, ReadGate, Arc<dyn ObjectStore>) {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for location in BENEATH.into_iter().chain(OUTSIDE) {
        block_on(inner.put(&Path::from(location), PutPayload::from_static(b"bytes"))).unwrap();
    }
    let gate = ReadGate::new();
    (
        GatedReads::new(Arc::clone(&inner), Path::from(PREFIX), gate.clone()),
        gate,
        inner,
    )
}

/// The reads the store gates.
#[derive(Clone, Copy, Debug)]
enum Read {
    Get,
    Range,
    Ranges,
    Head,
}

const READS: [Read; 4] = [Read::Get, Read::Range, Read::Ranges, Read::Head];

impl Read {
    /// Reads `location` through `store`, and hands back how many bytes it read.
    async fn of(self, store: &GatedReads, location: &Path) -> object_store::Result<usize> {
        match self {
            Self::Get => Ok(store.get(location).await?.bytes().await?.len()),
            Self::Range => Ok(store.get_range(location, 1..3).await?.len()),
            Self::Ranges => Ok(store
                .get_ranges(location, &[0..1, 2..4])
                .await?
                .iter()
                .map(bytes::Bytes::len)
                .sum()),
            Self::Head => store.head(location).await.map(|_| 0),
        }
    }

    /// Whether this read of `location` is held at the store's gate: it is still pending once the
    /// gate has been reached.
    fn is_held(self, store: &GatedReads, gate: &ReadGate, location: &str) -> bool {
        let location = Path::from(location);
        block_on(async {
            tokio::select! {
                biased;
                read = self.of(store, &location) => {
                    read.unwrap();
                    false
                }
                () = gate.reached() => true,
            }
        })
    }
}

#[test]
fn every_read_beneath_the_prefix_is_held_at_the_gate() {
    for kind in READS {
        for location in BENEATH {
            let (store, gate, _) = stores();

            assert!(kind.is_held(&store, &gate, location), "{kind:?} {location}");
        }
    }
}

#[test]
fn every_read_outside_the_prefix_passes_the_gate() {
    for kind in READS {
        for location in OUTSIDE {
            let (store, gate, _) = stores();

            assert!(
                !kind.is_held(&store, &gate, location),
                "{kind:?} {location}"
            );
        }
    }
}

#[test]
fn a_released_gate_lets_every_held_read_finish_and_holds_no_later_one() {
    let (store, gate, _) = stores();
    let store = Arc::new(store);
    let held: Vec<_> = READS
        .map(|kind| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || block_on(kind.of(&store, &Path::from("data/table/a"))))
        })
        .into_iter()
        .collect();
    block_on(gate.reached());

    gate.release();

    let read: Vec<usize> = held
        .into_iter()
        .map(|thread| thread.join().unwrap().unwrap())
        .collect();
    assert_eq!(read, [5, 2, 3, 0]);
    for kind in READS {
        assert!(!kind.is_held(&store, &gate, "data/table/a"), "{kind:?}");
    }
}

#[test]
fn writes_lists_and_deletes_beneath_the_prefix_reach_the_inner_store_unheld() {
    let (store, _gate, inner) = stores();
    let location = Path::from("data/table/new");

    block_on(store.put(&location, PutPayload::from_static(b"new"))).unwrap();
    block_on(store.copy(&location, &Path::from("data/table/copied"))).unwrap();
    let listed: Vec<Path> = block_on(
        store
            .list(Some(&Path::from(PREFIX)))
            .map_ok(|meta| meta.location)
            .try_collect(),
    )
    .unwrap();
    let delimited = block_on(store.list_with_delimiter(Some(&Path::from("data")))).unwrap();
    let deleted: Vec<Path> = block_on(
        store
            .delete_stream(futures::stream::iter([Ok(location.clone())]).boxed())
            .try_collect(),
    )
    .unwrap();

    assert!(listed.contains(&location), "{listed:?}");
    assert!(
        delimited.common_prefixes.contains(&Path::from(PREFIX)),
        "{delimited:?}"
    );
    assert_eq!(deleted, std::slice::from_ref(&location));
    let remaining: Vec<Path> =
        block_on(inner.list(None).map_ok(|meta| meta.location).try_collect()).unwrap();
    assert!(!remaining.contains(&location), "{remaining:?}");
    assert!(
        remaining.contains(&Path::from("data/table/copied")),
        "{remaining:?}"
    );
}
