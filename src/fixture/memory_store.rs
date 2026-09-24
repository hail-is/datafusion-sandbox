use super::failing_writes::FailingWrites;

use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use std::sync::Arc;

/// A fresh in-memory object store and the URL a session reaches it by.
///
/// Clones share the store. Every shared dataset fixture holds one; a planning
/// test holds one with nothing in it and builds metadata-only tables over it.
#[derive(Clone, Debug)]
pub struct MemoryStore {
    url: ObjectStoreUrl,
    store: Arc<dyn ObjectStore>,
}

impl MemoryStore {
    /// An empty store at `memory://{name}`. The name is the URL authority, so it
    /// must not contain a slash. Stores are registered per session, so names only
    /// need to read well in plan text and errors, not be globally unique.
    ///
    /// # Panics
    ///
    /// Panics if `name` is not a valid URL authority.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self::with_store(name, Arc::new(InMemory::new()))
    }

    /// An empty store at `memory://{name}` on which every write under `prefix`, a path within
    /// the store such as `metrics/runs`, fails, and every other operation succeeds as on
    /// [`MemoryStore::new`]. A test puts its dataset, output, and metrics directory on one such
    /// store to make exactly one of their writes fail.
    ///
    /// # Panics
    ///
    /// Panics if `name` is not a valid URL authority.
    #[must_use]
    pub fn failing_writes_under(name: &str, prefix: &str) -> Self {
        Self::with_store(name, Arc::new(FailingWrites::new(Path::from(prefix))))
    }

    fn with_store(name: &str, store: Arc<dyn ObjectStore>) -> Self {
        let url = ObjectStoreUrl::parse(format!("memory://{name}"))
            .unwrap_or_else(|error| panic!("parsing the {name} memory store URL: {error}"));
        Self { url, store }
    }

    /// The URL to construct tables against and to register under.
    #[must_use]
    pub const fn url(&self) -> &ObjectStoreUrl {
        &self.url
    }

    /// The store itself, for direct puts and reads and for footer inference.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Registers the store on the session's runtime under `url`.
    pub fn register(&self, ctx: &SessionContext) {
        ctx.register_object_store(self.url.as_ref(), Arc::clone(&self.store));
    }
}
