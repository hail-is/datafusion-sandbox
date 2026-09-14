use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};
use object_store::{ObjectStore, memory::InMemory};
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
        let url = ObjectStoreUrl::parse(format!("memory://{name}"))
            .unwrap_or_else(|error| panic!("parsing the {name} memory store URL: {error}"));
        Self {
            url,
            store: Arc::new(InMemory::new()),
        }
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
