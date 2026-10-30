use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use std::{fmt, sync::Arc};
use tokio::sync::watch;

/// The gate a store with gated reads holds its reads at, until the test releases it.
///
/// Clones share the gate. Once released it stays open.
#[derive(Clone, Debug)]
pub struct ReadGate {
    state: Arc<GateState>,
}

#[derive(Debug)]
struct GateState {
    /// How many reads have reached the gate.
    reached: watch::Sender<usize>,
    /// Whether the gate has been released.
    released: watch::Sender<bool>,
}

impl ReadGate {
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(GateState {
                reached: watch::Sender::new(0),
                released: watch::Sender::new(false),
            }),
        }
    }

    /// Waits until a read has reached the gate: one held there now, or one that was held and has
    /// since been dropped or released.
    pub async fn reached(&self) {
        let mut reached = self.state.reached.subscribe();
        // The sender lives as long as `self`, so the wait cannot fail.
        drop(reached.wait_for(|&count| count > 0).await);
    }

    /// Opens the gate: every held read proceeds, and no later read is held.
    pub fn release(&self) {
        self.state.released.send_replace(true);
    }

    /// Holds a read until the gate is released.
    async fn hold(&self) {
        let mut released = self.state.released.subscribe();
        self.state
            .reached
            .send_modify(|count| *count = count.saturating_add(1));
        drop(released.wait_for(|&released| released).await);
    }
}

/// A store that holds every read under one prefix at a [`ReadGate`] and serves everything else.
///
/// It delegates every operation to its inner store, holding a get, a ranged get, or a head whose
/// location is under the prefix until the gate is released. Writes, deletes, and listings are
/// never held. Prefixes match whole path segments, as listing does.
#[derive(Debug)]
pub(super) struct GatedReads {
    inner: Arc<dyn ObjectStore>,
    prefix: Path,
    gate: ReadGate,
}

impl GatedReads {
    pub(super) fn new(inner: Arc<dyn ObjectStore>, prefix: Path, gate: ReadGate) -> Self {
        Self {
            inner,
            prefix,
            gate,
        }
    }
}

impl fmt::Display for GatedReads {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "GatedReads({}, under '{}')",
            self.inner, self.prefix
        )
    }
}

#[async_trait]
impl ObjectStore for GatedReads {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if location.prefix_matches(&self.prefix) {
            self.gate.hold().await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
