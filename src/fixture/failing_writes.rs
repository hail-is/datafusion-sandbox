use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, memory::InMemory, path::Path,
};
use std::fmt;

/// An in-memory store that fails every write under one prefix and serves everything else.
///
/// It delegates every operation to its inner store, except a put, a multipart upload, or a copy
/// whose destination is under the prefix, which fails before anything is stored. Prefixes match whole path
/// segments, as listing does.
#[derive(Debug)]
pub(super) struct FailingWrites {
    inner: InMemory,
    prefix: Path,
}

impl FailingWrites {
    pub(super) fn new(prefix: Path) -> Self {
        Self {
            inner: InMemory::new(),
            prefix,
        }
    }

    fn check(&self, location: &Path) -> Result<()> {
        if location.prefix_matches(&self.prefix) {
            return Err(object_store::Error::Generic {
                store: "FailingWrites",
                source: format!(
                    "writes under '{}' fail, so '{location}' was not written",
                    self.prefix
                )
                .into(),
            });
        }
        Ok(())
    }
}

impl fmt::Display for FailingWrites {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "FailingWrites({}, under '{}')",
            self.inner, self.prefix
        )
    }
}

#[async_trait]
impl ObjectStore for FailingWrites {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.check(location)?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.check(location)?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
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
        self.check(to)?;
        self.inner.copy_opts(from, to, options).await
    }
}
