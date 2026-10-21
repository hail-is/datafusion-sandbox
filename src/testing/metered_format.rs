//! A file format adapter that records every metadata inference, can script its results, and can
//! hold inferences at a gate so a test controls how many run at once and the order they complete.
//!
//! A format answers statistics and ordering with one `infer_stats_and_ordering` call; its
//! `infer_stats` and `infer_ordering` panic, so a caller that asks for either alone fails the test.
//! Everything else delegates to the wrapped format.

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::{Result, Statistics},
    datasource::{
        file_format::{
            FileFormat, FileMeta, file_compression_type::FileCompressionType,
            parquet::ParquetFormat,
        },
        physical_plan::{FileScanConfig, FileSource},
        table_schema::TableSchema,
    },
    physical_expr::LexOrdering,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

/// How a metered format releases an inference once it has started.
#[derive(Clone, Debug)]
pub(crate) enum Gate {
    /// Release immediately.
    Open,
    /// Hold every inference until `inferences` of them have started at once.
    UntilStarted { inferences: usize },
    /// Complete inferences in exactly this path order. An inference of a path not in the order
    /// panics.
    CompleteInOrder(Vec<String>),
}

/// A file format that records every metadata inference and can script its results.
///
/// Paths are object store locations as strings, such as `a.parquet`.
pub(crate) struct MeteredFormat {
    inner: Arc<dyn FileFormat>,
    scripted: Option<HashMap<String, FileMeta>>,
    gate: Gate,
    started: Mutex<Vec<String>>,
    completed: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
}

impl std::fmt::Debug for MeteredFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MeteredFormat")
            .finish_non_exhaustive()
    }
}

impl MeteredFormat {
    /// Infers through `inner`, with an open gate.
    pub(crate) fn delegating(inner: Arc<dyn FileFormat>) -> Self {
        Self {
            inner,
            scripted: None,
            gate: Gate::Open,
            started: Mutex::new(Vec::new()),
            completed: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
        }
    }

    /// Answers inferences from `scripted`, by path, without touching any store, and delegates
    /// everything else to Parquet. An inference of a path not in `scripted` panics.
    pub(crate) fn scripted(scripted: Vec<(&str, FileMeta)>, gate: Gate) -> Self {
        Self {
            scripted: Some(
                scripted
                    .into_iter()
                    .map(|(path, meta)| (path.to_string(), meta))
                    .collect(),
            ),
            gate,
            ..Self::delegating(Arc::new(ParquetFormat::default()))
        }
    }

    /// The path of every inference started so far, in start order.
    pub(crate) fn started(&self) -> Vec<String> {
        self.started.lock().unwrap().clone()
    }

    /// The path of every inference completed so far, in completion order.
    pub(crate) fn completed(&self) -> Vec<String> {
        self.completed.lock().unwrap().clone()
    }

    /// The most inferences that have been in flight at once.
    pub(crate) fn peak_in_flight(&self) -> usize {
        self.peak_in_flight.load(Ordering::SeqCst)
    }

    async fn wait_for_gate(&self, path: &str) {
        match &self.gate {
            Gate::Open => {}
            Gate::UntilStarted { inferences } => {
                while self.started.lock().unwrap().len() < *inferences {
                    tokio::task::yield_now().await;
                }
            }
            Gate::CompleteInOrder(order) => {
                let turn = order
                    .iter()
                    .position(|scripted| scripted == path)
                    .unwrap_or_else(|| panic!("{path} is not in the completion order"));
                while self.completed.lock().unwrap().len() < turn {
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}

#[async_trait]
impl FileFormat for MeteredFormat {
    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(&self, compression: &FileCompressionType) -> Result<String> {
        self.inner.get_ext_with_compression(compression)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        panic!(
            "statistics for {} must be inferred together with ordering",
            object.location
        )
    }

    async fn infer_ordering(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Option<LexOrdering>> {
        panic!(
            "ordering for {} must be inferred together with statistics",
            object.location
        )
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<FileMeta> {
        let path = object.location.to_string();
        self.started.lock().unwrap().push(path.clone());
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_in_flight.fetch_max(in_flight, Ordering::SeqCst);
        self.wait_for_gate(&path).await;
        let meta = match &self.scripted {
            Some(scripted) => scripted
                .get(&path)
                .cloned()
                .unwrap_or_else(|| panic!("no scripted metadata for {path}")),
            None => {
                self.inner
                    .infer_stats_and_ordering(state, store, table_schema, object)
                    .await?
            }
        };
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.completed.lock().unwrap().push(path);
        Ok(meta)
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(state, conf).await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}
