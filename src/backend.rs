//! The backend: the Rust side that takes what a frontend sends and runs it. See ADR 0022.
//!
//! A [`Backend`] opens datasets for a frontend to plan against. Opening one hands out a scan
//! description per input table, each in a schema-only table provider the frontend's plans read in
//! place of the input table, and the dataset's stored ordering as plain sort columns. Plan bytes a
//! frontend encodes with the backend's codec hold those descriptions, which the backend reads back.
//!
//! The backend reaches object stores only through the [`ObjectStoreResolver`] it is built with:
//! [`ResolveFromUrls`] in production, or a suite's in-memory stores. Every call that touches a
//! store runs through the pipeline runner.
//!
//! - [`codec`] is the logical extension codec scan descriptions cross in.
//! - [`scan_description`] holds scan descriptions and the schema-only provider that holds one.

pub mod codec;
pub mod scan_description;

#[cfg(test)]
mod tests;

use crate::{
    format::InputFormat,
    pipeline::{self, PipelineOptions},
    row_ordering::{RowOrdering, SortColumn},
    stored::dataset::Dataset,
};
use codec::Codec;
use scan_description::{DescribedScan, ScanDescription};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{DataFusionError, tree_node::TreeNodeRecursion},
    datasource::{listing::ListingTableUrl, source_as_provider},
    error::Result,
    logical_expr::LogicalPlan,
    prelude::SessionContext,
};
use datafusion_proto::bytes::logical_plan_from_bytes_with_extension_codec;
use std::{fmt, num::NonZeroUsize, slice, sync::Arc};

/// Where a backend's runs find the object stores the paths they read and write live on, and the
/// credentials to reach them.
pub trait ObjectStoreResolver: fmt::Debug + Send + Sync {
    /// Options for a run with `threads` threads per runtime that reads and writes `paths`, which
    /// register the stores those paths live on.
    ///
    /// # Errors
    ///
    /// Returns an error naming a path on a store the resolver cannot serve.
    fn pipeline_options(&self, threads: NonZeroUsize, paths: &[&str]) -> Result<PipelineOptions>;
}

/// The production resolver: a store for each path's URL, with credentials from the environment,
/// as [`PipelineOptions::for_paths`] resolves them.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResolveFromUrls;

impl ObjectStoreResolver for ResolveFromUrls {
    fn pipeline_options(&self, threads: NonZeroUsize, paths: &[&str]) -> Result<PipelineOptions> {
        PipelineOptions::for_paths(threads, paths.iter().copied())
    }
}

/// The in-process backend.
#[derive(Debug)]
pub struct Backend {
    object_stores: Arc<dyn ObjectStoreResolver>,
}

/// A dataset a frontend plans against, as [`Backend::open_dataset`] hands it out.
#[derive(Debug)]
pub struct OpenedDataset {
    /// The dataset's input tables, in name order.
    pub input_tables: Vec<InputTableScan>,
    /// The schema every scan reads: the dataset's rows with their sample.
    pub schema: SchemaRef,
    /// The order the dataset's rows are stored in, under the row ordering it was opened with.
    pub ordering: Vec<SortColumn>,
}

/// One input table of an opened dataset.
#[derive(Debug)]
pub struct InputTableScan {
    /// The samples the input table covers, in sorted order.
    pub sample_set: Vec<String>,
    /// The table provider a frontend plan reads in place of the input table.
    pub scan: Arc<DescribedScan>,
}

impl Backend {
    #[must_use]
    pub fn new(object_stores: Arc<dyn ObjectStoreResolver>) -> Self {
        Self { object_stores }
    }

    /// Opens the dataset at `path`, discovering it as [`Dataset::discover`] does, stored in
    /// `input_format` and declared in `row_ordering`.
    ///
    /// Each scan description it hands out names `path` as given, so a backend reopens it the way
    /// it was opened.
    ///
    /// # Errors
    ///
    /// Returns an error if the resolver cannot serve `path`, discovery fails, an input table cannot
    /// be read, or the input tables' scans read different schemas.
    pub fn open_dataset(
        &self,
        path: &str,
        input_format: InputFormat,
        row_ordering: RowOrdering,
    ) -> Result<OpenedDataset> {
        let options = self
            .object_stores
            .pipeline_options(pipeline::default_thread_count(), &[path])?;
        let path = path.to_string();
        pipeline::run(
            move |ctx| async move {
                let dataset = Dataset::discover(
                    &ctx,
                    ListingTableUrl::parse(&path)?,
                    input_format.clone(),
                    row_ordering.clone(),
                    None,
                )
                .await?;
                let ordering = dataset.query_ordering(&row_ordering)?.sort_columns();
                let mut input_tables = Vec::with_capacity(dataset.input_tables().len());
                for input_table in dataset.input_tables() {
                    let name = input_table.name().to_string();
                    let frame = dataset
                        .restrict_to(slice::from_ref(&name))?
                        .read(&ctx)
                        .await?;
                    let description = ScanDescription {
                        dataset_path: path.clone(),
                        input_format: input_format.clone(),
                        row_ordering: row_ordering.clone(),
                        input_table: name,
                    };
                    input_tables.push(InputTableScan {
                        sample_set: input_table.sample_set().to_vec(),
                        scan: Arc::new(DescribedScan::new(
                            description,
                            Arc::clone(frame.schema().inner()),
                        )),
                    });
                }
                let schema = scan_schema(&path, &input_tables)?;
                Ok(OpenedDataset {
                    input_tables,
                    schema,
                    ordering,
                })
            },
            options,
        )
    }

    /// The scan descriptions `plan_bytes` read, each once, in the order the plan first reads them.
    /// The bytes are a logical plan a frontend encoded with the backend's [codec].
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not such a plan, or it reads a table that is not a scan
    /// description.
    pub fn scan_descriptions(&self, plan_bytes: &[u8]) -> Result<Vec<ScanDescription>> {
        let ctx = SessionContext::new_with_config(pipeline::session_config());
        let plan =
            logical_plan_from_bytes_with_extension_codec(plan_bytes, &ctx.task_ctx(), &Codec)?;
        let mut descriptions = Vec::<ScanDescription>::new();
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let provider = source_as_provider(&scan.source)?;
                let Some(described) = provider.downcast_ref::<DescribedScan>() else {
                    return Err(DataFusionError::Plan(format!(
                        "the plan reads table '{}', which is not a scan description",
                        scan.table_name
                    )));
                };
                if !descriptions.contains(described.description()) {
                    descriptions.push(described.description().clone());
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(descriptions)
    }
}

/// The schema every one of `input_tables`' scans reads.
fn scan_schema(path: &str, input_tables: &[InputTableScan]) -> Result<SchemaRef> {
    let mut schemas = input_tables.iter().map(|table| table.scan.schema_ref());
    let Some(schema) = schemas.next() else {
        return Err(DataFusionError::Internal(format!(
            "dataset '{path}' was opened with no input tables"
        )));
    };
    if let Some(other) = schemas.find(|other| *other != schema) {
        return Err(DataFusionError::Internal(format!(
            "the input tables of dataset '{path}' read different schemas: {schema:?} and {other:?}"
        )));
    }
    Ok(Arc::clone(schema))
}
