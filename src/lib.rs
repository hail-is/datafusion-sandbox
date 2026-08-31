//! Prototypes and benchmarks for comparing DataFusion formulations of Hail-style genomics pipelines.
//!
//! - [`combiner_run`] resolves and executes one combiner run.
//! - [`cpu_runtime`] builds the runtime used for plan execution.
//! - [`dataset`] describes and reads stored datasets.
//! - [`format`] reads and writes supported file formats.
//! - [`formulation`] builds the alternative combiner plans under comparison.
//! - [`locus`] describes the supported stored locus representations.
//! - [`pipeline`] runs a pipeline to completion on separate CPU and IO runtimes.
//! - [`sorted_table`] scans files as one ordered partition.
//! - [`synthetic`] builds generated in-memory tables for tests and benchmarks.
//!
//! See the [project glossary](../CONTEXT.md) for domain vocabulary.

pub mod combiner_run;
pub mod cpu_runtime;
pub mod dataset;
pub mod format;
pub mod formulation;
pub mod locus;
pub mod pipeline;
pub mod sorted_table;
pub mod synthetic;
