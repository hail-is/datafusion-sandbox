//! Prototypes and benchmarks for comparing `DataFusion` formulations of Hail-style genomics pipelines.
//!
//! - [`combiner_run`] resolves and executes one combiner run.
//! - [`cpu_runtime`] builds the runtime used for plan execution.
//! - [`dataset`] describes and reads stored datasets.
//! - [`file_order`] recovers the order of a sorted table's files from their statistics.
//! - [`fixture`] builds small stored datasets for tests and benchmarks.
//! - [`format`] reads and writes supported file formats.
//! - [`formulation`] builds the alternative combiner plans under comparison.
//! - [`locus`] describes the supported stored locus representations, split points, and locus
//!   intervals.
//! - [`ordered_frame`] carries a formulation's deferred rows, ordering, and output layout.
//! - [`pipeline`] runs a pipeline to completion on separate CPU and IO runtimes.
//! - [`sink`] ends a plan in a sink that requires the combiner's ordering, of the whole or of
//!   every partition.
//! - [`sorted_table`] scans files as one ordered partition.
//! - [`generated`] builds generated tables for tests and benchmarks.
//!
//! See the [project glossary](../CONTEXT.md) for domain vocabulary.

// `debug_assertions` here is a proxy for dev builds. Optimized builds don't trigger the linker warning.
#![cfg_attr(
    all(test, target_os = "macos", debug_assertions),
    allow(
        linker_messages,
        reason = "Apple ld falls back to DWARF when the CLI exceeds compact unwind's 16 MiB offset range; rust-lang/rust#159105 tracks this diagnostic"
    )
)]

/// The allocator every binary built from this crate uses, when the `snmalloc` feature is on.
///
/// It lives here rather than in `main.rs` so the benchmark, library-test, and CLI crate-test
/// binaries allocate the same way the CLI does. Building this feature takes `cmake` and a
/// `CXXFLAGS` that matches the `-Ctarget-cpu` in `RUSTFLAGS`; see the README.
#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOCATOR: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

pub mod combiner_run;
pub mod cpu_runtime;
pub mod dataset;
pub mod file_order;
pub mod fixture;
pub mod format;
pub mod formulation;
pub mod generated;
pub mod locus;
pub mod ordered_frame;
pub mod pipeline;
pub mod sink;
pub mod sorted_table;

#[cfg(test)]
mod tests;
