//! Prototypes and benchmarks for comparing `DataFusion` formulations of Hail-style genomics pipelines.
//!
//! - [`combiner_run`] resolves and executes one combiner run.
//! - [`cpu_runtime`] builds the runtime used for plan execution.
//! - [`dataset`] describes and reads stored datasets.
//! - [`file_order`] recovers the order of a sorted table's files from their statistics.
//! - [`format`] reads and writes supported file formats.
//! - [`formulation`] builds the alternative combiner plans under comparison.
//! - [`locus`] describes the supported stored locus representations.
//! - [`pipeline`] runs a pipeline to completion on separate CPU and IO runtimes.
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
/// It lives here rather than in `main.rs` so that the benchmarks and the integration tests, each
/// its own binary linking this library, allocate the same way the CLI does. Building this feature
/// takes `cmake` and a `CXXFLAGS` that matches the `-Ctarget-cpu` in `RUSTFLAGS`; see the README.
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
pub mod pipeline;
pub mod sorted_table;
