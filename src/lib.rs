//! Prototypes and benchmarks for comparing `DataFusion` formulations of Hail-style genomics
//! pipelines.
//!
//! - [`combiner_run`] resolves and executes one combiner run.
//! - [`mod@format`] describes supported input and output file formats.
//! - [`formulation`] builds the alternative combiner plans under comparison.
//! - [`locus`] describes the supported stored locus representations, split points, and locus
//!   intervals.
//! - [`ordered_frame`] carries a formulation's deferred rows, ordering, and output layout.
//! - [`metrics_directory`] lays out the run tables under a metrics directory, fills and records a
//!   run's run record, run metrics and progress samples there, and replays the shadow probes it
//!   holds.
//! - [`pipeline`] runs a pipeline to completion on separate CPU and IO runtimes.
//! - [`row_ordering`] declares row orderings in row components and expands them into stored fields
//!   under a locus representation.
//! - [`sample`] defines the field that carries each row's sample id.
//! - [`sink`] ends a plan in a sink that requires the combiner's ordering, of the whole or of every
//!   partition.
//! - [`stored`] reads stored tables under a row ordering: datasets and locus-sorted tables.
//! - [`throughput_probe`] holds a throughput probe's settings and progress samples, and decides
//!   when it stops.
//! - [`mod@write`] builds and executes file writes.
//! - [`testing`] holds all test support: dataset fixtures, the in-memory object store, and
//!   generated tables, public for crate tests, benchmarks, and examples, and `cfg(test)` support
//!   that only module tests use. No module above imports it.
//!
//! See the [project glossary](../GLOSSARY.md) for domain vocabulary.

// `debug_assertions` here is a proxy for dev builds. Optimized builds don't trigger the linker
// warning.
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
/// `CXXFLAGS` that matches the `-Ctarget-cpu` in `RUSTFLAGS`; see docs/snmalloc.md.
#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOCATOR: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

pub mod combiner_run;
pub mod format;
pub mod formulation;
pub mod locus;
pub mod metrics_directory;
pub mod ordered_frame;
pub mod pipeline;
pub mod row_ordering;
pub mod sample;
pub mod sink;
pub mod stored;
pub mod testing;
pub mod throughput_probe;
pub mod write;

#[cfg(test)]
mod tests;
