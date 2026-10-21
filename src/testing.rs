//! Test support: what module tests, crate tests, benchmarks, and examples build their inputs from
//! and observe their outputs with.
//!
//! All test support lives here. A module is `cfg(test)` unless a crate test, benchmark, or example
//! needs it; those compile as separate crates, so the support they need must be public library
//! code. A helper one suite uses stays in that suite's files, and moves into a module here once a
//! second suite needs it. Within a module here, items are placed by concept, not by which tests use
//! them. Production modules never import from here.
//!
//! Public:
//!
//! - [`fixture`] builds small stored datasets laid out like the real datasets, and reads back what
//!   a test wrote.
//! - [`memory_store`] is the in-memory object store adapter.
//! - [`generated`] builds generated tables.
//!
//! `cfg(test)`:
//!
//! - `columns` reads a record batch's columns as plain values and asserts a schema's columns.
//! - `formulation_plans` discovers datasets over the shared fixtures and plans formulations over
//!   them through a sink, for the suites of `formulation` and its plan builders.
//! - `formulations` builds formulations and split points from compact test notation.
//! - `metered_format` is a file format adapter that records, scripts, and gates metadata inference.
//! - `plan_shape` observes a physical plan's shape and asserts the shapes this crate relies on.
//! - `probe_series` makes synthetic series of progress samples and the decisions a probe would make
//!   over them.
//! - `recording_cache` is a file statistics cache adapter that records the keys read and written.
//! - `run_records` builds run records for tests that assert on none of their facts.
//! - `session` builds the hostile session config.

#[cfg(test)]
pub(crate) mod columns;
pub mod fixture;
#[cfg(test)]
pub(crate) mod formulation_plans;
#[cfg(test)]
pub(crate) mod formulations;
pub mod generated;
pub mod memory_store;
#[cfg(test)]
pub(crate) mod metered_format;
#[cfg(test)]
pub(crate) mod plan_shape;
#[cfg(test)]
pub(crate) mod probe_series;
#[cfg(test)]
pub(crate) mod recording_cache;
#[cfg(test)]
pub(crate) mod run_records;
#[cfg(test)]
pub(crate) mod session;

#[cfg(test)]
mod tests;
