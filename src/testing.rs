//! Public test support: what crate tests, benchmarks, and examples build their inputs from.
//!
//! These compile as separate crates, so the support they need must be public library code. A
//! module moves here once one of them needs it; support only module tests use stays `cfg(test)` in
//! a `tests` subtree. Within a module here, items are placed by concept, not by which tests use
//! them. Production modules never import from here.
//!
//! - [`fixture`] builds small stored datasets laid out like the real datasets, and reads back what
//!   a test wrote.
//! - [`memory_store`] is the in-memory object store adapter.
//! - [`generated`] builds generated tables.

pub mod fixture;
pub mod generated;
pub mod memory_store;
