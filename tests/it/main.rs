//! The integration suite uses one Cargo test target to avoid linking and starting
//! a full `DataFusion` binary for every test module. Add new integration test
//! modules here and keep their source files under `tests/it/`; a Rust file placed
//! directly under `tests/` becomes a separate test target.

#![cfg(test)]
#![expect(
    clippy::as_conversions,
    reason = "test fixtures cast values whose ranges are controlled by the test"
)]
// `debug_assertions` here is a proxy for dev builds. Optimized builds don't trigger the linker warning.
#![cfg_attr(
    all(target_os = "macos", debug_assertions),
    allow(
        linker_messages,
        reason = "Apple ld falls back to DWARF when the CLI exceeds compact unwind's 16 MiB offset range; rust-lang/rust#159105 tracks this diagnostic"
    )
)]

mod cli;
mod combiner_run;
mod dataset;
mod file_order;
mod fixture;
mod format;
mod formulation;
mod locus;
mod pipeline;
mod sorted_table;
