//! The integration suite uses one Cargo test target to avoid linking and starting
//! a full DataFusion binary for every test module. Add new integration test
//! modules here and keep their source files under `tests/it/`; a Rust file placed
//! directly under `tests/` becomes a separate test target.

mod cli;
mod combiner_run;
mod dataset;
mod fixture;
mod format;
mod formulation;
mod pipeline;
