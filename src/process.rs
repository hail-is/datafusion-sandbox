//! The measurement of the running process that a run record carries.
//!
//! A combiner run's plan-level metrics come from `DataFusion`'s operators; what the process as a
//! whole held in memory does not, since the pipeline configures no memory pool and scans and
//! merge trees reserve nothing from one. The kernel's peak resident set size is the figure that
//! sees scan buffers and the pages the allocator keeps resident, under any allocator, and this
//! module reads it. See
//! [ADR 0016](../docs/adr/0016-record-run-metrics-as-wide-parquet-tables.md) for why the memory
//! pool's peak is not recorded instead.

use datafusion::error::{DataFusionError, Result};

use std::{io, mem::MaybeUninit};

/// How many bytes one unit of `getrusage`'s `ru_maxrss` is: macOS reports bytes, Linux
/// kibibytes.
#[cfg(target_os = "macos")]
const BYTES_PER_MAXRSS_UNIT: u64 = 1;
#[cfg(target_os = "linux")]
const BYTES_PER_MAXRSS_UNIT: u64 = 1024;
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
compile_error!("the unit of getrusage's ru_maxrss is known only for macOS and Linux");

/// The process's peak resident set size over its lifetime so far, in bytes whatever the
/// platform.
///
/// A whole-process figure: it counts scan buffers and the pages the allocator keeps resident,
/// under any allocator. The CLI runs one combiner run per process, so there it is the run's peak.
///
/// # Errors
///
/// Returns an error if the kernel refuses the resource-usage query, which it does not for the
/// calling process, or reports a negative peak.
pub fn peak_rss_bytes() -> Result<u64> {
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `RUSAGE_SELF` is a valid subject and `usage` is writable storage of exactly the
    // size `getrusage` fills.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return Err(DataFusionError::Execution(format!(
            "reading the process's resource usage failed: {}",
            io::Error::last_os_error()
        )));
    }
    // SAFETY: on the two platforms this compiles for, a zero status means the kernel wrote the
    // whole `rusage` struct, so every field of `usage` is initialized.
    let usage = unsafe { usage.assume_init() };
    let max_rss = u64::try_from(usage.ru_maxrss).map_err(|error| {
        DataFusionError::Execution(format!(
            "the process's peak resident set size is negative: {error}"
        ))
    })?;
    Ok(max_rss.saturating_mul(BYTES_PER_MAXRSS_UNIT))
}
