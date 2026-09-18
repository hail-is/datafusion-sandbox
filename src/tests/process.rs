//! The process measurement a run record carries, read from the process running the tests.

use crate::process;

use std::hint::black_box;

/// Sixty-four mebibytes: the margin by which the test exceeds every earlier peak of the process.
const MARGIN_BYTES: u64 = 64 * 1024 * 1024;

/// The peak resident set size is a live reading in bytes: making more memory resident than the
/// process has ever held raises the next reading by at least the excess, and by no more than the
/// buffer and what the tests running beside this one could hold meanwhile. A reading left in
/// kibibytes fails the first bound; one scaled to bytes twice fails the second.
#[test]
fn the_peak_resident_set_size_rises_with_memory_made_resident() {
    let before = process::peak_rss_bytes().unwrap();
    assert!(before > 0);

    // Filling with a nonzero byte touches every page, so the whole buffer is resident at once.
    let size = before.saturating_add(MARGIN_BYTES);
    let resident = vec![1u8; usize::try_from(size).unwrap()];
    black_box(&resident);
    let after = process::peak_rss_bytes().unwrap();
    drop(resident);

    assert!(
        after >= before.saturating_add(MARGIN_BYTES),
        "the peak rose from {before} only to {after} after {size} bytes were made resident"
    );
    assert!(
        after <= before.saturating_add(size.saturating_mul(2)),
        "the peak rose from {before} to {after} after only {size} bytes were made resident"
    );
}
