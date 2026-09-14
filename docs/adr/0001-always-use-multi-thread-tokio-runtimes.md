# Always use multi-thread Tokio runtimes

A pipeline's CPU runtime and IO runtime are both built with
`tokio::runtime::Builder::new_multi_thread()` at every thread count, including `--threads 1`.
Neither is ever a current-thread runtime. The two runtimes agree here for unrelated reasons, so
they are constructed separately rather than through a shared helper, and are free to diverge.

## Why the CPU runtime is multi-thread

DataFusion's `spawn_buffered` (`datafusion-physical-plan/src/common.rs`) keys off the runtime
*flavor*, not the thread count:

```rust
match tokio::runtime::Handle::try_current() {
    Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => { /* spawn */ }
    _ => input,   // current-thread: the stream is returned unbuffered
}
```

Three call sites in DataFusion 55 depend on it: `SortPreservingMergeExec`
(`sorts/sort_preserving_merge.rs`), each in-memory sorted run `SortExec` feeds to its internal
merge (`sorts/sort.rs`, in `in_mem_sort_stream`), and spill reads (`spill/spill_manager.rs`). On a
current-thread runtime all three quietly lose their buffering.

For the combiners this matters most at `SortPreservingMergeExec`, and the cost is *request
concurrency* rather than running more work at once. The merge's demand is inherently serial: it needs the
next batch only from whichever input stream it just drained. Unbuffered, it issues one read and
waits for it, putting read latency on the critical path. Buffered, all inputs become tasks each
reading a batch ahead, so reads for every sample are in flight at once — and a single worker thread
multiplexes those waits perfectly well. So the buffering is worth having even at one thread, which
is exactly the configuration a current-thread runtime would have taken away.

Only the CPU runtime's flavor is observable to DataFusion: the pipeline is spawned with
`spawn_on(.., cpu_runtime.handle())`, so `Handle::try_current()` inside execution always resolves
to the CPU runtime. `src/tests/pipeline.rs` asserts this from inside a one-thread pipeline, which is
the same vantage point `spawn_buffered` sees.

## Why the IO runtime is multi-thread

Nothing observes the IO runtime's flavor, so this is a throughput choice rather than a semantic
one. TLS record decryption and HTTP framing for every object store request run on this runtime,
and TLS decryption is CPU-bound at roughly 1–3 GB/s per core with AES-NI. A current-thread IO
runtime would funnel all of it through one thread and cap read throughput regardless of how many
CPU workers were available.

At one thread the choice is close to arbitrary — current-thread versus one worker differs by a
single parked thread and one scheduler's bookkeeping, since the spawn onto the IO handle is issued
from a CPU worker and goes through a remote queue either way. Scaling with the thread count at
every value avoids a special case whose only justification would be a saving too small to measure.

## What was rejected

Keeping current-thread runtimes as a benchmarkable option, so both could be timed. Rejected as not
worth the configuration surface: it needs a flag, a sum type to stop `(current_thread, threads: 8)`
from being expressible, and a permanent extra axis on every benchmark grid. If the question returns,
the reversal is a `--current-thread` flag that switches the *CPU* runtime only, so that it stays a
one-variable comparison against `--threads 1`.

## Consequences

`--threads 1` changed meaning. It previously built current-thread runtimes, so any timings recorded
under it before this decision measured the unbuffered plan and are not comparable with later runs.
