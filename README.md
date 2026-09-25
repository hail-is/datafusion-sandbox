This repo is to help us start to experiment with implementing hail style pipelines in datafusion, and to benchmark their performance.

- `benches/example.rs`: Some initial infrastructure for running benchmarks. Run using `cargo bench`. Setting `RUSTFLAGS='-C target-cpu=native'` is probably a good idea.
- `data/`: Data files for use in benchmarks, or just ad hoc experimentation.
- `src/lib.rs`: Definitions of dataframes, for use in benchmarks or ad hoc experimentation.
- `src/main.rs`: A CLI dispatching to the pipelines in `src/`. Run using `cargo run -r -- <subcommand>`.
- `notes/`: A place for notes on datafusion. Has an explainer I had Claude generate on how aggregation works, and how it can take advantage of ordered inputs, and a reference on what each recorded DataFusion metric measures.

### Setup
1. Install `vx`
  - I used `cargo install vortex-tui --locked -F unstable_encodings` to build from source with the `unstable_encodings` feature, which enables
    the run-end encoder, which makes a significant difference on our `locus.position` columns.
  - See the [official instructions](https://github.com/vortex-data/vortex#command-line-ui-vx) for other installation options.
2. Run `uv run --directory python hailtools setup`. This downloads the `1kg_chr22` data from our
   benchmarks bucket and creates VDS, Parquet, and Vortex datasets for reference and allele data.
   It writes both contig-position and packed locus representations.

### Tests

Run the Rust suite with `cargo test` or `cargo nextest run`. Each combiner run test owns a
private disk dataset fixture under the system temporary directory selected by `tempfile`.
Dropping the handle removes the directory, including during ordinary panic unwinding. Forced
termination or a cleanup error can still leave files behind.

To retain these dataset fixtures for debugging, set `DATAFUSION_SANDBOX_KEEP_FIXTURES=1`:

```sh
DATAFUSION_SANDBOX_KEEP_FIXTURES=1 cargo test --lib tests::combiner_run -- --show-output
DATAFUSION_SANDBOX_KEEP_FIXTURES=1 cargo nextest run --lib -E 'test(tests::combiner_run)' --success-output immediate
```

Each accessor call still creates a unique directory. Before writing, it prints the test name
and full directory path, whose prefix identifies the dataset-fixture format and locus
representation. Retention applies to passing and failing tests, including incomplete writes.
Retained directories are yours to remove; later runs do not delete or reuse them. Unset the
variable or set it to `0` to restore cleanup. This option does not retain test outputs or
directories owned by callers of the lower-level fixture writers.

### Python tools (`python/`)

`python/` is a [uv](https://docs.astral.sh/uv/)-managed Python project providing `hailtools`, a CLI for
miscellaneous utilities that create parquet and vortex files from existing Hail data. Requires Python 3.13
(uv will fetch it if needed).

```
cd pytools
uv run hailtools --help
```
or
```
uv run --directory python hailtool --help
```

### Combiner prototype
So far there are simple pipelines for combining the 50 samples of our benchmark data. Each takes the path of a
directory containing one subdirectory per sample, of the form `s=HG123456`, either local or in object storage.
```
cargo run -r -- combine-refs data/vortices_chr22        # -> data/combined.vortex
cargo run -r -- combine-alleles data/vortices_alleles_chr22  # -> data/combined_alleles.vortex
cargo run -r -- combine-refs data/vortices_packed_chr22
cargo run -r -- combine-alleles data/vortices_alleles_packed_chr22
```
The directory alone selects the representation. Packed input produces packed output; there is no
representation flag.

The reference combiner has three formulations. `--formulation union` merges every sample's scan in
one merge. `--formulation grouped-merge` merges each sample group first and then merges the groups,
so the merges run on several cores; `--groups N` sets the number of sample groups and defaults to
the thread count. `--formulation interval-merge` merges every sample within each locus interval
and writes one file per interval, so the merges run on several cores and the write does too;
`--split-points` names the loci that cut the ordering into intervals, as comma-separated
`contig:position` with the contig ordinal, strictly increasing, and is required. Its `--write`
path names a directory, which gets one file per interval named by index, `0.vortex`, `1.vortex`,
and so on; a path with an extension is rejected. A `--limit` makes no promise about the plan's
shape, and a limited write of this formulation puts one file in the directory. `--explain` and
`--explain-analyze` may be combined with `--write` to render or analyze the plan of the write
itself.
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation grouped-merge --groups 7 --explain --write data/combined.vortex
cargo run -r -- combine-refs data/vortices_chr22 --formulation interval-merge --split-points 22:20000000,22:30000000,22:40000000 --write data/combined
```
A measured write, `--write PATH --metrics DIR`, writes the output as a plain write does and records
the run under `DIR`: one row of resolved settings, wall-clock timings, and peak resident set size
in `DIR/runs/<id>.parquet`, and one row per plan operator per partition of DataFusion's metrics in
`DIR/metrics/<id>.parquet`.
`--run-id ID` names the run; without it a UUID is generated and printed. An id that already has a
run record under `DIR` is refused before anything is written, and a run that fails records
nothing. Both tables are Parquet whatever the output format, and each run adds one file, so
`DIR/runs` and `DIR/metrics` read as tables of every run with datafusion-cli, DuckDB, or pandas.
See
[ADR 0016](docs/adr/0016-record-run-metrics-as-wide-parquet-tables.md).
Several metric names mislead: a Parquet scan's `elapsed_compute` excludes decoding, for one.
[What the run metrics measure](notes/datafusion-metrics.md) says what each column of the metrics
table measures, read from the DataFusion source.
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation grouped-merge --write data/combined.vortex --metrics data/runs --run-id grouped-8
```
A throughput probe, `--probe --metrics DIR`, runs the formulation's unchanged plan, drains its
rows, and stops early to estimate the plan's steady-state throughput without running it to the
end. Every `--poll-period` it takes a progress sample: the time since execution started and the
rows the sink has received. After each progress sample it applies a stopping rule, a pure
function of its settings, the samples so far and the first partition end:
1. It batches the samples into batches spanning at least `--batch` each, and takes each batch's
   rate as its rows over the time between its end samples.
2. It picks the end of warmup with MSER: the number of batches d that minimizes
   Σ_{i>d} (Yᵢ − Ȳ_d)² / (n − d)² over the batch rates Yᵢ. Only cuts that leave at least 5 batches
   count, since the variance of the last few rates is too noisy to compare. If the minimum lies
   past n/2, the run is still too short to judge, and the probe keeps running.
3. Its measurement window runs from the end of warmup to the latest sample, or to the first
   partition end.
4. At each batch end, it checks whether the window is tight: split into `--window-groups` groups
   of equal duration, with boundaries snapped to the nearest sample, the group rates' 95%
   t-interval has a half-width below `--precision` of its mean.
5. It stops with stop reason `steady` at a batch end past `--min-duration` once the last
   `--consecutive` checks were all tight.

It also stops at the first finished partition of the plan, with stop reason `completed`, or once
`--max-duration` has passed, with stop reason `capped`, unless the same sample stops it steady.
Whatever the reason, its steady-state throughput is the rows received between the two end samples
of its measurement window over the time between them, and when MSER has found no end of warmup,
the window starts at the first sample. It prints the run id, the rows received, that throughput,
and the stop reason. Each setting requires `--probe`:

| Flag | Default | Meaning |
|---|---|---|
| `--poll-period SECONDS` | 0.1 | the time between progress samples |
| `--batch SECONDS` | 1 | the least time a batch spans |
| `--precision FRACTION` | 0.02 | the relative half-width below which a check is tight |
| `--consecutive COUNT` | 3 | the tight checks in a row that stop the probe |
| `--window-groups COUNT` | 10 | the groups, 2 to 1000, the window splits into for its interval |
| `--min-duration SECONDS` | 20 | the execution time before which it does not stop steady |
| `--max-duration SECONDS` | 300 | the execution time at which it stops, capped |

Durations are in decimal seconds. The window's group count is not `--groups`, which grouped-merge
takes.
A probe records three tables under `DIR`, all Parquet and one file per run: the run record in
`DIR/runs/<id>.parquet`, the run metrics as they stood at the stop in `DIR/metrics/<id>.parquet`,
and its progress samples, one row each of run id, sample index, elapsed nanoseconds, and rows, in
`DIR/progress/<id>.parquet`. Its run record holds the rows received as `rows_written`, times
`execute_ns` to the stop, and fills the probe columns a measured write leaves empty: `action`,
`stop_reason`, `steady_state_throughput`, `warmup_end_ns`, `window_end_ns`, `window_rows`,
`first_partition_end_ns`, and every setting, as `poll_period_ns`, `batch_duration_ns`,
`precision`, `consecutive_checks`, `window_groups`, `min_duration_ns`, and `max_duration_ns`. `warmup_end_ns` is empty
when MSER found no end of warmup, and `first_partition_end_ns` when no partition finished before
the stop. `--run-id` works as for a measured write, a repeated id is refused before anything
runs, and a probe that fails records nothing. `--probe` refuses `--limit`, which would change the
plan measured. See
[ADR 0017](docs/adr/0017-estimate-throughput-by-stopping-full-plan-runs.md).
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation grouped-merge --probe --metrics data/runs --run-id grouped-8-probe --max-duration 60
```
An earlier variant of the reference combiner is still available as `cargo run -r --example combiner1`.

### Building for a specific GCE instance family

`.cargo/config.toml` sets `-Ctarget-cpu=native`, which is correct when you build and run on the same
machine, and wrong for a build matrix: cargo hashes the *flag string* into its fingerprint, and
`native` is the same string everywhere even though it means something different on each machine.
Reusing a target dir across instance types therefore silently keeps artifacts built for the old
microarchitecture (bad benchmark numbers), or emits instructions the new CPU lacks (SIGILL).

`hailtools gce` works out what is safe to compile with for a given GCE machine family, by asking
rustc for the feature set of each CPU platform the family can schedule you onto and intersecting
them. No instances need to be booted.

```
uv run --directory python hailtools gce list       # every family: what's safe, and what it costs
uv run --directory python hailtools gce flags n2   # -> -Ctarget-cpu=cascadelake
uv run --directory python hailtools gce verify     # run ON a GCE VM: does reality match?
```

The implementation lives in `python/src/hailtools/gce.py` and imports nothing outside the standard
library — in particular not hail. It gets used to bootstrap build machines, which have a rust
toolchain but no reason to carry a JVM, and `verify` has to run on the GCE instance itself. So on a
build VM you can skip the `hailtools` install (and uv, and Python 3.13) entirely and just run the
file:

```
python3 python/src/hailtools/gce.py flags c4
```

Most modern families (`c2`, `c2d`, `c3`, `c3d`, `c4`, `c4d`, `n4`, `t2d`) are single-platform, so
you get the full feature set with no compromise. `n2` and `n2d` span two platforms and cost you 9
and 2 features respectively; `n1` spans five and drops you all the way to Sandy Bridge (no AVX2 or
FMA), so avoid it for benchmarking. Where a family does span platforms, pinning with
`--min-cpu-platform` at instance-creation time recovers the difference.

Build one variant per family, each with its own target dir, since differing rustflags invalidate a
shared one:

```
RUSTFLAGS="$(python3 python/src/hailtools/gce.py flags c4)" \
  CARGO_TARGET_DIR=target-c4 cargo build -r
```

Two caveats. The `FAMILY_CPUS` table in `gce.py` is the fragile part — Google adds platforms to
existing families over time, so re-check it against
[the CPU platforms docs](https://cloud.google.com/compute/docs/cpu-platforms). And the feature sets
come from LLVM's model of each microarchitecture, which can be wider than what a GCE VM actually
exposes, since the hypervisor may mask features; `verify` checks the computed set against
`/proc/cpuinfo` and is worth running once per family.

Note that more features is not automatically faster: AVX-512 causes downclocking on Skylake and
Cascade Lake (much less so on Ice Lake and later), and for Arrow/DataFusion kernels most of the
autovectorization win is at the `x86-64-v3` level (AVX2 + FMA + BMI2). Worth measuring variants
against each other on the same instance rather than assuming.

### Alternate allocator (snmalloc)

DataFusion
[recommends](https://datafusion.apache.org/user-guide/crate-configuration.html#alternate-allocator-snmalloc)
snmalloc in place of the system allocator. The `snmalloc` feature switches to it. It is off by
default, so the system allocator stays the baseline to compare against.

```
cargo build -r --features snmalloc
cargo test --features snmalloc
cargo bench --features snmalloc
```

The feature builds snmalloc's C++, which needs `cmake` and a C++20 compiler installed. A build VM
carrying only a rust toolchain needs those two packages before it can use the feature, and nothing
new if it doesn't.

There is no CLI flag for this and there cannot be one. A program has one global allocator and the
linker picks it. By the time `Cli::parse()` runs, the allocator has already served every allocation
made during runtime startup and inside clap, and only the allocator that handed out a block can
free it, so a runtime switch would have to record an owner per block and branch on every allocation
and free. That overhead is roughly the size of the difference worth measuring.

The `#[global_allocator]` sits in `src/lib.rs` rather than `src/main.rs`, where DataFusion's docs
put it. The benchmark, library test, and CLI crate-test binaries all link this library, so a static
in `main.rs` would cover `cargo run` and leave those binaries on the system allocator.

#### Give the allocator the same CPU flags as the rust code

snmalloc's C++ never sees `RUSTFLAGS`. Pass the microarchitecture through `CXXFLAGS` too, or you
get an allocator compiled for a baseline CPU underneath tuned rust. `-Ctarget-cpu=cascadelake`
becomes `-march=cascadelake`:

```
RUSTFLAGS="$(python3 python/src/hailtools/gce.py flags n2)" CXXFLAGS="-march=cascadelake" \
  CARGO_TARGET_DIR=target-n2 cargo build -r --features snmalloc
```

The two names usually match, since clang shares rustc's LLVM vocabulary and gcc accepts most of the
same spellings, but gcc keeps its own list and lags on newer entries. `cmake` picks the C++ compiler
through the `cc` crate, which defaults to `c++`, so on a Linux build VM this is gcc rather than
clang. Check the name you pick compiles before trusting a number that came out of it.

Don't reach for snmalloc-rs's own `native-cpu` feature. On the cmake build path it only sets
`SNMALLOC_OPTIMISE_FOR_CURRENT_MACHINE`, which means native or nothing, and native is the case that
already works.

Cargo does not fingerprint `CXXFLAGS`, and `snmalloc-sys` declares no `rerun-if-env-changed`, so
changing the flag on its own will not rebuild the C++. Cargo reports `Finished` in a hundredth of a
second and keeps the object built for the previous microarchitecture. The one-target-dir-per-family
rule above is what saves you, and it now covers the C++ as well as the rust.

### Tips
`vx browse file.vortex` is extremely handy for inspecting vortex files.
