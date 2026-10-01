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

`convert-hts` converts a directory of per-sample reference Hail Tables, such as the production data in
`data/vdss_prod_1k`, into a Parquet dataset with one file per contig. `--limit N` converts only the first N
tables in sorted path order. The sample comes from each table's global `s`, and contigs are written zero-padded
(`chr01` … `chr08`). Only numeric contigs are accepted. `convert-parquets` then turns the result into a Vortex
dataset:
```
uv run --directory python hailtools convert-hts vdss_prod_1k parquets_prod_50 --limit 50
uv run --directory python hailtools convert-parquets parquets_prod_50 vortices_prod_50
```

### Combiner prototype
So far there are simple pipelines for combining the 50 samples of our benchmark data. Each takes the path of a
directory containing one subdirectory per sample, of the form `s=HG123456`, either local or in object storage.
Each subdirectory is an input table, named by its entry: `s=HG123456`. A run merges every input table
unless `--inputs` names some, as comma-separated names such as `--inputs s=HG00308,s=HG00592`; an
unknown name is an error.
```
cargo run -r -- combine-refs data/vortices_chr22        # -> data/combined.vortex
cargo run -r -- combine-alleles data/vortices_alleles_chr22  # -> data/combined_alleles.vortex
cargo run -r -- combine-refs data/vortices_packed_chr22
cargo run -r -- combine-alleles data/vortices_alleles_packed_chr22
```
The directory alone selects the representation. Packed input produces packed output; there is no
representation flag.

The reference combiner has three formulations. `--formulation union` merges every input table's scan
in one merge. `--formulation grouped-merge` merges each input group, a contiguous run of input
tables in name order, first and then merges the groups, so the merges run on several cores;
`--groups N` sets the number of input groups and defaults to the thread count. `--formulation
interval-merge` merges every input table within each locus interval and writes one file per
interval, so the merges run on several cores and the write does too; `--split-points` names the loci
that cut the ordering into intervals, as comma-separated `contig:position` with the contig ordinal,
strictly increasing, and is required. Its `--write` path names a directory, which gets one file per
interval named by index, `0.vortex`, `1.vortex`, and so on; a path with an extension is rejected.
Such a write replaces only the files it writes, so a write, measured or analyzed, refuses a path
where a file or a non-empty directory already exists, rather than leave an earlier write's files
among its own; remove an earlier output first. A `--limit` makes no promise about the plan's shape,
and a limited write of this formulation puts one file in the directory. `--explain` and
`--explain-analyze` may be combined with `--write` to render or analyze the plan of the write
itself.
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation grouped-merge --groups 7 --explain --write data/combined.vortex
cargo run -r -- combine-refs data/vortices_chr22 --formulation interval-merge --split-points 22:20000000,22:30000000,22:40000000 --write data/combined
```
A reference combiner write, measured or not, ends by writing a sample annotation table beside its
output, in the output format: `data/combined.samples.vortex` beside the file `data/combined.vortex`
or the directory `data/combined`. It holds the run's sample ids, sorted, in one `s` column, which
makes the output a complete multi-sample input table for a later run. A write that fails leaves
none, and a probe, a collect, an explain or an allele combiner write writes none. See
[ADR 0018](docs/adr/0018-declare-a-multi-sample-input-tables-samples-in-a-sample-annotation-table.md).
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
rows, or writes them as described below, and stops early to estimate the plan's steady-state
throughput without running it to the end. Every `--poll-period` it takes a progress sample: the
time since execution started and the rows the sink has received. After each progress sample it
applies a stopping rule, a pure function of its settings, the samples so far and the first
partition end:
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
   t-interval, the estimate interval, has a half-width below `--precision` of its mean.
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
`stop_reason`, `steady_state_throughput`, `relative_half_width`, `warmup_end_ns`, `window_end_ns`,
`window_rows`, `first_partition_end_ns`, and every setting, as `poll_period_ns`,
`batch_duration_ns`, `precision`, `consecutive_checks`, `window_groups`, `min_duration_ns`, and
`max_duration_ns`. `relative_half_width` is the estimate interval's half-width over its mean at
the decision the probe stopped with, whatever its stop reason: the value a tightness check of its
measurement window compares with `--precision`. It is empty when there is no estimate interval,
because the window has no more samples than `--window-groups` or a group spans no time, and when
the interval's mean is not positive. `warmup_end_ns` is empty when MSER found no end of warmup,
and `first_partition_end_ns` when no partition finished before the stop. `--run-id` works as for
a measured write, a repeated id is refused before anything runs, and a probe that fails records
nothing. `--probe` refuses `--limit`, which would change the plan measured. See
[ADR 0017](docs/adr/0017-estimate-throughput-by-stopping-full-plan-runs.md).
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation grouped-merge --probe --metrics data/runs --run-id grouped-8-probe --max-duration 60
```
A writing probe, `--probe --write PATH --metrics DIR`, writes the rows through the sink a plain
write to `PATH` would use, one file or a directory of one file per interval, with the output
format and `--compression` a plain write takes, so encoding and writing count in its throughput.
It samples the rows reaching the sink, as the draining form does; under interval-merge's
file-per-interval sink, that is the rows reaching all the interval writers, and the first interval
to finish closes the window. Its run record also fills `output_path`, `output_format`, and
`compression`. The output of a stopped write is incomplete, so a probe keeps none of it: after
every ending, whether steady, completed, capped, or failed, it removes everything under `PATH`,
and a local directory it created at `PATH`. So that the removal only ever touches what the probe
wrote, a probe refuses a `PATH` that already exists, as a file, as a non-empty directory, or,
on local disk, as a symlink of any kind, before it discovers the dataset.
On Google Cloud Storage, the removal does not reach unfinished multipart uploads. A write the
probe stops leaves one for each file it had not finished: every such Vortex file, whose writer
always uploads in parts, and every such Parquet file past its first 10 MiB. They hold no object
at `PATH` and no listing shows them, but their uploaded parts are billed until they are aborted,
and object_store can neither list nor abort them once the writer is gone. Probe into a bucket
with a lifecycle rule that aborts incomplete multipart uploads, for example after one day. This
command replaces the bucket's lifecycle configuration, so merge any rules it already has into
the file first:
```
echo '{"rule":[{"action":{"type":"AbortIncompleteMultipartUpload"},"condition":{"age":1}}]}' > lifecycle.json
gcloud storage buckets update gs://BUCKET --lifecycle-file=lifecycle.json
```
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation interval-merge --probe --write data/probe-out --metrics data/runs --run-id interval-probe --max-duration 60
```
A shadow probe, `--probe --shadow`, drained or written, applies the stopping rule after each
progress sample exactly as a probe does, but never acts on it: it ignores `--max-duration` and
the first partition end, runs the plan to completion, and always stops with stop reason
`completed`. Its measurement window still closes at the first partition end, and its
`steady_state_throughput` and `relative_half_width` are the rule's estimate and estimate interval
at the end of the run. Its run record has `action` `shadow`, and records the first `steady`
decision the rule reached before the first partition end, the decision a probe would have stopped
at, in four more columns: `would_stop_ns`, the elapsed nanoseconds at which it would have stopped,
`would_be_steady_state_throughput`, `would_be_relative_half_width`, the relative half-width of the
estimate interval at that decision, and `would_be_warmup_end_ns`. Since the cap stops nothing,
`would_stop_ns` may lie past `--max-duration`, where a probe would have stopped capped instead.
They are empty when the rule never stopped steady, and for every other run.
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation grouped-merge --probe --shadow --metrics data/runs --run-id grouped-8-shadow
```
Calibrate the stopping rule with shadow probes before relying on it in a sweep:
1. Run shadow probes on a few extreme configurations: the smallest and the largest branching
   factor, one thread and all threads, and interval-merge with many intervals.
2. Compare each run's would-be steady-state throughput and its estimate interval,
   `would_be_steady_state_throughput` and `would_be_relative_half_width`, with its steady-state
   throughput at the end of the run and its estimate interval, `steady_state_throughput` and
   `relative_half_width`, the estimate over the longest measurement window the dataset allows. An
   empty `would_stop_ns` means the rule never settled on that configuration.
   `uv run --directory python hailtools probe-viewer DIR [-o PATH]` draws this comparison for every
   shadow probe in the metrics directory `DIR`, and counts how many would-be estimate intervals
   cover the end-of-run estimate. It writes one self-contained page, `DIR/probe-viewer.html` by
   default, that opens from a file and loads its chart scripts from a CDN. A relative `DIR` is
   under `data/`.
3. Check that the work per row holds steady along the genome: the rate between progress samples
   should not drift over the run, since the estimate stands for the loci a probe does not reach
   only if it doesn't. The probe viewer's detail chart for each shadow run draws its batch rates
   over the run, with the stopping rule's decisions and the running estimate on them.
4. Tune `--batch` from the recorded progress samples. Their row counts are cumulative, so merging
   samples replays the rule offline with any longer batch duration, and with any other setting.

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

### Running on GCE

Combiner runs on GCE read their dataset from, and write their output and metrics directory to,
`gs://hail-pschultz`, which sits in `us-central1` like the VMs:

```
combiner-bench/datasets/<name>/                     datasets, uploaded with gcloud storage rsync
combiner-bench/bin/<commit>/<family>/<profile>/     published binaries, each with a build-info.txt
combiner-bench/runs/<campaign>/                     metrics directories
scratch/<campaign>/<run-id>/                        outputs; deleted after 7 days
```

The bucket's lifecycle rules delete `scratch/` after 7 days and abort incomplete multipart uploads
after 1 day, which a stopped writing probe leaves behind. They were set, replacing any rules the
bucket had, with
`gcloud storage buckets update gs://hail-pschultz --lifecycle-file=scripts/gce/bucket-lifecycle.json`.

A long-lived **build VM** compiles published binaries for any instance family, using the flags
from `gce.py` in the section above. Its disk is only a cache: `scripts/gce/create-build-vm.sh`
creates it from nothing, and its startup script, `scripts/gce/provision.sh`, provisions it on
every boot. So deleting the VM loses nothing. It is a `c4d-standard-16`, for the fastest single
core in the serial fat-LTO step, in whichever `us-central1` zone has one, and an `n2-standard-16`
when none does. `scripts/gce/build.sh` builds a pushed commit for each family named and publishes
each binary. It refuses a commit that isn't a full hash, and a binary that already exists:

```
scripts/gce/create-build-vm.sh
ZONE=$(gcloud compute instances list --filter=name=combiner-build --format='value(zone.basename())')
gcloud compute ssh combiner-build --zone $ZONE -- \
  sudo -iu builder datafusion-sandbox/scripts/gce/build.sh COMMIT c4
gcloud compute instances stop combiner-build --zone $ZONE
```

After changing `provision.sh`, give the VM the new version with
`gcloud compute instances add-metadata combiner-build --zone $ZONE --metadata-from-file startup-script=scripts/gce/provision.sh`.

Stop the build VM at the end of a session rather than between builds: a stop takes about 20 s and
a start 10–50 s, and an idle half hour costs well under a dollar. A stopped VM keeps no capacity,
so a start can fail when its family is stocked out; then delete it and create it again, which
falls back to n2. On `c4d-standard-16`, `create-build-vm.sh` takes 2–3 minutes, returning once the
VM is provisioned with the pinned toolchain, and the first release build about 12 minutes, nearly
all of it the single-threaded fat-LTO step. So deleting the VM only pays when it would otherwise
sit unused for about a week.

A **runner** is a throwaway VM of the family a binary was built for, on the build VM's image,
which carries `gcloud`. A run needs nothing else installed:

```
gcloud compute instances create combiner-runner --zone us-central1-a --machine-type c4-standard-16 \
  --image-family debian-12 --image-project debian-cloud --boot-disk-type hyperdisk-balanced \
  --scopes cloud-platform
gcloud compute ssh combiner-runner --zone us-central1-a
# on the runner:
gcloud storage cp gs://hail-pschultz/combiner-bench/bin/COMMIT/c4/release/datafusion-sandbox .
chmod +x datafusion-sandbox
SPLIT_POINTS=$(./datafusion-sandbox balance-split-points \
  gs://hail-pschultz/combiner-bench/datasets/vortices_chr22_nomindp/s=HG00187 --intervals 16)
./datafusion-sandbox combine-refs gs://hail-pschultz/combiner-bench/datasets/vortices_chr22_nomindp \
  --formulation interval-merge --split-points "$SPLIT_POINTS" \
  --probe --write gs://hail-pschultz/scratch/CAMPAIGN/RUN_ID \
  --metrics gs://hail-pschultz/combiner-bench/runs/CAMPAIGN --run-id RUN_ID
# back on your machine:
gcloud compute instances delete combiner-runner --zone us-central1-a
gcloud storage rsync -r gs://hail-pschultz/combiner-bench/runs/CAMPAIGN data/CAMPAIGN
```

On the first runner of each new family, also check that the family's computed CPU features are
really there. `gce.py verify` asks rustc for them, so it needs a minimal stable toolchain and the
script from the commit the binary was built from:

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. ~/.cargo/env
curl -sSfO https://raw.githubusercontent.com/hail-is/datafusion-sandbox/COMMIT/python/src/hailtools/gce.py
python3 gce.py verify c4
```

Families can be out of stock in every `us-central1` zone at once: c4d was, both 16 and 8 vCPUs,
when this was first run. Try each zone, then fall back to another family.

The binary leaves out DataFusion's `compression` feature: under fat LTO with any AVX-512
target-cpu, its bzip2 crashes LLVM. See
[#222](https://github.com/hail-is/datafusion-sandbox/issues/222).

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
