This repo is to help us start to experiment with implementing hail style pipelines in datafusion, and to benchmark their performance.

- `benches/example.rs`: Some initial infrastructure for running benchmarks. Run using `cargo bench`. Setting `RUSTFLAGS='-C target-cpu=native'` is probably a good idea.
- `data/`: Data files for use in benchmarks, or just ad hoc experimentation.
- `src/lib.rs`: Definitions of dataframes, for use in benchmarks or ad hoc experimentation.
- `src/main.rs`: A CLI dispatching to the pipelines in `src/`. Run using `cargo run -r -- <subcommand>`.
- `notes/`: A place for notes on datafusion. Right now just has an explainer I had Claude generate on how aggregation works, and how it can take advantage of ordered inputs.

### Setup
1. Install `vx`
  - I used `cargo install vortex-tui --locked -F unstable_encodings` to build from source with the `unstable_encodings` feature, which enables
    the run-end encoder, which makes a significant difference on our `locus.position` columns.
  - See the [official instructions](https://github.com/vortex-data/vortex#command-line-ui-vx) for other installation options.
2. Run `uv run --directory python hailtools setup`. This downloads the `1kg_chr22` data from our
   benchmarks bucket and creates VDS, Parquet, and Vortex datasets for reference and allele data.
   It writes both contig-position and packed locus representations.

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

### Tips
`vx browse file.vortex` is extremely handy for inspecting vortex files.
