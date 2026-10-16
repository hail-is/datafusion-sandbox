# Building for a GCE instance family

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

A build with the [`snmalloc` feature](snmalloc.md) also needs the family's CPU in `CXXFLAGS`.
