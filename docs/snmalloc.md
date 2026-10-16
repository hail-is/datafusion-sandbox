# Alternate allocator (snmalloc)

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

## Give the allocator the same CPU flags as the rust code

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
rule of [Building for a GCE instance family](target-cpu.md) is what saves you, and it now covers the
C++ as well as the rust.
