This repo is to help us start to experiment with implementing hail style pipelines in datafusion, and to benchmark their performance.

- `src/main.rs`: A CLI dispatching to the pipelines in `src/`. Run using `cargo run -r -- <subcommand>`; `--help` lists the subcommands and their flags.
- `src/lib.rs`: The library behind the CLI. Its doc comment maps the modules.
- `benches/example.rs`: Some initial infrastructure for running benchmarks. Run using `cargo bench`.
- `examples/`: Ad hoc experiments, run using `cargo run -r --example <name>`.
- `python/`: [`hailtools`](python/README.md), a CLI that converts Hail data to Parquet and Vortex and analyzes benchmark runs.
- `scripts/gce/`: Scripts for building and running on GCE; see [Running on GCE](docs/gce.md).
- `data/`: Data files for use in benchmarks, or just ad hoc experimentation. Not tracked; setup fills it.
- `notes/`: A place for notes on datafusion. Has an explainer I had Claude generate on how aggregation works, and how it can take advantage of ordered inputs, and a reference on what each recorded DataFusion metric measures.
- [`GLOSSARY.md`](GLOSSARY.md) defines the domain vocabulary these docs use, and [`docs/adr/`](docs/adr/) records the decisions behind the design.

### Setup
1. Install `vx`
  - I used `cargo install vortex-tui --locked -F unstable_encodings` to build from source with the `unstable_encodings` feature, which enables
    the run-end encoder, which makes a significant difference on our `locus.position` columns.
  - See the [official instructions](https://github.com/vortex-data/vortex#command-line-ui-vx) for other installation options.
2. Run `uv run --directory python hailtools setup`. This downloads the `1kg_chr22` data from our
   benchmarks bucket and creates VDS, Parquet, and Vortex datasets for reference and allele data.
   It writes both contig-position and packed locus representations.
3. Combine the reference data with `cargo run -r -- combine-refs data/vortices_chr22`, which writes
   `data/combined.vortex`. [Combiner runs](docs/combiner.md) covers the rest.

### Docs

Each subsystem has its own doc. [`docs/README.md`](docs/README.md) lists them, with when to
read each.

### Tests

Run the Rust suite with `cargo test` or `cargo nextest run`, and the Python suite with
`uv run --directory python pytest`. Each combiner run test owns a
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

### Tips
`vx browse file.vortex` is extremely handy for inspecting vortex files.
