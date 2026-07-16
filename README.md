This repo is to help us start to experiment with implementing hail style pipelines in datafusion, and to benchmark their performance.

- `benches/example.rs`: Some initial infrastructure for running benchmarks. Run using `cargo bench`. Setting `RUSTFLAGS='-C target-cpu=native'` is probably a good idea.
- `data/`: Data files for use in benchmarks, or just ad hoc experimentation.
- `src/lib.rs`: Definitions of dataframes, for use in benchmarks or ad hoc experimentation.
- `src/main.rs`: I'm using the main function as a scratch area to run ad hoc experiments using `cargo run`.
- `notes/`: A place for notes on datafusion. Right now just has an explainer I had Claude generate on how aggregation works, and how it can take advantage of ordered inputs.

### Setup
1. Install `vx`
  - I used `cargo install vortex-tui --locked -F unstable_encodings` to build from source with the `unstable_encodings` feature, which enables
    the run-end encoder, which makes a significant difference on our `locus.position` columns.
  - See the [official instructions](https://github.com/vortex-data/vortex#command-line-ui-vx) for other installation options.
2. Run `uv run --directory python hailtools setup`. This will download the `1kg_chr22` data from our benchmarks bucket, and convert them to vds,
   parquet, and vortex (only the reference data for the last two, for now).

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
So far there is just a simple pipeline for combining the 50 samples of reference data from our benchmark data. Run it using
```
cargo run -r --example combiner1
```

### Tips
`vx browse file.vortex` is extremely handy for inspecting vortex files.
