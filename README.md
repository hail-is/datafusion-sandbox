This repo is to help us start to experiment with implementing hail style pipelines in datafusion, and to benchmark their performance.

- `benches/example.rs`: Some initial infrastructure for running benchmarks. Run using `cargo bench`. Setting `RUSTFLAGS='-C target-cpu=native'` is probably a good idea.
- `data/`: Data files for use in benchmarks, or just ad hoc experimentation.
- `src/lib.rs`: Definitions of dataframes, for use in benchmarks or ad hoc experimentation.
- `src/main.rs`: I'm using the main function as a scratch area to run ad hoc experiments using `cargo run`.
- `notes/`: A place for notes on datafusion. Right now just has an explainer I had Claude generate on how aggregation works, and how it can take advantage of ordered inputs.
