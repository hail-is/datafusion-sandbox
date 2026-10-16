# hailtools

Python utilities for using hail to convert hail datasets to parquet/vortex, run the hail combiner, and anything else we find useful to evaluate datafusion.

A [uv](https://docs.astral.sh/uv/)-managed project. Requires Python 3.13 (uv will fetch it if
needed). `--help` lists every command:

```
uv run --directory python hailtools --help
```

`setup` fetches the benchmark data; see the [root README](../README.md#setup). Other commands are
documented beside what they serve:

- `gce`: [Building for a GCE instance family](../docs/target-cpu.md)
- `probe-viewer`: [Calibrating the stopping rule](../docs/probe-calibration.md#probe-viewer)
- `sweep-report`: [Running on GCE](../docs/gce.md#sweep-reports)

## Converting Hail tables

`convert-hts` converts a directory of per-sample reference Hail Tables, such as the production data in
`data/vdss_prod_1k`, into a Parquet dataset with one file per contig. `--limit N` converts only the first N
tables in sorted path order. The sample comes from each table's global `s`, and contigs are written zero-padded
(`chr01` … `chr08`). Only numeric contigs are accepted. `convert-parquets` then turns the result into a Vortex
dataset:
```
uv run --directory python hailtools convert-hts vdss_prod_1k parquets_prod_50 --limit 50
uv run --directory python hailtools convert-parquets parquets_prod_50 vortices_prod_50
```
