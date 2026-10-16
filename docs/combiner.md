# Combiner runs

So far there are simple pipelines for combining the 50 samples of our benchmark data. This doc
covers what a combiner run reads, the reference combiner's formulations and row ordering, what a
write leaves behind, and measured writes. [Throughput probes](throughput-probe.md) have their own
doc.

## Datasets and input tables

Each pipeline takes the path of a dataset directory, either local or in object storage, whose
entries are input tables of two kinds:
- a single-sample input table is a subdirectory `s=HG123456/` holding one sample's files;
- a multi-sample input table is a file `g0.vortex` or a directory `g1/` of files whose rows carry
  their sample in an `s` column, beside its sample annotation table `g0.samples.vortex` or
  `g1.samples.vortex`, which a combiner write leaves beside its output.

An input table is named by its entry without the format's extension: `s=HG123456`, `g0`, `g1`. Any
other entry is an error, and so is a multi-sample input table without its sample annotation table,
which is what a failed write leaves, or an annotation table without its data. No sample may be in
two input tables. Every input table must have the same columns, ignoring `s`, and a multi-sample
input table's `s`, in its data and in its annotation table, must be a non-null string. A run
merges every input table unless `--inputs` names some, as comma-separated names such as
`--inputs s=HG00308,g0`; an unknown name is an error. So to combine in two levels, write the
first-level runs into one directory, perhaps with some samples held back there as `s=<id>/`
tables, and run the second level over that directory. The allele combiner reads only single-sample
input tables.
```
cargo run -r -- combine-refs data/vortices_chr22        # -> data/combined.vortex
cargo run -r -- combine-alleles data/vortices_alleles_chr22  # -> data/combined_alleles.vortex
cargo run -r -- combine-refs data/vortices_packed_chr22
cargo run -r -- combine-alleles data/vortices_alleles_packed_chr22
```
The directory alone selects the representation. Packed input produces packed output; there is no
representation flag.

## Formulations

The reference combiner has three formulations. `--formulation union` merges every input table's scan
in one merge. `--formulation grouped-merge` merges each input group, a contiguous run of input
tables in name order, first and then merges the groups, so the merges run on several cores;
`--groups N` sets the number of input groups and defaults to the thread count. `--formulation
interval-merge` merges every input table within each locus interval and writes one file per
interval, so the merges run on several cores and the write does too; `--split-points` names the loci
that cut locus order into intervals, as comma-separated `contig:position` with the contig ordinal,
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

## Row ordering

Every formulation merges under the run's row ordering, the locus alone by default.
`--preserve-sample-ordering` makes it the locus then the sample `s`: every input table is trusted to
be in that order already, and the output keeps it, so a written output is an input table a run
with the flag above it merges without re-sorting. Without the flag, ties on a locus come out in
any order. The run record names the row ordering in its `row_ordering` column, `locus` or
`locus,s`.

## Outputs and sample annotation tables

A reference combiner write, measured or not, ends by writing a sample annotation table beside its
output, in the output format: `data/combined.samples.vortex` beside the file `data/combined.vortex`
or the directory `data/combined`. It holds the run's sample ids, sorted, in one `s` column, which
makes the output a complete multi-sample input table for a later run. Any write of one file,
the allele combiner's included, must be named as a later run finds an input table, so its path
needs the output format's extension and no trailing slash, and a path such as
`g0.samples.vortex`, named as an annotation table, is rejected. A write that fails leaves none, and a probe, a
collect, an explain or an allele combiner write writes none. See
[ADR 0018](adr/0018-declare-a-multi-sample-input-tables-samples-in-a-sample-annotation-table.md).

## Measured writes

A measured write, `--write PATH --metrics DIR`, writes the output as a plain write does and records
the run under `DIR`: one row of resolved settings, wall-clock timings, and peak resident set size
in `DIR/runs/<id>.parquet`, and one row per plan operator per partition of DataFusion's metrics in
`DIR/metrics/<id>.parquet`.
`--run-id ID` names the run; without it a UUID is generated and printed. An id that already has a
run record under `DIR` is refused before anything is written, and a run that fails records
nothing. Both tables are Parquet whatever the output format, and each run adds one file, so
`DIR/runs` and `DIR/metrics` read as tables of every run with datafusion-cli, DuckDB, or pandas.
See
[ADR 0016](adr/0016-record-run-metrics-as-wide-parquet-tables.md).
Several metric names mislead: a Parquet scan's `elapsed_compute` excludes decoding, for one.
[What the run metrics measure](../notes/datafusion-metrics.md) says what each column of the metrics
table measures, read from the DataFusion source.
```
cargo run -r -- combine-refs data/vortices_chr22 --formulation grouped-merge --write data/combined.vortex --metrics data/runs --run-id grouped-8
```
