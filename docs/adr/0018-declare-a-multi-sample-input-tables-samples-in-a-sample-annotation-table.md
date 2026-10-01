# Declare a multi-sample input table's samples in a sample annotation table

Combiner runs are to form a hierarchy: a run's written output is a multi-sample input table to the
runs above it. A run needs each input table's sample set at plan time, to reject a sample that two
input tables share, to narrow by input table, to form input groups, and to record the run. A
single-sample input table's sample is its `s=<id>/` directory name, as before. A multi-sample input
table's rows carry their sample in a stored `s` column, and its sample set is declared in a sample
annotation table stored beside it: `<stem>.samples.<ext>` next to the data at `<stem>.<ext>` or
`<stem>/`, in the data's format, with one non-null `s` row per sample in sorted order. Readers require
`s` and ignore any other column, so the table can grow to hold per-sample data. A reference
combiner write or measured write writes it after every data file, and a run trusts it without
checking it against the rows. A dataset root holds only `s=<id>/` directories and paired
multi-sample input tables; anything else is a plan error.

The annotation table doubles as the mark that an input table is complete. A write that fails partway
leaves data with no annotation table, which discovery rejects instead of reading as an input table
with missing rows. Trusting it follows [ADR 0011](0011-recover-file-order-instead-of-proving-it.md):
the run trusts the writer about what it cannot check cheaply, and tests check the writer.

## Considered options

- **`SELECT DISTINCT s` at plan time.** It needs no new file, but it scans every multi-sample input
  table in full before the run starts.
- **Footer key-value metadata.** Parquet has it and Vortex support is uncertain. Every file of an
  interval-merge directory would have to agree, and per-sample data has no room to grow there.
- **Sample ids in the path**, as `s=<id>/` does for one sample. This stops working past a handful of
  samples.
- **A plain-text sample list.** This is the simplest option, but it could never hold per-sample data
  alongside the ids.

## Consequences

The annotation table is a sibling and not inside the data because one-file outputs have no directory
to hold it, and changing every output layout to a directory was out of scope. The expected next step
is a directory per input table that holds both its data and its annotation table. At that point the
sibling naming, and perhaps `s=<id>/`, would be retired.
