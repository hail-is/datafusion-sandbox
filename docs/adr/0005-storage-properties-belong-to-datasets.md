# Storage properties belong to datasets

A dataset carries a `DatasetLayout` that declares its locus ordering, partition columns, and optional schema. `Dataset` owns reading because it has the path, input format, layout, and discovered sample set needed to construct a table. `OutputFormat` owns writing because it has the writer factory and format options. This keeps storage declarations out of formulations and prevents separate call sites from describing the same files differently.

The split is between a type and an instance. A layout describes how one kind of dataset distributes rows across files, while a dataset identifies one stored instance and the samples found there. The input format stays on the dataset because it describes each file's encoding, not the arrangement of files. The sample set also stays on the dataset because it is discovered data rather than part of the representation.

A formulation declares the layout it requires and checks the dataset's locus ordering before it builds a plan. The check accepts a finer ordering when the required ordering is its prefix. A wrong ordering otherwise produces correct rows through a slower plan, so relying on DataFusion to insert a sort would hide the declaration error. The dataset also checks that its resolved schema contains the columns named by the locus ordering. It reports a missing column as a plan error before building a reader. Declared partition columns count as present because DataFusion adds them to the file schema when it builds the table.

`CombinerRun` currently creates the dataset layout from the chosen formulation. That makes the check look redundant, but the declared locus ordering is a claim about stored data and will eventually come from Parquet or Vortex metadata. ADR 0003 rejected making callers pair a plan builder with the session settings it needs because those settings are configuration choices the formulation can derive. A dataset's locus ordering is different: it is an external fact that a formulation can only require and validate.

## Consequences

Formulations no longer construct listing options or reach into datasets for paths and formats. Per-sample reads remove the `s` partition column because the sample directory is below that partition boundary. The proposed explicit output partitioning work in #44 should add any new storage declaration to `DatasetLayout`, not duplicate it across formulation files.
