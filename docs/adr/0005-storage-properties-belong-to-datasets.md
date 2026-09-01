# Storage properties belong to datasets

A dataset carries a `DatasetLayout` that declares its locus ordering and optional schema. `Dataset` owns reading because it has the path, input format, layout, and discovered sample set needed to list each sample's files and construct its sorted table. `OutputFormat` owns writing because it has the writer factory and format options. This keeps storage declarations out of formulations and prevents separate call sites from describing the same files differently.

The split is between a type and an instance. A layout describes how one kind of dataset distributes rows across files, while a dataset identifies one stored instance and the samples found there. The input format stays on the dataset because it describes each file's encoding, not the arrangement of files. The sample set also stays on the dataset because it is discovered data rather than part of the representation.

A formulation declares the layout it requires and checks the dataset's locus ordering before it builds a plan. The check accepts a finer ordering when the required ordering is its prefix. A wrong ordering otherwise produces correct rows through a slower plan, so relying on DataFusion to insert a sort would hide the declaration error. The dataset expands the locus ordering under its detected representation and checks that its resolved schema contains every stored column in that expansion. It reports a missing column as a plan error before building a reader.

`CombinerRun` currently creates the dataset layout from the chosen formulation. That makes the check look redundant, but the declared locus ordering is a claim about stored data and will eventually come from Parquet or Vortex metadata. ADR 0003 rejected making callers pair a plan builder with private session settings; the pipeline supplies shared defaults and formulations use the session they are handed. A dataset's locus ordering is different: it is an external fact that a formulation can only require and validate.

## Consequences

Formulations do not construct readers or reach into datasets for paths and formats. A per-sample read lists explicit files, builds a `SortedTable`, and attaches the sample id as a scalar field. The table declares its own single-partition scan; that partitioning is a reader guarantee rather than a `DatasetLayout` property.
