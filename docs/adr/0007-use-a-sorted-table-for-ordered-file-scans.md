# Use a sorted table for ordered file scans

A combiner needs each sample's files in one partition, ordered by locus. `ListingTable` cannot guarantee both properties at once. Its statistics-based grouping is guarded by `declared_output_partitioning.is_none()` in `datafusion-catalog-listing-55.0.0/src/table.rs:567`. Declaring partitioning pins the partition count but disables the code that orders files from their statistics.

The earlier one-shared-scan formulation also relied on grouping by partition values. When the number of unique partition tuples exceeds the target partition count, `FileGroup::group_by_partition_values` assigns excess groups round robin in `datafusion-datasource-55.0.0/src/file_groups.rs:531-541`. Files from several samples can then land in one group. Position values restart between samples, so the group is not locus-ordered. DataFusion detects that the declared ordering is false and inserts a sort, preserving correct rows at the cost of the intended plan shape.

Issue #44 proposed `ListingOptions::with_output_partitioning`. We adopted its useful mechanism, but not through `ListingTable`. `list_files_for_declared_output_partitioning` ignores partition values and calls `split_files(n)` in `datafusion-catalog-listing-55.0.0/src/table.rs:910-946`. That method sorts paths and divides them into chunks with equal file counts. It yields one sample per group only when every sample has the same number of files.

We will use a `SortedTable` provider for this scan shape. It takes explicit files, a schema, an ordering, and an optional scalar field. A dataset passes the stored ordering it expanded from its layout, but the provider does not assign domain meaning to it. It infers each file's statistics through its `FileFormat`, then calls `FileScanConfig::split_groups_by_statistics_with_target_partitions` with one target partition. That call both orders the files and proves they do not overlap. More than one returned group is a plan error rather than a silent fallback.

> Superseded by [ADR 0011](0011-recover-file-order-instead-of-proving-it.md). The statistics grouping call is gone; the sorted table recovers the file order from the same statistics without proving it. The rest of this ADR stands.

The scan declares `UnknownPartitioning(1)`. This is the strongest true claim. Any declared partitioning prevents `FileScanConfig::repartitioned` from splitting the group according to session settings, while a hash claim would incorrectly describe how rows were assigned. The provider then delegates plan creation to the format through `FileScanConfigBuilder`; it does not define a custom execution plan.

## Consequences

Sorted tables reject files with missing ordering statistics and file ranges that overlap. Both errors name a file and occur while planning. File names do not determine scan order.

The partition count belongs to the table rather than `target_partitions` or `preserve_file_partitions`. A hostile session cannot change this scan into several partitions.

The optional scalar is represented as a partition value, without requiring a hive-style path. DataFusion therefore includes it in the scan's schema and equivalence properties.
