# What the run metrics measure

This note says what each metric in `METRICS` (`src/run_metrics.rs`) measures, read from the pinned sources: DataFusion 55.0.0, parquet 59.2.0, and the `vortex-datafusion` fork in `Cargo.lock`. Nothing here was measured by running queries. References name functions and types, not lines, so they should survive small upstream changes. Recheck the caveats after a DataFusion upgrade, since several describe upstream bugs.

Crate abbreviations used below:

- `pec` is `datafusion-physical-expr-common`, module `metrics`. `datafusion-physical-plan::metrics` only re-exports it.
- `pp` is `datafusion-physical-plan`.
- `ds` is `datafusion-datasource`.
- `dsp` is `datafusion-datasource-parquet`.
- `pruning` is `datafusion-pruning`.

## Rules that apply to every metric

**Times are wall clock.** `Time::timer` (`pec`) captures `Instant::now()` and adds the elapsed nanoseconds when the guard stops or drops. No metric is CPU time. A span includes time the thread sat descheduled.

**Every recorded span adds at least 1 ns.** `Time::add_duration` adds `max(nanos, 1)`. A `Time` of 0 means no timer ever ran. A `Time` of a few ns means a timer ran and measured nothing.

**Clones share a value.** `Count`, `Gauge`, `Time` and `Timestamp` wrap an `Arc`. Concurrent tasks adding to one `Time` can push it past the operator's wall-clock duration.

**Registration never dedupes.** `ExecutionPlanMetricsSet::register` appends. Code that registers a metric once per file leaves many same-named readings in one partition. Our table folds them (`Recorded::fold`) the way `MetricValue::aggregate` does.

**The partition of a reading is whatever the registering code passed.** `MetricBuilder::counter`, `gauge`, `subset_time`, `pruning_metrics` and `ratio_metrics` set it. `MetricBuilder::global_counter` and hand-built `Metric::new(value, None)` leave it unset, and those readings land in the operator's null-partition row. Null-partition metrics today: `rows_written`, `bytes_written`, `num_predicate_creation_errors`, `output_rows_skew`.

**An operator that returns its child's stream unchanged reports nothing.** `SortExec::execute` does this when the input is already sorted and there is no fetch. `SortPreservingMergeExec` and `CoalescePartitionsExec` do it for a single input partition with no fetch. These get one all-null row.

## Baseline

`BaselineMetrics::new` (`pec`, `baseline`) registers all six for one partition. `BaselineMetrics::record_poll` is the usual driver: a ready batch records output, end of stream or an error calls `done`.

**`output_rows`.** Rows in batches the operator's output stream returned, added by `BaselineMetrics::record_output`.

**`output_batches`.** Incremented once per batch by `RecordOutput for RecordBatch`. The `usize` form of `record_output` adds rows without a batch. On a scan this counts batches before `BatchSplitStream` re-slices them, so the parent can receive more batches than the scan reports.

**`output_bytes`.** Per batch, `get_record_batch_memory_size` (`datafusion-common`, `utils::memory`), which sums `Buffer::capacity` over the distinct buffers of that batch. It counts allocated capacity, not the sliced length. Buffers are deduplicated within a batch only, so an operator that emits one large batch as zero-copy slices counts the whole buffer once per slice. Hash aggregate output does exactly this. Treat it as an upper bound.

**`elapsed_compute`.** Wall-clock time inside whatever spans the operator chose to time. The intent is the operator's own work excluding child polls, and the operators follow it unevenly:

| Operator | What the span covers |
|---|---|
| `ProjectionExec`, `FilterExec` | Expression evaluation and filtering per batch, after the child poll returns. Matches the intent. |
| `AggregateExec`, all streams | Stream construction, per-batch grouping and accumulation, emit, spill. Child polls excluded. |
| `DataSourceExec` over Parquet | Only `project_batch` and the predicate-cache metric copy in `PushDecoderStreamState::transition` (`dsp`). Decode, decompression and I/O are untimed. `FileStream` never touches it. Use the file stream timers for scan cost. |
| `RepartitionExec` | The consumer side of the channel in `PerPartitionStream::poll_next_inner`. The real work runs in spawned tasks and lands in `fetch_time`, `repartition_time`, `send_time`. |
| `SortExec`, `SortPreservingMergeExec` | Sorting and merging. `SortPreservingMergeStream` drops its timer around child polls and emits. |
| `CoalescePartitionsExec`, `UnionExec` | A token span over setup inside `execute`, there so the value is nonzero. Unrelated to data volume. |
| `DataSinkExec` over `ParquetSink` | See [Sink](#sink). Summed across parallel writer tasks, always at partition 0. |

**`start_timestamp`.** `Utc::now()` when `BaselineMetrics::new` ran. For most operators that is inside `execute`, before any data flows. `RepartitionExec` builds its baseline on the first poll of the output stream. Folds to the earliest.

**`end_timestamp`.** Set by `BaselineMetrics::done`, which `record_poll` calls at end of stream or on error, and otherwise by `Drop` through `try_done`, which only writes if unset. The aggregate streams and `FilterExec` never reach `done` on normal completion, so theirs is the moment the consumer dropped the stream. Folds to the latest. `end - start` bounds the operator's lifetime from above. It is not its busy time.

The Parquet opener builds an extra `BaselineMetrics` per file in `ParquetMorselizer::prepare_open_file`. Those add zero rows, bytes and batches, but their timestamps fold into the scan's row.

**`output_rows_skew`** (ratio, null partition). Derived, not recorded. `DataSourceExec::metrics` (`ds`, `source`) appends it on every call, only for Parquet scans, from `BaselineMetrics::output_rows_skew_metric`. With per-partition `output_rows` values `r`, the score is `1 - (sum(r)^2 / sum(r^2) - 1) / (partitions - 1)`, clamped to `[0, 1]`. `_num` is the score times 10000, rounded, and `_den` is 10000. Zero is perfectly balanced, 10000 is every row in one partition, a single partition scores 0, and no rows gives 0/0. Partitions that registered no `output_rows` are not counted. File stream work stealing makes per-partition row counts vary between runs, so this value does too.

## File stream

`FileStreamMetrics` (`ds`, `file_stream::metrics`), one set per scan partition, all driven from `ScanState::poll_scan`. Vortex scans report these too, since they run on the same `FileStream`. Within a partition the stream has one file in flight, so opening and scanning never overlap. Both current morselizers produce one morsel per file, which the notes below assume. Several field doc comments upstream are stale, and the code is what is described here.

**`files_opened`.** Incremented when `Morselizer::plan_file` returns `Ok` for a file popped from the work source. No I/O has happened yet. A Parquet file that file-level pruning then discards still counts. With work stealing on, the default, a partition counts the files it took, which may come from another partition's file group.

**`files_processed`.** Incremented when a file ends for any reason: reader end of stream, planning finished with nothing to read, or an error under `OnError::Skip`. When a limit ends the scan it adds 1 plus `WorkSource::skipped_on_limit`, which is every unopened file left in a local queue and 0 for a shared queue. It can therefore exceed `files_opened`.

**`file_open_errors`.** Errors from `plan_file`, `MorselPlanner::plan`, or the pending planner's I/O. A `plan_file` error is counted only under `OnError::Skip`.

**`file_scan_errors`.** Error items yielded by the active reader.

**`time_elapsed_opening`.** From just before `plan_file` until the file's morsel becomes the active reader, or the file ends without one. A raw `Instant` difference across polls, so it includes time the consumer was not polling. For Parquet it spans file-level pruning, the footer fetch, filter preparation, row group pruning, page index and bloom filter fetches, and building the decoder. Summed over the partition's files.

**`time_elapsed_scanning_until_data`.** From the moment a reader becomes active until its first batch, error, or end of stream. Summed over files. A file that yields no batches still contributes.

**`time_elapsed_scanning_total`.** The lifetime of each active reader, summed. The timer restarts just before each batch is returned, so time the batch spends with downstream operators and any backpressure is included. It grows when the parent is slow.

**`time_elapsed_processing`.** Total time inside `ScanState::poll_scan`, which is time inside the stream's `poll_next`. The only one of the four that excludes unpolled time. It covers planning and opening polls as well as reader polls, so it overlaps the polled part of `time_elapsed_opening`. Neither this nor `time_elapsed_scanning_total` contains the other. This is the closest thing to scan cost that the scan reports.

**`batches_split`.** `SplitMetrics` (`pec`, `baseline`), registered by `DataSourceExec::execute` for every data source. `BatchSplitStream::next_sliced_batch` (`pp`, `stream`) increments it once per slice emitted from an input batch with more rows than the session `batch_size`. It counts output slices, not input batches split. A batch of `batch_size` rows or fewer adds nothing.

## Parquet scan

`ParquetFileMetrics::new` (`dsp`, `metrics`) registers a full set per call, labeled with the filename. It is called once per file by the opener in `ParquetMorselizer::prepare_open_file` and again by each `ParquetFileReaderFactory::create_reader`, which happens twice for a file when bloom filters are loaded. The readers' sets carry `bytes_scanned` and `scan_efficiency_ratio`. The opener's set carries everything else. Our per-partition fold sums across these sets and across files.

Row group pruning in the opener runs in this order, each stage seeing the survivors of the one before: byte range, statistics, bloom filter, limit, page index. The byte range stage is counted nowhere. With no predicate, the statistics and bloom filter metrics still report every row group as matched, and the page index metrics stay 0/0.

### I/O and timers

**`bytes_scanned`.** Sum of the requested range lengths in `ParquetFileReader::get_bytes` and `get_byte_ranges` (`dsp`, `reader`), added before the read is awaited. These are compressed on-disk bytes for data pages, bloom filters, and a page index loaded after the footer. Footer and metadata bytes are not counted, because `DFParquetMetadata::fetch_metadata` reads from the `ObjectStore` directly. Extra bytes the store reads when it coalesces ranges are not counted either.

**`metadata_load_time`.** `ArrowReaderMetadata::load_async` inside `PreparedParquetOpen::load`: the footer fetch, thrift decode, and Arrow schema derivation. The page index is excluded because the load passes `PageIndexPolicy::Skip`, and the later page index fetch falls under no timer. With the metadata cache hit it is close to zero. It is never exactly zero.

**`statistics_eval_time`.** All of `RowGroupAccessPlanFilter::prune_by_statistics`, which includes `identify_fully_matched_row_groups`. That second step builds and evaluates the negated predicate, so the span covers two pruning passes. Building the original `PruningPredicate` and mid-scan dynamic filter pruning are untimed. Zero with no predicate.

**`bloom_filter_eval_time`.** In-memory evaluation of the predicate against bloom filters already loaded. Fetching them happens earlier in `load_bloom_filters`, untimed but counted in `bytes_scanned`. Two nested guards time it, one in the opener's `prune_bloom_filters` and one in `prune_by_bloom_filters`, so the value is about twice the real time. The outer guard runs unconditionally, so it is nonzero for every file that reaches the decode stage.

**`page_index_eval_time`.** All of `PagePruningAccessPlanFilter::prune_plan_with_page_index_and_metrics`. Excludes loading the page index. Zero unless page index pruning is enabled and a single-column page predicate exists.

**`row_pushdown_eval_time`.** Time inside `DatafusionArrowPredicate::evaluate` (`dsp`, `row_filter`), summed over all pushed-down predicates. It covers expression evaluation only. Decoding the filter columns happens in the parquet reader before `evaluate` is called and is usually the larger cost. Zero unless `pushdown_filters` is on and a predicate exists.

### Row filter

**`pushdown_rows_pruned`, `pushdown_rows_matched`.** Counted in `DatafusionArrowPredicate::evaluate`. `build_row_filter` gives every predicate the pruned counter and only the last predicate the matched counter. So pruned is the total rejected across predicates, a null result counting as rejected, and matched is the rows that passed all of them. Their sum is the rows fed to the first predicate, which is what remains after row group and page pruning. Zero unless `pushdown_filters` is on.

**`predicate_cache_inner_records`, `predicate_cache_records`** (gauges). Copied from parquet's `ArrowReaderMetrics::records_read_from_inner` and `records_read_from_cache` by `PushDecoderStreamState::copy_arrow_reader_metrics` each time a batch is emitted. A file that emits no batch reports 0. Only `CachedArrayReader::read_records` feeds them, and only columns used by both a pushed-down filter and the output projection are cached. Both are summed over columns, so 100 rows in 2 cached columns reads 200. The inner count adds whole decoded batches, not just the selected rows. Zero without filter pushdown.

**`predicate_evaluation_errors`.** Swallowed failures during pruning: a `PruningPredicate::prune` error at the statistics, bloom filter, page index, or dynamic filter stage, a bloom filter that failed to load, or missing page row counts. Each makes that stage keep everything. After a statistics stage error the file adds nothing to `row_groups_pruned_statistics`. Row filter evaluation errors are not counted here. They fail the query.

**`num_predicate_creation_errors`** (null partition). Listed under Vortex in `METRICS`, but both the Vortex and Parquet openers register it, once per file, and pass it to `FilePruner` (`pruning`). It counts failures to build a `PruningPredicate`, in `PruningPredicateBuilder::build`, or to evaluate one against file statistics, in `FilePruner::should_prune`. The only effect is that the file is not pruned. It has nothing to do with converting expressions to Vortex. In Vortex a filter that cannot convert is a hard error.

### Pruning

Each pruning metric fills `_pruned` and `_total`, where total is pruned plus matched.

**`files_ranges_pruned_statistics`.** Unit: `PartitionedFile`, which may be a byte range of a file. `PreparedParquetOpen::prune_file` evaluates the predicate against file-level statistics through `FilePruner`, before any footer I/O, and counts the file as pruned or matched. It counts every opened file as matched even with no predicate. `EarlyStoppingStream` rechecks after each batch when the predicate holds a dynamic filter, and moves a file from matched to pruned if it now fails, after part of it was read. Files removed at planning time never appear.

**`row_groups_pruned_statistics`.** Unit: row groups left after the byte range. `prune_by_statistics` evaluates min, max and null counts from the footer. When statistics pruning is disabled or no pruning predicate exists, all are counted matched. DataFusion also tracks a fully matched count, for row groups where every row must pass. Our table drops it.

**`row_groups_pruned_bloom_filter`.** Unit: row groups that survived statistics. `prune_by_bloom_filters`. Matched usually means no bloom filter existed for the column, not that one was consulted and passed. With `bloom_filter_on_read` off, or with no predicate, all survivors are counted matched.

**`limit_pruned_row_groups`.** `prune_by_limit` runs when the scan has a limit and need not preserve order. If fully matched row groups alone hold enough rows for the limit, it keeps only those and adds the number dropped to pruned. Matched is never incremented, so `_total` equals `_pruned`. Zero without a predicate, because no row group is fully matched.

**`page_index_pages_pruned`.** Per surviving row group that is not fully matched, pruned plus matched equals the page count of the row group's first column chunk. Page indexes from every single-column predicate are intersected in that first column's numbering, so the numbers mean something only when the columns share a page layout. Stays 0/0 without a page predicate.

**`page_index_rows_pruned`.** Same stage, in rows: selected and skipped rows of the combined `RowSelection`, over the same row groups. The total is not the file's row count.

**`row_groups_pruned_dynamic_filter`** (count). Row groups dropped mid-scan by `RowGroupPruner::should_prune` (`dsp`, `push_decoder`), which re-evaluates footer statistics against a tightened dynamic filter at each row group boundary. It runs only when the predicate holds a live dynamic filter, more than one row group is planned, and there is no row selection, so page index pruning disables it. The earlier stages already counted these row groups as matched, and nothing subtracts them.

**`page_index_pages_skipped_by_fully_matched`** (count, registered only when nonzero). First-column page count of the fully matched row groups, for which page index evaluation was skipped. It measures work avoided, not data pruned.

**`page_index_load_skipped`** (count, registered only when nonzero). Unit: files. Adds 1 when a page predicate exists but `should_load_page_index` is false. That happens when every surviving row group is fully matched, when none survive, or when the file has no page index for the predicate's columns.

**`scan_efficiency_ratio`.** Written only when a `ParquetFileReader` drops: `_num` gains that reader's `bytes_scanned`, and `_den` is set to the file's full object size. It merges by `RatioMergeStrategy::AddPartSetTotal`. Within one file that is right. Across files it is not, and both our per-partition fold and DataFusion's own aggregation produce bytes scanned over all files divided by the size of the last file folded. For a partition that reads more than one file, ignore `_den` and compare `bytes_scanned` with known file sizes. A higher ratio means more of the file was read.

## Repartition

`RepartitionMetrics::new` (`pp`, `repartition`) registers these per input partition, timed in the spawned `RepartitionExec::pull_from_input` task. Their `partition` is the input partition. The baseline and spill metrics on the same operator are per output partition. A `RepartitionExec` row therefore mixes input-side timers with output-side counts under one partition number.

**`fetch_time`.** Wall-clock wait for each `next()` on the input stream, plus the `input.execute` call. It contains all upstream compute, I/O and scheduling delay.

**`repartition_time`.** `BatchPartitioner` work. For hash or range partitioning that is evaluating the key expressions, hashing or binary search, and the `take` that builds the output batches. Round robin starts no timer, so it stays exactly 0.

**`send_time`.** Registered once per input and output partition pair, the output partition being a label only. Our fold sums over output partitions, giving the input task's total send time. The span covers coalescing into the output's shared `LimitedBatchCoalescer` including its mutex, the `try_grow` memory reservation, writing the batch to a spill file if the reservation fails, and awaiting the channel send. The channel gate in `distributor_channels` closes when no output channel is empty, so one slow consumer inflates `send_time` toward every output partition.

`RepartitionExec` registers the spill metrics per output partition, but `consume_input_streams` wires every channel to the `SpillMetrics` of whichever output partition executed first. The others stay 0.

## Aggregate

`AggregateExec::execute_typed` (`pp`, `aggregates`) picks one of nine streams. With `execution.enable_migration_aggregate` on, the default, single group-by aggregates run the migrated streams: `PartialHashAggregateStream`, `FinalHashAggregateStream`, `SingleHashAggregateStream`, the ordered variants, and `PartialReduceHashAggregateStream`. The legacy `GroupedHashAggregateStream` handles the rest, such as grouping sets. `AggregateStream` runs when there is no GROUP BY and reports baseline metrics only.

**`peak_mem_used`** (gauge). Registered only by the legacy `GroupedHashAggregateStream`. A null here usually means a migrated stream ran. `update_memory_reservation` sets it to the maximum successful reservation size: accumulators, group values, group ordering and the group index buffer, plus an equal sort headroom when the stream is in spill mode. That roughly doubles it in Final mode. It excludes the input batch, the output batch, and the sort buffer reserved during a spill. Summing it across partitions gives a bound on the real peak, not the peak.

**`spill_count`, `spilled_bytes`, `spilled_rows`.** `SpillMetrics` (`pec`, `baseline`), updated in `InProgressSpillFile` (`pp`, `spill`). The count is spill files created. Bytes are bytes written to disk, after IPC encoding and any spill compression. Rows are rows written. The spill merge writes through the same `SpillManager`, so merge-phase re-spills count too. The Partial streams register these and never spill, so zero there says nothing. The no-grouping and top-k streams do not register them.

**`skipped_aggregation_rows`.** Registered only in Partial mode, and only when `skip_partial_aggregation_probe_ratio_threshold` is below 1. `SkipAggregationProbe::record_skipped` adds the rows of each input batch that arrives after the stream decided to stop grouping. Those rows pass through one to one as aggregate state. Rows seen during probing are not counted.

**`reduction_factor`** (ratio). Partial mode only, and not `PartialReduce`. `_den` is input rows and `_num` is output rows, added per batch, so lower means more reduction. Rows handled in skip mode are in neither count, so after a skip the ratio describes only the rows before the switch. Read it next to `skipped_aggregation_rows`.

The four `GroupByMetrics` timers (`pp`, `aggregates::group_values::metrics`) sit inside `elapsed_compute` spans and do not add up to it. Their meaning depends on which stream ran:

| Timer | Legacy `GroupedHashAggregateStream` | Migrated hash, `AggregateHashTable` | Migrated ordered, `OrderedAggregateTable` |
|---|---|---|---|
| `time_calculating_group_ids` | `GroupValues::intern` only. Group-by expressions are evaluated before the timer starts. | `evaluate_group_by` only. `intern` is excluded. | Same as migrated hash. |
| `aggregate_arguments_time` | Argument expressions. `FILTER` expressions are excluded and fall under no timer. | Argument and `FILTER` expressions. | Same as migrated hash. |
| `aggregation_time` | The accumulators' `update_batch` or `merge_batch`. Inflated: `add_elapsed` sits inside the per-accumulator loop with a start instant fixed before it, so with N aggregates the i-th accumulator's time is added N - i + 1 times. It can exceed `elapsed_compute`. | `intern` and the accumulator calls, under one guard. | Accumulator calls only. `intern` falls under no timer. |
| `emitting_time` | `GroupValues::emit` and the accumulators' `evaluate` or `state`. This includes materializing the table for a spill, and excludes the spill sort and write. | Same, without spill. | Same. |

`GroupedTopKAggregateStream` registers all four. It never touches `aggregation_time`, and its `time_calculating_group_ids` covers the priority-map inserts, which is the aggregation work itself.

## Sink

`DataSinkExec` has no baseline metrics and reports whatever its `DataSink::metrics` returns. Among DataFusion's sinks only `ParquetSink` returns any. `VortexSink` registers the same two names. Both counters have a null partition.

**`rows_written`.** Parquet: the footer's `num_rows`, added once per file when that file's writer task joins in `ParquetSink::spawn_writer_tasks_and_join` (`dsp`, `sink`). It stays 0 until a file completes. It is the same `Count` the sink returns as its count row. Vortex: `WriteSummary::row_count` per completed file.

**`bytes_written`.** Parquet: the sum of `RowGroupMetaData::compressed_size` from the finished footer, added at the same moment. It excludes the footer, page indexes and other file overhead, so it is below the object's size. Vortex: `WriteSummary::size`, the full file size. The two formats are not comparable on this column.

`ParquetSink` also registers `elapsed_compute` at partition 0, so the sink has a partition 0 row beside its null-partition row. On the sequential write path it is time inside polls of each file's write future, through `ElapsedComputeFuture`, and appears only when that future completes. On the parallel path it is time in `ArrowColumnWriter::write` and the writer close. Both paths sum across concurrent tasks, so it can exceed the run's wall-clock time.
