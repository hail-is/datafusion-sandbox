# Where DataFusion 55 keeps, uses or loses ordering in physical planning

Research for [hail-is/datafusion-sandbox#318](https://github.com/hail-is/datafusion-sandbox/issues/318), on the map [#311](https://github.com/hail-is/datafusion-sandbox/issues/311). It feeds two decisions from [#315](https://github.com/hail-is/datafusion-sandbox/issues/315): the backend's preference for ordered operators, and the list of cases that need our own physical rules. Read alongside ADRs [0007](../docs/adr/0007-use-a-sorted-table-for-ordered-file-scans.md), [0011](../docs/adr/0011-recover-file-order-instead-of-proving-it.md), [0012](../docs/adr/0012-preserve-declared-ordering-with-a-data-source-wrapper.md) and [0014](../docs/adr/0014-hold-the-merge-tree-with-the-sinks-ordering-requirement.md).

## Sources and conventions

- **DataFusion 55.1.0**, this crate's pin. Read in the cargo registry (`~/.cargo/registry/src/index.crates.io-*/datafusion-*-55.1.0/`). Citations look like `datafusion-physical-plan-55.1.0/src/...:LINE`; some sections shorten the crate to `physical-plan-55.1.0`, and `core-55.1.0` means the `datafusion` facade crate.
- **apache/datafusion `main`** at `0442090c3d925ca9f42092511614ff2dab28d9d8` (2026-10-26), cited as `main:datafusion/...:LINE`. `main` will become v56. The PyPI wheel on the map is built on 55.2; nothing below depends on a 55.1→55.2 difference.
- **Issues and PRs.** A bare `#NNNNN` is an **apache/datafusion** issue or PR. Our own issues are written `hail-is/datafusion-sandbox#N`.
- **How each claim is known:** **read** = read in source or in an issue; **ran** = observed in one of the probes below; **inferred** = reasoned from code that was read but not run.
- **Probes**, throwaway code kept under [`df-ordering-probes/`](df-ordering-probes/) for reference. Neither is built by the crate.
  - `explain_probe.rs` ran as a temporary example in this crate (DataFusion 55.1). It prints the physical plan of 32 SQL queries over CSV tables declared `WITH ORDER (contig, pos)`. The columns are `NOT NULL` and each table is one file, so each scan is one ordered partition, like our sorted table. It runs under four sessions: `target_partitions` (tp) 1 or 4, `prefer_hash_join` true or false, and `prefer_existing_sort` true or false. Output: `explain_probe.out.txt`.
  - `assert_probe.rs` (with `assert_probe.Cargo.toml`) ran as a standalone crate on DataFusion 55.1. It wraps the unoptimized physical plan in an `AssertExec`, which requires `SinglePartition` plus a hard ordering, much like the ordering assertion from #315. It then runs the default physical optimizer rules one at a time and prints the plan after each rule that changes it. Scenarios are lettered A–L. Output: `assert_probe.out*.txt`.

## Answers in brief

1. **Joins.**
   - *When SMJ is chosen.* The planner chooses sort-merge join (SMJ) only if `prefer_hash_join=false`, `target_partitions>1` and `repartition_joins=true`, the join has equality keys, and it is not a null-aware anti join. Otherwise it chooses a hash join. **With one target partition, `prefer_hash_join=false` is silently ignored** (read; ran). No optimizer rule picks SMJ because the inputs are already sorted.
   - *What SMJ requires.* SMJ requires each input sorted `ASC NULLS FIRST` on the keys, in the order written in `ON`. An input ordered on a permutation of the keys gets sorted (ran). For non-nullable keys, `NULLS LAST` still satisfies the requirement.
   - *What SMJ declares.* It declares the left input's order for Inner, Left, LeftSemi, LeftAnti and LeftMark joins, the right input's order for Right, RightSemi, RightAnti and RightMark, and **no order for Full**.
   - *Hash joins.* A hash join keeps and declares probe-side (right) order for Inner, Right, RightSemi, RightAnti and RightMark joins. Because statistics can trigger a side swap, the order kept depends on statistics.
2. **Aggregation.**
   - `AggregateExec` chooses `Sorted` when the input order covers every group key, in any order or direction. It chooses `PartiallySorted` when the order covers some keys, and `Linear` otherwise. It never sorts just to enable streaming.
   - The output is declared ordered on the group keys.
   - With a one-partition ordered source and tp=4, the partial and final stages are joined by an order-preserving hash repartition, then a merge. No sort appears, whatever `prefer_existing_sort` is set to (ran).
3. **Windows.**
   - `BoundedWindowAggExec` is used when every expression is bounded. A frame ending in `UNBOUNDED FOLLOWING`, `ntile`, `cume_dist` or `percent_rank` forces the blocking `WindowAggExec`.
   - Both operators reuse an existing order that covers PARTITION BY and then ORDER BY, including a reversed order for reversible functions (ran).
   - On bounded input only `Sorted` mode is used, so a partially matching order is re-sorted.
4. **Projections, unions and repartitions.**
   - *Projections.* Renames keep the order, as do widening casts and filters. A column fixed to a constant by `WHERE col = lit` satisfies any position in a requirement. `pos + 1`, `pos * -1`, `concat`, narrowing casts, and `get_field` on a struct-ordered column **lose** the order (ran). A UDF keeps order only by overriding `output_ordering`, and keeps the keys after it only if it also overrides `strictly_order_preserving`.
   - *Unions.* The planner always builds `UnionExec`. `UnionExec` declares the common prefix of its children's orderings, and an ordered union becomes a merge (`SortPreservingMergeExec`, SPM) over the `UnionExec` (ran). `InterleaveExec` declares no order, but it replaces a union only when every child has identical hash or range partitioning; `prefer_existing_union=true` rules it out.
   - *Repartitions.* Any repartition over a single input partition keeps the order in each output partition. A merging `RepartitionExec(preserve_order)` over several input partitions is kept when `prefer_existing_sort` is set, or when it lets a streaming operator above it keep streaming.
5. **Sort elimination.**
   - In 55.1 `EnforceDistribution` and `EnforceSorting` have been merged into a single rule, `EnsureRequirements`. It adds a `SortExec` wherever a hard requirement is unmet and removes sorts that turn out to be redundant.
   - The later `PushdownSort` rule can **delete** an inserted `SortExec` by reordering the files in a scan by statistics (ran).
   - `SanityCheckPlan` accepts a requirement met by a sort.
   - The assertion check should walk the final plan below the assertion. It follows `maintains_input_order` edges, every child of a `UnionExec`, and SPMs, and fails on `SortExec` (including TopK), `PartialSortExec` and `PartitionedTopKExec`. The natural place for it is the assertion node's own `check_invariants(Executable)`.
6. **Known gaps.**
   - **No operator in 55.1 or `main` uses sortedness for a locus interval join.** An equality on contig plus range predicates plans as a hash join or SMJ on contig with a residual filter. Range predicates alone plan as a nested-loop join.
   - The piecewise merge join (PWMJ) is experimental, off by default, limited to one range predicate with no equality keys, declares no order, and has a correctness bug in 55.1 (#24360). IEJoin (#23741) is unmerged. ASOF joins arrive in v56 and do nearest-match lookups, not overlap joins.
   - Other gaps: SMJ cannot be chosen from orderings (#26260/#26261). Combining another side's order into a join's declared order is unsound (#25052). A merging repartition can wrongly claim several orderings at once (#26745). Multi-file scans drop compound orderings (the composed-row check that ADRs 0011 and 0012 already work around).

The two lists the ticket asks for are at the end: [by configuration alone](#ordered-operators-the-backend-gets-by-configuration-alone) and [needs our own physical rules](#cases-that-need-our-own-physical-rules).

## What the probes showed

Plans from `explain_probe.out.txt`. "sort" means a `SortExec` somewhere in the plan, and "no sort" means none. All **ran**. Setting `prefer_existing_sort` to false changed none of these plans: every source is a single partition, so every repartition keeps its order without merging.

| Query (over tables ordered `(contig, pos)`) | tp=1 | tp=4, `prefer_hash_join=false` | tp=4, `prefer_hash_join=true` |
|---|---|---|---|
| Inner join on `(contig, pos)` + `ORDER BY` key | hash join (CollectLeft), no sort | SMJ over hash repartitions (1 input partition, order kept) → SPM, no sort | partitioned hash join → SPM, no sort |
| Same, `ON` written `pos, contig` | hash join, no sort | **SMJ sorts both inputs on `(pos, contig)`, then sorts the output** | hash join, no sort |
| Left join + `ORDER BY` left key | hash join, **sort** | SMJ, no sort | hash join, **sort** |
| Right join + `ORDER BY` right key | hash join, no sort | SMJ, no sort | hash join, no sort |
| Full join + `ORDER BY coalesce(keys)` | **sort** | **sort** | **sort** |
| `EXISTS` (left semi) + `ORDER BY` | hash join, **sort** | SMJ, no sort | hash join, **sort** |
| Equality on `contig`, `a.pos < b.pos` | hash join with filter, **sort** | SMJ on `contig` with filter, no sort | hash join, **sort** |
| Interval join: `contig =`, `pos >= start AND pos < end` | hash join with filter, **sort** | SMJ on `contig` with filter, no sort | hash join, **sort** |
| Range predicates only, no equality | nested-loop join, **sort** | nested-loop join, **sort** | nested-loop join, **sort** |
| `GROUP BY contig, pos` / `GROUP BY contig` / `GROUP BY pos, contig` | `Sorted`, no sort | Partial `Sorted` → order-preserving hash repartition → FinalPartitioned `Sorted` → SPM, no sort | same as previous column |
| `GROUP BY contig, v` | `PartiallySorted([0])` | `PartiallySorted([0])` both stages | same |
| `first_value(v ORDER BY pos) GROUP BY contig` | `Sorted`, no sort | `Sorted`, no sort | same |
| Window `PARTITION BY contig ORDER BY pos ROWS UNBOUNDED PRECEDING` | bounded window, `Sorted` | hash repartition on contig (1 input partition, order kept), bounded window, no sort | same |
| Window `ORDER BY contig DESC, pos DESC` (reverse of input) | bounded window, no sort | same | same |
| Window frame `CURRENT ROW .. UNBOUNDED FOLLOWING` | `WindowAggExec`, no sort | same | same |
| `pos + 1`, `pos * -1`, `concat(contig, ':')`, `ORDER BY` struct column | **sort** | **sort** | **sort** |
| `CAST(pos AS DOUBLE)`; `named_struct(...)['contig']` | no sort | no sort | no sort |
| `WHERE v > 3`; `WHERE contig = '1' ORDER BY pos` | no sort | SPM, no sort | same |
| `UNION ALL` + `ORDER BY` key | SPM over `UnionExec`, no sort | same | same |
| `ORDER BY pos` (contradicts the declared order) | **sort** | **sort** | **sort** |

Notes on the table:
- In the range-only joins the scans lose their declared ordering altogether: once `contig` is projected away, `(pos)` alone is not a valid ordering.
- `named_struct(...)['contig']` is simplified away at the logical level, so that case says nothing about struct orderings. `assert_probe` scenarios E–E4 cover structs at the physical level (Q4 below).

`assert_probe` adds the sort-detection results:
- The `PushdownSort` absorption (scenario J): a sort is inserted, then removed by reordering files.
- The same run with `enable_sort_pushdown=false` keeps the sort (L).
- An unrelated TopK under the assertion still needs a sort for the asserted keys (K).
- A union branch with a longer ordering (H3).

## Where the sources disagree, and how this note reads them

- **#23338 against our run (aggregation).** #23338 says a sorted aggregate at tp>1 needs a `SortExec` after the hash repartition between partial and final, which makes it slower than hashing. Neither probe saw that in 55.1. With a one-partition ordered source, the repartition was an order-preserving merge with no sort, kept even with `prefer_existing_sort=false`. Our runs match `preserving_order_enables_streaming`, added by PR #21107 and present in 55.1. The issue may describe another input shape or an older build (**inferred**). Either way, the final stage pays for a k-way merge per output partition, and #23338's point stands: no cost check compares sorted against hash aggregation.
- **Is `PushdownSort` absorption "sorting"?** It reorders whole files using min/max statistics and sorts no rows. Its `Exact` result requires file groups that don't overlap, and files without NULLs in the sort columns. A check in the final plan would accept this, and a check placed right after `EnsureRequirements` would reject it. Under #315 the assertion rejects a plan that "can only meet [the order] by sorting", so accepting file reordering seems right, but it is a policy decision (see "For the map" on the ticket). For our sorted table the question doesn't come up: `OrderedSource` declares its recovered order and answers `Exact` itself (`src/stored/sorted_table/ordered_source.rs:172-186`). For any other requested order it deliberately delegates to the inner config without re-wrapping, as its comment says (**read**).
- **Hash repartition under SMJ.** The joins section says the hash repartition under SMJ destroys the order at default settings. That holds for an input with several partitions. Our probes used one-partition sources, which keep the order in every output partition, so no sort appeared. Both agree with `RepartitionExec::maintains_input_order = preserve_order || input partitions <= 1` (`datafusion-physical-plan-55.1.0/src/repartition/mod.rs:2039-2045`).


## Q1–Q3: Joins, aggregation and windows: how DataFusion 55.1 keeps, uses or loses a declared ordering

Sources: crates in `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/` (cited as `<crate>-55.1.0/src/...:LINE`), the apache/datafusion `main` snapshot 0442090c (cited as `main:datafusion/...:LINE`), and GitHub issues/PRs fetched through the REST API (cited as `#NNNNN`).
Labels: **read** means I read it in source or an issue, **ran** means I executed it, **inferred** means I reasoned from code I read. This section was written from source alone. Where a claim here was later checked by a probe, the probe table above says so.

Background for all three questions:
- 55.1 no longer has separate `EnforceDistribution`/`EnforceSorting` rules. They are merged into one `EnsureRequirements` rule. It runs a top-down join-key reorder, then distribution enforcement, then sorting enforcement (both bottom-up). After that come `parallelize_sorts`, `replace_with_order_preserving_variants`, `pushdown_sorts` and `replace_with_partial_sort`. **read** `datafusion-physical-optimizer-55.1.0/src/ensure_requirements/mod.rs:180-247`, `datafusion-physical-optimizer-55.1.0/src/optimizer.rs:101,136`. `main` keeps the same structure and adds a "Phase 0" that turns Interleave back into Union. **read** `main:datafusion/physical-optimizer/src/ensure_requirements/mod.rs`.
- Rule order: `JoinSelection` (101) → `EnsureRequirements` (136) → `CombinePartialFinalAggregate` (138) → `OptimizeAggregateOrder` (140) → … → `SanityCheckPlan` (190). **read** `datafusion-physical-optimizer-55.1.0/src/optimizer.rs:94-190`.
- Ordering satisfaction goes through equivalence properties, so constants, equal columns and permutations of the leading prefix are understood. NULLS FIRST/LAST is ignored only when the column is **not nullable**. **read** `datafusion-physical-expr-common-55.1.0/src/sort_expr.rs:158-170,326-338`.
- `RepartitionExec` keeps order when `preserve_order` is set or when its input has ≤1 partition. `with_preserve_order()` only takes effect when the input is ordered and has more than 1 partition. **read** `datafusion-physical-plan-55.1.0/src/repartition/mod.rs:2039-2045,2088-2098`.
- `EnsureRequirements` always inserts hash and round-robin repartitions in their order-preserving form (`.with_preserve_order()`). **read** `datafusion-physical-optimizer-55.1.0/src/ensure_requirements/enforce_distribution.rs:692,1107,1383`.
  - It reverts them to plain repartitions plus a `SortExec` unless one of these holds: the consumer's ordering requirement is satisfied through them and `prefer_existing_sort=true` or the plan is unbounded (`:1166-1167,1443-1458`), or the order lets a single-child parent keep streaming ("streaming benefit", `:750-779,1426-1430`).
  - Ancestors that don't need the order also revert them (`:1462-1488`, propagated via `update_children` `:1550-1580`). **read**
  - The streaming-benefit exception came from PR #21107 (merged 2026-03-26) and is in 55.1. **read** (GitHub)
- Later, `replace_with_order_preserving_variants` uses order-preserving `RepartitionExec` only when `prefer_existing_sort` is set or the input is unbounded. It always tries `SortPreservingMergeExec` (`is_spm_better = true`). **read** `.../enforce_sorting/replace_with_order_preserving_variants.rs:272-274`, `.../ensure_requirements/mod.rs:230`.
- Caveat on order-preserving repartitions: open bug #26745. When a `RepartitionExec` with `preserve_order=true` has an input with several declared orderings, it merges on the first one but keeps claiming all of them, which gives wrong `ORDER BY` results. 55.1 has this code (`repartition/mod.rs:2047-2063,2101-2107`), and `main` is unchanged (`main:datafusion/physical-plan/src/repartition/mod.rs:2199-2215,2268`). **read**

### Q1. Joins

**Short answer:**
- The planner picks `SortMergeJoinExec` only when all of these hold: `prefer_hash_join=false`, `target_partitions>1`, `repartition_joins=true`, the join has equi-keys, and it is not null-aware.
- No later rule ever turns a hash join into a sort-merge join or back.
- SMJ has a *hard* `required_input_ordering` on the join keys in the planner's key order, always `ASC NULLS FIRST`. `EnsureRequirements` sorts each side unless the side already satisfies that (a prefix of a longer order is fine).
- SMJ never reorders its keys to match an input's *sort order*. 55.1 reorders only to match hash partitioning; `main` also matches range partitioning.
- SMJ declares left order for Inner/Left/LeftSemi/LeftAnti/LeftMark, right order for Right/RightSemi/RightAnti/RightMark, and none for Full.
- Hash join declares probe-side (right) order for Inner/Right/RightSemi/RightAnti/RightMark in every `PartitionMode`. In `Partitioned` mode the probe side is hash-repartitioned, so the order usually doesn't survive.

**How the planner chooses** (`datafusion-55.1.0/src/physical_planner.rs`):
- `prefer_hash_join` is read at `:1570-1571`. **read**
- No equi-keys (`join_on.is_empty()`):
  - `CrossJoinExec` if there is no filter and the join is Inner (`:1574-1577`).
  - Otherwise `PiecewiseMergeJoinExec` if there is exactly one range filter, the join type is not semi/anti/mark, and `enable_piecewise_merge_join` is set (`:1578-1683`, default false, `datafusion-common-55.1.0/src/config.rs:1688`).
  - Otherwise `NestedLoopJoinExec` (`:1693-1700`). **read**
- SMJ when `target_partitions()>1 && repartition_joins() && !prefer_hash_join && !null_aware` (`:1702-1721`). Sort options are hard-coded: `vec![SortOptions::default(); join_on_len]` (`:1719`), which is ASC NULLS FIRST (`arrow-schema-59.2.0/src/lib.rs:162-168`). **read**
- `HashJoinExec` with `PartitionMode::Auto` when the same conditions hold but `prefer_hash_join` is true (`:1722-1738`). **read**
- Otherwise `HashJoinExec` with `PartitionMode::CollectLeft` (`:1739-1750`). This covers `target_partitions==1`, `repartition_joins=false`, and null-aware (`NOT IN`) joins. So with one partition, `prefer_hash_join=false` is silently ignored. **read**
- Join type, `null_equality` and the join filter do not affect the SMJ/HJ choice. They are passed straight into `SortMergeJoinExec::try_new` (`:1713-1721`).
  - SMJ executes every join type: semi/anti/mark go to `BitwiseSortMergeJoinStream`, everything else to `MaterializingSortMergeJoinStream` (`datafusion-physical-plan-55.1.0/src/joins/sort_merge_join/exec.rs:547-591`). **read**
- `main`: SMJ is chosen the same way (`main:datafusion/core/src/physical_planner.rs:1746-1765`, still `SortOptions::default()` at `:1763`). The two hash-join branches are merged into one builder (`:1766-1788`). Null-aware joins without equi-keys are now a plan error (`:1602-1620`), and PWMJ no longer excludes semi/anti/mark joins. **read**
- Open PR #26261 / issue #26260 would derive SMJ key order and sort options from the input orderings and allow SMJ at `target_partitions=1`. **read** (GitHub)

**Join-related settings:**
- Defaults: `prefer_hash_join=true` (`config.rs:1683`), `repartition_joins=true` (`:1559`), `join_reordering=true` (`:1673`), `top_down_join_key_reordering=true` (`:1667`). **read**
- `hash_join_single_partition_threshold` = 1 MiB (`:1692`) and `..._rows` = 128 Ki (`:1696`). These only decide CollectLeft vs Partitioned for `Auto` hash joins inside `JoinSelection` (`join_selection.rs:118-138,200-261`). `main` raises the byte threshold to 4 MiB (`main:datafusion/common/src/config.rs:1855`). **read**
- `enable_join_dynamic_filter_pushdown` (`config.rs:1524`) is a hash-join-only gate (`hash_join/exec.rs:864-867`); SMJ has no dynamic filter code. The pushed filter only drops probe rows; `FilterExec` and sources keep their order (`datafusion-physical-plan-55.1.0/src/filter.rs:550-553`). It does not change the choice between join kinds or the declared order. **read** / **inferred** for "does not change order".
- `allow_symmetric_joins_without_pruning` (`config.rs:1568`) is only used by `SanityCheckPlan` for `SymmetricHashJoinExec` (`datafusion-physical-optimizer-55.1.0/src/sanity_checker.rs:96-104`). **read**

**`JoinSelection`** (`datafusion-physical-optimizer-55.1.0/src/join_selection.rs`):
- It only rewrites `HashJoinExec`, `CrossJoinExec` and `NestedLoopJoinExec`. It never touches `SortMergeJoinExec`. **read** `:312-368`; `grep` shows no SMJ reference in the file.
- Subrules (`:165-168`):
  - Hash join → `SymmetricHashJoinExec`, only when both inputs are unbounded and incremental (`:395-490`).
  - Swap sides when the left input is unbounded and the right is not (`:535-551`).
- Statistics-based selection (`:312-368`):
  - `Auto` becomes `CollectLeft` if one side is under the threshold, swapping so the small side builds (`:200-261`). Otherwise it becomes `Partitioned`, swapping if the left is larger (`:271-301`).
  - Swapping needs `join_reordering`, a join type that supports swapping, a non-null-aware join, and known byte or row statistics (`:88-116`). **read**
  - When statistics are absent: no swap, and `Auto` becomes `Partitioned` (`supports_collect_by_thresholds` returns false at `:126-137`). **read**
- `main` additionally allows swapping null-aware LeftAnti joins (`main:datafusion/physical-optimizer/src/join_selection.rs:176`). It also adds `EliminateConstantJoin`, which turns inner NLJ/cross joins against a single literal row into a `FilterExec` and so keeps the other side's order (`main:datafusion/physical-optimizer/src/optimizer.rs:207`). **read**
- **inferred:** a swap flips which original input becomes the order-preserving probe side.

**SMJ input sorting and order reuse:**
- `required_input_ordering` is a hard requirement of `[left keys]` and `[right keys]` with the per-key options (`sort_merge_join/exec.rs:161-185,428-433`). **read**
- `ensure_sorting` adds a `SortExec` on any child whose equivalence properties don't satisfy it (`.../enforce_sorting/mod.rs:427-445`). **read**
- Prefix: input `(a,b,c)` satisfies join keys `(a,b)`. **inferred** from the `ordering_satisfy_requirement` semantics.
- Keys stricter than the order: input `(a)` vs keys `(a,b)` gets a full sort. `PartialSortExec` is used only for unbounded inputs (`.../enforce_sorting/mod.rs:204`). **read**
- Permutation: input `(b,a)` vs keys `(a,b)` gets a sort. **inferred:** the key order comes from the logical `ON` list, and nothing reorders it to follow sort order:
  - Phase 1 top-down reordering only matches a *parent's hash-key* requirement (`enforce_distribution.rs:199-231,277-313`).
  - The bottom-up alternative (`top_down_join_key_reordering=false`) only matches the *children's `Partitioning::Hash`* (`:450-527,529-554`).
  - **inferred:** Phase 1 can also *reorder SMJ keys away* from the input order to satisfy a parent's hash requirement, which then forces a sort.
- `main` adds `reorder_join_keys_to_range_inputs`. It permutes partitioned-HJ and SMJ keys, and for SMJ the sort options too, to match an input's `Partitioning::Range` ordering, unless that would force a sort on an unbounded input (`main:datafusion/physical-optimizer/src/ensure_requirements/enforce_distribution.rs:558-709`, called at `:1594`). **read**
- Sort options: an input ordered `ASC NULLS LAST` on a *nullable* key, or ordered `DESC`, does not satisfy `ASC NULLS FIRST` and gets re-sorted. Non-nullable keys avoid the NULLS problem (`sort_expr.rs:326-338`). **read**
  - Note that DataFusion's own `ORDER BY` default is NULLS LAST (`default_null_ordering = "nulls_max"`, `config.rs:314`). **read**
- SMJ distribution is co-partitioned `KeyPartitioned(keys)` on both sides (`exec.rs:416-426`), and subset satisfaction is disabled for it (`enforce_distribution.rs:1230-1233,1289-1300`).
  - With `target_partitions` greater than the child's partition count, a hash repartition is added even when the child already satisfies the requirement (`:1357-1363`). The exception is when `preserve_file_partitions` is met.
  - A `Partitioning::Range` child counts as satisfying only if its ordering expressions equal the keys in order, and the two sides must have identical split points and sort options (`datafusion-physical-expr-55.1.0/src/partitioning.rs:425-437`, `datafusion-physical-plan-55.1.0/src/distribution_requirements.rs:332-359`). **read**
- Net effect for a multi-partition ordered input at default settings: hash repartition that loses the order, then a per-partition `SortExec`. With `prefer_existing_sort=true`: an order-preserving (merging) hash repartition and no sort. A single-partition input keeps its order through the repartition. **inferred** from the code paths above.
- A parent's sort requirement can be pushed into an SMJ child only if that child still satisfies the SMJ key requirement and the recomputed output order then satisfies the parent (`.../enforce_sorting/sort_pushdown.rs:460-495,699-760`). **read**

**SMJ declared output ordering** (`exec.rs:209-243,281-306` → `datafusion-physical-expr-55.1.0/src/equivalence/properties/joins.rs:25-107`). Probe/stream side = left for Inner/Left/Full/LeftSemi/LeftAnti/LeftMark, right for the Right* types.

| Join type | `maintains_input_order` | Declared ordering |
|---|---|---|
| Inner | [true,false] | each left ordering followed by each right ordering (`joins.rs:55-73`), plus key equalities left.k≡right.k (`datafusion-physical-expr-55.1.0/src/equivalence/class.rs:800-807`) |
| Left | [true,false] | left ordering followed by right ordering (`joins.rs:55`); no key equivalence |
| LeftSemi / LeftAnti / LeftMark | [true,false] | left orderings only (`joins.rs:75`) |
| Right | [false,true] | right orderings (shifted by left width) followed by left orderings (`joins.rs:79-97`) |
| RightSemi / RightAnti / RightMark | [false,true] | right orderings only, no offset (`joins.rs:100,117-130`) |
| Full | [false,false] | none (`joins.rs:103`) |

All **read**.
- Caveat (open issue #25052, fix PR #25053): appending the other side's order (Inner/Left/Right) is unsound when probe keys repeat. Example with SMJ: `(1,100),(1,200),(1,100),(1,200)` is claimed sorted by `(key,value)`, so required sorts get removed. `main` still has the same code (`calculate_join_output_ordering` and `joins.rs` unchanged). **read** (GitHub + diff)
- The leading key order itself is sound. **inferred**

**Hash join probe order:**
- `maintains_input_order = [false, Inner|Right|RightAnti|RightSemi|RightMark]` in every `PartitionMode` (`hash_join/exec.rs:1011-1023,1339-1341`). The probe side is always the right input (`:1026-1029`). **read**
- Equivalence properties use the same `join_equivalence_properties`, so Inner/Right append the left order after the right order and carry the #25052 caveat. **read** `:1057-1065`
- Distribution (`:1299-1321`): `CollectLeft` requires [SinglePartition, Unspecified], so the probe side is not repartitioned and its per-partition order survives (round-robin inserted above a 1-partition probe keeps order, `repartition/mod.rs:2044`). `Partitioned` is co-partitioned `KeyPartitioned`, so the probe side is hash-repartitioned, keeping order only through an order-preserving repartition (see Background). **read** + **inferred**
- `HashJoinBuffering` adds a `BufferExec` on the probe side, which keeps order (`datafusion-physical-plan-55.1.0/src/buffer.rs:153-155`). **read**
- `main` writes the same rule as `!need_produce_result_in_final(join_type)` (`main:datafusion/physical-plan/src/joins/hash_join/exec.rs:1225-1230`). Open #26228 (build-side order across batches) and #26235 (declare build order for LeftSemi/LeftAnti/LeftMark) are not merged. **read**

**Other joins:**
- `SymmetricHashJoinExec` declares no output order ([false,false], `symmetric_hash_join.rs:270-279`). It requires input order only when the join filter references ordered columns (`join_selection.rs:419-475`, `symmetric_hash_join.rs:441-450`). **read**
- `CrossJoinExec` and `NestedLoopJoinExec` declare no order (`cross_join.rs:156-164` with the default `maintains_input_order=false`, `execution_plan.rs:241-243`; `nested_loop_join.rs:427-429`). **read**
- `PiecewiseMergeJoinExec` declares none for every join type (`piecewise_merge_join/exec.rs:452-466`). **read**

**For a backend that wants ordered joins on locus-ordered inputs:**
- `prefer_hash_join=false` turns *every* eligible equi-join into SMJ, including unsorted ones, and only when `target_partitions>1`.
- The key order follows the SQL `ON` list. Writing `ON` in locus order (contig, position, …) is needed to reuse an order. **inferred**
- Locus key columns should be non-nullable, or the input order must be NULLS FIRST.
- Hash repartitioning undoes the order unless `prefer_existing_sort=true`, the inputs have 1 partition, or (55.1) both inputs are already `Range`-partitioned on exactly the keys with equal split points and at least `target_partitions` partitions.
- Hash joins already keep the probe order for Inner/Right*. **inferred:** stats-driven swaps or `Partitioned` mode can drop it.

**Config-only vs needs own rule:**
- Config-only: SMJ everywhere (`prefer_hash_join=false`, `target_partitions>1`, `repartition_joins=true`). Order-preserving repartition instead of re-sorting (`prefer_existing_sort=true`, which brings the #26745 risk). Keeping probe-side order for hash joins (`join_reordering=false` to stop swaps; `target_partitions=1` or `repartition_joins=false` for CollectLeft without repartition).
- Needs own rule or planner extension:
  - choosing SMJ per join, only when both inputs already satisfy a key order
  - setting SMJ key order and `SortOptions` from the input order (what PR #26261 proposes)
  - SMJ at `target_partitions=1`
  - avoiding hash repartition for range-partitioned inputs with fewer partitions than `target_partitions`, or with keys in a different order than the range ordering (55.1; `main` handles the key order)
  - not trusting the appended-other-side order (#25052)

### Q2. Aggregation

**Short answer:**
- `AggregateExec::try_new` picks its `InputOrderMode` from the input's equivalence properties via `find_longest_permutation(group_by)`:
  - `Sorted` when every group key is ordered (any permutation, any direction) and there are no grouping sets
  - `PartiallySorted(idx)` when only some keys are ordered
  - `Linear` otherwise
- It never asks for a sort on group keys just to stream.
- Order-sensitive aggregates with `ORDER BY` (`array_agg`; UDAFs default to Hard) add a sort; `first_value`/`last_value` don't.
- Output order: the input order projected onto the group columns (non-Linear modes), plus a uniqueness constraint on the group keys.
- In 55.1, a sorted Partial→FinalPartitioned pair keeps an order-preserving hash repartition even with `prefer_existing_sort=false`, because that order lets the final aggregate stream (PR #21107).

**Mode selection** (`datafusion-physical-plan-55.1.0/src/aggregates/mod.rs`):
- `find_longest_permutation` (`:982-983`, `datafusion-physical-expr-55.1.0/src/equivalence/properties/mod.rs:1197`) gives the already-satisfied group-key prefix. Indices nulled by a grouping set are dropped (`:1005-1009`). The mode is chosen at `:1011-1021`. **read**
- The requirement is that satisfied group-key prefix plus the aggregate-ordering requirement, wrapped as **soft** (`OrderingRequirements::new_soft`, `:999`). **read** `:985-999`
- `ensure_sorting` does not look at hard vs soft, so an unsatisfied aggregate `ORDER BY` part still gets a `SortExec` (`.../enforce_sorting/mod.rs:427-445`). This matches the `SoftRequirement` docs ("when possible DataFusion will insert a SortExec", `datafusion-functions-aggregate-common-55.1.0/src/order.rs:28-38`). **read** + **inferred** (no softness check in the code path)
- The group-key part is satisfied by construction, so no sort is ever added to enable `Sorted` mode. **inferred**
- Order-sensitive aggregates (`get_aggregate_expr_req`, `:2705-2746`):
  - The requirement applies only to raw-input modes; it is dropped for Final/FinalPartitioned/PartialReduce (`:2714`).
  - `Insensitive` and `Beneficial` add nothing (`:2721-2726`).
  - Requirements are merged across aggregates, and an aggregate may be swapped for its reverse, e.g. `first_value`↔`last_value` (`:2786-2873`).
  - Conflicting *hard* requirements are a `not_impl` error (`:2859`).
  - `array_agg` is `SoftRequirement` (`datafusion-functions-aggregate-55.1.0/src/array_agg.rs:160-162`). `first_value`/`last_value` are `Beneficial` (`first_last.rs:362-364,1127`). The UDAF default is `HardRequirement` (`datafusion-expr-55.1.0/src/udaf.rs:659-663`). **read**
  - `OptimizeAggregateOrder` then marks beneficial aggregates as running on pre-ordered input when the input satisfies them (`datafusion-physical-optimizer-55.1.0/src/update_aggr_exprs.rs:75-121`). **read**
- Execution: non-Linear modes use the ordered partial/final streams, Linear uses hash streams (`mod.rs:1232-1293`). **read**
- Emission is `Final` (blocking) in Linear mode and follows the input's emission otherwise (`:1404-1408`). **read**

**Partial/final and repartitioning:**
- The planner always builds a `Partial` aggregate followed by `FinalPartitioned` if `groups` is non-empty, `target_partitions>1` and `repartition_aggregations` is set; otherwise by `Final` (`datafusion-55.1.0/src/physical_planner.rs:1091-1131`). `main` is the same (`main:datafusion/core/src/physical_planner.rs:2121-2155`). **read**
- Distribution: Partial is Unspecified; FinalPartitioned and SinglePartitioned are `KeyPartitioned(group keys)`; Final and Single are `SinglePartition` (`mod.rs:2000-2012`). **read**
- `EnsureRequirements` puts an order-preserving hash `RepartitionExec` between them (`enforce_distribution.rs:1376-1386`). The final aggregate was built over the ordered partial, so its soft group-key requirement is satisfied through that repartition. `preserving_order_enables_streaming` is then true: with the ordered child the final aggregate is `Sorted` and incremental, with a coalesced child it is `Linear` and `Final` (`:750-779`). So the order-preserving repartition is kept even without `prefer_existing_sort` (`:1443-1458`). **read** code + PR #21107 text; **inferred** that this fires for every bounded sorted aggregate whose source emits incrementally.
- `Final` mode (single partition) gets a `SortPreservingMergeExec` when the input is ordered (`add_merge_on_top`, `:785-818`). It is kept by the same streaming-benefit rule. **inferred**
- `SinglePartitioned`: `CombinePartialFinalAggregate` merges an adjacent Partial plus Final/FinalPartitioned into Single/SinglePartitioned (`combine_partial_final_agg.rs:88-92`). They are adjacent only when no repartition was needed, i.e. the input already satisfies `KeyPartitioned(group keys)`:
  - exact Hash or Range with ≥ `target_partitions` partitions, or
  - a subset Hash/Range when partitions ≥ `subset_repartition_threshold` (default 4, `config.rs:1647`; `enforce_distribution.rs:1289-1300,1357-1363`)
  - **read** + **inferred**
- Since 55.1, `reorder_aggregate_keys` may reorder the Partial/Final group keys to match a parent's hash requirement and add a projection (`enforce_distribution.rs:315-406`). **read**

**Declared output ordering** (`compute_properties`, `mod.rs:1346-1414`):
- The input's equivalence properties are projected through the group-by mapping (`:1356-1358`). So the output is ordered on the group columns that formed the input's ordered prefix, in the input's direction. **read**
- A `Unique(group cols)` constraint is added (`:1376-1386`). A true no-group aggregate's outputs are marked constant. **read**
- `maintains_input_order = [mode != Linear]` (`:2027-2029`). **read**
- `Linear` (hash) mode: 55.1 does not explicitly clear orderings. **inferred:** projection normally yields nothing, because no group key leads the input order, but grouping-set cases could keep a stale order.
- `main` renames `InputOrderMode` to `GroupClusteringMode` (None/Partial/Full, PR #24697, merged 2026-10-16). It also:
  - excludes constant group keys from the mode
  - forces `None` for grouping sets and PartialReduce
  - explicitly calls `clear_orderings()` when the mode is None
  - (`main:datafusion/physical-plan/src/aggregates/mod.rs:1045-1060,1465-1467,1527,2165-2166`; `main:datafusion/physical-plan/src/aggregates/order/mod.rs:48`). **read**
- Every mode in the output-mode × partitioning grid (Partial/Final/FinalPartitioned/Single/SinglePartitioned) uses the same projection logic. Only the input order seen by *that* node matters. **read**

**For a backend that wants ordered aggregation on locus-ordered inputs:**
- `GROUP BY` on (contig, position, …) over a source that declares that ordering gets `Sorted`/streaming aggregation automatically, in any key order. The order also survives the Partial→Final repartition in 55.1.
- The order-preserving repartition costs a k-way merge per output partition and is exposed to #26745 if the source declares several orderings. **inferred**
- Input that is already locus-range-partitioned on the group keys avoids the repartition entirely (SinglePartitioned). **inferred**

**Config-only vs needs own rule:**
- Config-only (works by default): `Sorted`/`PartiallySorted` selection; keeping order through Partial→Final (no `prefer_existing_sort` needed in 55.1); dropping the Final repartition (`repartition_aggregations=false` or `target_partitions=1`, giving SPM/Single).
- Config-only: `first_value`/`last_value` reuse an existing order; `array_agg ... ORDER BY` reuses it when satisfied, otherwise adds a sort.
- Needs own rule: forcing a sort on group keys to get streaming aggregation (DataFusion never does this); avoiding the merge-repartition while keeping order (e.g. declaring compatible Range/Hash partitioning on group keys from the source); protecting against #26745 / the Linear stale-order case.

### Q3. Windows

**Short answer:**
- The planner always builds `BoundedWindowAggExec(InputOrderMode::Sorted)` when every window expression in the node runs in bounded memory, and `WindowAggExec` otherwise.
- `EnsureRequirements` calls `get_best_fitting_window`, which reuses the input order if it satisfies PARTITION BY (as any permutation, any direction) followed by ORDER BY. It tries ORDER BY reversed as well (flipping reversible functions).
- For *bounded* inputs it accepts only `Sorted` mode. `Linear`/`PartiallySorted` are used only for unbounded inputs. If nothing fits, a `SortExec` is added.
- Windows with PARTITION BY are hash-repartitioned on the partition keys, which loses order unless `prefer_existing_sort=true`. Windows without PARTITION BY need a single partition.

**Planner** (`datafusion-55.1.0/src/physical_planner.rs:922-942`):
- `can_repartition = target_partitions>1 && repartition_windows` (`:922-923`; `config.rs:1601`). **read**
- `uses_bounded_memory()` for all expressions selects `BoundedWindowAggExec::try_new(…, InputOrderMode::Sorted, can_repartition)`; otherwise `WindowAggExec::try_new` (`:925-942`). **read**
- `main` is the same (`main:datafusion/core/src/physical_planner.rs:1138-1150`). **read**

**Which frames and functions force `WindowAggExec`:**
- Aggregate window functions (plain and sliding) are bounded iff the frame's end bound is not `UNBOUNDED FOLLOWING` (`datafusion-physical-expr-55.1.0/src/window/aggregate.rs:218-220`, `sliding_aggregate.rs:155-157`). **read**
  - The default frame without `ORDER BY` is `ROWS UNBOUNDED PRECEDING..UNBOUNDED FOLLOWING`, so `sum(x) OVER (PARTITION BY p)` uses `WindowAggExec`. With `ORDER BY` the default is `RANGE UNBOUNDED PRECEDING..CURRENT ROW`, which is bounded (`datafusion-expr-55.1.0/src/window_frame.rs:149-176`). **read**
- Built-in/UDWF functions need `supports_bounded_execution()` and, if they use the frame, a bounded end (`window/standard.rs:277-285`). **read**
  - Supported: `row_number` (`datafusion-functions-window-55.1.0/src/row_number.rs:159`), `rank`/`dense_rank` (`rank.rs:363-365`), `lead`/`lag` (`lead_lag.rs:738`), `first_value`/`last_value`/`nth_value` (frame-dependent, `nth_value.rs:476-482`).
  - Not supported: `percent_rank`, `cume_dist`, `ntile`, and any UDWF by default (`datafusion-expr-55.1.0/src/partition_evaluator.rs:242-244`). **read**
- One unbounded expression makes the whole node `WindowAggExec` (`.all(...)`). `WindowAggExec` emission is `Final`, i.e. blocking (`window_agg_exec.rs:145`). **read**

**Required and declared ordering:**
- Requirement = PARTITION BY expressions with **no direction** (in the input's permutation order when the input already orders all of them), followed by ORDER BY with options (`windows/mod.rs:304-337`; `bounded_window_agg_exec.rs:182-197,437-445`; `window_agg_exec.rs:237-249`).
  - `calc_requirements` also emits an ORDER-BY-only alternative, but `ensure_sorting` uses only the first one via `into_single()` (`datafusion-physical-expr-common-55.1.0/src/sort_expr.rs:864-868`). **read**
- Distribution: `KeyPartitioned(shortest partition_by)` when `can_repartition` is set, else `SinglePartition` (`bounded_window_agg_exec.rs:329-344,451-460`; `window_agg_exec.rs:150-165,255-262`). **read**
- Both operators have `maintains_input_order=[true]` (`bounded_window_agg_exec.rs:462-464`, `window_agg_exec.rs:233-235`). **read**
- Output equivalence = the input's, plus orderings for window result columns, e.g. `row_number` is monotonic within the partition-key order, and full-partition aggregates are constants (`windows/mod.rs:375-580`). **read**

**Reusing an order (`get_window_mode` / `get_best_fitting_window`, `windows/mod.rs:592-736`):**
- `get_window_mode` treats the partition keys as constants and checks `[ordered partition-key permutation] + ORDER BY`, then the same with ORDER BY reversed. It returns `(should_reverse, Sorted | PartiallySorted(idx) | Linear)` (`:679-736`, loop at `:698-714`). (`can_skip_sort` is the old name of this logic.) **read**
- `get_best_fitting_window`:
  - Returns `None` for a bounded input in a non-`Sorted` mode (`:618-623`).
  - Reverses all expressions via `get_reverse_expr`, giving up if any can't be reversed (`:625-641`).
  - Picks `BoundedWindowAggExec` if all reversed expressions are bounded (`:643-652`).
  - Picks `WindowAggExec` only in `Sorted` mode (`:653-664`).
  - **read**
- Reversibility:
  - Aggregates: reversible if their UDAF is; the frame is mirrored, so `CURRENT ROW..UNBOUNDED FOLLOWING` becomes bounded and the reverse becomes unbounded (`window/aggregate.rs:187-216`).
  - `lead`↔`lag` and `first_value`↔`last_value`/`nth_value` are reversible (`lead_lag.rs:299-304`, `nth_value.rs:338-344`).
  - `row_number`/`rank`/`dense_rank` use the default `NotSupported` (`datafusion-expr-55.1.0/src/udwf.rs:416-418`).
  - **read**
  - So a reversed reuse can turn `WindowAggExec` into `BoundedWindowAggExec` or the other way round. **inferred**
- Where `EnsureRequirements` calls it:
  - In distribution enforcement before deciding repartitions (`enforce_distribution.rs:1181-1198`).
  - In `ensure_sorting`, when a sort sits under the window: `adjust_window_sort_removal` removes the linked sort and tries to fit again. If that fails, it adds the sort back and rebuilds a `Sorted` window (`.../enforce_sorting/mod.rs:459-460,542-627`).
  - **read**
- `PartialSortExec` (sort only the non-ordered suffix) is used only for unbounded inputs (`.../enforce_sorting/mod.rs:195-230`). **read**
- `main`: only a field-name assertion on reversal was added (`main:datafusion/physical-plan/src/windows/mod.rs:633-646`). Window exec semantics are unchanged. **read** (diff)

**For a backend that wants ordered windows on locus-ordered inputs:**
- `OVER (PARTITION BY contig ORDER BY position)` on an input sorted `(contig, position)` needs no sort if the hash repartition on `contig` keeps order. That needs `prefer_existing_sort=true`, single-partition input, or input already partitioned on `contig`. Windows get no streaming-benefit exception: `BoundedWindowAggExec` emission just follows its input (`bounded_window_agg_exec.rs:324`). **inferred**
- `PARTITION BY sample ORDER BY locus` over locus-only order is `Linear`. For bounded input, DataFusion therefore sorts by `(sample, locus)`, although `BoundedWindowAggExec` could run `Linear`.
- `ntile`/`cume_dist`/`percent_rank`, and frames ending `UNBOUNDED FOLLOWING`, always use the blocking `WindowAggExec`.

**Config-only vs needs own rule:**
- Config-only: reuse of a matching (or reversed) order happens automatically. Keeping it through the partition-key repartition needs `prefer_existing_sort=true`, `repartition_windows=false`, or `target_partitions=1`.
  - **inferred:** with `repartition_windows=false` and multi-partition input, the requirement becomes SinglePartition and the order is kept or restored through an SPM or a sort. Check with EXPLAIN.
- Needs own rule: running `BoundedWindowAggExec` in `Linear`/`PartiallySorted` mode on bounded inputs.
  - **inferred:** a custom-built `Linear` exec would survive `EnsureRequirements`. `get_best_fitting_window` returns `None`, so the plan is left as is, and a `Linear` requirement is only the ORDER BY keys.
- Needs own rule: avoiding hash repartition for locus-range-partitioned inputs whose partition keys are not exactly the range keys.

## Q4–Q5: How ordering propagates and how sort enforcement works (DataFusion 55.1)

Conventions. `X-55.1.0/src/...:N` is a path in the cargo registry copy of crate `datafusion-X` 55.1.0 (`core-55.1.0` is the `datafusion` facade crate). `main:...` is apache/datafusion at 0442090c. Each claim is tagged:
- **read**: I read it in the source.
- **ran**: I observed it in a scratch probe (`assert_probe`, see the top of this note).
- **inferred**: my reasoning from the code, not checked directly.

Probe setup. A one-partition `MemTable` (and a parquet table) declares the ordering `[contig, position, alleles]`. The probe takes the unoptimized physical plan and wraps it in an `AssertExec`, which requires `SinglePartition` and a hard ordering on the asserted keys. It then runs `PhysicalOptimizer::new().rules` one rule at a time and prints the plan after every rule that changes it. Source: [`df-ordering-probes/assert_probe.rs`](df-ordering-probes/assert_probe.rs). Output: `df-ordering-probes/assert_probe.out*.txt`.

**Correction to the question's framing.** 55.1 has no `EnforceDistribution` or `EnforceSorting` rule. A single rule, `EnsureRequirements`, replaces both. It runs these phases:
1. Join-key reorder.
2. `ensure_distribution`, bottom-up.
3. `ensure_sorting`, bottom-up.
4. `parallelize_sorts`.
5. `replace_with_order_preserving_variants`.
6. `pushdown_sorts`.
7. `replace_with_partial_sort`.

Citations: `physical-optimizer-55.1.0/src/ensure_requirements/mod.rs:18-61,176-248`; upstream epic apache/datafusion#21973, closed 2026-08-09 (**read**). The old function names survive as modules under `ensure_requirements/` (`physical-optimizer-55.1.0/src/lib.rs`: `pub use ensure_requirements::{enforce_distribution, enforce_sorting}`) (**read**).

Physical rule order in 55.1, all from `physical-optimizer-55.1.0/src/optimizer.rs` (**read**):

| Line | Rule |
|---|---|
| :94 | `OutputRequirements` (add mode) |
| :95 | `AggregateStatistics` |
| :101 | `JoinSelection` |
| :105 | `LimitedDistinctAggregation` |
| :111 | `FilterPushdown` |
| :117 | `WindowTopN` |
| :136 | `EnsureRequirements` |
| :138 | `CombinePartialFinalAggregate` |
| :140 | `OptimizeAggregateOrder` |
| :142 | `ProjectionPushdown` |
| :145 | `OutputRequirements` (remove mode) |
| :150 | `TopKAggregation` |
| :154 | `LimitPushPastWindows` |
| :158 | `HashJoinBuffering` |
| :162 | `LimitPushdown` |
| :167 | `TopKRepartition` |
| :174 | `ProjectionPushdown` |
| :176 | `PushdownSort` |
| :177 | `EnsureCooperative` |
| :181 | `FilterPushdown` (post-optimization) |
| :190 | `SanityCheckPlan` |

Main differs here:
- main adds `EliminateConstantJoin` before `JoinSelection` (`main:datafusion/physical-optimizer/src/optimizer.rs:207`).
- main documents a "physical optimizer contract": `EnsureRequirements`, `OutputRequirements` and `SanityCheckPlan` must keep their relative order (`main:.../optimizer.rs:86-177`).
- main's `EnsureRequirements` gains a Phase 0 that turns `InterleaveExec` back into `UnionExec` (`main:.../ensure_requirements/mod.rs:49-50,199-204`).

All **read**.

---

### 4. Ordering through projections, filters, unions, repartitions

**Short answer.**
- A projection keeps an ordering through plain columns and aliases. It also keeps it through an expression whose `ExprProperties.sort_properties` comes out `Ordered`.
- The *rest* of the sort key (suffix keys) survives only when that expression is also `strictly_order_preserving`.
- In 55.1 that holds for widening `CAST`s. It does not hold for:
  - `a + 1` with default settings, because of an overflow guard;
  - narrowing or Utf8 casts;
  - `concat`;
  - `get_field` applied to a struct-ordered column;
  - `floor` and similar functions, beyond the key itself.
- The reverse direction does work: `named_struct(...)` maps input columns to `get_field(out, 'f')`.
- Filters keep order, and `col = lit` makes `col` constant.
- The physical planner always produces `UnionExec`. `EnsureRequirements` swaps it for `InterleaveExec` only when every child has the same Hash or Range partitioning; `InterleaveExec` declares no ordering.
- With a single-partition source, every `RepartitionExec` keeps per-partition order, because its input has ≤1 partition. A `SortPreservingMergeExec` (SPM) then merges the partitions, so no re-sort is needed.

#### Projections

- **Where it happens.**
  - `ProjectionExec` computes its output equivalence via `input_eq_properties.project(projection_mapping, schema)` (`physical-plan-55.1.0/src/projection.rs:220-238`) (**read**).
  - It always reports `maintains_input_order = [true]` (`:343-346`) (**read**).
  - `EquivalenceProperties::project` (`physical-expr-55.1.0/src/equivalence/properties/mod.rs:1170-1185`) projects the equivalence group and calls `projected_orderings` (`:1067-1154`) (**read**).
- **Aliases and renames.** `ProjectionMapping::try_new` maps every source expression to `Column(alias, out_idx)` targets. The same source can have several targets (`physical-expr-55.1.0/src/projection.rs:1219-1250`) (**read**). Renames therefore keep the full ordering (**ran**: scenario D, `SELECT contig AS c, position AS p, alleles AS a` satisfied `[c,p,a]` with no sort).
- **Computed keys: two mechanisms.**
  1. *Dependency map.*
     - For each mapping source, `get_expr_properties(source, deps)` evaluates `PhysicalExpr::get_properties` bottom-up over the ordered sort keys it references (`mod.rs:1495-1550`).
     - If the result is `SortProperties::Ordered(opts)`, the target becomes an ordering key.
     - Its prefix is the projected prefix of the keys it depends on (`mod.rs:1083-1120`).
     - Dependency construction stops at the first sort key that cannot be projected (`mod.rs:969-1003`, the `break`).
     - Result: if `position` is replaced by `f(position)` and dropped, `alleles` (which depends on `position`) is lost (**read**).
  2. *Substitution.*
     - `substitute_oeq_class` swaps a sort key for a referring expression in place, keeping the suffix of the ordering.
     - It does this only if `properties.strictly_order_preserving && sort_properties == Ordered(same options)` (`mod.rs:209-224, 874-904`) (**read**).
     - The difference between monotone (`preserves_lex_ordering`) and strict (`strictly_order_preserving`) is documented at `expr-common-55.1.0/src/sort_properties.rs:135-198`, with the `floor` counterexample at `:186-197` (**read**).
- **What each common expression does in 55.1.**
  - `CAST`: `cast_expr_properties` keeps `Ordered` for numeric/bool→numeric, temporal→temporal and same-type casts. It sets `strictly_order_preserving` only for widening casts per `check_bigger_cast`, e.g. Int32→Int64/Float64 or Utf8→LargeUtf8 (`physical-expr-55.1.0/src/expressions/cast.rs:254-307`) (**read**). Probe results:
    - widening `CAST(position AS BIGINT)` kept the full `[contig, position, alleles]` (scenario C, **ran**);
    - narrowing `CAST(position AS SMALLINT)` added a `SortExec` because the `alleles` suffix was lost (C2, **ran**);
    - `CAST(position AS VARCHAR)` added a `SortExec` (C3, **ran**).
  - `a + 1`, `a - lit`:
    - `BinaryExpr::get_properties` combines the children's properties with `SortProperties::add`/`sub`.
    - `arithmetic_sort_properties` then returns `Unordered` unless `fail_on_overflow` is set or the result range is bounded (`physical-expr-55.1.0/src/expressions/binary.rs:128-154, 822-891`) (**read**).
    - The planner builds `BinaryExpr` with `fail_on_overflow: false` (`binary.rs:94`), and columns get an unbounded range (`mod.rs:1435-1436`, and `get_expr_properties` at `:1517-1525`, `expr-common-55.1.0/src/interval_arithmetic.rs:347-350, 415-417`) (**read**).
    - So `position + 1` is **Unordered**, and the probe got `CAST(position@1 AS Int64) + 1` plus an inserted `SortExec` (scenario B, **ran**).
    - `+`/`-` are never strict (`binary.rs:836, 849`) (**read**).
  - Comparisons `>`, `>=`, `<`, `<=` and `AND`/`OR` can be `Ordered` (`binary.rs:851-889`) (**read**).
  - `get_field` / struct field access:
    - `get_field` overrides none of `output_ordering`, `preserves_lex_ordering` or `strictly_order_preserving`. The only built-ins that do are listed under "Monotonic functions" below; `get_field` is not among them (`grep` over `functions-55.1.0`, **read**).
    - So `locus['contig']` over a source ordered by struct column `locus` is Unordered, and a `SortExec` was inserted (E4, **ran**).
    - The reverse works. `ProjectionMapping::try_new` asks `ScalarUDFImpl::struct_field_mapping`, implemented by `named_struct` (`functions-55.1.0/src/core/named_struct.rs:180`), and adds `p.x → get_field(out_col,'x')` entries (`physical-expr-55.1.0/src/projection.rs:1252-1305`) (**read**).
    - So a requirement written as `[locus['contig'], locus['position'], alleles]` is satisfied after `named_struct('contig',contig,'position',position) AS locus` (E, **ran**).
    - A requirement on the struct column itself (`[locus, alleles]`) is not satisfied and gets a sort (E2, **ran**).
    - Main also implements `struct_field_mapping` for `struct()` (`main:datafusion/functions/src/core/struct.rs:156`) (**read**).
  - `concat`: no ordering hooks, so Unordered, and a `SortExec` was inserted (F, **ran**).
- **Monotonic functions.**
  - `ScalarFunctionExpr::get_properties` reads `fun.output_ordering(children)`, `fun.preserves_lex_ordering` and `fun.strictly_order_preserving` (`physical-expr-55.1.0/src/scalar_function.rs:316-332`) (**read**).
  - Defaults on `ScalarUDFImpl` (`expr-55.1.0/src/udf.rs:965-999`) (**read**):
    - `output_ordering` returns `Unordered` unless `preserves_lex_ordering` is true. If it is true, it returns the shared ordering of all inputs.
    - `preserves_lex_ordering` defaults to `false`.
    - `strictly_order_preserving` defaults to `false`.
  - Built-ins that declare ordering in 55.1 (**read**):
    - the `make_math_unary_udf!` family via `math/monotonicity.rs`: acos, acosh, asin, asinh, atan, atanh, cbrt, cos, cosh, degrees, exp, ln, log2, log10, radians, sin, sinh, sqrt, tan, tanh. Each declares only `output_ordering` (`functions-55.1.0/src/macros.rs:282,428`; `math/mod.rs:57-260`).
    - `floor` (`math/floor.rs:233`), `ceil` (`math/ceil.rs:193`), `round`, `trunc`, `abs`, `signum`, `log`, `pi`.
    - `date_trunc` (`datetime/date_trunc.rs:443`), `date_bin` (`date_bin.rs:270`).
    - `from_unixtime`, the only one that also overrides `preserves_lex_ordering`/`strictly_order_preserving` (`datetime/from_unixtime.rs:151-162`).
  - Monotone-but-not-strict functions keep only `[prefix…, f(key)]`. Example: `floor(position)` lost the `alleles` suffix and a sort was added (F2, **ran**).
  - **A user UDF** declares ordering as follows:
    - override `ScalarUDFImpl::output_ordering`, or override `preserves_lex_ordering` and rely on the default `output_ordering`;
    - to keep the keys after it, also override `strictly_order_preserving` to `true`, and only when the function is genuinely one-to-one and order-preserving (`udf.rs:965-999`) (**read**).
  - Main adds ordering hooks to `to_unixtime`, the `to_timestamp*` family and `factorial`, and adds `strictly_order_preserving` to `floor` (`main:datafusion/functions/src/...`, grep) (**read**).
  - Main tightens `SortProperties::add/sub/gt_or_gteq` so that mismatched `nulls_first` gives Unordered, and splits `and_or` into `and`/`or` (`main:datafusion/expr-common/src/sort_properties.rs`) (**read**).
- **Sort pushdown through projections** handles only plain-column projections. `ProjectionExec::try_pushdown_sort` returns `Unsupported` if a sort key maps to a computed expression (`physical-plan-55.1.0/src/projection.rs:521-585`) (**read**).

#### Filters

- `FilterExec` has `maintains_input_order = [true]` (`physical-plan-55.1.0/src/filter.rs:550-553`) (**read**). Ordering kept (G, **ran**).
- `compute_properties` (`filter.rs:419-479`) (**read**):
  - adds `col = col` equalities with `add_equal_conditions`;
  - adds singleton-statistics columns as constants;
  - adds `ConstExpr::collect_predicate_constants`, so `col = lit` makes `col` a uniform constant (`physical-expr-55.1.0/src/utils/mod.rs:62-110`).
- Constants satisfy requirements trivially. `add_sort_above` also strips constant keys before building a sort (`physical-optimizer-55.1.0/src/utils.rs:44-50`) (**read**).
- `WHERE contig = '1'` followed by dropping `contig` still satisfies `[position, alleles]` (G2, **ran**).

#### `UnionExec` vs `InterleaveExec`

- **Planner.** `LogicalPlan::Union` always becomes `UnionExec::try_new` (`core-55.1.0/src/physical_planner.rs:1785`). A union with one input collapses to that input (`physical-plan-55.1.0/src/union.rs:189-215`) (**read**).
- **When `InterleaveExec` appears.** Only in `ensure_distribution`, when all three hold (`physical-optimizer-55.1.0/src/ensure_requirements/enforce_distribution.rs:1498-1525`) (**read**):
  - the node is a `UnionExec`;
  - `!prefer_existing_union` (default `false`, `common-55.1.0/src/config.rs:1734`);
  - `can_interleave`: all children have an identical `Partitioning::Hash` or `Partitioning::Range` (`union.rs:899-911`).
- **Orderings and partitioning.**
  - `UnionExec` passes partitions through, giving `UnknownPartitioning(sum of input partitions)`. Its equivalence is `calculate_union`, i.e. the common-prefix "meet" of the children's orderings plus constants shared by both sides (`union.rs:234-257`; `physical-expr-55.1.0/src/equivalence/properties/union.rs:38-107`) (**read**).
  - `UnionExec::maintains_input_order` is `true` only for children whose ordering length equals the output ordering's length (`union.rs:293-316`) (**read**). A child with a *longer* ordering is reported `false` even though its rows are not reordered. This matters for the check in Q5.
  - `InterleaveExec` takes the first input's partitioning, has empty `EquivalenceProperties::new(schema)` (no ordering), and `maintains_input_order = false` (`union.rs:680-727`) (**read**).
- **Probe result.** A union of two ordered one-partition tables becomes `SortPreservingMergeExec ← UnionExec`, with no `SortExec`, at both `target_partitions` 4 and 1 (H/H2, **ran**).
- **Main changes.**
  - `InterleaveExec` has `try_new_unchecked` and a `check_invariants(Executable)` that asserts `can_interleave`.
  - `EnsureRequirements` Phase 0 normalizes Interleave back to Union before re-deriving it (`main:datafusion/physical-plan/src/union.rs`; `main:.../ensure_requirements/mod.rs:199-204`) (**read**).

#### `SortPreservingMergeExec` (SPM)

- **What it declares.** It requires `UnspecifiedDistribution` plus the merge ordering on its input. It maintains order, outputs `UnknownPartitioning(1)`, and adds its ordering to the input's equivalence (`physical-plan-55.1.0/src/sorts/sort_preserving_merge.rs:162-187, 265-281`) (**read**).
- **Main:** SPM first clears all other input orderings when `input_partitions > 1` (`main:datafusion/physical-plan/src/sorts/sort_preserving_merge.rs`, `if input_partitions > 1 { eq_properties.clear_orderings(); }`) (**read**).
- **Where SPMs are inserted:**
  - `add_merge_on_top`: when a `SinglePartition` requirement sits over N>1 partitions and the input has an `output_ordering`, it inserts an SPM, otherwise a `CoalescePartitionsExec`. It carries any `fetch` from removed SPM/Coalesce nodes (`enforce_distribution.rs:785-817, 845-871, 1333-1335`) (**read**).
  - `parallelize_sorts` (if `repartition_sorts`, default true, `config.rs:1619`): turns `Sort ← … ← Coalesce` into `SPM(fetch) ← Sort(preserve_partitioning)` (`enforce_sorting/mod.rs:319-381`; gated at `ensure_requirements/mod.rs:216-221`) (**read**).
  - `replace_with_order_preserving_variants`: replaces a `CoalescePartitionsExec` under a `SortExec` with an SPM when that makes the sort redundant (`replace_with_order_preserving_variants.rs:137-165`) (**read**).
  - `remove_corresponding_sort_from_sub_plan`: re-adds an SPM (or Coalesce) when removing a sort would break a `SinglePartition` requirement (`enforce_sorting/mod.rs:786-803`) (**read**).
  - `add_sort_above_with_distribution` in sort pushdown (`utils.rs:65-111`) (**read**).
  - `PushdownSort` patterns (`pushdown_sort.rs:102-152, 197-210`) (**read**).
- **Where SPMs are removed.** `ensure_sorting` drops an SPM whose input has one partition, keeping any `fetch` as a `LocalLimitExec` (`enforce_sorting/mod.rs:461-477`) (**read**).
- **`fetch` handling.**
  - `analyze_immediate_sort_removal` turns a redundant `SortExec(fetch)` into an SPM with that fetch (multi-partition) or into a Global/Local limit (`enforce_sorting/mod.rs:487-538`) (**read**).
  - Main changed the fetch handling around coalesce/sort linkage, so that sorts and coalesces under a fetch are not removed (`main:.../enforce_sorting/mod.rs`, diff hunks at :105-107, :174-177, :435-452) (**read**).

#### `RepartitionExec`, `CoalescePartitionsExec`, `CoalesceBatchesExec`

- **When `RepartitionExec` keeps order.**
  - `maintains_input_order = preserve_order || input partitions <= 1`. If that is false, it clears the orderings. With >1 input partitions it also clears per-partition constants (`physical-plan-55.1.0/src/repartition/mod.rs:2039-2062`) (**read**).
  - `with_preserve_order()` is a no-op unless the input is ordered *and* has >1 partition (`repartition/mod.rs:2088-2098`) (**read**).
  - Display prints `maintains_sort_order=true` for the 1-input-partition case and `preserve_order=true` for the merging variant (`repartition/mod.rs:1505-1513`) (**read**).
- **Consequence for a one-partition ordered source.** Any round-robin or hash repartition directly above it yields N partitions that are each still sorted. A `SinglePartition` + ordering requirement above is then met by an SPM, not a sort. Observed:
  - `SPM ← Filter ← RepartitionExec(RoundRobinBatch(4), input_partitions=1, maintains_sort_order=true) ← DataSourceExec` with `target_partitions=4` and `prefer_existing_sort` either false or true (G3/G4, **ran**);
  - `RepartitionExec(Hash(...),4), input_partitions=1, maintains_sort_order=true` under a sorted aggregate (I, **ran**).
- **How `EnsureRequirements` uses order-preserving repartitions.**
  - It always inserts round-robin and hash repartitions with `.with_preserve_order()` first (`enforce_distribution.rs:689-692, 1377-1385, 1101-1107`) (**read**).
  - It reverts them through `replace_order_preserving_variants` (SPM→Coalesce, preserving repartition→plain) in these cases (**read**):
    - an ordering requirement exists, and either it is not satisfied or `order_preserving_variants_desirable` is false. That flag is `prefer_existing_sort || (unbounded && incremental)` (`:1157-1167, 1433-1460`); when the ordering *was* satisfied, a `SortExec` is added back with `add_sort_above_with_check`;
    - there is no ordering requirement and the parent doesn't maintain order (`:1463-1487`);
    - either case is skipped when keeping the ordered variant avoids a pipeline-breaking parent: `preserving_order_enables_streaming` (`:750-779, 1426-1430`).
  - Later, Phase 3b `replace_with_order_preserving_variants` is called with `is_spr_better=false, is_spm_better=true` (`ensure_requirements/mod.rs:227-233`) (**read**). At each `SortExec` it builds an alternative using SPM instead of Coalesce. It also uses preserving repartitions, but only if `prefer_existing_sort` is set or the plan is unbounded and blocking (`replace_with_order_preserving_variants.rs:251-299`, esp. :272-282). It keeps the alternative if that makes the sort redundant (:285-298) (**read**).
  - Net effect: `prefer_existing_sort` (default false, `config.rs:1655`) only decides whether a *multi-input* `RepartitionExec(preserve_order=true)` may be kept in place of a re-sort. SPM-for-Coalesce is always preferred (**inferred** from the flags above).
  - Observed with 50k rows and `target_partitions=4`: `RR(4) ← src` (1→4, maintains), then `Hash(4), input_partitions=4, preserve_order=true` under a `FinalPartitioned` aggregate in `ordering_mode=Sorted`, then SPM, and no `SortExec`. This held with `prefer_existing_sort` both false and true (I3/I4, **ran**). The preserving hash repartition probably survives because the sorted aggregate above it maintains order and benefits from streaming (`:1426-1430, 1470-1476`) (**inferred**).
- **Repartition at the source.** `repartition_file_scans` (default true, `config.rs:1584`) can call `child.plan.repartitioned(target_partitions, config)` on a source whenever row-count stats make round-robin beneficial (`enforce_distribution.rs:1323-1329`) (**read**). This is gated on statistics only, not on the parent's `benefits_from_input_partitioning`. For a `FileScanConfig` it passes the output ordering (`datasource-55.1.0/src/file_scan_config/mod.rs:799-819`) (**read**).
  - Observed: a two-file parquet table at `target_partitions=4` became two one-file groups, and the result was an SPM, not a sort (J2, **ran**).
- **`CoalescePartitionsExec`** always clears orderings and per-partition constants, even with one input partition. It outputs `UnknownPartitioning(1)` and uses the default `maintains_input_order=false` (`physical-plan-55.1.0/src/coalesce_partitions.rs:85-108`) (**read**).
- **`CoalesceBatchesExec`** still exists in 55.1 but is `#[deprecated(since="52.0.0")]` (`coalesce_batches.rs:63-66`). It maintains order (`:169-171`). No production optimizer rule inserts it: the only `CoalesceBatchesExec::new` in the optimizer crate is in `topk_repartition.rs` tests (`:320`, after `#[cfg(test)]` at `:180`) (**read**).
- **Possible bug, rare corner case** (**read**, impact **inferred**): `RepartitionExec::try_swapping_with_projection` rebuilds with `RepartitionExec::try_new(...)` and drops `preserve_order` (`repartition/mod.rs:1825-1886`, :1883; same in main). If `ProjectionPushdown` swapped a narrowing column-only projection under a merging `preserve_order=true` repartition, the ordering would be lost. `SanityCheckPlan` should then reject the plan. This cannot happen for one-input-partition repartitions, which maintain order anyway.

**Config-only vs needs own rule (Q4)**
- Config-only:
  - `prefer_existing_sort=true` to keep multi-input order-preserving repartitions;
  - `prefer_existing_union=true` to never interleave;
  - `repartition_sorts`, `enable_round_robin_repartition`, `repartition_file_scans`, `target_partitions=1`.

  With a one-partition source none of these is needed to avoid sorts (G3/G4/I/H, **ran**).
- Needs code:
  - a key computed by `+`, `concat`, `get_field`, a narrowing cast or `floor` must be rewritten or must not be part of the asserted key. The other options are a user UDF with `output_ordering` + `strictly_order_preserving`, or asserting on `get_field(struct,'f')` exprs produced by `named_struct`;
  - nothing in config makes `a + 1` ordered.

---

### 5. Sort elimination, and detecting a requirement met by a sort

**Short answer.**
- Sorts are added only by `EnsureRequirements`, plus a few later rules that add TopK sorts.
- Sorts are removed by `EnsureRequirements` and by `PushdownSort`.
- **`PushdownSort` can delete an `EnsureRequirements`-inserted `SortExec`** by reordering the files within a file group by min/max statistics. The final plan then contains no sort node, although the declared ordering was not trusted at first (J, **ran**).
- The most robust check is therefore made in two places:
  1. a custom rule placed **immediately after `EnsureRequirements`** (or a run with `enable_sort_pushdown=false`);
  2. a final post-optimization check, best implemented as `check_invariants(InvariantLevel::Executable)` on the assertion node itself.

  Both walk the order-carrying path below the assertion and fail on any sorting node.

#### What each pass does (55.1)

- **`OutputRequirements` (add/remove).**
  - The add pass wraps the root in `OutputRequirementExec`, an ancillary node that carries the global ORDER BY/fetch requirement. It finds the top `SortExec`/SPM through order-maintaining, non-requiring single-child nodes (`physical-optimizer-55.1.0/src/output_requirements.rs:105-112, 390-496`) (**read**).
  - The remove pass, after `EnsureRequirements`, strips it again (`:355-371`) (**read**).
- **`ensure_distribution` (Phase 2a).** Adds repartitions and merges as described in Q4.
- **`ensure_sorting` (Phase 2b, bottom-up)** (`enforce_sorting/mod.rs:404-479`) (**read**):
  1. `analyze_immediate_sort_removal`: removes a `SortExec` whose input already satisfies its ordering, or replaces it with SPM/limit when `fetch`/partitioning require that (`:487-538`).
  2. For each child with a `required_input_ordering`: if `ordering_satisfy_requirement` fails, it removes linked downstream sorts and adds a `SortExec` with `add_sort_above` (`:430-446`).
     - `add_sort_above` drops constant keys and sets `preserve_partitioning=true` when the input has >1 partitions (`utils.rs:38-59`).
     - It uses only the **first** alternative of `OrderingRequirements` (`into_single`, `physical-expr-common-55.1.0/src/sort_expr.rs:862-868`).
  3. For a child without a requirement whose parent does not maintain order: removes the sort that has become pointless (`:447-451`).
  4. Window adjustments (`:459-460`).
  5. Removes an SPM over a single partition (`:461-477`).

  Sorts with a `fetch` (TopK) are never removed this way (`:747-750`).
- **`parallelize_sorts` (3a)**: Coalesce→Sort cascades become Sort(per-partition)→SPM (`:319-381`). It may re-add a `SortExec` through `add_sort_above_with_check` (`:349`) (**read**).
- **`replace_with_order_preserving_variants` (3b)**: described in Q4. It removes a `SortExec` when switching to order-preserving variants makes the sort redundant (`replace_with_order_preserving_variants.rs:251-299`) (**read**).
- **`pushdown_sorts` (3c)** (`enforce_sorting/sort_pushdown.rs:81-322`) (**read**):
  - moves existing sorts *down* through these nodes:
    - projections (column-only remap, `:540-587`);
    - `UnionExec`, by pushing the requirement into every branch (`:456`);
    - SPM, joins, aggregates, windows;
    - custom nodes via `handle_custom_pushdown` (`:858`);
  - stops at `RepartitionExec`, `FilterExec`, leaves and order-destroying nodes (`:498-507`);
  - removes sorts that become redundant (`:188-250`) and re-adds a sort where pushdown stops (`:309-317`).
  - Consequence: the sort that satisfies the assertion **need not be its direct child**. It can sit below projections or inside each union branch (**inferred**).
- **`replace_with_partial_sort` (3d)**: turns `SortExec` into `PartialSortExec` only when the sort's input is **unbounded** and shares a prefix with it (`enforce_sorting/mod.rs:195-231`, check at :204) (**read**).
- **Later rules that create sorting nodes** (**read**):
  - `LimitPushdown` folds limits into `SortExec.fetch`, i.e. TopK (`limit_pushdown.rs:239-293`; `SortExec::with_fetch`, `physical-plan-55.1.0/src/sorts/sort.rs:1057-1082`);
  - `TopKRepartition` inserts a copy of `SortExec(fetch)` below a hash repartition (`topk_repartition.rs:17-44`, :143, :161);
  - `LimitPushPastWindows` (`limit_pushdown_past_window.rs:301`);
  - `TopKAggregation` (`topk_aggregation.rs:168`);
  - `WindowTopN`, which runs *before* `EnsureRequirements`, creates `PartitionedTopKExec` (`window_topn.rs:186`);
  - `PushdownSort` re-creates `SortExec` (and SPM) on `Inexact` (`pushdown_sort.rs:129-147, 181-213`).
- **`PushdownSort`** (`pushdown_sort.rs:84-222`; on unless `enable_sort_pushdown=false`, `:90`, `config.rs:1754`) (**read**):
  - For every `SortExec`, and for the SPM→`SortExec(preserve_partitioning)` pattern, it calls `input.try_pushdown_sort(sort.expr())`.
  - **`Exact`**: removes the `SortExec`. A `fetch` becomes `with_fetch` or a Global/Local limit. In the SPM pattern a `BufferExec` is inserted (`:109-128, 165-180`).
  - **`Inexact`**: keeps a `SortExec` over the rewritten input.
  - **`Unsupported`**: no change.
  - Nodes that forward the call (**read**):
    - `ProjectionExec` (column-only, `projection.rs:521-585`);
    - `RepartitionExec`, only if it maintains order (`repartition/mod.rs:1907-1926`);
    - `CoalescePartitionsExec`, downgraded to `Inexact` with >1 input partitions (`coalesce_partitions.rs:345-375`);
    - `CoalesceBatchesExec` (:306), `BufferExec` (`buffer.rs:318`), `CooperativeExec` (`coop.rs:376`);
    - `DataSourceExec`, which delegates to its `DataSource` (`datasource-55.1.0/src/source.rs:575-585`).
  - The default is `Unsupported` (`execution_plan.rs:991-996`). Result enum: `physical-plan-55.1.0/src/sort_pushdown.rs:30-60`.
  - The config doc string still says pushdown "Returns **inexact ordering**" and calls exact sort elimination "Future" (`config.rs:1741-1753`). The code does eliminate sorts. The doc is stale (**read**).
- **Can a scan absorb a sort?** Yes, but no scan sorts rows.
  - `FileScanConfig::try_pushdown_sort` asks the `FileSource` first (`file_scan_config/mod.rs:1048-1145`) (**read**):
    - **`Exact`** (the natural ordering satisfies the request): it rebuilds the scan with files sorted by statistics and keeps `Exact` only if the groups do not overlap (`:1096-1110`).
    - **`Inexact`**: it may upgrade to `Exact` after reordering files, dropping the row-group reorder hints (`:1111-1144`).
    - **`Unsupported`**: it calls `try_sort_file_groups_by_statistics`, which sorts the files *within* each group by min/max stats. It returns `Exact` only if the groups do not overlap, no file has NULLs in the sort columns, and the new `eq_properties` satisfy the order (`file_scan_config/sort_pushdown.rs:234-305`).
  - `ParquetSource::try_pushdown_sort` returns `Exact` when the declared ordering already satisfies the request. Otherwise it returns `Inexact`, setting runtime row-group reorder or reverse hints and *keeping* the `SortExec` (`datasource-parquet-55.1.0/src/source.rs:931-1010`) (**read**).
  - So rows are never sorted inside the scan. Files are re-read in a different order, and row groups too in `Inexact` mode. Epics: apache/datafusion#17348 and #21956, both closed (**read**).
  - Observed (J, **ran**):
    - two parquet files listed `a` (positions 100-199) then `b` (0-99), `file_sort_order` declared, `target_partitions=1`;
    - the scan's ordering was dropped up front (no `output_ordering` in the initial plan), and `EnsureRequirements` inserted `SortExec`;
    - **`PushdownSort` then removed it** and rewrote `file_groups=[[b, a]]` with `output_ordering=[contig, position, alleles]`.
  - The same run with `enable_sort_pushdown=false` kept the `SortExec` (L, **ran**).
- **Relevance to this repo** (**read**, impact **inferred**): `OrderedSource::try_pushdown_sort` returns `Exact` when its declared ordering satisfies the request. Otherwise it delegates to the inner `FileScanConfig`, and the result is *not* re-wrapped in `OrderedSource` (`src/stored/sorted_table/ordered_source.rs:172-186`). So a requested ordering other than the declared one can still be met by file reordering, which also drops the wrapper.
- **`SanityCheckPlan`**, the last rule (**read**):
  - for each node, checks `ordering_satisfy_requirement(sort_req.into_single())` and distribution satisfaction for every child;
  - calls `plan.check_invariants(InvariantLevel::Executable)` (`sanity_checker.rs:143-192`, :189).
  - It only checks that requirements are met. It does not care *how*: a `SortExec` passes.
- **Planner hook.** After all rules, `DefaultPhysicalPlanner::optimize_physical_plan` runs `InvariantChecker(InvariantLevel::Executable)` over every node, in release builds too (`core-55.1.0/src/physical_planner.rs:2855-2904, 3253-3277`; main `:3352`) (**read**).
  - `ExecutionPlan::check_invariants` is overridable (`physical-plan-55.1.0/src/execution_plan.rs:168`) (**read**).
  - The probe's `AssertExec::check_invariants` was called once by `SanityCheckPlan` and once by a planner-style walk (all scenarios, **ran**).

#### Which nodes can satisfy an ordering requirement by actually sorting

- **Sorts rows** (**read**; each re-derives ordering with `eq_properties.reorder` and leaves `maintains_input_order` at its default `false`):
  - `SortExec` (`sorts/sort.rs:1143-1193`, :1180). Name is `"SortExec"`, or `"SortExec(TopK)"` with `fetch` (`:1253-1258`); `preserve_partitioning=true` sorts each partition.
  - `PartialSortExec` (`sorts/partial_sort.rs:285-293`).
  - `PartitionedTopKExec` (`sorts/partitioned_topk.rs:283-284, 362-364`).
- **Merges only, needs sorted input:** `SortPreservingMergeExec`. It has a required ordering on its input and `maintains_input_order=[true]` (`sort_preserving_merge.rs:275-281`) (**read**).
- **Does not sort, keeps or derives ordering:**
  - aggregates: `maintains_input_order = input_order_mode != Linear` (`aggregates/mod.rs:2027-2029`);
  - windows (`bounded_window_agg_exec.rs:462`, `window_agg_exec.rs:233`). They add orderings for *new* columns such as `row_number` (`windows/mod.rs:465-569`) but do not reorder rows;
  - joins maintain one side:
    - `HashJoinExec`: probe side for Inner/Right* (`hash_join/exec.rs:1011-1023`);
    - `SortMergeJoinExec`: left for Inner/Left*, right for Right* (`sort_merge_join/exec.rs:228-240`);
    - an SMJ's own required input orderings can cause sorts under it, which are on a different requirement's behalf (**read**).
- **TopK.**
  - It appears when a LIMIT folds into a sort.
  - An unrelated TopK below the assertion does not stop the asserted keys from needing a sort: `ORDER BY v LIMIT 10` under the assertion gave `SortExec[contig,…] ← SortExec(TopK)[v]` (K, **ran**).

#### Is a check that walks the final plan below the assertion sound?

It is sound against `SortExec`, `PartialSortExec` and `PartitionedTopKExec` if the walk meets these conditions (**inferred**):
- It starts at the assertion's child.
- It descends into child *i* iff `maintains_input_order()[i]`, **or** the node is `UnionExec` (descend into all children, because `UnionExec` reports `false` for children with a longer ordering, `union.rs:293-316`), **or** the node is `SortPreservingMergeExec`.
- It fails on any node whose `name()` starts with `"SortExec"`, or on `PartialSortExec` or `PartitionedTopKExec`, matched by type via `downcast_ref` or by name.
- It accepts leaves.
- It treats any other node that has `maintains_input_order` all-false yet whose `equivalence_properties()` satisfy the requirement as "unknown establisher", and fails closed.

Why this is sound:
- Every built-in node that keeps rows in order reports `maintains_input_order=true`.
- Every built-in node that sorts reports `false` and is listed above.
- `CoalescePartitionsExec`, `InterleaveExec` and a merging non-preserving `RepartitionExec` clear orderings (Q4).
- So the asserted ordering can only reach the assertion through `maintains_input_order` edges, through `UnionExec`, or through SPM.

It is **not** sound against two things:
1. **Sort absorption by `PushdownSort`.** The `SortExec` disappears and the scan's file order is rewritten (J, **ran**). Two ways to catch it:
   - run a check rule between `EnsureRequirements` and `PushdownSort`. Inserting the rule directly after `EnsureRequirements` is simplest, and it sees every sort that `EnsureRequirements` inserted for the assertion;
   - or set `enable_sort_pushdown=false`.

   A leaf-level alternative: compare the source's `output_ordering`/`file_groups` with the initial plan.
2. **Policy false positives.** A user `ORDER BY` on the same keys below the assertion, or an SMJ-required sort on the maintained side, will be flagged. That may be the desired behaviour, since the ordering was established by a sort.

Point the check at these properties (**inferred** recommendation):
- `ExecutionPlan::name()`/type, `maintains_input_order()`, and `children()`;
- `equivalence_properties().ordering_satisfy_requirement(req)` at each step, to stop descending once a subtree no longer satisfies the requirement;
- `output_partitioning().partition_count()`, only to explain SPMs.

`benefits_from_input_partitioning` is irrelevant to the check. It only steers round-robin insertion: set it to `false` on the assertion, as `DataSinkExec` does (`datasource-55.1.0/src/sink.rs:271-302`).

Mirror `DataSinkExec` for the node itself (**read**):
- `required_input_ordering = Some(OrderingRequirements::Hard(...))`, using a single alternative, because enforcement uses only the first (`into_single`);
- `input_distribution_requirements = SinglePartition`;
- `maintains_input_order = [true]`.

Without `SinglePartition`, a multi-partition input would get `SortExec(preserve_partitioning=true)` per partition (`utils.rs:55-57`) (**read**).

**Where to hook** (**read** for the APIs, **inferred** for the advice):
- `SessionStateBuilder::with_physical_optimizer_rule` appends after `SanityCheckPlan`. `with_physical_optimizer_rules` replaces the whole list (`core-55.1.0/src/execution/session_state.rs:1331-1350`).
- To check directly after `EnsureRequirements`, rebuild the default list from `PhysicalOptimizer::new().rules`, which is `pub` (`optimizer.rs:73`), and insert the rule after the element named `"EnsureRequirements"`.
- Put the final check in `AssertExec::check_invariants(Executable)`. It runs after every rule (via `SanityCheckPlan` and the planner's final `InvariantChecker`) without touching the rule list.
- Main keeps both hooks (`main:.../core/src/physical_planner.rs:3352`).

**Config-only vs needs own rule (Q5)**
- Config-only:
  - `enable_sort_pushdown=false` stops sorts from vanishing into reordered scans;
  - `target_partitions=1` / `repartition_sorts` / `prefer_existing_sort` change plan shape but cannot *fail* a plan.

  No setting makes DataFusion reject a plan because a sort was inserted, since `SanityCheckPlan` accepts sorts.
- Needs own code:
  - the assertion node with a hard requirement and `SinglePartition`;
  - a check walking `maintains_input_order` / `UnionExec` / SPM edges that fails on `SortExec*`, `PartialSortExec` or `PartitionedTopKExec`, run (a) as a rule inserted right after `EnsureRequirements` and (b) in `check_invariants(Executable)` or as a rule appended at the end, to catch the TopK and sort-creating rules that run later.

## Q6. Known gaps: where DataFusion drops a declared ordering or does not use it

Sources: apache/datafusion issues and PRs, read on 2026-10-26 through the unauthenticated GitHub API. Source citations point to our pin, `datafusion-*-55.1.0` (paths relative to `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`), or to the `main` snapshot at `0442090c` (written `main:` below, relative to `datafusion/`).
Tags: **[read]** means I read the issue, PR, or source myself. **[inferred]** means a conclusion I drew from what I read but did not test.

### Summary: the gaps that matter most for locus-ordered data

1. **No operator in 55.1 or `main` uses sortedness for an interval-overlap join keyed on contig.** Take `l.contig = r.contig AND l.start <= r.end AND r.start <= l.end`. Because it has an equi-key, the planner skips the piecewise-merge-join (PWMJ) branch entirely. PWMJ runs only when there are no equi-keys and exactly one range predicate. The query becomes a `HashJoinExec`, or a `SortMergeJoinExec` when `prefer_hash_join=false`, with both inequalities applied as a residual filter. Without the equality it becomes `NestedLoopJoinExec`. The only open fix is IEJoin (#23741, a stale PR, disabled by default, inner joins only). The point-in-interval operator (#21859) was closed without merging. **[read]**
2. **PWMJ itself is experimental and off by default** (`enable_piecewise_merge_join=false`). It always buffers one side into a single partition and re-sorts every streamed batch. For classic joins it declares no output ordering, so sorted inputs gain nothing beyond the buffered-side sort it requires. On 55.1 it also reports a false equivalence between the two sides of its range predicate, which can produce wrongly ordered `ORDER BY` results (#24360, fixed on `main` only). **[read]**
3. **ASOF JOIN exists on `main` (v56), not in 55.1.** It is reached through `ASOF JOIN … MATCH_CONDITION(…)` SQL or `DataFrame::join_asof`. It requires both inputs sorted by `(eq keys…, match key)`, keeps left order, and collects the whole right side into a single partition that cannot spill. Partitioned mode is an open PR (#26592). It fits "nearest/preceding interval start per locus" lookups. It is not an overlap join. **[read]**
4. **Multi-file scans lose `(contig, position, …)` orderings to the min/max check.** `MinMaxStatistics` builds each file's min row and max row from each sort column's min and max taken separately. A file that crosses a contig boundary therefore looks like it overlaps the next file. The whole lexicographic ordering is then dropped, and no shorter valid prefix such as `(contig)` is kept. Separately, `ListingTable` throws away statistics-based groups when there are more of them than `target_partitions` (#16919, closed on 2026-10-25 as a deliberate trade-off). Related open issues: #6672 (epic), #10336, #22198 (statistics reorder looks only at the first sort key), and #19724. **[read]**
5. **Sort-merge join (SMJ) cannot be chosen by ordering.** The planner hardcodes `ASC NULLS FIRST` for every key and plans SMJ only when `target_partitions>1 && repartition_joins && !prefer_hash_join`. With `target_partitions=1` the planner always produces a hash join. No optimizer rule picks SMJ because the inputs are already sorted. Issue #26260 and PR #26261 are open. SMJ also needs `KeyPartitioned` inputs, so with several partitions it hash-repartitions and then re-sorts unless an order-preserving repartition is used. For Full joins SMJ declares no output order. **[read]**
6. **Aggregation:** the ordered (streaming) aggregate is always chosen when the input is sorted, with no cost check. With `target_partitions>1`, the hash repartition between the partial and final stages forces a re-sort (#23338, open). A "grouped/clustered" input property, which would let concatenated sorted runs stream, is half-landed on `main` (#24697 merged; #24698 and #24497 open). **[read]**

---

### 1. Range / interval joins

**What the source shows**

- **PWMJ planner gate, 55.1** `datafusion-55.1.0/src/physical_planner.rs:1573-1691` **[read]**
  - It is taken only when `join_on.is_empty()`. Line 1573 reads `// TODO: Allow PWMJ to deal with residual equijoin conditions`.
  - It also needs `num_range_filters == 1 && total_filters == 1`, a join type that is not Semi/Anti/Mark (lines 1578-1593), and `enable_piecewise_merge_join`.
  - Each `<, <=, >, >=` conjunct counts as one range filter. `BETWEEN` is not handled (`TODO … IEJoins`, line 1485).
  - Everything else falls back to `NestedLoopJoinExec` (line 1693).
  - The config is `datafusion-common-55.1.0/src/config.rs:1688`: `enable_piecewise_merge_join: bool, default = false`, documented as "currently experimental".
- **PWMJ planner gate, `main`** `main:core/src/physical_planner.rs:1622-1737` **[read]**
  - The equi-key and single-predicate limits are unchanged (TODO at line 1622, single predicate at line 1627).
  - The join-type exclusion is gone: Semi, Anti and Mark are now supported, through merged PRs #23870, #24457 and #25467.
  - The default is still `false` (`main:common/src/config.rs:1851`).
- **`PiecewiseMergeJoinExec`, 55.1** `datafusion-physical-plan-55.1.0/src/joins/piecewise_merge_join/exec.rs` **[read]**
  - `try_new` returns `not_impl_err!("Existence Joins are currently not supported…")` (lines 300-304).
  - Distribution: `[SinglePartition, UnspecifiedDistribution]` (lines 495-503), so the buffered (left) side is coalesced to one partition.
  - Ordering: only the buffered side carries a requirement (lines 506-519). It is DESC for `<`/`<=` and ASC for `>`/`>=`, with nulls first (lines 308-330). The streamed side is "sorted per batch in memory" (doc, lines 96-100).
  - `maintains_input_order` returns `[false,false]` for every join type, marked `// TODO: Add input order` (lines 449-466).
  - `compute_properties` passes the range `on` pair to `join_equivalence_properties` (lines 421-447). This is the false-equivalence bug #24360.
  - `swap_inputs` is `todo!()`.
- **`PiecewiseMergeJoinExec`, `main`** `main:physical-plan/src/joins/piecewise_merge_join/exec.rs` **[read]**
  - Right-existence joins now declare `[false, true]`, keeping streamed order (lines 474-496). Classic joins still declare `[false,false]`.
  - The buffered side is `SinglePartition`, except for right-existence joins (lines 557-575).
  - The equivalence fix passes `&[]` (lines 439-469).
  - `swap_inputs` is still `todo!()` (line 499).
- **`NestedLoopJoinExec`** declares `maintains_input_order` as `vec![false, false]` for every join type, both in 55.1 (`joins/nested_loop_join.rs:427-429`) and on `main` (`:454-456`). Its left/build side must be `SinglePartition` (55.1 `:547-555`). So even where NLJ happens to emit in probe order, the planner never relies on it. **[read]**
- **`HashJoinExec`, the realistic plan for `contig = AND range-filter`**, declares probe (right) order kept for Inner, Right, RightSemi, RightAnti and RightMark. It never declares build order (55.1 `joins/hash_join/exec.rs:1011-1022`). **[read]**
  - In `Partitioned` mode, the hash repartition on `contig` destroys locus order. **[inferred]**
- **`AsOfJoinExec`, `main` only** (`main:physical-plan/src/joins/asof_join.rs`; it does not exist in 55.1's `joins/` directory) **[read]**
  - Semantics are Snowflake-style. Each left row gets the closest right row in its eq-key group that satisfies one ordered comparison (module doc, lines 18-66).
  - Distribution: `[Unspecified, SinglePartition]` (lines 379-389).
  - Both inputs must be sorted by eq keys and then the match key. The match key is ASC for `>=`/`>` and DESC for `<=`/`<` (lines 41-53, 391-396).
  - `maintains_input_order = [true, false]` (lines 398-402).
  - It rejects unbounded inputs, floating-point eq keys, and volatile expressions (lines 1420-1480).
  - Planning: `LogicalPlan::AsOfJoin` is mapped to `AsOfJoinExec` (`main:core/src/physical_planner.rs:1806-1849`). SQL uses `JoinOperator::AsOf { match_condition, constraint }` (`main:sql/src/relation/join.rs:107-121`). The builder has `asof_join_on`/`asof_join_using` (`main:expr/src/logical_plan/builder.rs:1126,1173`), and the DataFrame API has `DataFrame::join_asof` (`main:core/src/dataframe/mod.rs:1584`).
  - It is never picked automatically for ordinary inequality joins. **[read]**

**Issues and PRs**

| # | Title | State | What it means for us |
|---|---|---|---|
| #8393 | Range/inequality joins are slow | open (2023-12-01) | Umbrella issue: inequality joins go to NLJ, while DuckDB uses IEJoin. **[read]** |
| apache/datafusion#318 | ASOF join support / Specialize Range Joins | closed 2026-09-15 | Closed when the ASOF stack landed. "Specialize range joins" was closed with it but not delivered for overlap joins. **[read]** |
| #23828 / #23829 / #23830 / #23831 / #23832 | ASOF physical operator / logical semantics / SQL / DataFrame / serialization | merged 2026-08-20 / 09-04 / 09-09 / 09-11 / 09-13 (all labelled v56.0.0) | ASOF is in v56, not 55.1. Umbrella PR #23738 closed unmerged (2026-09-20). Follow-ups merged: #24799 (functional dependencies), #24801 (left filter pushdown), #24873 (embedded projections). **[read]** (#23828 and #23829 bodies read; the others from titles and state.) |
| #26478 | Support hash-partitioned execution in AsOfJoinExec for memory-bounded joins | open (2026-10-18) | The right side is collected into a reservation that cannot spill, so a large right side fails with "Resources exhausted". **[read]** |
| #26592 | feat: support partitioned execution for ASOF joins | open PR (2026-10-22) | Adds `AsOfJoinMode::Partitioned`, co-partitioned on eq keys and sorted per partition, behind `repartition_asof_joins` (default false). With `contig` as the eq key, this would stream per contig. **[read]** |
| #17482 | feat: `ClassicJoin` for PWMJ | merged 2025-10-21 | The PWMJ that shipped (the original #16660 closed unmerged). **[read]** (title and state only) |
| #23870, #24457, #25467 | PWMJ LeftSemi/LeftAnti; RightSemi/RightAnti; Left/Right Mark | merged 2026-08-14, 09-11, 09-20 | Existence joins in PWMJ on `main`, not in 55.1. **[read]** (title and state only) |
| #24360 / #24361 | PWMJ reports a false equi-key equivalence for INNER range joins, dropping a required sort / fix | issue closed 2026-08-14; fix merged 2026-08-14 | **55.1 still has the bug**: `exec.rs:435` passes `from_ref(join_on)`. If we enable PWMJ on 55.1, a later `ORDER BY` can be wrongly removed. **[read]** |
| #24335, #24348, #24297, #25957 | PWMJ drops unmatched NULL-key rows / drops rows at small batch_size / loops forever / wrong results for List keys | all closed Aug–Oct 2026 | PWMJ correctness fixes after 55.1. More reason not to enable it on 55.1. **[read]** (titles and state; #24360 body read) |
| #26287 | perf(pwmj): build output batches from buffered runs… | open PR | Performance only. **[read]** (title) |
| #24157 | Support IEJoin for inner joins with two range predicates | open (2026-08-07) | Names interval-overlap joins as the motivating case. Supports optional equality keys. Inner joins only, no spilling, behind a config flag. **[read]** |
| #23741 | feat: add IEJoin for joins with two range predicates | open PR, marked stale on 2026-10-19 | Adds `IEJoinExec` behind `datafusion.optimizer.enable_ie_join` (default false). Equality keys are handled by hash co-partitioning, which discards locus order. The snapshot of `main` contains no `IEJoin` code. **[read]** |
| #12754 | feat: support inner iejoin | closed unmerged 2025-04-19 | An earlier attempt. **[read]** (title and state) |
| #21859 | feat: Adds IntervalJoinExec for point-in-interval range joins | closed unmerged 2026-07-05 (stale) | Pattern `probe >= lo AND probe < hi`, inner joins only, default on. The author waited on the "JoinAccelerator" API (#21921). Point-in-interval (variant in region) still goes to NLJ today. **[read]** |
| #21921 | Proposal: Refactor NLJ into an extensible framework for specialized joins | open | Proposed extension point for range/interval joins built on NLJ semantics. No merged API found. **[read]** |
| #26553 | refactor: extract join physical planning | open PR | Moves the planner join arm (hash, SMJ and PWMJ selection) into methods. It is a possible seam for a custom planner, with no behaviour change. **[read]** |
| #22395 / #25421 | [EPIC] Implement Range Partitioning / [EPIC] Optimize Range Partitioning for Joins… | open | `Partitioning::Range` exists in 55.1 (`datafusion-physical-expr-55.1.0/src/partitioning.rs:128`). Co-partitioned range equi-joins merged (#23184, 2026-07-10). This could let locus-range-partitioned tables join without a hash shuffle. **[read]** (epic bodies; #23184 from title only) |

### 2. Sort-merge join

| # | Title | State | What it means for us |
|---|---|---|---|
| #26260 | SortMergeJoin: derive sort options from input ordering, and allow it with a single partition when prefer_hash_join=false | open (2026-10-13) | Confirms both problems in the source. 55.1 `physical_planner.rs:1701-1722` requires `target_partitions>1 && repartition_joins && !prefer_hash_join` and uses `vec![SortOptions::default(); join_on_len]` (line 1719). `main` has the same code (lines 1746-1765). Inputs sorted with `NULLS LAST`/`DESC`, or run with `target_partitions=1`, get a re-sort or a hash join. **[read]** |
| #26261 | feat/perf: derive SMJ sort options from input ordering and allow SMJ for a single partition | open PR (2026-10-13) | The fix. Until it lands, a custom physical planner or rule has to build the SMJ itself. **[read]** |
| (source) | SMJ distribution and order | — | SMJ requires `KeyPartitioned` on both sides (55.1 `joins/sort_merge_join/exec.rs:416-426`). `maintains_input_order` keeps left order for Inner/Left*, right order for Right*, and none for Full (lines 228-243; `main` lines 257-272 are identical). No rule chooses SMJ from existing orderings: the 55.1 `join_selection.rs` does not mention `SortMergeJoinExec` at all (grep). **[read]** |
| #23111 | feat: add CollectLeft partition mode to SortMergeJoinExec | closed unmerged 2026-08-12 | Broadcast-sorted SMJ was tried and found slower because the merge is single-threaded. **[read]** |
| #20910 | [EPIC] Sort-merge join (SMJ) execution improvements | open | Mostly semi/anti and comparator performance work (done). **[read]** |
| #19013 | Spill error (oom) with sort merge join | open (2025-12-01) | SMJ spilling under memory pressure still fails. **[read]** |
| #25217 / #26015 | sort-merge fallback for partitioned hash joins under memory pressure | open PRs | Hash join would fall back to SMJ when memory runs short. Not ordering-driven. **[read]** (titles) |
| #26624 / #26623 | SanityCheckPlan error with SMJ when a filter makes a key constant / `a + b = a` | open (2026-10-23) | Equivalence-tracking bugs around SMJ orderings. **[read]** (titles) |
| #26235 / #26226 / #26228 | HashJoinExec: declare build-side order for LeftSemi/LeftAnti/LeftMark / loses build-side order across batches / fix | issue open / issue open / PR open | Even an ordered build side is not declared. The hash-map path reverses batch order. **[read]** |

### 3. Aggregation

- **Source, 55.1:** `datafusion-physical-plan-55.1.0/src/aggregates/mod.rs` **[read]**
  - `InputOrderMode` is `Sorted` when every group expression is ordered and there is one grouping set. It is `PartiallySorted(indices)` when some are, and `Linear` otherwise (lines 1010-1019). Grouping on a prefix of `(contig, pos, alleles)` such as `GROUP BY contig, pos` is therefore `Sorted`.
  - The streams that handle ordered input are `OrderedPartialAggregateStream` and `OrderedFinalAggregateStream`, used when `enable_migration_aggregate` is on (default true, `config.rs:890`). They require a single grouping set and supported limit options (lines 1240-1291). Everything else falls back to `GroupedHashAggregateStream`.

| # | Title | State | What it means for us |
|---|---|---|---|
| #23338 | Perf: Streaming Aggregation should not be chosen unconditionally when the input is sorted | open (2026-07-06) | With `target_partitions>1`, hash repartition after the partial stage scatters keys and forces a `SortExec` before the final `Sorted` aggregate. The reporter measured 1.3–1.8x slower than hash at 8 partitions. Expect this on locus GROUP BYs unless partitioning lines up. **[read]** |
| #24438 | Supporting analytics over large amounts of pre-partitioned data | open | Many sorted file groups merged into one DataFusion partition lose `(key,time)` order, so the aggregate cannot stream. Maintainers settled on a new `GROUPED` sort property. **[read]** |
| #24737 / #24697 / #24698 / #24497 | baseline test / refactor to `GroupClusteringMode` / Grouped equivalence properties / stream aggregates over grouped input | merged 2026-09-28 / merged 2026-10-16 / open / open | `main` has `GroupClusteringMode {None, Partial, Full}` (`main:physical-plan/src/aggregates/order/mod.rs:48-62`). The equivalence-level "grouped" property is not merged. **[read]** |
| #25150 | perf(aggregate): specialize fully ordered group keys without hashing | merged 2026-10-01 (v56) | Hash-free grouping for fully ordered multi-column keys, which is our `(contig,pos,…)` case, on `main` only. Its `GroupValuesOrdered` type is not present under that name in the snapshot; there is `group_values/multi_group_by/clustered.rs`. **[read]** PR / **[inferred]** rename |
| #23318 | Simplify aggregate table for ordered cases | open | Partially ordered input still uses hash aggregation with early emit. A TPC-DS regression was reported on the partially ordered path. **[read]** |
| #26725 | Fully sorted partial aggregate still fails with ResourcesExhausted after emitting completed groups… | open (2026-10-24) | A memory-accounting gap in the clustered partial stream on `main`. **[read]** |
| #25676 | Aggregate UDF's hard ordering requirement is dropped when it has no reverse_expr | open; reproduced on 55.1.0 | A custom order-sensitive UDAF (for example a first-allele-by-position aggregate) that does not implement `reverse_expr` can silently get unsorted input. Built-in aggregates are unaffected. **[read]** |
| #16919 | Streaming Aggregate operator not being used in deduplication of pre-sorted Parquet files | closed 2026-10-25 (not fixed; trade-off accepted) | `WITH ORDER (col_1, col_2)` over many files: `ListingTable` drops the ordering at scan time (see §4), so aggregation falls back to hashing. **[read]** |

### 4. Scans: file-level ordering checks, sort pushdown, partitioning

- **The min/max proof, 55.1** **[read]**
  - `FileScanConfig::eq_properties` uses `validated_output_ordering()` (`datafusion-datasource-55.1.0/src/file_scan_config/mod.rs:877-881`, `1242`).
  - That reaches `validate_orderings` and `is_ordering_valid_for_file_groups` (`file_scan_config/sort_pushdown.rs:464-497`). Single-file groups pass. Multi-file groups need `MinMaxStatistics::new_from_files(...).is_sorted()`, and an `Err` counts as unsorted.
  - `new_from_files` takes each sort column's file-level min and max separately (`statistics.rs:69-110`). `is_sorted` checks `max_row[i] <= min_row[i+1]` (`statistics.rs:276-284`).
  - So file A covering `chr1:100 … chr2:50` has max row `(chr2, max_pos_in_A)`. That exceeds file B's min row `(chr2, 51)` whenever A's largest position, which comes from chr1, is above 51. The ordering is then dropped. **[inferred from source]**
  - The filter keeps or drops a whole `LexOrdering`. There is no fallback to a valid prefix such as `(contig)`. **[read]**
  - Complex projections keep the ordering only if every group has at most one file (`sort_pushdown.rs:574-622`). **[read]**
  - `main` uses the same algorithm (`main:datasource/src/statistics.rs:307-312`, `file_scan_config/sort_pushdown.rs:477-493`), plus skipping of known-empty files (#24648). **[read]**
  - If `alleles` (a list column) has no Parquet min/max statistics, `new_from_files` returns `Err`. Any multi-file group would then drop a `(contig, position, alleles)` ordering entirely. **[inferred]**
- **ListingTable fallback:** with `split_file_groups_by_statistics` (default false, `config.rs:994`), if the statistics-based groups outnumber `target_partitions`, `ListingTable::scan` logs at debug level and falls back to unordered groups (`datafusion-catalog-listing-55.1.0/src/table.rs:567-594`). This happens before the optimizer runs, so `EXPLAIN VERBOSE` shows no ordering from the start (#16919 comments). **[read]**

| # | Title | State | What it means for us |
|---|---|---|---|
| #20329 | fix: validate inter-file ordering in eq_properties() | merged 2026-02-14 | Introduced `validated_output_ordering()`. Since then a declared `WITH ORDER` is no longer trusted across files. This is the origin of the min/max proof above. **[read]** |
| #6672 | [EPIC] Avoid sort for already sorted Parquet files that do not overlap values | open (2023) | Umbrella for statistics-based sort elimination. **[read]** |
| #10336 | Enable `split_file_groups_by_statistics` by default | open (2024) | Still default false. Open questions: the `>` vs `>=` touching-boundary inconsistency, and tests. **[read]** |
| #22198 | Follow-ups for stats-based RG / file reorder: multi-column, function-wrapped, compound exprs | open | Statistics reorder looks only at `sort_order.first()`. With `contig` leading, files that tie on contig are not ordered by position. Multi-column lexicographic reorder is the P0 item. **[read]** |
| #24270 | [EPIC] Sort Pushdown: remaining improvements | open | Lists the remaining items: #22198, #21381 (parallel SPM after sort elimination), #19486 and #19048 (an exact reverse scan is blocked on an arrow-rs reverse page reader), and #19394 (more nodes supporting sort pushdown). `try_pushdown_sort` exists in 55.1 (`datasource-55.1.0/src/source.rs:218`). Reverse scans are `Inexact` only. **[read]** |
| #22494 | SortExec elimination regression for wrong-order multi-file scans (regression from #21956) | closed 2026-05-24 (fix #22493) | The Inexact path stripped `output_ordering` even when reordered groups would re-validate. Fixed before 55. **[read]** |
| #19724 | Re-sort file groups in FileScanConfig to satisfy ordering requirements | open | The group-rebuilding question: parallelism against keeping order. **[read]** |
| #26458 | perf: balance file groups by size for scans with an output ordering | merged 2026-10-23 (v56) | On 55.1, ordered scans keep lopsided count-based groups. `main` re-cuts them by size when the result re-validates. **[read]** |
| #26434 | fix: keep file order for scans that advertise an output ordering | merged 2026-10-19 (v56) | **55.1 correctness risk.** `LimitPushdown` can call `with_preserve_order(false)` on an ordered scan (55.1 `file_scan_config/mod.rs:1157-1167` overwrites the flag). That re-enables file work stealing (`enable_file_stream_work_stealing` default true, `config.rs:1010`) and Parquet limit pruning, so `ORDER BY … LIMIT` can return wrong rows (#24272). `main` deprecates `ExecutionPlan::with_preserve_order` (`main:physical-plan/src/execution_plan.rs:997-1005`). **[read]** |
| #26278 | ORDER BY can return NULLs out of order across sorted Parquet files | closed 2026-10-21 | Reproduced on 55.0. Files with NULLs in the sort key were treated as sorted. Matters only if contig or position can be NULL. **[read]** |
| #24647 / #24648 | Empty Parquet files invalidate file ordering / fix | closed / merged 2026-09-01 | An empty file in a group used to drop the ordering. **[read]** (titles and state) |
| #25817 | Incorrect ORDER BY results with mismatched Parquet timestamp units | open | Not locus-relevant. **[read]** (title) |

### 5. Unions, merges, repartitions

| # | Title | State | What it means for us |
|---|---|---|---|
| #10314 | Implement a way to preserve partitioning through `UnionExec` without losing ordering | open (2024) | `EnforceDistribution` can replace `UnionExec` with `InterleaveExec`, which destroys ordering. The only switch is `prefer_existing_union` (default false, `config.rs:1734`). `InterleaveExec::maintains_input_order` is all-false (`physical-plan-55.1.0/src/union.rs:725-727`). **[read]** |
| #26625 / #26636 | SanityCheckPlan error for `UNION ALL … ORDER BY` when each arm selects the same column twice / fix: preserve equivalence classes in union output | issue open / PR open | `UnionExec` drops equivalence classes when it combines child properties. **[read]** |
| #26745 | Wrong results: order-preserving `RepartitionExec` keeps orderings of its input that do not hold after the merge | open (2026-10-25) | With `prefer_existing_sort=true` and several declared orderings, the planner can drop a needed sort. Same class of bug as #26646, which #26654 fixed for SPM. Relevant if we declare more than one ordering and enable `prefer_existing_sort`. **[read]** |
| #26162 | ORDER BY b, a over a subquery with ORDER BY b LIMIT n is not sorted by (b, a) when SPM has a fetch | open | A wrong-order bug around SPM with a fetch. **[read]** (title) |
| #21381 / #10316 / #15191 | Parallel merge in SPM after sort elimination / SPM that skips comparisons when key ranges are ordered / SPM to ProgressiveEval | open | After sort elimination, SPM over many ordered partitions runs single-threaded. There is no "ranges are disjoint, just concatenate" mode. **[read]** |
| #23276 / #26697 | Extension hook for custom operators in sort-requirement pushdown / [EPIC] Custom ExecutionPlan nodes: correct results… | open | Relevant to any custom genomics operator. Sort pushdown stops at nodes it does not know. Several rules guess about custom nodes, for example by assuming column *i* maps to column *i* when a node "maintains input order" (#26656). **[read]** |

### 6. Projections, UDF monotonicity, structs

- 55.1 `ScalarUDFImpl` has `output_ordering`, `preserves_lex_ordering` (default `false`), `strictly_order_preserving`, and `struct_field_mapping` (`datafusion-expr-55.1.0/src/udf.rs:965-1043`). The default `output_ordering` returns the shared `SortProperties` when all inputs agree, and `Unordered` otherwise. **[read]**
  - So a multi-argument key function such as `locus_key(contig, pos)` cannot claim to be ordered under `(contig, pos)` lexicographic order. `pos` alone is not `Ordered` in that context, so the inputs disagree. **[inferred]**
- #21217 "Propagate orderings through struct-producing projections", closed 2026-04-08. It added `struct_field_mapping`, so `named_struct('contig', contig, …)['contig']` keeps the ordering. That is in 55.1. **[read]**
- #25921 "LEFT JOIN with struct-field ORDER BY fails ordering validation", closed 2026-10-02. An equivalence-projection bug with `get_field`, fixed after 55.1. **[read]**
- #21307 "`ProjectionExec` produces unknown statistics for all `ScalarFunctionExpr` outputs", open. Computed keys also lose statistics, which further blocks statistics-based ordering. **[read]** (title)
- #26391 and #26430 are open SanityCheckPlan errors involving constant sort keys or computed columns under projections. They show that equivalence projection through computed columns is still fragile. **[read]** (titles)

### 7. Umbrella issues and API changes after 55.1

- **Umbrellas:**
  - #10313 "[Epic] A Collection of Sort Based Optimizations" (open). Its remaining children are #10336, #6672, #4177, #10314, #10316, #7053 and #14748. **[read]**
  - #24270 (sort pushdown remainder), #20910 (SMJ), #22395 and #25421 (range partitioning), and #24438 (grouped input). **[read]**
  - #26481 "EPIC: Separate optimizer rules from correctness transformation rules" and #26697 (custom nodes) are open. **[read]** (titles, and the #26697 body)
  - I found no single "ordering-aware planning" epic beyond #10313.
- **`ExecutionPlan` trait, 55.1 against `main`:** the same 40 method names. The only change is that `with_preserve_order` is `#[deprecated(since = "56.0.0")]` (#26434). Scans now always preserve file order when they advertise an ordering. **[read]**
- **Other ordering-relevant additions on `main` that 55.1 lacks:** `GroupClusteringMode` (#24697), right-existence `maintains_input_order` in PWMJ, `AsOfJoinExec` and `LogicalPlan::AsOfJoin`, size-balanced ordered file groups (#26458), and `FileScanConfig::preserves_file_order()` (`main:datasource/src/file_scan_config/mod.rs:1319`). **[read]**

## Ordered operators the backend gets by configuration alone

These assume an ordered source that is one partition, like our sorted table (ADRs 0007, 0011, 0012). Recommended session settings: `prefer_hash_join=false`, `target_partitions>1`, `repartition_joins=true`, `prefer_existing_union=true`. Turning on `prefer_existing_sort=true` costs nothing here and helps sources with several partitions, but read the #26745 caveat in item 9 first. Lock the list in with plan-shape tests, as ADR 0014 does.

1. **Streaming aggregation** (`Sorted`) on the locus key, on a prefix of it (`contig`), or on any permutation of it. `PartiallySorted` applies when group keys extend the order. `first_value` and `last_value ... ORDER BY pos` within a key also stream. These work with defaults. At tp>1 the ordered partial and final stages are joined by an order-preserving repartition and an SPM, with no sort (ran).
2. **Sort-merge join on the locus key or on a prefix of it**, for Inner, Left, Right, LeftSemi, LeftAnti, RightSemi and RightAnti joins (and Mark joins, read). It keeps the left order (the right order for Right joins), so a following requirement on the key needs no sort (ran). Conditions:
   - `prefer_hash_join=false` with `target_partitions>1`;
   - `ON` written in key order;
   - non-nullable key columns, or an input order of `NULLS FIRST`.

   The cost is a hash repartition into `target_partitions` partitions and a final SPM. Note that `prefer_hash_join=false` turns *every* equality join into SMJ, unordered ones included, and those get sorts.
3. **Hash-join probe order** for Inner and Right* joins, when a hash join is used anyway, as at tp=1. `join_reordering=false` stops statistics-driven side swaps from changing which input keeps its order.
4. **Windows over the locus order**: running and sliding frames, `lag`/`lead`, `row_number`, `rank`, also in reverse order, and `PARTITION BY contig ORDER BY pos` at tp>1, all without a sort (ran). Frames ending `UNBOUNDED FOLLOWING`, `ntile`, `cume_dist` and `percent_rank` reuse the order but block (`WindowAggExec`).
5. **Merge of ordered unions** (SPM over `UnionExec`, ran). This is the mechanism behind ADR 0014's merge tree. `prefer_existing_union=true` guarantees that a union never becomes `InterleaveExec`.
6. **Order through filters, renames, widening casts, and `col = lit` constants.** A filter on `contig = '1'` satisfies an order on `pos` (ran).
7. **`LIMIT` on an ordered scan** is folded into the scan (ran). On 55.1, `ORDER BY … LIMIT` over a *multi-file* ordered scan carries #26434's wrong-rows risk, fixed on `main`. With our single file group this risk is low, though that is untested (**inferred**).
8. **Statistics-based file reordering** (`PushdownSort`) for sources other than our sorted table. Set `enable_sort_pushdown=false` if the assertion policy treats it as a sort.
9. **Caveats that apply to everything above in 55.1:**
   - Declare **one** ordering per source (#26745).
   - Don't trust the second half of a joined ordering, i.e. the other side's order appended after the kept side's keys (#25052).
   - Don't enable PWMJ (#24360 and the other PWMJ correctness fixes that landed after 55.1).

## Cases that need our own physical rules

Each entry gives the case, why configuration can't get it, and the shape of a fix. "Rule" means a physical optimizer rule or an extension planner in the backend. Where noted, the case needs a custom `ExecutionPlan` as well.

1. **Interval and overlap joins by locus** (`contig =` plus `start <= pos < end`, or two overlapping intervals).
   - *Why not stock:* stock planning gives a hash join or SMJ on `contig` with a residual filter, which compares every pair within a contig, or a nested-loop join without the equality.
   - *What else exists:* PWMJ, IEJoin and the closed `IntervalJoinExec` (#21859) all fall short (Q6), and ASOF (v56) answers nearest-match lookups only.
   - *Fix:* a rule that recognises the stock logical shape (an equality join on contig plus a range filter) and plans a streaming interval-merge join over the two ordered inputs. That is a custom exec, plus the rule.
2. **SMJ where the inputs are already ordered, and only there.**
   - *Why not stock:* `prefer_hash_join` is global, and nothing derives the key order or `SortOptions` from the inputs (#26260/#26261, open).
   - *Fix:* a rule that replaces a hash join with SMJ when both inputs satisfy the key order, permuting the keys to match.
3. **SMJ at one partition, without the hash shuffle and final merge.**
   - *Why not stock:* the planner never builds SMJ at tp=1, and at tp>1 SMJ needs hash co-partitioning.
   - *Fix:* the same rule as item 2, or range partitioning by locus once the range-partitioning work (#22395/#25421) is usable.
4. **Full outer join by key, ordered on the merged key.**
   - *Why not stock:* SMJ declares no order for Full joins, and `coalesce(l.k, r.k)` loses order through the projection (ran). This is Hail's outer `join` on keyed tables.
   - *Fix:* a rule or exec that declares the merged key order, or a key-merging projection the backend knows to be ordered.
5. **Merge joins that keep the left side's key order when that side is the build side.**
   - *Why not stock:* a hash join never declares its build side's order (#26235 is open). This only matters where SMJ isn't used. Items 2 and 3 cover it if SMJ always is.
6. **Computed and struct keys.**
   - *Why not stock:* `+`, `-` and `*` with a literal, narrowing casts, `concat`, a key function over several columns such as `locus_key(contig, pos)`, and `get_field` on a struct-ordered `locus` column all lose the order.
   - *Fix:* declare and assert orderings on flat columns (`contig`, `position`, `alleles`), in line with ADR 0009. Or provide backend UDFs that override `output_ordering` and `strictly_order_preserving`, which only works for single-argument monotone functions. Or rewrite such keys in a rule.
7. **Windows in `Linear` or `PartiallySorted` mode on bounded input**, e.g. `PARTITION BY sample ORDER BY locus` over a locus-only order.
   - *Why not stock:* DataFusion sorts by `(sample, locus)` instead.
   - *Fix:* a rule that builds a `BoundedWindowAggExec` in `Linear` mode.
8. **Parallelism without a shuffle.** Hash repartitioning plus a final SPM is the only stock way to run ordered operators on more than one partition.
   - *Fix:* splitting by locus ranges and declaring `Partitioning::Range` with equal split points on both join inputs needs our scan to declare it. In 55.1 that only helps if partition counts and key order match exactly (Q1).
9. **The ordering assertion itself.** No setting makes DataFusion fail on an inserted sort.
   - *Fix:* a backend node that mirrors `DataSinkExec`: a single hard ordering, `SinglePartition`, and `maintains_input_order=[true]`. Its check lives in `check_invariants(Executable)` and walks the final plan as described in Q5. Optionally, a rule right after `EnsureRequirements` too, if file reordering is to count as sorting.
10. **Multi-file ordered scans.** These are already handled by our `SortedTable` and `OrderedSource` (ADRs 0011 and 0012). Stock `FileScanConfig` drops compound orderings across files.
