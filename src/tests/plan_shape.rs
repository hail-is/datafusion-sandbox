//! Shared observation and assertions for physical plan shape.
//!
//! Generic observers expose the rendered tree, typed nodes, operators, and scanned files. Named
//! assertions state the plan shapes this crate relies on. Every assertion includes the whole plan
//! on failure; checks within a child plan include that subtree as well.

use crate::{
    locus::{LocusOrdering, LocusRepresentation, StoredOrdering},
    sink::PartitionedSinkExec,
    sorted_table,
};

use datafusion::{
    datasource::{physical_plan::FileScanConfig, sink::DataSinkExec, source::DataSourceExec},
    physical_expr::expressions::Column,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, displayable,
        filter::FilterExec,
        repartition::RepartitionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
};
use object_store::path::Path;

use std::{fmt, sync::Arc};

/// An owned handle through which tests observe and assert a physical plan's shape.
pub struct PlanShape {
    plan: Arc<dyn ExecutionPlan>,
}

impl PlanShape {
    /// Observes `plan` without taking ownership from its test.
    #[must_use]
    pub fn of(plan: &Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan: Arc::clone(plan),
        }
    }

    /// Every node of type `T` in the plan, root first.
    #[must_use]
    pub fn nodes_of<T: ExecutionPlan>(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        nodes_of::<T>(&self.plan)
    }

    /// The operator on each rendered line, root first, with display indentation retained and
    /// operator details removed.
    #[must_use]
    pub fn operators(&self) -> Vec<String> {
        self.to_string()
            .lines()
            .map(|line| {
                line.split_once(':')
                    .map_or(line, |(operator, _)| operator)
                    .to_string()
            })
            .collect()
    }

    /// The object-store paths read by each scan, with scans and files in plan order.
    ///
    /// This reads each scan's typed [`FileScanConfig`]. Sorted-table scans wrap that config in an
    /// `OrderedSource`, so the test-only bridge in `sorted_table` unwraps it without making the
    /// production source type part of the crate API.
    #[must_use]
    pub fn scanned_files(&self) -> Vec<Vec<Path>> {
        self.nodes_of::<DataSourceExec>()
            .into_iter()
            .map(|node| {
                let scan = node
                    .downcast_ref::<DataSourceExec>()
                    .expect("nodes_of returned a node of another type");
                let config = scan
                    .data_source()
                    .downcast_ref::<FileScanConfig>()
                    .or_else(|| sorted_table::file_scan_config(scan.data_source().as_ref()))
                    .unwrap_or_else(|| {
                        panic!(
                            "expected DataSourceExec to contain a FileScanConfig:\n{}",
                            self.context(&node)
                        )
                    });
                config
                    .file_groups
                    .iter()
                    .flat_map(datafusion_datasource::file_groups::FileGroup::files)
                    .map(|file| file.object_meta.location.clone())
                    .collect()
            })
            .collect()
    }

    /// The object-store paths read by the plan's only scan, in file order.
    #[must_use]
    pub fn files_in_only_scan(&self) -> Vec<Path> {
        let mut scans = self.scanned_files();
        assert_eq!(
            scans.len(),
            1,
            "expected exactly one DataSourceExec, got {}:\n{self}",
            scans.len()
        );
        scans.pop().expect("the scan count was checked")
    }

    /// Asserts the merge tree over sample groups of the given sizes, in sample order.
    ///
    /// One group is a flat union of one single-partition input per sample beneath one
    /// sort-preserving merge. Several groups add an outer union with one single-partition input
    /// per group. A group of one sample is its scan; a larger group is a flat merge. A formulation
    /// may merge once more after parallel operators above the outer union. No sort may appear.
    pub fn assert_merge_tree(&self, groups: &[usize]) {
        if let [n_samples] = groups {
            self.assert_flat_merge(&self.plan, *n_samples);
            return;
        }

        let n_groups = groups.len();
        let unions = self.nodes_of::<UnionExec>();
        let outer = unions
            .first()
            .unwrap_or_else(|| panic!("expected a union of {n_groups} sample groups:\n{self}"));
        let inputs = outer.children();
        assert_eq!(
            inputs.len(),
            n_groups,
            "expected one union input per sample group:\n{}",
            self.context(outer)
        );
        for (input, &n_samples) in inputs.iter().zip(groups) {
            assert_eq!(
                input.output_partitioning().partition_count(),
                1,
                "expected one partition per sample group input:\n{}",
                self.context(input)
            );
            if n_samples == 1 {
                assert!(
                    input.is::<DataSourceExec>(),
                    "expected a group of one sample to be its scan:\n{}",
                    self.context(input)
                );
            } else {
                self.assert_flat_merge(input, n_samples);
            }
        }

        let merged_groups = groups.iter().filter(|&&n_samples| n_samples > 1).count();
        let expected_merged_nodes = merged_groups.checked_add(1).unwrap();
        assert_eq!(
            unions.len(),
            expected_merged_nodes,
            "expected one UnionExec per merged sample group plus the union of groups:\n{self}"
        );
        let merges = self.nodes_of::<SortPreservingMergeExec>();
        assert_eq!(
            merges.len(),
            expected_merged_nodes,
            "expected one SortPreservingMergeExec per merged sample group plus the final merge:\n{self}"
        );
        let final_merge = merges.first().expect("the final merge was counted");
        assert!(
            contains_node(final_merge, outer),
            "expected the union of sample groups beneath the final merge:\n{}",
            self.context(final_merge)
        );
        self.assert_no_sorts();
    }

    /// Asserts one union with one ordered, single-partition input per sample and no merge above it.
    pub fn assert_one_ordered_partition_per_sample(&self, n_samples: usize) {
        let unions = self.nodes_of::<UnionExec>();
        assert_eq!(
            unions.len(),
            1,
            "expected exactly one UnionExec, got {}:\n{}",
            unions.len(),
            self
        );
        let union = unions.first().expect("the union count was checked");
        assert_eq!(
            union.children().len(),
            n_samples,
            "expected one union input per sample:\n{self}"
        );
        for input in union.children() {
            assert_eq!(
                input.output_partitioning().partition_count(),
                1,
                "expected one partition per sample input:\n{}",
                self.context(input)
            );
            assert!(
                input.output_ordering().is_some(),
                "expected each sample input to carry an ordering:\n{}",
                self.context(input)
            );
        }
        let merges = self.nodes_of::<SortPreservingMergeExec>();
        assert!(
            merges.is_empty(),
            "expected no merge above the per-sample partitions:\n{self}"
        );
        self.assert_no_sorts();
    }

    /// Asserts that the plan contains no re-sort.
    pub fn assert_no_sorts(&self) {
        self.assert_no_sorts_in(&self.plan);
    }

    /// Asserts that every one of `n_scans` scans displays a predicate over the representation's
    /// first locus column.
    ///
    /// This observation is text-based on purpose. Parquet renders `predicate=`, Vortex renders
    /// `predicate:`, and Vortex's file source is a foreign type. The absence of a [`FilterExec`]
    /// elsewhere and the result assertions establish that this displayed predicate is effective.
    pub fn assert_filter_reaches_every_scan(
        &self,
        n_scans: usize,
        representation: LocusRepresentation,
    ) {
        let locus_column = LocusOrdering::locus()
            .expand(representation)
            .column_names()
            .into_iter()
            .next()
            .expect("a locus ordering has a stored field");
        let scans = self.nodes_of::<DataSourceExec>();
        assert_eq!(
            scans.len(),
            n_scans,
            "expected {n_scans} scans, got {}:\n{}",
            scans.len(),
            self
        );
        for scan in scans {
            let text = Self::of(&scan).to_string();
            let predicate = text
                .split_once("predicate=")
                .or_else(|| text.split_once("predicate:"))
                .map(|(_, predicate)| predicate);
            assert!(
                predicate.is_some_and(|predicate| predicate.contains(&locus_column)),
                "expected a filter on {locus_column} to reach the scan:\n{}",
                self.context(&scan)
            );
        }
    }

    /// Asserts the merge tree and that the optimizer left the filter inside every scan.
    pub fn assert_filter_stays_inside_the_scans(&self, groups: &[usize]) {
        self.assert_merge_tree(groups);
        assert!(
            self.nodes_of::<FilterExec>().is_empty(),
            "expected the filter to be applied inside every scan, but the plan contains a FilterExec:\n{self}"
        );
        for union in self.nodes_of::<UnionExec>() {
            let union = union
                .downcast_ref::<UnionExec>()
                .expect("nodes_of returned a node of another type");
            for input in union.children() {
                assert!(
                    nodes_of::<RepartitionExec>(input).is_empty(),
                    "expected no repartition beneath a union:\n{}",
                    self.context(input)
                );
            }
        }
        for node in self.nodes_of::<RepartitionExec>() {
            let repartition = node
                .downcast_ref::<RepartitionExec>()
                .expect("nodes_of returned a node of another type");
            assert!(
                repartition.maintains_input_order().iter().all(|&kept| kept),
                "expected every repartition above the union to preserve its input order:\n{}",
                self.context(&node)
            );
        }
    }

    /// Asserts one flat sample merge per locus interval, with at most one final merge above the
    /// union of intervals and no filter, sort, or repartition operator.
    pub fn assert_one_merge_per_interval(
        &self,
        intervals: usize,
        samples: usize,
        representation: LocusRepresentation,
    ) {
        assert!(intervals > 0, "expected at least one interval:\n{self}");
        if intervals == 1 {
            self.assert_flat_merge(&self.plan, samples);
        } else {
            let unions = self.nodes_of::<UnionExec>();
            let outer = unions
                .first()
                .unwrap_or_else(|| panic!("expected a union of intervals:\n{self}"));
            assert_eq!(
                outer.children().len(),
                intervals,
                "expected one union input per interval:\n{}",
                self.context(outer)
            );
            for input in outer.children() {
                self.assert_flat_merge(input, samples);
            }

            let merges = self.nodes_of::<SortPreservingMergeExec>();
            let final_merges = merges
                .len()
                .checked_sub(intervals)
                .unwrap_or_else(|| panic!("expected a merge per interval:\n{self}"));
            assert!(
                final_merges <= 1,
                "expected at most one merge above the union of intervals:\n{self}"
            );
            if final_merges == 1 {
                let final_merge = merges.first().expect("one final merge was counted");
                let child = final_merge
                    .children()
                    .into_iter()
                    .next()
                    .expect("a sort-preserving merge has one child");
                assert!(
                    Arc::ptr_eq(child, outer),
                    "expected the final merge directly above the union of intervals:\n{}",
                    self.context(outer)
                );
            }
        }

        let n_scans = intervals.checked_mul(samples).unwrap();
        if intervals > 1 {
            self.assert_filter_reaches_every_scan(n_scans, representation);
        } else {
            let scans = self.nodes_of::<DataSourceExec>();
            assert_eq!(
                scans.len(),
                n_scans,
                "expected {n_scans} scans, got {}:\n{}",
                scans.len(),
                self
            );
        }
        for (name, found) in [
            ("FilterExec", self.nodes_of::<FilterExec>().len()),
            ("SortExec", self.nodes_of::<SortExec>().len()),
            ("RepartitionExec", self.nodes_of::<RepartitionExec>().len()),
        ] {
            assert_eq!(found, 0, "expected no {name}:\n{self}");
        }
        for node in self.nodes_of::<UnionExec>() {
            let union = node
                .downcast_ref::<UnionExec>()
                .expect("nodes_of returned a node of another type");
            assert_eq!(
                node.output_partitioning().partition_count(),
                union.children().len(),
                "expected every union input to be one partition:\n{}",
                self.context(&node)
            );
        }
    }

    /// Asserts that the root is a file or partitioned sink requiring `ordering`.
    ///
    /// Only column names are compared. Every stored ordering in these tests uses the same direction
    /// and null placement.
    pub fn assert_ends_in_sink_requiring(&self, ordering: &StoredOrdering) {
        let required = match (
            self.plan.downcast_ref::<DataSinkExec>(),
            self.plan.downcast_ref::<PartitionedSinkExec>(),
        ) {
            (Some(sink), _) => sink.sort_order().as_ref(),
            (_, Some(sink)) => sink.ordering(),
            (None, None) => {
                panic!("expected the plan to end in DataSinkExec or PartitionedSinkExec:\n{self}")
            }
        };
        let required =
            required.unwrap_or_else(|| panic!("expected the sink to require an ordering:\n{self}"));
        let required: Vec<String> = required
            .iter()
            .map(|sort| {
                sort.expr.downcast_ref::<Column>().map_or_else(
                    || "<not a column>".to_string(),
                    |column| column.name().to_string(),
                )
            })
            .collect();
        assert_eq!(
            required,
            ordering.column_names(),
            "expected the sink to require the stored ordering:\n{self}"
        );
    }

    /// One union with `n_samples` single-partition inputs under one sort-preserving merge, and no
    /// sort in `subtree`.
    fn assert_flat_merge(&self, subtree: &Arc<dyn ExecutionPlan>, n_samples: usize) {
        let unions = nodes_of::<UnionExec>(subtree);
        assert_eq!(
            unions.len(),
            1,
            "expected exactly one UnionExec, got {}:\n{}",
            unions.len(),
            self.context(subtree)
        );
        let union = unions.first().expect("the union count was checked");
        assert_eq!(
            union.children().len(),
            n_samples,
            "expected one union input per sample:\n{}",
            self.context(subtree)
        );
        for input in union.children() {
            assert_eq!(
                input.output_partitioning().partition_count(),
                1,
                "expected one partition per sample input:\n{}",
                self.context(input)
            );
        }

        let merges = nodes_of::<SortPreservingMergeExec>(subtree);
        assert_eq!(
            merges.len(),
            1,
            "expected exactly one SortPreservingMergeExec, got {}:\n{}",
            merges.len(),
            self.context(subtree)
        );
        let merge = merges.first().expect("the merge count was checked");
        assert!(
            contains_node(merge, union),
            "expected the union beneath the sort-preserving merge:\n{}",
            self.context(merge)
        );
        self.assert_no_sorts_in(subtree);
    }

    fn assert_no_sorts_in(&self, subtree: &Arc<dyn ExecutionPlan>) {
        assert!(
            nodes_of::<SortExec>(subtree).is_empty(),
            "expected no re-sort, but the plan contains a SortExec:\n{}",
            self.context(subtree)
        );
    }

    fn context(&self, subtree: &Arc<dyn ExecutionPlan>) -> String {
        if Arc::ptr_eq(&self.plan, subtree) {
            self.to_string()
        } else {
            format!("whole plan:\n{}\nsubtree:\n{}", self, Self::of(subtree))
        }
    }
}

impl fmt::Display for PlanShape {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}",
            displayable(self.plan.as_ref()).indent(true)
        )
    }
}

/// Operator names from rendered explain text, in display order.
///
/// Unlike [`PlanShape::operators`], this text-side observer is flat because explain output does not
/// expose a typed plan and may be embedded in a table.
#[must_use]
pub(super) fn exec_names(plan: &str) -> Vec<&str> {
    plan.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| word.ends_with("Exec"))
        .collect()
}

fn contains_node(plan: &Arc<dyn ExecutionPlan>, target: &Arc<dyn ExecutionPlan>) -> bool {
    Arc::ptr_eq(plan, target)
        || plan
            .children()
            .into_iter()
            .any(|child| contains_node(child, target))
}

fn nodes_of<T: ExecutionPlan>(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut found = Vec::new();
    if plan.downcast_ref::<T>().is_some() {
        found.push(Arc::clone(plan));
    }
    for child in plan.children() {
        found.extend(nodes_of::<T>(child));
    }
    found
}
