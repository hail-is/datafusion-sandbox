use crate::formulation::combine_refs_grouped_merge::input_groups;
use crate::stored::dataset::InputTable;

use std::num::NonZeroUsize;

#[test]
fn splits_the_input_tables_into_contiguous_count_balanced_input_groups() {
    let input_tables = ["a", "b", "c", "d", "e"].map(InputTable::single_sample);

    assert_eq!(input_groups(&input_tables, groups(1)), [&input_tables[..]]);
    assert_eq!(
        input_groups(&input_tables, groups(2)),
        [&input_tables[..3], &input_tables[3..]]
    );
    assert_eq!(
        input_groups(&input_tables, groups(3)),
        [&input_tables[..2], &input_tables[2..4], &input_tables[4..]]
    );
    assert_eq!(
        input_groups(&input_tables, groups(8)),
        input_tables
            .iter()
            .map(std::slice::from_ref)
            .collect::<Vec<_>>()
    );
}

fn groups(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}
