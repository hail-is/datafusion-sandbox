use crate::formulation::combine_refs_grouped_merge::sample_groups;

use std::num::NonZeroUsize;

#[test]
fn splits_the_sample_set_into_contiguous_count_balanced_sample_groups() {
    let samples = ["a", "b", "c", "d", "e"].map(ToString::to_string);

    assert_eq!(sample_groups(&samples, groups(1)), [&samples[..]]);
    assert_eq!(
        sample_groups(&samples, groups(2)),
        [&samples[..3], &samples[3..]]
    );
    assert_eq!(
        sample_groups(&samples, groups(3)),
        [&samples[..2], &samples[2..4], &samples[4..]]
    );
    assert_eq!(
        sample_groups(&samples, groups(8)),
        samples.iter().map(std::slice::from_ref).collect::<Vec<_>>()
    );
}

fn groups(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}
