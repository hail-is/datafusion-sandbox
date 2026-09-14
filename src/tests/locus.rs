use crate::locus::{Locus, LocusInterval, LocusOrdering, LocusRepresentation, SplitPoints};
use datafusion::{
    arrow::{
        array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    common::{DFSchema, DataFusionError},
    prelude::{SessionContext, col},
};
use std::sync::Arc;

fn locus(contig_ordinal: u32, position: i32) -> Locus {
    Locus::new(contig_ordinal, position).unwrap()
}

fn configuration_error(error: &DataFusionError) -> String {
    assert!(
        matches!(error, DataFusionError::Configuration(_)),
        "{error}"
    );
    error.to_string()
}

/// A contig ordinal and position, compared as a plain tuple so the expected side of a filter
/// test does not go through `Locus`.
type Probe = (u32, i32);

/// Loci around a contig boundary, in locus order: neighbours of every interval bound the filter
/// tests use, plus a contig's first position.
const PROBES: [Probe; 7] = [(1, 2), (1, 3), (1, 4), (2, 0), (2, 1), (2, 2), (2, 3)];

/// One row per probe, stored under `representation`.
fn probe_batch(representation: LocusRepresentation) -> RecordBatch {
    let loci = PROBES.map(|(contig_ordinal, position)| locus(contig_ordinal, position));
    let (fields, columns): (Vec<Field>, Vec<ArrayRef>) = match representation {
        LocusRepresentation::ContigPosition => (
            vec![
                Field::new("contig", DataType::Utf8, false),
                Field::new("position", DataType::Int32, false),
            ],
            vec![
                Arc::new(StringArray::from_iter_values(
                    loci.iter().map(|locus| locus.contig_name()),
                )),
                Arc::new(Int32Array::from_iter_values(
                    loci.iter().map(|locus| locus.position()),
                )),
            ],
        ),
        LocusRepresentation::Packed => (
            vec![Field::new("locus", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from_iter_values(
                loci.iter().map(|locus| locus.packed()),
            ))],
        ),
    };
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// The probes `interval`'s filter keeps under `representation`, found by evaluating the filter
/// on the probe batch as a physical expression.
fn kept_by_filter(interval: LocusInterval, representation: LocusRepresentation) -> Vec<Probe> {
    let batch = probe_batch(representation);
    let filter = interval
        .filter(representation)
        .expect("every interval under test has a bound");
    let schema = DFSchema::try_from(batch.schema()).unwrap();
    let filter = SessionContext::new()
        .create_physical_expr(filter, &schema)
        .unwrap();
    let mask = filter
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let mask = mask.as_any().downcast_ref::<BooleanArray>().unwrap();
    PROBES
        .iter()
        .zip(mask)
        .filter(|(_, kept)| *kept == Some(true))
        .map(|(probe, _)| *probe)
        .collect()
}

/// The probes in `[start, end)` by tuple comparison, judged without the filter.
fn probes_within(start: Option<Probe>, end: Option<Probe>) -> Vec<Probe> {
    PROBES
        .into_iter()
        .filter(|probe| {
            start.is_none_or(|start| *probe >= start) && end.is_none_or(|end| *probe < end)
        })
        .collect()
}

#[test]
fn a_locus_parses_and_renders_as_contig_colon_position() {
    let parsed: Locus = "22:16050075".parse().unwrap();

    assert_eq!(parsed, locus(22, 16_050_075));
    assert_eq!(parsed.contig_ordinal(), 22);
    assert_eq!(parsed.position(), 16_050_075);
    assert_eq!(parsed.to_string(), "22:16050075");
    assert_eq!(parsed.contig_name(), "chr22");
}

#[test]
fn a_locus_reads_its_contig_ordinal_from_a_contig_name() {
    assert_eq!(Locus::from_contig_name("chr22", 5).unwrap(), locus(22, 5));
    assert_eq!(
        Locus::from_contig_name(&locus(3, 7).contig_name(), 7).unwrap(),
        locus(3, 7)
    );
}

#[test]
fn a_locus_rejects_a_contig_name_without_an_ordinal() {
    for name in ["22", "chrX", "chr", "Chr1", "chr-1"] {
        let message = configuration_error(&Locus::from_contig_name(name, 1).unwrap_err());
        assert!(
            message.contains(&format!("'{name}'")) && message.contains("chr{ordinal}"),
            "{name:?}: {message}"
        );
    }

    let message = configuration_error(&Locus::from_contig_name("chr1", -1).unwrap_err());
    assert!(
        message.contains("position must be non-negative"),
        "{message}"
    );
}

#[test]
fn a_locus_orders_by_contig_then_position() {
    assert!(locus(1, 1000) < locus(2, 5));
    assert!(locus(2, 5) < locus(2, 6));
    assert_eq!(locus(2, 5), locus(2, 5));
}

#[test]
fn a_locus_rejects_malformed_text() {
    for (text, reason) in [
        ("100", "expected contig:position"),
        ("chr1:100", "contig must be a non-negative integer"),
        ("-1:100", "contig must be a non-negative integer"),
        ("1:-100", "position must be a non-negative integer"),
        ("1:100.5", "position must be a non-negative integer"),
        ("1: 100", "position must be a non-negative integer"),
        (
            "1:2147483648",
            "position must be a non-negative integer that fits an i32",
        ),
        ("", "expected contig:position"),
    ] {
        let message = configuration_error(&text.parse::<Locus>().unwrap_err());
        assert!(
            message.contains(&format!("'{text}'")) && message.contains(reason),
            "{text:?}: {message}"
        );
    }
}

#[test]
fn a_locus_rejects_coordinates_outside_the_packed_range() {
    let message = configuration_error(&Locus::new(1, -1).unwrap_err());
    assert!(
        message.contains("position must be non-negative"),
        "{message}"
    );

    let message = configuration_error(&Locus::new(u32::MAX, 1).unwrap_err());
    assert!(
        message.contains("contig ordinal must be at most"),
        "{message}"
    );
}

#[test]
fn a_locus_packs_its_contig_ordinal_above_its_position_and_unpacks_again() {
    let packed = locus(22, 16_050_075).packed();

    assert_eq!(packed, (22_i64 << 32) + 16_050_075);
    assert_eq!(Locus::from_packed(packed).unwrap(), locus(22, 16_050_075));
    assert_eq!(Locus::from_packed(0).unwrap(), locus(0, 0));
    assert_eq!(
        Locus::from_packed(locus(u32::try_from(i32::MAX).unwrap(), i32::MAX).packed()).unwrap(),
        locus(u32::try_from(i32::MAX).unwrap(), i32::MAX)
    );
}

#[test]
fn a_locus_rejects_packed_values_no_locus_produces() {
    let message = configuration_error(&Locus::from_packed(-1).unwrap_err());
    assert!(message.contains("-1"), "{message}");

    let position_bit_31 = (1_i64 << 32) | (1_i64 << 31);
    let message = configuration_error(&Locus::from_packed(position_bit_31).unwrap_err());
    assert!(message.contains(&position_bit_31.to_string()), "{message}");
}

#[test]
fn split_points_parse_and_render_as_a_comma_separated_list() {
    let points: SplitPoints = "1:100,1:200,2:50".parse().unwrap();

    assert_eq!(
        points,
        SplitPoints::new(vec![locus(1, 100), locus(1, 200), locus(2, 50)]).unwrap()
    );
    assert_eq!(points.to_string(), "1:100,1:200,2:50");
}

#[test]
fn split_points_reject_the_empty_string_but_admit_the_empty_list() {
    let message = configuration_error(&"".parse::<SplitPoints>().unwrap_err());
    assert!(message.contains("empty"), "{message}");

    let none = SplitPoints::new(vec![]).unwrap();
    assert_eq!(none.to_string(), "");
    assert_eq!(none.intervals(), [LocusInterval::new(None, None).unwrap()]);
}

#[test]
fn split_points_reject_an_empty_item_by_quoting_it() {
    let message = configuration_error(&"1:100,,2:50".parse::<SplitPoints>().unwrap_err());
    assert!(message.contains("invalid locus ''"), "{message}");

    let message = configuration_error(&"1:100,".parse::<SplitPoints>().unwrap_err());
    assert!(message.contains("invalid locus ''"), "{message}");
}

#[test]
fn split_points_reject_a_pair_out_of_order_by_naming_it() {
    for (text, pair) in [
        ("1:100,1:100", "1:100 does not follow 1:100"),
        ("1:100,1:200,1:150,2:1", "1:150 does not follow 1:200"),
        ("2:1,1:500", "1:500 does not follow 2:1"),
    ] {
        let message = configuration_error(&text.parse::<SplitPoints>().unwrap_err());
        assert!(message.contains("strictly increasing"), "{text}: {message}");
        assert!(message.contains(pair), "{text}: {message}");
    }
}

#[test]
fn split_points_expand_to_one_more_interval_than_points() {
    let points: SplitPoints = "1:100,2:50".parse().unwrap();

    assert_eq!(
        points.intervals(),
        [
            LocusInterval::new(None, Some(locus(1, 100))).unwrap(),
            LocusInterval::new(Some(locus(1, 100)), Some(locus(2, 50))).unwrap(),
            LocusInterval::new(Some(locus(2, 50)), None).unwrap(),
        ]
    );
    let rendered: Vec<String> = points.intervals().iter().map(ToString::to_string).collect();
    assert_eq!(rendered, ["..1:100", "1:100..2:50", "2:50.."]);
}

#[test]
fn a_locus_interval_rejects_a_start_not_before_its_end() {
    let message =
        configuration_error(&LocusInterval::new(Some(locus(2, 1)), Some(locus(1, 9))).unwrap_err());
    assert!(
        message.contains("2:1 is not before its end 1:9"),
        "{message}"
    );

    let message =
        configuration_error(&LocusInterval::new(Some(locus(1, 1)), Some(locus(1, 1))).unwrap_err());
    assert!(
        message.contains("1:1 is not before its end 1:1"),
        "{message}"
    );
}

/// The filter keeps a locus iff it lies in `[start, end)` by contig ordinal then position, under
/// either representation, whether the interval crosses a contig, stays within one, or is
/// unbounded on a side.
#[test]
fn a_locus_interval_filter_keeps_exactly_the_loci_from_its_start_up_to_its_end() {
    let cases: [(Option<Probe>, Option<Probe>); 6] = [
        (Some((1, 3)), Some((2, 2))),
        (None, Some((1, 3))),
        (Some((2, 2)), None),
        (Some((1, 3)), Some((1, 4))),
        (Some((1, 4)), Some((2, 1))),
        (Some((2, 0)), Some((2, 3))),
    ];
    for (start, end) in cases {
        let interval = LocusInterval::new(
            start.map(|(contig_ordinal, position)| locus(contig_ordinal, position)),
            end.map(|(contig_ordinal, position)| locus(contig_ordinal, position)),
        )
        .unwrap();
        let expected = probes_within(start, end);
        assert!(!expected.is_empty(), "{interval}: no probe falls inside");
        assert!(
            expected.len() < PROBES.len(),
            "{interval}: every probe falls inside"
        );
        for representation in [
            LocusRepresentation::ContigPosition,
            LocusRepresentation::Packed,
        ] {
            assert_eq!(
                kept_by_filter(interval, representation),
                expected,
                "{interval} under {representation}"
            );
        }
    }
}

#[test]
fn the_whole_ordering_has_no_filter() {
    let whole = LocusInterval::new(None, None).unwrap();

    assert_eq!(whole.filter(LocusRepresentation::ContigPosition), None);
    assert_eq!(whole.filter(LocusRepresentation::Packed), None);
    assert_eq!(whole.to_string(), "..");
}

#[test]
fn accepts_an_exact_locus_ordering_prefix() {
    assert!(LocusOrdering::locus().is_prefix_of(&LocusOrdering::locus()));
}

#[test]
fn accepts_a_finer_locus_ordering_prefix() {
    assert!(LocusOrdering::locus().is_prefix_of(&LocusOrdering::locus_then_alleles()));
}

#[test]
fn rejects_an_insufficient_locus_ordering_prefix() {
    assert!(!LocusOrdering::locus_then_alleles().is_prefix_of(&LocusOrdering::locus()));
}

#[test]
fn expands_a_contig_position_locus_ordering() {
    assert_eq!(
        LocusOrdering::locus()
            .expand(LocusRepresentation::ContigPosition)
            .sort_expressions(),
        vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ]
    );
}

#[test]
fn expands_a_packed_locus_then_alleles_ordering() {
    assert_eq!(
        LocusOrdering::locus_then_alleles()
            .expand(LocusRepresentation::Packed)
            .sort_expressions(),
        vec![
            col("locus").sort(true, false),
            col("alleles").sort(true, false),
        ]
    );
}

#[test]
fn contig_position_stored_ordering_exposes_its_columns_and_locus_prefix() {
    let ordering = LocusOrdering::locus_then_alleles().expand(LocusRepresentation::ContigPosition);

    assert_eq!(ordering.column_names(), ["contig", "position", "alleles"]);
    let locus_prefix = ordering.locus_prefix();
    assert_eq!(
        locus_prefix.sort_expressions(),
        vec![
            col("contig").sort(true, false),
            col("position").sort(true, false),
        ]
    );
    assert_eq!(
        locus_prefix.partition_expressions(),
        vec![col("contig"), col("position")]
    );
}

#[test]
fn packed_stored_ordering_exposes_its_columns_and_locus_prefix() {
    let ordering = LocusOrdering::locus_then_alleles().expand(LocusRepresentation::Packed);

    assert_eq!(ordering.column_names(), ["locus", "alleles"]);
    let locus_prefix = ordering.locus_prefix();
    assert_eq!(
        locus_prefix.sort_expressions(),
        vec![col("locus").sort(true, false)]
    );
    assert_eq!(locus_prefix.partition_expressions(), vec![col("locus")]);
}

#[test]
fn rejects_contig_without_position_as_a_locus_representation() {
    let error =
        LocusRepresentation::detect(&schema(vec![Field::new("contig", DataType::Utf8, false)]))
            .expect_err("contig-position requires both fields");

    assert!(matches!(error, DataFusionError::Plan(_)));
    assert!(error.to_string().contains("position"));
}

#[test]
fn detects_a_contig_position_locus_representation() {
    let representation = LocusRepresentation::detect(&schema(vec![
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
    ]))
    .unwrap();

    assert_eq!(representation, LocusRepresentation::ContigPosition);
}

#[test]
fn detects_an_int64_packed_locus_representation() {
    let representation =
        LocusRepresentation::detect(&schema(vec![Field::new("locus", DataType::Int64, false)]))
            .unwrap();

    assert_eq!(representation, LocusRepresentation::Packed);
}

#[test]
fn rejects_both_locus_representations() {
    let error = LocusRepresentation::detect(&schema(vec![
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
        Field::new("locus", DataType::Int64, false),
    ]))
    .expect_err("a schema must have exactly one locus representation");

    assert!(matches!(error, DataFusionError::Plan(_)));
    let message = error.to_string();
    assert!(message.contains("both"), "unexpected error: {message}");
    assert!(message.contains("contig"), "unexpected error: {message}");
    assert!(message.contains("locus"), "unexpected error: {message}");
}

#[test]
fn rejects_neither_locus_representation() {
    let error =
        LocusRepresentation::detect(&schema(vec![Field::new("alleles", DataType::Utf8, false)]))
            .expect_err("a schema must have one locus representation");

    assert!(matches!(error, DataFusionError::Plan(_)));
    let message = error.to_string();
    assert!(message.contains("neither"), "unexpected error: {message}");
    assert!(message.contains("contig"), "unexpected error: {message}");
    assert!(message.contains("locus"), "unexpected error: {message}");
}

#[test]
fn rejects_a_non_int64_packed_locus_representation() {
    let error =
        LocusRepresentation::detect(&schema(vec![Field::new("locus", DataType::Utf8, false)]))
            .expect_err("a packed locus must be Int64");

    assert!(matches!(error, DataFusionError::Plan(_)));
    let message = error.to_string();
    assert!(message.contains("locus"), "unexpected error: {message}");
    assert!(message.contains("Int64"), "unexpected error: {message}");
    assert!(message.contains("Utf8"), "unexpected error: {message}");
}

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}
