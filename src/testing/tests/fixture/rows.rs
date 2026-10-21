//! The fixture's rows as one sample's expected result, and the helpers that build filters over
//! them and decode what a plan returned.

use super::{DOCUMENTED_FILES, REPRESENTATIONS, documented_rows};
use crate::{
    locus::{Locus, LocusRepresentation},
    pipeline::{self, PipelineOptions},
    testing::fixture::{
        self, contig_filter, decode_loci, decode_rows, file_stems, locus_cells, sample_rows,
        string_column,
    },
};

use datafusion::{
    arrow::{
        array::{ArrayRef, Int64Array, StringArray, StringViewArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    prelude::{col, lit},
};
use object_store::path::Path;
use std::sync::Arc;

#[test]
fn sample_rows_are_the_documented_rows_in_locus_then_alleles_order() {
    let expected: Vec<(Locus, String)> = DOCUMENTED_FILES
        .iter()
        .flat_map(|(stem, _)| documented_rows(stem))
        .collect();

    let rows: Vec<(Locus, String)> = sample_rows()
        .into_iter()
        .map(|(locus, alleles)| (locus, alleles.to_string()))
        .collect();

    assert_eq!(rows, expected);
    assert!(rows.is_sorted());
}

/// A batch of `loci` in `representation`, with the alleles and sample of each row.
fn batch(
    loci: &[Locus],
    alleles: Vec<&str>,
    samples: Vec<&str>,
    representation: LocusRepresentation,
) -> RecordBatch {
    let mut fields = representation.fields();
    fields.push(Field::new("alleles", DataType::Utf8View, false));
    fields.push(Field::new("s", DataType::Utf8, false));
    let mut columns = representation.locus_arrays(loci);
    let alleles: ArrayRef = Arc::new(StringViewArray::from(alleles));
    let samples: ArrayRef = Arc::new(StringArray::from(samples));
    columns.extend([alleles, samples]);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn locus(contig: u32, position: i32) -> Locus {
    Locus::new(contig, position).unwrap()
}

#[test]
fn contig_filter_keeps_exactly_the_rows_on_the_contig() {
    let loci = [
        locus(1, 0),
        locus(1, 1),
        locus(1, i32::MAX),
        locus(2, 0),
        locus(2, 7),
        locus(3, 1),
    ];
    for representation in REPRESENTATIONS {
        for (contig, ordinal) in [("chr01", 1), ("chr02", 2)] {
            let batch = batch(&loci, vec!["A,G"; 6], vec!["s"; 6], representation);
            let filter = contig_filter(representation, contig);

            let kept = pipeline::run(
                move |ctx| async move { ctx.read_batch(batch)?.filter(filter)?.collect().await },
                PipelineOptions::single_threaded(),
            )
            .unwrap();

            let kept: Vec<Locus> = kept
                .iter()
                .flat_map(|batch| decode_loci(batch, representation))
                .collect();
            let expected: Vec<Locus> = loci
                .iter()
                .copied()
                .filter(|locus| locus.contig_ordinal() == ordinal)
                .collect();
            assert_eq!(kept, expected, "{representation:?} {contig}");
        }
    }
}

#[test]
fn under_contig_position_contig_filter_is_an_equality_on_the_contig() {
    assert_eq!(
        contig_filter(LocusRepresentation::ContigPosition, "chr01"),
        col("contig").eq(lit("chr01"))
    );
}

#[test]
fn contig_filter_panics_on_a_name_with_no_contig_ordinal() {
    for representation in REPRESENTATIONS {
        let panicked = std::panic::catch_unwind(|| contig_filter(representation, "X"));

        assert!(panicked.is_err(), "{representation:?}");
    }
}

#[test]
fn decode_loci_reads_each_rows_locus_in_either_representation() {
    let loci = [locus(2, 3), locus(1, 1), locus(22, 50_000_000)];
    for representation in REPRESENTATIONS {
        let batch = batch(&loci, vec!["A,G"; 3], vec!["s"; 3], representation);

        assert_eq!(
            decode_loci(&batch, representation),
            loci,
            "{representation:?}"
        );
    }
}

#[test]
#[should_panic(expected = "a fixture result has valid locus fields")]
fn decode_loci_panics_on_a_batch_without_the_representations_fields() {
    let batch = batch(
        &[locus(1, 1)],
        vec!["A,G"],
        vec!["s"],
        LocusRepresentation::Packed,
    );

    drop(decode_loci(&batch, LocusRepresentation::ContigPosition));
}

#[test]
fn decode_rows_reads_each_rows_locus_alleles_and_sample() {
    let loci = [locus(1, 2), locus(1, 1)];
    for representation in REPRESENTATIONS {
        let batch = batch(
            &loci,
            vec!["A,C", "A,G"],
            vec!["HG00308", "NA18534"],
            representation,
        );

        assert_eq!(
            decode_rows(&batch, representation),
            [
                (locus(1, 2), "A,C".to_string(), "HG00308".to_string()),
                (locus(1, 1), "A,G".to_string(), "NA18534".to_string())
            ],
            "{representation:?}"
        );
    }
}

#[test]
fn string_column_reads_plain_and_view_strings_in_row_order() {
    let batch = batch(
        &[locus(1, 1), locus(1, 2)],
        vec!["A,G", "A,C"],
        vec!["x", "y"],
        LocusRepresentation::Packed,
    );

    assert_eq!(string_column(&batch, "alleles"), ["A,G", "A,C"]);
    assert_eq!(string_column(&batch, "s"), ["x", "y"]);
}

#[test]
#[should_panic(expected = "None")]
fn string_column_panics_on_a_missing_column() {
    let batch = batch(
        &[locus(1, 1)],
        vec!["A,G"],
        vec!["x"],
        LocusRepresentation::Packed,
    );

    drop(string_column(&batch, "missing"));
}

#[test]
#[should_panic(expected = "n is not a string column: Int64")]
fn string_column_panics_on_a_column_of_another_type() {
    let column: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let batch = RecordBatch::try_from_iter([("n", column)]).unwrap();

    drop(string_column(&batch, "n"));
}

#[test]
#[should_panic(expected = "None")]
fn string_column_panics_on_a_null() {
    let column: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None]));
    let batch = RecordBatch::try_from_iter([("s", column)]).unwrap();

    drop(string_column(&batch, "s"));
}

#[test]
fn file_stems_names_the_documented_stem_of_each_path_in_order() {
    let paths = [
        "fixtures/samples/s=HG00308/d.parquet",
        "a.vortex",
        "elsewhere/c.anything",
        "b.parquet",
    ]
    .map(Path::from);

    assert_eq!(file_stems(&paths), ["d", "a", "c", "b"]);
}

#[test]
#[should_panic(expected = "e.parquet is not a documented fixture file")]
fn file_stems_panics_on_an_undocumented_stem() {
    drop(file_stems(&[
        Path::from("d.parquet"),
        Path::from("e.parquet"),
    ]));
}

#[test]
#[should_panic(expected = "s=HG00308/d is not a fixture file")]
fn file_stems_panics_on_a_path_without_an_extension() {
    drop(file_stems(&[Path::from("s=HG00308/d")]));
}

#[test]
fn locus_cells_are_the_cells_datafusion_displays_for_the_locus() {
    let loci = [locus(1, 1), locus(22, 50_000_000), locus(3, 0)];
    for representation in REPRESENTATIONS {
        let columns = representation.locus_arrays(&loci);
        for (row, &locus) in loci.iter().enumerate() {
            let displayed: Vec<String> = columns
                .iter()
                .map(|column| array_value_to_string(column, row).unwrap())
                .collect();

            assert_eq!(
                fixture::locus_cells(locus, representation),
                displayed,
                "{representation:?} {locus}"
            );
        }
    }
    assert_eq!(
        locus_cells(locus(1, 1), LocusRepresentation::ContigPosition),
        ["chr01", "1"]
    );
}
