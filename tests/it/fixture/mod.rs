//! Tiny per-sample tables written to a temporary directory, laid out the way the
//! real datasets are. Lets the combiners' plans be built and asserted on offline,
//! with no credentials and without touching the excluded `data/` directory.

use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    prelude::*,
};
use datafusion_sandbox::format::OutputFormat;
use datafusion_sandbox::locus::LocusRepresentation;
use datafusion_sandbox::pipeline::{self, PipelineOptions};

use std::{path::Path, sync::Arc};

/// Four samples from the `1kg_chr22` benchmark dataset, the most any test needs.
pub const SAMPLES: &[&str] = &["HG00308", "HG00592", "HG02230", "NA18534"];

/// Contigs and filenames in locus order. The filenames sort in the opposite
/// order, so a reader must use statistics rather than path order.
const CONTIG_FILES: &[(&str, &str)] = &[("chr1", "d"), ("chr2", "c"), ("chr3", "b"), ("chr4", "a")];

/// Loci per contig. Four contigs keep the existing eight rows per sample.
const ROWS_PER_CONTIG: i32 = 2;

/// Writes one Vortex table per sample under `dir`, as several files in
/// `s=<sample>/`, with `contig` stored in every file.
/// Returns the root path the combiners read.
///
/// Every sample covers the same loci with the same alleles, so a plan that
/// de-duplicates across the sample set has something to de-duplicate.
pub fn write_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    write_sample_tables_with_representation(
        dir,
        sample_set,
        &OutputFormat::VORTEX,
        LocusRepresentation::ContigPosition,
    )
}

/// The parquet counterpart of [`write_sample_tables`], laid out identically so
/// the two formats' plan shapes are compared over the same data.
pub fn write_parquet_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    write_sample_tables_with_representation(
        dir,
        sample_set,
        &OutputFormat::PARQUET,
        LocusRepresentation::ContigPosition,
    )
}

pub fn write_packed_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    write_sample_tables_with_representation(
        dir,
        sample_set,
        &OutputFormat::VORTEX,
        LocusRepresentation::Packed,
    )
}

pub fn write_packed_parquet_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    write_sample_tables_with_representation(
        dir,
        sample_set,
        &OutputFormat::PARQUET,
        LocusRepresentation::Packed,
    )
}

pub fn write_sample_table_without_alleles(dir: &Path) -> String {
    let batch = RecordBatch::try_from_iter(vec![
        ("contig", Arc::new(StringArray::from(vec!["chr1"])) as _),
        ("position", Arc::new(Int32Array::from(vec![1])) as _),
    ])
    .expect("no-alleles fixture batch matches its schema");
    write_single_sample_table(dir, batch, "no-alleles")
}

fn write_single_sample_table(dir: &Path, batch: RecordBatch, description: &str) -> String {
    let root = dir.join("samples");
    let pipeline_root = root.clone();
    pipeline::run(
        move |ctx: SessionContext| async move {
            let path = pipeline_root.join("s=sample-a/a.vortex");
            let df = ctx.read_batch(batch)?;
            OutputFormat::VORTEX
                .write(df, path.to_str().expect("fixture path is valid UTF-8"))
                .await?;
            Ok(())
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| panic!("writing {description} fixture table: {error}"));
    root.to_str()
        .expect("fixture path is valid UTF-8")
        .to_string()
}

/// Writes the sample tables in `format`, deriving every filename from it.
fn write_sample_tables_with_representation(
    dir: &Path,
    sample_set: &[&str],
    format: &'static OutputFormat,
    representation: LocusRepresentation,
) -> String {
    let root = dir.join("samples");
    let pipeline_root = root.clone();
    let sample_set = sample_set
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    pipeline::run(
        move |ctx: SessionContext| async move {
            for sample in sample_set {
                for &(contig, filename) in CONTIG_FILES {
                    let path = pipeline_root
                        .join(format!("s={sample}"))
                        .join(format!("{filename}.{}", format.extension()));
                    let path = path.to_str().expect("fixture path is valid UTF-8");
                    let df = ctx.read_batch(sample_batch(contig, representation))?;
                    format.write(df, path).await?;
                }
            }
            Ok(())
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .expect("writing fixture tables");
    root.to_str()
        .expect("fixture path is valid UTF-8")
        .to_string()
}

/// One sample's rows, sorted by the locus ordering: one locus per position, with
/// alleles alternating between two values.
fn sample_batch(contig: &str, representation: LocusRepresentation) -> RecordBatch {
    let alleles = StringArray::from_iter_values(
        (1..=ROWS_PER_CONTIG).map(|p| if p % 2 == 0 { "A,C" } else { "A,G" }),
    );
    let (fields, columns): (Vec<Field>, Vec<ArrayRef>) = match representation {
        LocusRepresentation::ContigPosition => {
            let contigs = StringArray::from_iter_values(std::iter::repeat_n(
                contig,
                ROWS_PER_CONTIG as usize,
            ));
            let positions = Int32Array::from_iter_values(1..=ROWS_PER_CONTIG);
            (
                vec![
                    Field::new("contig", DataType::Utf8, false),
                    Field::new("position", DataType::Int32, false),
                    Field::new("alleles", DataType::Utf8, false),
                ],
                vec![Arc::new(contigs), Arc::new(positions), Arc::new(alleles)],
            )
        }
        LocusRepresentation::Packed => {
            let ordinal = contig.strip_prefix("chr").unwrap().parse::<i64>().unwrap();
            let loci = Int64Array::from_iter_values(
                (1..=ROWS_PER_CONTIG).map(|position| (ordinal << 32) | i64::from(position)),
            );
            (
                vec![
                    Field::new("locus", DataType::Int64, false),
                    Field::new("alleles", DataType::Utf8, false),
                ],
                vec![Arc::new(loci), Arc::new(alleles)],
            )
        }
    };
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("fixture batch matches its schema")
}
