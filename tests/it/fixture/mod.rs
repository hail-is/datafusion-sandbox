//! Tiny per-sample tables written to a temporary directory, laid out the way the
//! real datasets are. Lets the combiners' plans be built and asserted on offline,
//! with no credentials and without touching the excluded `data/` directory.

use datafusion::{
    arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    prelude::*,
};
use datafusion_sandbox::format::OutputFormat;
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
    write_sample_tables_with_format(dir, sample_set, &OutputFormat::VORTEX)
}

/// The parquet counterpart of [`write_sample_tables`], laid out identically so
/// the two formats' plan shapes are compared over the same data.
pub fn write_parquet_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    write_sample_tables_with_format(dir, sample_set, &OutputFormat::PARQUET)
}

/// Writes the sample tables in `format`, deriving every filename from it.
fn write_sample_tables_with_format(
    dir: &Path,
    sample_set: &[&str],
    format: &'static OutputFormat,
) -> String {
    let root = dir.join("samples");
    let pipeline_root = root.clone();
    let sample_set = sample_set
        .iter()
        .map(|sample| sample.to_string())
        .collect::<Vec<_>>();
    pipeline::run(
        move |ctx: SessionContext| async move {
            for sample in sample_set {
                for &(contig, filename) in CONTIG_FILES {
                    let path = pipeline_root
                        .join(format!("s={sample}"))
                        .join(format!("{filename}.{}", format.extension()));
                    let path = path.to_str().expect("fixture path is valid UTF-8");
                    let df = ctx.read_batch(sample_batch(contig))?;
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
fn sample_batch(contig: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("contig", DataType::Utf8, false),
        Field::new("position", DataType::Int32, false),
        Field::new("alleles", DataType::Utf8, false),
    ]));
    let contigs =
        StringArray::from_iter_values(std::iter::repeat_n(contig, ROWS_PER_CONTIG as usize));
    let positions = Int32Array::from_iter_values(1..=ROWS_PER_CONTIG);
    let alleles = StringArray::from_iter_values(
        (1..=ROWS_PER_CONTIG).map(|p| if p % 2 == 0 { "A,C" } else { "A,G" }),
    );
    RecordBatch::try_new(
        schema,
        vec![Arc::new(contigs), Arc::new(positions), Arc::new(alleles)],
    )
    .expect("fixture batch matches its schema")
}
