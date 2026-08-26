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

/// The sample set of the `1kg_chr22` benchmark dataset.
#[allow(dead_code)]
pub const SAMPLES: &[&str] = &[
    "HG00308", "HG00592", "HG02230", "NA18534", "NA20760", "NA18530", "HG03805", "HG02223",
    "HG00637", "NA12249", "HG02224", "NA21099", "NA11830", "HG01378", "HG00187", "HG01356",
    "HG02188", "NA20769", "HG00190", "NA18618", "NA18507", "HG03363", "NA21123", "HG03088",
    "NA21122", "HG00373", "HG01058", "HG00524", "NA18969", "HG03833", "HG04158", "HG03578",
    "HG00339", "HG00313", "NA20317", "HG00553", "HG01357", "NA19747", "NA18609", "HG01377",
    "NA19456", "HG00590", "HG01383", "HG00320", "HG04001", "NA20796", "HG00323", "HG01384",
    "NA18613", "NA20802",
];

/// The single contig the fixture writes, so that each sample is exactly one file
/// and a plan's partition count is its sample count.
const CONTIG: &str = "chr22";

/// Loci per sample. Small enough that writing fifty samples is instant.
const ROWS_PER_SAMPLE: i32 = 8;

/// Loci per sample for a fixture the optimizer is willing to byte-range split,
/// which is more than the 8192-row `batch_size` default: `enforce_distribution`
/// refuses to split a scan of fewer rows than one batch. Below this the hostile
/// session in `tests/plan_shape` is not hostile at all, and every assertion
/// there passes with the session derivation deleted.
const SPLITTABLE_ROWS_PER_SAMPLE: i32 = 20_000;

/// Writes one vortex table per sample under `dir`, as
/// `s=<sample>/contig=<contig>/fixture.vortex`, with rows in locus order.
/// Returns the root path the combiners read.
///
/// Every sample covers the same loci with the same alleles, so a plan that
/// de-duplicates across the sample set has something to de-duplicate.
#[allow(dead_code)]
pub fn write_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    write_sample_tables_with_format(dir, sample_set, &OutputFormat::VORTEX, ROWS_PER_SAMPLE)
}

/// The parquet counterpart of [`write_sample_tables`], laid out identically so
/// the two formats' plan shapes are compared over the same data.
// Each integration test crate compiles this shared module separately, and not
// all of them use every writer here.
#[allow(dead_code)]
pub fn write_parquet_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    write_sample_tables_with_format(dir, sample_set, &OutputFormat::PARQUET, ROWS_PER_SAMPLE)
}

/// [`write_sample_tables`] at [`SPLITTABLE_ROWS_PER_SAMPLE`], for tests that
/// need the optimizer to be willing to split a per-sample scan.
#[allow(dead_code)]
pub fn write_splittable_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    write_sample_tables_with_format(
        dir,
        sample_set,
        &OutputFormat::VORTEX,
        SPLITTABLE_ROWS_PER_SAMPLE,
    )
}

/// The parquet counterpart of [`write_splittable_sample_tables`].
#[allow(dead_code)]
pub fn write_splittable_parquet_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    write_sample_tables_with_format(
        dir,
        sample_set,
        &OutputFormat::PARQUET,
        SPLITTABLE_ROWS_PER_SAMPLE,
    )
}

/// Writes the sample tables in `format`, deriving every filename from it.
fn write_sample_tables_with_format(
    dir: &Path,
    sample_set: &[&str],
    format: &'static OutputFormat,
    rows_per_sample: i32,
) -> String {
    let root = dir.join("samples");
    for sample in sample_set {
        let path = root
            .join(format!("s={sample}"))
            .join(format!("contig={CONTIG}"));
        let path = path.join(format!("fixture.{}", format.extension()));
        let path = path
            .to_str()
            .expect("fixture path is valid UTF-8")
            .to_string();
        let batch = sample_batch(rows_per_sample);
        pipeline::run(
            move |ctx: SessionContext| async move {
                let df = ctx.read_batch(batch)?;
                format.write(df, &path).await
            },
            PipelineOptions {
                threads: 1,
                ..Default::default()
            },
        )
        .expect("writing a fixture table");
    }
    root.to_str()
        .expect("fixture path is valid UTF-8")
        .to_string()
}

/// One sample's rows, sorted by the locus ordering: one locus per position, with
/// alleles alternating between two values.
fn sample_batch(rows_per_sample: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("position", DataType::Int32, false),
        Field::new("alleles", DataType::Utf8, false),
    ]));
    let positions = Int32Array::from_iter_values(1..=rows_per_sample);
    let alleles = StringArray::from_iter_values(
        (1..=rows_per_sample).map(|p| if p % 2 == 0 { "A,C" } else { "A,G" }),
    );
    RecordBatch::try_new(schema, vec![Arc::new(positions), Arc::new(alleles)])
        .expect("fixture batch matches its schema")
}
