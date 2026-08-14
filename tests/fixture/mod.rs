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
use datafusion_sandbox::pipeline::{self, PipelineOptions};

use std::{path::Path, sync::Arc};

/// The single contig the fixture writes, so that each sample is exactly one file
/// and a plan's partition count is its sample count.
const CONTIG: &str = "chr22";

/// Loci per sample. Small enough that writing is instant, large enough that a
/// batch is a real sorted run.
const ROWS_PER_SAMPLE: i32 = 8;

/// Writes one table per sample under `dir`, as
/// `s=<sample>/contig=<contig>/<sample>.vortex`, with rows in locus order.
/// Returns the root path the combiners read.
///
/// Every sample covers the same loci with the same alleles, so a plan that
/// de-duplicates across samples has something to de-duplicate.
pub fn write_sample_tables(dir: &Path, samples: &[&str]) -> String {
    let root = dir.join("samples");
    for sample in samples {
        let path = root
            .join(format!("s={sample}"))
            .join(format!("contig={CONTIG}"));
        let path = path.join(format!("{sample}.vortex"));
        let path = path
            .to_str()
            .expect("fixture path is valid UTF-8")
            .to_string();
        let batch = sample_batch();
        pipeline::run(
            move |ctx: SessionContext| async move { ctx.read_batch(batch) },
            &path,
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
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("position", DataType::Int32, false),
        Field::new("alleles", DataType::Utf8, false),
    ]));
    let positions = Int32Array::from_iter_values(1..=ROWS_PER_SAMPLE);
    let alleles = StringArray::from_iter_values(
        (1..=ROWS_PER_SAMPLE).map(|p| if p % 2 == 0 { "A,C" } else { "A,G" }),
    );
    RecordBatch::try_new(schema, vec![Arc::new(positions), Arc::new(alleles)])
        .expect("fixture batch matches its schema")
}
