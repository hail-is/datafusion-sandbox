//! The dataset fixtures, laid out like the real datasets, and the helpers that read back what a
//! test wrote or a plan returned.

mod disk_datasets;
mod growing_datasets;
mod reading_back;
mod rows;
mod shared_datasets;
mod sorted_tables;

use crate::{
    format::InputFormat,
    locus::{Locus, LocusRepresentation},
    pipeline::{self, PipelineOptions},
    testing::fixture::{self, FixtureFormat, block_on},
};

use datafusion::{arrow::record_batch::RecordBatch, prelude::SessionContext};
use futures::TryStreamExt;
use object_store::{ObjectStore, path::Path};
use std::sync::Arc;

const FORMATS: [FixtureFormat; 2] = [FixtureFormat::Parquet, FixtureFormat::Vortex];
const REPRESENTATIONS: [LocusRepresentation; 2] = [
    LocusRepresentation::ContigPosition,
    LocusRepresentation::Packed,
];

/// A documented fixture file's stem, and its rows as contig ordinal, position, and alleles.
type DocumentedFile = (&'static str, [(u32, i32, &'static str); 2]);

/// The documented fixture files of one sample, in locus order, each with its rows as contig
/// ordinal, position, and alleles.
const DOCUMENTED_FILES: [DocumentedFile; 4] = [
    ("d", [(1, 1, "A,G"), (1, 2, "A,C")]),
    ("c", [(1, 2, "A,G"), (1, 3, "A,C")]),
    ("b", [(1, 4, "A,G"), (2, 1, "A,C")]),
    ("a", [(2, 2, "A,G"), (2, 3, "A,C")]),
];

/// The rows of `stem` in [`DOCUMENTED_FILES`], as loci and alleles.
fn documented_rows(stem: &str) -> Vec<(Locus, String)> {
    let (_, rows) = DOCUMENTED_FILES
        .iter()
        .find(|(known, _)| *known == stem)
        .unwrap();
    rows.iter()
        .map(|&(contig, position, alleles)| {
            (Locus::new(contig, position).unwrap(), alleles.to_string())
        })
        .collect()
}

/// Every object on `store` beneath `prefix`, by its path below the prefix, in path order.
fn listed_beneath(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Vec<String> {
    let mut locations: Vec<Path> = block_on(
        store
            .list(Some(prefix))
            .map_ok(|meta| meta.location)
            .try_collect(),
    )
    .unwrap();
    locations.sort();
    locations
        .iter()
        .map(|location| {
            location
                .prefix_match(prefix)
                .unwrap()
                .map(|part| part.as_ref().to_string())
                .collect::<Vec<_>>()
                .join("/")
        })
        .collect()
}

/// The one file at `path`, read back through a session `register` prepares.
fn read_back(
    path: String,
    input_format: InputFormat,
    register: impl FnOnce(&SessionContext) + Send + 'static,
) -> Vec<RecordBatch> {
    pipeline::run(
        move |ctx| async move {
            register(&ctx);
            fixture::read_file(&ctx, &path, &input_format, None).await
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

/// The loci and alleles of every row of `batches`, read in `representation`.
fn loci_and_alleles(
    batches: &[RecordBatch],
    representation: LocusRepresentation,
) -> Vec<(Locus, String)> {
    batches
        .iter()
        .flat_map(|batch| {
            fixture::decode_loci(batch, representation)
                .into_iter()
                .zip(fixture::string_column(batch, "alleles"))
        })
        .collect()
}

/// The column names of the batches' schema.
fn column_names(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .first()
        .unwrap()
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

/// The columns of a stored row's locus in `representation`.
fn locus_columns(representation: LocusRepresentation) -> Vec<String> {
    representation
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn extension(format: FixtureFormat) -> &'static str {
    match format {
        FixtureFormat::Parquet => "parquet",
        FixtureFormat::Vortex => "vortex",
    }
}
