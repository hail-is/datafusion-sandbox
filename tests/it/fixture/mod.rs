//! Small stored datasets laid out like the real datasets.
//!
//! Shared in-memory inventory:
//! - Vortex with contig-position loci
//! - Vortex with packed loci
//! - Parquet with contig-position loci
//! - Parquet with packed loci
//!
//! Per-test owned disk inventory:
//! - Vortex with contig-position loci
//! - Parquet with contig-position loci
//!
//! On-demand disk inventory:
//! - Vortex with packed loci
//! - Vortex with no `alleles` field

use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::listing::ListingTableUrl,
    execution::object_store::ObjectStoreUrl,
    prelude::*,
};
use datafusion_sandbox::format::{InputFormat, OutputFormat};
use datafusion_sandbox::locus::LocusRepresentation;
use datafusion_sandbox::pipeline::{self, PipelineOptions};
use futures::future::join_all;
use object_store::{ObjectStore, memory::InMemory};

use std::{
    path::Path,
    sync::{Arc, LazyLock},
};

/// Four samples from the `1kg_chr22` benchmark dataset, the most any test needs.
pub const SAMPLES: &[&str] = &["HG00308", "HG00592", "HG02230", "NA18534"];

/// Contigs and filenames in locus order. The filenames sort in the opposite
/// order, so a reader must use statistics rather than path order.
const CONTIG_FILES: &[(&str, &str)] = &[("chr1", "d"), ("chr2", "c"), ("chr3", "b"), ("chr4", "a")];

/// Loci per contig. Four contigs keep the existing eight rows per sample.
const ROWS_PER_CONTIG: i32 = 2;

#[derive(Clone, Copy)]
pub enum FixtureFormat {
    Parquet,
    Vortex,
}

impl FixtureFormat {
    fn datafusion_formats(self) -> (&'static OutputFormat, InputFormat) {
        match self {
            Self::Parquet => (&OutputFormat::PARQUET, InputFormat::PARQUET),
            Self::Vortex => (&OutputFormat::VORTEX, InputFormat::VORTEX),
        }
    }
}

static VORTEX_CONTIG_POSITION: LazyLock<Arc<DatasetFixture>> = LazyLock::new(|| {
    build_in_memory_fixture(
        "vortex-contig-position",
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    )
});
static VORTEX_PACKED: LazyLock<Arc<DatasetFixture>> = LazyLock::new(|| {
    build_in_memory_fixture(
        "vortex-packed",
        FixtureFormat::Vortex,
        LocusRepresentation::Packed,
    )
});
static PARQUET_CONTIG_POSITION: LazyLock<Arc<DatasetFixture>> = LazyLock::new(|| {
    build_in_memory_fixture(
        "parquet-contig-position",
        FixtureFormat::Parquet,
        LocusRepresentation::ContigPosition,
    )
});
static PARQUET_PACKED: LazyLock<Arc<DatasetFixture>> = LazyLock::new(|| {
    build_in_memory_fixture(
        "parquet-packed",
        FixtureFormat::Parquet,
        LocusRepresentation::Packed,
    )
});

pub struct DatasetFixture {
    format: FixtureFormat,
    store: Arc<dyn ObjectStore>,
    table_path: ListingTableUrl,
}

impl DatasetFixture {
    pub fn table_path(&self) -> &ListingTableUrl {
        &self.table_path
    }

    pub fn input_format(&self) -> InputFormat {
        self.format.datafusion_formats().1
    }

    pub fn register(&self, ctx: &SessionContext) {
        let store_url = self.table_path.object_store();
        ctx.register_object_store(store_url.as_ref(), Arc::clone(&self.store));
    }
}

pub fn dataset_fixture(
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> &'static Arc<DatasetFixture> {
    match (format, representation) {
        (FixtureFormat::Vortex, LocusRepresentation::ContigPosition) => &VORTEX_CONTIG_POSITION,
        (FixtureFormat::Vortex, LocusRepresentation::Packed) => &VORTEX_PACKED,
        (FixtureFormat::Parquet, LocusRepresentation::ContigPosition) => &PARQUET_CONTIG_POSITION,
        (FixtureFormat::Parquet, LocusRepresentation::Packed) => &PARQUET_PACKED,
    }
}

pub struct DiskDatasetFixture {
    format: FixtureFormat,
    table_path: String,
    _dir: tempfile::TempDir,
}

impl DiskDatasetFixture {
    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    pub fn input_format(&self) -> InputFormat {
        self.format.datafusion_formats().1
    }
}

/// Owns a private dataset fixture directory until the returned handle is dropped.
/// Set `DATAFUSION_SANDBOX_KEEP_FIXTURES=1` to retain it, including incomplete writes.
pub fn contig_position_disk_fixture(format: FixtureFormat) -> DiskDatasetFixture {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "build the dataset fixture before calling pipeline::run"
    );

    let name = match format {
        FixtureFormat::Vortex => "vortex-contig-position-",
        FixtureFormat::Parquet => "parquet-contig-position-",
    };
    let keep = match std::env::var("DATAFUSION_SANDBOX_KEEP_FIXTURES").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(std::env::VarError::NotPresent) => false,
        _ => panic!("DATAFUSION_SANDBOX_KEEP_FIXTURES must be unset, 0, or 1"),
    };
    let dir = tempfile::Builder::new()
        .prefix(name)
        .disable_cleanup(keep)
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("creating a private dataset fixture directory");
    if keep {
        eprintln!(
            "retaining dataset fixture for {}: {}",
            std::thread::current().name().unwrap_or("unnamed thread"),
            dir.path().display()
        );
    }
    let table_path = build_disk_sample_tables(
        dir.path(),
        SAMPLES,
        format,
        LocusRepresentation::ContigPosition,
    );
    DiskDatasetFixture {
        format,
        table_path,
        _dir: dir,
    }
}

fn build_in_memory_fixture(
    name: &'static str,
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> Arc<DatasetFixture> {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "build the dataset fixture before calling pipeline::run"
    );

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let table_path = ListingTableUrl::parse(format!("memory://{name}/fixtures/{name}/samples/"))
        .unwrap_or_else(|error| panic!("parsing the {name} dataset fixture path: {error}"));
    let fixture = Arc::new(DatasetFixture {
        format,
        store: Arc::clone(&store),
        table_path,
    });
    let root = fixture
        .table_path
        .as_str()
        .trim_end_matches('/')
        .to_string();
    let target = FixtureTarget::ObjectStore {
        root,
        store_url: fixture.table_path.object_store(),
        store,
    };
    build_sample_tables(target, SAMPLES, format, representation, name);
    fixture
}

enum FixtureTarget {
    Disk(String),
    ObjectStore {
        root: String,
        store_url: ObjectStoreUrl,
        store: Arc<dyn ObjectStore>,
    },
}

/// Writes one Vortex table per sample under `dir`, as several files in
/// `s=<sample>/`, with `contig` stored in every file.
/// Returns the root path the combiners read.
///
/// Every sample covers the same loci with the same alleles, so a plan that
/// de-duplicates across the sample set has something to de-duplicate.
pub fn write_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    build_disk_sample_tables(
        dir,
        sample_set,
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    )
}

pub fn write_packed_sample_tables(dir: &Path, sample_set: &[&str]) -> String {
    build_disk_sample_tables(
        dir,
        sample_set,
        FixtureFormat::Vortex,
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

fn build_disk_sample_tables(
    dir: &Path,
    sample_set: &[&str],
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> String {
    let root = dir
        .join("samples")
        .to_str()
        .expect("fixture path is valid UTF-8")
        .to_string();
    build_sample_tables(
        FixtureTarget::Disk(root),
        sample_set,
        format,
        representation,
        "disk",
    )
}

/// Writes every per-sample table in `format`, deriving every filename from it.
fn build_sample_tables(
    target: FixtureTarget,
    sample_set: &[&str],
    format: FixtureFormat,
    representation: LocusRepresentation,
    error_context: &str,
) -> String {
    let root = match &target {
        FixtureTarget::Disk(root) | FixtureTarget::ObjectStore { root, .. } => root.clone(),
    };
    let output_format = format.datafusion_formats().0;
    let sample_set = sample_set
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    pipeline::run(
        move |ctx: SessionContext| async move {
            let pipeline_root = match target {
                FixtureTarget::Disk(root) => root,
                FixtureTarget::ObjectStore {
                    root,
                    store_url,
                    store,
                } => {
                    ctx.register_object_store(store_url.as_ref(), store);
                    root
                }
            };

            let mut writes = Vec::new();
            for sample in sample_set {
                for &(contig, filename) in CONTIG_FILES {
                    let path = format!(
                        "{pipeline_root}/s={sample}/{filename}.{}",
                        output_format.extension()
                    );
                    let df = ctx.read_batch(sample_batch(contig, representation))?;
                    writes.push(async move { output_format.write(df, &path).await });
                }
            }
            join_all(writes)
                .await
                .into_iter()
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            Ok(())
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| panic!("writing the {error_context} dataset fixture: {error}"));
    root
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
