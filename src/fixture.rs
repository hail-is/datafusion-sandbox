//! Small stored datasets laid out like the real datasets.
//!
//! Shared in-memory inventory:
//! - Vortex with contig-position loci
//! - Single-file Vortex with contig-position loci and no `alleles` field
//! - Vortex with packed loci
//! - Parquet with contig-position loci
//! - Parquet with packed loci
//!
//! Per-test owned disk inventory:
//! - Vortex with contig-position loci
//! - Vortex with packed loci
//! - Parquet with contig-position loci
//! - Parquet with packed loci
//!
//! Except for the single-file no-alleles dataset, each sample has eight rows in
//! four files, listed here in locus-then-alleles order. Names sort in reverse.
//!
//! | File stem | Rows as contig, position, alleles |
//! | --- | --- |
//! | d | chr1, 1, A,G; chr1, 2, A,C |
//! | c | chr1, 2, A,G; chr1, 3, A,C |
//! | b | chr1, 4, A,G; chr2, 1, A,C |
//! | a | chr2, 2, A,G; chr2, 3, A,C |
//!
//! The d/c cut splits the alleles at chr1:2. The c/b and b/a cuts fall between
//! positions within a contig. File b spans contigs; d, c, and a have constant
//! contigs. Packed loci use the contig ordinal in the high 32 bits.
//!
//! Store handles:
//! - `MemoryStore`, an empty in-memory object store a test registers on its own
//!   session. Every shared dataset fixture holds one. The sorted-table planning
//!   tests build metadata-only tables over one with nothing in it.
//!
//! Row helpers, for tests that filter a fixture and check what comes back:
//! - `sample_rows`, the rows above as one sample's expected result.
//! - `contig_filter`, a whole-contig restriction written in a representation.
//!   Interval restrictions are `LocusInterval::filter` in the library.
//! - `decode_loci` and `string_column`, the contig and position or one string
//!   field of each row a plan returned.

#![expect(
    clippy::as_conversions,
    reason = "test fixtures cast values whose ranges are controlled by the test"
)]
#![expect(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "invalid fixture definitions and setup failures are test harness bugs"
)]

mod memory_store;

pub use memory_store::MemoryStore;

use crate::format::{InputFormat, OutputFormat};
use crate::locus::{Locus, LocusInterval, LocusRepresentation};
use crate::pipeline::{self, PipelineOptions};
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int32Array, Int64Array, StringArray},
        compute::cast,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::listing::ListingTableUrl,
    logical_expr::Expr,
    prelude::*,
};
use futures::{TryStreamExt, future::join_all};
use object_store::{ObjectMeta, ObjectStore};

use std::{
    future::Future,
    path::Path,
    sync::{Arc, LazyLock},
};

/// Runs a future for a test that only builds plans and never executes one. Plan execution
/// belongs in the pipeline runner; see ADR 0006.
///
/// # Panics
///
/// Panics if Tokio cannot build the test runtime.
pub fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("building a current-thread runtime for a planning-only test")
        .block_on(future)
}

/// Four samples from the `1kg_chr22` benchmark dataset, the most any test needs.
pub const SAMPLES: &[&str] = &["HG00308", "HG00592", "HG02230", "NA18534"];

/// One fixture row as contig, position, and alleles.
pub type SampleRow = (&'static str, i32, &'static str);

/// Files and their rows in locus-then-alleles order, shared by memory and disk.
const SAMPLE_FILES: &[(&str, [SampleRow; 2])] = &[
    ("d", [("chr1", 1, "A,G"), ("chr1", 2, "A,C")]),
    ("c", [("chr1", 2, "A,G"), ("chr1", 3, "A,C")]),
    ("b", [("chr1", 4, "A,G"), ("chr2", 1, "A,C")]),
    ("a", [("chr2", 2, "A,G"), ("chr2", 3, "A,C")]),
];

#[derive(Clone, Copy, Debug)]
pub enum FixtureFormat {
    Parquet,
    Vortex,
}

impl FixtureFormat {
    const fn datafusion_formats(self) -> (&'static OutputFormat, InputFormat) {
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
static VORTEX_WITHOUT_ALLELES: LazyLock<Arc<DatasetFixture>> =
    LazyLock::new(|| build_in_memory_fixture_without_alleles("vortex-no-alleles"));
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
    representation: LocusRepresentation,
    store: MemoryStore,
    table_path: ListingTableUrl,
}

impl DatasetFixture {
    #[must_use]
    pub const fn table_path(&self) -> &ListingTableUrl {
        &self.table_path
    }

    #[must_use]
    pub const fn input_format(&self) -> InputFormat {
        self.format.datafusion_formats().1
    }

    /// The locus representation the fixture's rows were written in.
    #[must_use]
    pub const fn representation(&self) -> LocusRepresentation {
        self.representation
    }

    /// Registers the fixture's store on the session.
    pub fn register(&self, ctx: &SessionContext) {
        self.store.register(ctx);
    }

    /// The store holding the fixture's files.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        self.store.store()
    }

    /// One sample's files in path order, for tests that build a table from the
    /// listing rather than through dataset discovery.
    ///
    /// Path order is the reverse of locus order in every fixture here, so a caller
    /// that observes locus order has watched the table reorder the files.
    ///
    /// # Panics
    ///
    /// Panics if listing fails or the sample has no files. Every fixture sample has at least one,
    /// so an unknown sample id is a test bug.
    pub async fn sample_files(&self, sample: &str) -> Vec<ObjectMeta> {
        let prefix =
            object_store::path::Path::from(format!("{}/s={sample}", self.table_path.prefix()));
        let mut files: Vec<ObjectMeta> = self
            .store()
            .list(Some(&prefix))
            .try_collect()
            .await
            .unwrap_or_else(|error| {
                panic!("listing the files of fixture sample {sample}: {error}")
            });
        files.sort_by(|left, right| left.location.cmp(&right.location));
        assert!(!files.is_empty(), "fixture sample {sample} has no files");
        files
    }
}

#[must_use]
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

#[must_use]
pub fn vortex_without_alleles_fixture() -> &'static Arc<DatasetFixture> {
    &VORTEX_WITHOUT_ALLELES
}

pub struct DiskDatasetFixture {
    format: FixtureFormat,
    table_path: String,
    _dir: tempfile::TempDir,
}

impl DiskDatasetFixture {
    #[must_use]
    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    #[must_use]
    pub const fn input_format(&self) -> InputFormat {
        self.format.datafusion_formats().1
    }
}

/// Owns a private dataset fixture directory until the returned handle is dropped.
/// Set `DATAFUSION_SANDBOX_KEEP_FIXTURES=1` to retain it, including incomplete writes.
#[must_use]
pub fn contig_position_disk_fixture(format: FixtureFormat) -> DiskDatasetFixture {
    let name = match format {
        FixtureFormat::Vortex => "vortex-contig-position-",
        FixtureFormat::Parquet => "parquet-contig-position-",
    };
    build_disk_fixture(name, format, LocusRepresentation::ContigPosition)
}

#[must_use]
pub fn packed_disk_fixture(format: FixtureFormat) -> DiskDatasetFixture {
    let name = match format {
        FixtureFormat::Vortex => "vortex-packed-",
        FixtureFormat::Parquet => "parquet-packed-",
    };
    build_disk_fixture(name, format, LocusRepresentation::Packed)
}

fn build_disk_fixture(
    name: &str,
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> DiskDatasetFixture {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "build the dataset fixture before calling pipeline::run"
    );

    let keep = match std::env::var("DATAFUSION_SANDBOX_KEEP_FIXTURES").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(std::env::VarError::NotPresent) => false,
        _ => panic!("DATAFUSION_SANDBOX_KEEP_FIXTURES must be unset, 0, or 1"),
    };
    let dir = tempfile::Builder::new()
        .prefix(name)
        .disable_cleanup(keep)
        .tempdir()
        .expect("creating a private dataset fixture directory");
    if keep {
        eprintln!(
            "retaining dataset fixture for {}: {}",
            std::thread::current().name().unwrap_or("unnamed thread"),
            dir.path().display()
        );
    }
    let table_path = build_disk_sample_tables(dir.path(), SAMPLES, format, representation);
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
    let (fixture, target) = new_in_memory_fixture(name, format, representation);
    build_sample_tables(target, SAMPLES, format, representation, name);
    fixture
}

fn build_in_memory_fixture_without_alleles(name: &'static str) -> Arc<DatasetFixture> {
    let (fixture, target) = new_in_memory_fixture(
        name,
        FixtureFormat::Vortex,
        LocusRepresentation::ContigPosition,
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("contig", Arc::new(StringArray::from(vec!["chr1"])) as _),
        ("position", Arc::new(Int32Array::from(vec![1])) as _),
    ])
    .expect("no-alleles fixture batch matches its schema");

    pipeline::run(
        move |ctx| {
            let root = target.register(&ctx);
            async move {
                let path = format!("{root}/s=sample-a/a.vortex");
                let df = ctx.read_batch(batch)?;
                OutputFormat::VORTEX.write(df, &path).await?;
                Ok(())
            }
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| panic!("writing the {name} dataset fixture: {error}"));
    fixture
}

fn new_in_memory_fixture(
    name: &'static str,
    format: FixtureFormat,
    representation: LocusRepresentation,
) -> (Arc<DatasetFixture>, FixtureTarget) {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "build the dataset fixture before calling pipeline::run"
    );

    let store = MemoryStore::new(name);
    // An `ObjectStoreUrl` always ends in the root slash.
    let table_path =
        ListingTableUrl::parse(format!("{}fixtures/{name}/samples/", store.url().as_str()))
            .unwrap_or_else(|error| panic!("parsing the {name} dataset fixture path: {error}"));
    let root = table_path.as_str().trim_end_matches('/').to_string();
    let fixture = Arc::new(DatasetFixture {
        format,
        representation,
        store: store.clone(),
        table_path,
    });
    (fixture, FixtureTarget::Memory { root, store })
}

enum FixtureTarget {
    Disk(String),
    Memory { root: String, store: MemoryStore },
}

impl FixtureTarget {
    fn register(self, ctx: &SessionContext) -> String {
        match self {
            Self::Disk(root) => root,
            Self::Memory { root, store } => {
                store.register(ctx);
                root
            }
        }
    }
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
    let output_format = format.datafusion_formats().0;
    let sample_set = sample_set
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    pipeline::run(
        move |ctx: SessionContext| async move {
            let pipeline_root = target.register(&ctx);
            let mut writes = Vec::new();
            for sample in sample_set {
                for (filename, rows) in SAMPLE_FILES {
                    let path = format!(
                        "{pipeline_root}/s={sample}/{filename}.{}",
                        output_format.extension()
                    );
                    let df = ctx.read_batch(sample_batch(rows, representation))?;
                    writes.push(async move { output_format.write(df, &path).await });
                }
            }
            join_all(writes)
                .await
                .into_iter()
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            Ok(pipeline_root)
        },
        PipelineOptions {
            threads: 1,
            ..Default::default()
        },
    )
    .unwrap_or_else(|error| panic!("writing the {error_context} dataset fixture: {error}"))
}

fn sample_batch(rows: &[(&str, i32, &str)], representation: LocusRepresentation) -> RecordBatch {
    let alleles = StringArray::from_iter_values(rows.iter().map(|&(_, _, alleles)| alleles));
    let (fields, columns): (Vec<Field>, Vec<ArrayRef>) = match representation {
        LocusRepresentation::ContigPosition => {
            let contigs = StringArray::from_iter_values(rows.iter().map(|&(contig, _, _)| contig));
            let positions =
                Int32Array::from_iter_values(rows.iter().map(|&(_, position, _)| position));
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
            let loci = Int64Array::from_iter_values(
                rows.iter()
                    .map(|&(contig, position, _)| packed_locus(contig, position)),
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

/// One sample's rows in locus-then-alleles order, as contig, position, and alleles.
///
/// Every sample in a fixture holds these same rows, so a filtered plan's result over one sample
/// is the rows here that satisfy its filter.
#[must_use]
pub fn sample_rows() -> Vec<SampleRow> {
    SAMPLE_FILES
        .iter()
        .flat_map(|(_, rows)| rows.iter().copied())
        .collect()
}

/// The rows on `contig`, written in `representation`: an equality on `contig`, or under packed
/// the locus interval from the contig's first position to the next contig's.
///
/// # Panics
///
/// Panics if `contig` is not a contig name the library can read an ordinal from.
#[must_use]
pub fn contig_filter(representation: LocusRepresentation, contig: &str) -> Expr {
    match representation {
        LocusRepresentation::ContigPosition => col("contig").eq(lit(contig)),
        LocusRepresentation::Packed => {
            let first = Locus::from_contig_name(contig, 0).unwrap();
            let next_contig = first.contig_ordinal().checked_add(1).unwrap();
            LocusInterval::new(Some(first), Some(Locus::new(next_contig, 0).unwrap()))
                .unwrap()
                .filter(representation)
                .expect("a bounded interval has a filter")
        }
    }
}

/// The contig and position of each row in `batch`, read back from `representation`.
///
/// # Panics
///
/// Panics if `batch` lacks the representation's locus fields or holds them in another type.
#[must_use]
pub fn decode_loci(batch: &RecordBatch, representation: LocusRepresentation) -> Vec<(String, i32)> {
    match representation {
        LocusRepresentation::ContigPosition => {
            let positions = batch
                .column_by_name("position")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            string_column(batch, "contig")
                .into_iter()
                .zip(positions.values())
                .map(|(contig, &position)| (contig, position))
                .collect()
        }
        LocusRepresentation::Packed => {
            let loci = batch
                .column_by_name("locus")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            loci.values()
                .iter()
                .map(|&packed| {
                    let locus = Locus::from_packed(packed).unwrap();
                    (locus.contig_name(), locus.position())
                })
                .collect()
        }
    }
}

/// The values of the string column `name` in `batch`, in row order.
///
/// # Panics
///
/// Panics if `batch` has no column `name`, or it is not a string column without nulls.
#[must_use]
pub fn string_column(batch: &RecordBatch, name: &str) -> Vec<String> {
    // Parquet reads strings back as `Utf8View`; cast so one array type covers both formats.
    let values = cast(batch.column_by_name(name).unwrap(), &DataType::Utf8).unwrap();
    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
    values
        .iter()
        .map(|value| value.unwrap().to_string())
        .collect()
}

/// The packed locus of `contig:position`, under the library's packing rule.
fn packed_locus(contig: &str, position: i32) -> i64 {
    Locus::from_contig_name(contig, position).unwrap().packed()
}
