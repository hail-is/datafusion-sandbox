use crate::{
    format::OutputFormat,
    pipeline::{self, PipelineOptions},
    testing::{generated::make_range_table, memory_store::MemoryStore},
    write::WriteTarget,
};

use bytes::Bytes;
use datafusion::{
    arrow::array::{ArrayRef, Float64Array, RecordBatch},
    error::{DataFusionError, Result},
    parquet::{
        basic::CompressionCodec,
        file::{
            metadata::{ColumnChunkMetaData, RowGroupMetaData},
            reader::{FileReader, SerializedFileReader},
        },
    },
    prelude::{DataFrame, SessionContext},
};
use object_store::{ObjectStoreExt, path::Path};
use std::{collections::BTreeSet, sync::Arc};
use vortex::{
    VortexSessionDefault, array::stream::ArrayStreamExt, file::OpenOptionsSessionExt,
    session::VortexSession,
};

/// A Parquet file records each column chunk's codec but not its level, so the claim is that the
/// file is written with the codec its compression mode names.
#[test]
fn parquet_writes_with_the_codec_its_compression_mode_names() {
    for (compression, codec) in [
        ("uncompressed", CompressionCodec::UNCOMPRESSED),
        ("snappy", CompressionCodec::SNAPPY),
        ("gzip(6)", CompressionCodec::GZIP),
        ("brotli(5)", CompressionCodec::BROTLI),
        ("lz4", CompressionCodec::LZ4),
        ("zstd(7)", CompressionCodec::ZSTD),
        ("lz4_raw", CompressionCodec::LZ4_RAW),
    ] {
        let file = written_file(
            OutputFormat::PARQUET.with_compression(compression).unwrap(),
            |ctx| make_range_table(ctx, 1000, 128),
        );

        let reader = SerializedFileReader::new(file).unwrap();
        let codecs: Vec<_> = reader
            .metadata()
            .row_groups()
            .iter()
            .flat_map(RowGroupMetaData::columns)
            .map(ColumnChunkMetaData::compression_codec)
            .collect();
        assert!(!codecs.is_empty(), "{compression}: no column chunks");
        assert!(
            codecs.iter().all(|written| *written == codec),
            "{compression}: {codecs:?}"
        );
    }
}

/// Compact Vortex adds Pco and Zstd to the schemes the compressor may choose from, and uniformly
/// random floats are a column Pco compresses best. Standard Vortex never writes either encoding.
#[test]
fn vortex_writes_compact_encodings_only_in_compact_mode() {
    for (compression, pco_expected) in [("standard", false), ("compact", true)] {
        let file = written_file(
            OutputFormat::VORTEX.with_compression(compression).unwrap(),
            |ctx| ctx.read_batch(random_floats(16_384)),
        );

        let encodings = vortex_encodings(file);
        assert_eq!(
            encodings.contains("vortex.pco"),
            pco_expected,
            "{compression}: {encodings:?}"
        );
        if !pco_expected {
            assert!(
                !encodings.contains("vortex.zstd"),
                "{compression}: {encodings:?}"
            );
        }
    }
}

/// A one-column batch of `n_rows` uniformly distributed floats in `[0, 1)`, the same on every
/// run. A linear congruential generator supplies 52 random mantissa bits under the exponent of
/// 1.0 to make a float in `[1, 2)`, and subtracting 1.0 shifts it into `[0, 1)`.
fn random_floats(n_rows: usize) -> RecordBatch {
    let mut state = 110_u64;
    let values: ArrayRef = Arc::new(Float64Array::from_iter_values(
        std::iter::repeat_with(|| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            f64::from_bits(1.0_f64.to_bits() | (state >> 12)) - 1.0
        })
        .take(n_rows),
    ));
    RecordBatch::try_from_iter([("value", values)]).unwrap()
}

/// The IDs of every encoding in the arrays of the Vortex file `file`, read back undecoded.
fn vortex_encodings(file: Bytes) -> BTreeSet<String> {
    pipeline::run(
        |_ctx| async move {
            let array = async {
                VortexSession::default()
                    .open_options()
                    .open_buffer(file)?
                    .scan()?
                    .into_array_stream()?
                    .read_all()
                    .await
            }
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            Ok(array
                .depth_first_traversal()
                .map(|node| node.encoding_id().to_string())
                .collect())
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

/// The bytes of the one file `output_format` writes from the frame `frame` builds.
fn written_file(
    output_format: OutputFormat,
    frame: impl FnOnce(&SessionContext) -> Result<DataFrame> + Send + 'static,
) -> Bytes {
    let store = MemoryStore::new("written-file");
    let object_path = Path::from(format!("rows.{}", output_format.extension()));
    let target = WriteTarget {
        output_path: format!("{}{object_path}", store.url().as_str()),
        output_format,
    };

    pipeline::run(
        move |ctx| async move {
            store.register(&ctx);
            target.write_unordered(frame(&ctx)?).await?;
            Ok::<_, DataFusionError>(store.store().get(&object_path).await?.bytes().await?)
        },
        PipelineOptions::single_threaded(),
    )
    .unwrap()
}

#[test]
fn parquet_accepts_its_compression_modes() {
    for compression in [
        "uncompressed",
        "snappy",
        "gzip(6)",
        "brotli(5)",
        "lz4",
        "zstd(7)",
        "lz4_raw",
    ] {
        OutputFormat::PARQUET
            .with_compression(compression)
            .unwrap_or_else(|error| panic!("parquet rejected {compression}: {error}"));
    }
}

#[test]
fn vortex_accepts_its_compression_modes() {
    for compression in ["standard", "compact"] {
        OutputFormat::VORTEX
            .with_compression(compression)
            .unwrap_or_else(|error| panic!("vortex rejected {compression}: {error}"));
    }
}

#[test]
fn parquet_rejects_an_incomplete_compression_mode() {
    assert_rejects_compression(OutputFormat::PARQUET, "brotli", "parquet");
}

#[test]
fn parquet_rejects_a_malformed_compression_mode_without_panicking() {
    assert_rejects_compression(OutputFormat::PARQUET, "gzip(", "parquet");
}

#[test]
fn vortex_rejects_a_parquet_compression_mode() {
    assert_rejects_compression(OutputFormat::VORTEX, "zstd(7)", "vortex");
}

fn assert_rejects_compression(format: OutputFormat, compression: &str, format_name: &str) {
    let error = format.with_compression(compression).unwrap_err();
    let message = error.to_string();

    assert!(message.contains(compression), "got: {message}");
    assert!(message.contains(format_name), "got: {message}");
}
