use datafusion::{
    common::file_options::parquet_writer,
    datasource::file_format::{
        FileFormat, FileFormatFactory,
        parquet::{ParquetFormat, ParquetFormatFactory},
    },
    error::{DataFusionError, Result},
};
use std::{collections::HashMap, fmt, sync::Arc};
use vortex::{VortexSessionDefault, session::VortexSession};
use vortex_datafusion::{VortexFormat, VortexFormatFactory};

#[derive(Debug)]
pub struct InputFormat(InputRepr);

#[derive(Debug)]
enum InputRepr {
    Parquet,
    Vortex,
}

impl InputFormat {
    pub const PARQUET: Self = Self(InputRepr::Parquet);
    pub const VORTEX: Self = Self(InputRepr::Vortex);

    pub fn read_format(&self) -> Arc<dyn FileFormat> {
        match self.0 {
            InputRepr::Parquet => Arc::new(ParquetFormat::default()),
            InputRepr::Vortex => Arc::new(VortexFormat::new(VortexSession::default())),
        }
    }
}

#[derive(Debug)]
pub struct OutputFormat(OutputRepr);

#[derive(Debug)]
enum OutputRepr {
    Parquet { compression: Option<String> },
    Vortex { compact: Option<bool> },
}

impl OutputFormat {
    pub const PARQUET: Self = Self(OutputRepr::Parquet { compression: None });
    pub const VORTEX: Self = Self(OutputRepr::Vortex { compact: None });

    /// Applies `compression`, failing when this format does not accept it.
    pub fn with_compression(mut self, compression: &str) -> Result<Self> {
        match &mut self.0 {
            OutputRepr::Parquet {
                compression: parquet_compression,
            } => {
                // DataFusion 55's parser assumes anything after `(` ends with `)` and removes the
                // final byte. An input such as `gzip(` leaves an empty suffix, so the parser
                // panics instead of returning a configuration error.
                if !has_parquet_compression_syntax(compression)
                    || parquet_writer::parse_compression_string(compression).is_err()
                {
                    return Err(unrecognized_compression(compression, "parquet"));
                }
                *parquet_compression = Some(compression.to_string());
            }
            OutputRepr::Vortex { compact } => {
                *compact = Some(match compression {
                    "standard" => false,
                    "compact" => true,
                    _ => return Err(unrecognized_compression(compression, "vortex")),
                });
            }
        }
        Ok(self)
    }

    pub fn extension(&self) -> &'static str {
        match self.0 {
            OutputRepr::Parquet { .. } => "parquet",
            OutputRepr::Vortex { .. } => "vortex",
        }
    }

    pub(crate) fn output_factory(&self) -> Arc<dyn FileFormatFactory> {
        match self.0 {
            OutputRepr::Parquet { .. } => Arc::new(ParquetFormatFactory::new()),
            OutputRepr::Vortex { .. } => Arc::new(VortexFormatFactory::new()),
        }
    }

    pub(crate) fn format_options(&self) -> HashMap<String, String> {
        match &self.0 {
            OutputRepr::Parquet { compression: None } | OutputRepr::Vortex { compact: None } => {
                HashMap::new()
            }
            OutputRepr::Parquet {
                compression: Some(compression),
            } => HashMap::from([("format.compression".to_string(), compression.clone())]),
            OutputRepr::Vortex {
                compact: Some(compact),
            } => HashMap::from([(
                "format.use_compact_encodings".to_string(),
                compact.to_string(),
            )]),
        }
    }
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.extension())
    }
}

fn has_parquet_compression_syntax(compression: &str) -> bool {
    let compression = compression.to_ascii_lowercase();
    if ["uncompressed", "snappy", "lz4", "lz4_raw"].contains(&compression.as_str()) {
        return true;
    }
    ["gzip", "brotli", "zstd"].into_iter().any(|codec| {
        compression
            .strip_prefix(codec)
            .and_then(|suffix| suffix.strip_prefix('('))
            .and_then(|level| level.strip_suffix(')'))
            .is_some_and(|level| !level.is_empty() && level.chars().all(|c| c.is_ascii_digit()))
    })
}

fn unrecognized_compression(compression: &str, output_format: &str) -> DataFusionError {
    DataFusionError::Configuration(format!(
        "compression '{compression}' is not recognized for output format '{output_format}'"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_parquet_compression_modes_to_their_writer_options() {
        for compression in [
            "uncompressed",
            "snappy",
            "gzip(6)",
            "brotli(5)",
            "lz4",
            "zstd(7)",
            "lz4_raw",
        ] {
            assert_eq!(
                OutputFormat::PARQUET
                    .with_compression(compression)
                    .unwrap()
                    .format_options(),
                std::collections::HashMap::from([(
                    "format.compression".to_string(),
                    compression.to_string(),
                )]),
            );
        }
    }

    #[test]
    fn maps_vortex_compression_modes_to_compact_encodings() {
        for (compression, use_compact_encodings) in [("standard", "false"), ("compact", "true")] {
            assert_eq!(
                OutputFormat::VORTEX
                    .with_compression(compression)
                    .unwrap()
                    .format_options(),
                std::collections::HashMap::from([(
                    "format.use_compact_encodings".to_string(),
                    use_compact_encodings.to_string(),
                )]),
            );
        }
    }
}
