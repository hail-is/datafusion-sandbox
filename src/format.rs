//! Supported input and output file formats, their options, and file naming.

use datafusion::{
    catalog::Session,
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

#[derive(Clone, Debug)]
pub struct InputFormat(InputRepr);

#[derive(Clone, Debug)]
enum InputRepr {
    Parquet,
    Vortex,
}

impl InputFormat {
    pub const PARQUET: Self = Self(InputRepr::Parquet);
    pub const VORTEX: Self = Self(InputRepr::Vortex);

    /// The name of this format: `parquet` or `vortex`.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self.0 {
            InputRepr::Parquet => "parquet",
            InputRepr::Vortex => "vortex",
        }
    }

    /// The `DataFusion` reader for files in this format.
    #[must_use]
    pub fn read_format(&self) -> Arc<dyn FileFormat> {
        match self.0 {
            InputRepr::Parquet => Arc::new(ParquetFormat::default()),
            InputRepr::Vortex => Arc::new(VortexFormat::new(VortexSession::default())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OutputFormat(OutputRepr);

#[derive(Clone, Debug)]
enum OutputRepr {
    Parquet { compression: Option<String> },
    Vortex { compact: Option<bool> },
}

impl OutputFormat {
    pub const PARQUET: Self = Self(OutputRepr::Parquet { compression: None });
    pub const VORTEX: Self = Self(OutputRepr::Vortex { compact: None });

    /// Applies `compression`, failing when this format does not accept it.
    ///
    /// # Errors
    ///
    /// Returns an error if `compression` is invalid for this output format.
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

    /// The name of this format: `parquet` or `vortex`.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self.0 {
            OutputRepr::Parquet { .. } => "parquet",
            OutputRepr::Vortex { .. } => "vortex",
        }
    }

    /// The compression mode this format writes with, as a caller would spell it, or `None` when
    /// the format's default applies.
    #[must_use]
    pub fn compression(&self) -> Option<&str> {
        match &self.0 {
            OutputRepr::Parquet { compression } => compression.as_deref(),
            OutputRepr::Vortex { compact } => {
                compact.map(|compact| if compact { "compact" } else { "standard" })
            }
        }
    }

    /// The extension of every file this format writes. It carries no compression suffix because
    /// neither format here has one; a format that does would add it here, and every path named
    /// from it would follow.
    #[must_use]
    pub const fn extension(&self) -> &'static str {
        match self.0 {
            OutputRepr::Parquet { .. } => "parquet",
            OutputRepr::Vortex { .. } => "vortex",
        }
    }

    /// The path of the file holding partition `index` of `count` when this format writes a
    /// directory at `directory` with one file per partition: the index zero-padded to
    /// the digit count of `count`, with this format's extension. The write names its files
    /// through this function, so a caller predicting them names the same files.
    #[must_use]
    pub fn partition_file_path(&self, directory: &str, index: usize, count: usize) -> String {
        let width = count.to_string().len();
        format!(
            "{}/{index:0width$}.{}",
            directory.trim_end_matches('/'),
            self.extension()
        )
    }

    /// The `DataFusion` format that writes files in this format on `state`, with this format's
    /// options applied over the session's defaults.
    pub(crate) fn write_format(&self, state: &dyn Session) -> Result<Arc<dyn FileFormat>> {
        let factory: Arc<dyn FileFormatFactory> = match self.0 {
            OutputRepr::Parquet { .. } => Arc::new(ParquetFormatFactory::new()),
            OutputRepr::Vortex { .. } => Arc::new(VortexFormatFactory::new()),
        };
        factory.create(state, &self.format_options())
    }

    fn format_options(&self) -> HashMap<String, String> {
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
        formatter.write_str(self.name())
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
    //! These tests need private access to writer-option mapping until it moves behind an output-format configuration surface.

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
