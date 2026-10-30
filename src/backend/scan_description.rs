//! Scan descriptions: what a frontend plan holds in place of a scan, and the schema-only table
//! provider that holds one.
//!
//! A description names the dataset, its input format, its declared row ordering and the input
//! table a scan reads, which is enough for any backend process to reopen the scan. Its bytes are
//! the payload the backend's codec writes for the scan.

use crate::{format::InputFormat, row_ordering::RowOrdering};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::DataFusionError,
    datasource::TableType,
    error::Result,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use std::sync::Arc;

/// The bytes every scan description starts with.
const MAGIC: &[u8] = b"HAILSCAN";

/// The version of the layout [`ScanDescription::to_bytes`] writes, the byte after [`MAGIC`].
const VERSION: u8 = 1;

/// One input table of a dataset, as a scan reads it: what the frontend plans against in place of
/// the scan and the backend reopens.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ScanDescription {
    /// The dataset's path, as the dataset was opened by: a local path or a URL.
    pub dataset_path: String,
    pub input_format: InputFormat,
    /// The row ordering the dataset was opened with.
    pub row_ordering: RowOrdering,
    /// The input table's name in the dataset.
    pub input_table: String,
}

impl ScanDescription {
    /// This description's bytes: `HAILSCAN`, the layout version `1`, and then the dataset path,
    /// the input format's name, the row ordering's name and the input table's name, each as a
    /// little-endian `u32` byte length followed by its UTF-8 bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        bytes.push(VERSION);
        for field in [
            self.dataset_path.as_str(),
            self.input_format.name(),
            &self.row_ordering.to_string(),
            &self.input_table,
        ] {
            // A field longer than `u32::MAX` bytes can't be a path or a name.
            let length = u32::try_from(field.len()).unwrap_or(u32::MAX);
            bytes.extend_from_slice(&length.to_le_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        bytes
    }

    /// Reads a description back from [its bytes](Self::to_bytes).
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` are not a scan description in this layout version, or name an
    /// input format or row ordering this backend doesn't support.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let Some(rest) = bytes.strip_prefix(MAGIC) else {
            return Err(malformed("it does not start with HAILSCAN"));
        };
        let Some((&version, mut rest)) = rest.split_first() else {
            return Err(malformed("it has no layout version"));
        };
        if version != VERSION {
            return Err(malformed(&format!(
                "its layout version is {version}, and this backend reads {VERSION}"
            )));
        }
        let mut fields = Vec::with_capacity(4);
        for _ in 0..4 {
            let (field, tail) = read_field(rest)?;
            fields.push(field);
            rest = tail;
        }
        if !rest.is_empty() {
            return Err(malformed("bytes follow its last field"));
        }
        let [dataset_path, input_format, row_ordering, input_table] = fields.as_slice() else {
            return Err(malformed("it has the wrong number of fields"));
        };
        Ok(Self {
            dataset_path: (*dataset_path).to_string(),
            input_format: input_format.parse()?,
            row_ordering: row_ordering.parse()?,
            input_table: (*input_table).to_string(),
        })
    }
}

/// One length-prefixed field from the start of `bytes`, and the bytes after it.
fn read_field(bytes: &[u8]) -> Result<(&str, &[u8])> {
    let Some((length, rest)) = bytes.split_first_chunk::<4>() else {
        return Err(malformed("a field's length is cut off"));
    };
    let length = usize::try_from(u32::from_le_bytes(*length))
        .map_err(|error| malformed(&format!("a field's length is unaddressable: {error}")))?;
    let Some((field, rest)) = rest.split_at_checked(length) else {
        return Err(malformed("a field is cut off"));
    };
    let field = std::str::from_utf8(field)
        .map_err(|error| malformed(&format!("a field is not UTF-8: {error}")))?;
    Ok((field, rest))
}

fn malformed(reason: &str) -> DataFusionError {
    DataFusionError::Plan(format!("the payload is not a scan description: {reason}"))
}

/// A table provider that holds a scan description and the schema of the scan it describes.
///
/// A plan reads it as it would the input table, but it refuses to scan: only the backend reopens
/// the scan it describes.
#[derive(Debug)]
pub struct DescribedScan {
    description: ScanDescription,
    schema: SchemaRef,
}

impl DescribedScan {
    #[must_use]
    pub const fn new(description: ScanDescription, schema: SchemaRef) -> Self {
        Self {
            description,
            schema,
        }
    }

    #[must_use]
    pub const fn description(&self) -> &ScanDescription {
        &self.description
    }

    /// The schema of the scan it describes, as [`TableProvider::schema`] returns it.
    #[must_use]
    pub const fn schema_ref(&self) -> &SchemaRef {
        &self.schema
    }
}

#[async_trait]
impl TableProvider for DescribedScan {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Fails: a description is planned against, never scanned.
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(format!(
            "the scan of input table '{}' is a scan description, which only the backend opens",
            self.description.input_table
        )))
    }
}
