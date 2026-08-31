//! Stored shapes for a genomic locus and the expressions they require.

use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    common::DataFusionError,
    error::Result,
    logical_expr::{Expr, SortExpr},
    prelude::col,
};
use std::fmt;

/// How one stored row records its locus.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocusRepresentation {
    /// Separate `contig` and `position` fields.
    ContigPosition,
    /// One `locus` field containing the packed coordinate.
    Packed,
}

impl LocusRepresentation {
    /// Detects the representation from the mutually exclusive stored fields.
    pub fn detect(schema: &SchemaRef) -> Result<Self> {
        let has_locus = schema.field_with_name("locus").is_ok();
        let has_contig = schema.field_with_name("contig").is_ok();
        match (has_locus, has_contig) {
            (false, true) => Ok(Self::ContigPosition),
            (true, false) => {
                let locus_type = schema.field_with_name("locus")?.data_type();
                if locus_type != &DataType::Int64 {
                    return Err(DataFusionError::Plan(format!(
                        "packed locus field must have type Int64, found {locus_type}"
                    )));
                }
                Ok(Self::Packed)
            }
            (true, true) => Err(DataFusionError::Plan(
                "could not detect locus representation: found both 'locus' and 'contig'"
                    .to_string(),
            )),
            (false, false) => Err(DataFusionError::Plan(
                "could not detect locus representation: found neither 'locus' nor 'contig'"
                    .to_string(),
            )),
        }
    }

    /// The stored ordering expressions for the locus alone.
    pub fn ordering(self) -> Vec<SortExpr> {
        match self {
            Self::ContigPosition => vec![
                col("contig").sort(true, false),
                col("position").sort(true, false),
            ],
            Self::Packed => vec![col("locus").sort(true, false)],
        }
    }

    /// Stored locus fields to retain in an output projection.
    pub fn projection_columns(self) -> &'static [&'static str] {
        match self {
            Self::ContigPosition => &["contig", "position"],
            Self::Packed => &["locus"],
        }
    }

    /// Expressions that partition a window by locus.
    pub fn window_partition(self) -> Vec<Expr> {
        match self {
            Self::ContigPosition => vec![col("contig"), col("position")],
            Self::Packed => vec![col("locus")],
        }
    }
}

impl fmt::Display for LocusRepresentation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContigPosition => formatter.write_str("contig-position"),
            Self::Packed => formatter.write_str("packed"),
        }
    }
}
