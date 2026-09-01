//! Stored shapes for a genomic locus and the expressions they require.

use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    common::DataFusionError,
    error::Result,
    logical_expr::{Expr, SortExpr},
    prelude::col,
};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Component {
    Locus,
    Alleles,
}

/// A nonempty ordering declared in locus terms rather than stored field names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocusOrdering(Vec<Component>);

impl LocusOrdering {
    /// Orders rows by locus.
    pub fn locus() -> Self {
        Self(vec![Component::Locus])
    }

    /// Orders rows by locus, then alleles.
    pub fn locus_then_alleles() -> Self {
        Self(vec![Component::Locus, Component::Alleles])
    }

    /// Whether this ordering is a prefix of `other`.
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        other.0.starts_with(&self.0)
    }

    /// Expands this declaration into a stored ordering. The component sequence
    /// remains private.
    pub fn expand(&self, representation: LocusRepresentation) -> Vec<SortExpr> {
        self.0
            .iter()
            .flat_map(|component| match component {
                Component::Locus => match representation {
                    LocusRepresentation::ContigPosition => vec![
                        col("contig").sort(true, false),
                        col("position").sort(true, false),
                    ],
                    LocusRepresentation::Packed => vec![col("locus").sort(true, false)],
                },
                Component::Alleles => vec![col("alleles").sort(true, false)],
            })
            .collect()
    }
}

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
            (false, true) => {
                if schema.field_with_name("position").is_err() {
                    return Err(DataFusionError::Plan(
                        "contig-position locus representation is missing required 'position' field"
                            .to_string(),
                    ));
                }
                Ok(Self::ContigPosition)
            }
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
