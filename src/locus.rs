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
    #[must_use]
    pub fn locus() -> Self {
        Self(vec![Component::Locus])
    }

    /// Orders rows by locus, then alleles.
    #[must_use]
    pub fn locus_then_alleles() -> Self {
        Self(vec![Component::Locus, Component::Alleles])
    }

    /// Whether this ordering is a prefix of `other`.
    #[must_use]
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        other.0.starts_with(&self.0)
    }

    /// Expands this declaration into stored fields under `representation`.
    #[must_use]
    pub fn expand(&self, representation: LocusRepresentation) -> StoredOrdering {
        StoredOrdering {
            ordering: self.clone(),
            representation,
        }
    }
}

/// A locus ordering expanded into the fields used by one stored representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredOrdering {
    ordering: LocusOrdering,
    representation: LocusRepresentation,
}

impl StoredOrdering {
    /// Sort expressions for every field in this ordering.
    #[must_use]
    pub fn sort_expressions(&self) -> Vec<SortExpr> {
        self.column_names()
            .into_iter()
            .map(|name| col(name).sort(true, false))
            .collect()
    }

    /// Expressions that partition rows by every field in this ordering.
    #[must_use]
    pub fn partition_expressions(&self) -> Vec<Expr> {
        self.column_names().into_iter().map(col).collect()
    }

    /// Names of every stored field covered by this ordering.
    #[must_use]
    pub fn column_names(&self) -> Vec<&'static str> {
        self.ordering
            .0
            .iter()
            .flat_map(|component| component.column_names(self.representation))
            .collect()
    }

    /// The stored prefix that identifies a locus without later components.
    #[must_use]
    pub fn locus_prefix(&self) -> Self {
        let components = self
            .ordering
            .0
            .iter()
            .take_while(|component| **component == Component::Locus)
            .cloned()
            .collect();
        Self {
            ordering: LocusOrdering(components),
            representation: self.representation,
        }
    }
}

impl Component {
    fn column_names(
        &self,
        representation: LocusRepresentation,
    ) -> impl Iterator<Item = &'static str> {
        let columns: &'static [&'static str] = match (self, representation) {
            (Self::Locus, LocusRepresentation::ContigPosition) => &["contig", "position"],
            (Self::Locus, LocusRepresentation::Packed) => &["locus"],
            (Self::Alleles, _) => &["alleles"],
        };
        columns.iter().copied()
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
    ///
    /// # Errors
    ///
    /// Returns an error if the schema does not contain exactly one supported locus
    /// representation or if its required fields have invalid types.
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
}

impl fmt::Display for LocusRepresentation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContigPosition => formatter.write_str("contig-position"),
            Self::Packed => formatter.write_str("packed"),
        }
    }
}
