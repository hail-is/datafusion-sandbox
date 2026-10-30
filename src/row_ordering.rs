//! Row orderings: orderings declared in row components, the locus and named fields such as the
//! sample, and their expansion into stored fields under a locus representation.
//!
//! An expanded ordering states itself to a sink or an ordering assertion as a plain sort list.

use crate::{locus::LocusRepresentation, sample::sample_field};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::DataFusionError,
    error::Result,
    logical_expr::{Expr, SortExpr},
    prelude::col,
};
use std::fmt;

/// One component of a row ordering: the locus, which expands under the locus representation, or a
/// field that names itself.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Component {
    Locus,
    Field(String),
}

/// A nonempty ordering declared in row components rather than stored field names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowOrdering(Vec<Component>);

impl RowOrdering {
    /// Orders rows by locus.
    #[must_use]
    pub fn locus() -> Self {
        Self(vec![Component::Locus])
    }

    /// Orders rows by locus, then alleles.
    #[must_use]
    pub fn locus_then_alleles() -> Self {
        Self(vec![
            Component::Locus,
            Component::Field("alleles".to_string()),
        ])
    }

    /// Orders rows by locus, then sample: the field every row read from a dataset carries, as
    /// [`sample_field`] defines it.
    #[must_use]
    pub fn locus_then_sample() -> Self {
        Self(vec![
            Component::Locus,
            Component::Field(sample_field().name().clone()),
        ])
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

    /// Detects the locus representation of `schema` and expands this ordering under it.
    ///
    /// # Errors
    ///
    /// Returns an error if `schema` has no supported locus representation or lacks a stored
    /// ordering column.
    pub fn validate_against(
        &self,
        schema: &SchemaRef,
    ) -> Result<(LocusRepresentation, StoredOrdering)> {
        let representation = LocusRepresentation::detect(schema)?;
        let stored_ordering = self.expand(representation);
        for column in stored_ordering.column_names() {
            if schema.field_with_name(&column).is_err() {
                return Err(DataFusionError::Plan(format!(
                    "row ordering column '{column}' is missing from the schema"
                )));
            }
        }
        Ok((representation, stored_ordering))
    }
}

/// A row ordering expanded into the fields used by one stored representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredOrdering {
    ordering: RowOrdering,
    representation: LocusRepresentation,
}

impl StoredOrdering {
    /// Sort expressions for every field in this ordering.
    #[must_use]
    pub fn sort_expressions(&self) -> Vec<SortExpr> {
        self.sort_columns()
            .iter()
            .map(SortColumn::sort_expression)
            .collect()
    }

    /// This ordering as a plain sort list: every field ascending, nulls last.
    #[must_use]
    pub fn sort_columns(&self) -> Vec<SortColumn> {
        self.column_names()
            .into_iter()
            .map(|column| SortColumn {
                column,
                ascending: true,
                nulls_first: false,
            })
            .collect()
    }

    /// Expressions that partition rows by every field in this ordering.
    #[must_use]
    pub fn partition_expressions(&self) -> Vec<Expr> {
        self.column_names().into_iter().map(col).collect()
    }

    /// Names of every stored field covered by this ordering.
    #[must_use]
    pub fn column_names(&self) -> Vec<String> {
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
            ordering: RowOrdering(components),
            representation: self.representation,
        }
    }
}

/// One entry of a plain sort list, `(column, ascending, nulls_first)`: a column by name, its
/// direction, and where its nulls sort.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortColumn {
    pub column: String,
    pub ascending: bool,
    pub nulls_first: bool,
}

impl SortColumn {
    /// The sort expression ordering rows by this column.
    #[must_use]
    pub fn sort_expression(&self) -> SortExpr {
        col(self.column.as_str()).sort(self.ascending, self.nulls_first)
    }
}

impl Component {
    fn column_names(&self, representation: LocusRepresentation) -> Vec<String> {
        match self {
            Self::Locus => representation
                .fields()
                .into_iter()
                .map(|field| field.name().clone())
                .collect(),
            Self::Field(name) => vec![name.clone()],
        }
    }
}

/// Writes the components joined by commas, the locus as `locus` and every other component by its
/// field name: `locus`, `locus,s` or `locus,alleles`.
impl fmt::Display for RowOrdering {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, component) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            match component {
                Component::Locus => formatter.write_str("locus")?,
                Component::Field(name) => formatter.write_str(name)?,
            }
        }
        Ok(())
    }
}
