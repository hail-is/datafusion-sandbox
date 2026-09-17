//! Stored shapes for a genomic locus and the expressions they require, and the split points and
//! half-open locus intervals a caller names in locus terms.

use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array, Int64Array, StringViewArray},
        datatypes::{DataType, Field, SchemaRef},
        record_batch::RecordBatch,
    },
    common::{
        DataFusionError,
        cast::{as_int32_array, as_int64_array, as_string_view_array},
    },
    error::Result,
    logical_expr::{Expr, SortExpr},
    prelude::{col, lit},
};
use std::{fmt, str::FromStr, sync::Arc};

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
            ordering: LocusOrdering(components),
            representation: self.representation,
        }
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
            Self::Alleles => vec!["alleles".to_string()],
        }
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
    /// The stored fields that record a locus, in storage order.
    #[must_use]
    pub fn fields(self) -> Vec<Field> {
        match self {
            Self::ContigPosition => vec![
                Field::new("contig", DataType::Utf8View, false),
                Field::new("position", DataType::Int32, false),
            ],
            Self::Packed => vec![Field::new("locus", DataType::Int64, false)],
        }
    }

    /// Builds the stored locus arrays for `loci`, in the same order as [`Self::fields`].
    #[must_use]
    pub fn locus_arrays(self, loci: &[Locus]) -> Vec<ArrayRef> {
        match self {
            Self::ContigPosition => vec![
                Arc::new(StringViewArray::from_iter_values(
                    loci.iter().map(|locus| locus.contig_name()),
                )),
                Arc::new(Int32Array::from_iter_values(
                    loci.iter().map(|locus| locus.position()),
                )),
            ],
            Self::Packed => vec![Arc::new(Int64Array::from_iter_values(
                loci.iter().map(|locus| locus.packed()),
            ))],
        }
    }

    /// Reads the loci in `batch` from this representation's stored fields.
    ///
    /// # Errors
    ///
    /// Returns a plan error if a required field is absent, has the wrong type, contains a null,
    /// or holds a value that is not a locus.
    pub fn loci(self, batch: &RecordBatch) -> Result<Vec<Locus>> {
        match self {
            Self::ContigPosition => {
                let contigs = as_string_view_array(
                    self.column(batch, "contig", &DataType::Utf8View)?.as_ref(),
                )?;
                let positions =
                    as_int32_array(self.column(batch, "position", &DataType::Int32)?.as_ref())?;
                contigs
                    .iter()
                    .zip(positions)
                    .map(|(contig, position)| {
                        let contig = contig.ok_or_else(|| {
                            DataFusionError::Plan(
                                "locus field 'contig' contains a null".to_string(),
                            )
                        })?;
                        let position = position.ok_or_else(|| {
                            DataFusionError::Plan(
                                "locus field 'position' contains a null".to_string(),
                            )
                        })?;
                        Locus::from_contig_name(contig, position).map_err(|error| {
                            DataFusionError::Plan(format!(
                                "invalid contig-position locus in stored row: {error}"
                            ))
                        })
                    })
                    .collect()
            }
            Self::Packed => {
                as_int64_array(self.column(batch, "locus", &DataType::Int64)?.as_ref())?
                    .iter()
                    .map(|packed| {
                        let packed = packed.ok_or_else(|| {
                            DataFusionError::Plan("locus field 'locus' contains a null".to_string())
                        })?;
                        Locus::from_packed(packed).map_err(|error| {
                            DataFusionError::Plan(format!(
                                "invalid packed locus in stored row: {error}"
                            ))
                        })
                    })
                    .collect()
            }
        }
    }

    fn column<'a>(
        self,
        batch: &'a RecordBatch,
        name: &str,
        expected_type: &DataType,
    ) -> Result<&'a ArrayRef> {
        let column = batch.column_by_name(name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{self} locus representation is missing required '{name}' field"
            ))
        })?;
        if column.data_type() != expected_type {
            return Err(DataFusionError::Plan(format!(
                "{self} locus field '{name}' must have type {expected_type}, found {}",
                column.data_type()
            )));
        }
        Ok(column)
    }

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
                Self::ContigPosition.validate_fields(schema)?;
                Ok(Self::ContigPosition)
            }
            (true, false) => {
                Self::Packed.validate_fields(schema)?;
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

    fn validate_fields(self, schema: &SchemaRef) -> Result<()> {
        for expected in self.fields() {
            let found = schema.field_with_name(expected.name())?.data_type();
            if found != expected.data_type() {
                let message = match self {
                    Self::ContigPosition => format!(
                        "contig-position locus field '{}' must have type {}, found {found}",
                        expected.name(),
                        expected.data_type()
                    ),
                    Self::Packed => {
                        format!("packed locus field must have type Int64, found {found}")
                    }
                };
                return Err(DataFusionError::Plan(message));
            }
        }
        Ok(())
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

/// A locus named by its contig ordinal and position, as a caller writes it: `contig:position`.
///
/// Orders by contig ordinal, then position. The packed representation's numeric order follows
/// that order. The contig-position representation stores the rendered name `chr{ordinal}`, whose
/// string order diverges once ordinals have different digit counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Locus {
    contig_ordinal: u32,
    position: i32,
}

impl Locus {
    /// The prefix a contig's name puts before its ordinal.
    const CONTIG_NAME_PREFIX: &'static str = "chr";

    /// The locus at `position` on the contig with ordinal `contig_ordinal`.
    ///
    /// # Errors
    ///
    /// Returns an error if `position` is negative or `contig_ordinal` exceeds `i32::MAX`,
    /// either of which would put the packed form outside the non-negative `Int64` range.
    pub fn new(contig_ordinal: u32, position: i32) -> Result<Self> {
        if i32::try_from(contig_ordinal).is_err() {
            return Err(DataFusionError::Configuration(format!(
                "invalid locus {contig_ordinal}:{position}: contig ordinal must be at most {}",
                i32::MAX
            )));
        }
        if position < 0 {
            return Err(DataFusionError::Configuration(format!(
                "invalid locus {contig_ordinal}:{position}: position must be non-negative"
            )));
        }
        Ok(Self {
            contig_ordinal,
            position,
        })
    }

    /// The locus at `position` on the contig named `contig_name`, as the contig-position
    /// representation stores it: `chr` followed by the contig ordinal.
    ///
    /// # Errors
    ///
    /// Returns an error if `contig_name` is not `chr` followed by a contig ordinal, or if the
    /// coordinates are rejected by [`Locus::new`].
    pub fn from_contig_name(contig_name: &str, position: i32) -> Result<Self> {
        let contig_ordinal = contig_name
            .strip_prefix(Self::CONTIG_NAME_PREFIX)
            .and_then(|ordinal| ordinal.parse::<u32>().ok())
            .ok_or_else(|| {
                DataFusionError::Configuration(format!(
                    "invalid contig name '{contig_name}': expected {}{{ordinal}}",
                    Self::CONTIG_NAME_PREFIX
                ))
            })?;
        Self::new(contig_ordinal, position)
    }

    /// The contig ordinal.
    #[must_use]
    pub const fn contig_ordinal(self) -> u32 {
        self.contig_ordinal
    }

    /// The position within the contig.
    #[must_use]
    pub const fn position(self) -> i32 {
        self.position
    }

    /// The contig's name under the contig-position representation.
    #[must_use]
    pub fn contig_name(self) -> String {
        format!("{}{}", Self::CONTIG_NAME_PREFIX, self.contig_ordinal)
    }

    /// This locus as the packed representation stores it: the contig ordinal in the high 32
    /// bits and the position in the low 32.
    #[must_use]
    pub fn packed(self) -> i64 {
        (i64::from(self.contig_ordinal) << 32) | i64::from(self.position)
    }

    /// The locus a packed `locus` value stores.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is negative or its low 32 bits do not hold an `i32`
    /// position. [`Locus::packed`] never produces such a value; a stored `locus` column might.
    pub fn from_packed(value: i64) -> Result<Self> {
        let malformed = || {
            DataFusionError::Configuration(format!(
                "packed locus {value} is not a non-negative contig ordinal above an i32 position"
            ))
        };
        if value < 0 {
            return Err(malformed());
        }
        let contig_ordinal = u32::try_from(value >> 32).ok().ok_or_else(malformed)?;
        let position = i32::try_from(value & 0xffff_ffff)
            .ok()
            .ok_or_else(malformed)?;
        Self::new(contig_ordinal, position)
    }
}

impl FromStr for Locus {
    type Err = DataFusionError;

    /// Parses `contig:position` with both parts non-negative decimal integers, and nothing else:
    /// no `chr` prefix, no whitespace.
    fn from_str(text: &str) -> Result<Self> {
        let invalid = |reason: &str| {
            DataFusionError::Configuration(format!("invalid locus '{text}': {reason}"))
        };
        let (contig_ordinal, position) = text
            .split_once(':')
            .ok_or_else(|| invalid("expected contig:position"))?;
        let contig_ordinal = contig_ordinal
            .parse::<u32>()
            .ok()
            .ok_or_else(|| invalid("contig must be a non-negative integer"))?;
        let position = position
            .parse::<u32>()
            .ok()
            .and_then(|position| i32::try_from(position).ok())
            .ok_or_else(|| invalid("position must be a non-negative integer that fits an i32"))?;
        Self::new(contig_ordinal, position)
    }
}

impl fmt::Display for Locus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.contig_ordinal, self.position)
    }
}

/// The `j - 1` loci that cut the locus ordering into `j` locus intervals, strictly increasing.
///
/// Parses from and renders to a comma-separated list of `contig:position`, so a computed list
/// prints in the syntax a caller pastes back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitPoints(Vec<Locus>);

impl SplitPoints {
    /// The split points `points`, in the order given.
    ///
    /// The empty list is valid and defines the single interval covering the whole ordering.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first pair of points that is not strictly increasing.
    pub fn new(points: Vec<Locus>) -> Result<Self> {
        let out_of_order = points.windows(2).find_map(|pair| match pair {
            [earlier, later] if earlier >= later => Some((*earlier, *later)),
            _ => None,
        });
        if let Some((earlier, later)) = out_of_order {
            return Err(DataFusionError::Configuration(format!(
                "split points must be strictly increasing: {later} does not follow {earlier}"
            )));
        }
        Ok(Self(points))
    }

    /// The `j` half-open locus intervals these `j - 1` points define, in locus order: the first
    /// unbounded below, the last unbounded above.
    #[must_use]
    pub fn intervals(&self) -> Vec<LocusInterval> {
        let starts = std::iter::once(None).chain(self.0.iter().copied().map(Some));
        let ends = self
            .0
            .iter()
            .copied()
            .map(Some)
            .chain(std::iter::once(None));
        starts
            .zip(ends)
            .map(|(start, end)| LocusInterval { start, end })
            .collect()
    }
}

impl FromStr for SplitPoints {
    type Err = DataFusionError;

    /// Parses a comma-separated list of `contig:position`, rejecting the empty string and empty
    /// items.
    fn from_str(text: &str) -> Result<Self> {
        if text.is_empty() {
            return Err(DataFusionError::Configuration(
                "split points are empty: expected comma-separated contig:position".to_string(),
            ));
        }
        let points = text
            .split(',')
            .map(str::parse)
            .collect::<Result<Vec<Locus>>>()?;
        Self::new(points)
    }
}

impl fmt::Display for SplitPoints {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, point) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            write!(formatter, "{point}")?;
        }
        Ok(())
    }
}

/// A half-open locus interval: includes `start`, excludes `end`, either unbounded when absent.
///
/// Renders as `start..end` with an absent bound left blank: `1:100..2:50`, `..1:100`, `2:50..`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocusInterval {
    start: Option<Locus>,
    end: Option<Locus>,
}

impl LocusInterval {
    /// The interval from `start` inclusive to `end` exclusive.
    ///
    /// # Errors
    ///
    /// Returns an error if both bounds are present and `start` is not before `end`, which
    /// would make the interval empty or inverted.
    pub fn new(start: Option<Locus>, end: Option<Locus>) -> Result<Self> {
        if let (Some(start), Some(end)) = (start, end)
            && start >= end
        {
            return Err(DataFusionError::Configuration(format!(
                "locus interval start {start} is not before its end {end}"
            )));
        }
        Ok(Self { start, end })
    }

    /// The filter selecting this interval's rows under `representation`, or `None` when the
    /// interval is unbounded on both sides and selects every row.
    ///
    /// Under contig-position a bound is the compound `contig > c OR (contig = c AND position >= p)`
    /// (`<`, `<` at the end), so an interval may cross contigs; under packed it is a plain
    /// comparison on `locus`. Both formats push this form whole into the scan and prune files
    /// by it exactly, except that per-column statistics over-keep a file straddling a contig
    /// boundary under contig-position.
    #[must_use]
    pub fn filter(self, representation: LocusRepresentation) -> Option<Expr> {
        let lower = self.start.map(|start| match representation {
            LocusRepresentation::ContigPosition => {
                let contig = lit(start.contig_name());
                col("contig").gt(contig.clone()).or(col("contig")
                    .eq(contig)
                    .and(col("position").gt_eq(lit(start.position))))
            }
            LocusRepresentation::Packed => col("locus").gt_eq(lit(start.packed())),
        });
        let upper = self.end.map(|end| match representation {
            LocusRepresentation::ContigPosition => {
                let contig = lit(end.contig_name());
                col("contig").lt(contig.clone()).or(col("contig")
                    .eq(contig)
                    .and(col("position").lt(lit(end.position))))
            }
            LocusRepresentation::Packed => col("locus").lt(lit(end.packed())),
        });
        match (lower, upper) {
            (Some(lower), Some(upper)) => Some(lower.and(upper)),
            (Some(one), None) | (None, Some(one)) => Some(one),
            (None, None) => None,
        }
    }
}

impl fmt::Display for LocusInterval {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(start) = self.start {
            write!(formatter, "{start}")?;
        }
        formatter.write_str("..")?;
        if let Some(end) = self.end {
            write!(formatter, "{end}")?;
        }
        Ok(())
    }
}
