# Detect locus representation from stored data

A combiner dataset may store a locus as separate `contig` and `position` fields or as one packed
`Int64` `locus` field. The dataset detects that representation from its resolved schema. Exactly
one of `contig` and `locus` must exist. That marker chooses the representation, then every locus
field must be present with the declared Arrow type: `contig` as `Utf8View` and `position` as
`Int32`, or `locus` as `Int64`. Detection ignores nullability. A nullable locus field with no nulls
is valid stored data.

Finding both markers or neither is a plan error that names them. Finding `contig` without
`position` is a plan error that names the missing field. A type mismatch names the field and both
the expected and found types.

No CLI flag, manifest, or metadata key selects the representation. The stored data is the source
of truth. This prevents a caller from pairing a directory with the wrong setting and makes a
half-converted directory fail before a reader is built.

A formulation declares its required locus ordering without seeing the schema, because the
declaration names locus components rather than stored fields; see
[ADR 0009](0009-declare-locus-ordering-independently-of-representation.md). A dataset receives that
ordering when it is constructed, discovers its sample set, resolves its schema, detects its
representation, and expands the ordering into stored fields, rejecting any component whose column
the schema lacks. This preserves the ownership boundary from
[ADR 0005](0005-storage-properties-belong-to-datasets.md): formulations declare what they require,
and datasets validate stored facts.

`LocusRepresentation::fields` declares these types. It uses `Utf8View`, even though fixtures may
write an ordinary UTF-8 array, because that is what both readers infer: DataFusion's Parquet format
forces view types during schema inference by default, and vortex-arrow maps its UTF-8 dtype to a
view type. The dataset test
`inferred_schema_locus_fields_match_the_representation_fields` in `src/stored/tests/dataset.rs`
pins those upstream defaults for both formats and both representations.

Conversion belongs only to the Python generator. It packs the contig ordinal into the high 32 bits
and the position into the low 32 bits, drops `contig` and `position`, and writes the result to a
separate directory. Rust reads and writes the detected shape without converting one representation
into the other.

## Consequences

The dataset expands each formulation's locus ordering under its detected representation. Both
formulations derive projected locus fields and window partition fields from that same
`LocusRepresentation` module. A packed run therefore keeps packed rows through its output.

`LocusRepresentation` owns the stored locus fields and the matching row operations through
`fields`, `locus_arrays`, and `loci`. Fixtures and tests use those methods instead of restating the
shape.

Adding another representation requires an unambiguous schema marker and implementations of those
shape methods. It does not require another run setting.
