# Detect locus representation from stored data

A combiner dataset may store a locus as separate `contig` and `position` fields or as one packed
`Int64` `locus` field. The dataset detects that representation from its resolved schema. Exactly
one of `contig` and `locus` must exist, and every field that representation names must be present:
`contig` with `position`, or `locus` alone as `Int64`. Finding both or neither is a plan error that
names the two fields. Finding `contig` without `position` is a plan error that names the missing
field.

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

Conversion belongs only to the Python generator. It packs the contig ordinal into the high 32 bits
and the position into the low 32 bits, drops `contig` and `position`, and writes the result to a
separate directory. Rust reads and writes the detected shape without converting it.

## Consequences

The dataset expands each formulation's locus ordering under its detected representation. Both
formulations derive projected locus fields and window partition fields from that same
`LocusRepresentation` module. A packed run therefore keeps packed rows through its output.

Adding another representation requires an unambiguous schema marker and corresponding shape
helpers. It does not require another run setting.
