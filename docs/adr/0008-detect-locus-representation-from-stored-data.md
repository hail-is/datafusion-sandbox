# Detect locus representation from stored data

A combiner dataset may store a locus as separate `contig` and `position` fields or as one packed
`Int64` `locus` field. The dataset detects that representation from its resolved schema. Exactly
one of `contig` and `locus` must exist. Finding both or neither is a plan error that names the two
fields.

No CLI flag, manifest, or metadata key selects the representation. The stored data is the source
of truth. This prevents a caller from pairing a directory with the wrong setting and makes a
half-converted directory fail before a reader is built.

Schema resolution now precedes layout declaration. A dataset first discovers its sample set,
resolves its schema, and detects its locus representation. The formulation then receives that
representation and declares its required locus ordering. The dataset validates the declaration
against the resolved schema before reading a sample. This preserves the ownership boundary from
[ADR 0005](0005-storage-properties-belong-to-datasets.md): formulations declare what they require,
and datasets validate stored facts.

Conversion belongs only to the Python generator. It packs the contig ordinal into the high 32 bits
and the position into the low 32 bits, drops `contig` and `position`, and writes the result to a
separate directory. Rust reads and writes the detected shape without converting it.

## Consequences

Both formulations derive their ordering, projected locus fields, and window partition fields from
one `LocusRepresentation` module. A packed run therefore keeps packed rows through its output.

Adding another representation requires an unambiguous schema marker and corresponding shape
helpers. It does not require another run setting.
