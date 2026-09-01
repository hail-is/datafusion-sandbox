# Declare locus ordering independently of the representation

A formulation declares the ordering it requires in locus terms rather than stored field names. A
locus ordering is a sequence of components, currently the locus and alleles. A dataset expands it
into a stored ordering using the representation it detected: the locus becomes `contig` then
`position`, or the packed `locus` field; alleles becomes `alleles`.

This removes a cycle that forced datasets through an invalid intermediate state. [ADR
0008](0008-detect-locus-representation-from-stored-data.md) required a dataset to resolve its schema
and detect its representation before a formulation could name the fields to sort by, so discovery
returned a dataset with an empty ordering that a later call had to replace. Nothing required that
call, and skipping it surfaced as a missing ordering in the scan provider, two modules from the
mistake. A representation-independent declaration breaks the cycle: the layout is known before
discovery starts, so a dataset takes it as a constructor argument and is valid from the moment it
exists.

It also carries ADR 0008's principle further than 0008 did. A formulation cannot name `contig` or
`locus` at all, so it cannot pair a packed directory with a contig-position ordering. The vocabulary
makes the mismatch unsayable rather than detected.

The ordering type is opaque, with named constructors and no public component sequence. An empty
ordering is not constructable and every ordering begins with the locus.

Comparing a required ordering against a declared one happens on orderings, not on their expansions.
This is sound because each component expands to a fixed, nonempty list of fields determined by the
representation alone, so one ordering is a prefix of another exactly when its expansion is a prefix
of the other's.

## Consequences

Dataset construction is one phase. A dataset either exists with a validated layout or construction
failed. The empty ordering and the layout attachment step are gone, and so is the deferred error
from the scan provider.

A dataset can be constructed from data alone, with no session and no object store, which makes the
ordering check, the sample set narrowing, and the representation accessor testable without encoding
files.

When a declared ordering eventually comes from Parquet or Vortex metadata rather than from a
formulation, that path will need to recognize a stored ordering as a locus ordering. An ordering that
matches none is one no formulation can require, so failing to recognize it is the correct outcome
rather than a gap.
