# Keep terminal defaults and validation in the CLI

The CLI resolves action and format defaults, applies the twenty-row default for `--show`, and
validates output extensions and compression before it executes a combiner run. These choices depend
on arguments a person typed and the diagnostic they should see; the library accepts resolved
settings and applies no presentation defaults. A default that depends on neither, such as the
thread count, belongs to the pipeline: it states the default once and the CLI reads it from there.

## Considered option

Moving these choices into `CombinerRun` would put all row-limit behavior in one place, but it would
also make a library `Action::Collect` silently truncate its collected batches and make the library
validate a file extension against a format default it did not choose.

## Consequences

The CLI matches on its parsed action to supply its own row limit. It also rejects an invalid
compression or contradictory output extension before dataset discovery, so a bad terminal argument
does not wait for a run or get hidden by a dataset error.
