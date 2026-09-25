# Estimate throughput by stopping full-plan runs

Comparing formulations as the per-node step of a distributed combiner needs each one's
steady-state throughput over a large matrix of parameters, and running every cell to completion
costs too much. A throughput probe runs the formulation's unchanged plan over the whole dataset,
samples how many rows its sink has received, and stops once a rule judges the estimate settled.
This is the decision from [issue #188](https://github.com/hail-is/datafusion-sandbox/issues/188),
decided in a grilling session on 2026-09-24.

## Decision

- **Stop the full plan early.** Stopping is the one way to shorten a run without changing the
  plan measured. The probe decides where warmup ends from the samples, so each run takes about
  as long as its noise requires, whatever the formulation. Rejected: benchmarking a prefix of the
  genome. A row limit becomes a fetch on every merge, a locus filter empties most of
  interval-merge's intervals, the prefix size that clears warmup is unknown and may vary across
  parameters, and a fixed prefix takes grouped-merge much longer than interval-merge at the same
  core count.
- **A rule computed from the samples alone.** MSER picks the end of warmup, and the probe stops
  once a confidence interval over the rest has been tight for several batches in a row. The rule is
  a pure function of the samples and its settings, so a recorded run replays exactly. Rejected: a
  fixed time budget, which gives a noisy configuration the same time as a quiet one and still needs
  a guess at warmup.
- **Record every progress sample.** Each run keeps its full sample series beside its run record,
  so any stopping rule can be re-judged offline without rerunning a sweep. Samples are taken
  faster than the rule batches them, and batching is exact after the fact because the counts are
  cumulative. Rejected: recording only the estimate, which ties every result to the rule's first
  tuning.

## Consequences

The estimate is a rate over whatever loci the plan reached before stopping, and it stands for the
whole run only if the work per row is the same along the genome. Shadow probes, which evaluate the
rule but run to completion, check that assumption and calibrate the rule against the full-run
rate. The rows a probe writes are incomplete, so the probe removes its output after every ending
and refuses an output path that already exists. A probe also stops at the first finished partition,
because the rate after it reflects fewer partitions running than the deployed step would see.
