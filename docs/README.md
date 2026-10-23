# Docs

Each usage doc covers one subsystem. Read the one for what you are about to run or change:

- [Combiner runs](combiner.md): datasets and input tables, the reference combiner's
  formulations and row ordering, outputs and sample annotation tables, and measured writes.
- [Throughput probes](throughput-probe.md): estimating a plan's steady-state throughput
  without running it to the end: the stopping rule, its settings, what a probe records, and
  writing and shadow probes.
- [Calibrating the stopping rule](probe-calibration.md): shadow probes, `replay`, and the
  probe viewer.
- [Running on GCE](gce.md): the bucket, the build VM, runners, sweeps, and sweep reports.
- [Building for a GCE instance family](target-cpu.md): choosing `target-cpu` per family with
  `gce.py`, and one target dir per family.
- [Alternate allocator (snmalloc)](snmalloc.md): the `snmalloc` feature and its `CXXFLAGS`.
- [Cloud sessions](cloud-sessions.md): running an agent on this repo in a Claude Code cloud session:
  the VM, the environment and its setup script, and GitHub access from the session.
- [`hailtools`](../python/README.md): converting Hail data to Parquet and Vortex.
- [What the run metrics measure](../notes/datafusion-metrics.md): each column of the metrics table,
  read from the DataFusion source.

Design docs:

- [Modules and tests](modules-and-tests.md): how the crate is split into modules and how
  their tests are written.
- [`adr/`](adr/): the decisions behind the design, one record each.
- [`../GLOSSARY.md`](../GLOSSARY.md): the domain vocabulary these docs use.
- [`agents/`](agents/): configuration for agent skills: the issue tracker, triage labels, and
  domain docs.
