# Running on GCE

Combiner runs on GCE read their dataset from, and write their output and metrics directory to,
`gs://hail-pschultz`, which sits in `us-central1` like the VMs. A build VM publishes binaries, and
runners run them, one at a time or as a sweep.

## Bucket layout

```
combiner-bench/datasets/<name>/                     datasets, uploaded with gcloud storage rsync
combiner-bench/bin/<commit>/<family>/<profile>/     published binaries, each with a build-info.txt
combiner-bench/runs/<campaign>/                     metrics directories, with a record of each runner in runners/
scratch/<campaign>/<run-id>/                        outputs; deleted after 7 days
```

The bucket's lifecycle rules delete `scratch/` after 7 days and abort incomplete multipart uploads
after 1 day, which a stopped [writing probe](throughput-probe.md#writing-probes) leaves behind.
They were set, replacing any rules the bucket had, with
`gcloud storage buckets update gs://hail-pschultz --lifecycle-file=scripts/gce/bucket-lifecycle.json`.

## Build VM

A long-lived **build VM** compiles published binaries for any instance family, using the flags
from [`gce.py`](target-cpu.md). Its disk is only a cache: `scripts/gce/create-build-vm.sh`
creates it from nothing, and its startup script, `scripts/gce/provision.sh`, provisions it on
every boot. So deleting the VM loses nothing. It is a `c4d-standard-16`, for the fastest single
core in the serial fat-LTO step, in whichever `us-central1` zone has one, and an `n2-standard-16`
when none does. `scripts/gce/build.sh` builds a pushed commit for each family named and publishes
each binary. It refuses a commit that isn't a full hash, and a binary that already exists:

```
scripts/gce/create-build-vm.sh
ZONE=$(gcloud compute instances list --filter=name=combiner-build --format='value(zone.basename())')
gcloud compute ssh combiner-build --zone $ZONE -- \
  sudo -iu builder datafusion-sandbox/scripts/gce/build.sh COMMIT c4
gcloud compute instances stop combiner-build --zone $ZONE
```

After changing `provision.sh`, give the VM the new version with
`gcloud compute instances add-metadata combiner-build --zone $ZONE --metadata-from-file startup-script=scripts/gce/provision.sh`.

Stop the build VM at the end of a session rather than between builds: a stop takes about 20 s and
a start 10–50 s, and an idle half hour costs well under a dollar. A stopped VM keeps no capacity,
so a start can fail when its family is stocked out; then delete it and create it again, which
falls back to n2. On `c4d-standard-16`, `create-build-vm.sh` takes 2–3 minutes, returning once the
VM is provisioned with the pinned toolchain, and the first release build about 12 minutes, nearly
all of it the single-threaded fat-LTO step. So deleting the VM only pays when it would otherwise
sit unused for about a week.

The binary leaves out DataFusion's `compression` feature: under fat LTO with any AVX-512
target-cpu, its bzip2 crashes LLVM. See
[#222](https://github.com/hail-is/datafusion-sandbox/issues/222).

## One runner

A **runner** is a throwaway VM of the family a binary was built for, on the build VM's image,
which carries `gcloud`. A run needs nothing else installed:

```
gcloud compute instances create combiner-runner --zone us-central1-a --machine-type c4-standard-16 \
  --image-family debian-12 --image-project debian-cloud --boot-disk-type hyperdisk-balanced \
  --scopes cloud-platform
gcloud compute ssh combiner-runner --zone us-central1-a
# on the runner:
gcloud storage cp gs://hail-pschultz/combiner-bench/bin/COMMIT/c4/release/datafusion-sandbox .
chmod +x datafusion-sandbox
SPLIT_POINTS=$(./datafusion-sandbox balance-split-points \
  gs://hail-pschultz/combiner-bench/datasets/vortices_chr22_nomindp/s=HG00187 --intervals 16)
./datafusion-sandbox combine-refs gs://hail-pschultz/combiner-bench/datasets/vortices_chr22_nomindp \
  --formulation interval-merge --split-points "$SPLIT_POINTS" \
  --probe --write gs://hail-pschultz/scratch/CAMPAIGN/RUN_ID \
  --metrics gs://hail-pschultz/combiner-bench/runs/CAMPAIGN --run-id RUN_ID
# back on your machine:
gcloud compute instances delete combiner-runner --zone us-central1-a
gcloud storage rsync -r gs://hail-pschultz/combiner-bench/runs/CAMPAIGN data/CAMPAIGN
```

## Sweeps

A sweep instead runs a **cell list** on runners that delete themselves.
`scripts/gce/launch-runners.sh` creates one runner per runner shape and repetition, trying each
`us-central1` zone in turn on a stockout, and returns once all are created. A shape is a machine
type and a threads per core of 1 or 2, and `REPETITIONS` lists the repetitions to launch, `1` by
default. Each runner's startup script, `scripts/gce/runner.sh`, downloads the binary published for
the commit and the shape's family, runs every cell once in a shuffled order, and deletes the
runner, after a failure too. A cell is one line of the cell list, a name and then `combine-refs`
arguments without quoting; lines starting with `#` are skipped. The script adds `--metrics`,
`--run-id`, and a `--write` path under `scratch/CAMPAIGN/`, so a cell with `--probe` is a written
probe and one without is a measured write:

```
cat >cells.txt <<'EOF'
pre-vortices_prod_10s_1f-b50 gs://hail-pschultz/combiner-bench/datasets/vortices_prod_10s_1f --threads 1 --probe --shadow
pre-vortices_prod_10s_1f-b50-ord gs://hail-pschultz/combiner-bench/datasets/vortices_prod_10s_1f --threads 1 --probe --shadow --preserve-sample-ordering
EOF
REPETITIONS="1 2 3" scripts/gce/launch-runners.sh COMMIT CAMPAIGN cells.txt c4-standard-2:1 c4-standard-2:2
```

A runner is named `<machine type>-tpc<threads per core>-r<repetition>`, its instance
`CAMPAIGN-<runner>`, and each of its runs `<cell>-<runner>`, such as
`pre-vortices_prod_10s_1f-b50-c4-standard-2-tpc2-r1`. The run record has no column for the
runner shape, so each runner also records itself in `combiner-bench/runs/CAMPAIGN/runners/`:
`<runner>.json` holds its machine type, threads per core, zone, CPU platform, memory, the memory
limit its cells ran under, shuffled cell order, each cell's exit status and duration, and a
`status` of `running`, `done`, `failed`, or `interrupted`, and `<runner>.log` holds the script's
output. Each cell runs in a cgroup whose `MemoryMax` leaves 512 MiB of the runner's memory to
everything else. Without it, a cell just short of the runner's memory left it thrashing, out of
reach by SSH, with no OOM kill. A cell that fails, which a run out of memory does with status
137, does not stop the cells after it; `failed` means the script itself
failed, and `interrupted` that the runner rebooted part way and stopped. A runner still `running`
with no instance left was deleted by its maximum run duration, `MAX_RUN_DURATION`, `24h` by
default. The launcher refuses a runner that already has a record, since its run ids would
be taken. Watch a runner with
`gcloud compute instances get-serial-port-output CAMPAIGN-<runner> --zone ZONE`.

## Sweep reports

Once a campaign's runners are done, `uv run --directory python hailtools sweep-report CAMPAIGN`
after the `rsync` [above](#one-runner) prints a Markdown table with a row per cell and runner
shape. It gives the mean over repetitions, ± half their range, of the whole-run and steady-state
normalized throughput in millions of rows per second, their ratio, and the largest peak RSS. A
second table gives the same figures for each repetition. It joins run records to runner records
by run id, and lists after the tables every cell that failed, left no run record, or left a run
record whose `status` is not `completed`, and every runner whose status is not `done`. A relative path is under `data/`.

## New families

On the first runner of each new family, also check that the family's computed CPU features are
really there. `gce.py verify` asks rustc for them, so it needs a minimal stable toolchain and the
script from the commit the binary was built from:

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. ~/.cargo/env
curl -sSfO https://raw.githubusercontent.com/hail-is/datafusion-sandbox/COMMIT/python/src/hailtools/gce.py
python3 gce.py verify c4
```

Families can be out of stock in every `us-central1` zone at once: c4d was, both 16 and 8 vCPUs,
when this was first run. Try each zone, then fall back to another family.
