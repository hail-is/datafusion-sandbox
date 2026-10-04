#!/usr/bin/env bash
# Launches one runner for each shape named and each repetition in $REPETITIONS, each running the
# cell list at <cell list> from the binary published for <commit> and its family:
#
#   scripts/gce/launch-runners.sh <commit> <campaign> <cell list> <machine type>:<threads per core>...
#
# A runner's startup script, runner.sh, does the rest and deletes the runner when it is done; see
# there for the cell list's format and what a runner records. This returns once every runner is
# created. Before creating any, it refuses a runner that has already run, which would find its run
# ids taken. See "Running on GCE" in the README.
set -euo pipefail

BUCKET=gs://hail-pschultz
ZONES=${ZONES:-"us-central1-f us-central1-a us-central1-b us-central1-c"}
REPETITIONS=${REPETITIONS:-1}
# A runner that outlives this is deleted, so one that hangs costs no more than this.
MAX_RUN_DURATION=${MAX_RUN_DURATION:-24h}

here=$(dirname "$0")

main() {
  if [ $# -lt 4 ]; then
    echo "usage: $0 <commit> <campaign> <cell list> <machine type>:<threads per core>..." >&2
    exit 2
  fi
  local commit=$1 campaign=$2 cells=$3
  shift 3
  if ! [[ $commit =~ ^[0-9a-f]{40}$ ]]; then
    echo "launch-runners: '$commit' is not a full commit hash" >&2
    exit 2
  fi
  if ! [[ $campaign =~ ^[a-z][a-z0-9-]*$ ]]; then
    echo "launch-runners: campaign '$campaign' must be lowercase letters, digits and hyphens" >&2
    exit 2
  fi
  if ! [ -s "$cells" ]; then
    echo "launch-runners: no cell list at '$cells'" >&2
    exit 2
  fi
  local repetition
  for repetition in $REPETITIONS; do
    if ! [[ $repetition =~ ^[1-9][0-9]*$ ]]; then
      echo "launch-runners: repetition '$repetition' is not a positive integer" >&2
      exit 2
    fi
  done

  # Every runner as "<instance name> <family> <machine type> <threads per core> <repetition>",
  # checked before any is created.
  local shape machine_type threads_per_core family runner name runners=() families=" " names=" "
  for shape in "$@"; do
    if ! [[ $shape =~ ^(([a-z0-9]+)-[a-z0-9-]+):([12])$ ]]; then
      echo "launch-runners: shape '$shape' is not <machine type>:<threads per core of 1 or 2>" >&2
      exit 2
    fi
    machine_type=${BASH_REMATCH[1]}
    family=${BASH_REMATCH[2]}
    threads_per_core=${BASH_REMATCH[3]}
    if [[ $families != *" $family "* ]]; then
      families+="$family "
      if ! exists "$BUCKET/combiner-bench/bin/$commit/$family/release/datafusion-sandbox"; then
        echo "launch-runners: no $family binary is published for $commit; see build.sh" >&2
        exit 1
      fi
    fi
    for repetition in $REPETITIONS; do
      # runner.sh names the runner the same way, from what it measures.
      runner=$machine_type-tpc$threads_per_core-r$repetition
      name=$campaign-$runner
      if [ ${#name} -gt 63 ]; then
        echo "launch-runners: instance name $name is longer than GCE's 63 characters" >&2
        exit 2
      fi
      if [[ $names == *" $name "* ]]; then
        echo "launch-runners: $name is named twice" >&2
        exit 2
      fi
      names+="$name "
      if exists "$BUCKET/combiner-bench/runs/$campaign/runners/$runner.json"; then
        echo "launch-runners: runner $runner has already run in campaign $campaign" >&2
        exit 1
      fi
      runners+=("$name $family $machine_type $threads_per_core $repetition")
    done
  done

  # Names are unique only within a zone, so a runner left in one zone must not get a twin in the
  # next.
  local existing
  existing=" $(gcloud compute instances list --filter="name~^$campaign-" --format="value(name)" |
    tr '\n' ' ')"
  for runner in "${runners[@]}"; do
    read -r name _ <<<"$runner"
    if [[ $existing == *" $name "* ]]; then
      echo "launch-runners: $name already exists" >&2
      exit 1
    fi
  done

  local failed=()
  for runner in "${runners[@]}"; do
    read -r name family machine_type threads_per_core repetition <<<"$runner"
    if ! create "$name" "$commit" "$campaign" "$cells" "$family" "$machine_type" \
      "$threads_per_core" "$repetition"; then
      failed+=("$name")
    fi
  done
  if [ ${#failed[@]} -gt 0 ]; then
    echo "launch-runners: no zone of $ZONES had room for: ${failed[*]}" >&2
    exit 1
  fi
}

# Creates the runner $1 in the first zone with room for it. Returns 1 when every zone is stocked
# out, and exits on any other failure, which would fail the same way everywhere.
create() {
  local name=$1 commit=$2 campaign=$3 cells=$4 family=$5 machine_type=$6 threads_per_core=$7
  local repetition=$8 disk_type zone out
  # These families take only Hyperdisk, and the rest take Persistent Disk.
  case $family in
    c4 | c4d | n4 | n4d) disk_type=hyperdisk-balanced ;;
    *) disk_type=pd-balanced ;;
  esac
  for zone in $ZONES; do
    if out=$(gcloud compute instances create "$name" \
      --zone "$zone" \
      --machine-type "$machine_type" \
      --threads-per-core "$threads_per_core" \
      --image-family debian-12 \
      --image-project debian-cloud \
      --boot-disk-type "$disk_type" \
      --scopes cloud-platform \
      --max-run-duration "$MAX_RUN_DURATION" \
      --instance-termination-action DELETE \
      --metadata "commit=$commit,family=$family,campaign=$campaign,repetition=$repetition" \
      --metadata-from-file "startup-script=$here/runner.sh,cells=$cells" 2>&1); then
      echo "launch-runners: created $name in $zone"
      return 0
    fi
    if ! grep -q -E 'ZONE_RESOURCE_POOL_EXHAUSTED|does not have enough resources' <<<"$out"; then
      echo "$out" >&2
      exit 1
    fi
    echo "launch-runners: no $machine_type in $zone, trying the next" >&2
  done
  return 1
}

# Whether the object at $1 exists. Exits when the listing fails for any other reason than its
# absence, since a failure read as absence would let a runner run twice.
exists() {
  local out
  if out=$(gcloud storage ls "$1" 2>&1); then
    return 0
  fi
  if [[ $out == *"matched no objects"* ]]; then
    return 1
  fi
  echo "launch-runners: cannot tell whether $1 exists: $out" >&2
  exit 1
}

main "$@"
