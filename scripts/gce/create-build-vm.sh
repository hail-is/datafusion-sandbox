#!/usr/bin/env bash
# Creates the build VM. Its startup script, provision.sh, provisions it on every boot, so deleting
# the VM loses nothing: running this again rebuilds it. See "Running on GCE" in the README.
set -euo pipefail

NAME=${NAME:-combiner-build}
ZONES=${ZONES:-"us-central1-f us-central1-a us-central1-b us-central1-c"}

here=$(dirname "$0")

# The build VM cross-compiles for every instance family, so only its single-core speed matters,
# for the serial fat-LTO step. c4d is fastest but often stocked out, and a slower build beats no
# build VM, so it falls back to n2. Each machine type comes with a disk type it takes: c4d takes
# only Hyperdisk, and n2 no Hyperdisk Balanced.
CANDIDATES=("c4d-standard-16 hyperdisk-balanced" "n2-standard-16 pd-balanced")

# Names are unique only within a zone, so a failed create in one zone must not make a second build
# VM in the next.
existing=$(gcloud compute instances list --filter="name=$NAME" --format="value(zone.basename())")
if [ -n "$existing" ]; then
  echo "$NAME already exists in $existing" >&2
  exit 1
fi

for candidate in "${CANDIDATES[@]}"; do
  read -r machine_type disk_type <<<"$candidate"
  for zone in $ZONES; do
    # Runners use the same image, since the binary links glibc dynamically.
    if out=$(gcloud compute instances create "$NAME" \
      --zone "$zone" \
      --machine-type "$machine_type" \
      --image-family debian-12 \
      --image-project debian-cloud \
      --boot-disk-size 100GB \
      --boot-disk-type "$disk_type" \
      --scopes cloud-platform \
      --metadata-from-file startup-script="$here/provision.sh" 2>&1); then
      echo "$out"
      echo "created $NAME as $machine_type in $zone"
      exit 0
    fi
    # Only a stockout moves on. Any other failure, such as quota or a bad flag, would fail the
    # same way everywhere, and one that left a VM behind would let the next zone make a second.
    if ! grep -q -E 'ZONE_RESOURCE_POOL_EXHAUSTED|does not have enough resources' <<<"$out"; then
      echo "$out" >&2
      exit 1
    fi
    echo "no $machine_type in $zone, trying the next" >&2
  done
done

echo "no candidate machine type is available in any of: $ZONES" >&2
exit 1
