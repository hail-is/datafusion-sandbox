#!/usr/bin/env bash
# Builds one commit for each instance family named and publishes each binary to
# $BIN/<commit>/<family>/<profile>/, beside a build-info.txt. Runs on the build VM as `builder`:
#
#   gcloud compute ssh combiner-build --zone ZONE -- sudo -iu builder \
#     datafusion-sandbox/scripts/gce/build.sh <commit> <family>...
#
# A published binary is never overwritten, so a path under $BIN names exactly one build. See
# "Running on GCE" in the README.
set -euo pipefail

BIN=${BIN:-gs://hail-pschultz/combiner-bench/bin}
PROFILE=${PROFILE:-release}

# Everything runs inside main, so bash has read the whole script before the checkout below
# replaces it.
main() {
  if [ $# -lt 2 ]; then
    echo "usage: $0 <commit> <family>..." >&2
    exit 2
  fi
  local commit=$1
  shift
  if ! [[ $commit =~ ^[0-9a-f]{40}$ ]]; then
    echo "build: '$commit' is not a full commit hash; push the commit and pass its hash" >&2
    exit 2
  fi

  wait_for_provisioning
  . ~/.cargo/env
  cd ~/datafusion-sandbox

  # Check out the commit, then hand over to its own build.sh, which may differ from this one.
  if [ "${BUILD_SH_COMMIT:-}" != "$commit" ]; then
    git fetch --quiet origin "$commit"
    git checkout --quiet --detach "$commit"
    BUILD_SH_COMMIT=$commit exec scripts/gce/build.sh "$commit" "$@"
  fi

  # Refuse before building anything, rather than after the families ahead of a published one.
  local family
  for family in "$@"; do
    if published "$BIN/$commit/$family/$PROFILE/datafusion-sandbox"; then
      echo "build: $BIN/$commit/$family/$PROFILE/ is already published, refusing to overwrite it" >&2
      exit 1
    fi
  done

  rustup toolchain install
  for family in "$@"; do
    build_family "$commit" "$family"
  done
}

# Waits for provision.sh, which runs on every boot, to finish for this boot.
wait_for_provisioning() {
  local boot tries=0
  boot=$(cat /proc/sys/kernel/random/boot_id)
  until [ "$(cat /var/lib/provision/boot-id 2>/dev/null)" = "$boot" ]; do
    if [ $tries -eq 0 ]; then
      echo "build: waiting for this boot's provisioning to finish" >&2
    fi
    tries=$((tries + 1))
    if [ $tries -gt 180 ]; then
      echo "build: provisioning has not finished after 15 minutes;" \
        "see journalctl -u google-startup-scripts" >&2
      exit 1
    fi
    sleep 5
  done
}

# Whether the object at $1 exists. Exits when the listing fails for any other reason than its
# absence, since a failure read as absence would let a build overwrite a published binary.
published() {
  local out
  if out=$(gcloud storage ls "$1" 2>&1); then
    return 0
  fi
  if [[ $out == *"matched no objects"* ]]; then
    return 1
  fi
  echo "build: cannot tell whether $1 exists: $out" >&2
  exit 1
}

build_family() {
  local commit=$1 family=$2
  local dest=$BIN/$commit/$family/$PROFILE

  local rustflags start seconds
  rustflags=$(python3 python/src/hailtools/gce.py flags "$family")
  start=$(date +%s)
  # RUSTFLAGS replaces the -Ctarget-cpu=native of .cargo/config.toml. Each family keeps its own
  # target dir, since cargo rebuilds everything whenever the flags change.
  RUSTFLAGS=$rustflags CARGO_TARGET_DIR=target-$family cargo build --quiet --profile "$PROFILE"
  seconds=$(($(date +%s) - start))

  # cargo writes the dev profile to debug/ and every other profile to a directory of its name.
  local out=target-$family/$PROFILE
  if [ "$PROFILE" = dev ]; then
    out=target-$family/debug
  fi
  {
    echo "commit: $commit"
    echo "family: $family"
    echo "profile: $PROFILE"
    echo "rustflags: $rustflags"
    echo "os: $(. /etc/os-release && echo "$PRETTY_NAME")"
    echo "image: $(metadata image)"
    echo "build machine type: $(metadata machine-type)"
    echo "built at: $(date -u +%FT%TZ)"
    echo "build seconds: $seconds"
    rustc -Vv
  } >"$out/build-info.txt"

  # The binary goes last: its presence is what marks a published binary, so a retry after an
  # upload that died part way replaces the build-info.txt it left.
  gcloud storage cp "$out/build-info.txt" "$dest/build-info.txt"
  gcloud storage cp --no-clobber "$out/datafusion-sandbox" "$dest/datafusion-sandbox"
  echo "build: published $dest/ in ${seconds}s"
}

# The last path segment of the instance's metadata entry $1.
metadata() {
  curl -sS -H 'Metadata-Flavor: Google' \
    "http://metadata.google.internal/computeMetadata/v1/instance/$1" | sed 's|.*/||'
}

main "$@"
exit
