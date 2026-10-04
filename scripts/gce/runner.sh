#!/usr/bin/env bash
# A runner's startup script, run as root when launch-runners.sh creates the runner. It runs a cell
# list from a published binary, recording each run in its campaign's metrics directory, and then
# deletes the runner, after a failure too. Everything it needs comes as instance metadata:
#
#   commit, family  name the published binary, $BUCKET/combiner-bench/bin/<commit>/<family>/release/
#   campaign        names the metrics directory, $BUCKET/combiner-bench/runs/<campaign>/, and the
#                   directory the runs write under, $BUCKET/scratch/<campaign>/
#   repetition      which repetition of its shape the runner is
#   cells           the cell list
#
# A cell is one line of the cell list: a name, then combine-refs arguments, split on whitespace with
# no quoting. Blank lines and lines starting with # are skipped. The runner is named
# <machine type>-tpc<threads per core>-r<repetition>, and runs each cell once, in a shuffled order,
# as the run <cell>-<runner>. The script adds --metrics, --run-id, and a --write path under the
# scratch directory, so a cell with --probe is a written probe and one without is a measured write.
# A failed cell does not stop the cells after it.
#
# Beside the run records, in runners/ under the metrics directory, it writes <runner>.json,
# describing the runner, its shuffled cell order, and each cell's exit status, and <runner>.log,
# this script's output. See "Running on GCE" in the README.
set -euo pipefail

BUCKET=gs://hail-pschultz
WORK=/var/lib/runner

main() {
  # main runs in a pipeline's subshell, which inherits the set +e that lets finish run after it.
  set -euo pipefail
  echo "runner: start"

  # The record's name comes first, so that any later failure leaves a record.
  local campaign repetition machine_type threads_per_core runner records
  campaign=$(attribute campaign)
  repetition=$(attribute repetition)
  machine_type=$(metadata machine-type | sed 's|.*/||')
  threads_per_core=$(lscpu | sed -n 's/^Thread(s) per core: *//p')
  if ! [[ $threads_per_core =~ ^[0-9]+$ ]]; then
    echo "runner: lscpu reports no threads per core" >&2
    exit 1
  fi
  runner=$machine_type-tpc$threads_per_core-r$repetition
  records=$BUCKET/combiner-bench/runs/$campaign
  echo "$records/runners/$runner" >"$WORK/record"
  {
    field runner "$runner"
    field campaign "$campaign"
    field repetition "$repetition"
    field machine_type "$machine_type"
    field threads_per_core "$threads_per_core"
    field started_at "$(date -u +%FT%TZ)"
  } >>"$WORK/description"

  # Each lookup is assigned before it is used, since set -e ignores a failure inside an argument.
  local instance zone cpu_platform commit family binary
  instance=$(metadata name)
  zone=$(metadata zone | sed 's|.*/||')
  cpu_platform=$(metadata cpu-platform)
  {
    field instance "$instance"
    field zone "$zone"
    field cpu_platform "$cpu_platform"
  } >>"$WORK/description"
  commit=$(attribute commit)
  family=$(attribute family)
  binary=$BUCKET/combiner-bench/bin/$commit/$family/release/datafusion-sandbox
  {
    field commit "$commit"
    field family "$family"
    field binary "$binary"
  } >>"$WORK/description"

  attribute cells | read_cells | shuf >"$WORK/cells"
  cut -d ' ' -f 1 "$WORK/cells" >"$WORK/order"
  render_record running >"$WORK/runner.json"
  gcloud storage cp "$WORK/runner.json" "$records/runners/$runner.json"

  gcloud storage cp "$binary" "$WORK/datafusion-sandbox"
  chmod +x "$WORK/datafusion-sandbox"

  local line words cell run_id start status
  while IFS= read -r line; do
    read -r -a words <<<"$line"
    cell=${words[0]}
    run_id=$cell-$runner
    echo "runner: running $run_id"
    start=$(date +%s)
    if "$WORK/datafusion-sandbox" combine-refs "${words[@]:1}" \
      --metrics "$records" --run-id "$run_id" \
      --write "$(write_path "$BUCKET/scratch/$campaign/$run_id" "${words[@]:1}")" </dev/null; then
      status=0
    else
      status=$?
    fi
    echo "runner: $run_id exited with status $status"
    printf '%s\t%s\t%s\t%s\n' "$cell" "$run_id" "$status" $(($(date +%s) - start)) >>"$WORK/outcomes"
  done <"$WORK/cells"

  echo "runner: done"
}

# The cell list on stdin without its blank and comment lines, one space between words. Exits on a
# cell name that cannot be part of a run id, or that names two cells.
read_cells() {
  local line words names=" "
  # The second test keeps a last line that has no newline.
  while IFS= read -r line || [ -n "$line" ]; do
    read -r -a words <<<"$line"
    if [ ${#words[@]} -eq 0 ] || [[ ${words[0]} == \#* ]]; then
      continue
    fi
    if ! [[ ${words[0]} =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
      echo "runner: '${words[0]}' is not a cell name" >&2
      exit 1
    fi
    if [[ $names == *" ${words[0]} "* ]]; then
      echo "runner: the cell list names ${words[0]} twice" >&2
      exit 1
    fi
    names+="${words[0]} "
    echo "${words[*]}"
  done
  if [ "$names" = " " ]; then
    echo "runner: the cell list has no cells" >&2
    exit 1
  fi
}

# The --write path for a run writing under directory $1 with combine-refs arguments $2...: a
# directory in it for the interval-merge formulation, which writes one file per partition, and
# otherwise a file in it with the output format's extension. combine-refs requires the extension of
# the file and rejects any of the directory, so neither may take its name from a run id, which can
# hold a dot.
write_path() {
  local dir=$1 formulation=union input_format=vortex output_format=
  shift
  while [ $# -gt 0 ]; do
    case $1 in
      --formulation=*) formulation=${1#*=} ;;
      --input-format=*) input_format=${1#*=} ;;
      --output-format=*) output_format=${1#*=} ;;
      --formulation) formulation=${2:-} ;;
      --input-format) input_format=${2:-} ;;
      --output-format) output_format=${2:-} ;;
    esac
    shift
  done
  if [ "$formulation" = interval-merge ]; then
    echo "$dir/combined"
  else
    echo "$dir/combined.${output_format:-$input_format}"
  fi
}

# Uploads the runner's record, with the status $1 and main's exit status $2 when it failed, and
# its log, then deletes the runner.
finish() {
  local record
  if record=$(cat "$WORK/record" 2>/dev/null); then
    render_record "$@" >"$WORK/runner.json"
    gcloud storage cp "$WORK/runner.json" "$record.json"
    gcloud storage cp "$WORK/runner.log" "$record.log"
  else
    echo "runner: failed before naming its record, so it leaves none" >&2
  fi

  # A delete that succeeds stops this script with the runner. One that keeps failing leaves the
  # runner running, not stopped: a stop would cancel the deletion its maximum run duration
  # schedules, and leave the runner and its disk behind for good.
  local name zone tries
  for tries in 1 2 3 4 5; do
    if name=$(metadata name) && zone=$(metadata zone | sed 's|.*/||') &&
      gcloud compute instances delete "$name" --zone "$zone" --quiet; then
      return
    fi
    sleep 60
  done
  echo "runner: cannot delete itself, so it is left to its maximum run duration" >&2
}

# The runner's record as JSON: the description main left, the shuffled cell order, the outcome of
# each cell run so far, and the status $1, with main's exit status $2 when it failed.
render_record() {
  local status=$1 exit_status=${2:-} cell run_id cell_status seconds sep=
  echo "{"
  cat "$WORK/description"
  printf '  "cell_order": [%s],\n' "$(json_strings <"$WORK/order")"
  echo '  "cells": ['
  while IFS=$'\t' read -r cell run_id cell_status seconds; do
    printf '%s    {"cell": %s, "run_id": %s, "exit_status": %s, "seconds": %s}' \
      "$sep" "$(json_string "$cell")" "$(json_string "$run_id")" "$cell_status" "$seconds"
    sep=$',\n'
  done <"$WORK/outcomes"
  if [ -n "$sep" ]; then
    echo
  fi
  echo '  ],'
  if [ -n "$exit_status" ]; then
    printf '  "exit_status": %s,\n' "$exit_status"
  fi
  printf '  "status": "%s"\n' "$status"
  echo "}"
}

# One line of a JSON object: the string member $1 with value $2.
field() {
  printf '  %s: %s,\n' "$(json_string "$1")" "$(json_string "$2")"
}

# The lines of stdin as the comma-separated elements of a JSON array of strings.
json_strings() {
  local line sep=
  while IFS= read -r line; do
    printf '%s%s' "$sep" "$(json_string "$line")"
    sep=", "
  done
}

# $1 as a JSON string. Every value the record holds is free of control characters.
json_string() {
  local s=${1//\\/\\\\}
  printf '"%s"' "${s//\"/\\\"}"
}

# The instance's metadata entry $1.
metadata() {
  curl -sSf -H 'Metadata-Flavor: Google' \
    "http://metadata.google.internal/computeMetadata/v1/instance/$1"
}

# The instance's custom metadata attribute $1.
attribute() {
  metadata "attributes/$1"
}

# Nothing from here on may stop the runner from deleting itself.
set +e

# The startup script runs on every boot. A runner that rebooted part way through its cell list
# would find the run ids it recorded taken, so it records the interruption, keeping the outcomes
# so far, and stops.
if [ -s "$WORK/record" ]; then
  echo "runner: rebooted part way through the cell list" | tee -a "$WORK/runner.log"
  finish interrupted
  exit
fi

rm -rf "$WORK"
mkdir -p "$WORK"
: >"$WORK/description"
: >"$WORK/order"
: >"$WORK/outcomes"
main 2>&1 | tee "$WORK/runner.log"
status=${PIPESTATUS[0]}
if [ "$status" -eq 0 ]; then
  finish done
else
  finish failed "$status"
fi
