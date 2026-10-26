#!/usr/bin/env bash
# Builds and runs the experiment against one uv-managed CPython.
# Usage: ./run.sh 3.14t [SECONDS] [THREADS] [REPS] [WORKLOADS]
# Needs: uv python install <ver>; uv venv -p <ver> /tmp/pyudf-venv-<ver>; uv pip install --python /tmp/pyudf-venv-<ver>/bin/python numpy pyarrow
set -euo pipefail
ver=$1; shift
py=$(uv python find "$ver")
prefix=$(dirname "$py")/..
venv=${VENV:-/tmp/pyudf-venv-$ver}
here=$(cd "$(dirname "$0")" && pwd)
bin=$(cd "$here" && PYO3_PYTHON=$py SANDBOX_TARGET_SUFFIX="py$ver" \
  cargo build --profile release --message-format=json 2>/dev/null |
  sed -n 's/.*"executable":"\([^"]*\)".*/\1/p')
site=$("$venv/bin/python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
LD_LIBRARY_PATH="$prefix/lib" PYTHONPATH="$site" "$bin" "${1:-2}" "${2:-1,2,4,8}" "${3:-5}" ${4:+"$4"}
