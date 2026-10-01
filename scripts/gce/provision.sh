#!/usr/bin/env bash
# The build VM's startup script, run as root on every boot. It is idempotent: on a fresh disk it
# installs everything a build needs, and on a disk that has it all it only fetches the checkout.
# Everything it installs belongs to the user `builder`, which build.sh runs as.
set -euo pipefail

echo "provision: start"

# cmake only matters for the snmalloc feature; bindgen, in a dependency's build script, needs
# libclang; python3 runs gce.py.
apt-get update -qq
apt-get install -y -qq build-essential cmake curl git libclang-dev pkg-config python3

id builder >/dev/null 2>&1 || useradd --create-home --shell /bin/bash builder

runuser -l builder <<'EOF'
set -euo pipefail
if [ ! -x ~/.cargo/bin/rustup ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y --profile minimal --default-toolchain none
fi
. ~/.cargo/env
if [ ! -d ~/datafusion-sandbox ]; then
  git clone --quiet https://github.com/hail-is/datafusion-sandbox ~/datafusion-sandbox
fi
cd ~/datafusion-sandbox
git fetch --quiet origin
# The toolchain the checkout pins, so a fresh VM is ready for work besides build.sh, which installs
# the pin of the commit it builds in any case. And stable, which gce.py asks for CPU feature sets.
if [ -f rust-toolchain.toml ]; then
  rustup toolchain install
fi
rustup toolchain install stable --profile minimal
EOF

# build.sh waits for this boot's id here, so a build never runs against a half-provisioned VM.
mkdir -p /var/lib/provision
cat /proc/sys/kernel/random/boot_id >/var/lib/provision/boot-id

echo "provision: done"
