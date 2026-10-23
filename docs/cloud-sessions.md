# Cloud sessions

A [Claude Code cloud session](https://code.claude.com/docs/en/claude-code-on-the-web) runs an agent
in a fresh Anthropic-hosted VM with a clone of this repo. Here it is for implementing a ticket
away from a workstation: edit, run the checks, open a pull request. Combiner runs on real data,
anything on [GCE](gce.md), and benchmarks stay off it; the VM has no GCP credentials, and a timing
taken on a shared VM means nothing.

## The VM

Ubuntu 24.04 on x86_64 with about 4 vCPU, 16 GB of RAM and 30 GB of disk. It ships rustc and cargo,
uv, gcc and clang, and `gh`. That is enough memory for dev builds and `release-nonlto`, but not to
link the fat-LTO `-r` profile. A cold `scripts/check`, which compiles every dependency for clippy,
rustdoc and the tests, took about 5 minutes and peaked near 6 GB, and the Python tests about a
minute. So iterate with `cargo check` and filtered tests and run the full check once at the end;
CI runs it again on the PR.

## The environment

A session runs in an *environment*, configured on claude.ai/code and personal to whoever made it:

- **Network access**: Trusted, the default. It allows crates.io, static.rust-lang.org, GitHub
  (which serves the vortex git dependency and ast-grep) and PyPI.
- **Setup script**: the one below, then anything personal, such as cloning your own skills into
  `/root/.claude/skills`, since cloud sessions read nothing from your machine's `~/.claude`.

```
#!/bin/bash
set -euo pipefail
git clone --quiet --depth 1 --branch main https://github.com/hail-is/datafusion-sandbox /tmp/dfs
/tmp/dfs/scripts/cloud-setup
rm -rf /tmp/dfs
```

The setup script doesn't run in the session's checkout, so it clones `main` to run
`scripts/cloud-setup` from. What that installs outside the clone stays: the toolchains, the cargo
registry and the uv cache.

`scripts/cloud-setup` installs what the checks need beyond the image: libclang (for bindgen in a
dependency's build script) if it's missing, rustup with the toolchain `rust-toolchain.toml` pins
plus stable (which `python/tests/test_gce.py` uses), the dependency sources (`cargo fetch`),
ast-grep, and the Python environment (`uv sync`). It is idempotent and takes seconds when
everything is in place.

When the setup script finishes in about 5 minutes, the VM's filesystem is snapshotted, and later
sessions start from the snapshot. The snapshot is rebuilt only when the environment's setup script
text or network settings change, or after about 7 days, not when the repo changes. So the
SessionStart hook in `.claude/settings.json` runs `scripts/cloud-setup` again at the start of every
cloud session (it does nothing outside one), which installs a toolchain pin or lockfile change the
snapshot predates. The hook also sets `RUSTFLAGS=""` for the session, overriding
`-Ctarget-cpu=native` as CI does, so a build cached on one VM can't fail with SIGILL on another's
older CPU.

## Starting a session

From a checkout, `claude --cloud "<task>"`, which clones the branch from GitHub, so push it first;
or from claude.ai/code, in the browser or the mobile app. Pick Auto mode for unattended work.
`claude --teleport` brings a session's branch and conversation back to a local checkout.

## GitHub from a session

Sessions reach GitHub through a proxy that holds the credentials and blocks GraphQL, so `gh issue`
and `gh pr` fail. `gh api` with REST paths works, for this repo only; the
[issue tracker conventions](agents/issue-tracker.md) give the REST forms. To open a pull request
from the pushed branch:

```
gh api -X POST repos/hail-is/datafusion-sandbox/pulls \
  -f title="..." -f head="<branch>" -f base=main -F draft=true -f body="..."
```

## Java

`python/tests/test_cli.py` starts Hail on local Spark, which needs a JVM. Hail 0.2.139 is built and
tested with Java 11; the image has Java 21, on which those tests pass with a warning that Java 21 is
unsupported.
