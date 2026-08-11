"""Work out which CPU features are safe to compile with on a given GCE machine family.

Why this exists: `.cargo/config.toml` sets `-Ctarget-cpu=native`, which is the right
thing when you build and run on the same machine, and the wrong thing for a build
matrix. Cargo hashes the *flag string* into its fingerprint, and "native" is the same
string on every machine even though it means something different on each. Reusing a
target dir across instance types therefore silently keeps artifacts built for the old
microarchitecture (bad benchmark numbers), or emits instructions the new CPU lacks
(SIGILL).

The fix is to name the microarchitecture explicitly. A GCE machine family can schedule
you onto more than one CPU platform, so the safe choice is the feature set common to all
of them, which we get by asking rustc about each platform and intersecting. No instances
need to be booted.

This module deliberately depends on nothing outside the standard library, and in
particular does not import hail. It gets used to bootstrap build machines, which have a
rust toolchain but no reason to carry a JVM -- and `verify` has to run on the GCE
instance itself. Run it directly with a bare interpreter:

    python3 python/src/hailtools/gce.py list

or through the CLI, which exposes the same commands as `hailtools gce ...`.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from functools import cache
from pathlib import Path

TARGET = "x86_64-unknown-linux-gnu"

# GCE machine family -> the LLVM target-cpu name of each CPU platform the family may
# place you on, OLDEST FIRST.
#
# This table is the fragile part of this module. Google adds CPU platforms to existing
# families over time (n2 gained Ice Lake after launch; c4 gained Granite Rapids), so
# re-check it against https://cloud.google.com/compute/docs/cpu-platforms periodically.
# New additions have historically been supersets of the existing members, which keeps the
# intersection stable -- but passing --min-cpu-platform at instance-creation time is what
# makes that an enforced contract rather than an assumption.
#
# Arm families (t2a, c4a) are deliberately absent: they need a different --target
# entirely, not just a different -Ctarget-cpu.
FAMILY_CPUS: dict[str, tuple[str, ...]] = {
    "n1": ("sandybridge", "ivybridge", "haswell", "broadwell", "skylake-avx512"),
    "n2": ("cascadelake", "icelake-server"),
    "n2d": ("znver2", "znver3"),
    "c2": ("cascadelake",),
    "c2d": ("znver3",),
    "c3": ("sapphirerapids",),
    "c3d": ("znver4",),
    "c4": ("emeraldrapids", "graniterapids"),
    "c4d": ("znver5",),
    "n4": ("emeraldrapids",),
    "t2d": ("znver3",),
}

# LLVM feature names and /proc/cpuinfo flag names disagree often enough that a naive
# comparison reports features as missing when they are present. Anything absent from this
# mapping is assumed to use the same name in both.
CPUINFO_ALIASES: dict[str, str] = {
    "sse3": "pni",  # Linux still calls SSE3 by its "Prescott New Instructions" name
    "sse4.1": "sse4_1",
    "sse4.2": "sse4_2",
    "cmpxchg16b": "cx16",
    "lzcnt": "abm",
    "sha": "sha_ni",
    "avx512vnni": "avx512_vnni",
    "avx512bitalg": "avx512_bitalg",
    "avx512vpopcntdq": "avx512_vpopcntdq",
    "avx512bf16": "avx512_bf16",
    "avx512fp16": "avx512_fp16",
    "avx512vbmi2": "avx512_vbmi2",
    "avx512vp2intersect": "avx512_vp2intersect",
    "avxvnni": "avx_vnni",
}

METADATA_ROOT = "http://metadata.google.internal/computeMetadata/v1/instance"


class GceCpuError(Exception):
    """Anything the caller is expected to read and act on, rather than a traceback."""


def _rustc(*args: str) -> str:
    if shutil.which("rustc") is None:
        raise GceCpuError("rustc not found on PATH")
    try:
        return subprocess.run(
            ["rustc", *args], capture_output=True, text=True, check=True
        ).stdout
    except subprocess.CalledProcessError as e:
        raise GceCpuError(f"rustc failed: {e.stderr.strip()}") from e


@cache
def known_target_cpus() -> frozenset[str]:
    out = _rustc("--print", "target-cpus", "--target", TARGET)
    # Lines look like "    cascadelake" or "    native  - Select the CPU of the host".
    return frozenset(
        line.split()[0]
        for line in out.splitlines()
        if line.startswith(" ") and line.split()
    )


@cache
def cpu_features(cpu: str) -> frozenset[str]:
    """The features LLVM associates with a named microarchitecture."""
    # rustc silently falls back to the bare x86-64 baseline on an unrecognised
    # target-cpu rather than failing, which would quietly turn a typo in FAMILY_CPUS
    # into a pessimised build. Check the name up front instead.
    if cpu not in known_target_cpus():
        raise GceCpuError(
            f"rustc does not know target-cpu {cpu!r} "
            "(too old a toolchain, or a typo in FAMILY_CPUS)"
        )
    out = _rustc("--print", "cfg", "--target", TARGET, f"-Ctarget-cpu={cpu}")
    return frozenset(
        line.removeprefix('target_feature="').removesuffix('"')
        for line in out.splitlines()
        if line.startswith("target_feature=")
    )


def family_cpus(family: str) -> tuple[str, ...]:
    try:
        return FAMILY_CPUS[family.lower()]
    except KeyError:
        known = " ".join(FAMILY_CPUS)
        raise GceCpuError(f"unknown machine family {family!r}. Known: {known}") from None


def family_features(family: str) -> frozenset[str]:
    """Features common to every CPU platform the family can schedule you onto."""
    return frozenset.intersection(*(cpu_features(c) for c in family_cpus(family)))


def family_flags(family: str) -> str:
    """The rustc flags to build for any instance in this family.

    In every family as of this writing the platforms are strictly nested, so the
    intersection is exactly the oldest member's feature set. Where that holds we emit a
    plain -Ctarget-cpu, which additionally gives LLVM the right scheduling model for that
    core -- strictly better than an equivalent -Ctarget-feature list, which would leave
    tuning generic. If a future platform breaks the nesting we fall back to spelling out
    the intersection.
    """
    baseline = family_cpus(family)[0]
    common = family_features(family)
    if common == cpu_features(baseline):
        return f"-Ctarget-cpu={baseline}"
    features = ",".join(f"+{f}" for f in sorted(common))
    return f"-Ctarget-cpu={baseline} -Ctarget-feature={features}"


def _metadata(key: str) -> str | None:
    req = urllib.request.Request(
        f"{METADATA_ROOT}/{key}", headers={"Metadata-Flavor": "Google"}
    )
    try:
        with urllib.request.urlopen(req, timeout=2) as resp:
            return resp.read().decode().strip()
    except (urllib.error.URLError, OSError, TimeoutError):
        return None


def cpuinfo_flags(path: Path = Path("/proc/cpuinfo")) -> frozenset[str]:
    try:
        text = path.read_text()
    except OSError as e:
        raise GceCpuError(f"could not read {path}: {e}") from e
    flags: set[str] = set()
    for line in text.splitlines():
        if line.startswith("flags") and ":" in line:
            flags.update(line.split(":", 1)[1].split())
    if not flags:
        raise GceCpuError(f"no 'flags' line found in {path}")
    return frozenset(flags)


def missing_features(family: str, present: frozenset[str]) -> list[str]:
    """Which of the family's computed features are absent from a real CPU's flags."""
    return sorted(
        f
        for f in family_features(family)
        if CPUINFO_ALIASES.get(f, f) not in present
    )


# --------------------------------------------------------------------------- commands


def cmd_list() -> str:
    rows = [f"{'FAMILY':<6} {'BASELINE':<15} {'FEATURES':<9} NOTES"]
    for family, cpus in FAMILY_CPUS.items():
        common = family_features(family)
        baseline, newest = cpus[0], cpus[-1]
        if baseline == newest:
            note = "single platform, nothing given up"
        elif len(common) == len(cpu_features(newest)):
            note = "platforms are feature-identical, nothing given up"
        else:
            given_up = len(cpu_features(newest)) - len(common)
            note = f"{given_up} features given up vs {newest}"
        rows.append(f"{family:<6} {baseline:<15} {len(common):<9} {note}")
    rows.append(
        "\nFamilies listing 'features given up' span multiple CPU platforms. To recover\n"
        "those features, pin the platform at instance-creation time, e.g.\n"
        '  gcloud compute instances create ... --min-cpu-platform="Intel Ice Lake"\n'
        "and then build for the newer target-cpu directly. Pinning trades against\n"
        "capacity availability in a given zone."
    )
    return "\n".join(rows)


def cmd_verify(family: str | None = None) -> str:
    """Check a real instance against the computed feature set.

    The computed sets come from LLVM's model of each microarchitecture. What a GCE VM
    actually exposes can be narrower, because the hypervisor may mask features. Worth
    running once per family.
    """
    out = []
    if family is None:
        machine_type = _metadata("machine-type")
        if machine_type is None:
            raise GceCpuError(
                "could not reach the metadata server; pass the family explicitly"
            )
        machine_type = machine_type.rsplit("/", 1)[-1]
        family = machine_type.split("-", 1)[0]
        out.append(f"detected machine-type {machine_type} -> family {family}")

    out.append(f"reported CPU platform: {_metadata('cpu-platform') or 'unknown'}")
    out.append(f"expecting features common to: {' '.join(family_cpus(family))}")

    missing = missing_features(family, cpuinfo_flags())
    if missing:
        out.append("")
        out.append(f"MISSING from /proc/cpuinfo: {' '.join(missing)}")
        out.append("")
        out.append(
            "Building with these would risk SIGILL. Either FAMILY_CPUS is stale, or\n"
            "the hypervisor masks them on this platform."
        )
        raise GceCpuError("\n".join(out))
    out.append("OK: every computed feature is present on this instance.")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="gce.py",
        description=(__doc__ or "").split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "example build-matrix use:\n"
            '  RUSTFLAGS="$(python3 %(prog)s flags c4)" \\\n'
            "    CARGO_TARGET_DIR=target-c4 cargo build -r --example combiner2\n\n"
            "RUSTFLAGS overrides (does not merge with) build.rustflags in\n"
            ".cargo/config.toml, so this replaces the -Ctarget-cpu=native there."
        ),
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="all families: what's safe, and what it costs")
    p = sub.add_parser("flags", help="rustc flags to build for a family")
    p.add_argument("family")
    p = sub.add_parser("features", help="the intersected feature list, one per line")
    p.add_argument("family")
    p = sub.add_parser("verify", help="run ON a GCE VM: check reality matches")
    p.add_argument("family", nargs="?")

    args = parser.parse_args(argv)
    try:
        match args.cmd:
            case "list":
                print(cmd_list())
            case "flags":
                print(family_flags(args.family))
            case "features":
                print("\n".join(sorted(family_features(args.family))))
            case "verify":
                print(cmd_verify(args.family))
    except GceCpuError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
