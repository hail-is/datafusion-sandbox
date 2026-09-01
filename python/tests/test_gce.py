"""Tests for hailtools.gce.

The interesting thing to pin down here is CPUINFO_ALIASES. LLVM feature names and
/proc/cpuinfo flag names disagree in a handful of places, and every disagreement we fail
to record makes `verify` report a feature as missing on hardware that actually has it.
That is a silent-rot kind of bug: it only shows up on a real GCE instance, which is the
one place we are least able to iterate quickly. So we keep real flag lines from real
instances here as fixtures and check the mapping against them.

These tests import hailtools.gce directly rather than going through hailtools.cli,
because cli.py imports hail (JVM startup, several seconds) and gce.py deliberately does
not depend on it.
"""

from __future__ import annotations

import shutil

import pytest

from hailtools import gce

requires_rustc = pytest.mark.skipif(
    shutil.which("rustc") is None, reason="needs a rust toolchain"
)

# Real `flags` lines lifted from /proc/cpuinfo on GCE instances. Between them these cover
# every alias in CPUINFO_ALIASES that our families actually reference: pni, cx16, abm,
# sse4_1, sse4_2, avx512_vnni (Intel) and sha_ni, sse4a (AMD).
CASCADELAKE_FLAGS = (
    "fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx "
    "fxsr sse sse2 ss ht syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl "
    "xtopology nonstop_tsc cpuid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 "
    "sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm abm "
    "3dnowprefetch invpcid_single ssbd ibrs ibpb stibp fsgsbase tsc_adjust bmi1 hle avx2 "
    "smep bmi2 erms invpcid rtm mpx avx512f avx512dq rdseed adx smap clflushopt clwb "
    "avx512cd avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves arat avx512_vnni md_clear "
    "arch_capabilities"
)

MILAN_FLAGS = (
    "fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx "
    "fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc rep_good "
    "nopl nonstop_tsc cpuid extd_apicid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid "
    "sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm "
    "cmp_legacy svm cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw topoext "
    "perfctr_core invpcid_single ssbd ibrs ibpb stibp vmmcall fsgsbase tsc_adjust bmi1 "
    "avx2 smep bmi2 erms invpcid rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec "
    "xgetbv1 xsaves clzero xsaveerptr wbnoinvd arat npt lbrv nrip_save vaes vpclmulqdq "
    "umip pku ospke rdpid fsrm"
)


@pytest.fixture
def cpuinfo(tmp_path):
    """Write a synthetic /proc/cpuinfo and return its parsed flag set."""

    def make(flags: str):
        path = tmp_path / "cpuinfo"
        path.write_text(
            f"processor\t: 0\nmodel name\t: Test CPU\nflags\t\t: {flags}\n"
            f"processor\t: 1\nmodel name\t: Test CPU\nflags\t\t: {flags}\n"
        )
        return gce.cpuinfo_flags(path)

    return make


# -------------------------------------------------------------- rustc invocation


def test_rustc_uses_stable_toolchain(monkeypatch):
    commands = []

    def run(command, **kwargs):
        commands.append((command, kwargs))
        return gce.subprocess.CompletedProcess(command, 0, stdout="features\n")

    monkeypatch.setattr(gce.shutil, "which", lambda command: f"/usr/bin/{command}")
    monkeypatch.setattr(gce.subprocess, "run", run)

    assert gce._rustc("--print", "cfg") == "features\n"
    assert commands == [
        (
            ["rustc", "+stable", "--print", "cfg"],
            {"capture_output": True, "text": True, "check": True},
        )
    ]


# --------------------------------------------------------------- the family table


@requires_rustc
def test_every_table_cpu_is_known_to_rustc():
    """Guards against typos in FAMILY_CPUS, which rustc would otherwise swallow.

    An unrecognised -Ctarget-cpu makes rustc fall back to the bare x86-64 baseline
    instead of failing, so a typo here becomes a quietly pessimised build rather than an
    error. This also fails if the toolchain is too old to know a newer microarchitecture.
    """
    known = gce.known_target_cpus()
    unknown = {c for cpus in gce.FAMILY_CPUS.values() for c in cpus if c not in known}
    assert not unknown, f"rustc does not know: {sorted(unknown)}"


@requires_rustc
@pytest.mark.parametrize("family", sorted(gce.FAMILY_CPUS))
def test_common_features_are_supported_by_every_platform(family):
    common = gce.family_features(family)
    for cpu in gce.family_cpus(family):
        assert common <= gce.cpu_features(cpu)


@requires_rustc
def test_families_are_not_empty():
    for family, cpus in gce.FAMILY_CPUS.items():
        assert cpus, f"{family} lists no CPU platforms"
        assert gce.family_features(family), f"{family} has no common features"


def test_unknown_family_is_reported_clearly():
    with pytest.raises(gce.GceCpuError, match="unknown machine family"):
        gce.family_cpus("m1")


@requires_rustc
def test_unknown_cpu_is_rejected_rather_than_silently_downgraded():
    with pytest.raises(gce.GceCpuError, match="does not know target-cpu"):
        gce.cpu_features("definitely-not-a-cpu")


# --------------------------------------------------------------------- flag emission


@requires_rustc
def test_flags_name_the_oldest_platform():
    # cascadelake is the oldest platform n2 can place you on, so it is what an n2 build
    # has to target.
    assert gce.family_flags("n2").startswith("-Ctarget-cpu=cascadelake")
    assert gce.family_flags("c4").startswith("-Ctarget-cpu=emeraldrapids")


@requires_rustc
def test_nested_family_emits_bare_target_cpu():
    """A plain -Ctarget-cpu also gives LLVM the right scheduling model, so we prefer it
    over an equivalent -Ctarget-feature list whenever the platforms are nested."""
    assert gce.family_flags("n2") == "-Ctarget-cpu=cascadelake"
    assert "-Ctarget-feature" not in gce.family_flags("c3")


@requires_rustc
def test_non_nested_family_falls_back_to_explicit_features(monkeypatch):
    """If Google ever adds a platform that is not a superset of its siblings, naming the
    oldest CPU is no longer enough and we must spell the intersection out."""
    # znver3 and cascadelake are genuinely non-nested: AVX-512 on one side, sse4a/sha on
    # the other.
    monkeypatch.setitem(gce.FAMILY_CPUS, "franken", ("znver3", "cascadelake"))
    flags = gce.family_flags("franken")
    assert flags.startswith("-Ctarget-cpu=znver3 -Ctarget-feature=")
    assert "+avx2" in flags
    assert "+sse4a" not in flags  # cascadelake lacks it
    assert "+avx512f" not in flags  # znver3 lacks it


# ------------------------------------------------------------------ cpuinfo parsing


def test_cpuinfo_flags_parsed(cpuinfo):
    flags = cpuinfo(CASCADELAKE_FLAGS)
    assert "avx512_vnni" in flags
    assert "pni" in flags


def test_cpuinfo_without_flags_line_is_an_error(tmp_path):
    path = tmp_path / "cpuinfo"
    path.write_text("processor\t: 0\nmodel name\t: Test CPU\n")
    with pytest.raises(gce.GceCpuError, match="no 'flags' line"):
        gce.cpuinfo_flags(path)


def test_missing_cpuinfo_is_an_error(tmp_path):
    with pytest.raises(gce.GceCpuError, match="could not read"):
        gce.cpuinfo_flags(tmp_path / "nope")


# ------------------------------------------------------------------- alias coverage


@requires_rustc
@pytest.mark.parametrize("family", ["n2", "c2"])
def test_cascadelake_satisfies_intel_families(family, cpuinfo):
    """Every feature we would compile with must be present on the real hardware."""
    assert gce.missing_features(family, cpuinfo(CASCADELAKE_FLAGS)) == []


@requires_rustc
@pytest.mark.parametrize("family", ["n2d", "c2d", "t2d"])
def test_milan_satisfies_amd_families(family, cpuinfo):
    assert gce.missing_features(family, cpuinfo(MILAN_FLAGS)) == []


@requires_rustc
def test_aliases_are_load_bearing(cpuinfo):
    """Without the alias translation, verify reports features that are plainly present.

    This is the regression this file exists for: /proc/cpuinfo calls SSE3 'pni', so a
    naive comparison flags sse3 as missing on every Intel instance.
    """
    flags = cpuinfo(CASCADELAKE_FLAGS)
    naive = {f for f in gce.family_features("n2") if f not in flags}
    assert "sse3" in naive
    assert naive <= set(gce.CPUINFO_ALIASES), (
        f"unmapped names that differ in /proc/cpuinfo: {sorted(naive - set(gce.CPUINFO_ALIASES))}"
    )


@requires_rustc
def test_verify_detects_genuinely_absent_features(cpuinfo):
    """Negative control: Cascade Lake really does lack Sapphire Rapids' features, and
    building c3 flags for it would SIGILL. verify must say so."""
    missing = gce.missing_features("c3", cpuinfo(CASCADELAKE_FLAGS))
    assert "avx512fp16" in missing
    assert "avx512bf16" in missing
