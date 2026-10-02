"""The probe viewer's copy of the stop over tightness checks, against the stops the Rust stopping
rule finds over the shared fixture (ADR 0019)."""

import json
from pathlib import Path

import altair as alt
import pyarrow as pa
import pytest

from hailtools import probe_viewer

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "stop_over_tightness_checks.json").read_text())
SETTINGS = sorted(
    {(stop["precision"], stop["consecutive_checks"], stop["min_duration_ns"]) for stop in FIXTURE["stops"]}
)


def fixture_checks() -> pa.Table:
    """The fixture's tightness checks, each with its run's first partition end and cap."""
    runs = {run["run_id"]: run for run in FIXTURE["runs"]}
    return pa.Table.from_pylist(
        [
            check
            | {
                "first_partition_end_ns": runs[check["run_id"]]["first_partition_end_ns"],
                "capped_at_ns": runs[check["run_id"]]["capped_at_ns"],
            }
            for check in FIXTURE["checks"]
        ]
    )


@pytest.mark.parametrize(("precision", "consecutive_checks", "min_duration_ns"), SETTINGS)
def test_the_pages_stop_matches_the_rust_stop_over_the_shared_fixture(precision, consecutive_checks, min_duration_ns):
    chart = probe_viewer.with_stop(alt.Chart(fixture_checks()).mark_point()).add_params(
        *probe_viewer.stop_params(precision, consecutive_checks, min_duration_ns)
    )

    rows = chart.transformed_data().to_pylist()

    found = {}
    for row in rows:
        stop_ns = row["would_stop_ns"]
        found.setdefault(row["run_id"], set()).add(
            (None if stop_ns is None else int(stop_ns), row["probe_stop_reason"])
        )
    expected = {
        stop["run_id"]: {(stop["would_stop_ns"], stop["probe_stop_reason"])}
        for stop in FIXTURE["stops"]
        if (stop["precision"], stop["consecutive_checks"], stop["min_duration_ns"])
        == (precision, consecutive_checks, min_duration_ns)
    }
    assert found == expected
    # Exactly the check a probe would stop at is marked, carrying the would-be decision.
    stopping = {row["run_id"]: int(row["elapsed_ns"]) for row in rows if row["stops_here"]}
    assert stopping == {run_id: stop_ns for run_id, [(stop_ns, _)] in expected.items() if stop_ns is not None}
