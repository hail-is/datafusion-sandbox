import json
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hailtools import probe_viewer

RUN_RECORD_TYPES = {
    "run_id": pa.string(),
    "formulation": pa.string(),
    "groups": pa.uint64(),
    "split_points": pa.string(),
    "threads": pa.uint64(),
    "action": pa.string(),
    "stop_reason": pa.string(),
    "steady_state_throughput": pa.float64(),
    "relative_half_width": pa.float64(),
    "would_stop_ns": pa.uint64(),
    "would_be_steady_state_throughput": pa.float64(),
    "would_be_relative_half_width": pa.float64(),
}


def write_run_record(metrics, run_id, **columns):
    columns = {"run_id": run_id, **columns}
    runs = metrics / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({name: pa.array([value], type=RUN_RECORD_TYPES[name]) for name, value in columns.items()}),
        runs / f"{run_id}.parquet",
    )


def shadow(metrics, run_id, *, end, end_width, would_be=None, would_be_width=None, threads=1):
    write_run_record(
        metrics,
        run_id,
        formulation="union",
        threads=threads,
        action="shadow",
        stop_reason="completed",
        steady_state_throughput=end,
        relative_half_width=end_width,
        would_stop_ns=None if would_be is None else 30_000_000_000,
        would_be_steady_state_throughput=would_be,
        would_be_relative_half_width=would_be_width,
    )


@pytest.fixture
def metrics(tmp_path):
    metrics = tmp_path / "metrics"
    # 3% fast, and its would-be estimate interval [100.94, 105.06] misses 100.
    shadow(metrics, "shadow-union-j1", end=100.0, end_width=0.005, would_be=103.0, would_be_width=0.02)
    # 1% slow, and its would-be estimate interval [194.04, 201.96] covers 200.
    shadow(metrics, "shadow-union-j8", end=200.0, end_width=0.01, would_be=198.0, would_be_width=0.02, threads=8)
    shadow(metrics, "shadow-union-j4", end=150.0, end_width=0.03, threads=4)
    write_run_record(
        metrics,
        "probe-union-j8",
        formulation="union",
        threads=8,
        action="probe",
        stop_reason="steady",
        steady_state_throughput=210.0,
        relative_half_width=0.01,
    )
    # A measured write recorded before the probe columns existed.
    write_run_record(metrics, "write-union-j8", formulation="union", threads=8)
    return metrics


def test_calibration_normalises_each_settled_shadow_run_to_its_end_of_run_estimate(metrics):
    calibration = probe_viewer.calibration(metrics)

    settled = {run.run_id: run for run in calibration.settled}
    assert settled.keys() == {"shadow-union-j1", "shadow-union-j8"}

    fast = settled["shadow-union-j1"]
    assert fast.error == pytest.approx(0.03)
    assert (fast.low, fast.high) == (pytest.approx(0.0094), pytest.approx(0.0506))
    assert fast.band == pytest.approx(0.005)
    assert not fast.covers

    slow = settled["shadow-union-j8"]
    assert slow.error == pytest.approx(-0.01)
    assert (slow.low, slow.high) == (pytest.approx(-0.0298), pytest.approx(0.0098))
    assert slow.band == pytest.approx(0.01)
    assert slow.covers


def test_calibration_counts_the_would_be_estimate_intervals_that_cover_the_end_of_run_estimate(metrics):
    calibration = probe_viewer.calibration(metrics)

    assert (calibration.covered, calibration.intervals) == (1, 2)


def test_calibration_keeps_a_shadow_run_that_never_settled_apart(metrics):
    calibration = probe_viewer.calibration(metrics)

    [unsettled] = calibration.never_settled
    assert unsettled.run_id == "shadow-union-j4"
    assert unsettled.band == pytest.approx(0.03)


def test_calibration_refuses_a_metrics_directory_without_run_records(tmp_path):
    with pytest.raises(probe_viewer.ProbeViewerError, match="no run records"):
        probe_viewer.calibration(tmp_path)


def test_calibration_refuses_a_metrics_directory_without_shadow_probes(tmp_path):
    write_run_record(tmp_path, "write-union-j8", formulation="union", threads=8)

    with pytest.raises(probe_viewer.ProbeViewerError, match="no shadow probes"):
        probe_viewer.calibration(tmp_path)


def test_write_page_writes_into_the_metrics_directory_by_default(metrics):
    written = probe_viewer.write_page(metrics)

    assert written == metrics / "probe-viewer.html"
    assert "shadow-union-j1" in written.read_text()


def test_write_page_writes_where_it_is_told(metrics, tmp_path):
    written = probe_viewer.write_page(metrics, tmp_path / "pages" / "calibration.html")

    assert written == tmp_path / "pages" / "calibration.html"
    assert "shadow-union-j1" in written.read_text()
    assert not (metrics / "probe-viewer.html").exists()


def embedded_specs(page):
    return {
        name: json.loads(spec)
        for name, spec in re.findall(r'<script type="application/json" id="([^"]+)">(.*?)</script>', page, re.S)
    }


def inline_rows(spec):
    """Every row of data inlined anywhere in a Vega-Lite spec."""
    if isinstance(spec, dict):
        rows = [row for value in spec.get("datasets", {}).values() for row in value]
        rows += spec.get("values", []) if isinstance(spec.get("values"), list) else []
        return rows + [row for value in spec.values() for row in inline_rows(value)]
    if isinstance(spec, list):
        return [row for value in spec for row in inline_rows(value)]
    return []


def data_urls(spec):
    if isinstance(spec, dict):
        return ([spec["url"]] if "url" in spec else []) + [url for value in spec.values() for url in data_urls(value)]
    if isinstance(spec, list):
        return [url for value in spec for url in data_urls(value)]
    return []


def test_page_frames_the_headline_chart_it_embeds(metrics):
    page = probe_viewer.write_page(metrics).read_text()

    assert "<h2>Reading the headline</h2>" in page
    specs = embedded_specs(page)
    assert specs.keys() == {"headline"}
    headline = specs["headline"]
    assert "vega-lite" in headline["$schema"]
    assert data_urls(headline) == []
    assert {row["run_id"] for row in inline_rows(headline)} == {
        "shadow-union-j1",
        "shadow-union-j4",
        "shadow-union-j8",
    }


def test_page_states_how_many_would_be_estimate_intervals_cover_the_end_of_run_estimate(metrics):
    page = probe_viewer.write_page(metrics).read_text()

    assert "1 of 2 would-be estimate intervals cover the end-of-run estimate" in page
    assert "1 of 3 shadow runs never settled" in page


def test_page_tabulates_the_settings_of_every_shadow_run_and_only_those(metrics):
    page = probe_viewer.write_page(metrics).read_text()

    [table] = re.findall(r"<table.*?</table>", page, re.S)
    assert re.findall(r"<tr><td>([^<]+)</td>", table) == ["shadow-union-j1", "shadow-union-j4", "shadow-union-j8"]
    assert "probe-union-j8" not in page
    assert "write-union-j8" not in page


def test_page_loads_nothing_but_the_vega_scripts_so_it_opens_from_a_file(metrics):
    page = probe_viewer.write_page(metrics).read_text()

    sources = re.findall(r'<script[^>]* src="([^"]+)"', page)
    assert {re.sub(r"@.*", "", source) for source in sources} == {
        "https://cdn.jsdelivr.net/npm/vega",
        "https://cdn.jsdelivr.net/npm/vega-lite",
        "https://cdn.jsdelivr.net/npm/vega-embed",
    }
    assert "<link" not in page


def test_page_says_how_many_settled_runs_recorded_no_would_be_estimate_interval(tmp_path):
    shadow(tmp_path, "shadow-union-j1", end=100.0, end_width=0.005, would_be=103.0, would_be_width=0.02)
    shadow(tmp_path, "shadow-union-j8", end=200.0, end_width=0.01, would_be=198.0, threads=8)

    calibration = probe_viewer.calibration(tmp_path)
    page = probe_viewer.write_page(tmp_path).read_text()

    assert (calibration.covered, calibration.intervals) == (0, 1)
    [unmeasured] = [run for run in calibration.settled if run.run_id == "shadow-union-j8"]
    assert (unmeasured.error, unmeasured.low, unmeasured.high) == (pytest.approx(-0.01), None, None)
    assert "0 of 1 would-be estimate intervals cover the end-of-run estimate" in page
    assert "1 settled run recorded no would-be estimate interval" in page
