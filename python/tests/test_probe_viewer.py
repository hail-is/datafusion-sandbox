import html
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
    "warmup_end_ns": pa.uint64(),
    "first_partition_end_ns": pa.uint64(),
    "window_end_ns": pa.uint64(),
    "would_be_warmup_end_ns": pa.uint64(),
    "batch_duration_ns": pa.uint64(),
    "precision": pa.float64(),
}

# A synthetic progress series: (elapsed ns, cumulative rows).
SERIES = [
    (0, 0),
    (400_000_000, 40),
    (1_000_000_000, 100),
    (1_500_000_000, 160),
    (2_200_000_000, 300),
    (2_600_000_000, 340),
    (3_200_000_000, 400),
    (3_500_000_000, 430),
]


def samples(series):
    return [probe_viewer.ProgressSample(elapsed_ns, rows) for elapsed_ns, rows in series]


def write_run_record(metrics, run_id, **columns):
    columns = {"run_id": run_id, **columns}
    runs = metrics / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({name: pa.array([value], type=RUN_RECORD_TYPES[name]) for name, value in columns.items()}),
        runs / f"{run_id}.parquet",
    )


def write_progress(metrics, run_id, series):
    progress = metrics / "progress"
    progress.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "run_id": pa.array([run_id] * len(series), type=pa.string()),
                "sample_index": pa.array(range(len(series)), type=pa.uint64()),
                "elapsed_ns": pa.array([elapsed_ns for elapsed_ns, _ in series], type=pa.uint64()),
                "rows": pa.array([rows for _, rows in series], type=pa.uint64()),
            }
        ),
        progress / f"{run_id}.parquet",
    )


def shadow(metrics, run_id, *, end, end_width, would_be=None, would_be_width=None, threads=1, progress=SERIES):
    """A shadow run over `progress`, which settles at 2.2 s on a warmup end at 1.0 s if it has a
    would-be estimate, and ends its run with a warmup end at 1.5 s and its first partition end at
    3.2 s, which closes its end-of-run measurement window."""
    settled = would_be is not None
    if progress is not None:
        write_progress(metrics, run_id, progress)
    write_run_record(
        metrics,
        run_id,
        formulation="union",
        threads=threads,
        action="shadow",
        stop_reason="completed",
        steady_state_throughput=end,
        relative_half_width=end_width,
        would_stop_ns=2_200_000_000 if settled else None,
        would_be_steady_state_throughput=would_be,
        would_be_relative_half_width=would_be_width,
        warmup_end_ns=1_500_000_000,
        first_partition_end_ns=3_200_000_000,
        window_end_ns=3_200_000_000,
        would_be_warmup_end_ns=1_000_000_000 if settled else None,
        batch_duration_ns=1_000_000_000,
        precision=0.02,
    )


@pytest.fixture
def metrics(tmp_path):
    metrics = tmp_path / "metrics"
    # 3% fast, and its would-be estimate interval [100.94, 105.06] misses 100.
    shadow(metrics, "shadow-union-j1", end=100.0, end_width=0.005, would_be=103.0, would_be_width=0.02)
    # 1% slow, and its would-be estimate interval [194.04, 201.96] covers 200.
    shadow(
        metrics,
        "shadow-union-j8",
        end=200.0,
        end_width=0.01,
        would_be=198.0,
        would_be_width=0.02,
        threads=8,
        progress=None,
    )
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
    headline = embedded_specs(page)["headline"]
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


def test_batch_rates_end_each_batch_at_the_first_sample_a_batch_duration_after_the_last_end():
    batches = probe_viewer.batch_rates(samples(SERIES), batch_duration_ns=1_000_000_000)

    # 0 s to 1.0 s: 100 rows in 1.0 s. 1.0 s to 2.2 s, as 2.0 s is not a sample: 200 rows in 1.2 s.
    # 2.2 s to 3.2 s: 100 rows in 1.0 s. The samples after 3.2 s do not span a batch duration.
    assert [(batch.start_s, batch.end_s) for batch in batches] == [
        pytest.approx((0.0, 1.0)),
        pytest.approx((1.0, 2.2)),
        pytest.approx((2.2, 3.2)),
    ]
    assert [batch.rate for batch in batches] == [pytest.approx(100.0), pytest.approx(200 / 1.2), pytest.approx(100.0)]


def test_running_estimate_is_the_rows_since_the_warmup_end_over_the_time_since():
    running = probe_viewer.running_estimate(samples(SERIES), warmup_end_ns=1_000_000_000, from_ns=2_200_000_000)

    # From 100 rows at 1.0 s: at the would-stop point it is the would-be steady-state throughput.
    assert [point.elapsed_s for point in running] == [
        pytest.approx(2.2),
        pytest.approx(2.6),
        pytest.approx(3.2),
        pytest.approx(3.5),
    ]
    assert [point.rate for point in running] == [
        pytest.approx(200 / 1.2),
        pytest.approx(240 / 1.6),
        pytest.approx(300 / 2.2),
        pytest.approx(330 / 2.5),
    ]


def test_run_details_mark_where_the_stopping_rule_decided_on_a_run_that_settled(metrics):
    details = {detail.run_id: detail for detail in probe_viewer.run_details(metrics, probe_viewer.calibration(metrics))}

    detail = details["shadow-union-j1"]
    assert {marker.label: marker.value for marker in detail.times} == {
        "would-be warmup end": pytest.approx(1.0),
        "end-of-run warmup end": pytest.approx(1.5),
        "would-stop": pytest.approx(2.2),
        "first partition end": pytest.approx(3.2),
    }
    assert {marker.label: marker.value for marker in detail.levels} == {
        "would-be estimate": pytest.approx(103.0),
        "end-of-run estimate": pytest.approx(100.0),
    }
    # The end-of-run estimate of 100 rows per second, plus or minus a precision of 2%.
    assert detail.precision_band == (pytest.approx(98.0), pytest.approx(102.0))
    assert (detail.running[0].elapsed_s, detail.running[0].rate) == (pytest.approx(2.2), pytest.approx(200 / 1.2))
    assert detail.running[-1].elapsed_s == pytest.approx(3.5)
    # Each estimate interval where its measurement window ends: the would-be one at the would-stop
    # point, and the end-of-run one at the first partition end, before the last sample at 3.5 s.
    assert {bar.label: (bar.elapsed_s, bar.low, bar.high) for bar in detail.intervals} == {
        "would-be estimate": (pytest.approx(2.2), pytest.approx(100.94), pytest.approx(105.06)),
        "end-of-run estimate": (pytest.approx(3.2), pytest.approx(99.5), pytest.approx(100.5)),
    }


def test_run_details_leave_would_be_marks_off_a_run_that_never_settled(metrics):
    details = {detail.run_id: detail for detail in probe_viewer.run_details(metrics, probe_viewer.calibration(metrics))}

    detail = details["shadow-union-j4"]
    assert len(detail.batches) == 3
    assert [marker.label for marker in detail.times] == ["end-of-run warmup end", "first partition end"]
    assert [marker.label for marker in detail.levels] == ["end-of-run estimate"]
    assert [bar.label for bar in detail.intervals] == ["end-of-run estimate"]
    assert detail.running == []


def detail_specs(page):
    """Each detail chart's spec, by the run its section names."""
    specs = embedded_specs(page)
    sections = re.findall(r'<section class="detail" data-run="([^"]+)">.*?<script type="application/json" id="([^"]+)">', page, re.S)
    return {html.unescape(run_id): specs[name] for run_id, name in sections}


def test_page_carries_a_detail_chart_per_shadow_run_framed_by_how_to_read_it(metrics):
    page = probe_viewer.write_page(metrics).read_text()

    assert "<h2>Reading the detail charts</h2>" in page
    details = detail_specs(page)
    assert list(details) == ["shadow-union-j1", "shadow-union-j4", "shadow-union-j8"]
    assert all(data_urls(spec) == [] for spec in details.values())
    labels = {row.get("mark") for row in inline_rows(details["shadow-union-j1"])}
    assert {"would-stop", "would-be estimate", "running estimate"} <= labels


def test_page_draws_no_would_be_marks_on_the_detail_chart_of_a_run_that_never_settled(metrics):
    page = probe_viewer.write_page(metrics).read_text()

    rows = inline_rows(detail_specs(page)["shadow-union-j4"])
    labels = {row["mark"] for row in rows if "mark" in row}
    assert labels == {"end-of-run warmup end", "first partition end", "end-of-run estimate", "batch rate", "sample rate"}
