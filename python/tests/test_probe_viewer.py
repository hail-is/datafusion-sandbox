import dataclasses
import html
import json
import math
import re

import altair as alt
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
    "window_groups": pa.uint64(),
    "precision": pa.float64(),
    "consecutive_checks": pa.uint64(),
    "min_duration_ns": pa.uint64(),
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


def shadow(
    metrics,
    run_id,
    *,
    end,
    end_width,
    would_be=None,
    would_be_width=None,
    threads=1,
    progress=SERIES,
    consecutive_checks=1,
):
    """A shadow run over `progress`, which settles at 2.2 s on a warmup end at 1.0 s if it has a
    would-be estimate, and ends its run with a warmup end at 1.5 s and its first partition end at
    3.2 s, which closes its end-of-run measurement window. Its settings are those of `RECORDED`,
    but for `consecutive_checks`."""
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
        window_groups=10,
        precision=0.02,
        consecutive_checks=consecutive_checks,
        min_duration_ns=2_000_000_000,
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


# The replay grid of the synthetic metrics directory: two pairs of batch duration and window groups.
PAIRS = [(1_000_000_000, 10), (2_000_000_000, 10)]
PRECISIONS = [0.01, 0.02, 0.05]
CONSECUTIVE_CHECKS = [1, 3]
MIN_DURATIONS_NS = [2_000_000_000, 20_000_000_000]
# The settings `shadow` records by default, which are on the grid.
RECORDED = probe_viewer.Settings(1_000_000_000, 10, 0.02, 1, 2_000_000_000)

# Each pair's tightness checks over `SERIES`: (sample index, warmup end ns, steady-state
# throughput, relative half-width). The 1 s batches end at 1.0 s, 2.2 s and 3.2 s, and the one
# 2 s batch at 2.2 s.
CHECKS = {
    PAIRS[0]: [(2, None, 100.0, None), (4, 1_000_000_000, 200 / 1.2, 0.03), (6, 1_000_000_000, 300 / 2.2, 0.01)],
    PAIRS[1]: [(4, 1_000_000_000, 200 / 1.2, 0.04)],
}


def write_table(metrics, directory, run_id, columns):
    path = metrics / directory
    path.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path / f"{run_id}.parquet")


def write_replay(metrics, run_id, *, end, end_width, capped_at_ns=None, pairs=PAIRS):
    """Replay tables for a run over `SERIES`, as `replay` writes them over `pairs`. Each pair's
    end-of-run estimate is `end` under the recorded pair and 1% faster under the other. The
    replays carry the grid's combinations; their stops are left empty, as nothing reads them."""
    checks = [(pair, index, *check) for pair in pairs for index, check in enumerate(CHECKS[pair])]
    write_table(
        metrics,
        "checks",
        run_id,
        {
            "run_id": pa.array([run_id] * len(checks), pa.string()),
            "batch_duration_ns": pa.array([pair[0] for pair, *_ in checks], pa.uint64()),
            "window_groups": pa.array([pair[1] for pair, *_ in checks], pa.uint64()),
            "check_index": pa.array([check[1] for check in checks], pa.uint64()),
            "sample_index": pa.array([check[2] for check in checks], pa.uint64()),
            "elapsed_ns": pa.array([SERIES[check[2]][0] for check in checks], pa.uint64()),
            "warmup_end_ns": pa.array([check[3] for check in checks], pa.uint64()),
            "steady_state_throughput": pa.array([check[4] for check in checks], pa.float64()),
            "relative_half_width": pa.array([check[5] for check in checks], pa.float64()),
        },
    )
    write_table(
        metrics,
        "replay-baselines",
        run_id,
        {
            "run_id": pa.array([run_id] * len(pairs), pa.string()),
            "batch_duration_ns": pa.array([pair[0] for pair in pairs], pa.uint64()),
            "window_groups": pa.array([pair[1] for pair in pairs], pa.uint64()),
            "steady_state_throughput": pa.array(
                [end if pair == PAIRS[0] else end * 1.01 for pair in pairs], pa.float64()
            ),
            "relative_half_width": pa.array([end_width] * len(pairs), pa.float64()),
            "warmup_end_ns": pa.array(
                [1_500_000_000 if pair == PAIRS[0] else 1_000_000_000 for pair in pairs], pa.uint64()
            ),
            "window_end_ns": pa.array([3_200_000_000] * len(pairs), pa.uint64()),
            "capped_at_ns": pa.array([capped_at_ns] * len(pairs), pa.uint64()),
        },
    )
    combinations = [
        (*pair, precision, consecutive, min_duration)
        for pair in pairs
        for precision in PRECISIONS
        for consecutive in CONSECUTIVE_CHECKS
        for min_duration in MIN_DURATIONS_NS
    ]
    names = ["batch_duration_ns", "window_groups", "precision", "consecutive_checks", "min_duration_ns"]
    types = [pa.uint64(), pa.uint64(), pa.float64(), pa.uint64(), pa.uint64()]
    write_table(
        metrics,
        "replays",
        run_id,
        {
            "run_id": pa.array([run_id] * len(combinations), pa.string()),
            **{
                name: pa.array([combination[axis] for combination in combinations], type)
                for axis, (name, type) in enumerate(zip(names, types))
            },
            "would_stop_ns": pa.array([None] * len(combinations), pa.uint64()),
            "would_be_steady_state_throughput": pa.array([None] * len(combinations), pa.float64()),
            "would_be_relative_half_width": pa.array([None] * len(combinations), pa.float64()),
            "would_be_warmup_end_ns": pa.array([None] * len(combinations), pa.uint64()),
            "probe_stop_reason": pa.array(["completed"] * len(combinations), pa.string()),
        },
    )


@pytest.fixture
def replayed(metrics):
    """`metrics` after `replay`, which replayed its shadow runs with progress samples."""
    write_replay(metrics, "shadow-union-j1", end=100.0, end_width=0.005)
    write_replay(metrics, "shadow-union-j4", end=150.0, end_width=0.03, capped_at_ns=2_600_000_000)
    return metrics


def test_replay_is_absent_from_a_metrics_directory_without_replay_tables(metrics):
    assert probe_viewer.replay(metrics, probe_viewer.calibration(metrics)) is None


def test_replay_takes_the_grid_from_the_replay_tables(replayed):
    replay = probe_viewer.replay(replayed, probe_viewer.calibration(replayed))

    assert replay.pairs == PAIRS
    assert replay.precisions == PRECISIONS
    assert replay.consecutive_checks == CONSECUTIVE_CHECKS
    assert replay.min_durations_ns == MIN_DURATIONS_NS


def test_replay_selects_the_recorded_settings_when_every_shadow_run_shares_them_on_the_grid(replayed):
    replay = probe_viewer.replay(replayed, probe_viewer.calibration(replayed))

    assert replay.selection == RECORDED


def test_replay_selects_the_default_settings_when_shadow_runs_were_recorded_with_different_ones(tmp_path):
    shadow(tmp_path, "shadow-union-j1", end=100.0, end_width=0.005)
    shadow(tmp_path, "shadow-union-j4", end=150.0, end_width=0.03, consecutive_checks=3)
    for run_id in ["shadow-union-j1", "shadow-union-j4"]:
        write_replay(tmp_path, run_id, end=100.0, end_width=0.005)

    replay = probe_viewer.replay(tmp_path, probe_viewer.calibration(tmp_path))

    # The probe's defaults: 1 s batches, 10 window groups, 2%, 3 consecutive checks and 20 s.
    assert replay.selection == probe_viewer.Settings(1_000_000_000, 10, 0.02, 3, 20_000_000_000)


def test_replay_selects_the_default_settings_when_the_recorded_ones_are_off_the_grid(tmp_path):
    shadow(tmp_path, "shadow-union-j1", end=100.0, end_width=0.005, consecutive_checks=5)
    write_replay(tmp_path, "shadow-union-j1", end=100.0, end_width=0.005)

    replay = probe_viewer.replay(tmp_path, probe_viewer.calibration(tmp_path))

    assert replay.selection == probe_viewer.Settings(1_000_000_000, 10, 0.02, 3, 20_000_000_000)


def test_replay_selects_the_combination_nearest_the_default_settings_when_the_grid_lacks_them(tmp_path):
    shadow(tmp_path, "shadow-union-j1", end=100.0, end_width=0.005, consecutive_checks=5)
    # As if the run's poll period were longer than 1 s, so that its 1 s pair was left out.
    write_replay(tmp_path, "shadow-union-j1", end=100.0, end_width=0.005, pairs=[PAIRS[1]])

    replay = probe_viewer.replay(tmp_path, probe_viewer.calibration(tmp_path))

    assert replay.selection == probe_viewer.Settings(2_000_000_000, 10, 0.02, 3, 20_000_000_000)


def replayed_details(metrics, **selection):
    """Each shadow run's detail under the replay in `metrics`, at `RECORDED` but for `selection`."""
    calibration = probe_viewer.calibration(metrics)
    replay = dataclasses.replace(
        probe_viewer.replay(metrics, calibration), selection=dataclasses.replace(RECORDED, **selection)
    )
    return {detail.run_id: detail for detail in probe_viewer.run_details(metrics, calibration, replay)}


def test_run_details_under_a_replay_draw_batch_rates_at_the_selected_batch_duration(replayed):
    detail = replayed_details(replayed, batch_duration_ns=2_000_000_000)["shadow-union-j1"]

    # 0 s to 2.2 s, the first sample 2 s on: 300 rows in 2.2 s. The samples after it span 1.3 s.
    assert [(batch.start_s, batch.end_s, batch.rate) for batch in detail.batches] == [
        (pytest.approx(0.0), pytest.approx(2.2), pytest.approx(300 / 2.2))
    ]


def test_run_details_under_a_replay_take_the_end_of_run_marks_from_the_selected_pairs_baseline(replayed):
    detail = replayed_details(replayed, batch_duration_ns=2_000_000_000)["shadow-union-j1"]

    assert {marker.label: marker.value for marker in detail.levels} == {"end-of-run estimate": pytest.approx(101.0)}
    assert {marker.label: marker.value for marker in detail.times} == {
        "end-of-run warmup end": pytest.approx(1.0),
        "first partition end": pytest.approx(3.2),
    }
    assert {bar.label: (bar.elapsed_s, bar.low, bar.high) for bar in detail.intervals} == {
        "end-of-run estimate": (pytest.approx(3.2), pytest.approx(100.495), pytest.approx(101.505)),
    }
    assert detail.replayed.end_of_run == pytest.approx(101.0)


def test_run_details_under_a_replay_mark_the_sample_that_caps_a_probe(replayed):
    detail = replayed_details(replayed)["shadow-union-j4"]

    assert {marker.label: marker.value for marker in detail.times}["maximum duration"] == pytest.approx(2.6)


def test_run_details_under_a_replay_leave_a_run_without_replay_tables_as_recorded(replayed):
    detail = replayed_details(replayed)["shadow-union-j8"]

    assert detail.replayed is None
    assert {marker.label for marker in detail.levels} == {"would-be estimate", "end-of-run estimate"}


def stops(detail, precision, consecutive_checks, min_duration_ns):
    """The page's stop over `detail`'s tightness checks: (would-stop ns, stop reason)."""
    chart = probe_viewer.with_stop(alt.Chart(alt.Data(values=detail.replayed.checks)).mark_point()).add_params(
        *probe_viewer.stop_params(precision, consecutive_checks, min_duration_ns)
    )
    rows = chart.transformed_data().to_dict("records")
    return {
        (None if math.isnan(row["would_stop_ns"]) else row["would_stop_ns"], row["probe_stop_reason"]) for row in rows
    }


def test_the_pages_stop_over_a_replayed_run_reads_the_selected_pairs_checks(replayed):
    details = replayed_details(replayed)

    # At 5%, the check at 2.2 s passes, which is past the minimum duration of 2 s and before the
    # first partition end at 3.2 s. At 2% it fails, and the check at 3.2 s comes too late.
    assert stops(details["shadow-union-j1"], 0.05, 1, 2_000_000_000) == {(2_200_000_000, "steady")}
    assert stops(details["shadow-union-j1"], 0.02, 1, 2_000_000_000) == {(None, "completed")}
    # The cap at 2.6 s comes after the stop at 5% and before the first partition end.
    assert stops(details["shadow-union-j4"], 0.05, 1, 2_000_000_000) == {(2_200_000_000, "steady")}
    assert stops(details["shadow-union-j4"], 0.02, 1, 2_000_000_000) == {(None, "capped")}
    # The 2 s batches' one check at 2.2 s passes at 5% but not at 2%.
    two_seconds = replayed_details(replayed, batch_duration_ns=2_000_000_000)["shadow-union-j1"]
    assert stops(two_seconds, 0.05, 1, 2_000_000_000) == {(2_200_000_000, "steady")}
    assert stops(two_seconds, 0.03, 1, 2_000_000_000) == {(None, "completed")}


def sliders(page):
    """Each slider of the control bar: its parameter and its min, max, step and value."""
    return {
        attributes["data-param"]: {name: float(attributes[name]) for name in ["min", "max", "step", "value"]}
        for attributes in (
            dict(re.findall(r'([\w-]+)="([^"]*)"', tag)) for tag in re.findall(r'<input type="range"[^>]*>', page)
        )
    }


def test_page_without_replay_tables_names_the_replay_command_and_has_no_control_bar(metrics):
    page = probe_viewer.write_page(metrics).read_text()

    assert "cargo run -r -- replay" in page
    assert sliders(page) == {}
    assert all("vconcat" not in spec for spec in detail_specs(page).values())


def test_page_with_replay_tables_has_a_control_bar_whose_sliders_span_the_grid_from_the_selection(replayed):
    page = probe_viewer.write_page(replayed).read_text()

    assert 'class="controls"' in page
    assert "cargo run -r -- replay" not in page
    assert sliders(page) == {
        "precision": {"min": 0.01, "max": 0.05, "step": 0.001, "value": 0.02},
        "consecutive_checks": {"min": 1, "max": 3, "step": 1, "value": 1},
        # In seconds; the page scales them to the nanoseconds of the parameter.
        "min_duration_ns": {"min": 2, "max": 20, "step": 1, "value": 2},
    }
    assert "Batch duration 1 s, 10 window groups" in page


def test_page_with_replay_tables_says_the_headline_shows_the_recorded_decisions(replayed):
    page = probe_viewer.write_page(replayed).read_text()

    assert "The headline shows the would-be decisions each shadow probe recorded under its own settings" in page


def test_page_with_replay_tables_gives_each_replayed_run_a_tightness_check_panel_on_the_selection(replayed):
    page = probe_viewer.write_page(replayed).read_text()

    details = detail_specs(page)
    replayed_run = details["shadow-union-j1"]
    # Besides the zoom, which has no value.
    assert {param["name"]: param["value"] for param in replayed_run["params"] if "value" in param} == {
        "precision": 0.02,
        "consecutive_checks": 1,
        "min_duration_ns": 2_000_000_000,
    }
    assert len(replayed_run["vconcat"]) == 3
    checks = [row for row in inline_rows(replayed_run) if "check_index" in row]
    assert sorted({row["elapsed_ns"] for row in checks}) == [1_000_000_000, 2_200_000_000, 3_200_000_000]
    # The run without replay tables is drawn as recorded.
    assert "vconcat" not in details["shadow-union-j8"]


def flatten(frames):
    """The frames of `transformed_data()` on a compound chart, which nests a list per subchart."""
    return [frame for item in frames for frame in (flatten(item) if isinstance(item, list) else [item])]


def test_detail_chart_under_a_replay_draws_the_would_be_decision_at_the_pages_stop(replayed):
    [detail] = [detail for detail in replayed_details(replayed).values() if detail.run_id == "shadow-union-j1"]
    bar = probe_viewer.Controls(dataclasses.replace(RECORDED, precision=0.05), 0.05, (0.005, 0.1))

    data = probe_viewer.detail_chart(detail, bar).transformed_data()

    rows = [row for frame in flatten(data) for row in frame.to_dict("records")]

    def at(mark, *columns):
        return [
            tuple(row[column] for column in columns) for row in rows if row.get("mark") == mark and columns[0] in row
        ]

    assert at("would-stop", "elapsed_s") == [(pytest.approx(2.2),)]
    assert at("would-be warmup end", "elapsed_s") == [(pytest.approx(1.0),)]
    # The would-be estimate's level, and its estimate interval at the would-stop point.
    assert at("would-be estimate", "rate") == [(pytest.approx(200 / 1.2),)]
    assert at("would-be estimate", "low", "high", "elapsed_s") == [
        (pytest.approx(200 / 1.2 * 0.97), pytest.approx(200 / 1.2 * 1.03), pytest.approx(2.2))
    ]
    # The running estimate starts at the would-be estimate, from 100 rows at 1.0 s.
    running = sorted((row["elapsed_s"], row["rate"]) for row in rows if row.get("mark") == "running estimate")
    assert running == [(pytest.approx(2.2), pytest.approx(200 / 1.2)), (pytest.approx(3.2), pytest.approx(300 / 2.2))]


def half_width_domains(spec):
    """The domain of every relative half-width axis in a Vega-Lite spec."""
    if isinstance(spec, dict):
        y = spec.get("encoding", {}).get("y", {})
        own = [y["scale"]["domain"]] if y.get("field") == "relative_half_width" and "scale" in y else []
        return own + [domain for value in spec.values() for domain in half_width_domains(value)]
    if isinstance(spec, list):
        return [domain for value in spec for domain in half_width_domains(value)]
    return []


def test_page_draws_every_tightness_check_panel_on_one_axis_that_reaches_every_precision_the_sliders_do(replayed):
    page = probe_viewer.write_page(replayed).read_text()

    details = detail_specs(page)
    domains = {
        tuple(domain)
        for run_id in ["shadow-union-j1", "shadow-union-j4"]
        for domain in half_width_domains(details[run_id])
    }
    [(low, high)] = domains
    # The precision slider runs from 1% to 5%, and the half-widths under the selected pair from 1% to 3%.
    assert low <= 0.01 and high >= 0.05


def test_page_folds_each_reading_guide_into_a_disclosure_collapsed_by_default(replayed):
    page = probe_viewer.write_page(replayed).read_text()

    guides = re.findall(r"<details( open)?>\s*<summary><h2>(Reading [^<]+)</h2></summary>(.*?)</details>", page, re.S)
    assert [(opened, title) for opened, title, _ in guides] == [
        ("", "Reading the headline"),
        ("", "Reading the detail charts"),
        ("", "Reading the tightness check panels"),
    ]
    assert all("<p>" in body and "<script" not in body for _, _, body in guides)
    # No guide is left outside its disclosure.
    assert page.count("<h2>Reading ") == 3


def test_tightness_check_panel_pins_a_check_without_a_relative_half_width_to_its_top_edge(replayed):
    [detail] = [detail for detail in replayed_details(replayed).values() if detail.run_id == "shadow-union-j1"]
    bar = probe_viewer.Controls(RECORDED, 0.05, (0.005, 0.1))

    data = probe_viewer.detail_chart(detail, bar).transformed_data()

    rows = [row for frame in flatten(data) for row in frame.to_dict("records") if "state" in row]
    # The check at 1.0 s found neither an end of warmup nor an estimate interval. At 2%, the check at
    # 2.2 s fails and the one at 3.2 s passes.
    assert sorted((row["elapsed_s"], row["state"], row["shown_half_width"], row["off_scale"]) for row in rows) == [
        (pytest.approx(1.0), "no end of warmup", pytest.approx(0.1), True),
        (pytest.approx(2.2), "fails", pytest.approx(0.03), False),
        (pytest.approx(3.2), "passes", pytest.approx(0.01), False),
    ]


def test_page_with_replay_tables_says_why_a_run_without_them_has_no_tightness_check_panel(replayed):
    page = probe_viewer.write_page(replayed).read_text()

    title = detail_specs(page)["shadow-union-j8"]["title"]
    assert any("was not replayed" in line for line in title["subtitle"])
    assert not any("was not replayed" in line for line in detail_specs(page)["shadow-union-j1"]["title"]["subtitle"])
