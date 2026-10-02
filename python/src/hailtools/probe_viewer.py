"""The probe viewer: one self-contained HTML page about the shadow probes under a metrics directory.

It answers whether the estimate a shadow probe would have stopped with, and its estimate interval,
is calibrated against the steady-state throughput the run reaches at its end. Estimates are drawn
relative to that end-of-run estimate, and each estimate interval as its relative half-width around
the estimate it belongs to.
"""

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
import html
import json
from pathlib import Path
from string import Template
from typing import Any

import altair as alt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


class ProbeViewerError(Exception):
    """Anything the caller is expected to read and act on, rather than a traceback."""


@dataclass(frozen=True)
class SettledRun:
    """A shadow run whose stopping rule settled, relative to its end-of-run estimate.

    `error` is the would-be steady-state throughput over the end-of-run one, less 1, and `low` and
    `high` bound its would-be estimate interval on the same scale. `band` is the relative
    half-width of the end-of-run estimate interval. Interval fields are None when the run
    recorded no such interval.
    """

    run_id: str
    formulation: str
    threads: int
    error: float
    low: float | None
    high: float | None
    band: float | None

    @property
    def covers(self) -> bool | None:
        """Whether the would-be estimate interval covers the end-of-run estimate."""
        if self.low is None or self.high is None:
            return None
        return self.low <= 0 <= self.high


@dataclass(frozen=True)
class UnsettledRun:
    """A shadow run whose stopping rule never settled: it has only its end-of-run estimate."""

    run_id: str
    formulation: str
    threads: int
    band: float | None


@dataclass(frozen=True)
class Calibration:
    """Every shadow run under a metrics directory, split by whether its stopping rule settled."""

    runs: pa.Table
    settled: list[SettledRun]
    never_settled: list[UnsettledRun]

    @property
    def intervals(self) -> int:
        """The settled runs with a would-be estimate interval."""
        return sum(run.covers is not None for run in self.settled)

    @property
    def covered(self) -> int:
        """The would-be estimate intervals that cover their run's end-of-run estimate."""
        return sum(run.covers is True for run in self.settled)


def calibration(metrics_dir: Path) -> Calibration:
    """Read the shadow runs' run records under `metrics_dir` and relate each run's estimates."""
    runs = _shadow_runs(metrics_dir)
    settled = []
    never_settled = []
    for record in runs.to_pylist():
        # A shadow run recorded before the estimate interval columns existed lacks them.
        band = record.get("relative_half_width")
        if record["would_stop_ns"] is None:
            never_settled.append(
                UnsettledRun(record["run_id"], record["formulation"], record["threads"], band)
            )
            continue
        # A settled run always has an end-of-run estimate: its measurement window only grew
        # after the would-stop decision.
        end = record["steady_state_throughput"]
        would_be = record["would_be_steady_state_throughput"]
        width = record.get("would_be_relative_half_width")
        low = high = None
        if width is not None:
            low = would_be * (1 - width) / end - 1
            high = would_be * (1 + width) / end - 1
        settled.append(
            SettledRun(
                record["run_id"],
                record["formulation"],
                record["threads"],
                would_be / end - 1,
                low,
                high,
                band,
            )
        )
    return Calibration(runs, settled, never_settled)


def _shadow_runs(metrics_dir: Path) -> pa.Table:
    records = [pq.read_table(path) for path in sorted((metrics_dir / "runs").glob("*.parquet"))]
    if not records:
        raise ProbeViewerError(f"{metrics_dir} has no run records under runs/")
    # Records written before a column existed lack it; permissive promotion leaves it empty.
    runs = pa.concat_tables(records, promote_options="permissive")
    if "action" in runs.column_names:
        runs = runs.filter(pc.field("action") == "shadow")
    else:
        runs = runs.slice(0, 0)
    if runs.num_rows == 0:
        raise ProbeViewerError(
            f"{metrics_dir} has no shadow probes: none of its {len(records)} run records has action shadow"
        )
    return runs.sort_by("run_id")


@dataclass(frozen=True)
class Settings:
    """A combination of the stopping rule settings a replay varies."""

    batch_duration_ns: int
    window_groups: int
    precision: float
    consecutive_checks: int
    min_duration_ns: int


# The throughput probe's default settings, `ProbeSettings::default` in src/throughput_probe.rs.
DEFAULT_SETTINGS = Settings(1_000_000_000, 10, 0.02, 3, 20_000_000_000)


@dataclass(frozen=True)
class Replay:
    """The shadow runs' replay tables under a metrics directory, and the settings the page selects.

    The grid's axes are the values the tables hold, in ascending order.
    """

    checks: pa.Table
    baselines: pa.Table
    replays: pa.Table
    selection: Settings

    @property
    def pairs(self) -> list[tuple[int, int]]:
        return sorted(set(_row_tuples(self.baselines, "batch_duration_ns", "window_groups")))

    @property
    def precisions(self) -> list[float]:
        return sorted(set(self.replays["precision"].to_pylist()))

    @property
    def consecutive_checks(self) -> list[int]:
        return sorted(set(self.replays["consecutive_checks"].to_pylist()))

    @property
    def min_durations_ns(self) -> list[int]:
        return sorted(set(self.replays["min_duration_ns"].to_pylist()))


_REPLAY_TABLES = {"checks": "checks", "baselines": "replay-baselines", "replays": "replays"}


def replay(metrics_dir: Path, calibration: Calibration) -> Replay | None:
    """The replay tables of the shadow runs in `calibration`, or None if `replay` has not written
    them under `metrics_dir`.

    The page selects the settings every shadow run was recorded with if they share them and the
    grid holds them, and the probe's default settings otherwise. If the grid lacks those too, as
    when every run's poll period is longer than the default batch duration, it selects the
    combination nearest them, by batch duration first.
    """
    run_ids = set(calibration.runs["run_id"].to_pylist())
    tables = {}
    for name, directory in _REPLAY_TABLES.items():
        paths = [
            path for path in sorted((metrics_dir / directory).glob("*.parquet")) if path.stem in run_ids
        ]
        if not paths:
            return None
        tables[name] = pa.concat_tables([pq.read_table(path) for path in paths])
    settings = list(Settings.__dataclass_fields__)
    combinations = {Settings(*combination) for combination in _row_tuples(tables["replays"], *settings)}
    recorded = {Settings(*(record.get(name) for name in settings)) for record in calibration.runs.to_pylist()}
    if len(recorded) == 1 and recorded <= combinations:
        selection = recorded.pop()
    else:
        selection = min(
            combinations,
            key=lambda combination: tuple(
                abs(getattr(combination, name) / getattr(DEFAULT_SETTINGS, name) - 1) for name in settings
            ),
        )
    return Replay(selection=selection, **tables)


def _row_tuples(table: pa.Table, *names: str) -> list[tuple[Any, ...]]:
    """Each row of `table`, as the tuple of its values in the columns `names`."""
    return list(zip(*(table[name].to_pylist() for name in names)))


@dataclass(frozen=True)
class ProgressSample:
    """A progress sample: the rows the sink had received `elapsed_ns` after execution started."""

    elapsed_ns: int
    rows: int


@dataclass(frozen=True)
class Rate:
    """Rows per second between two progress samples, in seconds since execution started."""

    start_s: float
    end_s: float
    rate: float


def batch_rates(samples: Sequence[ProgressSample], batch_duration_ns: int) -> list[Rate]:
    """The rate of each batch: a batch ends at the first sample at least `batch_duration_ns` after
    the previous batch end, or after the first sample.

    This rebuilds the stopping rule's batches for display only. Samples after the last batch end
    that do not span a batch duration are left out.
    """
    batches = []
    if not samples:
        return batches
    start = samples[0]
    for sample in samples[1:]:
        # A batch spans some time even when the batch duration is zero, as in the stopping rule.
        if sample.elapsed_ns > start.elapsed_ns and sample.elapsed_ns - start.elapsed_ns >= batch_duration_ns:
            batches.append(_rate(start, sample))
            start = sample
    return batches


@dataclass(frozen=True)
class Point:
    """A rate in rows per second, at `elapsed_s` seconds since execution started."""

    elapsed_s: float
    rate: float


def running_estimate(samples: Sequence[ProgressSample], warmup_end_ns: int, from_ns: int) -> list[Point]:
    """At each sample from `from_ns` on, the rows since the warmup end over the time since.

    A warmup end is the elapsed time of a sample; if none is at it exactly, the last sample before
    it stands in.
    """
    start = _sample_at(samples, warmup_end_ns)
    if start is None:
        return []
    return [
        Point(sample.elapsed_ns / 1e9, _rate(start, sample).rate)
        for sample in samples
        if sample.elapsed_ns >= from_ns and sample.elapsed_ns > start.elapsed_ns
    ]


@dataclass(frozen=True)
class Marker:
    """A labelled rule on a detail chart: a time in seconds, or a rate in rows per second."""

    label: str
    value: float


@dataclass(frozen=True)
class EstimateInterval:
    """An estimate interval in rows per second, drawn as a bar at `elapsed_s`."""

    label: str
    elapsed_s: float
    low: float
    high: float


@dataclass(frozen=True)
class ReplayedRun:
    """A shadow run's replay under the selected pair of batch duration and window groups.

    `checks` are its tightness checks under the pair, as `with_stop` takes them. Each also carries
    the cumulative `rows` at its sample, and the time and rows of the sample its warmup end falls
    at, `warmup_sample_ns` and `warmup_rows`, for the running estimate from a stop. `baseline` is
    the end-of-run decision under the pair, in the columns of the replay baselines table. Both are
    empty if the run was not replayed under the pair, as its batch duration is shorter than the
    run's poll period.
    """

    checks: list[dict[str, Any]]
    baseline: dict[str, Any]

    @property
    def end_of_run(self) -> float | None:
        """The end-of-run steady-state throughput under the pair."""
        return self.baseline.get("steady_state_throughput")


@dataclass(frozen=True)
class RunDetail:
    """How one shadow run's throughput evolved, and where its stopping rule's decisions fall.

    `samples` are the rates between consecutive progress samples and `batches` the rates of the
    batches rebuilt from them. `times` mark the warmup ends, the would-stop point and the first
    partition end, and `levels` the steady-state throughputs, each where the run recorded it.
    `precision_band` is the end-of-run estimate plus or minus the run's precision. `running` is the
    running estimate from the would-stop point to the end of the run. A run that never `settled`
    has no would-be marks and no running estimate.

    A run that was `replayed` is drawn under the selected settings instead: its batches at the
    selected batch duration, and its end-of-run marks and the sample that caps a probe, `times`
    "maximum duration", under the selected pair. The page finds its would-be marks, precision band
    and running estimate from its tightness checks as the sliders move, so they are left empty.
    """

    run_id: str
    settled: bool
    samples: list[Rate]
    batches: list[Rate]
    times: list[Marker]
    levels: list[Marker]
    precision_band: tuple[float, float] | None
    running: list[Point]
    intervals: list[EstimateInterval]
    replayed: ReplayedRun | None = None


WOULD_BE_ESTIMATE = "would-be estimate"
END_OF_RUN_ESTIMATE = "end-of-run estimate"
MAXIMUM_DURATION = "maximum duration"


def run_details(metrics_dir: Path, calibration: Calibration, replay: Replay | None = None) -> list[RunDetail]:
    """The detail of each shadow run in `calibration`, from its progress samples under `metrics_dir`,
    under the settings `replay` selects if there is one."""
    details = []
    for record in calibration.runs.to_pylist():
        samples = _progress(metrics_dir, record["run_id"])
        replayed = None if replay is None else _replayed(record, samples, replay)
        details.append(_run_detail(record, samples, replayed, replay and replay.selection))
    return details


def _progress(metrics_dir: Path, run_id: str) -> list[ProgressSample]:
    path = metrics_dir / "progress" / f"{run_id}.parquet"
    if not path.exists():
        return []
    table = pq.read_table(path, columns=["sample_index", "elapsed_ns", "rows"]).sort_by("sample_index")
    return [ProgressSample(record["elapsed_ns"], record["rows"]) for record in table.to_pylist()]


def _replayed(record: dict[str, Any], samples: list[ProgressSample], replay: Replay) -> ReplayedRun | None:
    def under_pair(table: pa.Table) -> list[dict[str, Any]]:
        return table.filter(
            (pc.field("run_id") == record["run_id"])
            & (pc.field("batch_duration_ns") == replay.selection.batch_duration_ns)
            & (pc.field("window_groups") == replay.selection.window_groups)
        ).to_pylist()

    if record["run_id"] not in set(replay.baselines["run_id"].to_pylist()):
        return None
    baselines = under_pair(replay.baselines)
    baseline = baselines[0] if baselines else {}
    checks = []
    for check in sorted(under_pair(replay.checks), key=lambda check: check["check_index"]):
        warmup = None if check["warmup_end_ns"] is None else _sample_at(samples, check["warmup_end_ns"])
        checks.append(
            {
                "run_id": check["run_id"],
                "check_index": check["check_index"],
                "elapsed_ns": check["elapsed_ns"],
                "warmup_end_ns": check["warmup_end_ns"],
                "steady_state_throughput": check["steady_state_throughput"],
                "relative_half_width": check["relative_half_width"],
                "first_partition_end_ns": record.get("first_partition_end_ns"),
                "capped_at_ns": baseline.get("capped_at_ns"),
                "rows": samples[check["sample_index"]].rows if check["sample_index"] < len(samples) else None,
                "warmup_sample_ns": None if warmup is None else warmup.elapsed_ns,
                "warmup_rows": None if warmup is None else warmup.rows,
            }
        )
    return ReplayedRun(checks, baseline)


def _sample_at(samples: Sequence[ProgressSample], warmup_end_ns: int) -> ProgressSample | None:
    """The sample a warmup end is at, or the last one before it if none is at it exactly."""
    before = [sample for sample in samples if sample.elapsed_ns <= warmup_end_ns]
    return before[-1] if before else None


def _run_detail(
    record: dict[str, Any],
    samples: list[ProgressSample],
    replayed: ReplayedRun | None,
    selection: Settings | None,
) -> RunDetail:
    # Records written before a column existed lack it; every such mark is left off.
    def seconds(row: dict[str, Any], column: str) -> float | None:
        ns = row.get(column)
        return None if ns is None else ns / 1e9

    def markers(pairs: list[tuple[str, float | None]]) -> list[Marker]:
        return [Marker(label, value) for label, value in pairs if value is not None]

    # Under a replay, the end-of-run decision is the selected pair's and the would-be decision is
    # left to the page.
    end_of_run_row = record if replayed is None else replayed.baseline
    would_be_row = record if replayed is None else {}
    end_of_run = end_of_run_row.get("steady_state_throughput")
    end_of_run_width = end_of_run_row.get("relative_half_width")
    would_be = would_be_row.get("would_be_steady_state_throughput")
    would_stop_ns = would_be_row.get("would_stop_ns")
    would_be_warmup_end_ns = would_be_row.get("would_be_warmup_end_ns")
    would_be_width = would_be_row.get("would_be_relative_half_width")
    precision = would_be_row.get("precision")
    batch_duration_ns = record.get("batch_duration_ns") if selection is None else selection.batch_duration_ns

    # Each estimate interval is drawn where its measurement window ends.
    window_end = seconds(end_of_run_row, "window_end_ns")
    intervals = []
    if would_be is not None and would_stop_ns is not None and would_be_width is not None:
        intervals.append(
            EstimateInterval(WOULD_BE_ESTIMATE, would_stop_ns / 1e9, *_around(would_be, would_be_width))
        )
    if end_of_run is not None and window_end is not None and end_of_run_width is not None:
        intervals.append(
            EstimateInterval(END_OF_RUN_ESTIMATE, window_end, *_around(end_of_run, end_of_run_width))
        )

    running = []
    if would_stop_ns is not None and would_be_warmup_end_ns is not None:
        running = running_estimate(samples, would_be_warmup_end_ns, would_stop_ns)

    return RunDetail(
        run_id=record["run_id"],
        settled=record.get("would_stop_ns") is not None,
        samples=[
            _rate(first, second)
            for first, second in zip(samples, samples[1:])
            if second.elapsed_ns > first.elapsed_ns
        ],
        batches=[] if batch_duration_ns is None else batch_rates(samples, batch_duration_ns),
        times=markers(
            [
                ("would-be warmup end", seconds(would_be_row, "would_be_warmup_end_ns")),
                ("end-of-run warmup end", seconds(end_of_run_row, "warmup_end_ns")),
                ("would-stop", seconds(would_be_row, "would_stop_ns")),
                ("first partition end", seconds(record, "first_partition_end_ns")),
                (MAXIMUM_DURATION, None if replayed is None else seconds(replayed.baseline, "capped_at_ns")),
            ]
        ),
        levels=markers([(WOULD_BE_ESTIMATE, would_be), (END_OF_RUN_ESTIMATE, end_of_run)]),
        precision_band=None if end_of_run is None or precision is None else _around(end_of_run, precision),
        running=running,
        intervals=intervals,
        replayed=replayed,
    )


def _around(centre: float, relative_half_width: float) -> tuple[float, float]:
    return centre * (1 - relative_half_width), centre * (1 + relative_half_width)


def _rate(start: ProgressSample, end: ProgressSample) -> Rate:
    seconds = (end.elapsed_ns - start.elapsed_ns) / 1e9
    return Rate(start.elapsed_ns / 1e9, end.elapsed_ns / 1e9, (end.rows - start.rows) / seconds)


@dataclass(frozen=True)
class Controls:
    """What the control bar selects, and how far it reaches, for every replayed run's detail chart.

    `max_precision` is the precision slider's largest value. `half_widths` is the relative
    half-width axis every tightness check panel shares: it spans every precision the slider
    reaches, so the precision line stays in view, and the half-widths of every run's checks under
    the selected pair, up to 100%.
    """

    selection: Settings
    max_precision: float
    half_widths: tuple[float, float]


def controls(replay: Replay, details: Sequence[RunDetail]) -> Controls:
    """The control bar over `replay`, for the replayed runs among `details`."""
    widths = [
        check["relative_half_width"]
        for detail in details
        if detail.replayed is not None
        for check in detail.replayed.checks
        if (check["relative_half_width"] or 0) > 0
    ]
    precisions = replay.precisions
    # The first checks' half-widths can be far wider than any precision; past 100% they are clipped.
    low = min([*widths, min(precisions)]) / 1.5
    high = max(min(max(widths, default=0), 1.0), max(precisions)) * 1.5
    return Controls(replay.selection, max(precisions), (low, high))


def stop_params(precision: float, consecutive_checks: int, min_duration_ns: int) -> list[alt.Parameter]:
    """The settings `with_stop` reads, as Vega-Lite parameters the page's sliders set."""
    return [
        alt.param(name="precision", value=precision),
        alt.param(name="consecutive_checks", value=consecutive_checks),
        alt.param(name="min_duration_ns", value=min_duration_ns),
    ]


def with_stop(chart: alt.Chart) -> alt.Chart:
    """`chart` over tightness checks, with each run's stop at the settings of `stop_params`.

    This copies `stop` and `probe_stop_reason` in src/throughput_probe.rs, so that the page's
    sliders move the stop without a replay (ADR 0019). Change them together. The shared fixture
    python/tests/fixtures/stop_over_tightness_checks.json holds stops the Rust functions find, and
    python/tests/test_stop_over_tightness_checks.py checks this copy against them.

    Each row is a tightness check of a run, in the columns of the checks table, plus its run's
    `first_partition_end_ns` and `capped_at_ns`. Every row gains:
    - `passes`: whether the check found an end of warmup and a relative half-width strictly below
      the precision;
    - `stops_here`: whether a probe would stop at it, the first check at or past the minimum
      duration whose last `consecutive_checks` checks, itself included, all passed, among the
      checks before the first partition end;
    - `would_stop_ns`: its run's stop, empty if it has none;
    - `probe_stop_reason`: how a probe of its run would have ended.

    Every comparison with a nullable column first checks that it is valid, since a null compares
    as 0 in the browser but as null in VegaFusion.
    """
    in_order = [alt.SortField("check_index")]
    return (
        chart.transform_calculate(
            passes="isValid(datum.warmup_end_ns) && isValid(datum.relative_half_width)"
            " && datum.relative_half_width < precision"
        )
        .transform_calculate(failed_index="datum.passes ? -1 : datum.check_index")
        # The streak at a check is its index less that of the last failed check up to it.
        .transform_window(
            last_failed_index="max(failed_index)", groupby=["run_id"], sort=in_order, frame=[None, 0]
        )
        .transform_calculate(
            stops_here="datum.check_index - datum.last_failed_index >= consecutive_checks"
            " && datum.elapsed_ns >= min_duration_ns"
            " && (!isValid(datum.first_partition_end_ns) || datum.elapsed_ns < datum.first_partition_end_ns)"
        )
        # Times grow with the check index, so the first stopping check has the least time.
        .transform_calculate(stop_ns="datum.stops_here ? datum.elapsed_ns : null")
        .transform_joinaggregate(would_stop_ns="min(stop_ns)", groupby=["run_id"])
        .transform_calculate(stops_here="isValid(datum.would_stop_ns) && datum.elapsed_ns == datum.would_stop_ns")
        .transform_calculate(
            probe_stop_reason="isValid(datum.would_stop_ns)"
            " && (!isValid(datum.capped_at_ns) || datum.would_stop_ns <= datum.capped_at_ns) ? 'steady'"
            " : isValid(datum.capped_at_ns)"
            " && (!isValid(datum.first_partition_end_ns) || datum.capped_at_ns < datum.first_partition_end_ns)"
            " ? 'capped' : 'completed'"
        )
    )


def write_page(metrics_dir: Path, output: Path | None = None) -> Path:
    """Write the page about the shadow runs under `metrics_dir`, by default into it."""
    runs = calibration(metrics_dir)
    replayed = replay(metrics_dir, runs)
    page = render_page(runs, run_details(metrics_dir, runs, replayed), replayed)
    output = output if output is not None else metrics_dir / "probe-viewer.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(page)
    return output


def render_page(calibration: Calibration, details: list[RunDetail], replay: Replay | None = None) -> str:
    """The page about `calibration`: fixed framing, the settings table, the headline chart and a
    detail chart per run in `details`, with a control bar over the settings `replay` selects if
    there is one."""
    bar = None if replay is None else controls(replay, details)
    return _PAGE.substitute(
        vega=alt.VEGA_VERSION,
        vega_lite=alt.VEGALITE_VERSION,
        vega_embed=alt.VEGAEMBED_VERSION,
        controls=_REPLAY_MISSING if replay is None else _controls(replay),
        headline_note="" if replay is None else _HEADLINE_NOTE,
        caption=_caption(calibration),
        settings=_settings_table(calibration.runs),
        headline=_embedded_spec("headline", headline_chart(calibration)),
        details="\n".join(
            f'<section class="detail" data-run="{html.escape(detail.run_id)}">\n'
            f"<h3>{html.escape(detail.run_id)}</h3>\n"
            f"{_embedded_spec(f'detail-{index}', detail_chart(detail, bar))}\n"
            "</section>"
            for index, detail in enumerate(details)
        ),
    )


_REPLAY_MISSING = """<p class="note">
This metrics directory has no replay tables, so the page shows each shadow probe only under the
settings it was recorded with. Run <code>cargo run -r -- replay DIR</code> on it, then draw the page
again, for a tightness check panel under each detail chart and sliders that move the stop.
</p>"""

_HEADLINE_NOTE = """<p>
The headline shows the would-be decisions each shadow probe recorded under its own settings. It
does not follow the control bar, which moves only the detail charts.
</p>"""


def _controls(replay: Replay) -> str:
    """The control bar: a slider per setting the page's stop reads, over the grid's range, and the
    selected pair of batch duration and window groups."""
    selection = replay.selection

    def slider(title: str, param: str, values: list[float], step: float, value: float, scale: float = 1) -> str:
        unit = " s" if scale != 1 else ""
        return (
            f'<label>{title} <input type="range" data-param="{param}" data-scale="{scale:g}"'
            f' min="{min(values) / scale:g}" max="{max(values) / scale:g}" step="{step:g}" value="{value / scale:g}">'
            f" <output>{value / scale:g}{unit}</output></label>"
        )

    return "\n".join(
        [
            '<div class="controls" id="controls">',
            slider("Precision", "precision", replay.precisions, 0.001, selection.precision),
            slider(
                "Consecutive checks", "consecutive_checks", replay.consecutive_checks, 1, selection.consecutive_checks
            ),
            slider("Minimum duration", "min_duration_ns", replay.min_durations_ns, 1, selection.min_duration_ns, 1e9),
            f"<span>Batch duration {selection.batch_duration_ns / 1e9:g} s,"
            f" {selection.window_groups} window groups</span>",
            "</div>",
        ]
    )


def headline_chart(calibration: Calibration) -> alt.TopLevelMixin:
    """One row per settled shadow run, then a strip of the runs that never settled."""
    colour = alt.Color("formulation:N", title="Formulation")
    charts = []
    if calibration.settled:
        rows = [
            {
                "run_id": run.run_id,
                "formulation": run.formulation,
                "threads": run.threads,
                "error": run.error,
                "low": run.low,
                "high": run.high,
                **_band(run.band),
            }
            for run in calibration.settled
        ]
        charts.append(
            _runs_chart(
                "Would-be estimate against the end-of-run estimate",
                rows,
                alt.Chart().mark_rule(strokeWidth=2).encode(x="low:Q", x2="high:Q", color=colour),
                alt.Chart().mark_point(filled=True, size=80).encode(
                    x="error:Q",
                    color=colour,
                    shape=alt.Shape("threads:N", title="Threads"),
                    tooltip=[
                        alt.Tooltip("run_id:N", title="Run"),
                        alt.Tooltip("error:Q", title="Would-be estimate", format="+.2%"),
                        alt.Tooltip("low:Q", title="Would-be interval from", format="+.2%"),
                        alt.Tooltip("high:Q", title="Would-be interval to", format="+.2%"),
                        alt.Tooltip("band_high:Q", title="End-of-run half-width", format=".2%"),
                    ],
                ),
            )
        )
    if calibration.never_settled:
        rows = [
            {
                "run_id": run.run_id,
                "formulation": run.formulation,
                "threads": run.threads,
                "label": f"never settled, {run.threads} thread{'' if run.threads == 1 else 's'}",
                **_band(run.band),
            }
            for run in calibration.never_settled
        ]
        charts.append(
            _runs_chart(
                "Never settled",
                rows,
                alt.Chart()
                .mark_text(align="left", dx=4)
                .encode(x=alt.datum(0), text="label:N", color=colour),
            )
        )
    return alt.vconcat(*charts).resolve_scale(x="shared")


def _runs_chart(title: str, rows: list[dict[str, Any]], *marks: alt.Chart) -> alt.TopLevelMixin:
    """One row per run over its end-of-run estimate interval as a grey band, with `marks` on top."""
    x = alt.X(
        "band_low:Q",
        title="Would-be over end-of-run steady-state throughput, less 1",
        axis=alt.Axis(format="+%"),
    )
    # The rule at 0 has no y, so it spans every row.
    zero = alt.Chart().mark_rule(color="black").encode(x=alt.datum(0))
    y = alt.Y("run_id:N", title=None)
    return alt.layer(
        alt.Chart().mark_bar(color="#dddddd").encode(x, alt.X2("band_high:Q"), y=y),
        *(mark.encode(y=y) for mark in marks),
        zero,
        data=alt.Data(values=rows),
    ).properties(title=title, width=600)


# Would-be marks are warm and end-of-run marks cool, so a legend entry is not needed to pair them.
_DETAIL_COLOURS = {
    "sample rate": "#999999",
    "batch rate": "#333333",
    "would-be warmup end": "#fdae6b",
    "would-stop": "#d62728",
    WOULD_BE_ESTIMATE: "#e6550d",
    "running estimate": "#fd8d3c",
    "end-of-run warmup end": "#9ecae1",
    END_OF_RUN_ESTIMATE: "#3182bd",
    "first partition end": "#756bb1",
    MAXIMUM_DURATION: "#636363",
}
# The page finds these from a replayed run's tightness checks.
_STOP_MARKS = ["would-be warmup end", "would-stop", WOULD_BE_ESTIMATE, "running estimate"]
_TIGHTNESS_CHECK_COLOURS = {"passes": "#31a354", "fails": "#999999", "no end of warmup": "#999999"}
_SECONDS = alt.Axis(title="Seconds since execution started")


def detail_chart(detail: RunDetail, bar: Controls | None = None) -> alt.TopLevelMixin:
    """The run's sample and batch rates over time, with its stopping rule's decisions on them.

    A replayed run's would-be decision is the page's stop over its tightness checks at the
    precision, consecutive checks and minimum duration `bar` selects, which the control bar moves;
    under the rates go its tightness check panel and its end of warmup at each check. The rate axis
    reaches every stop the control bar can move to.
    """
    replayed = detail.replayed
    if replayed is None or bar is None:
        return _rates_chart(detail).properties(title=_detail_title(detail, replay_tables=bar is not None))
    checks = alt.Data(values=replayed.checks)
    return (
        alt.vconcat(
            _rates_chart(detail, bar.max_precision),
            _tightness_check_panel(checks, bar.half_widths),
            _warmup_strip(checks),
        )
        # The rates and the tightness checks each have their own colours.
        .resolve_scale(x="shared", color="independent")
        .add_params(
            *stop_params(bar.selection.precision, bar.selection.consecutive_checks, bar.selection.min_duration_ns)
        )
        .properties(title=_detail_title(detail, replay_tables=True))
    )


def _detail_title(detail: RunDetail, replay_tables: bool) -> alt.TitleParams:
    """The run's id, over why its chart lacks what it lacks; `replay_tables` if the page has them."""
    subtitle = []
    if replay_tables and detail.replayed is None:
        subtitle.append(
            "The run was not replayed, so it has no tightness check panel and is drawn under its recorded"
            " settings. Run replay again to replay it."
        )
    if detail.replayed is not None and not detail.replayed.checks:
        subtitle.append(
            "The run was not replayed under the selected batch duration, as it is shorter than its poll period."
        )
    if detail.replayed is None and not detail.settled:
        subtitle.append("The stopping rule never settled.")
    if not detail.samples:
        subtitle.append("No progress samples were recorded.")
    return alt.TitleParams(detail.run_id, subtitle=subtitle, anchor="start")


def _rates_chart(detail: RunDetail, max_precision: float | None = None) -> alt.LayerChart:
    """The run's rates and the marks it carries, with the would-be decision the page finds from its
    tightness checks if it was replayed."""
    replayed = detail.replayed
    replayed_checks = [] if replayed is None else replayed.checks
    # The legend lists only the marks the run has, each in its fixed colour.
    present = {"sample rate": detail.samples, "batch rate": detail.batches, "running estimate": detail.running}
    labels = {marker.label for marker in detail.times + detail.levels} | {bar.label for bar in detail.intervals}
    labels |= {label for label, items in present.items() if items}
    if replayed_checks:
        labels |= set(_STOP_MARKS)
    shown = {label: colour for label, colour in _DETAIL_COLOURS.items() if label in labels}
    colour = alt.Color(
        "mark:N",
        title=None,
        scale=alt.Scale(domain=list(shown), range=list(shown.values())),
        legend=alt.Legend(orient="bottom", columns=3),
    )
    # Raw sample rates swing far wider than anything else, so they are clipped to the rest.
    rates = [batch.rate for batch in detail.batches]
    rates += [marker.value for marker in detail.levels]
    rates += [point.rate for point in detail.running]
    rates += [bound for bar in detail.intervals for bound in (bar.low, bar.high)]
    rates += list(detail.precision_band or ())
    if replayed is not None:
        # Every stop the control bar reaches has an estimate interval narrower than its largest
        # precision.
        rates += [
            bound
            for check in replayed_checks
            if check["warmup_end_ns"] is not None
            and check["relative_half_width"] is not None
            and check["steady_state_throughput"] is not None
            and (max_precision is None or check["relative_half_width"] < max_precision)
            for bound in _around(check["steady_state_throughput"], check["relative_half_width"])
        ]
        if replayed.end_of_run is not None and max_precision is not None:
            rates += _around(replayed.end_of_run, max_precision)
    y_scale = alt.Scale(zero=False)
    if rates:
        pad = (max(rates) - min(rates)) * 0.1 or max(rates) * 0.05
        y_scale = alt.Scale(domain=[min(rates) - pad, max(rates) + pad])
    y = alt.Y("rate:Q", title="Rows per second", scale=y_scale)

    def rows(mark: str, items: list[Any]) -> alt.Data:
        return alt.Data(values=[{"mark": mark, **asdict(item)} for item in items])

    layers = [
        alt.Chart(rows("sample rate", detail.samples))
        .mark_rule(clip=True, strokeWidth=1.5)
        .encode(alt.X("start_s:Q", axis=_SECONDS), x2="end_s:Q", y=y, color=colour),
        alt.Chart(rows("batch rate", detail.batches))
        .mark_rule(strokeWidth=2.5)
        .encode(
            alt.X("start_s:Q", axis=_SECONDS),
            x2="end_s:Q",
            y=y,
            color=colour,
            tooltip=[alt.Tooltip("rate:Q", title="Batch rate", format=",.0f")],
        ),
        alt.Chart(alt.Data(values=[{"mark": marker.label, "rate": marker.value} for marker in detail.levels]))
        .mark_rule(strokeDash=[6, 3])
        .encode(y=y, color=colour, tooltip=[alt.Tooltip("mark:N"), alt.Tooltip("rate:Q", format=",.0f")]),
        alt.Chart(alt.Data(values=[{"mark": marker.label, "elapsed_s": marker.value} for marker in detail.times]))
        .mark_rule(strokeDash=[4, 4])
        .encode(
            alt.X("elapsed_s:Q", axis=_SECONDS),
            color=colour,
            tooltip=[alt.Tooltip("mark:N"), _seconds_tooltip("elapsed_s")],
        ),
        alt.Chart(rows("running estimate", detail.running))
        .mark_line(strokeWidth=2)
        .encode(alt.X("elapsed_s:Q", axis=_SECONDS), y=y, color=colour),
        alt.Chart(alt.Data(values=[asdict(bar) | {"mark": bar.label} for bar in detail.intervals]))
        .mark_rule(strokeWidth=4)
        .encode(
            alt.X("elapsed_s:Q", axis=_SECONDS),
            alt.Y("low:Q", scale=y_scale),
            y2="high:Q",
            color=colour,
            tooltip=_interval_tooltip(),
        ),
    ]
    if replayed_checks:
        layers += _would_be_marks(alt.Data(values=replayed_checks), y, y_scale, colour)
    precision_band = None
    if detail.precision_band is not None:
        low, high = detail.precision_band
        precision_band = alt.Chart(alt.Data(values=[{"low": low, "high": high}]))
    elif replayed is not None and replayed.end_of_run is not None:
        precision_band = alt.Chart(alt.Data(values=[{"end": replayed.end_of_run}])).transform_calculate(
            low="datum.end * (1 - precision)", high="datum.end * (1 + precision)"
        )
    if precision_band is not None:
        # The band has no x, so it spans the chart; it goes first to sit behind everything.
        layers.insert(
            0,
            precision_band.mark_rect(color=_DETAIL_COLOURS[END_OF_RUN_ESTIMATE], opacity=0.12).encode(
                alt.Y("low:Q", scale=y_scale), y2="high:Q"
            ),
        )
    return alt.layer(*layers).properties(width=600, height=250).interactive()


def _would_be_marks(checks: alt.Data, y: alt.Y, y_scale: alt.Scale, colour: alt.Color) -> list[alt.Chart]:
    """The would-stop point, the would-be warmup end, estimate and estimate interval, and the
    running estimate, at the page's stop over `checks`."""
    stop = with_stop(alt.Chart(checks)).transform_filter("datum.stops_here")

    def time(mark: str, ns: str) -> alt.Chart:
        return (
            stop.transform_calculate(mark=f"'{mark}'", elapsed_s=f"datum.{ns} / 1e9")
            .mark_rule(strokeDash=[4, 4], clip=True)
            .encode(
                alt.X("elapsed_s:Q", axis=_SECONDS),
                color=colour,
                tooltip=[alt.Tooltip("mark:N"), _seconds_tooltip("elapsed_s")],
            )
        )

    return [
        time("would-stop", "elapsed_ns"),
        time("would-be warmup end", "warmup_end_ns"),
        stop.transform_calculate(mark=f"'{WOULD_BE_ESTIMATE}'", rate="datum.steady_state_throughput")
        .mark_rule(strokeDash=[6, 3], clip=True)
        .encode(y=y, color=colour, tooltip=[alt.Tooltip("mark:N"), alt.Tooltip("rate:Q", format=",.0f")]),
        stop.transform_filter("isValid(datum.relative_half_width)")
        .transform_calculate(
            mark=f"'{WOULD_BE_ESTIMATE}'",
            elapsed_s="datum.elapsed_ns / 1e9",
            low="datum.steady_state_throughput * (1 - datum.relative_half_width)",
            high="datum.steady_state_throughput * (1 + datum.relative_half_width)",
        )
        .mark_rule(strokeWidth=4, clip=True)
        .encode(
            alt.X("elapsed_s:Q", axis=_SECONDS),
            alt.Y("low:Q", scale=y_scale),
            y2="high:Q",
            color=colour,
            tooltip=_interval_tooltip(),
        ),
        _running_estimate(checks).encode(y=y, color=colour),
    ]


def _running_estimate(checks: alt.Data) -> alt.Chart:
    """The running estimate from the page's stop: at each later tightness check, the rows since the
    sample of the stop's warmup end over the time since."""
    return (
        with_stop(alt.Chart(checks))
        .transform_calculate(
            stop_warmup_sample_ns="datum.stops_here ? datum.warmup_sample_ns : null",
            stop_warmup_rows="datum.stops_here ? datum.warmup_rows : null",
        )
        .transform_joinaggregate(
            from_ns="max(stop_warmup_sample_ns)", from_rows="max(stop_warmup_rows)", groupby=["run_id"]
        )
        .transform_filter(
            "isValid(datum.would_stop_ns) && isValid(datum.from_ns) && isValid(datum.rows)"
            " && datum.elapsed_ns >= datum.would_stop_ns && datum.elapsed_ns > datum.from_ns"
        )
        .transform_calculate(
            mark="'running estimate'",
            elapsed_s="datum.elapsed_ns / 1e9",
            rate="(datum.rows - datum.from_rows) / ((datum.elapsed_ns - datum.from_ns) / 1e9)",
        )
        .mark_line(strokeWidth=2, clip=True)
        .encode(alt.X("elapsed_s:Q", axis=_SECONDS))
    )


def _tightness_check_panel(checks: alt.Data, half_widths: tuple[float, float]) -> alt.LayerChart:
    """Each tightness check's relative half-width against the precision, on the axis `half_widths`,
    coloured by whether it passes and hollow if it found no end of warmup, with the minimum
    duration, the stop and how a probe would have ended.

    A tightness check whose relative half-width is off the axis is pinned to its nearer edge as a
    triangle, and one that found no estimate interval to the top edge, so that every check shows.
    """
    low, high = half_widths
    y_scale = alt.Scale(type="log", domain=[low, high])
    axis = alt.Axis(format="%")
    y = alt.Y("relative_half_width:Q", title="Relative half-width", scale=y_scale, axis=axis)
    state = alt.Color(
        "state:N",
        title="Tightness check",
        scale=alt.Scale(domain=list(_TIGHTNESS_CHECK_COLOURS), range=list(_TIGHTNESS_CHECK_COLOURS.values())),
        legend=alt.Legend(orient="bottom"),
    )
    tooltip = [
        _seconds_tooltip("elapsed_s"),
        alt.Tooltip("relative_half_width:Q", title="Relative half-width", format=".3%"),
        alt.Tooltip("warmup_end_s:Q", title="End of warmup, seconds", format=".1f"),
        alt.Tooltip("state:N", title="Tightness check"),
    ]
    points = (
        with_stop(alt.Chart(checks))
        .transform_calculate(
            elapsed_s="datum.elapsed_ns / 1e9",
            warmup_end_s="isValid(datum.warmup_end_ns) ? datum.warmup_end_ns / 1e9 : null",
            state="!isValid(datum.warmup_end_ns) ? 'no end of warmup' : datum.passes ? 'passes' : 'fails'",
            off_scale=f"!isValid(datum.relative_half_width) || datum.relative_half_width > {high!r}"
            f" || datum.relative_half_width < {low!r}",
            # VegaFusion, which tests the page's transforms, has no min() or max().
            shown_half_width=f"!isValid(datum.relative_half_width) || datum.relative_half_width > {high!r}"
            f" ? {high!r} : datum.relative_half_width < {low!r} ? {low!r} : datum.relative_half_width",
        )
        .encode(
            alt.X("elapsed_s:Q", axis=_SECONDS),
            alt.Y("shown_half_width:Q", title="Relative half-width", scale=y_scale, axis=axis),
            color=state,
            shape=alt.condition("datum.off_scale", alt.value("triangle-up"), alt.value("circle")),
            tooltip=tooltip,
        )
    )
    # VegaFusion, which tests the page's transforms, has no format(); this rounds to 0.1 s.
    stop_s = "toString(round(datum.would_stop_ns / 1e8) / 10)"
    reason = (
        f"datum.probe_stop_reason == 'steady' ? 'A probe would stop steady at ' + {stop_s} + ' s.'"
        " : datum.probe_stop_reason == 'capped' ? 'A probe would be capped at its maximum duration'"
        f" + (isValid(datum.would_stop_ns) ? ', before settling at ' + {stop_s} + ' s.' : ', unsettled.')"
        " : 'A probe would complete at its first partition end, unsettled.'"
    )
    return alt.layer(
        points.transform_filter("isValid(datum.warmup_end_ns)").mark_point(filled=True, size=30),
        points.transform_filter("!isValid(datum.warmup_end_ns)").mark_point(filled=False, size=30),
        alt.Chart(alt.Data(values=[{}]))
        .transform_calculate(relative_half_width="precision", mark="'precision'")
        .mark_rule(color="black", clip=True)
        .encode(y, tooltip=[alt.Tooltip("relative_half_width:Q", title="Precision", format=".2%")]),
        alt.Chart(alt.Data(values=[{}]))
        .transform_calculate(elapsed_s="min_duration_ns / 1e9", mark="'minimum duration'")
        .mark_rule(color="black", strokeDash=[2, 2])
        .encode(alt.X("elapsed_s:Q", axis=_SECONDS), tooltip=[_seconds_tooltip("elapsed_s", "Minimum duration")]),
        with_stop(alt.Chart(checks))
        .transform_filter("datum.stops_here")
        .transform_calculate(elapsed_s="datum.elapsed_ns / 1e9")
        .mark_rule(color=_DETAIL_COLOURS["would-stop"], strokeDash=[4, 4], clip=True)
        .encode(alt.X("elapsed_s:Q", axis=_SECONDS)),
        # How a probe would have ended is the same on every check of the run; one says it.
        with_stop(alt.Chart(checks))
        .transform_filter("datum.check_index == 0")
        .transform_calculate(reason=reason)
        .mark_text(align="left", baseline="bottom", dy=-4)
        .encode(x=alt.value(0), y=alt.value(0), text="reason:N"),
    ).properties(width=600, height=150)


def _warmup_strip(checks: alt.Data) -> alt.Chart:
    """The end of warmup each tightness check found, at the check's time."""
    return (
        alt.Chart(checks)
        .transform_filter("isValid(datum.warmup_end_ns)")
        .transform_calculate(elapsed_s="datum.elapsed_ns / 1e9", warmup_end_s="datum.warmup_end_ns / 1e9")
        .mark_line(
            point=alt.OverlayMarkDef(size=12, color=_DETAIL_COLOURS["would-be warmup end"]),
            color=_DETAIL_COLOURS["would-be warmup end"],
            clip=True,
        )
        .encode(
            alt.X("elapsed_s:Q", axis=_SECONDS),
            alt.Y("warmup_end_s:Q", title="End of warmup, s"),
            tooltip=[_seconds_tooltip("elapsed_s"), _seconds_tooltip("warmup_end_s", "End of warmup, seconds")],
        )
        .properties(width=600, height=60)
    )


def _seconds_tooltip(field: str, title: str = "Seconds") -> alt.Tooltip:
    return alt.Tooltip(f"{field}:Q", title=title, format=".1f")


def _interval_tooltip() -> list[alt.Tooltip]:
    return [
        alt.Tooltip("mark:N"),
        alt.Tooltip("low:Q", title="Interval from", format=",.0f"),
        alt.Tooltip("high:Q", title="Interval to", format=",.0f"),
    ]


def _band(width: float | None) -> dict[str, float | None]:
    return {"band_low": None if width is None else -width, "band_high": width}


def _caption(calibration: Calibration) -> str:
    runs = len(calibration.settled) + len(calibration.never_settled)
    sentences = []
    if calibration.intervals:
        sentences.append(
            f"{calibration.covered} of {calibration.intervals} would-be estimate intervals cover the"
            " end-of-run estimate. About 95% should if the estimate interval is calibrated."
        )
    unmeasured = len(calibration.settled) - calibration.intervals
    if unmeasured:
        runs_were = "run" if unmeasured == 1 else "runs"
        sentences.append(f"{unmeasured} settled {runs_were} recorded no would-be estimate interval.")
    sentences.append(f"{len(calibration.never_settled)} of {runs} shadow runs never settled.")
    return " ".join(sentences)


def _embedded_spec(name: str, chart: alt.TopLevelMixin) -> str:
    # A "</" inside the JSON would end the script element early; "<\/" parses to the same string.
    spec = json.dumps(chart.to_dict()).replace("</", "<\\/")
    return (
        f'<div id="{name}-chart"></div>\n'
        f'<script type="application/json" id="{name}">{spec}</script>'
    )


_SETTINGS: list[tuple[str, str, Callable[[Any], str]]] = [
    ("Run", "run_id", str),
    ("Formulation", "formulation", str),
    ("Groups", "groups", str),
    ("Split points", "split_points", str),
    ("Threads", "threads", str),
    ("Samples", "samples", str),
    ("Dataset", "dataset_path", str),
    ("Input format", "input_format", str),
    ("Output format", "output_format", str),
    ("Precision", "precision", lambda value: f"{value:g}"),
    ("Consecutive checks", "consecutive_checks", str),
    ("Window groups", "window_groups", str),
    ("Poll period", "poll_period_ns", lambda ns: f"{ns / 1e9:g} s"),
    ("Batch duration", "batch_duration_ns", lambda ns: f"{ns / 1e9:g} s"),
    ("Min duration", "min_duration_ns", lambda ns: f"{ns / 1e9:g} s"),
    ("Max duration", "max_duration_ns", lambda ns: f"{ns / 1e9:g} s"),
]


def _settings_table(runs: pa.Table) -> str:
    header = "".join(f"<th>{html.escape(title)}</th>" for title, _, _ in _SETTINGS)
    rows = []
    for record in runs.to_pylist():
        cells = []
        for _, column, show in _SETTINGS:
            value = record.get(column)
            if column == "output_format" and value is None:
                value = "drained"
            cells.append("" if value is None else html.escape(show(value)))
        rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in cells) + "</tr>")
    return f"<table>\n<tr>{header}</tr>\n" + "\n".join(rows) + "\n</table>"


_PAGE = Template("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Shadow probe calibration</title>
<script src="https://cdn.jsdelivr.net/npm/vega@$vega"></script>
<script src="https://cdn.jsdelivr.net/npm/vega-lite@$vega_lite"></script>
<script src="https://cdn.jsdelivr.net/npm/vega-embed@$vega_embed"></script>
<style>
body { font-family: system-ui, sans-serif; max-width: 60rem; margin: 2rem auto; padding: 0 1rem; line-height: 1.4; }
table { border-collapse: collapse; font-size: 0.85rem; }
th, td { border: 1px solid #ccc; padding: 0.2rem 0.4rem; text-align: left; }
.caption { font-weight: bold; }
summary { cursor: pointer; }
summary h2 { display: inline; }
.detail { scroll-margin-top: 4rem; padding: 0.5rem; border-radius: 4px; }
.controls {
  position: sticky; top: 0; z-index: 1; display: flex; flex-wrap: wrap; gap: 0.5rem 1.5rem; align-items: center;
  padding: 0.5rem; background: #f4f4f4; border-bottom: 1px solid #ccc; font-size: 0.9rem;
}
.controls output { display: inline-block; min-width: 3rem; font-variant-numeric: tabular-nums; }
.note { padding: 0.5rem; background: #fff8e1; border-left: 4px solid #fdae6b; }
.detail.selected { outline: 3px solid #d62728; }
</style>
</head>
<body>
<h1>Shadow probe calibration</h1>
<p>
A shadow probe applies the throughput probe's stopping rule after every progress sample but never
stops on it. It records the steady-state throughput and estimate interval of the decision a probe
would have stopped with, the would-be estimate, then runs to completion and records its
steady-state throughput and estimate interval at the end of the run, over the longest measurement
window the dataset allows. This page asks whether the would-be estimate and its estimate interval
are calibrated against that end-of-run estimate. Neither is a truth: both come from the same
estimator, over a shorter and a longer measurement window. The page covers every shadow probe
recorded in the metrics directory and leaves out every other run.
</p>
$controls

<details>
<summary><h2>Reading the headline</h2></summary>
<p>
An estimate interval is the range a steady-state throughput likely lies in, judged by how the
throughput varied over its measurement window: the 95% t-interval over the throughputs of the
window's groups of equal duration. It is drawn as its relative half-width around the estimate.
</p>
<p>
Each row is one shadow run. Its point is the would-be steady-state throughput over the end-of-run
steady-state throughput, less 1: at 0 they agree, and at +2% the probe would have reported 2% more
rows per second than the run settled on at its end. The line through the point is the would-be
estimate interval, on the same scale. The grey band is the end-of-run estimate interval, around 0.
Colour is the formulation and shape the thread count.
</p>
<p>
If the estimate interval is calibrated, about 95% of the lines through the points cross 0; the
caption counts them. Lines that miss 0 all on one side suggest the throughput drifts over a run,
rather than that the estimate is noisy. Lines much wider than the grey bands mean the rule stops on
a looser estimate than a run ends with.
</p>
<p>
A run whose stopping rule never settled before its first partition end has no would-be estimate.
It appears in the never-settled strip below the headline, with only its end-of-run estimate
interval.
</p>
</details>
$headline_note
$headline
<p class="caption">$caption</p>
<p>Click a run in the headline to bring its detail chart into view.</p>

<details>
<summary><h2>Reading the detail charts</h2></summary>
<p>
Each shadow run has a chart of how its throughput evolved over the run, in rows per second against
seconds since execution started, with the stopping rule's decisions drawn on it. It shows whether
the warmup ends where the throughput levels off, whether the throughput drifts over the run,
and whether the running estimate stays inside the would-be estimate interval.
</p>
<p>
The dark horizontal segments are batch rates, one per batch. They are rebuilt from the progress
samples using the run's batch duration, or the selected one when the page has replay tables: a
batch ends at the first sample at least a batch duration after the previous batch end. This is for
display, not a copy of the stopping rule. The thin grey segments behind them are the rates between
consecutive progress samples, cut off where they leave the chart.
</p>
<p>
Warm colours are the would-be decision and cool colours the end of the run. The dashed vertical
rules mark the would-be warmup end, the would-stop point, the end-of-run warmup end and the first
partition end, which closes the end-of-run measurement window. The dashed horizontal rules are the
would-be and the end-of-run steady-state throughputs, and the pale blue band is the end-of-run
estimate plus or minus the run's precision, the tightness the stopping rule asks of an estimate
interval.
</p>
<p>
The orange line from the would-stop point to the end of the run is the running estimate: the rows
since the would-be warmup end over the time since. It starts at the would-be estimate. If it stays
inside the would-be estimate interval, the thick bar at the would-stop point, the probe would have
stopped on an estimate that holds; if it drifts steadily away, the throughput changes along the
genome. The thick blue bar is the end-of-run estimate interval, drawn where its measurement window
ends: at the first partition end if the run reached one, which can be before the end of the run.
The bars are short beside the warmup: scroll over a chart to zoom, drag to pan, and double-click to
reset.
</p>
<p>
A run whose stopping rule never settled has no would-be marks and no running estimate, only its
rates and its end-of-run marks.
</p>
</details>

<details>
<summary><h2>Reading the tightness check panels</h2></summary>
<p>
When the metrics directory has replay tables, each replayed run's chart shows how the stopping rule
would have judged it under the settings in the control bar, which stays at the top of the page. Its
batch duration and window groups decide the run's tightness checks, one at the end of each probe
batch, and its end-of-run marks. Its precision, consecutive checks and minimum duration decide
where the probe stops among those checks, and the sliders move them freely between the grid's
values. The would-stop point, the would-be warmup end, estimate and estimate interval, the running
estimate and the precision band follow the sliders. The grey dashed rule is the maximum duration:
the first progress sample at or past it, where a probe that has not stopped is capped.
</p>
<p>
The panel under the rates draws each tightness check's relative half-width, the relative half-width
of the estimate interval it found, on a log scale against the precision, the black line. Every
panel shares the scale. A triangle on its top or bottom edge is a tightness check whose relative
half-width is off the scale, or one that found no estimate interval yet. A tightness
check passes, in green, if it found an end of warmup and its relative half-width is strictly below
the precision. A hollow tightness check found no end of warmup, so it fails at any precision. The
dotted line is the minimum duration: passes before it count toward a streak, but a probe stops only
at a tightness check at or past it whose last consecutive checks, itself included, all passed,
before the first partition end. The red dashed rule is that tightness check, and the text above the
panel says how a probe with these settings would have ended: steady at its stop, capped at the
maximum duration, or completed at its first partition end. The thin strip at the bottom is the end
of warmup each tightness check found, so the warmup cut can be seen moving over the run.
</p>
<p>
The page finds the stop itself, with a copy of the stopping rule's last step, so the sliders need
no replay; a test keeps the copy in step with the rule. At the grid's values, its stops are the ones
<code>replay</code> recorded.
</p>
</details>
$details

<h2>Settings</h2>
<p>The settings each shadow run was recorded with. An empty cell is a setting the run record leaves empty.</p>
$settings

<script>
function showDetail(run) {
  for (const section of document.querySelectorAll("section.detail")) {
    const selected = section.dataset.run === run;
    section.classList.toggle("selected", selected);
    if (selected) section.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}
// The views whose stop the control bar moves: the replayed runs' detail charts.
const stopViews = [];
for (const spec of document.querySelectorAll('script[type="application/json"]')) {
  const parsed = JSON.parse(spec.textContent);
  vegaEmbed("#" + spec.id + "-chart", parsed).then((result) => {
    if ((parsed.params || []).some((param) => param.name === "precision")) stopViews.push(result.view);
    if (spec.id !== "headline") return;
    result.view.addEventListener("click", (event, item) => {
      if (item && item.datum && item.datum.run_id) showDetail(item.datum.run_id);
    });
  });
}
for (const input of document.querySelectorAll(".controls input")) {
  input.addEventListener("input", () => {
    const scale = Number(input.dataset.scale);
    input.nextElementSibling.textContent = input.value + (scale === 1 ? "" : " s");
    for (const view of stopViews) view.signal(input.dataset.param, Number(input.value) * scale).runAsync();
  });
}
</script>
</body>
</html>
""")
