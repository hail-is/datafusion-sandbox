"""The probe viewer: one self-contained HTML page about the shadow probes under a metrics directory.

It answers whether the estimate a shadow probe would have stopped with, and its estimate interval,
is calibrated against the steady-state throughput the run reaches at its end. Estimates are drawn
relative to that end-of-run estimate, and each estimate interval as its relative half-width around
the estimate it belongs to.
"""

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, replace
import html
import json
from pathlib import Path
from statistics import median
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

    The grid's axes are the values the tables hold, in ascending order. `run_ids` are every shadow
    run's, replayed or not, and `recorded` the combinations of the grid shadow runs were recorded
    with.
    """

    checks: pa.Table
    baselines: pa.Table
    replays: pa.Table
    selection: Settings
    run_ids: list[str]
    recorded: frozenset[Settings]

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
        [selection] = recorded
    else:
        selection = min(
            combinations,
            key=lambda combination: tuple(
                abs(getattr(combination, name) / getattr(DEFAULT_SETTINGS, name) - 1) for name in settings
            ),
        )
    return Replay(
        selection=selection,
        run_ids=calibration.runs["run_id"].to_pylist(),
        recorded=frozenset(recorded & combinations),
        **tables,
    )


@dataclass(frozen=True)
class Combination:
    """One settings combination of the replay grid, judged over the shadow runs replayed under it.

    `runs` are the runs replayed under it and `not_replayed` the other shadow runs, whose poll
    period is longer than its batch duration or which were not replayed at all. `steady` counts
    the runs a probe with it would have stopped steady. Over those runs, `median_stop_ns` is the
    median would-stop time and `worst_error` the largest relative difference of the would-be
    steady-state throughput from the end-of-run one under its pair; both are None if there are
    none. `covered` counts their would-be estimate intervals that cover that end-of-run estimate,
    out of `intervals`. `pareto` is whether no other combination is at least as fast and as
    accurate and better at one, and `recorded` whether shadow runs were recorded with it.
    """

    settings: Settings
    runs: list[str]
    not_replayed: list[str]
    steady: int
    median_stop_ns: float | None
    worst_error: float | None
    covered: int
    intervals: int
    pareto: bool
    recorded: bool

    @property
    def not_steady(self) -> int:
        """The runs a probe with the combination would have been capped or completed on."""
        return len(self.runs) - self.steady


def overview(replay: Replay) -> list[Combination]:
    """Every settings combination of `replay`'s grid, from the stops `replay` recorded.

    These are the stopping rule's stops, not the page's copy of its last step (ADR 0019).
    """
    ends = {
        (baseline["run_id"], baseline["batch_duration_ns"], baseline["window_groups"]): baseline[
            "steady_state_throughput"
        ]
        for baseline in replay.baselines.to_pylist()
    }
    settings = list(Settings.__dataclass_fields__)
    stops: dict[Settings, list[dict[str, Any]]] = {}
    for stop in replay.replays.to_pylist():
        stops.setdefault(Settings(*(stop[name] for name in settings)), []).append(stop)
    combinations = []
    for combination, rows in sorted(stops.items(), key=lambda item: tuple(asdict(item[0]).values())):
        steady = [row for row in rows if row["probe_stop_reason"] == "steady"]
        errors = []
        covered = intervals = 0
        for row in steady:
            # A run that stopped steady has an end-of-run estimate under the pair: its measurement
            # window only grew after the stop.
            end = ends[(row["run_id"], combination.batch_duration_ns, combination.window_groups)]
            would_be = row["would_be_steady_state_throughput"]
            errors.append(abs(would_be / end - 1))
            width = row["would_be_relative_half_width"]
            if width is not None:
                intervals += 1
                covered += would_be * (1 - width) <= end <= would_be * (1 + width)
        runs = sorted(row["run_id"] for row in rows)
        combinations.append(
            Combination(
                settings=combination,
                runs=runs,
                not_replayed=[run_id for run_id in replay.run_ids if run_id not in runs],
                steady=len(steady),
                median_stop_ns=median(row["would_stop_ns"] for row in steady) if steady else None,
                worst_error=max(errors, default=None),
                covered=covered,
                intervals=intervals,
                pareto=False,
                recorded=combination in replay.recorded,
            )
        )
    placed = [
        (combination.median_stop_ns, combination.worst_error)
        for combination in combinations
        if combination.median_stop_ns is not None and combination.worst_error is not None
    ]
    return [
        replace(
            combination,
            pareto=combination.median_stop_ns is not None
            and combination.worst_error is not None
            and not any(_dominates(other, (combination.median_stop_ns, combination.worst_error)) for other in placed),
        )
        for combination in combinations
    ]


def _dominates(first: tuple[float, float], second: tuple[float, float]) -> bool:
    """Whether `first` is at least as low as `second` on both axes, and lower on one."""
    return all(a <= b for a, b in zip(first, second)) and first != second


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
    """A shadow run's replay under every pair of batch duration and window groups, so that the page
    can switch between pairs.

    `checks` are its tightness checks under every pair, as `with_stop` takes them once
    `on_selected_pair` keeps one pair's. Each also carries the cumulative `rows` at its sample, and
    the time and rows of the sample its warmup end falls at, `warmup_sample_ns` and `warmup_rows`,
    for the running estimate from a stop. `baselines` are the end-of-run decision under each pair,
    in the columns of the replay baselines table, and `batches` the batch rates at each pair's
    batch duration, each row with its `batch_duration_ns`. `missing` are the grid's pairs the run
    was not replayed under, as their batch duration is shorter than its poll period.
    """

    checks: list[dict[str, Any]]
    baselines: list[dict[str, Any]]
    batches: list[dict[str, Any]]
    missing: list[tuple[int, int]]


@dataclass(frozen=True)
class RunDetail:
    """How one shadow run's throughput evolved, and where its stopping rule's decisions fall.

    `samples` are the rates between consecutive progress samples and `batches` the rates of the
    batches rebuilt from them. `times` mark the warmup ends, the would-stop point and the first
    partition end, and `levels` the steady-state throughputs, each where the run recorded it.
    `precision_band` is the end-of-run estimate plus or minus the run's precision. `running` is the
    running estimate from the would-stop point to the end of the run. A run that never `settled`
    has no would-be marks and no running estimate.

    A run that was `replayed` is drawn under the selected settings instead, which the page moves:
    its batches at the selected batch duration, its end-of-run marks and the sample that caps a
    probe under the selected pair, and its would-be marks, precision band and running estimate at
    the page's stop over its tightness checks. They are all in `replayed`, so only the first
    partition end is left here.
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
    and its replay under every pair of `replay` if there is one."""
    details = []
    for record in calibration.runs.to_pylist():
        samples = _progress(metrics_dir, record["run_id"])
        replayed = None if replay is None else _replayed(record, samples, replay)
        details.append(_run_detail(record, samples, replayed))
    return details


def _progress(metrics_dir: Path, run_id: str) -> list[ProgressSample]:
    path = metrics_dir / "progress" / f"{run_id}.parquet"
    if not path.exists():
        return []
    table = pq.read_table(path, columns=["sample_index", "elapsed_ns", "rows"]).sort_by("sample_index")
    return [ProgressSample(record["elapsed_ns"], record["rows"]) for record in table.to_pylist()]


def _replayed(record: dict[str, Any], samples: list[ProgressSample], replay: Replay) -> ReplayedRun | None:
    def of_run(table: pa.Table) -> list[dict[str, Any]]:
        return table.filter(pc.field("run_id") == record["run_id"]).to_pylist()

    baselines = of_run(replay.baselines)
    if not baselines:
        return None
    capped_at_ns_by_pair = {
        (baseline["batch_duration_ns"], baseline["window_groups"]): baseline["capped_at_ns"] for baseline in baselines
    }
    checks = []
    for check in sorted(
        of_run(replay.checks),
        key=lambda check: (check["batch_duration_ns"], check["window_groups"], check["check_index"]),
    ):
        warmup = None if check["warmup_end_ns"] is None else _sample_at(samples, check["warmup_end_ns"])
        checks.append(
            {
                "run_id": check["run_id"],
                "batch_duration_ns": check["batch_duration_ns"],
                "window_groups": check["window_groups"],
                "check_index": check["check_index"],
                "elapsed_ns": check["elapsed_ns"],
                "warmup_end_ns": check["warmup_end_ns"],
                "steady_state_throughput": check["steady_state_throughput"],
                "relative_half_width": check["relative_half_width"],
                "first_partition_end_ns": record.get("first_partition_end_ns"),
                "capped_at_ns": capped_at_ns_by_pair[(check["batch_duration_ns"], check["window_groups"])],
                "rows": samples[check["sample_index"]].rows if check["sample_index"] < len(samples) else None,
                "warmup_sample_ns": None if warmup is None else warmup.elapsed_ns,
                "warmup_rows": None if warmup is None else warmup.rows,
            }
        )
    batches = [
        {"batch_duration_ns": batch_duration_ns, **asdict(batch)}
        for batch_duration_ns in sorted({batch_duration_ns for batch_duration_ns, _ in capped_at_ns_by_pair})
        for batch in batch_rates(samples, batch_duration_ns)
    ]
    missing = [pair for pair in replay.pairs if pair not in capped_at_ns_by_pair]
    return ReplayedRun(checks, sorted(baselines, key=lambda baseline: baseline["batch_duration_ns"]), batches, missing)


def _sample_at(samples: Sequence[ProgressSample], warmup_end_ns: int) -> ProgressSample | None:
    """The sample a warmup end is at, or the last one before it if none is at it exactly."""
    before = [sample for sample in samples if sample.elapsed_ns <= warmup_end_ns]
    return before[-1] if before else None


def _run_detail(record: dict[str, Any], samples: list[ProgressSample], replayed: ReplayedRun | None) -> RunDetail:
    # Records written before a column existed lack it; every such mark is left off.
    def seconds(row: dict[str, Any], column: str) -> float | None:
        ns = row.get(column)
        return None if ns is None else ns / 1e9

    def markers(pairs: list[tuple[str, float | None]]) -> list[Marker]:
        return [Marker(label, value) for label, value in pairs if value is not None]

    # Under a replay, the page draws both decisions under the selected settings.
    end_of_run_row = record if replayed is None else {}
    would_be_row = record if replayed is None else {}
    end_of_run = end_of_run_row.get("steady_state_throughput")
    end_of_run_width = end_of_run_row.get("relative_half_width")
    would_be = would_be_row.get("would_be_steady_state_throughput")
    would_stop_ns = would_be_row.get("would_stop_ns")
    would_be_warmup_end_ns = would_be_row.get("would_be_warmup_end_ns")
    would_be_width = would_be_row.get("would_be_relative_half_width")
    precision = would_be_row.get("precision")
    batch_duration_ns = record.get("batch_duration_ns") if replayed is None else None

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


def selection_params(selection: Settings) -> list[alt.Parameter]:
    """Every setting the page selects, as Vega-Lite parameters: those `on_selected_pair` reads, which
    a click on the overview sets, and those of `stop_params`."""
    return [
        alt.param(name="batch_duration_ns", value=selection.batch_duration_ns),
        alt.param(name="window_groups", value=selection.window_groups),
        *stop_params(selection.precision, selection.consecutive_checks, selection.min_duration_ns),
    ]


def on_selected_pair(chart: alt.Chart) -> alt.Chart:
    """`chart` over rows of every pair of batch duration and window groups, keeping the selected
    pair's, at the settings of `selection_params`."""
    return chart.transform_filter(
        "datum.batch_duration_ns == batch_duration_ns && datum.window_groups == window_groups"
    )


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
    detail chart per run in `details`. If there is a `replay`, a control bar holds the settings it
    selects, an overview of its grid selects them too, and the headline and detail charts follow."""
    bar = None if replay is None else controls(replay, details)
    overview_section = ""
    if replay is not None:
        overview_section = _OVERVIEW_GUIDE + _embedded_spec("overview", overview_chart(overview(replay), replay.selection))
    return _PAGE.substitute(
        vega=alt.VEGA_VERSION,
        vega_lite=alt.VEGALITE_VERSION,
        vega_embed=alt.VEGAEMBED_VERSION,
        controls=_REPLAY_MISSING if replay is None else _controls(replay),
        overview=overview_section,
        headline_note="" if replay is None else _HEADLINE_NOTE,
        # Under a replay, the headline counts its runs itself, as the selection moves.
        caption=f'<p class="caption">{_caption(calibration)}</p>' if replay is None else "",
        settings=_settings_table(calibration.runs),
        headline=_embedded_spec("headline", headline_chart(calibration, replay)),
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
again, for an overview of the stopping rule under a grid of settings, a tightness check panel under
each detail chart, and sliders that move the stop.
</p>"""

_HEADLINE_NOTE = """<p>
The headline shows each shadow probe under the settings in the control bar: its would-be decision
is the page's stop over the selected pair's tightness checks, and its end-of-run estimate the one
under the selected pair. Under the settings a probe was recorded with, it shows what the probe
recorded.
</p>"""

_OVERVIEW_GUIDE = """<details>
<summary><h2>Reading the overview</h2></summary>
<p>
The overview shows every combination of settings in the replay grid, as <code>replay</code> judged
it with the stopping rule itself. A point is placed by the median would-stop time over the runs a
probe with its settings would have stopped steady, and by the worst relative error of their
would-be estimates against the end-of-run estimates under its batch duration and window groups.
Its colour counts the runs a probe would not have stopped steady: capped at the maximum duration,
or completed at the first partition end. The dashed line joins the Pareto frontier, the
combinations no other is both faster and more accurate than. The diamond is the combination the
shadow probes were recorded with. A combination under which no run would stop steady has no place
on those axes, and is listed by its settings under them.
</p>
<p>
Hover over a combination for its settings, how many of the runs it replayed a probe would have
stopped steady, how many of their would-be estimate intervals cover the end-of-run estimate, its
median stop and worst error, and the runs it could not replay, whose poll period is longer than its
batch duration. Click it to select it for the whole page: the sliders move to it, and the headline
and every detail chart follow. The selected combination is outlined; when the sliders sit between
the grid's values, the selected pair's combinations are outlined instead.
</p>
</details>
"""


def _controls(replay: Replay) -> str:
    """The control bar: a slider per setting the page's stop reads, over the grid's range, and the
    selected pair of batch duration and window groups, which a click on the overview moves."""
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
            f'<span id="pair">Batch duration {selection.batch_duration_ns / 1e9:g} s,'
            f" {selection.window_groups} window groups</span>",
            "</div>",
        ]
    )


def headline_chart(calibration: Calibration, replay: Replay | None = None) -> alt.TopLevelMixin:
    """One row per settled shadow run, then a strip of the runs that never settled: as each run
    recorded them, or under the settings `replay` selects, which the page moves."""
    if replay is not None:
        return _replayed_headline_chart(calibration, replay)
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
                alt.InlineData(values=rows),
                alt.Chart(),
                alt.Chart().mark_rule(strokeWidth=2).encode(x="low:Q", x2="high:Q", color=colour),
                alt.Chart().mark_point(filled=True, size=80).encode(
                    x="error:Q", color=colour, shape=_THREADS, tooltip=_headline_tooltip()
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
                alt.InlineData(values=rows),
                alt.Chart(),
                alt.Chart().mark_text(align="left", dx=4).encode(x=alt.datum(0), text="label:N", color=colour),
            )
        )
    return alt.vconcat(*charts).resolve_scale(x="shared")


_THREADS = alt.Shape("threads:N", title="Threads")


def _headline_tooltip() -> list[alt.Tooltip]:
    return [
        alt.Tooltip("run_id:N", title="Run"),
        alt.Tooltip("error:Q", title="Would-be estimate", format="+.2%"),
        alt.Tooltip("low:Q", title="Would-be interval from", format="+.2%"),
        alt.Tooltip("high:Q", title="Would-be interval to", format="+.2%"),
        alt.Tooltip("band_high:Q", title="End-of-run half-width", format=".2%"),
    ]


def _replayed_headline_chart(calibration: Calibration, replay: Replay) -> alt.TopLevelMixin:
    """The headline at the page's stop over every run's tightness checks under the selected pair,
    against the pair's end-of-run estimates, with a caption that counts them.

    As in the headline of the recorded decisions, a run is drawn if its stopping rule settled,
    whatever its maximum duration; one a probe would have been capped on first is hollow. The strip
    lists the runs that never settled, those a probe would have been capped on apart, and those not
    replayed under the selected pair.
    """
    colour = alt.Color("formulation:N", title="Formulation")
    # A run's status is the same on each of its checks, and every run has a check 0.
    judged = with_stop(on_selected_pair(alt.Chart())).transform_calculate(
        status="datum.replayed ? datum.probe_stop_reason : 'not replayed'",
        band_low="-datum.end_width",
        band_high="datum.end_width",
    )
    settled = judged.transform_filter("datum.stops_here").transform_calculate(
        error="datum.steady_state_throughput / datum.end - 1",
        low="datum.steady_state_throughput * (1 - datum.relative_half_width) / datum.end - 1",
        high="datum.steady_state_throughput * (1 + datum.relative_half_width) / datum.end - 1",
    )
    never_settled = judged.transform_filter(
        "datum.check_index == 0 && !isValid(datum.would_stop_ns)"
    ).transform_calculate(
        label="(datum.status == 'capped' ? 'never settled, capped at the maximum duration'"
        " : datum.status == 'completed' ? 'never settled' : 'not replayed under these settings')"
        " + ', ' + toString(datum.threads) + (datum.threads == 1 ? ' thread' : ' threads')"
    )
    data = alt.InlineData(values=_headline_checks(calibration, replay))
    return (
        alt.vconcat(
            _runs_chart(
                "Would-be estimate against the end-of-run estimate",
                data,
                settled,
                settled.mark_rule(strokeWidth=2).encode(x="low:Q", x2="high:Q", color=colour),
                settled.mark_point(filled=True, size=80, strokeWidth=1.5).encode(
                    x="error:Q",
                    color=colour,
                    # The stroke keeps a hollow point's colour; it shares the colour's scheme.
                    stroke=alt.Stroke("formulation:N", legend=None),
                    fillOpacity=alt.condition("datum.status == 'capped'", alt.value(0), alt.value(1)),
                    shape=_THREADS,
                    tooltip=[*_headline_tooltip(), alt.Tooltip("status:N", title="A probe would have ended")],
                ),
            ),
            _runs_chart(
                "No would-be estimate",
                data,
                never_settled,
                never_settled.mark_text(align="left", dx=4).encode(x=alt.datum(0), text="label:N", color=colour),
            ),
            _headline_caption(judged).properties(data=data),
        )
        .resolve_scale(x="shared")
        .add_params(*selection_params(replay.selection))
    )


def _headline_checks(calibration: Calibration, replay: Replay) -> list[dict[str, Any]]:
    """Every shadow run's tightness checks under every pair of the grid, as `with_stop` takes them,
    each with its run's formulation and threads and the pair's end-of-run estimate, `end`, and its
    relative half-width, `end_width`. A run not replayed under a pair has one row there that is
    not `replayed`, so that every run is drawn under every pair."""
    checks: dict[tuple[str, int, int], list[dict[str, Any]]] = {}
    for check in replay.checks.to_pylist():
        checks.setdefault((check["run_id"], check["batch_duration_ns"], check["window_groups"]), []).append(check)
    baselines = {
        (baseline["run_id"], baseline["batch_duration_ns"], baseline["window_groups"]): baseline
        for baseline in replay.baselines.to_pylist()
    }
    rows = []
    for record in calibration.runs.to_pylist():
        for batch_duration_ns, window_groups in replay.pairs:
            run = {
                "run_id": record["run_id"],
                "formulation": record["formulation"],
                "threads": record["threads"],
                "batch_duration_ns": batch_duration_ns,
                "window_groups": window_groups,
            }
            key = (record["run_id"], batch_duration_ns, window_groups)
            baseline = baselines.get(key)
            if baseline is None:
                rows.append(run | {"replayed": False, "check_index": 0, "elapsed_ns": 0})
                continue
            run |= {
                "replayed": True,
                "first_partition_end_ns": record.get("first_partition_end_ns"),
                "capped_at_ns": baseline["capped_at_ns"],
                "end": baseline["steady_state_throughput"],
                "end_width": baseline["relative_half_width"],
            }
            # A run that ended before its first probe batch did has no tightness check, and never
            # settled; a check that fails stands in for it.
            for check in checks.get(key) or [{"check_index": 0, "elapsed_ns": 0}]:
                rows.append(
                    run
                    | {
                        name: check.get(name)
                        for name in [
                            "check_index",
                            "elapsed_ns",
                            "warmup_end_ns",
                            "steady_state_throughput",
                            "relative_half_width",
                        ]
                    }
                )
    return rows


def _headline_caption(judged: alt.Chart) -> alt.Chart:
    """The caption of the headline: how many would-be estimate intervals cover the end-of-run
    estimate, how many runs never settled, and how many a probe would have been capped on or were
    not replayed. Every stop has an estimate interval, so none is counted without one."""
    first = "datum.check_index == 0"
    interval = "datum.stops_here && isValid(datum.relative_half_width)"
    covers = (
        f"{interval} && datum.steady_state_throughput * (1 - datum.relative_half_width) <= datum.end"
        " && datum.end <= datum.steady_state_throughput * (1 + datum.relative_half_width)"
    )
    counts = {
        "runs": first,
        "settled": "datum.stops_here",
        "intervals": interval,
        "covered": covers,
        "capped": f"{first} && datum.status == 'capped'",
        "never_settled": f"{first} && datum.status != 'not replayed' && !isValid(datum.would_stop_ns)",
        "not_replayed": f"{first} && datum.status == 'not replayed'",
    }

    def count(name: str) -> str:
        return f"toString(datum.{name})"

    sums: dict[str, Any] = {name: f"sum(is_{name})" for name in counts}
    return (
        judged.transform_calculate(**{f"is_{name}": f"{test} ? 1 : 0" for name, test in counts.items()})
        .transform_aggregate(**sums)
        # One sentence per line, as a text mark does not wrap.
        .transform_calculate(
            caption=f"(datum.intervals > 0 ? {count('covered')} + ' of ' + {count('intervals')}"
            " + ' would-be estimate intervals cover the end-of-run estimate."
            " About 95% should if the estimate interval is calibrated.\\n' : '')"
            f" + {count('never_settled')} + ' of ' + {count('runs')} + ' shadow runs never settled.'"
            f" + (datum.capped > 0 ? '\\n' + {count('capped')} + ' of ' + {count('runs')}"
            " + ' shadow runs would have been capped at their maximum duration.' : '')"
            f" + (datum.not_replayed > 0 ? '\\n' + {count('not_replayed')} + ' of ' + {count('runs')}"
            " + ' shadow runs were not replayed under these settings.' : '')"
        )
        .mark_text(align="left", baseline="top", fontWeight="bold", lineBreak="\n", lineHeight=16)
        .encode(x=alt.value(0), y=alt.value(0), text="caption:N")
        .properties(width=600, height=64, view=alt.ViewBackground(stroke=None))
    )


def _runs_chart(title: str, data: alt.InlineData, rows: alt.Chart, *marks: alt.Chart) -> alt.LayerChart:
    """One row per run of `rows` over its end-of-run estimate interval as a grey band, with `marks`
    on top."""
    x = alt.X(
        "band_low:Q",
        title="Would-be over end-of-run steady-state throughput, less 1",
        axis=alt.Axis(format="+%"),
    )
    # The rule at 0 has no y, so it spans every row.
    zero = alt.Chart().mark_rule(color="black").encode(x=alt.datum(0))
    y = alt.Y("run_id:N", title=None)
    return alt.layer(
        rows.mark_bar(color="#dddddd").encode(x, alt.X2("band_high:Q"), y=y),
        *(mark.encode(y=y) for mark in marks),
        zero,
        data=data,
    ).properties(title=title, width=600)


def overview_chart(combinations: list[Combination], selection: Settings) -> alt.TopLevelMixin:
    """Every combination of the replay grid, from `overview`.

    A combination some run would stop steady under is placed by its median stop time and its worst
    error, with the Pareto frontier joined; the rest are listed below by their settings. Colour
    counts the runs a probe would not have stopped steady, and a diamond marks the recorded
    combinations. The combination selected, at first `selection`, is outlined if the sliders sit on
    the grid's values, and otherwise the selected pair's combinations are.
    """
    pairs = sorted({(c.settings.batch_duration_ns, c.settings.window_groups) for c in combinations})
    stops = sorted({(c.settings.precision, c.settings.consecutive_checks, c.settings.min_duration_ns) for c in combinations})
    rows = []
    for combination in combinations:
        settings = combination.settings
        pair = (settings.batch_duration_ns, settings.window_groups)
        stop = (settings.precision, settings.consecutive_checks, settings.min_duration_ns)
        rows.append(
            asdict(settings)
            | {
                "settings": f"{_pair_label(settings)}, {_stop_label(settings)}",
                "pair": _pair_label(settings),
                "pair_order": pairs.index(pair),
                "stop": _stop_label(settings),
                "stop_order": stops.index(stop),
                "placed": combination.median_stop_ns is not None,
                "median_stop_s": None if combination.median_stop_ns is None else combination.median_stop_ns / 1e9,
                "worst_error": combination.worst_error,
                "not_steady": combination.not_steady,
                "stopped": f"{combination.steady} of {len(combination.runs)}",
                "coverage": f"{combination.covered} of {combination.intervals}",
                "not_replayed": ", ".join(combination.not_replayed) or "none",
                "pareto": combination.pareto,
                "recorded": combination.recorded,
            }
        )
    judged = (
        alt.Chart(alt.InlineData(values=rows))
        .transform_calculate(
            on_pair="datum.batch_duration_ns == batch_duration_ns && datum.window_groups == window_groups"
        )
        .transform_calculate(
            selected="datum.on_pair && datum.precision == precision"
            " && datum.consecutive_checks == consecutive_checks && datum.min_duration_ns == min_duration_ns ? 1 : 0"
        )
        .transform_joinaggregate(on_grid="max(selected)")
        .transform_calculate(highlighted="datum.on_grid == 1 ? datum.selected == 1 : datum.on_pair")
    )
    runs = max((len(combination.runs) for combination in combinations), default=1)
    colour = alt.Color(
        "not_steady:Q",
        title="Runs not stopped steady",
        scale=alt.Scale(domain=[0, runs], scheme="redyellowblue", reverse=True),
        legend=alt.Legend(values=list(range(runs + 1)), format="d"),
    )
    tooltip = [
        alt.Tooltip("settings:N", title="Settings"),
        alt.Tooltip("stopped:N", title="Runs stopped steady"),
        alt.Tooltip("coverage:N", title="Would-be estimate intervals covering"),
        alt.Tooltip("median_stop_s:Q", title="Median stop, seconds", format=".1f"),
        alt.Tooltip("worst_error:Q", title="Worst error", format=".2%"),
        alt.Tooltip("not_replayed:N", title="Not replayed"),
    ]
    outline = {
        "strokeWidth": alt.condition("datum.highlighted", alt.value(2.5), alt.value(0.5)),
        "opacity": alt.condition("datum.highlighted", alt.value(1), alt.value(0.5)),
    }
    recorded: dict[str, Any] = {"shape": "diamond", "size": 300, "filled": False, "color": "black"}
    listed_recorded: dict[str, Any] = recorded | {"size": 200}

    placed = judged.transform_filter("datum.placed")
    x = alt.X(
        "median_stop_s:Q",
        title="Median would-stop time over the runs stopped steady, seconds",
        scale=alt.Scale(type="log"),
    )
    y = alt.Y("worst_error:Q", title="Worst error of a would-be estimate", axis=alt.Axis(format="%"))
    charts = [
        alt.layer(
            placed.transform_filter("datum.pareto")
            .mark_line(color="#555555", strokeDash=[4, 2])
            .encode(x, y, order="median_stop_s:Q"),
            placed.mark_point(filled=True, size=70, stroke="black").encode(
                x, y, color=colour, tooltip=tooltip, **outline
            ),
            placed.transform_filter("datum.recorded").mark_point(**recorded).encode(x, y, tooltip=tooltip),
        ).properties(title="Settings combinations some run would stop steady under", width=600, height=300)
    ]
    if any(combination.median_stop_ns is None for combination in combinations):
        listed = judged.transform_filter("!datum.placed")
        column = alt.X(
            "pair:N", title="Batch duration, window groups", sort=alt.EncodingSortField("pair_order", op="min")
        )
        row = alt.Y(
            "stop:N",
            title="Precision, consecutive checks, minimum duration",
            sort=alt.EncodingSortField("stop_order", op="min"),
        )
        charts.append(
            alt.layer(
                listed.mark_square(size=80, stroke="black").encode(column, row, color=colour, tooltip=tooltip, **outline),
                listed.transform_filter("datum.recorded")
                .mark_point(**listed_recorded)
                .encode(column, row, tooltip=tooltip),
            ).properties(title="Settings combinations no run would stop steady under", height=alt.Step(14))
        )
    return alt.vconcat(*charts).resolve_scale(color="shared").add_params(*selection_params(selection))


def _pair_label(settings: Settings) -> str:
    return f"{settings.batch_duration_ns / 1e9:g} s batches, {settings.window_groups} window groups"


def _stop_label(settings: Settings) -> str:
    return (
        f"{settings.precision * 100:g}%, {settings.consecutive_checks} consecutive,"
        f" {settings.min_duration_ns / 1e9:g} s minimum"
    )


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

    A replayed run is drawn under the settings `bar` selects, which the page moves: its batch rates
    and end-of-run marks under the selected pair, and its would-be decision at the page's stop over
    the selected pair's tightness checks. Under the rates go its tightness check panel and its end
    of warmup at each check. The rate axis reaches every stop the page can move to.
    """
    replayed = detail.replayed
    if replayed is None or bar is None:
        return _rates_chart(detail).properties(title=_detail_title(detail, replay_tables=bar is not None))
    checks = alt.InlineData(values=replayed.checks)
    return (
        alt.vconcat(
            _rates_chart(detail, bar.max_precision),
            _tightness_check_panel(checks, bar.half_widths, replayed.missing),
            _warmup_strip(checks),
        )
        # The rates and the tightness checks each have their own colours.
        .resolve_scale(x="shared", color="independent")
        .add_params(*selection_params(bar.selection))
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
    if detail.replayed is None and not detail.settled:
        subtitle.append("The stopping rule never settled.")
    if not detail.samples:
        subtitle.append("No progress samples were recorded.")
    return alt.TitleParams(detail.run_id, subtitle=subtitle, anchor="start")


def _rates_chart(detail: RunDetail, max_precision: float | None = None) -> alt.LayerChart:
    """The run's rates and the marks it carries, with its batch rates, end-of-run marks and the
    would-be decision the page finds under the selected settings if it was replayed."""
    replayed = detail.replayed
    batches: list[dict[str, Any]] = [asdict(batch) for batch in detail.batches]
    # The legend lists only the marks the run has, each in its fixed colour.
    labels = {marker.label for marker in detail.times + detail.levels} | {bar.label for bar in detail.intervals}
    if replayed is not None:
        batches = replayed.batches
        labels |= set(_STOP_MARKS) | _end_of_run_labels(replayed.baselines)
    present = {"sample rate": detail.samples, "batch rate": batches, "running estimate": detail.running}
    labels |= {label for label, items in present.items() if items}
    shown = {label: colour for label, colour in _DETAIL_COLOURS.items() if label in labels}
    colour = alt.Color(
        "mark:N",
        title=None,
        scale=alt.Scale(domain=list(shown), range=list(shown.values())),
        legend=alt.Legend(orient="bottom", columns=3),
    )
    # Raw sample rates swing far wider than anything else, so they are clipped to the rest.
    rates = [batch["rate"] for batch in batches]
    rates += [marker.value for marker in detail.levels]
    rates += [point.rate for point in detail.running]
    rates += [bound for bar in detail.intervals for bound in (bar.low, bar.high)]
    rates += list(detail.precision_band or ())
    if replayed is not None:
        # Every stop the page reaches has an estimate interval narrower than its largest precision.
        rates += [
            bound
            for check in replayed.checks
            if check["warmup_end_ns"] is not None
            and check["relative_half_width"] is not None
            and check["steady_state_throughput"] is not None
            and (max_precision is None or check["relative_half_width"] < max_precision)
            for bound in _around(check["steady_state_throughput"], check["relative_half_width"])
        ]
        for baseline in replayed.baselines:
            end = baseline["steady_state_throughput"]
            if end is None:
                continue
            rates.append(end)
            if baseline["relative_half_width"] is not None:
                rates += _around(end, baseline["relative_half_width"])
            if max_precision is not None:
                rates += _around(end, max_precision)
    y_scale = alt.Scale(zero=False)
    if rates:
        pad = (max(rates) - min(rates)) * 0.1 or max(rates) * 0.05
        y_scale = alt.Scale(domain=[min(rates) - pad, max(rates) + pad])
    y = alt.Y("rate:Q", title="Rows per second", scale=y_scale)

    def rows(mark: str, items: list[Any]) -> alt.InlineData:
        return alt.InlineData(values=[{"mark": mark, **asdict(item)} for item in items])

    batch_rates = alt.Chart(alt.InlineData(values=[{"mark": "batch rate", **batch} for batch in batches]))
    if replayed is not None:
        batch_rates = batch_rates.transform_filter("datum.batch_duration_ns == batch_duration_ns")
    layers = [
        alt.Chart(rows("sample rate", detail.samples))
        .mark_rule(clip=True, strokeWidth=1.5)
        .encode(alt.X("start_s:Q", axis=_SECONDS), x2="end_s:Q", y=y, color=colour),
        batch_rates.mark_rule(strokeWidth=2.5).encode(
            alt.X("start_s:Q", axis=_SECONDS),
            x2="end_s:Q",
            y=y,
            color=colour,
            tooltip=[alt.Tooltip("rate:Q", title="Batch rate", format=",.0f")],
        ),
        alt.Chart(alt.InlineData(values=[{"mark": marker.label, "rate": marker.value} for marker in detail.levels]))
        .mark_rule(strokeDash=[6, 3])
        .encode(y=y, color=colour, tooltip=[alt.Tooltip("mark:N"), alt.Tooltip("rate:Q", format=",.0f")]),
        alt.Chart(alt.InlineData(values=[{"mark": marker.label, "elapsed_s": marker.value} for marker in detail.times]))
        .mark_rule(strokeDash=[4, 4])
        .encode(
            alt.X("elapsed_s:Q", axis=_SECONDS),
            color=colour,
            tooltip=[alt.Tooltip("mark:N"), _seconds_tooltip("elapsed_s")],
        ),
        alt.Chart(rows("running estimate", detail.running))
        .mark_line(strokeWidth=2)
        .encode(alt.X("elapsed_s:Q", axis=_SECONDS), y=y, color=colour),
        alt.Chart(alt.InlineData(values=[asdict(bar) | {"mark": bar.label} for bar in detail.intervals]))
        .mark_rule(strokeWidth=4)
        .encode(
            alt.X("elapsed_s:Q", axis=_SECONDS),
            alt.Y("low:Q", scale=y_scale),
            y2="high:Q",
            color=colour,
            tooltip=_interval_tooltip(),
        ),
    ]
    precision_band = None
    if detail.precision_band is not None:
        low, high = detail.precision_band
        precision_band = alt.Chart(alt.InlineData(values=[{"low": low, "high": high}]))
    if replayed is not None:
        baselines = on_selected_pair(alt.Chart(alt.InlineData(values=replayed.baselines)))
        layers += _end_of_run_marks(baselines, y, y_scale, colour)
        layers += _would_be_marks(alt.InlineData(values=replayed.checks), y, y_scale, colour)
        precision_band = baselines.transform_filter("isValid(datum.steady_state_throughput)").transform_calculate(
            mark="'precision band'",
            low="datum.steady_state_throughput * (1 - precision)",
            high="datum.steady_state_throughput * (1 + precision)",
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


def _end_of_run_labels(baselines: list[dict[str, Any]]) -> set[str]:
    """The end-of-run marks some pair of a replayed run has."""
    columns = {
        END_OF_RUN_ESTIMATE: "steady_state_throughput",
        "end-of-run warmup end": "warmup_end_ns",
        MAXIMUM_DURATION: "capped_at_ns",
    }
    return {label for label, column in columns.items() if any(baseline[column] is not None for baseline in baselines)}


def _end_of_run_marks(baselines: alt.Chart, y: alt.Y, y_scale: alt.Scale, colour: alt.Color) -> list[alt.Chart]:
    """The end-of-run warmup end, estimate and estimate interval, and the sample that caps a probe,
    under the selected pair."""

    def time(mark: str, ns: str) -> alt.Chart:
        return (
            baselines.transform_filter(f"isValid(datum.{ns})")
            .transform_calculate(mark=f"'{mark}'", elapsed_s=f"datum.{ns} / 1e9")
            .mark_rule(strokeDash=[4, 4])
            .encode(
                alt.X("elapsed_s:Q", axis=_SECONDS),
                color=colour,
                tooltip=[alt.Tooltip("mark:N"), _seconds_tooltip("elapsed_s")],
            )
        )

    estimate = baselines.transform_filter("isValid(datum.steady_state_throughput)")
    # Each estimate interval is drawn where its measurement window ends.
    return [
        time("end-of-run warmup end", "warmup_end_ns"),
        time(MAXIMUM_DURATION, "capped_at_ns"),
        estimate.transform_calculate(mark=f"'{END_OF_RUN_ESTIMATE}'", rate="datum.steady_state_throughput")
        .mark_rule(strokeDash=[6, 3])
        .encode(y=y, color=colour, tooltip=[alt.Tooltip("mark:N"), alt.Tooltip("rate:Q", format=",.0f")]),
        estimate.transform_filter("isValid(datum.relative_half_width) && isValid(datum.window_end_ns)")
        .transform_calculate(
            mark=f"'{END_OF_RUN_ESTIMATE}'",
            elapsed_s="datum.window_end_ns / 1e9",
            low="datum.steady_state_throughput * (1 - datum.relative_half_width)",
            high="datum.steady_state_throughput * (1 + datum.relative_half_width)",
        )
        .mark_rule(strokeWidth=4)
        .encode(
            alt.X("elapsed_s:Q", axis=_SECONDS),
            alt.Y("low:Q", scale=y_scale),
            y2="high:Q",
            color=colour,
            tooltip=_interval_tooltip(),
        ),
    ]


def _would_be_marks(checks: alt.InlineData, y: alt.Y, y_scale: alt.Scale, colour: alt.Color) -> list[alt.Chart]:
    """The would-stop point, the would-be warmup end, estimate and estimate interval, and the
    running estimate, at the page's stop over `checks`."""
    stop = _stop_over(checks).transform_filter("datum.stops_here")

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


def _running_estimate(checks: alt.InlineData) -> alt.Chart:
    """The running estimate from the page's stop: at each later tightness check, the rows since the
    sample of the stop's warmup end over the time since."""
    return (
        _stop_over(checks)
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


def _tightness_check_panel(
    checks: alt.InlineData, half_widths: tuple[float, float], missing: list[tuple[int, int]]
) -> alt.LayerChart:
    """Each tightness check of the selected pair's relative half-width against the precision, on
    the axis `half_widths`, coloured by whether it passes and hollow if it found no end of warmup,
    with the minimum duration, the stop and how a probe would have ended. Under a pair the run is
    `missing`, a note says why it has no tightness checks.

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
        _stop_over(checks)
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
        alt.Chart(alt.InlineData(values=[{}]))
        .transform_calculate(relative_half_width="precision", mark="'precision'")
        .mark_rule(color="black", clip=True)
        .encode(y, tooltip=[alt.Tooltip("relative_half_width:Q", title="Precision", format=".2%")]),
        alt.Chart(alt.InlineData(values=[{}]))
        .transform_calculate(elapsed_s="min_duration_ns / 1e9", mark="'minimum duration'")
        .mark_rule(color="black", strokeDash=[2, 2])
        .encode(alt.X("elapsed_s:Q", axis=_SECONDS), tooltip=[_seconds_tooltip("elapsed_s", "Minimum duration")]),
        _stop_over(checks)
        .transform_filter("datum.stops_here")
        .transform_calculate(elapsed_s="datum.elapsed_ns / 1e9")
        .mark_rule(color=_DETAIL_COLOURS["would-stop"], strokeDash=[4, 4], clip=True)
        .encode(alt.X("elapsed_s:Q", axis=_SECONDS)),
        # How a probe would have ended is the same on every check of the run; one says it.
        _stop_over(checks)
        .transform_filter("datum.check_index == 0")
        .transform_calculate(reason=reason)
        .mark_text(align="left", baseline="bottom", dy=-4)
        .encode(x=alt.value(0), y=alt.value(0), text="reason:N"),
        on_selected_pair(
            alt.Chart(
                alt.InlineData(
                    values=[
                        {
                            "batch_duration_ns": batch_duration_ns,
                            "window_groups": window_groups,
                            "note": f"Not replayed under {batch_duration_ns / 1e9:g} s batches, as they are"
                            " shorter than the run's poll period.",
                        }
                        for batch_duration_ns, window_groups in missing
                    ]
                )
            )
        )
        .mark_text(align="left", baseline="bottom", dy=-4)
        .encode(x=alt.value(0), y=alt.value(0), text="note:N"),
    ).properties(width=600, height=150)


def _warmup_strip(checks: alt.InlineData) -> alt.Chart:
    """The end of warmup each tightness check found, at the check's time."""
    return (
        on_selected_pair(alt.Chart(checks))
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


def _stop_over(checks: alt.InlineData) -> alt.Chart:
    """The selected pair's tightness checks among `checks`, with the page's stop over them."""
    return with_stop(on_selected_pair(alt.Chart(checks)))


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
    ("Row ordering", "row_ordering", str),
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
$overview

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
interval. When the metrics directory has replay tables, the headline follows the selected settings.
A hollow point is a run that settled only after the sample that caps a probe at its maximum
duration, so a probe would have been capped first. The strip below lists the runs that never
settled, saying which a probe would have been capped on, and the runs not replayed under the
selected batch duration.
</p>
</details>
$headline_note
$headline
$caption
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
batch duration and window groups, which a click on the overview selects, decide the run's tightness
checks, one at the end of each probe batch, and its end-of-run marks. Its precision, consecutive checks and minimum duration decide
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
// The settings the page selects, each a parameter of every view that follows the selection.
const SETTINGS = ["batch_duration_ns", "window_groups", "precision", "consecutive_checks", "min_duration_ns"];
// Every setting moved since the page opened, so that a view that loads late catches up.
const selection = {};
const selectionViews = [];
function apply(view, settings) {
  for (const [name, value] of Object.entries(settings)) view.signal(name, value);
  view.runAsync();
}
function select(settings) {
  Object.assign(selection, settings);
  for (const view of selectionViews) apply(view, settings);
}
function showSlider(input) {
  input.nextElementSibling.textContent = input.value + (input.dataset.scale === "1" ? "" : " s");
}
function selectCombination(datum) {
  for (const input of document.querySelectorAll(".controls input")) {
    input.value = datum[input.dataset.param] / Number(input.dataset.scale);
    showSlider(input);
  }
  document.getElementById("pair").textContent =
    "Batch duration " + datum.batch_duration_ns / 1e9 + " s, " + datum.window_groups + " window groups";
  select(Object.fromEntries(SETTINGS.map((name) => [name, datum[name]])));
}
for (const spec of document.querySelectorAll('script[type="application/json"]')) {
  const parsed = JSON.parse(spec.textContent);
  vegaEmbed("#" + spec.id + "-chart", parsed).then((result) => {
    if ((parsed.params || []).some((param) => param.name === "precision")) {
      selectionViews.push(result.view);
      apply(result.view, selection);
    }
    if (spec.id === "overview") {
      result.view.addEventListener("click", (event, item) => {
        // The frontier's line stands for several combinations, so a click on it selects none.
        if (item && item.mark.marktype !== "line" && item.datum && item.datum.batch_duration_ns !== undefined) {
          selectCombination(item.datum);
        }
      });
    }
    if (spec.id !== "headline") return;
    result.view.addEventListener("click", (event, item) => {
      if (item && item.datum && item.datum.run_id) showDetail(item.datum.run_id);
    });
  });
}
for (const input of document.querySelectorAll(".controls input")) {
  input.addEventListener("input", () => {
    showSlider(input);
    // Rounded, so that a slider on a grid value selects it exactly despite floating point steps.
    select({ [input.dataset.param]: Number((Number(input.value) * Number(input.dataset.scale)).toPrecision(12)) });
  });
}
</script>
</body>
</html>
""")
