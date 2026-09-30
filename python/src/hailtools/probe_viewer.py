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
    before = [sample for sample in samples if sample.elapsed_ns <= warmup_end_ns]
    if not before:
        return []
    start = before[-1]
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
class RunDetail:
    """How one shadow run's throughput evolved, and where its stopping rule's decisions fall.

    `samples` are the rates between consecutive progress samples and `batches` the rates of the
    batches rebuilt from them. `times` mark the warmup ends, the would-stop point and the first
    partition end, and `levels` the steady-state throughputs, each where the run recorded it.
    `precision_band` is the end-of-run estimate plus or minus the run's precision. `running` is the
    running estimate from the would-stop point to the end of the run. A run that never `settled`
    has no would-be marks and no running estimate.
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


WOULD_BE_ESTIMATE = "would-be estimate"
END_OF_RUN_ESTIMATE = "end-of-run estimate"


def run_details(metrics_dir: Path, calibration: Calibration) -> list[RunDetail]:
    """The detail of each shadow run in `calibration`, from its progress samples under `metrics_dir`."""
    return [
        _run_detail(record, _progress(metrics_dir, record["run_id"]))
        for record in calibration.runs.to_pylist()
    ]


def _progress(metrics_dir: Path, run_id: str) -> list[ProgressSample]:
    path = metrics_dir / "progress" / f"{run_id}.parquet"
    if not path.exists():
        return []
    table = pq.read_table(path, columns=["sample_index", "elapsed_ns", "rows"]).sort_by("sample_index")
    return [ProgressSample(record["elapsed_ns"], record["rows"]) for record in table.to_pylist()]


def _run_detail(record: dict[str, Any], samples: list[ProgressSample]) -> RunDetail:
    # Records written before a column existed lack it; every such mark is left off.
    def seconds(column: str) -> float | None:
        ns = record.get(column)
        return None if ns is None else ns / 1e9

    def markers(pairs: list[tuple[str, float | None]]) -> list[Marker]:
        return [Marker(label, value) for label, value in pairs if value is not None]

    end_of_run = record.get("steady_state_throughput")
    would_be = record.get("would_be_steady_state_throughput")
    would_stop_ns = record.get("would_stop_ns")
    would_be_warmup_end_ns = record.get("would_be_warmup_end_ns")
    batch_duration_ns = record.get("batch_duration_ns")
    precision = record.get("precision")
    would_be_width = record.get("would_be_relative_half_width")
    end_of_run_width = record.get("relative_half_width")

    # Each estimate interval is drawn where its measurement window ends.
    window_end = seconds("window_end_ns")
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
        settled=would_stop_ns is not None,
        samples=[
            _rate(first, second)
            for first, second in zip(samples, samples[1:])
            if second.elapsed_ns > first.elapsed_ns
        ],
        batches=[] if batch_duration_ns is None else batch_rates(samples, batch_duration_ns),
        times=markers(
            [
                ("would-be warmup end", seconds("would_be_warmup_end_ns")),
                ("end-of-run warmup end", seconds("warmup_end_ns")),
                ("would-stop", seconds("would_stop_ns")),
                ("first partition end", seconds("first_partition_end_ns")),
            ]
        ),
        levels=markers([(WOULD_BE_ESTIMATE, would_be), (END_OF_RUN_ESTIMATE, end_of_run)]),
        precision_band=None if end_of_run is None or precision is None else _around(end_of_run, precision),
        running=running,
        intervals=intervals,
    )


def _around(centre: float, relative_half_width: float) -> tuple[float, float]:
    return centre * (1 - relative_half_width), centre * (1 + relative_half_width)


def _rate(start: ProgressSample, end: ProgressSample) -> Rate:
    seconds = (end.elapsed_ns - start.elapsed_ns) / 1e9
    return Rate(start.elapsed_ns / 1e9, end.elapsed_ns / 1e9, (end.rows - start.rows) / seconds)


def write_page(metrics_dir: Path, output: Path | None = None) -> Path:
    """Write the page about the shadow runs under `metrics_dir`, by default into it."""
    runs = calibration(metrics_dir)
    page = render_page(runs, run_details(metrics_dir, runs))
    output = output if output is not None else metrics_dir / "probe-viewer.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(page)
    return output


def render_page(calibration: Calibration, details: list[RunDetail]) -> str:
    """The page about `calibration`: fixed framing, the settings table, the headline chart and a
    detail chart per run in `details`."""
    return _PAGE.substitute(
        vega=alt.VEGA_VERSION,
        vega_lite=alt.VEGALITE_VERSION,
        vega_embed=alt.VEGAEMBED_VERSION,
        caption=_caption(calibration),
        settings=_settings_table(calibration.runs),
        headline=_embedded_spec("headline", headline_chart(calibration)),
        details="\n".join(
            f'<section class="detail" data-run="{html.escape(detail.run_id)}">\n'
            f"<h3>{html.escape(detail.run_id)}</h3>\n"
            f"{_embedded_spec(f'detail-{index}', detail_chart(detail))}\n"
            "</section>"
            for index, detail in enumerate(details)
        ),
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
}


def detail_chart(detail: RunDetail) -> alt.TopLevelMixin:
    """The run's sample and batch rates over time, with its stopping rule's decisions on them."""
    # The legend lists only the marks the run has, each in its fixed colour.
    present = {"sample rate": detail.samples, "batch rate": detail.batches, "running estimate": detail.running}
    labels = {marker.label for marker in detail.times + detail.levels} | {bar.label for bar in detail.intervals}
    labels |= {label for label, items in present.items() if items}
    shown = {label: colour for label, colour in _DETAIL_COLOURS.items() if label in labels}
    colour = alt.Color(
        "mark:N",
        title=None,
        scale=alt.Scale(domain=list(shown), range=list(shown.values())),
        legend=alt.Legend(orient="bottom", columns=3),
    )
    seconds = alt.Axis(title="Seconds since execution started")
    # Raw sample rates swing far wider than anything else, so they are clipped to the rest.
    rates = [batch.rate for batch in detail.batches]
    rates += [marker.value for marker in detail.levels]
    rates += [point.rate for point in detail.running]
    rates += [bound for bar in detail.intervals for bound in (bar.low, bar.high)]
    rates += list(detail.precision_band or ())
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
        .encode(alt.X("start_s:Q", axis=seconds), x2="end_s:Q", y=y, color=colour),
        alt.Chart(rows("batch rate", detail.batches))
        .mark_rule(strokeWidth=2.5)
        .encode(
            alt.X("start_s:Q", axis=seconds),
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
            alt.X("elapsed_s:Q", axis=seconds),
            color=colour,
            tooltip=[alt.Tooltip("mark:N"), alt.Tooltip("elapsed_s:Q", title="Seconds", format=".1f")],
        ),
        alt.Chart(rows("running estimate", detail.running))
        .mark_line(strokeWidth=2)
        .encode(alt.X("elapsed_s:Q", axis=seconds), y=y, color=colour),
        alt.Chart(alt.Data(values=[asdict(bar) | {"mark": bar.label} for bar in detail.intervals]))
        .mark_rule(strokeWidth=4)
        .encode(
            alt.X("elapsed_s:Q", axis=seconds),
            alt.Y("low:Q", scale=y_scale),
            y2="high:Q",
            color=colour,
            tooltip=[
                alt.Tooltip("mark:N"),
                alt.Tooltip("low:Q", title="Interval from", format=",.0f"),
                alt.Tooltip("high:Q", title="Interval to", format=",.0f"),
            ],
        ),
    ]
    if detail.precision_band is not None:
        low, high = detail.precision_band
        # The band has no x, so it spans the chart; it goes first to sit behind everything.
        layers.insert(
            0,
            alt.Chart(alt.Data(values=[{"low": low, "high": high}]))
            .mark_rect(color=_DETAIL_COLOURS[END_OF_RUN_ESTIMATE], opacity=0.12)
            .encode(alt.Y("low:Q", scale=y_scale), y2="high:Q"),
        )
    subtitle = []
    if not detail.settled:
        subtitle.append("The stopping rule never settled.")
    if not detail.samples:
        subtitle.append("No progress samples were recorded.")
    return (
        alt.layer(*layers)
        .properties(title=alt.TitleParams(detail.run_id, subtitle=subtitle), width=600, height=250)
        .interactive()
    )


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
.detail { scroll-margin-top: 1rem; padding: 0.5rem; border-radius: 4px; }
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

<h2>Reading the headline</h2>
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
$headline
<p class="caption">$caption</p>
<p>Click a run in the headline to bring its detail chart into view.</p>

<h2>Reading the detail charts</h2>
<p>
Each shadow run has a chart of how its throughput evolved over the run, in rows per second against
seconds since execution started, with the stopping rule's decisions drawn on it. It shows whether
the warmup ends where the throughput levels off, whether the throughput drifts over the run,
and whether the running estimate stays inside the would-be estimate interval.
</p>
<p>
The dark horizontal segments are batch rates, one per batch. They are rebuilt from the progress
samples using the run's batch duration: a batch ends at the first sample at least a batch duration
after the previous batch end. This is for display, not a copy of the stopping rule. The thin grey
segments behind them are the rates between consecutive progress samples, cut off where they leave
the chart.
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
ends: at the first partition end if the run reached one, which can be before the end of the run. The bars are short
beside the warmup: scroll over a chart to zoom, drag to pan, and double-click to reset.
</p>
<p>
A run whose stopping rule never settled has no would-be marks and no running estimate, only its
rates and its end-of-run marks.
</p>
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
for (const spec of document.querySelectorAll('script[type="application/json"]')) {
  vegaEmbed("#" + spec.id + "-chart", JSON.parse(spec.textContent)).then((result) => {
    if (spec.id !== "headline") return;
    result.view.addEventListener("click", (event, item) => {
      if (item && item.datum && item.datum.run_id) showDetail(item.datum.run_id);
    });
  });
}
</script>
</body>
</html>
""")
