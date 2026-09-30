"""The probe viewer: one self-contained HTML page about the shadow probes under a metrics directory.

It answers whether the estimate a shadow probe would have stopped with, and its estimate interval,
is calibrated against the steady-state throughput the run reaches at its end. Estimates are drawn
relative to that end-of-run estimate, and each estimate interval as its relative half-width around
the estimate it belongs to.
"""

from collections.abc import Callable
from dataclasses import dataclass
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


def write_page(metrics_dir: Path, output: Path | None = None) -> Path:
    """Write the page about the shadow runs under `metrics_dir`, by default into it."""
    page = render_page(calibration(metrics_dir))
    output = output if output is not None else metrics_dir / "probe-viewer.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(page)
    return output


def render_page(calibration: Calibration) -> str:
    """The page about `calibration`: fixed framing, the settings table and the headline chart."""
    return _PAGE.substitute(
        vega=alt.VEGA_VERSION,
        vega_lite=alt.VEGALITE_VERSION,
        vega_embed=alt.VEGAEMBED_VERSION,
        caption=_caption(calibration),
        settings=_settings_table(calibration.runs),
        headline=_embedded_spec("headline", headline_chart(calibration)),
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

<h2>Settings</h2>
<p>The settings each shadow run was recorded with. An empty cell is a setting the run record leaves empty.</p>
$settings

<script>
for (const spec of document.querySelectorAll('script[type="application/json"]')) {
  vegaEmbed("#" + spec.id + "-chart", JSON.parse(spec.textContent));
}
</script>
</body>
</html>
""")
