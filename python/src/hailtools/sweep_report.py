"""The sweep report: a campaign's figures per cell and runner shape, over repetitions.

A campaign's metrics directory holds a run record for each run, and in runners/ a record of each
runner that ran a cell list, naming its machine type, threads per core, and the run id of each
cell it ran. The report joins the two, and for each cell on each runner shape gives the whole-run
and steady-state normalized throughput, and the peak RSS, of its repetitions.
"""

from dataclasses import dataclass
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


class SweepReportError(Exception):
    """Anything the caller is expected to read and act on, rather than a traceback."""


def normalized_throughput(throughput: float, branching_factor: int, threads: int) -> float:
    """Throughput times the natural log of the branching factor, over the thread count."""
    return throughput * math.log(branching_factor) / threads


@dataclass(frozen=True)
class Run:
    """One recorded run of a cell, on the runner that ran it."""

    cell: str
    runner: str
    machine_type: str
    threads_per_core: int
    repetition: int
    run_id: str
    branching_factor: int
    threads: int
    rows_written: int
    run_ns: int
    steady_state_throughput: float | None
    peak_rss_bytes: int | None

    @property
    def whole_run_throughput(self) -> float:
        """Rows written per second over the whole run."""
        return self.rows_written / (self.run_ns / 1e9)

    @property
    def whole_run_normalized(self) -> float:
        return normalized_throughput(self.whole_run_throughput, self.branching_factor, self.threads)

    @property
    def steady_state_normalized(self) -> float | None:
        if self.steady_state_throughput is None:
            return None
        return normalized_throughput(self.steady_state_throughput, self.branching_factor, self.threads)


@dataclass(frozen=True)
class CellFigures:
    """A cell's runs on one runner shape, one per repetition that recorded a run, in repetition
    order."""

    cell: str
    machine_type: str
    threads_per_core: int
    runs: list[Run]

    @property
    def whole_run(self) -> list[float]:
        return [run.whole_run_normalized for run in self.runs]

    @property
    def steady_state(self) -> list[float]:
        return [value for run in self.runs if (value := run.steady_state_normalized) is not None]

    @property
    def steady_over_whole(self) -> float | None:
        """Mean steady-state over mean whole-run normalized throughput, over the repetitions with a
        steady-state estimate: how much warmup costs."""
        estimated = [run for run in self.runs if run.steady_state_normalized is not None]
        if not estimated:
            return None
        return mean(run.steady_state_normalized for run in estimated) / mean(
            run.whole_run_normalized for run in estimated
        )

    @property
    def peak_rss_bytes(self) -> int | None:
        """The largest peak RSS of any repetition."""
        peaks = [run.peak_rss_bytes for run in self.runs if run.peak_rss_bytes is not None]
        return max(peaks, default=None)


@dataclass(frozen=True)
class FailedCell:
    """A cell a runner ran that exited with a nonzero status, or recorded no run."""

    runner: str
    cell: str
    run_id: str
    exit_status: int


@dataclass(frozen=True)
class Report:
    figures: list[CellFigures]
    failed: list[FailedCell]
    # Runners whose status is not done: still running, failed, or interrupted.
    unfinished: list[tuple[str, str]]


def report(metrics_dir: Path) -> Report:
    """Join the run records under `metrics_dir` to its runner records, grouping by cell and
    runner shape.

    A runner records its cells' outcomes only when it finishes, so the join goes through each
    runner's cell order, naming each cell's run `<cell>-<runner>` as runner.sh does. A run that
    recorded itself counts even when its runner never recorded its outcome, as when the runner
    reached its maximum run duration."""
    runners = _runners(metrics_dir)
    records = _run_records(metrics_dir)
    groups: dict[tuple[str, str, int], list[Run]] = {}
    failed = []
    for runner in runners:
        outcomes = {outcome["cell"]: outcome for outcome in runner["cells"]}
        for cell in runner["cell_order"]:
            run_id = f"{cell}-{runner['runner']}"
            outcome = outcomes.get(cell)
            record = records.get(run_id)
            if outcome is not None and (outcome["exit_status"] != 0 or record is None):
                failed.append(FailedCell(runner["runner"], cell, run_id, outcome["exit_status"]))
                continue
            if record is None:
                # The runner never reached this cell, or never finished it.
                continue
            run = Run(
                cell=cell,
                runner=runner["runner"],
                machine_type=runner["machine_type"],
                threads_per_core=int(runner["threads_per_core"]),
                repetition=int(runner["repetition"]),
                run_id=run_id,
                branching_factor=record["input_tables"],
                threads=record["threads"],
                rows_written=record["rows_written"],
                run_ns=record["run_ns"],
                steady_state_throughput=record.get("steady_state_throughput"),
                peak_rss_bytes=record.get("peak_rss_bytes"),
            )
            groups.setdefault((run.cell, run.machine_type, run.threads_per_core), []).append(run)
    figures = [
        CellFigures(cell, machine_type, threads_per_core, sorted(runs, key=lambda run: run.repetition))
        for (cell, machine_type, threads_per_core), runs in sorted(groups.items())
    ]
    unfinished = [(runner["runner"], runner["status"]) for runner in runners if runner["status"] != "done"]
    return Report(figures, failed, unfinished)


def _runners(metrics_dir: Path) -> list[dict[str, Any]]:
    paths = sorted((metrics_dir / "runners").glob("*.json"))
    if not paths:
        raise SweepReportError(f"{metrics_dir} has no runner records under runners/")
    return [json.loads(path.read_text()) for path in paths]


def _run_records(metrics_dir: Path) -> dict[str, dict[str, Any]]:
    paths = sorted((metrics_dir / "runs").glob("*.parquet"))
    if not paths:
        return {}
    # Records written before a column existed lack it; permissive promotion leaves it empty.
    runs = pa.concat_tables([pq.read_table(path) for path in paths], promote_options="permissive")
    return {record["run_id"]: record for record in runs.to_pylist()}


def render_markdown(report: Report) -> str:
    """The report as Markdown: a table with one row per cell and runner shape, then a table with
    one row per repetition of each, then any failed cells and unfinished runners. Throughputs are in
    millions of rows per second. In the first table each is a mean over repetitions, ± half their
    range as a percentage of that mean."""
    lines = [
        "| cell | machine type | tpc | n | whole-run NT | steady-state NT | steady / whole | peak RSS GiB |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for cell in report.figures:
        ratio = cell.steady_over_whole
        lines.append(
            f"| {cell.cell} | {cell.machine_type} | {cell.threads_per_core} | {len(cell.runs)} "
            f"| {_mean_and_range(cell.whole_run)} | {_mean_and_range(cell.steady_state)} "
            f"| {'' if ratio is None else f'{ratio:.3f}'} "
            f"| {_gib(cell.peak_rss_bytes)} |"
        )
    lines += [
        "",
        "| cell | machine type | tpc | repetition | whole-run NT | steady-state NT | steady / whole | peak RSS GiB |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for cell in report.figures:
        for run in cell.runs:
            steady = run.steady_state_normalized
            lines.append(
                f"| {cell.cell} | {cell.machine_type} | {cell.threads_per_core} | {run.repetition} "
                f"| {run.whole_run_normalized / 1e6:.2f} "
                f"| {'' if steady is None else f'{steady / 1e6:.2f}'} "
                f"| {'' if steady is None else f'{steady / run.whole_run_normalized:.3f}'} "
                f"| {_gib(run.peak_rss_bytes)} |"
            )
    if report.failed:
        lines += ["", "Failed cells:", ""]
        lines += [
            f"- {failed.run_id}: exit status {failed.exit_status}"
            + ("" if failed.exit_status != 0 else ", no run record")
            for failed in report.failed
        ]
    if report.unfinished:
        lines += ["", "Unfinished runners:", ""]
        lines += [f"- {runner}: {status}" for runner, status in report.unfinished]
    return "\n".join(lines) + "\n"


def _gib(bytes_: int | None) -> str:
    return "" if bytes_ is None else f"{bytes_ / 2**30:.2f}"


def _mean_and_range(values: list[float]) -> str:
    if not values:
        return ""
    centre = mean(values)
    spread = (max(values) - min(values)) / centre
    return f"{centre / 1e6:.2f} ±{spread / 2:.1%}"
