import json
import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hailtools import sweep_report

RUN_RECORD_TYPES = {
    "run_id": pa.string(),
    "threads": pa.uint64(),
    "input_tables": pa.uint64(),
    "rows_written": pa.uint64(),
    "run_ns": pa.uint64(),
    "steady_state_throughput": pa.float64(),
    "peak_rss_bytes": pa.uint64(),
}


def write_run_record(metrics, run_id, **columns):
    columns = {"run_id": run_id, **columns}
    runs = metrics / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({name: pa.array([value], type=RUN_RECORD_TYPES[name]) for name, value in columns.items()}),
        runs / f"{run_id}.parquet",
    )


def write_runner(metrics, runner, machine_type, threads_per_core, cells, status="done", repetition=1):
    """`cells` maps each cell, in cell order, to its exit status, or to None when the runner
    recorded no outcome for it."""
    runners = metrics / "runners"
    runners.mkdir(parents=True, exist_ok=True)
    (runners / f"{runner}.json").write_text(
        json.dumps({
            "runner": runner,
            "machine_type": machine_type,
            "threads_per_core": str(threads_per_core),
            "repetition": str(repetition),
            "cell_order": list(cells),
            "cells": [
                {"cell": cell, "run_id": f"{cell}-{runner}", "exit_status": exit_status, "seconds": 1}
                for cell, exit_status in cells.items()
                if exit_status is not None
            ],
            "status": status,
        })
    )


def write_cell_run(metrics, cell, runner, run_s, steady_state, b=50, rows=1_000_000, peak=2**30):
    write_run_record(
        metrics,
        f"{cell}-{runner}",
        threads=1,
        input_tables=b,
        rows_written=rows,
        run_ns=int(run_s * 1e9),
        steady_state_throughput=steady_state,
        peak_rss_bytes=peak,
    )


def test_normalized_throughput_scales_by_ln_b_over_threads():
    assert sweep_report.normalized_throughput(100.0, 50, 2) == pytest.approx(100 * math.log(50) / 2)


def test_report_groups_repetitions_by_cell_and_runner_shape(tmp_path):
    for repetition, (run_s, steady) in enumerate([(1.0, 1.2e6), (2.0, 0.6e6)], start=1):
        runner = f"c4-standard-2-tpc2-r{repetition}"
        write_runner(tmp_path, runner, "c4-standard-2", 2, {"b50": 0}, repetition=repetition)
        write_cell_run(tmp_path, "b50", runner, run_s, steady, peak=repetition * 2**30)
    write_runner(tmp_path, "n4-standard-2-tpc1-r1", "n4-standard-2", 1, {"b50": 0})
    write_cell_run(tmp_path, "b50", "n4-standard-2-tpc1-r1", 1.0, 1.0e6)

    report = sweep_report.report(tmp_path)

    assert [(cell.cell, cell.machine_type, cell.threads_per_core, len(cell.runs)) for cell in report.figures] == [
        ("b50", "c4-standard-2", 2, 2),
        ("b50", "n4-standard-2", 1, 1),
    ]
    c4 = report.figures[0]
    ln_b = math.log(50)
    assert c4.whole_run == pytest.approx([1e6 * ln_b, 0.5e6 * ln_b])
    assert c4.steady_state == pytest.approx([1.2e6 * ln_b, 0.6e6 * ln_b])
    assert c4.steady_over_whole == pytest.approx(1.2)
    assert c4.peak_rss_bytes == 2 * 2**30
    assert report.failed == []
    assert report.unfinished == []


def test_whole_run_figure_uses_the_records_branching_factor(tmp_path):
    write_runner(tmp_path, "r", "c4-standard-2", 1, {"b100": 0})
    write_cell_run(tmp_path, "b100", "r", 2.0, None, b=100)

    (cell,) = sweep_report.report(tmp_path).figures

    assert cell.whole_run == pytest.approx([0.5e6 * math.log(100)])
    assert cell.steady_state == []
    assert cell.steady_over_whole is None


def test_report_lists_failed_cells_and_unfinished_runners(tmp_path):
    write_runner(tmp_path, "r1", "c4-standard-2", 2, {"oom": 137, "lost": 0, "ok": 0}, status="interrupted")
    write_cell_run(tmp_path, "ok", "r1", 1.0, 1e6)

    report = sweep_report.report(tmp_path)

    assert [cell.cell for cell in report.figures] == ["ok"]
    assert report.failed == [
        sweep_report.FailedCell("r1", "oom", "oom-r1", 137),
        sweep_report.FailedCell("r1", "lost", "lost-r1", 0),
    ]
    assert report.unfinished == [("r1", "interrupted")]
    markdown = sweep_report.render_markdown(report)
    assert "- oom-r1: exit status 137" in markdown
    assert "- lost-r1: exit status 0, no run record" in markdown
    assert "- r1: interrupted" in markdown


def test_markdown_gives_mean_and_half_range_in_millions(tmp_path):
    for repetition, run_s in enumerate([1.0, 0.5], start=1):
        write_runner(tmp_path, f"r{repetition}", "c4-standard-2", 2, {"b": 0}, repetition=repetition)
        write_cell_run(tmp_path, "b", f"r{repetition}", run_s, None, peak=3 * 2**29)

    markdown = sweep_report.render_markdown(sweep_report.report(tmp_path))

    # Whole-run throughputs of 1M and 2M rows/s: mean 1.5M, half-range 0.5M, before the ln b.
    assert f"| b | c4-standard-2 | 2 | 2 | {1.5 * math.log(50):.2f} ±33.3% |  |  | 1.50 |" in markdown


def test_markdown_gives_each_repetition_in_order(tmp_path):
    # Runner names that sort against repetition order, so the order can only come from repetition.
    for runner, repetition, run_s, steady in [("a", 2, 0.5, None), ("b", 1, 1.0, 1.5e6)]:
        write_runner(tmp_path, runner, "c4-standard-2", 2, {"b": 0}, repetition=repetition)
        write_cell_run(tmp_path, "b", runner, run_s, steady, peak=2**30)

    report = sweep_report.report(tmp_path)
    markdown = sweep_report.render_markdown(report)

    assert [run.repetition for run in report.figures[0].runs] == [1, 2]
    ln_b = math.log(50)
    first = f"| b | c4-standard-2 | 2 | 1 | {ln_b:.2f} | {1.5 * ln_b:.2f} | 1.500 | 1.00 |"
    second = f"| b | c4-standard-2 | 2 | 2 | {2 * ln_b:.2f} |  |  | 1.00 |"
    assert first in markdown
    assert second in markdown
    assert markdown.index(first) < markdown.index(second)


def test_steady_over_whole_leaves_out_repetitions_without_an_estimate(tmp_path):
    for repetition, run_s, steady in [(1, 1.0, 1.5e6), (2, 0.5, None)]:
        write_runner(tmp_path, f"r{repetition}", "c4-standard-2", 2, {"b": 0}, repetition=repetition)
        write_cell_run(tmp_path, "b", f"r{repetition}", run_s, steady)

    (cell,) = sweep_report.report(tmp_path).figures

    # Only repetition 1 has a steady-state estimate, at 1.5 times its whole-run throughput.
    assert cell.steady_over_whole == pytest.approx(1.5)


def test_report_counts_the_recorded_runs_of_a_runner_that_recorded_no_outcomes(tmp_path):
    # A runner deleted by its maximum run duration leaves the record it wrote at its start.
    write_runner(tmp_path, "r1", "c4-standard-2", 2, {"ran": None, "never-ran": None}, status="running")
    write_cell_run(tmp_path, "ran", "r1", 1.0, 1e6)

    report = sweep_report.report(tmp_path)

    assert [(cell.cell, [run.run_id for run in cell.runs]) for cell in report.figures] == [("ran", ["ran-r1"])]
    assert report.failed == []
    assert report.unfinished == [("r1", "running")]


def test_report_needs_runner_records(tmp_path):
    with pytest.raises(sweep_report.SweepReportError, match="no runner records"):
        sweep_report.report(tmp_path)
