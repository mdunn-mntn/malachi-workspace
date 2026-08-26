"""crawl/optimize pipeline: dir expansion, chimera guard, ranking, labels, dedup."""

from __future__ import annotations

import json
from pathlib import Path

from airflow_optimizer.crawl import JobReport, _event_logs, crawl, render_crawl
from airflow_optimizer.optimizations import OptFinding
from airflow_optimizer.optimize import _dedup_rank


def _write_log(path: Path, app: str) -> None:
    events = [
        {"Event": "SparkListenerApplicationStart", "App Name": app, "App ID": app,
         "Timestamp": 1000},
        {"Event": "SparkListenerJobStart"},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 2000},
    ]
    path.write_text("\n".join(json.dumps(e) for e in events))


def test_flat_dir_with_loose_parts_is_not_hijacked(tmp_path: Path) -> None:
    """Loose events_* parts beside standalone logs must NOT merge the dir into one log."""
    d = tmp_path / "download"
    d.mkdir()
    _write_log(d / "app-1.json", "job-a")
    _write_log(d / "app-2.json", "job-b")
    (d / "events_1_batch-x.json").write_text("")  # a stray flattened rolling part
    logs = _event_logs([str(d)])
    assert str(d) not in logs  # the dir itself is not one log
    assert {Path(p).name for p in logs} >= {"app-1.json", "app-2.json", "events_1_batch-x.json"}
    reports = crawl([str(d)])
    assert {r.app_name for r in reports if r.app_name} == {"job-a", "job-b"}


def test_rolling_dir_is_one_log_and_nested_dirs_recurse(tmp_path: Path) -> None:
    """eventlog_v2_* stays one log; a date-partitioned tree expands per child log."""
    tree = tmp_path / "dt=2026-08-07"
    roll = tree / "eventlog_v2_batch-y"
    roll.mkdir(parents=True)
    (roll / "appstatus_batch-y").write_text("")
    _write_log(roll / "events_1_batch-y", "rolling-job")
    _write_log(tree / "app-3.json", "nested-job")
    logs = _event_logs([str(tmp_path)])
    assert str(roll) in logs
    assert any(p.endswith("app-3.json") for p in logs)


def test_inprogress_logs_are_visible_as_skipped(tmp_path: Path) -> None:
    """An .inprogress log shows up as SKIPPED in the report, never silently dropped."""
    d = tmp_path / "logs"
    d.mkdir()
    _write_log(d / "app-4.json", "done-job")
    (d / "app-5.zstd.inprogress").write_bytes(b"")
    reports = crawl([str(d)])
    skipped = [r for r in reports if r.error]
    assert len(skipped) == 1 and "in-progress" in skipped[0].error
    assert "SKIPPED" in render_crawl(reports)


def test_ranking_uses_medium_tiebreak() -> None:
    """Two jobs with equal highs rank by medium count, not raw total."""
    f_high = OptFinding("k", "t", "high", "e", "f")
    f_med = OptFinding("k2", "t2", "medium", "e", "f")
    f_low = OptFinding("k3", "t3", "low", "e", "f")
    noisy_low = JobReport(source="a", findings=[f_high] + [f_low] * 8)
    solid_med = JobReport(source="b", findings=[f_high, f_med, f_med])
    assert solid_med.score > noisy_low.score


def test_render_label_keeps_source_filename() -> None:
    """Recurring jobs share an app_name; the row must still identify the run."""
    r = JobReport(source="app-123.zstd", app_name="Populate hourly.Job",
                  findings=[OptFinding("k", "t", "high", "e", "f")])
    out = render_crawl([r])
    assert "app-123.zstd" in out and "Populate hourly.Job" in out


def test_dedup_is_stage_aware_and_keeps_higher_impact() -> None:
    """Same key + same stage collapses to the higher impact; different stages both live."""
    a = OptFinding("shuffle_partition_sizing", "Stage 5 wide shuffle (182 GiB)", "high", "e", "f")
    b = OptFinding("shuffle_partition_sizing", "Wide shuffle on Stage 5 (~182 GiB)", "medium",
                   "e", "f")
    c = OptFinding("shuffle_partition_sizing", "Stage 9 wide shuffle (60 GiB)", "medium", "e", "f")
    out = _dedup_rank([b, a, c])
    titles = [f.title for f in out]
    assert "Stage 5 wide shuffle (182 GiB)" in titles  # high survives the collision
    assert "Wide shuffle on Stage 5 (~182 GiB)" not in titles
    assert "Stage 9 wide shuffle (60 GiB)" in titles


def test_a_no_op_run_is_reported_and_a_truncated_one_is_skipped(tmp_path: Path) -> None:
    """An app that allocated executors and ran nothing is a finding, not an unreadable log.

    Both parse to zero jobs and zero stages; only the truncated log lacks ApplicationEnd.
    Skipping both cost the 30-day corpus 15 high-impact findings over 546 executor-hours.
    """
    d = tmp_path / "logs"
    d.mkdir()
    start = [{"Event": "SparkListenerApplicationStart", "App Name": "noop", "App ID": "noop",
              "Timestamp": 1000}]
    (d / "noop.json").write_text("\n".join(json.dumps(e) for e in
                                 start + [{"Event": "SparkListenerApplicationEnd",
                                           "Timestamp": 2000}]))
    (d / "torn.json").write_text(json.dumps(start[0]))
    by_source = {r.source: r for r in crawl([str(d)])}
    assert by_source["noop.json"].error is None
    assert by_source["noop.json"].app_name == "noop"
    assert "truncated" in by_source["torn.json"].error
