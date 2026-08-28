"""BigQuery profiling: attribution, findings, and surface isolation in the ledger."""

from __future__ import annotations

from pathlib import Path

from airflow_optimizer import bq_profile, ledger
from airflow_optimizer.crawl import JobReport
from airflow_optimizer.optimizations import OptFinding

ROWS = [
    {
        "dag": "category_taxonomy",
        "task": "load",
        "jobs": "12",
        "slot_h": "109.4",
        "tib_billed": "1.2",
    },
    {
        "dag": "category_taxonomy",
        "task": "verify",
        "jobs": "12",
        "slot_h": "0.3",
        "tib_billed": "0.0",
    },
    {"dag": "", "task": "", "jobs": "780", "slot_h": "32.1", "tib_billed": "0.4"},
]


def _costs() -> list[bq_profile.TaskCost]:
    return [
        bq_profile.TaskCost(
            project="p",
            dag=r["dag"],
            task=r["task"],
            jobs=int(r["jobs"]),
            slot_h=float(r["slot_h"]),
            tib_billed=float(r["tib_billed"]),
        )
        for r in ROWS
    ]


def test_profile_parses_the_rest_response(monkeypatch) -> None:
    """Rows come back as dicts keyed by the schema; profile turns them into TaskCosts."""
    monkeypatch.setattr(bq_profile, "query", lambda project, sql: ROWS)
    costs = bq_profile.profile("2026-08-28", projects="p1")
    assert [c.dag for c in costs] == ["category_taxonomy", "category_taxonomy", ""]
    assert costs[0].slot_h == 109.4


def test_heavy_task_is_a_finding_and_unattributed_is_not() -> None:
    """A 109 slot-hour task fires the detector; the unlabeled bucket never does."""
    reports = bq_profile.reports(_costs())
    by_name = {r.app_name: r for r in reports}
    assert [f.key for f in by_name["category_taxonomy"].findings] == ["bq_heavy_task:load"]
    assert by_name["category_taxonomy"].n_high == 1
    assert by_name["unattributed"].findings == []
    assert by_name["unattributed"].exec_h == 32.1


def test_exec_h_sums_the_dag_not_the_task() -> None:
    """The dag's day total covers every task, so before/after series compare like with like."""
    reports = bq_profile.reports(_costs())
    dag = next(r for r in reports if r.app_name == "category_taxonomy")
    assert dag.exec_h == 109.7


def test_render_ranks_by_slot_hours() -> None:
    """The report table is heaviest first."""
    text = bq_profile.render(_costs(), "2026-08-28")
    assert text.index("109.4") < text.index("32.1") < text.index("0.3")


def test_bq_sweep_never_resolves_spark_keys(tmp_path: Path) -> None:
    """A BigQuery pass saw no Spark logs, so a quiet Spark key must survive it untouched."""
    p = str(tmp_path / "l.jsonl")
    spark = JobReport(
        source="a.zstd",
        app_name="Populate fangorn.F",
        findings=[
            OptFinding("shuffle_fetch_wait", "Stage 9 slow", "high", "why", "fix", rec_type="code")
        ],
    )
    ledger.record([spark], "2026-08-25", path=p)
    for day in ("2026-08-26", "2026-08-27", "2026-08-28"):
        ledger.record(bq_profile.reports(_costs()), day, path=p, surface="bq")
    states = {
        e["state"]
        for e in ledger.read(p)
        if e.get("dag_id") == "fangorn" and e.get("key") == "shuffle_fetch_wait"
    }
    assert "resolved" not in states


def test_spark_sweep_never_resolves_bq_keys(tmp_path: Path) -> None:
    """The mirror direction: Spark sweeps see no BigQuery history."""
    p = str(tmp_path / "l.jsonl")
    ledger.record(bq_profile.reports(_costs()), "2026-08-25", path=p, surface="bq")
    spark = JobReport(
        source="a.zstd",
        app_name="Populate fangorn.F",
        findings=[
            OptFinding("shuffle_fetch_wait", "Stage 9 slow", "high", "why", "fix", rec_type="code")
        ],
    )
    for day in ("2026-08-26", "2026-08-27", "2026-08-28"):
        ledger.record([spark], day, path=p)
    states = {e["state"] for e in ledger.read(p) if e.get("dag_id") == "category_taxonomy"}
    assert "resolved" not in states


def test_savings_keep_surfaces_in_separate_units(tmp_path: Path) -> None:
    """BigQuery surfaces don't sum with Spark; each surface tracks separately."""
    p = str(tmp_path / "l.jsonl")
    ledger.record(bq_profile.reports(_costs()), "2026-08-20", path=p, surface="bq")
    ledger.mark_applied(
        "category_taxonomy",
        "bq_heavy_task:load",
        "https://github.com/x/pull/1",
        "2026-08-21",
        path=p,
    )
    light = [
        bq_profile.TaskCost(
            project="p", dag="category_taxonomy", task="load", jobs=12, slot_h=9.0, tib_billed=0.1
        ),
        bq_profile.TaskCost(
            project="p", dag="other", task="t", jobs=1, slot_h=200.0, tib_billed=2.0
        ),
    ]
    for day in ("2026-08-22", "2026-08-23", "2026-08-24", "2026-08-25"):
        ledger.record(bq_profile.reports(light), day, path=p, surface="bq")
    s = ledger.savings(p, today="2026-08-25")
    assert s["by_surface"]["bq"]["total"] > 0
    assert s["total_exec_h_saved"] == s["by_surface"].get("spark", {}).get("total", 0.0)
