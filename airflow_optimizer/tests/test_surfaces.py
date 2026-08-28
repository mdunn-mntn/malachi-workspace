"""Per-surface rates, Databricks findings, and the multi-surface savings headline."""

from __future__ import annotations

from pathlib import Path

from airflow_optimizer import billing, databricks, ledger
from airflow_optimizer.bq_profile import Finding, Report
from airflow_optimizer.databricks import Cost, Submission


def test_slot_rate_falls_back_to_the_env(monkeypatch) -> None:
    """An unreadable export degrades to the configured rate, marked as such, never to zero."""
    monkeypatch.setattr(billing, "_blended_rate", lambda *a, **k: (None, "no access"))
    monkeypatch.setenv("OPTIMIZER_USD_PER_SLOT_H", "0.04")
    rate, note = billing.blended_usd_per_slot_h()
    assert rate == 0.04
    assert "fallback" in note


def test_slot_rate_without_export_or_env_is_none(monkeypatch) -> None:
    """No measured rate and no configured one: say so instead of inventing a number."""
    monkeypatch.setattr(billing, "_blended_rate", lambda *a, **k: (None, "no access"))
    monkeypatch.delenv("OPTIMIZER_USD_PER_SLOT_H", raising=False)
    assert billing.blended_usd_per_slot_h() == (None, "no access")


def test_surface_rates_covers_spark_and_bq(monkeypatch) -> None:
    """One call returns every surface the savings math can price."""
    monkeypatch.setattr(billing, "_blended_rate", lambda *a, **k: (0.05, "blended"))
    rates = billing.surface_rates()
    assert set(rates) == {"spark", "bq"}
    assert rates["bq"][0] == 0.05


def test_dbx_heavy_job_and_failing_model_become_findings(monkeypatch) -> None:
    """A $75/day job and a thrice-failed model each get a finding; a cheap job gets none."""
    monkeypatch.setattr(databricks, "job_costs", lambda *a, **k: [
        Cost(name="big_model", runs=7, dbu=700.0, usd=525.0),
        Cost(name="small_model", runs=7, dbu=7.0, usd=5.0),
    ])
    monkeypatch.setattr(databricks, "submissions", lambda *a, **k: [
        Submission(model="flaky_model", run_id=str(i), result_state="FAILED",
                   duration_s=60, started="") for i in range(3)
    ])
    reports = {r.app_name: r for r in databricks.findings_reports(days=7)}
    assert [f.key for f in reports["big_model"].findings] == ["dbx_heavy_job"]
    assert reports["big_model"].exec_h == 100.0
    assert reports["small_model"].findings == []
    assert [f.key for f in reports["flaky_model"].findings] == ["dbx_failing_model"]


def _resolved_bq_saving(path: str) -> None:
    heavy = [Report(source="bq:d", app_name="d", exec_h=100.0,
                    findings=[Finding(key="bq_heavy_task:t", impact="high", title="t")])]
    quiet = [Report(source="bq:d", app_name="d", exec_h=10.0),
             Report(source="bq:other", app_name="other", exec_h=200.0,
                    findings=[Finding(key="bq_heavy_task:x", impact="high", title="x")])]
    ledger.record(heavy, "2026-08-20", path=path, surface="bq")
    ledger.mark_applied("d", "bq_heavy_task:t", "https://github.com/x/pull/9", "2026-08-21",
                        path=path)
    for day in ("2026-08-22", "2026-08-23", "2026-08-24", "2026-08-25"):
        ledger.record(quiet, day, path=path, surface="bq")


def test_headline_prices_each_surface_in_its_own_unit(tmp_path: Path) -> None:
    """A bq saving reads in slot-hours and bq dollars, never in the spark line."""
    p = str(tmp_path / "l.jsonl")
    _resolved_bq_saving(p)
    s = ledger.savings(p, today="2026-08-25", usd_rates={"bq": 0.05})
    text = ledger.savings_headline(s)
    assert s["total_exec_h_saved"] == 0.0
    assert "bq:" in text and "slot-hours" in text and "$" in text


def test_headline_without_a_rate_stays_unitful_but_unpriced(tmp_path: Path) -> None:
    """No bq rate: the slot-hours still show, no dollar figure is invented."""
    p = str(tmp_path / "l.jsonl")
    _resolved_bq_saving(p)
    text = ledger.savings_headline(ledger.savings(p, today="2026-08-25"))
    assert "slot-hours" in text
    assert "bq" in text and "(~$" not in text
