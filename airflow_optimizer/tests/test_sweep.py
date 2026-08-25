"""The DAG's actual call path: sweep.run, its ledger gating, and the only GCS write."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from airflow_optimizer import coverage as cov_mod
from airflow_optimizer import fetch, ledger, sweep
from airflow_optimizer.crawl import JobReport
from airflow_optimizer.optimizations import OptFinding

FETCH = OptFinding("shuffle_fetch_wait", "Stage 9 spends 73% of task time waiting on shuffle fetch",
                   "high", "why", "fix", rec_type="code")


def _report(app: str, name: str = "Populate site_network_hourly.SiteNetworkHourly") -> JobReport:
    return JobReport(source=f"{app}.zstd", findings=[FETCH], app_name=name)


def _run(tmp: Path, date: str, **kw) -> dict:
    return sweep.run([str(tmp / "logs")], date, outdir=str(tmp / "out"),
                     ledger_path=str(tmp / "out" / "l.jsonl"), **kw)


@pytest.fixture
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A log dir plus a crawl that always finds the same one job."""
    (tmp_path / "logs").mkdir()
    monkeypatch.setattr(sweep, "crawl", lambda _p: [_report("a")])
    return tmp_path


def test_run_writes_the_backlog_and_digest_and_records_the_ledger(fleet: Path) -> None:
    """The happy path the DAG depends on, which had no test at all."""
    out = _run(fleet, "2026-08-18")
    assert out["scanned"] == 1 and out["findings"] == 1 and out["high"] == 1
    assert out["ledger_entries"] == 1 and out["complete"] is True
    assert os.path.exists(out["backlog"]) and os.path.exists(out["digest"])
    assert "site_network_hourly" in Path(out["digest"]).read_text()


def test_a_partial_sweep_resolves_nothing(fleet: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Absence is only evidence of a fix when we actually looked everywhere.

    Without this gate, a download failure makes the digest announce never-scanned jobs as
    "Stopped firing", which reads to an owner as a defect that got fixed.
    """
    both = [_report("a"), _report("b", "Populate aug_log_ip_hourly.AugLogIp")]
    only_b = [_report("b", "Populate aug_log_ip_hourly.AugLogIp")]

    monkeypatch.setattr(sweep, "crawl", lambda _p: both)
    for d in ("2026-08-01", "2026-08-02", "2026-08-03"):
        _run(fleet, d)

    # site_network_hourly stops appearing, but only because its logs did not download.
    monkeypatch.setattr(sweep, "crawl", lambda _p: only_b)
    for d in ("2026-08-04", "2026-08-05", "2026-08-06"):
        out = _run(fleet, d, complete=False)
        assert "Stopped firing" not in out["slack"]
        assert "Partial sweep" in out["slack"]
    assert not [e for e in ledger.read(str(fleet / "out" / "l.jsonl"))
                if e.get("state") == "resolved"]

    # A COMPLETE sweep with the job still absent is allowed to conclude it stopped.
    assert "Stopped firing" in _run(fleet, "2026-08-07")["slack"]


def test_coverage_failure_does_not_rekey_the_ledger(fleet: Path,
                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    """`known` disambiguates a trailing suffix; writing without it invents a second identity.

    That reads as a brand-new finding today and, three sweeps on, as a resolved one.
    """
    real = cov_mod.Coverage(date="2026-08-10",
                            dags=[cov_mod.DagCoverage(dag_id="site_network_hourly",
                                                      spark_tasks=["run"])])
    monkeypatch.setattr(sweep.cov_mod, "collect_local", lambda _d: real)
    _run(fleet, "2026-08-10", airflow_base="local")

    def _blind(_d: str) -> object:
        raise RuntimeError("metadata DB unreachable")

    monkeypatch.setattr(sweep.cov_mod, "collect_local", _blind)
    out = _run(fleet, "2026-08-11", airflow_base="local")
    assert out["ledger_entries"] == 0
    assert "coverage unavailable" in out["ledger_note"]
    assert "No change tracking" in out["slack"]
    rows = ledger.read(str(fleet / "out" / "l.jsonl"))
    assert {r["date"] for r in rows} == {"2026-08-10"}          # nothing written blind


def test_publish_never_raises_and_reports_what_landed(fleet: Path,
                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    """The only GCS write in the product. An upload fault must not lose the local artifacts."""
    calls = []

    class _R:
        def __init__(self, rc: int) -> None:
            self.returncode, self.stderr = rc, b"denied"

    def _cp(cmd: list, **_kw) -> _R:
        calls.append(cmd[-1])
        return _R(0 if "digest" in cmd[-1] else 1)

    monkeypatch.setattr(sweep.subprocess, "run", _cp)
    a, b = fleet / "a.md", fleet / "digest.md"
    a.write_text("x"), b.write_text("y")
    landed = sweep.publish([str(a), str(b), "", "/nope.md"], "gs://bucket/optimizer")
    assert landed == ["gs://bucket/optimizer/digest.md"]
    assert all(c.startswith("gs://bucket/optimizer/") for c in calls)   # never outside the prefix
    assert "/nope.md" not in " ".join(calls)                            # absent files skipped


def test_publish_is_a_noop_without_a_prefix(fleet: Path) -> None:
    """A local run must not attempt a GCS write."""
    assert sweep.publish([str(fleet / "logs")], "") == []


def test_download_reports_failures_instead_of_hiding_them(tmp_path: Path,
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    """A partial download is not a small full one; the caller has to be able to tell."""
    class _R:
        def __init__(self, rc: int) -> None:
            self.returncode, self.stderr = rc, b"503"

    monkeypatch.setattr(fetch.subprocess, "run",
                        lambda cmd, **_k: _R(0 if "ok" in cmd[-2] else 1))
    landed, failed = fetch.download(["gs://b/ok.zstd", "gs://b/bad.zstd"], str(tmp_path))
    assert (landed, failed) == (1, 1)


def test_newest_logs_raises_rather_than_returning_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty listing and a failed listing are different facts; conflating them ships a lie."""
    class _R:
        returncode, stdout, stderr = 1, "", "AccessDeniedException: 403"

    monkeypatch.setattr(fetch.subprocess, "run", lambda *_a, **_k: _R())
    with pytest.raises(RuntimeError, match="403"):
        fetch.newest_logs("gs://b/p", 10)


def test_fetch_optional_distinguishes_absent_from_unreadable(tmp_path: Path,
                                                             monkeypatch: pytest.MonkeyPatch) -> None:
    """Treating an unreadable ledger as an absent one destroys the history it republishes."""
    class _R:
        def __init__(self, rc: int) -> None:
            self.returncode, self.stderr = rc, b"503 backend error"

    monkeypatch.setattr(fetch.subprocess, "run", lambda cmd, **_k: _R(1))
    assert fetch.fetch_optional("gs://b/l.jsonl", str(tmp_path)) is False   # stat says absent

    def _stat_ok_cp_fails(cmd: list, **_k) -> _R:
        return _R(0 if "stat" in cmd else 1)

    monkeypatch.setattr(fetch.subprocess, "run", _stat_ok_cp_fails)
    with pytest.raises(RuntimeError, match="exists but could not be fetched"):
        fetch.fetch_optional("gs://b/l.jsonl", str(tmp_path))


def test_cap_never_cuts_a_rolling_log_in_half(monkeypatch: pytest.MonkeyPatch) -> None:
    """A half-fetched v2 log parses as a complete run and scores clean, which is a wrong answer."""
    listing = "".join(
        f"  10  2026-08-2{i}T01:00:00Z  gs://b/p/eventlog_v2_batch-x/events_{i}_batch-x.zstd\n"
        for i in range(1, 4)) + "  10  2026-08-29T01:00:00Z  gs://b/p/app-solo.zstd\n"

    class _R:
        returncode, stdout, stderr = 0, listing, ""

    monkeypatch.setattr(fetch.subprocess, "run", lambda *_a, **_k: _R())
    # cap=2 would slice off events_1; the whole batch dir has to come back with it.
    got = fetch.newest_logs("gs://b/p", 2)
    assert sum(1 for o in got if "eventlog_v2_batch-x" in o) == 3
    assert "gs://b/p/app-solo.zstd" in got


def test_digest_cites_the_published_backlog_not_the_container_path(
        fleet: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A reader cannot open /tmp/spark_events_<rand>/ on a pod that no longer exists."""
    monkeypatch.setattr(sweep, "publish", lambda *_a, **_k: [])
    out = _run(fleet, "2026-08-19", gcs_prefix="gs://bucket/optimizer/")
    assert "gs://bucket/optimizer/optimizer_backlog_2026-08-19.md" in out["slack"]
    assert "/tmp/" not in out["slack"]
