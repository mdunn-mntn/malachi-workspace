"""The DAG's actual call path: sweep.run, its ledger gating, and the only GCS write."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from airflow_optimizer import coverage as cov_mod
from airflow_optimizer import fetch, ledger, notify, sweep
from airflow_optimizer.crawl import JobReport
from airflow_optimizer.optimizations import OptFinding

FETCH = OptFinding(
    "shuffle_fetch_wait",
    "Stage 9 spends 73% of task time waiting on shuffle fetch",
    "high",
    "why",
    "fix",
    rec_type="code",
)


def _report(app: str, name: str = "Populate site_network_hourly.SiteNetworkHourly") -> JobReport:
    return JobReport(source=f"{app}.zstd", findings=[FETCH], app_name=name)


class _Upload:
    """What `gsutil cp` returns, as `subprocess.run` reports it."""

    def __init__(self, rc: int) -> None:
        self.returncode, self.stderr = rc, b"denied"


def _blind(_d: str) -> object:
    raise RuntimeError("metadata DB unreachable")


def _run(tmp: Path, date: str, **kw) -> dict:
    return sweep.run(
        [str(tmp / "logs")],
        date,
        outdir=str(tmp / "out"),
        ledger_path=str(tmp / "out" / "l.jsonl"),
        **kw,
    )


@pytest.fixture
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A log dir, a crawl that always finds the same one job, and no Slack credential."""
    (tmp_path / "logs").mkdir()
    monkeypatch.delenv(notify.TOKEN_ENV, raising=False)
    monkeypatch.delenv(notify.CHANNEL_ENV, raising=False)
    monkeypatch.setattr(sweep, "crawl", lambda _p: [_report("a")])
    monkeypatch.setattr(sweep.billing_mod, "blended_usd_per_exec_h", lambda: (None, "hermetic"))
    # A laptop with real BQ credentials would otherwise pull the fleet's actual job history.
    monkeypatch.setattr(sweep.bq_mod, "profile", lambda _d, projects="": [])
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
    assert not [
        e for e in ledger.read(str(fleet / "out" / "l.jsonl")) if e.get("state") == "resolved"
    ]

    # A COMPLETE sweep with the job still absent is allowed to conclude it stopped.
    assert "Stopped firing" in _run(fleet, "2026-08-07")["slack"]


def test_coverage_failure_does_not_rekey_the_ledger(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`known` disambiguates a trailing suffix; writing without it invents a second identity.

    That reads as a brand-new finding today and, three sweeps on, as a resolved one.
    """
    real = cov_mod.Coverage(
        date="2026-08-10",
        dags=[cov_mod.DagCoverage(dag_id="site_network_hourly", spark_tasks=["run"])],
        dag_ids_including_paused={"site_network_hourly"},
    )
    monkeypatch.setattr(sweep.cov_mod, "collect_local", lambda _d: real)
    _run(fleet, "2026-08-10", airflow_base="local")

    monkeypatch.setattr(sweep.cov_mod, "collect_local", _blind)
    out = _run(fleet, "2026-08-11", airflow_base="local")
    assert out["ledger_entries"] == 0
    assert "coverage unavailable" in out["ledger_note"]
    assert "No change tracking" in out["slack"]
    rows = ledger.read(str(fleet / "out" / "l.jsonl"))
    assert {r["date"] for r in rows} == {"2026-08-10"}  # nothing written blind


def test_publish_never_raises_and_reports_what_landed(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The only GCS write in the product. An upload fault must not lose the local artifacts."""
    calls = []

    def _cp(cmd: list, **_kw) -> _Upload:
        calls.append(cmd[-1])
        return _Upload(0 if "digest" in cmd[-1] else 1)

    monkeypatch.setattr(sweep.subprocess, "run", _cp)
    a, b = fleet / "a.md", fleet / "digest.md"
    a.write_text("x"), b.write_text("y")
    landed = sweep.publish([str(a), str(b), "", "/nope.md"], "gs://bucket/optimizer")
    assert landed == ["gs://bucket/optimizer/digest.md"]
    assert all(c.startswith("gs://bucket/optimizer/") for c in calls)  # never outside the prefix
    assert "/nope.md" not in " ".join(calls)  # absent files skipped


def test_publish_is_a_noop_without_a_prefix(fleet: Path) -> None:
    """A local run must not attempt a GCS write."""
    assert sweep.publish([str(fleet / "logs")], "") == []


def test_download_reports_failures_instead_of_hiding_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A partial download is not a small full one; the caller has to be able to tell."""

    class _R:
        returncode, stderr = 1, b"503"

    def bulk_cp(cmd: list, **kw: object) -> _R:
        target = cmd[-1].rstrip("/")
        for obj in kw["input"].decode().splitlines():
            if "ok" in obj:
                (Path(target) / Path(obj).name).write_bytes(b"x")
        return _R()

    monkeypatch.setattr(fetch.subprocess, "run", bulk_cp)
    landed, failed = fetch.download(["gs://b/ok.zstd", "gs://b/bad.zstd"], str(tmp_path))
    assert (landed, failed) == (1, 1)


def test_download_is_one_invocation_per_destination_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """200 serial spawns dominated the prod sweep; the copy must batch, keeping dir structure."""
    calls: list[tuple[str, list[str]]] = []

    class _R:
        returncode, stderr = 0, b""

    def bulk_cp(cmd: list, **kw: object) -> _R:
        objs = kw["input"].decode().splitlines()
        calls.append((cmd[-1], objs))
        for obj in objs:
            (Path(cmd[-1].rstrip("/")) / Path(obj).name).write_bytes(b"x")
        return _R()

    monkeypatch.setattr(fetch.subprocess, "run", bulk_cp)
    landed, failed = fetch.download(
        ["gs://b/a.zstd", "gs://b/c.zstd", "gs://b/eventlog_v2_x/events_1_x.zstd"], str(tmp_path)
    )
    assert (landed, failed) == (3, 0)
    assert len(calls) == 2, "one bulk cp per destination dir, not one per object"
    assert (tmp_path / "eventlog_v2_x" / "events_1_x.zstd").exists()


def test_newest_logs_raises_rather_than_returning_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty listing and a failed listing are different facts; conflating them ships a lie."""

    class _R:
        returncode, stdout, stderr = 1, "", "AccessDeniedException: 403"

    monkeypatch.setattr(fetch.subprocess, "run", lambda *_a, **_k: _R())
    with pytest.raises(RuntimeError, match="403"):
        fetch.newest_logs("gs://b/p", 10)


def test_fetch_optional_distinguishes_absent_from_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Treating an unreadable ledger as an absent one destroys the history it republishes."""

    class _R:
        def __init__(self, rc: int) -> None:
            self.returncode, self.stderr = rc, b"503 backend error"

    monkeypatch.setattr(fetch.subprocess, "run", lambda cmd, **_k: _R(1))
    assert fetch.fetch_optional("gs://b/l.jsonl", str(tmp_path)) is False  # stat says absent

    def _stat_ok_cp_fails(cmd: list, **_k) -> _R:
        return _R(0 if "stat" in cmd else 1)

    monkeypatch.setattr(fetch.subprocess, "run", _stat_ok_cp_fails)
    with pytest.raises(RuntimeError, match="exists but could not be fetched"):
        fetch.fetch_optional("gs://b/l.jsonl", str(tmp_path))


def test_cap_never_cuts_a_rolling_log_in_half(monkeypatch: pytest.MonkeyPatch) -> None:
    """A half-fetched v2 log parses as a complete run and scores clean, which is a wrong answer."""
    listing = (
        "".join(
            f"  10  2026-08-2{i}T01:00:00Z  gs://b/p/eventlog_v2_batch-x/events_{i}_batch-x.zstd\n"
            for i in range(1, 4)
        )
        + "  10  2026-08-29T01:00:00Z  gs://b/p/app-solo.zstd\n"
    )

    class _R:
        returncode, stdout, stderr = 0, listing, ""

    monkeypatch.setattr(fetch.subprocess, "run", lambda *_a, **_k: _R())
    # cap=2 would slice off events_1; the whole batch dir has to come back with it.
    got = fetch.newest_logs("gs://b/p", 2)
    assert sum(1 for o in got if "eventlog_v2_batch-x" in o) == 3
    assert "gs://b/p/app-solo.zstd" in got


def test_digest_cites_the_published_artifacts_not_the_container_path(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A reader cannot open /tmp/spark_events_<rand>/ on a pod that no longer exists."""
    blind = cov_mod.Coverage(
        date="2026-08-19",
        dag_ids_including_paused={"site_network_hourly", "notify_only"},
        dags=[
            cov_mod.DagCoverage(dag_id="site_network_hourly", spark_tasks=["run"]),
            cov_mod.DagCoverage(dag_id="notify_only", other_tasks=[("ping", "PythonOperator")]),
        ],
        report_path="optimizer_out/optimizer_coverage_2026-08-19.md",
    )
    monkeypatch.setattr(sweep.cov_mod, "collect_local", lambda _d: blind)
    monkeypatch.setattr(sweep.subprocess, "run", lambda *_a, **_k: _Upload(0))
    out = _run(fleet, "2026-08-19", airflow_base="local", gcs_prefix="gs://bucket/optimizer/")

    assert "gs://bucket/optimizer/optimizer_backlog_2026-08-19.md" in out["slack"]
    assert "gs://bucket/optimizer/optimizer_coverage_2026-08-19.md" in out["slack"]
    assert "Not scanned" in out["slack"]
    assert str(fleet) not in out["slack"]
    assert os.path.exists(out["coverage"])


def test_a_local_run_cites_the_paths_it_actually_wrote(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a GCS prefix nothing is published, so the on-disk paths are the only real ones."""
    blind = cov_mod.Coverage(
        date="2026-08-20",
        dag_ids_including_paused={"notify_only"},
        dags=[cov_mod.DagCoverage(dag_id="notify_only", other_tasks=[("ping", "PythonOperator")])],
    )
    monkeypatch.setattr(sweep.cov_mod, "collect_local", lambda _d: blind)
    out = _run(fleet, "2026-08-20", airflow_base="local")
    assert out["coverage"] in out["slack"] and out["backlog"] in out["slack"]


def test_a_ledger_key_does_not_move_when_paused_state_is_unavailable(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`known` is the name index. If it shrinks when the DB answers, one finding becomes two."""
    bundle = {"materialize_mntn_select", "other_dag"}
    monkeypatch.setattr(sweep, "crawl", lambda _p: [_report("a", "materialize_mntn_select_16")])

    def _cov(paused: set) -> object:
        return cov_mod.Coverage(
            date="x",
            dag_ids_including_paused=bundle,
            dags=[
                cov_mod.DagCoverage(dag_id=d, spark_tasks=["run"]) for d in sorted(bundle - paused)
            ],
        )

    monkeypatch.setattr(
        sweep.cov_mod, "collect_local", lambda _d: _cov({"materialize_mntn_select"})
    )
    _run(fleet, "2026-08-21", airflow_base="local")
    monkeypatch.setattr(sweep.cov_mod, "collect_local", lambda _d: _cov(set()))
    out = _run(fleet, "2026-08-22", airflow_base="local")

    rows = ledger.read(str(fleet / "out" / "l.jsonl"))
    assert {r["dag_id"] for r in rows} == {"materialize_mntn_select"}
    assert "New today" not in out["slack"]
    assert [r["streak"] for r in rows if r["date"] == "2026-08-22"] == [2]


def test_the_digest_never_cites_an_upload_that_failed(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A gs:// URL in the digest is a promise, so an upload that failed must not be cited."""
    blind = cov_mod.Coverage(
        date="2026-08-23",
        dag_ids_including_paused={"site_network_hourly", "notify_only"},
        dags=[
            cov_mod.DagCoverage(dag_id="site_network_hourly", spark_tasks=["run"]),
            cov_mod.DagCoverage(dag_id="notify_only", other_tasks=[("ping", "PythonOperator")]),
        ],
    )
    monkeypatch.setattr(sweep.cov_mod, "collect_local", lambda _d: blind)
    monkeypatch.setattr(
        sweep.subprocess, "run", lambda cmd, **_k: _Upload(1 if "backlog" in cmd[-1] else 0)
    )
    out = _run(fleet, "2026-08-23", airflow_base="local", gcs_prefix="gs://bucket/optimizer/")

    assert "gs://bucket/optimizer/optimizer_backlog_2026-08-23.md" not in out["slack"]
    assert out["backlog"] in out["slack"]
    assert "upload failed" in out["slack"]  # that local path dies with the pod
    assert "gs://bucket/optimizer/optimizer_coverage_2026-08-23.md" in out["slack"]


def _short_cov(unparsed: list) -> object:
    """Coverage as an unimportable `targeting_dag.py` leaves it: the DAG-id set is short."""
    known = {"other_dag"} if unparsed else {"materialize_mntn_select", "other_dag"}
    return cov_mod.Coverage(
        date="x",
        dag_ids_including_paused=known,
        unparsed_files=unparsed,
        dags=[cov_mod.DagCoverage(dag_id=d, spark_tasks=["run"]) for d in sorted(known)],
    )


def test_a_run_indexed_job_keeps_one_ledger_key_while_the_id_set_is_short(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A trailing `_<n>` keys against the DAG-id set, so a short set must not fork the row."""
    monkeypatch.setattr(sweep, "crawl", lambda _p: [_report("a", "materialize_mntn_select_16")])
    monkeypatch.setattr(sweep.cov_mod, "collect_local", lambda _d: _short_cov([]))
    for d in ("2026-09-01", "2026-09-02", "2026-09-03"):
        _run(fleet, d, airflow_base="local")

    monkeypatch.setattr(sweep.cov_mod, "collect_local", lambda _d: _short_cov(["targeting_dag.py"]))
    for d in ("2026-09-04", "2026-09-05", "2026-09-06"):
        out = _run(fleet, d, airflow_base="local")
        assert out["ledger_entries"] == 1
        assert "Stopped firing" not in out["slack"]

    rows = ledger.read(str(fleet / "out" / "l.jsonl"))
    assert {r["dag_id"] for r in rows} == {"materialize_mntn_select"}
    assert [r["streak"] for r in rows if r["date"] == "2026-09-06"] == [6]


def test_an_unrelated_import_error_does_not_stop_a_fix_from_resolving(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolution is gated on what was actually held out, not on the bundle being whole."""
    live = [_report("a"), _report("b", "Populate aug_log_ip_hourly.AugLogIp")]
    monkeypatch.setattr(
        sweep.cov_mod, "collect_local", lambda _d: _short_cov(["unrelated_broken_dag.py"])
    )
    monkeypatch.setattr(sweep, "crawl", lambda _p: live)
    for d in ("2026-10-01", "2026-10-02", "2026-10-03"):
        _run(fleet, d, airflow_base="local")

    monkeypatch.setattr(sweep, "crawl", lambda _p: [live[1]])
    for d in ("2026-10-04", "2026-10-05", "2026-10-06"):
        out = _run(fleet, d, airflow_base="local")
    assert "Stopped firing" in out["slack"]
    assert [
        r["state"]
        for r in ledger.read(str(fleet / "out" / "l.jsonl"))
        if r["dag_id"] == "site_network_hourly" and r["date"] == "2026-10-06"
    ] == ["resolved"]


def test_a_first_sweep_without_coverage_writes_no_ledger_rows(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty ledger is no licence to key blind: those rows are what re-key on the next sweep."""
    monkeypatch.setattr(sweep.cov_mod, "collect_local", _blind)
    out = _run(fleet, "2026-10-10", airflow_base="local")
    assert out["ledger_entries"] == 0
    assert ledger.read(str(fleet / "out" / "l.jsonl")) == []


def test_a_sweep_without_a_slack_credential_renders_but_does_not_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate is the credential, so a local run cannot post by accident."""
    monkeypatch.delenv(notify.TOKEN_ENV, raising=False)
    monkeypatch.delenv(notify.CHANNEL_ENV, raising=False)
    assert not notify.enabled()
    assert notify.deliver_thread([{"type": "section"}], [])["reason"].startswith("no ")


def test_delivery_reports_the_slack_error_rather_than_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bot that was never invited returns channel_not_found; the sweep must survive it."""
    monkeypatch.setenv(notify.TOKEN_ENV, "x")
    monkeypatch.setenv(notify.CHANNEL_ENV, "C123")
    monkeypatch.setattr(notify, "_post", lambda m, p: {"ok": False, "error": "channel_not_found"})
    assert notify.deliver_thread([{"type": "section"}], []) == {
        "sent": False,
        "error": "channel_not_found",
        "replies": 0,
    }


def test_coverage_judges_every_name_the_digest_can_print() -> None:
    """A name keyed one way in the ledger and another in coverage reads as self-disagreement."""
    from types import SimpleNamespace

    entries = [SimpleNamespace(dag_id="ipdsc_ds_35")]
    delta = SimpleNamespace(
        new=[],
        chronic=[],
        notified=[],
        resolved=[SimpleNamespace(dag_id="fangorn_score_monitor")],
        fix_not_working=[],
    )
    scored = [JobReport(source="app-1.zstd", app_name="Populate other.Thing", findings=[1])]
    names = sweep._rendered_dags(entries, delta, scored, None)
    assert {"ipdsc_ds_35", "fangorn_score_monitor", "other"} <= names
    assert "" not in names


def _entry(dag: str, impact: str, title: str, hours: float, owner: str = "team") -> Any:
    from airflow_optimizer.ledger import Entry

    return Entry(
        date="2026-08-26",
        dag_id=dag,
        app_id="app-1.zstd",
        key="k",
        impact=impact,
        title=title,
        fix="do the thing",
        owner=owner,
        exec_h=hours,
        streak=3,
    )


def test_the_parent_carries_the_whole_ranked_list_not_just_the_worst() -> None:
    """A reader who stops at the summary still needs to know which jobs cost what."""
    from types import SimpleNamespace

    from airflow_optimizer import digest

    delta = SimpleNamespace(
        new=[],
        chronic=[_entry("a", "high", "spilled", 433), _entry("b", "medium", "waited", 276)],
        notified=[],
        resolved=[_entry("c", "low", "", 0)],
        fix_not_working=[],
    )
    parent, replies = digest.blocks(
        delta, scanned=10, findings=2, high=1, date="2026-08-26", base="https://x/dags/{dag_id}"
    )
    text = json.dumps(parent)
    assert "spilled" in text and "waited" in text
    assert "433" in text and "276" in text
    assert len(replies) == 2
    assert "do the thing" in json.dumps(replies)


def test_an_unresolved_job_renders_as_a_name_never_a_broken_link() -> None:
    """`ipdsc_ds_35` is a task id; linking it as a dag_id lands on an empty Airflow page."""
    from types import SimpleNamespace

    from airflow_optimizer import digest

    delta = SimpleNamespace(
        new=[_entry("ipdsc_ds_35", "high", "straggler", 348)],
        chronic=[],
        notified=[],
        resolved=[],
        fix_not_working=[],
    )
    cov = SimpleNamespace(unprofiled=[], resolve=lambda n: "", report_path="")
    parent, _ = digest.blocks(
        delta,
        scanned=1,
        findings=1,
        high=1,
        date="2026-08-26",
        coverage=cov,
        base="https://x/dags/{dag_id}",
    )
    text = json.dumps(parent)
    assert "https://x/dags/ipdsc_ds_35" not in text
    assert "`ipdsc_ds_35`" in text


def _states(dag: str, state: str) -> Any:
    e = _entry(dag, "high", "shuffle spill, stage 7", 433)
    e.state = state
    return e


def test_every_state_the_header_reacts_to_is_named_in_the_post() -> None:
    """A red post whose body says nothing is firing tells the team a DAG name it never prints."""
    from types import SimpleNamespace

    from airflow_optimizer import digest

    stuck = SimpleNamespace(
        new=[],
        chronic=[],
        notified=[],
        resolved=[],
        fix_not_working=[_states("site_network_hourly", "fix_not_working")],
    )
    parent, replies = digest.blocks(stuck, scanned=160, findings=1, high=1, date="2026-08-26")
    text = json.dumps(parent)
    assert "needs attention" in text and "site_network_hourly" in text
    assert "No job is firing" not in text and len(replies) == 1

    owned = SimpleNamespace(
        new=[],
        chronic=[],
        resolved=[],
        fix_not_working=[],
        notified=[_states("ipdsc", "owner_notified")],
    )
    text = json.dumps(digest.blocks(owned, scanned=160, findings=1, high=1, date="2026-08-26")[0])
    assert "ipdsc" in text and "owner notified" in text and "all clear" not in text


def _posted(monkeypatch: pytest.MonkeyPatch) -> list:
    """The Block Kit payload the sweep hands to Slack, captured instead of sent."""
    sent: list = []

    def record(parent: list, replies: list) -> dict:
        sent.append(parent)
        return {"sent": True}

    monkeypatch.setattr(sweep.notify_mod, "deliver_thread", record)
    return sent


def test_the_savings_log_reads_the_runs_ledger_not_the_default(fleet: Path) -> None:
    """A run given its own ledger must not compute savings from the CWD-relative default."""
    rows = [
        ledger.Entry(
            date=d,
            dag_id="good",
            app_id="a",
            key="skew:1",
            impact="high",
            title="Stage 1 skew",
            state="chronic",
            exec_h=100.0,
        )
        for d in ("2026-08-20", "2026-08-21")
    ]
    rows.append(
        ledger.Entry(
            date="2026-08-23",
            dag_id="good",
            app_id="a",
            key="skew:1",
            impact="high",
            title="Stage 1 skew",
            state="resolved",
            exec_h=40.0,
            fix_pr="https://x/pr/1",
            applied_date="2026-08-22",
        )
    )
    ledger.append(rows, str(fleet / "out" / "l.jsonl"))

    _run(fleet, "2026-08-24")
    savings = (fleet / "out" / "optimizer_savings.md").read_text()
    assert "Saved since 2026-08-22" in savings


def test_a_watching_only_ledger_does_not_headline_zero_savings(fleet: Path) -> None:
    """Between a fix landing and its first resolution there is nothing to announce."""
    p = str(fleet / "out" / "l.jsonl")
    ledger.append(
        [
            ledger.Entry(
                date="2026-08-24",
                dag_id="good",
                app_id="a",
                key="skew:1",
                impact="high",
                title="Stage 1 skew",
                state="chronic",
                exec_h=100.0,
            )
        ],
        p,
    )
    ledger.mark_applied("good", "skew:1", "https://x/pr/1", "2026-08-25", path=p)

    out = _run(fleet, "2026-08-26")
    assert "Saved since" not in out["slack"]


def test_a_sweep_that_lost_change_tracking_does_not_post_all_clear(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty delta means the ledger did not run, never that the fleet is clean."""
    monkeypatch.setattr(sweep.cov_mod, "collect_local", _blind)
    sent = _posted(monkeypatch)
    out = _run(fleet, "2026-08-12", airflow_base="local")
    assert out["findings"] == 1 and out["ledger_entries"] == 0
    assert "all clear" not in json.dumps(sent[0])
    assert "No change tracking" in json.dumps(sent[0])


def test_the_partial_sweep_caveat_reaches_the_channel_not_just_the_file(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sweep that lost half the fleet must not post indistinguishably from a whole one."""
    sent = _posted(monkeypatch)
    out = _run(fleet, "2026-08-13", complete=False)
    assert "Partial sweep" in out["slack"]
    assert "Partial sweep" in json.dumps(sent[0])


def test_the_digest_carries_the_savings_headline_with_dollars_when_a_rate_is_set(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The saved-to-date line leadership reads must reach Slack, not only a GCS file."""
    monkeypatch.setenv("OPTIMIZER_USD_PER_EXEC_H", "2.0")
    rows = [
        ledger.Entry(
            date=d,
            dag_id="good",
            app_id="a",
            key="skew:1",
            impact="high",
            title="Stage 1 skew",
            state="chronic",
            exec_h=100.0,
        )
        for d in ("2026-08-20", "2026-08-21")
    ]
    rows.append(
        ledger.Entry(
            date="2026-08-23",
            dag_id="good",
            app_id="a",
            key="skew:1",
            impact="high",
            title="Stage 1 skew",
            state="resolved",
            exec_h=40.0,
            fix_pr="https://x/pr/1",
            applied_date="2026-08-22",
        )
    )
    ledger.append(rows, str(fleet / "out" / "l.jsonl"))

    _run(fleet, "2026-08-24")
    text = (fleet / "out" / "optimizer_digest_2026-08-24.md").read_text()
    assert "Saved since 2026-08-22" in text and "$120" in text
    assert "est." in text


def test_the_live_billing_rate_wins_and_its_absence_falls_back_to_the_env(
    fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No billing access must not break a sweep or silently zero the dollars."""
    rows = [
        ledger.Entry(
            date=d,
            dag_id="good",
            app_id="a",
            key="skew:1",
            impact="high",
            title="Stage 1 skew",
            state="chronic",
            exec_h=100.0,
        )
        for d in ("2026-08-20", "2026-08-21")
    ]
    rows.append(
        ledger.Entry(
            date="2026-08-23",
            dag_id="good",
            app_id="a",
            key="skew:1",
            impact="high",
            title="Stage 1 skew",
            state="resolved",
            exec_h=40.0,
            fix_pr="https://x/pr/1",
            applied_date="2026-08-22",
        )
    )
    ledger.append(rows, str(fleet / "out" / "l.jsonl"))

    monkeypatch.setattr(sweep.billing_mod, "blended_usd_per_exec_h", lambda: (2.0, "live"))
    monkeypatch.setenv("OPTIMIZER_USD_PER_EXEC_H", "9.9")
    _run(fleet, "2026-08-24")
    text = (fleet / "out" / "optimizer_savings.md").read_text()
    assert "$2.00 per hour" in text

    monkeypatch.setattr(sweep.billing_mod, "blended_usd_per_exec_h", lambda: (None, "no access"))
    _run(fleet, "2026-08-25")
    text = (fleet / "out" / "optimizer_savings.md").read_text()
    assert "$9.90 per hour" in text
