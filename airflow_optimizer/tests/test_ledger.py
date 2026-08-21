"""Ledger state machine and digest rendering."""

from __future__ import annotations

from pathlib import Path

from airflow_optimizer import digest, ledger
from airflow_optimizer.crawl import JobReport
from airflow_optimizer.optimizations import OptFinding

# The real base is resolved from the deployment's own env, so tests pass one explicitly.
UI = "https://airflow.example.com/dags/{dag_id}"

FETCH = OptFinding("shuffle_fetch_wait", "Stage 9 spends 73% of task time waiting on shuffle fetch",
                   "high", "why", "fix", rec_type="code")
IDLE = OptFinding("idle_reserved_executors", "Executors 16% utilized: ~31 idle executor-hours held",
                  "high", "why", "fix", rec_type="infra")


def _report(app: str, *findings: OptFinding) -> JobReport:
    return JobReport(source=f"{app}.zstd", findings=list(findings),
                     app_name="Populate site_network_hourly.SiteNetworkHourly")


def test_key_uses_the_stage_not_every_number() -> None:
    """Task counts and byte totals move every run; only the stage number is stable."""
    assert ledger.finding_key(FETCH) == "shuffle_fetch_wait:9"
    # no stage in the title -> the detector alone, never a dangling colon
    assert ledger.finding_key(IDLE) == "idle_reserved_executors"


def test_states_walk_new_recurring_chronic(tmp_path: Path) -> None:
    """Three sweeps of the same finding is a standing defect, not three incidents."""
    p = str(tmp_path / "l.jsonl")
    assert [e.state for e in ledger.record([_report("a", FETCH)], "2026-08-18", path=p)] == ["new"]
    assert [e.state for e in ledger.record([_report("b", FETCH)], "2026-08-19", path=p)] == ["recurring"]
    third = ledger.record([_report("c", FETCH)], "2026-08-20", path=p)
    assert (third[0].state, third[0].streak) == ("chronic", 3)


def test_shipped_fix_that_works_is_attributed_and_costed(tmp_path: Path) -> None:
    """A shipped fix carries its PR forward and the register shows what the DAG cost after."""
    p = str(tmp_path / "l.jsonl")
    ledger.record([_report("a", FETCH)], "2026-08-01", dcu={"site_network_hourly": 100.0}, path=p)
    ledger.mark_applied("site_network_hourly", "shuffle_fetch_wait:9",
                        "https://github.com/x/y/pull/9", "2026-08-03", path=p)
    for date in ("2026-08-04", "2026-08-05", "2026-08-06"):  # finding gone, DAG cheaper
        ledger.record([_report("b", IDLE)], date, dcu={"site_network_hourly": 40.0}, path=p)
    row = ledger.shipped(p)[0]
    assert row["outcome"] == "resolved", row
    assert row["fix_pr"].endswith("/9") and row["applied_date"] == "2026-08-03"
    assert (row["dcu_h_before"], row["dcu_h_after"]) == (100.0, 40.0)


def test_shipped_fix_that_does_not_work_says_so(tmp_path: Path) -> None:
    """A merged fix is not a verified fix: if the finding keeps firing the register says so."""
    p = str(tmp_path / "l.jsonl")
    ledger.record([_report("a", FETCH)], "2026-08-01", path=p)
    ledger.mark_applied("site_network_hourly", "shuffle_fetch_wait:9",
                        "https://github.com/x/y/pull/11", "2026-08-02", path=p)
    for date in ("2026-08-03", "2026-08-04", "2026-08-05"):
        entries = ledger.record([_report("b", FETCH)], date, path=p)
    assert entries[0].state == "fix_not_working", entries[0]
    assert entries[0].fix_pr.endswith("/11"), entries[0]
    assert ledger.shipped(p)[0]["outcome"] == "fix_not_working"


def test_mark_applied_needs_history_and_a_pr(tmp_path: Path) -> None:
    """The register records real work, so it refuses an unattributed or unknown entry."""
    p = str(tmp_path / "l.jsonl")
    ledger.record([_report("a", FETCH)], "2026-08-01", path=p)
    for args in (("site_network_hourly", "shuffle_fetch_wait:9", "", "2026-08-02"),
                 ("no_such_dag", "shuffle_fetch_wait:9", "pr", "2026-08-02")):
        try:
            ledger.mark_applied(*args, path=p)
        except ValueError:
            continue
        raise AssertionError(f"accepted a bad mark_applied: {args}")


def test_one_entry_per_key_per_sweep(tmp_path: Path) -> None:
    """An hourly job contributes ~24 logs a day; the ledger must not count each as a finding."""
    p = str(tmp_path / "l.jsonl")
    entries = ledger.record(
        [_report("a", FETCH), _report("b", FETCH), _report("c", FETCH, IDLE)], "2026-08-18", path=p)
    assert sorted(e.key for e in entries) == ["idle_reserved_executors", "shuffle_fetch_wait:9"]


def test_owner_notified_is_sticky(tmp_path: Path) -> None:
    """A human decision must not be overwritten by the next replay."""
    p = str(tmp_path / "l.jsonl")
    ledger.record([_report("a", FETCH)], "2026-08-18", path=p)
    ledger.set_state("site_network_hourly", "shuffle_fetch_wait:9", "owner_notified",
                     note="asked Ryan", date="2026-08-18", path=p)
    again = ledger.record([_report("b", FETCH)], "2026-08-19", path=p)
    assert again[0].state == "owner_notified"
    assert again[0].note == "asked Ryan"


def test_set_state_rejects_a_derived_state(tmp_path: Path) -> None:
    """chronic/resolved are computed; hand-setting them would desync the replay."""
    p = str(tmp_path / "l.jsonl")
    try:
        ledger.set_state("d", "k", "chronic", path=p)
    except ValueError:
        return
    raise AssertionError("set_state accepted a derived state")


def test_resolved_only_after_the_grace_window(tmp_path: Path) -> None:
    """A job that skipped one sweep is not fixed; three quiet sweeps is the bar."""
    p = str(tmp_path / "l.jsonl")
    ledger.record([_report("a", FETCH, IDLE)], "2026-08-18", path=p)
    # fetch-wait gone, but still inside the window
    assert not [e for e in ledger.record([_report("b", IDLE)], "2026-08-19", path=p)
                if e.state == "resolved"]
    assert not [e for e in ledger.record([_report("c", IDLE)], "2026-08-20", path=p)
                if e.state == "resolved"]
    out = ledger.record([_report("d", IDLE)], "2026-08-21", path=p)
    assert [e.key for e in out if e.state == "resolved"] == ["shuffle_fetch_wait:9"]


def test_a_torn_line_does_not_sink_the_sweep(tmp_path: Path) -> None:
    """An interrupted append leaves half a line; the next sweep must still run."""
    p = tmp_path / "l.jsonl"
    ledger.record([_report("a", FETCH)], "2026-08-18", path=str(p))
    with open(p, "a") as fh:
        fh.write('{"date": "2026-08-19", "dag_id": "x"')  # no newline, no close
    assert ledger.record([_report("b", FETCH)], "2026-08-20", path=str(p))[0].state == "recurring"


def test_digest_leads_with_the_delta_and_links_the_dag(tmp_path: Path) -> None:
    """The digest is read for what changed, so a no-change sweep must say so plainly."""
    p = str(tmp_path / "l.jsonl")
    entries = ledger.record([_report("a", FETCH)], "2026-08-18",
                            dcu={"site_network_hourly": 8663}, path=p)
    text = digest.render(ledger.delta(entries), scanned=214, findings=278, high=197,
                         date="2026-08-18", backlog_path="outputs/b.md", base=UI)
    assert "214 Spark jobs scanned, 278 findings, 197 high." in text
    assert "*New today*" in text
    assert "|site_network_hourly>" in text  # a link, not a bare name
    assert "8,663 DCU-h/day" in text

    quiet = digest.render(ledger.Delta(), scanned=0, findings=0, high=0, date="2026-08-19")
    assert "No change since the last sweep." in quiet


def test_digest_plain_text_drops_slack_markup() -> None:
    """The file copy has to be readable without a Slack client."""
    plain = digest.render_plain("- *HIGH* <https://x/dags/d|d> — thing")
    assert plain == "- HIGH d (https://x/dags/d) — thing"


def test_run_stamps_are_stripped_but_data_source_ids_are_not() -> None:
    """`_16` is a run index; `_67` is a data source. Merging them loses three jobs into one."""
    known = {"materialize_mntn_select", "site_network_hourly"}

    def name(n: str) -> str:
        return ledger._dag_id(JobReport(source="x.zstd", app_name=n), known)

    assert name("Populate site_network_hourly.SiteNetworkHourly") == "site_network_hourly"
    assert name("segment-updates-to-parquet-2026-08-20-[19]") == "segment-updates-to-parquet"
    assert name("mntn-select-2026-08-20-1787258726-1") == "mntn-select"
    # stripped only because the base IS a known DAG
    assert name("materialize_mntn_select_16") == "materialize_mntn_select"
    # not a known DAG -> the suffix is data, keep it
    assert name("Populate ipdsc_ds_67.DS67") == "ipdsc_ds_67"
    assert name("Populate ipdsc_ds_13.DS13") == "ipdsc_ds_13"


def test_digest_does_not_link_a_dag_that_does_not_exist(tmp_path: Path) -> None:
    """A Spark app name is not always a dag_id; a dead link costs the reader trust."""
    class _Cov:
        dags = [type("D", (), {"dag_id": "site_network_hourly"})()]
        unprofiled: list = []
        error = ""
        report_path = ""

        def unprofiled_line(self) -> str:
            return "0 active DAGs had no Spark task to profile."

    p = str(tmp_path / "l.jsonl")
    entries = ledger.record([_report("a", FETCH)], "2026-08-18", path=p)
    entries.append(ledger.Entry(date="2026-08-18", dag_id="segment-updates-to-parquet",
                                app_id="b", key="shuffle_fetch_wait:2", impact="high",
                                title="Stage 2 spends 64% of task time waiting on shuffle fetch",
                                state="new"))
    text = digest.render(ledger.delta(entries), scanned=2, findings=2, high=2,
                         date="2026-08-18", coverage=_Cov(), base=UI)
    assert "|site_network_hourly>" in text          # known -> linked
    assert "`segment-updates-to-parquet`" in text   # unknown -> plain
    assert "dags/segment-updates-to-parquet|" not in text


def test_digest_falls_back_to_plain_text_with_no_ui_base() -> None:
    """A link to the wrong deployment is worse than no link, so an unset base drops the link."""
    assert digest.dag_link("site_network_hourly", base="") == "`site_network_hourly`"


def test_ui_base_prefers_the_override_then_airflow_config(monkeypatch: object) -> None:
    """Resolved per deployment: a dev Airflow must not link findings at prod."""
    monkeypatch.setenv("OPTIMIZER_AIRFLOW_UI", "https://x/dags/{dag_id}")
    monkeypatch.setenv("AIRFLOW__API__BASE_URL", "https://ignored")
    assert digest._ui_base() == "https://x/dags/{dag_id}"

    monkeypatch.delenv("OPTIMIZER_AIRFLOW_UI")
    assert digest._ui_base() == "https://ignored/dags/{dag_id}"

    monkeypatch.delenv("AIRFLOW__API__BASE_URL")
    monkeypatch.delenv("AIRFLOW__WEBSERVER__BASE_URL", raising=False)
    assert digest._ui_base() == ""
