"""Ledger state machine and digest rendering."""

from __future__ import annotations

from pathlib import Path

from airflow_optimizer import crawl, digest, ledger
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
    for block in ("*What*", "*Where*", "*Why*", "*How*"):
        assert block in text
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

        def resolve(self, name: str) -> str:
            return name if name == "site_network_hourly" else ""

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


def test_a_finding_carries_its_dags_executor_hours_for_the_sweep_day(tmp_path: Path) -> None:
    """Ranking by finding count puts a chatty cheap job above a quiet expensive one, and a
    multi-run dag costs the sum of its runs, not whichever run happened to hold the finding."""
    class _R:
        def __init__(self, name: str, hours: float) -> None:
            self.source, self.app_name, self.exec_h, self.error = f"{name}.zstd", name, hours, None
            self.findings = [OptFinding("shuffle_fetch_wait", "t", "high", "w", "f")]

    path = str(tmp_path / "l.jsonl")
    rows = ledger.record([_R("cheap", 0.4), _R("expensive", 812.5)], "2026-08-25", path=path)
    got = {e.dag_id: e.exec_h for e in rows}
    assert got == {"cheap": 0.4, "expensive": 812.5}
    assert all(e.dcu_h is None for e in rows)          # measured DCU is a separate, unset field
    rows = ledger.record([_R("hourly", 10.0), _R("hourly", 2.0)], "2026-08-26", path=path)
    assert [(e.dag_id, e.exec_h) for e in rows] == [("hourly", 12.0)]


def test_one_bad_dag_cannot_fill_the_whole_digest(tmp_path: Path) -> None:
    """The 2026-08-21 digest opened with eight consecutive fangorn_score_monitor lines."""
    class _R:
        def __init__(self, name: str, n: int, hours: float) -> None:
            self.source, self.app_name, self.exec_h, self.error = f"{name}.zstd", name, hours, None
            self.findings = [OptFinding(f"d{i}", f"stage {i} spills", "high", "w", f"fix {i}")
                             for i in range(n)]

    entries = ledger.record([_R("noisy_cheap", 8, 2.0), _R("quiet_expensive", 1, 900.0)],
                            "2026-08-25", path=str(tmp_path / "l.jsonl"))
    text = digest.render(ledger.delta(entries), scanned=2, findings=9, high=9, date="2026-08-25")

    assert text.index("quiet_expensive") < text.index("noisy_cheap")   # cost outranks count
    assert text.count("*What*") == 2                                   # one block per DAG
    assert "+7 more findings on this DAG" in text
    assert "900 executor-hours" in text
    assert text.count("executor-hours") == 2   # per run, never summed across its findings


def test_a_failed_fix_keeps_its_slot_and_its_label(tmp_path: Path) -> None:
    """A shipped fix that did not work is the one thing louder DAGs must not crowd out."""
    class _R:
        def __init__(self, name: str, hours: float) -> None:
            self.source, self.app_name, self.exec_h, self.error = f"{name}.zstd", name, hours, None
            self.findings = [FETCH]

    p = str(tmp_path / "l.jsonl")
    ledger.record([_R("payments_etl", 9.0)], "2026-08-02", path=p)
    ledger.mark_applied("payments_etl", "shuffle_fetch_wait:9",
                        "https://github.com/x/y/pull/42", "2026-08-02", path=p)
    for date in ("2026-08-03", "2026-08-04", "2026-08-05"):
        entries = ledger.record([_R("payments_etl", 9.0)]
                                + [_R(f"noise_{i}", 10.0 + i) for i in range(4)], date, path=p)

    text = digest.render(ledger.delta(entries), scanned=5, findings=5, high=5, date="2026-08-05")
    assert "*Fix not working*" in text
    assert "payments_etl" in text


def test_a_still_rolling_app_is_costed_not_ranked_free() -> None:
    """A killed or in-flight app writes no ApplicationEnd and releases no executors.

    Costing it 0.0 sorts the fleet's biggest runaway last, which is the opposite of the truth.
    """
    class _E:
        def __init__(self, added: int, removed: int | None) -> None:
            self.added_ts, self.removed_ts, self.run_time_ms = added, removed, 0

    hour = 3_600_000
    held = type("R", (), {"executors": [_E(0, None) for _ in range(40)],
                          "app_end_ts": None, "last_event_ts": 20 * hour})()
    assert crawl.executor_hours(held) == 800.0

    ended = type("R", (), {"executors": [_E(0, 20 * hour) for _ in range(40)],
                           "app_end_ts": None, "last_event_ts": 20 * hour})()
    assert crawl.executor_hours(ended) == 800.0

    blind = type("R", (), {"executors": [_E(0, None)], "app_end_ts": None,
                           "last_event_ts": None})()
    assert crawl.executor_hours(blind) == 0.0     # no clock at all: unknown, not free


def test_digest_links_the_dag_and_names_the_job_when_they_differ(tmp_path: Path) -> None:
    """The link goes to the DAG; the job name stays beside it because they differ."""
    class _Cov:
        dags = [type("D", (), {"dag_id": "feature_store_hourly"})()]
        unprofiled: list = []
        error = ""
        report_path = ""

        def resolve(self, name: str) -> str:
            return "feature_store_hourly" if name == "aug_log_ip_hourly" else ""

        def unprofiled_line(self) -> str:
            return "0 active DAGs had no Spark task to profile."

    entries = [ledger.Entry(date="2026-08-18", dag_id="aug_log_ip_hourly", app_id="app-1",
                            key="shuffle_fetch_wait:2", impact="high",
                            title="Stage 2 spends 64% of task time waiting on shuffle fetch",
                            state="new")]
    text = digest.render(ledger.delta(entries), scanned=1, findings=1, high=1,
                         date="2026-08-18", coverage=_Cov(), base=UI)
    assert "dags/feature_store_hourly|feature_store_hourly>" in text
    assert "`aug_log_ip_hourly`" in text
    assert "dags/aug_log_ip_hourly|" not in text


def test_stopped_firing_links_its_dags_and_says_what_it_left_out() -> None:
    """The one section that used to print raw job names while every other line resolved them."""
    class _Cov:
        dags = [type("D", (), {"dag_id": "tpa_ipdsc_export"})()]
        unprofiled: list = []
        error = ""
        report_path = ""

        def resolve(self, name: str) -> str:
            return "tpa_ipdsc_export" if name.startswith("ipdsc_ds_") else ""

        def unprofiled_line(self) -> str:
            return "0 active DAGs had no Spark task to profile."

    entries = [ledger.Entry(date="2026-08-18", dag_id=f"ipdsc_ds_{i}", app_id=f"app-{i}",
                            key="disk_spill:1", impact="high", title="Stage 1 spilled",
                            state="resolved") for i in range(12)]
    text = digest.render(ledger.delta(entries), scanned=12, findings=0, high=0,
                         date="2026-08-18", coverage=_Cov(), base=UI)
    line = next(x for x in text.splitlines() if x.startswith("*Stopped firing*"))
    assert "dags/tpa_ipdsc_export|tpa_ipdsc_export>" in line
    assert "and 4 more" in line
    assert line.count("ipdsc_ds_") == digest.CAP


def test_savings_counts_only_resolved_fixes_in_measured_units(tmp_path: Path) -> None:
    """A working fix accrues before-minus-after per day observed; a broken one accrues nothing."""
    p = str(tmp_path / "ledger.jsonl")
    rows = []
    for i, day in enumerate(("2026-08-20", "2026-08-21", "2026-08-22")):
        rows.append(ledger.Entry(date=day, dag_id="good", app_id=f"a{i}", key="skew:1",
                                 impact="high", title="Stage 1 skew", state="chronic",
                                 exec_h=100.0))
        rows.append(ledger.Entry(date=day, dag_id="bad", app_id=f"b{i}", key="spill:2",
                                 impact="high", title="Stage 2 spill", state="chronic",
                                 exec_h=80.0))
    ledger.append(rows, p)
    ledger.mark_applied("good", "skew:1", "https://x/pr/1", "2026-08-22", path=p)
    ledger.mark_applied("bad", "spill:2", "https://x/pr/2", "2026-08-22", path=p)
    after = []
    for day in ("2026-08-23", "2026-08-24"):
        after.append(ledger.Entry(date=day, dag_id="good", app_id="a9", key="skew:1",
                                  impact="high", title="Stage 1 skew", state="resolved",
                                  exec_h=40.0, fix_pr="https://x/pr/1",
                                  applied_date="2026-08-22"))
        after.append(ledger.Entry(date=day, dag_id="bad", app_id="b9", key="spill:2",
                                  impact="high", title="Stage 2 spill", state="fix_not_working",
                                  exec_h=80.0, fix_pr="https://x/pr/2",
                                  applied_date="2026-08-22"))
    ledger.append(after, p)

    s = ledger.savings(p)
    assert s["since"] == "2026-08-22"
    good = next(r for r in s["rows"] if r["dag_id"] == "good")
    assert good["days_observed"] == 2 and abs(good["exec_h_saved"] - 120.0) < 1e-6
    bad = next(r for r in s["rows"] if r["dag_id"] == "bad")
    assert bad["exec_h_saved"] is None
    assert abs(s["total_exec_h_saved"] - 120.0) < 1e-6
    text = ledger.render_savings(s)
    assert "Saved since 2026-08-22: 120 executor-hours" in text
    assert "fix_not_working" in text


def test_a_fix_that_fully_cleans_a_job_still_measures_savings(tmp_path: Path) -> None:
    """The best outcome is a job with no findings left; its savings must not read as zero."""
    class _R:
        def __init__(self, name: str, hours: float, *findings: OptFinding) -> None:
            self.source, self.app_name, self.exec_h, self.error = f"{name}.zstd", name, hours, None
            self.findings = list(findings)

    p = str(tmp_path / "l.jsonl")
    for day in ("2026-08-01", "2026-08-02", "2026-08-03"):
        ledger.record([_R("solo", 100.0, FETCH), _R("noisy", 10.0, IDLE)], day, path=p)
    ledger.mark_applied("solo", "shuffle_fetch_wait:9", "https://x/pr/7", "2026-08-03", path=p)
    for day in ("2026-08-04", "2026-08-05", "2026-08-06"):
        ledger.record([_R("solo", 40.0), _R("noisy", 10.0, IDLE)], day, path=p)

    s = ledger.savings(p)
    row = next(r for r in s["rows"] if r["dag_id"] == "solo")
    assert row["outcome"] == "resolved"
    assert row["days_observed"] == 1
    assert abs(row["exec_h_saved"] - 60.0) < 1e-6
    assert abs(s["total_exec_h_saved"] - 60.0) < 1e-6


def test_savings_measures_the_dag_not_whichever_run_iterated_last(tmp_path: Path) -> None:
    """A no-op fix on a multi-run dag must measure zero, not the gap between two of its runs."""
    class _R:
        def __init__(self, name: str, hours: float, *findings: OptFinding) -> None:
            self.source, self.app_name, self.exec_h, self.error = f"{name}.zstd", name, hours, None
            self.findings = list(findings)

    p = str(tmp_path / "l.jsonl")
    for day in ("2026-08-01", "2026-08-02", "2026-08-03"):
        ledger.record([_R("hourly", 10.0, FETCH), _R("hourly", 2.0), _R("noisy", 1.0, IDLE)],
                      day, path=p)
    ledger.mark_applied("hourly", "shuffle_fetch_wait:9", "https://x/pr/5", "2026-08-03", path=p)
    for day in ("2026-08-04", "2026-08-05", "2026-08-06"):
        ledger.record([_R("hourly", 10.0), _R("hourly", 2.0), _R("noisy", 1.0, IDLE)], day, path=p)

    s = ledger.savings(p)
    row = next(r for r in s["rows"] if r["dag_id"] == "hourly")
    assert row["outcome"] == "resolved"
    assert abs(row["exec_h_saved"]) < 1e-6
    assert abs(s["total_exec_h_saved"]) < 1e-6


def test_savings_counts_a_dag_once_across_its_resolved_findings(tmp_path: Path) -> None:
    """One PR often clears several findings on one job; the job's saving must not multiply."""
    p = str(tmp_path / "l.jsonl")
    rows = []
    for day in ("2026-08-01", "2026-08-02"):
        for key, title in (("skew:1", "Stage 1 skew"), ("spill:2", "Stage 2 spill")):
            rows.append(ledger.Entry(date=day, dag_id="jobx", app_id="a", key=key,
                                     impact="high", title=title, state="recurring",
                                     exec_h=100.0))
    ledger.append(rows, p)
    for key in ("skew:1", "spill:2"):
        ledger.mark_applied("jobx", key, "https://x/pr/3", "2026-08-03", path=p)
    ledger.append([ledger.Entry(date="2026-08-04", dag_id="jobx", app_id="a", key=key,
                                impact="high", title="t", state="resolved", exec_h=40.0,
                                fix_pr="https://x/pr/3", applied_date="2026-08-03")
                   for key in ("skew:1", "spill:2")], p)

    s = ledger.savings(p)
    assert [r["exec_h_saved"] for r in s["rows"]] == [60.0, 60.0]
    assert abs(s["total_exec_h_saved"] - 60.0) < 1e-6
    assert "60 executor-hours" in ledger.render_savings(s)


def test_savings_reports_ytd_run_rate_and_annual_estimate(tmp_path: Path) -> None:
    """The leadership view: all-time vs this year vs where the run rate lands in a year."""
    p = str(tmp_path / "l.jsonl")
    ledger.append([ledger.Entry(date=d, dag_id="jobx", app_id="a", key="skew:1", impact="high",
                                title="Stage 1 skew", state="chronic", exec_h=100.0)
                   for d in ("2025-12-27", "2025-12-28")], p)
    ledger.mark_applied("jobx", "skew:1", "https://x/pr/9", "2025-12-29", path=p)
    ledger.append([ledger.Entry(date=d, dag_id="jobx", app_id="a", key="skew:1", impact="high",
                                title="Stage 1 skew", state="resolved", exec_h=40.0,
                                fix_pr="https://x/pr/9", applied_date="2025-12-29")
                   for d in ("2025-12-30", "2025-12-31", "2026-01-02", "2026-01-03")], p)

    s = ledger.savings(p)
    assert s["ytd_year"] == "2026"
    assert abs(s["total_exec_h_saved"] - 240.0) < 1e-6
    assert abs(s["ytd_exec_h_saved"] - 120.0) < 1e-6
    assert abs(s["run_rate_exec_h_per_day"] - 60.0) < 1e-6
    assert abs(s["est_annual_exec_h"] - 21900.0) < 1e-6
    assert "$" not in ledger.savings_headline(s)

    head = ledger.savings_headline(ledger.savings(p, usd_per_exec_h=2.0))
    assert "$480" in head and "$240" in head and "$43,800" in head and "$2.00" in head
    assert "21,900" in head
