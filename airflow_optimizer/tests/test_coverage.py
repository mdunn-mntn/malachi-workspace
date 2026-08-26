"""The coverage pass: token auth, bundle parsing, and the counts it may not claim."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, NoReturn

import pytest

from airflow_optimizer import coverage


def _explode(*_a: Any, **_k: Any) -> NoReturn:
    raise AssertionError("shelled out to the astro CLI despite an injected token")


def test_bearer_prefers_the_injected_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Outside Airflow there is no CLI to ask, so an injected token is the only one there is."""
    monkeypatch.setenv("AIRFLOW_TI_API_TOKEN", "  tok-123  ")
    monkeypatch.setattr(coverage.subprocess, "run", _explode)
    assert coverage._bearer() == "tok-123"


def test_bearer_names_what_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A blank env var must not fall through to a stack trace about a missing file."""
    monkeypatch.setenv("AIRFLOW_TI_API_TOKEN", "   ")
    monkeypatch.setattr(coverage.os.path, "exists", lambda _p: False)
    with pytest.raises(RuntimeError, match="AIRFLOW_TI_API_TOKEN"):
        coverage._bearer()


def test_collect_local_classifies_without_a_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inside Airflow the DAG files are on disk, so coverage needs no credential at all."""
    spark = type("ModelPysparkBatchOperator", (), {"task_id": "run_model"})()
    dbx = type("DbxDbtOperator", (), {"task_id": "write_signal"})()
    plain = type("PythonOperator", (), {"task_id": "notify"})()
    dag = type("D", (), {"tasks": [spark, dbx, plain], "owner": "audi", "tags": ["spark"]})()
    quiet = type("D", (), {"tasks": [plain], "owner": "other", "tags": []})()

    monkeypatch.setattr(coverage, "_paused_dag_ids", lambda: {"off"})
    monkeypatch.setattr(coverage, "_load_bag",
                        lambda _f: ({"live": dag, "quiet": quiet, "off": dag}, {}))
    cov = coverage.collect_local("2026-08-21")
    assert cov.error == "" and cov.warning == ""
    assert [d.dag_id for d in cov.dags] == ["live", "quiet"]        # paused DAG dropped
    assert [d.dag_id for d in cov.profilable] == ["live"]
    assert [d.dag_id for d in cov.unprofiled] == ["quiet"]
    live = cov.dags[0]
    assert live.spark_tasks == ["run_model"]
    assert live.opaque_tasks == [("write_signal", coverage.OPAQUE_OPERATORS["DbxDbtOperator"])]


def test_collect_local_reports_a_failure_instead_of_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    """A coverage failure must never sink a sweep, but it must be VISIBLE on the report.

    The previous version of this test asserted only that `cov.error` was set, which it always
    is when Airflow is not installed, so it passed without exercising anything.
    """
    def _boom(_folder: str | None) -> tuple[dict, dict]:
        raise RuntimeError("bundle directory is empty")

    monkeypatch.setattr(coverage, "_paused_dag_ids", lambda: set())
    monkeypatch.setattr(coverage, "_load_bag", _boom)
    cov = coverage.collect_local("2026-08-21")
    assert not cov.dags
    assert "bundle directory is empty" in cov.error
    assert "Could not enumerate DAGs" in coverage.render(cov)


def test_an_unreachable_metadata_db_degrades_instead_of_blinding_the_sweep(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Losing paused state costs the paused exclusion only, not the whole DAG-id set."""
    dag = type("D", (), {"tasks": [type("DataprocSubmitJobOperator", (),
                                        {"task_id": "run"})()], "owner": "audi", "tags": []})()

    def _forbidden() -> set:
        raise RuntimeError("could not access attribute query because airflow session use is "
                           "forbidden in this context. Context manager was entered here:\n"
                           '  File "/astro-agent-package/astro-runtime/supervisor.py", line 1')

    monkeypatch.setattr(coverage, "_paused_dag_ids", _forbidden)
    monkeypatch.setattr(coverage, "_load_bag", lambda _f: ({"live": dag, "off": dag}, {}))
    cov = coverage.collect_local("2026-08-21")

    assert cov.error == ""
    assert {d.dag_id for d in cov.dags} == {"live", "off"}
    assert "forbidden in this context" in cov.warning
    assert "\n" not in cov.warning
    assert "counted as active" in coverage.render(cov)
    assert "Paused DAGs are counted as active" in cov.unprofiled_line()


def test_an_in_repo_subclass_of_a_spark_operator_is_not_read_as_no_spark_task(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """A subclass of a Spark operator still counts as a Spark task."""
    base = type("DataprocCreateBatchOperator", (), {})
    subclass = type("MntnDataprocBatchOperator", (base,), {"task_id": "submit"})()
    dag = type("D", (), {"tasks": [subclass], "owner": "audi", "tags": []})()

    monkeypatch.setattr(coverage, "_paused_dag_ids", lambda: set())
    monkeypatch.setattr(coverage, "_load_bag", lambda _f: ({"live": dag}, {}))
    cov = coverage.collect_local("2026-08-21")
    assert [d.dag_id for d in cov.profilable] == ["live"]
    assert cov.dags[0].spark_tasks == ["submit"]


def test_an_empty_bundle_is_an_error_not_a_fleet_with_no_dags(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero DAGs out of the bundle is an error, not a fleet with nothing in it."""
    monkeypatch.setattr(coverage, "_paused_dag_ids", lambda: set())
    monkeypatch.setattr(coverage, "_load_bag", lambda _f: ({}, {}))
    cov = coverage.collect_local("2026-08-21")
    assert "held no DAGs" in cov.error
    assert not cov.dag_ids_including_paused


def test_the_bundle_folder_comes_from_the_running_dag_not_the_config(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """collect_local parses the folder `_bundle_dag_folder` names, not DagBag's default."""
    seen = []
    folder = "/opt/airflow/dag_bundles/astro/main/dags/2026-08-10T21_33_02Z/dags"
    monkeypatch.setattr(coverage, "_bundle_dag_folder", lambda: folder)
    monkeypatch.setattr(coverage, "_paused_dag_ids", lambda: set())

    def _spy(folder: str | None) -> tuple[dict, dict]:
        seen.append(folder)
        return {}, {}

    monkeypatch.setattr(coverage, "_load_bag", _spy)
    coverage.collect_local("2026-08-21")
    assert seen == [folder]


def _airflow_sdk(monkeypatch: pytest.MonkeyPatch, get_current_context: Any) -> None:
    """Stand in for `airflow.sdk`, which is not installed in this test environment."""
    sdk = ModuleType("airflow.sdk")
    sdk.get_current_context = get_current_context
    monkeypatch.setitem(sys.modules, "airflow", ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)


def test_the_bundle_folder_is_the_nearest_dags_ancestor(monkeypatch: pytest.MonkeyPatch) -> None:
    """The outer `dags` is the parent of every retained bundle version, so the nearest one wins."""
    root = "/opt/airflow/dag_bundles/astro/main/dags/2026-08-10T21_33_02Z/dags"
    _airflow_sdk(monkeypatch,
                 lambda: {"dag": SimpleNamespace(fileloc=f"{root}/optimizer/sweep_dag.py")})
    assert coverage._bundle_dag_folder() == str(Path(root).resolve())


def test_a_layout_with_no_dags_ancestor_falls_back_to_the_dag_files_own_folder(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Not every deployment names the folder `dags`, and the file's own folder still parses."""
    folder = "/opt/airflow/bundles/main/pipelines"
    _airflow_sdk(monkeypatch,
                 lambda: {"dag": SimpleNamespace(fileloc=f"{folder}/sweep_dag.py")})
    assert coverage._bundle_dag_folder() == str(Path(folder).resolve())


def test_no_running_dag_leaves_the_bundle_folder_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Guessing a folder is worse than none, which lets DagBag fall back to its own default."""
    def _outside_a_task() -> dict:
        raise RuntimeError("no current task context")

    _airflow_sdk(monkeypatch, lambda: {"dag": SimpleNamespace(fileloc="")})
    assert coverage._bundle_dag_folder() is None
    _airflow_sdk(monkeypatch, _outside_a_task)
    assert coverage._bundle_dag_folder() is None


def test_an_unparsed_dag_file_is_named_on_the_report(monkeypatch: pytest.MonkeyPatch) -> None:
    """A file that failed to import is missing from every count, so the count has to say so."""
    dag = type("D", (), {"tasks": [type("DataprocSubmitJobOperator", (),
                                        {"task_id": "run"})()], "owner": "audi", "tags": []})()
    monkeypatch.setattr(coverage, "_paused_dag_ids", lambda: set())
    monkeypatch.setattr(coverage, "_load_bag",
                        lambda _f: ({"live": dag}, {"/bundle/dags/targeting/broken_dag.py": "x"}))
    cov = coverage.collect_local("2026-08-21")
    assert cov.unparsed_files == ["broken_dag.py"]
    assert "broken_dag.py" in coverage.render(cov)
    assert "failed to import" in cov.unprofiled_line()


def test_collect_keys_on_paused_dags_too(monkeypatch: pytest.MonkeyPatch) -> None:
    """The API path indexes paused DAGs too, so both paths produce the same id set."""
    pages = [{"dags": [{"dag_id": "live", "is_paused": False},
                       {"dag_id": "off", "is_paused": True}], "total_entries": 2}]
    monkeypatch.setattr(coverage, "_airflow",
                        lambda _b, _t, path, _p: pages.pop(0) if path == "/dags" else {"tasks": []})
    cov = coverage.collect("https://astro.example/api/v2", "2026-08-21", token="tok")
    assert cov.dag_ids_including_paused == {"live", "off"}
    assert [d.dag_id for d in cov.dags] == ["live"]


def test_resolve_ties_a_job_name_to_the_dag_that_runs_it() -> None:
    """A Spark app names the table it populates, which is a task id, never a dag_id.

    Matching job names against dag_ids alone resolved 0 of 57 names in the 2026-08-25 prod
    sweep, which counted every DAG that ran one of them as having produced no log.
    """
    cov = coverage.Coverage(date="2026-08-25", dag_ids_including_paused={"feature_store_hourly",
                                                                        "audience_intent"})
    cov.dags = [
        coverage.DagCoverage(dag_id="feature_store_hourly",
                             spark_tasks=["feature_group_1_source.aug_log_ip_hourly"]),
        coverage.DagCoverage(dag_id="audience_intent", spark_tasks=["fangorn_score_monitor"]),
    ]
    assert cov.resolve("aug_log_ip_hourly") == "feature_store_hourly"
    assert cov.resolve("fangorn_score_monitor") == "audience_intent"
    assert cov.resolve("audience_intent") == "audience_intent"


def test_a_task_name_two_dags_share_is_dropped_not_guessed() -> None:
    """A wrong Airflow link sends an owner to someone else's DAG, which is worse than no link."""
    cov = coverage.Coverage(date="x", dag_ids_including_paused={"dag_a", "dag_b"})
    cov.dags = [coverage.DagCoverage(dag_id="dag_a", spark_tasks=["run"]),
                coverage.DagCoverage(dag_id="dag_b", spark_tasks=["run"])]
    assert cov.resolve("run") == ""
    assert cov.resolve("dag_a") == "dag_a"


def test_job_keys_strips_only_what_it_can_justify() -> None:
    """A data-source or run index is a per-run stamp; anything else is part of the name.

    Both dotted segments are offered because a Spark app name and an Airflow task id put the
    job in opposite halves of the dot.
    """
    keys = coverage.job_keys
    assert "site_network_hourly" in keys("Populate site_network_hourly.SiteNetworkHourly")
    assert "aug_log_ip_hourly" in keys("feature_group_1_source.aug_log_ip_hourly")
    assert "ipdsc_monitor" in keys("Populate ipdsc_14_monitor.IPDSC14Monitor")
    assert keys("ipdsc_ds_13") == ["ipdsc_ds"]
    assert keys("audience_intent_scoring_staging_ds46") == ["audience_intent_scoring_staging"]
    assert keys("materialize_mntn_select_16") == ["materialize_mntn_select"]
    assert keys("conv_log_derived_ip") == ["conv_log_derived_ip"]


def test_the_report_names_every_job_it_could_not_tie_to_a_dag() -> None:
    """A silent gap reads as full coverage. Each miss states which of the three causes it is."""
    cov = coverage.Coverage(date="x", dag_ids_including_paused={"feature_store_hourly", "a", "b"})
    cov.dags = [
        coverage.DagCoverage(dag_id="feature_store_hourly", spark_tasks=["src.aug_log_ip_hourly"]),
        coverage.DagCoverage(dag_id="a", spark_tasks=["shared"]),
        coverage.DagCoverage(dag_id="b", spark_tasks=["shared"]),
    ]
    why = dict(cov.unresolved({"aug_log_ip_hourly", "shared", "app-20260825010524489-0368",
                               "never_heard_of_it"}))
    assert "aug_log_ip_hourly" not in why                       # resolved, so not listed
    assert "named by 2 DAGs" in why["shared"]
    assert "no app name" in why["app-20260825010524489-0368"]
    assert "no DAG in the bundle" in why["never_heard_of_it"]
    assert "could not be tied to a DAG" in coverage.render(cov, set(), {"shared"})


def test_a_name_that_is_one_dags_task_and_another_dags_id_reads_as_ambiguous() -> None:
    """One index for both views, so `unresolved` cannot deny a DAG the same object indexes."""
    cov = coverage.Coverage(date="x", dag_ids_including_paused={"feature_store_hourly",
                                                               "aug_log_ip_hourly"})
    cov.dags = [
        coverage.DagCoverage(dag_id="feature_store_hourly", spark_tasks=["grp.aug_log_ip_hourly"]),
        coverage.DagCoverage(dag_id="aug_log_ip_hourly", spark_tasks=["aug_log_ip_hourly"]),
    ]
    assert cov.resolve("aug_log_ip_hourly") == ""
    assert dict(cov.unresolved({"aug_log_ip_hourly"}))["aug_log_ip_hourly"].startswith(
        "named by 2 DAGs")


def test_a_profiled_job_is_counted_against_the_dag_that_runs_it() -> None:
    """The sweep profiles job NAMES, so counting them against dag_ids called a live DAG dark."""
    cov = coverage.Coverage(date="x", dag_ids_including_paused={"feature_store_hourly"})
    cov.dags = [coverage.DagCoverage(dag_id="feature_store_hourly",
                                     spark_tasks=["grp.aug_log_ip_hourly"])]
    page = coverage.render(cov, {"aug_log_ip_hourly"}, set())
    assert "- profiled this sweep: 1" in page
    assert "produced no log" not in page


def test_a_spark_app_name_resolves_to_the_dag_that_runs_it() -> None:
    """`Populate <table>.<Class>` names the TABLE, so the segment before the dot is the task.

    Keying on the segment after the dot tied 0 of 62 job names to a DAG in the 30-day corpus
    crawled 2026-08-26, which is why the 2026-08-25 prod digest linked one DAG out of eight.
    """
    cov = coverage.Coverage(date="x", dag_ids_including_paused={"tpa_export", "ipdsc_monitor"})
    cov.dags = [coverage.DagCoverage(dag_id="tpa_export", spark_tasks=["site_network_hourly"]),
                coverage.DagCoverage(dag_id="ipdsc_monitor", spark_tasks=["ipdsc_monitor"])]
    assert cov.resolve("Populate site_network_hourly.SiteNetworkHourly") == "tpa_export"
    assert cov.resolve("Populate ipdsc_14_monitor.IPDSC14Monitor") == "ipdsc_monitor"
    assert cov.resolve("Populate never_heard_of_it.NeverHeardOfIt") == ""
    # what the sweep actually passes: ledger._dag_id has already reduced the name
    assert cov.resolve("site_network_hourly") == "tpa_export"
    assert cov.resolve("ipdsc_14_monitor") == "ipdsc_monitor"


def test_resolve_walks_past_an_ambiguous_candidate_to_one_that_is_not() -> None:
    """The FIRST candidate here is ambiguous, so only the fallthrough can resolve the job.

    `Populate src.aug_log_ip` offers `src` first, which two DAGs claim, then `aug_log_ip`,
    which one does. Stopping at the first ambiguous candidate would leave the job unlinked.
    """
    cov = coverage.Coverage(date="x", dag_ids_including_paused={"a", "b", "feature_store"})
    cov.dags = [coverage.DagCoverage(dag_id="a", spark_tasks=["src.shared"]),
                coverage.DagCoverage(dag_id="b", spark_tasks=["src.other"]),
                coverage.DagCoverage(dag_id="feature_store", spark_tasks=["src.aug_log_ip"])]
    assert coverage.job_keys("Populate src.aug_log_ip")[0] == "src"
    assert len(cov._owners()["src"]) == 3
    assert cov.resolve("Populate src.aug_log_ip") == "feature_store"
    assert cov.resolve("Populate src.Src") == ""
