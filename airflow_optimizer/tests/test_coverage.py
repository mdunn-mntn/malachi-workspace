"""Coverage-pass auth: the container has no astro CLI, so the injected token has to win."""

from __future__ import annotations

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
                        lambda _f: {"live": dag, "quiet": quiet, "off": dag})
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
    def _boom(_folder: str | None) -> dict:
        raise RuntimeError("bundle directory is empty")

    monkeypatch.setattr(coverage, "_paused_dag_ids", lambda: set())
    monkeypatch.setattr(coverage, "_load_bag", _boom)
    cov = coverage.collect_local("2026-08-21")
    assert not cov.dags
    assert "bundle directory is empty" in cov.error
    assert "Could not enumerate DAGs" in coverage.render(cov)


def test_an_unreachable_metadata_db_degrades_instead_of_blinding_the_sweep(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Airflow 3 forbids ORM access from task code, and losing paused state lost everything.

    In prod that meant `known` came back empty, so `sweep` declined to write ledger rows and
    change tracking sat frozen for three days. The bundle is on disk and parses fine; only the
    paused exclusion needs the DB, so only the paused exclusion may be lost.
    """
    dag = type("D", (), {"tasks": [type("DataprocSubmitJobOperator", (),
                                        {"task_id": "run"})()], "owner": "audi", "tags": []})()

    def _forbidden() -> set:
        raise RuntimeError("could not access attribute query because airflow session use is "
                           "forbidden in this context. Context manager was entered here:\n"
                           '  File "/astro-agent-package/astro-runtime/supervisor.py", line 1')

    monkeypatch.setattr(coverage, "_paused_dag_ids", _forbidden)
    monkeypatch.setattr(coverage, "_load_bag", lambda _f: {"live": dag, "off": dag})
    cov = coverage.collect_local("2026-08-21")

    assert cov.error == ""
    assert {d.dag_id for d in cov.dags} == {"live", "off"}       # the ledger can key on this
    assert "forbidden in this context" in cov.warning
    assert "\n" not in cov.warning                               # one line, not a traceback
    assert "counted as active" in coverage.render(cov)
    assert "Paused DAGs are counted as active" in cov.unprofiled_line()
