"""Coverage-pass auth: the container has no astro CLI, so the injected token has to win."""

from __future__ import annotations

from typing import Any, NoReturn

import pytest

from airflow_optimizer import coverage


def _explode(*_a: Any, **_k: Any) -> NoReturn:
    raise AssertionError("shelled out to the astro CLI despite an injected token")


def test_bearer_prefers_the_injected_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ExternalSecret value is the only token that exists in the CronJob."""
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

    monkeypatch.setattr(coverage, "_load_bag_and_paused",
                        lambda _f: ({"live": dag, "quiet": quiet, "off": dag}, {"off"}))
    cov = coverage.collect_local("2026-08-21")
    assert cov.error == ""
    assert [d.dag_id for d in cov.dags] == ["live", "quiet"]        # paused DAG dropped
    assert [d.dag_id for d in cov.profilable] == ["live"]
    assert [d.dag_id for d in cov.unprofiled] == ["quiet"]
    live = cov.dags[0]
    assert live.spark_tasks == ["run_model"]
    assert live.opaque_tasks == [("write_signal", coverage.OPAQUE_OPERATORS["DbxDbtOperator"])]


def test_collect_local_reports_a_broken_bundle_instead_of_raising() -> None:
    """A coverage failure must never sink a sweep that already produced findings."""
    cov = coverage.collect_local("2026-08-21", dag_folder="/nonexistent/bundle")
    assert cov.error and not cov.dags
