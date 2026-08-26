"""The Databricks surfaces: naming a dbt run, and what the run table aggregates to."""

from __future__ import annotations

from typing import Any

from airflow_optimizer import databricks


def test_model_name_strips_only_the_per_run_uuid() -> None:
    """Every SUBMIT_RUN name ends in its own uuid, so raw names give one group per run."""
    uuid = "-3f2a1b4c-5d6e-7f80-91a2-b3c4d5e6f708"
    assert databricks.model_name(f"dbt_aug_log_ip_hourly{uuid}") == "dbt_aug_log_ip_hourly"
    assert databricks.model_name("dbt_aug_log_ip_hourly") == "dbt_aug_log_ip_hourly"
    assert databricks.model_name("") == ""


def test_a_run_that_has_not_finished_is_not_counted_as_failed() -> None:
    """`result_state` is empty until the run terminates; reading that as a failure invents one."""
    assert not databricks.Submission("m", "1", "SUCCEEDED", 1.0, "t").failed
    assert not databricks.Submission("m", "1", "", 1.0, "t").failed
    assert databricks.Submission("m", "1", "FAILED", 1.0, "t").failed


def test_by_model_ranks_on_total_time_and_counts_the_failures() -> None:
    """The model burning the most time is the one worth reading first."""
    subs = [
        databricks.Submission("light", "1", "SUCCEEDED", 10.0, "t"),
        databricks.Submission("heavy", "2", "FAILED", 60.0, "t"),
        databricks.Submission("heavy", "3", "SUCCEEDED", 30.0, "t"),
    ]
    assert databricks.by_model(subs) == [("heavy", 2, 90.0, 1), ("light", 1, 10.0, 0)]


def test_explain_cost_rejects_a_planner_error_reported_as_a_plan(monkeypatch: Any) -> None:
    """EXPLAIN COST succeeds and returns the planner's error as its result text.

    That text parses: an unresolved plan carries no statistics, so `missing_statistics` fires
    and a table that does not exist is reported as one lacking stats.
    """
    err = ("Error occurred during query planning: \n[TABLE_OR_VIEW_NOT_FOUND] The table or view "
           "`prod`.`ml`.`gone` cannot be found.\n'Aggregate [count(1) AS failures#1L]\n"
           "+- 'Project [unresolvedalias('max('load_ts))]")
    monkeypatch.setattr(databricks, "query", lambda *a, **k: [[err]])
    assert databricks.explain_cost("select 1") == ""

    plan = "== Optimized Logical Plan ==\nScan parquet prod.ml.t, Statistics(sizeInBytes=1.0 GiB)"
    monkeypatch.setattr(databricks, "query", lambda *a, **k: [[plan]])
    assert databricks.explain_cost("select 1") == plan
