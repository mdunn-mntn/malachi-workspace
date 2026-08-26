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


def test_costs_survive_a_row_with_no_hours_column(monkeypatch: Any) -> None:
    """`job_costs` selects four columns and `query_costs` five; one parser reads both."""
    monkeypatch.setattr(databricks, "query", lambda sql, wh="": [
        ["Generate Graph & Metrics", 1, 10497.7, 1574.66],
        [None, 0, None, None],
    ])
    jobs = databricks.job_costs(7, 2, "wh")
    assert (jobs[0].name, jobs[0].runs, jobs[0].usd, jobs[0].hours) == (
        "Generate Graph & Metrics", 1, 1574.66, 0.0)
    assert (jobs[1].name, jobs[1].dbu) == ("", 0.0)


def test_query_cost_sql_apportions_by_the_day_the_statement_ran(monkeypatch: Any) -> None:
    """Dividing a warehouse's daily dollars by a whole-window denominator understates every day."""
    seen = {}
    monkeypatch.setattr(databricks, "query", lambda sql, wh="": seen.setdefault("sql", sql) and [])
    databricks.query_costs(7, 5, "wh")
    assert "GROUP BY 1, 2" in seen["sql"]
    assert "ON q.wh = tot.wh AND q.d = tot.d" in seen["sql"]


def test_job_cost_sql_dedupes_the_run_timeline_before_joining(monkeypatch: Any) -> None:
    """job_run_timeline holds a row per period, so a raw join multiplies every usage record."""
    seen = {}
    monkeypatch.setattr(databricks, "query", lambda sql, wh="": seen.setdefault("sql", sql) and [])
    databricks.job_costs(7, 5, "wh")
    assert "GROUP BY run_id" in seen["sql"]
    assert databricks._RUN_UUID.pattern in seen["sql"]
