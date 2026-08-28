"""What the fleet's BigQuery tasks cost, from INFORMATION_SCHEMA, per dag and task.

The Spark half reads event logs; the 39 non-Spark DAGs mostly spend in BigQuery, where the
job metadata is already attributed: BigQueryInsertJobOperator stamps `airflow-dag` and
`airflow-task` labels on every job it submits. Jobs from a plain python-client call inside a
task carry no labels and land in one `unattributed` bucket rather than being guessed at.

Reads go through JOBS_BY_USER, not JOBS_BY_PROJECT: the sweep runs as the same service
account that runs the fleet's tasks, so its own job history is visible with no new grant.
The query itself runs over the REST API with a gcloud access token, like the GCS uploads -
neither bq nor gsutil is authenticated inside the task pod.

Dollars are deliberately absent here: slot-hours are the measured unit, and the per-surface
rate lives with the billing module.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field

PROJECTS = os.environ.get("OPTIMIZER_BQ_PROJECTS", "dw-main-bronze")
REGION = os.environ.get("OPTIMIZER_BQ_REGION", "us-central1")
HEAVY_SLOT_H = float(os.environ.get("OPTIMIZER_BQ_HEAVY_SLOT_H", "50"))
_TIMEOUT = 120

PROFILE_SQL = """
SELECT
  COALESCE((SELECT value FROM UNNEST(labels) WHERE key = 'airflow-dag'), '') AS dag,
  COALESCE((SELECT value FROM UNNEST(labels) WHERE key = 'airflow-task'), '') AS task,
  COUNT(*) AS jobs,
  SUM(total_slot_ms) / 3600000 AS slot_h,
  SUM(total_bytes_billed) / POW(1024, 4) AS tib_billed
FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_USER
WHERE creation_time >= TIMESTAMP('{date} 00:00:00')
  AND creation_time < TIMESTAMP_ADD(TIMESTAMP('{date} 00:00:00'), INTERVAL 1 DAY)
GROUP BY 1, 2
ORDER BY slot_h DESC
"""


@dataclass
class TaskCost:
    """One (dag, task)'s BigQuery consumption for one calendar day, in one project."""

    project: str
    dag: str
    task: str
    jobs: int
    slot_h: float
    tib_billed: float


@dataclass
class Finding:
    """Shaped like a crawl finding, so the ledger records it the same way."""

    key: str
    impact: str
    title: str
    fix: str = ""


@dataclass
class Report:
    """Shaped like a crawl JobReport, so `ledger.record` consumes it unchanged."""

    source: str
    app_name: str
    exec_h: float
    findings: list = field(default_factory=list)
    error: str | None = None

    @property
    def n_high(self) -> int:
        """High-impact findings on this report."""
        return sum(1 for f in self.findings if f.impact == "high")


def _token() -> str:
    r = subprocess.run(["gcloud", "auth", "print-access-token"],
                       capture_output=True, text=True, timeout=_TIMEOUT)
    if r.returncode != 0:
        raise RuntimeError(f"no access token: {(r.stderr or '').strip()[-200:]}")
    return r.stdout.strip()


def query(project: str, sql: str) -> list[dict]:
    """Run one query in `project` and return its rows as dicts. Raises on any failure."""
    body = json.dumps({"query": sql, "useLegacySql": False,
                       "timeoutMs": 100_000, "maxResults": 10_000})
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries"
    r = subprocess.run(
        ["curl", "-sS", "--fail-with-body", "-X", "POST", url,
         "-H", f"Authorization: Bearer {_token()}",
         "-H", "Content-Type: application/json", "--data", body],
        capture_output=True, text=True, timeout=_TIMEOUT,
    )  # fmt: skip
    if r.returncode != 0:
        raise RuntimeError(f"bq query failed: {(r.stdout or r.stderr).strip()[-300:]}")
    res = json.loads(r.stdout)
    if not res.get("jobComplete", False):
        raise RuntimeError("bq query did not complete inside the timeout")
    names = [f["name"] for f in res.get("schema", {}).get("fields", [])]
    return [dict(zip(names, [c.get("v") for c in row.get("f", [])], strict=False))
            for row in res.get("rows", [])]


def profile(date: str, projects: str = "") -> list[TaskCost]:
    """Every (dag, task)'s consumption on `date`, across the configured billing projects."""
    out = []
    for project in [p.strip() for p in (projects or PROJECTS).split(",") if p.strip()]:
        for r in query(project, PROFILE_SQL.format(project=project, region=REGION, date=date)):
            out.append(TaskCost(
                project=project, dag=r.get("dag") or "", task=r.get("task") or "",
                jobs=int(r.get("jobs") or 0), slot_h=float(r.get("slot_h") or 0),
                tib_billed=float(r.get("tib_billed") or 0),
            ))
    return out


def reports(costs: list[TaskCost]) -> list[Report]:
    """One Report per dag, findings attached, ready for the ledger.

    The unattributed bucket is reported (its spend is real) but never gets a finding: a fix
    cannot be filed against a job no dag will admit to.
    """
    by_dag: dict[str, list[TaskCost]] = {}
    for c in costs:
        by_dag.setdefault(c.dag, []).append(c)
    out = []
    for dag, rows in by_dag.items():
        slot_h = round(sum(r.slot_h for r in rows), 1)
        rep = Report(source=f"bq:{dag or 'unattributed'}", app_name=dag or "unattributed",
                     exec_h=slot_h)
        if dag:
            for c in sorted(rows, key=lambda r: r.slot_h, reverse=True):
                if c.slot_h >= HEAVY_SLOT_H:
                    rep.findings.append(Finding(
                        key="bq_heavy_task", impact="high",
                        title=f"task {c.task or '(unlabeled)'} used {c.slot_h:,.0f} slot-hours "
                              f"in one day ({c.jobs} jobs, {c.tib_billed:,.1f} TiB billed)",
                        fix="Read the query's execution plan for shuffle-heavy stages, missing "
                            "partition filters, or repeated identical runs.",
                    ))
        out.append(rep)
    return out


def render(costs: list[TaskCost], date: str) -> str:
    """The BigQuery section of a sweep, heaviest dag first."""
    lines = [f"# BigQuery cost — {date}", "",
             "Slot-hours per dag and task, from the job history of the fleet's own service "
             "account. Jobs submitted without airflow labels appear as `unattributed`.", ""]
    if not costs:
        return "\n".join(lines + ["No BigQuery jobs found for this day.", ""])
    lines += ["| DAG | task | jobs | slot-hours | TiB billed |", "|---|---|---:|---:|---:|"]
    for c in sorted(costs, key=lambda r: r.slot_h, reverse=True):
        lines.append(f"| `{c.dag or 'unattributed'}` | `{c.task or '-'}` | {c.jobs} | "
                     f"{c.slot_h:,.1f} | {c.tib_billed:,.2f} |")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    import sys

    day = sys.argv[1] if len(sys.argv) > 1 else ""
    if not day:
        raise SystemExit("usage: python -m airflow_optimizer.bq_profile YYYY-MM-DD")
    rows = profile(day)
    print(render(rows, day))
