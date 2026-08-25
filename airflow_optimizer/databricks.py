"""What dbt actually ran on Databricks, and what a query's plan costs.

The optimizer reads Spark event logs, and Databricks writes none for the ephemeral runs dbt
submits: `jobs get-run-output` carries no plan on success or failure, and the job clusters set
no `cluster_log_conf`. Two separate surfaces close that gap.

`submissions` enumerates the runs from `system.lakeflow.job_run_timeline`. `jobs list` cannot:
it returns named Jobs, and a dbt `runs/submit` is not one.

`explain_cost` returns a plan for a query text, which `optimizations.analyze_plan` already
parses. Note the asymmetry - enumeration tells you a model ran and how long it took, but not
the SQL it ran, so pairing the two still needs the model's source.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
WAREHOUSE = os.environ.get("DATABRICKS_WAREHOUSE", "")

# Every SUBMIT_RUN name ends in the submission's own uuid, so grouping raw gives one row a run.
_RUN_UUID = re.compile(r"-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


@dataclass
class Submission:
    """One ephemeral dbt run."""

    model: str
    run_id: str
    result_state: str
    duration_s: float
    started: str

    @property
    def failed(self) -> bool:
        """True when the run did not succeed."""
        return self.result_state not in ("SUCCEEDED", "")


def model_name(run_name: str) -> str:
    """The dbt identifier, with the per-run uuid stripped."""
    return _RUN_UUID.sub("", run_name or "")


def _api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, path, "-p", PROFILE]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if not r.stdout.strip():
        raise RuntimeError(f"databricks {method} {path}: {(r.stderr or '').strip()[:300]}")
    return json.loads(r.stdout)


def query(sql: str, warehouse: str = "", poll_s: int = 3, tries: int = 20) -> list[list]:
    """Run one statement and return its rows. Raises on a failed statement."""
    target = warehouse or WAREHOUSE
    if not target:
        raise RuntimeError("no warehouse: pass one or set DATABRICKS_WAREHOUSE")
    res = _api("post", "/api/2.0/sql/statements",
               {"warehouse_id": target, "statement": sql, "wait_timeout": "50s"})
    sid = res.get("statement_id")
    for _ in range(tries):
        state = res.get("status", {}).get("state")
        if state not in ("PENDING", "RUNNING"):
            break
        time.sleep(poll_s)
        res = _api("get", f"/api/2.0/sql/statements/{sid}")
    status = res.get("status", {})
    if status.get("state") != "SUCCEEDED":
        raise RuntimeError((status.get("error") or {}).get("message", "statement did not finish")[:300])
    return (res.get("result") or {}).get("data_array", []) or []


SUBMISSIONS_SQL = """
SELECT run_name, run_id, result_state,
       unix_timestamp(period_end_time) - unix_timestamp(period_start_time) AS duration_s,
       CAST(period_start_time AS STRING) AS started
FROM system.lakeflow.job_run_timeline
WHERE run_type = 'SUBMIT_RUN'
  AND period_start_time > current_date() - INTERVAL {days} DAYS
ORDER BY period_start_time DESC
LIMIT {limit}
"""


def submissions(days: int = 7, limit: int = 5000, warehouse: str = "") -> list[Submission]:
    """Every ephemeral dbt run in the window, newest first."""
    rows = query(SUBMISSIONS_SQL.format(days=int(days), limit=int(limit)), warehouse)
    out = []
    for r in rows:
        name, run_id, state, dur, started = (list(r) + [""] * 5)[:5]
        out.append(Submission(model=model_name(name), run_id=str(run_id),
                              result_state=state or "", duration_s=float(dur or 0),
                              started=started or ""))
    return out


def by_model(subs: list[Submission]) -> list[tuple[str, int, float, int]]:
    """(model, runs, total seconds, failures), heaviest first."""
    agg: dict[str, list] = {}
    for s in subs:
        row = agg.setdefault(s.model, [0, 0.0, 0])
        row[0] += 1
        row[1] += s.duration_s
        row[2] += 1 if s.failed else 0
    ranked = [(m, n, total, bad) for m, (n, total, bad) in agg.items()]
    ranked.sort(key=lambda t: t[2], reverse=True)
    return ranked


def explain_cost(sql: str, warehouse: str = "") -> str:
    """The EXPLAIN COST plan text for `sql`, or '' when the statement failed."""
    try:
        rows = query(f"EXPLAIN COST {sql}", warehouse)
    except RuntimeError:
        return ""
    return "\n".join(str(r[0]) for r in rows if r)


if __name__ == "__main__":
    import sys

    days = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    subs = submissions(days)
    print(f"{len(subs)} dbt submissions over {days} days\n")
    print(f"{'model':<60}{'runs':>6}{'hours':>9}{'fail':>6}")
    for model, n, total, bad in by_model(subs)[:25]:
        print(f"{model:<60}{n:>6}{total / 3600:>9.1f}{bad:>6}")
