"""Live Databricks EXPLAIN COST acquisition, validated 2026-08-20.

The specced route (jobs get-run-output) does NOT work and never will as written: on a
SUCCEEDED prod run it returns {"metadata": ..., "notebook_output": {}} - no plan, no stats,
no logs, and new_cluster.cluster_log_conf is None so no event log is persisted either.

The route that DOES work is running EXPLAIN COST directly against a SQL warehouse through
the Statement Execution API. That needs no dbt change and no cluster config: the plan text
it returns is exactly what airflow_optimizer.optimizations.analyze_plan parses.

    python3 artifacts/audi_1194_databricks_explain_cost.py "SELECT ..." [warehouse_id]
"""

from __future__ import annotations

import json
import subprocess
import sys
import time

PROFILE = "malachi@mountain.com"
DEFAULT_WAREHOUSE = "14b311ac86ee2ca2"  # Serverless Starter Warehouse (sql_warehouse_2xs is stopped)


def _api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, path, "-p", PROFILE]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    return json.loads(r.stdout) if r.stdout.strip() else {"error": r.stderr[:400]}


def explain_cost(query: str, warehouse_id: str = DEFAULT_WAREHOUSE, poll_s: int = 3) -> str:
    """Return the EXPLAIN COST plan text for `query`, or '' if the statement failed."""
    res = _api("post", "/api/2.0/sql/statements", {
        "warehouse_id": warehouse_id,
        "statement": f"EXPLAIN COST {query}",
        "wait_timeout": "50s",
    })
    sid = res.get("statement_id")
    while res.get("status", {}).get("state") in ("PENDING", "RUNNING") and sid:
        time.sleep(poll_s)
        res = _api("get", f"/api/2.0/sql/statements/{sid}")
    if res.get("status", {}).get("state") != "SUCCEEDED":
        return ""
    rows = (res.get("result") or {}).get("data_array") or []
    return "\n".join(r[0] for r in rows)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        raise SystemExit(2)
    from airflow_optimizer.optimizations import analyze_plan

    plan = explain_cost(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else DEFAULT_WAREHOUSE)
    if not plan:
        print("EXPLAIN COST failed")
        raise SystemExit(1)
    print(f"plan: {len(plan)} chars")
    for f in analyze_plan(plan):
        print(f"[{f.impact}] {f.key}: {f.title}\n    fix: {f.fix}")
