#!/usr/bin/env python3
"""Read-only pull of every live DAG (tags, file, paused) and every Airflow Variable from the airflow-ti deployment.

Usage (from the workspace root, astro SSO context logged in):
  python3 tickets/.../artifacts/audi_1280_live_dags_pull.py
Writes outputs/audi_1280_live_dags.json and outputs/audi_1280_live_variables.json next to this script's ticket folder.
"""

import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

WORKSPACE = Path("/Users/malachi/Developer/work/mntn/workspace")
DEPLOYMENT_ID = "cmd6bd10c0gl901rfuokgryiq"
TICKET = Path(__file__).resolve().parent.parent
KEEP = ["dag_id", "relative_fileloc", "fileloc", "tags", "is_paused", "is_stale", "has_import_errors", "owners", "bundle_name"]

sys.path.insert(0, str(WORKSPACE / ".claude" / "scripts"))
import airflow_api as api  # noqa: E402


def resolve_base() -> str:
    raw = subprocess.check_output(
        ["astro", "deployment", "inspect", DEPLOYMENT_ID, "--key", "metadata.airflow_api_url"],
        text=True,
        stderr=subprocess.DEVNULL,
    ).strip().strip('"')
    if not raw.endswith("/api/v2"):
        raw = raw.rstrip("/") + "/api/v2"
    return raw if raw.startswith("http") else f"https://{raw}"


def pull_dags(base: str, token: str) -> tuple[int, list[dict]]:
    rows, offset, total = [], 0, None
    while total is None or offset < total:
        status, page = api._get_json(base, token, "/dags", {"limit": 100, "offset": offset})
        api._die_on_status(status, page, "GET /dags")
        total = page["total_entries"]
        for d in page["dags"]:
            row = {k: d.get(k) for k in KEEP}
            row["tags"] = sorted(t["name"] if isinstance(t, dict) else t for t in (d.get("tags") or []))
            rows.append(row)
        offset += 100
    return total, rows


def main() -> None:
    base, token = resolve_base(), api.resolve_bearer()
    total, rows = pull_dags(base, token)
    stamp = datetime.now(UTC).isoformat()
    (TICKET / "outputs" / "audi_1280_live_dags.json").write_text(
        json.dumps({"fetched_at_utc": stamp, "total_entries": total, "dags": rows}, indent=1)
    )
    status, variables = api._get_json(base, token, "/variables", {"limit": 200})
    api._die_on_status(status, variables, "GET /variables")
    (TICKET / "outputs" / "audi_1280_live_variables.json").write_text(
        json.dumps({"fetched_at_utc": stamp, **variables}, indent=1)
    )
    print(f"dags total={total} fetched={len(rows)} variables total={variables.get('total_entries')} keys={[v['key'] for v in variables.get('variables', [])]}")
    print("sample", json.dumps(rows[0]))


if __name__ == "__main__":
    main()
