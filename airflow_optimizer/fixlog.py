"""Sync the fix log (ticket + PR + what it did + measured saving) into the on-call playbook.

Usage: python3 -m airflow_optimizer.fixlog [--dry-run]

Laptop-only: reads the prod ledger from GCS, rewrites the playbook's "Optimizer fix log"
section between its markers, and leaves everything else on the page untouched. Idempotent,
so the noon job can run it daily.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.parse

WIKI = "https://mntn.atlassian.net/wiki"
PLAYBOOK_ID = "2908061697"
LEDGER = "gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl"
TICKET = "AUDI-1241"
_START = "<!-- optimizer-fixlog-start -->"
_END = "<!-- optimizer-fixlog-end -->"


def _auth() -> str:
    tok = (os.environ.get("JIRA_API_TOKEN") or "").strip()
    if not tok:
        raise SystemExit("JIRA_API_TOKEN is unset")
    return f"malachi@mountain.com:{tok}"


def _ledger_rows() -> list[dict]:
    r = subprocess.run(["gsutil", "cat", LEDGER], capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise SystemExit(f"ledger read failed: {r.stderr[-200:]}")
    return [json.loads(line) for line in r.stdout.splitlines() if line.strip()]


def applied_fixes(rows: list[dict]) -> list[dict]:
    """One entry per (dag, finding) with a PR, newest first."""
    seen, out = set(), []
    for r in rows:
        if not r.get("fix_pr"):
            continue
        k = (r["dag_id"], r["key"])
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return sorted(out, key=lambda r: r.get("applied_date") or "", reverse=True)


def render(fixes: list[dict]) -> str:
    head = (
        f"{_START}<h2>Optimizer fix log</h2>"
        f'<p>Every merged optimization: what it was, the PR that proves it line by line, and '
        f'when it landed. Savings are measured per DAG in the '
        f'<a href="https://app.mode.com/mntn/reports/e81786de8403">Mode dashboard</a>; backlog: '
        f'<a href="https://mntn.atlassian.net/browse/{TICKET}">{TICKET}</a>.</p>'
        "<table><tbody><tr><th>Applied</th><th>DAG</th><th>What the fix did</th><th>PR</th></tr>"
    )
    rows = "".join(
        f"<tr><td>{f.get('applied_date')}</td><td>{f['dag_id']}</td>"
        f"<td>{f['title']}</td>"
        f'<td><a href="{f["fix_pr"]}">#{f["fix_pr"].rstrip("/").split("/")[-1]}</a></td></tr>'
        for f in fixes
    )
    return head + rows + f"</tbody></table>{_END}"


def sync(dry: bool = False) -> str:
    fixes = applied_fixes(_ledger_rows())
    section = render(fixes)
    url = f"{WIKI}/rest/api/content/{PLAYBOOK_ID}"
    r = subprocess.run(
        ["curl", "-s", "-u", _auth(), f"{url}?expand=body.storage,version"],
        capture_output=True, text=True, timeout=60,
    )  # fmt: skip
    page = json.loads(r.stdout)
    html = page["body"]["storage"]["value"]
    if _START in html:
        pre, _, rest = html.partition(_START)
        _, _, post = rest.partition(_END)
        html = pre + section + post
    else:
        at = html.find("<h2>Known issues and past fixes</h2>")
        html = html[:at] + section + html[at:] if at >= 0 else html + section
    if dry:
        return f"DRY: {len(fixes)} fixes"
    body = {"version": {"number": page["version"]["number"] + 1, "message": "optimizer fix log"},
            "title": page["title"], "type": "page",
            "body": {"storage": {"value": html, "representation": "storage"}}}  # fmt: skip
    r = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "-u", _auth(), "-X", "PUT",
         "-H", "Content-Type: application/json", "-d", json.dumps(body), url],
        capture_output=True, text=True, timeout=60,
    )  # fmt: skip
    return f"{len(fixes)} fixes synced (HTTP {r.stdout})"


if __name__ == "__main__":
    print(sync(dry="--dry-run" in sys.argv))
