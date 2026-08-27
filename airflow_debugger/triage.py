"""File one AUDI Bug per new failure in a day's RCA and log it in the on-call playbook.

Usage: python3 -m airflow_debugger.triage rca_<ds>.json [--dry-run]

Laptop-only, like synth.py: the Jira and Confluence credential never ships in the prod bundle.
Ticket shape is Bryce's (2026-08-27): type Bug, parent AUDI-1054, Environment Prod, Origin
Automated Testing / Monitors, priority from the failure class. Dedup is against every ticket
already labeled debugger_triage, matched on the dag/task pair in the summary, so a recurrence
comments on the existing Bug instead of filing a twin.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.parse

JIRA = "https://mntn.atlassian.net"
WIKI = f"{JIRA}/wiki"
PLAYBOOK_ID = "2908061697"
EPIC = "AUDI-1054"
LABEL = "debugger_triage"
_KNOWN_HEADING = "Known issues and past fixes"


def _auth() -> tuple[str, str]:
    tok = (os.environ.get("JIRA_API_TOKEN") or "").strip()
    if not tok:
        raise SystemExit("JIRA_API_TOKEN is unset")
    return "malachi@mountain.com", tok


def _call(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    user, tok = _auth()
    cmd = ["curl", "-s", "-w", "\n%{http_code}", "-u", f"{user}:{tok}", "-X", method,
           "-H", "Content-Type: application/json", url]  # fmt: skip
    if body is not None:
        cmd += ["-d", json.dumps(body)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    raw, _, code = r.stdout.rpartition("\n")
    try:
        return int(code or 0), json.loads(raw or "{}")
    except ValueError:
        return int(code or 0), {}


def priority(sig_class: str | None) -> str:
    if sig_class and sig_class.startswith(("infra", "upstream")):
        return "P1 - Critical"
    if not sig_class:
        return "P3 - Minor"
    return "P2 - Normal"


def existing_pairs() -> dict[str, str]:
    """dag/task pair -> ticket key, for every ticket ever filed by this pipeline."""
    q = urllib.parse.quote(f'project = AUDI AND labels = "{LABEL}"')
    pairs, cursor = {}, ""
    while True:
        url = f"{JIRA}/rest/api/3/search/jql?jql={q}&fields=summary&maxResults=100{cursor}"
        code, obj = _call("GET", url)
        if code != 200:
            raise SystemExit(f"jira search failed (HTTP {code}): {str(obj)[:200]}")
        for it in obj.get("issues", []):
            pair = it["fields"]["summary"].removeprefix("[TRIAGE]").split(" - ")[0].strip()
            pairs.setdefault(pair, it["key"])
        nxt = obj.get("nextPageToken")
        if not nxt or not obj.get("issues"):
            break
        cursor = f"&nextPageToken={urllib.parse.quote(nxt)}"
    return pairs


def file_bug(row: dict, dry: bool) -> str:
    pair = f"{row.get('dag_id')}/{row.get('task_id')}"
    klass = row.get("sig_class") or "unclassified"
    desc = (
        f"Filed automatically by the airflow debugger (AUDI-1191).\n\n"
        f"Failure class: {klass}\nSignature: {row.get('signature') or 'none matched'}\n"
        f"Run: {row.get('run_id')} try {row.get('try_number')}\n\n"
        f"{{noformat}}{(row.get('report') or '')[:1500]}{{noformat}}\n\n"
        f"Diagnosis reports: gs://mntn-data-archive-prod/debugger/"
    )
    body = {"fields": {
        "project": {"key": "AUDI"},
        "issuetype": {"name": "Bug"},
        "parent": {"key": EPIC},
        "summary": f"[TRIAGE] {pair} - {klass}"[:255],
        "description": desc,
        "labels": [LABEL],
        "priority": {"name": priority(row.get("sig_class"))},
        "customfield_16001": {"value": "Prod"},
        "customfield_16028": [{"value": "Automated Testing / Monitors"}],
    }}  # fmt: skip
    if dry:
        return f"DRY:{pair}"
    code, obj = _call("POST", f"{JIRA}/rest/api/2/issue", body)
    if code != 201:
        raise SystemExit(f"create failed for {pair} (HTTP {code}): {str(obj)[:300]}")
    key = obj["key"]
    _call("POST", f"{JIRA}/rest/api/2/issue/{key}/remotelink", {
        "object": {"url": f"{WIKI}/spaces/TAR/pages/{PLAYBOOK_ID}",
                   "title": "TI On Call Playbook - known issues"}
    })  # fmt: skip
    return key


def playbook_add(rows: list[tuple[str, str, str]], dry: bool) -> None:
    """Append (pair, class, ticket key) rows to the playbook's known-issues table."""
    if not rows or dry:
        return
    code, page = _call("GET", f"{WIKI}/rest/api/content/{PLAYBOOK_ID}?expand=body.storage,version")
    if code != 200:
        print(f"playbook read failed (HTTP {code}); rows not logged", file=sys.stderr)
        return
    html = page["body"]["storage"]["value"]
    at = html.find(_KNOWN_HEADING)
    end = html.find("</tbody>", at)
    if at < 0 or end < 0:
        print("known-issues table not found in playbook; rows not logged", file=sys.stderr)
        return
    add = "".join(
        f"<tr><td>{p}</td><td>{k}</td><td>Open</td>"
        f'<td><a href="{JIRA}/browse/{t}">{t}</a></td></tr>'
        for p, k, t in rows
    )
    body = {"version": {"number": page["version"]["number"] + 1,
                        "message": "debugger triage: new failure rows"},
            "title": page["title"], "type": "page",
            "body": {"storage": {"value": html[:end] + add + html[end:],
                                 "representation": "storage"}}}  # fmt: skip
    code, _ = _call("PUT", f"{WIKI}/rest/api/content/{PLAYBOOK_ID}", body)
    if code != 200:
        print(f"playbook update failed (HTTP {code})", file=sys.stderr)


def run(rca_path: str, dry: bool = False) -> dict:
    d = json.load(open(rca_path))
    seen = existing_pairs()
    filed, known = [], []
    for row in d.get("results", []):
        pair = f"{row.get('dag_id')}/{row.get('task_id')}"
        if pair in seen:
            known.append((pair, seen[pair]))
            continue
        key = file_bug(row, dry)
        seen[pair] = key
        filed.append((pair, row.get("sig_class") or "unclassified", key))
    playbook_add(filed, dry)
    return {"ds": d.get("ds"), "filed": filed, "already_ticketed": known}


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    out = run(args[0], dry="--dry-run" in sys.argv)
    print(json.dumps(out, indent=2))
