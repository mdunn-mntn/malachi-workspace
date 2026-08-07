"""BLUF/STAR root-cause report generator (deterministic, <=500 chars).

Turns a synthesized diagnosis (Airflow-task + Spark-engine layers) into a terse
report: an answer line (what failed + root cause + confidence), an
orchestration-only note when the Spark job actually succeeded, the likely cause,
a programmatic-fix-aware recommended action, and a console link. No LLM: for a
matched signature the report is fully deterministic; the orchestrator only adds
synthesis when the root signature is unknown (confidence "low").
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

DBX_HOST = "https://1262887251702944.4.gcp.databricks.com"
DATAPROC_CONSOLE = "https://console.cloud.google.com/dataproc/batches/us-central1"
GCP_PROJECT = "mntn-prj-prod-00"
GITHUB_AIRFLOW_TI = "https://github.com/SteelHouse/airflow-ti/blob/main"
_AIRFLOW_TI_LOCAL = Path.home() / "Developer" / "work" / "mntn" / "airflow-ti"
_FILE_LINE = re.compile(r'File "([^"]+\.py)", line (\d+)')
_KNOWN_FIX_MIN_SCORE = 0.5
_MAX = 500

_FIX_ACTION = {
    "yes": "Code fix likely (PR candidate).",
    "sometimes": "Code fix possible; verify first.",
    "no": "Not a code fix (compute/infra or upstream).",
}


def _confidence(diag: dict) -> str:
    return "high" if diag.get("root_signature") else "low"


def _short_cause(diag: dict) -> str:
    """Short root-cause tag: the bracketed error code (Spark failures) else the class."""
    root = diag.get("root_signature") or {}
    err = diag.get("root_error") or ""
    m = re.search(r"\[([A-Z0-9_]{4,}(?:\.[A-Z0-9_]+)*)\]", err)
    if m and not diag.get("orchestration_only"):
        return m.group(1)
    return root.get("sig_class") or "unclassified"


def _link(diag: dict) -> str | None:
    if diag.get("engine") == "databricks" and diag.get("dbx_run_id"):
        jid, rid = diag.get("job_id"), diag["dbx_run_id"]
        return f"{DBX_HOST}/jobs/{jid}/runs/{rid}" if jid else f"{DBX_HOST} run {rid}"
    if diag.get("engine") == "dataproc" and diag.get("batch_id"):
        return f"{DATAPROC_CONSOLE}/{diag['batch_id']}?project={GCP_PROJECT}"
    return None


def build_report(diag: dict) -> str:
    """Assemble the BLUF/STAR report (<=500 chars) from a diagnosis dict."""
    ident = diag.get("identity", {})
    who = "/".join(filter(None, [ident.get("dag_id"), ident.get("task_id")])) or "unknown task"
    root = diag.get("root_signature") or {}

    lines = [f"RCA [{_confidence(diag)}]: {who} - {_short_cause(diag)}"]
    if diag.get("orchestration_only"):
        lines.append(f"Downstream {diag.get('engine')} job SUCCEEDED, orchestration-only failure.")
    cause_idx = None
    likely = (root.get("likely_cause") or "").strip()
    if likely:
        lines.append(likely if likely.endswith(".") else likely + ".")
        cause_idx = len(lines) - 1
    fix = _FIX_ACTION.get(root.get("programmatic_fix"))
    if fix:
        lines.append(fix)
    link = _link(diag)
    if link:
        lines.append(link)
    if not root:
        notes = "; ".join(
            (diag.get("notes") or []) + ((diag.get("spark") or {}).get("notes") or [])
        )
        if notes:
            lines.append(notes)

    report = "\n".join(lines)
    if len(report) > _MAX and cause_idx is not None:
        # over budget: shrink the cause so the fix line and link survive whole
        budget = _MAX - (len(report) - len(lines[cause_idx]))
        lines[cause_idx] = lines[cause_idx][: max(budget, 16) - 1].rstrip() + "…"
        report = "\n".join(lines)
    if len(report) > _MAX and link:
        # still over: drop the link whole rather than emit a corrupted URL
        lines.remove(link)
        report = "\n".join(lines)
    if len(report) > _MAX:
        report = report[: _MAX - 1].rstrip() + "…"
    return report


def _repo_paths() -> dict[str, str]:
    """basename -> repo-relative path from the local airflow-ti checkout (empty if absent)."""
    try:
        out = subprocess.run(
            ["git", "-C", str(_AIRFLOW_TI_LOCAL), "ls-files"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return {}
    if out.returncode != 0:
        return {}
    paths: dict[str, str] = {}
    for p in out.stdout.splitlines():
        paths.setdefault(p.rsplit("/", 1)[-1], p)
    return paths


def code_links(diag: dict, repo_paths: dict[str, str] | None = None) -> list[str]:
    """GitHub links for the repo files named in the failure's tracebacks.

    Framework frames (site-packages, /usr/local) are skipped; a file only links
    when its basename exists in the airflow-ti tree, so an unrelated script never
    produces a wrong link.
    """
    spark = diag.get("spark") or {}
    text = " ".join(
        filter(None, [diag.get("root_error"), spark.get("error_text"), spark.get("state_message")])
    )
    if repo_paths is None:
        repo_paths = _repo_paths()
    links, seen = [], set()
    for full, line in _FILE_LINE.findall(text):
        if "site-packages" in full or full.startswith("/usr/"):
            continue
        name = full.rsplit("/", 1)[-1]
        if name in seen:
            continue
        seen.add(name)
        repo = repo_paths.get(name)
        if repo:
            links.append(f"{GITHUB_AIRFLOW_TI}/{repo}#L{line}")
    return links[:3]


def build_troubleshooting(
    diag: dict, matches: list[dict] | None = None, repo_paths: dict[str, str] | None = None
) -> str:
    """Paste-ready write-up: BLUF, problem, solution with any known fix PR, code links."""
    matches = matches or []
    root = diag.get("root_signature") or {}
    lines = [build_report(diag), "", "Problem"]
    problem = (root.get("likely_cause") or "").strip() or (
        (diag.get("root_error") or "").strip()[:400] or "Unclassified; see the log tail."
    )
    lines.append(problem)
    if root.get("matched_on"):
        lines.append(f'Matched on: "{root["matched_on"]}"')

    lines += ["", "Solution"]
    known = next(
        (m for m in matches if m.get("fix_pr") and m.get("score", 0) >= _KNOWN_FIX_MIN_SCORE), None
    )
    if known:
        lines.append(f"Known fix: {known['fix_pr']} ({known['inc']}, runbook §3)")
    fix = _FIX_ACTION.get(root.get("programmatic_fix"))
    if fix:
        lines.append(fix)
    if not known and not fix:
        lines.append("No known fix on record; diagnose from the code links and log tail.")

    links = code_links(diag, repo_paths)
    fix_files = (known or {}).get("fix_files") or []
    if links or fix_files:
        lines += ["", "Code"]
        lines += [f"- {link}" for link in links]
        for f in fix_files:
            url = f"{GITHUB_AIRFLOW_TI}/{f}"
            if not any(url in link for link in links):
                lines.append(f"- {url} (fixed by {known['inc']})")

    if matches:
        lines += ["", "Similar: " + ", ".join(f"{m['inc']}({m['score']})" for m in matches)]
    return "\n".join(lines)


def report_from_log(path: str) -> str:
    """Full deterministic chain: Airflow log file -> parse -> diagnose -> report."""
    from .parse import diagnose, parse_log_file

    return build_report(diagnose(parse_log_file(path)))


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("usage: python -m airflow_debugger.report <airflow_log_file>")
        raise SystemExit(2)
    print(report_from_log(sys.argv[1]))
