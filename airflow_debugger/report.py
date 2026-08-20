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
_FRAMEWORK_MARKERS = ("site-packages", "dist-packages", "/pyspark/", "/py4j/", "/lib/python")
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
    if not root and diag.get("no_error_text"):
        return "no error text in log"
    return root.get("sig_class") or "unclassified"


def _link(diag: dict) -> str | None:
    if diag.get("engine") == "databricks" and diag.get("dbx_run_id"):
        jid, rid = diag.get("job_id"), diag["dbx_run_id"]
        return f"{DBX_HOST}/jobs/{jid}/runs/{rid}" if jid else f"{DBX_HOST} run {rid}"
    if diag.get("engine") == "dataproc" and diag.get("batch_id"):
        return f"{DATAPROC_CONSOLE}/{diag['batch_id']}?project={GCP_PROJECT}"
    return None


def _no_output_note(ti_state: str | None) -> str:
    """An empty log means different things for a failed task and an upstream_failed one."""
    if ti_state == "upstream_failed":
        return "The task never ran; diagnose the upstream task that failed."
    if ti_state == "failed":
        # INC-021: the worker died rather than the task raising, so Airflow has no exception.
        return (
            "Empty log on a failed task: the worker died before the task could raise. "
            "Check whether it already retried before touching anything."
        )
    return "The task emitted no failure output; diagnose the upstream task that failed."


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
    if not root and diag.get("no_error_text"):
        lines.append(_no_output_note(diag.get("ti_state")))
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


def _repo_paths() -> dict[str, list[str]]:
    """basename -> all repo-relative paths from the local airflow-ti checkout (empty if absent)."""
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
    paths: dict[str, list[str]] = {}
    for p in out.stdout.splitlines():
        paths.setdefault(p.rsplit("/", 1)[-1], []).append(p)
    return paths


def _resolve_frame(full: str, candidates: list[str]) -> str | None:
    """Pick the repo path a traceback frame refers to; None when it can't be sure.

    A single candidate wins. On a basename collision, a Spark driver frame
    (Dataproc /var/dataproc tmp dir, Databricks) means the deployed spark/
    script; anything still ambiguous is skipped rather than guessed.
    """
    if len(candidates) == 1:
        return candidates[0]
    if "/var/dataproc/" in full or "/databricks/" in full:
        spark_hits = [c for c in candidates if c.startswith("spark/")]
        if len(spark_hits) == 1:
            return spark_hits[0]
    return None


def code_links(diag: dict, repo_paths: dict[str, list[str]] | None = None) -> list[tuple[str, str]]:
    """(url, repo_path) pairs for the repo files named in the failure's tracebacks.

    Framework frames are skipped, a file only links when its basename exists in
    the airflow-ti tree, and an ambiguous basename collision is skipped rather
    than guessed, so an unrelated script never produces a wrong link. The
    deepest frame per file wins (nearest the raise).
    """
    spark = diag.get("spark") or {}
    text = " ".join(
        filter(None, [diag.get("root_error"), spark.get("error_text"), spark.get("state_message")])
    )
    if repo_paths is None:
        repo_paths = _repo_paths()
    resolved: dict[str, tuple[str, str]] = {}  # basename -> (repo_path, line); last frame wins
    for full, line in _FILE_LINE.findall(text):
        if any(m in full for m in _FRAMEWORK_MARKERS) or full.startswith(("/usr/", "/opt/")):
            continue
        name = full.rsplit("/", 1)[-1]
        if name == "__init__.py":
            continue
        repo = _resolve_frame(full, repo_paths.get(name, []))
        if repo:
            resolved[name] = (repo, line)
    return [
        (f"{GITHUB_AIRFLOW_TI}/{repo}#L{line}", repo) for repo, line in list(resolved.values())[:3]
    ]


def _one_line(text: str, cap: int = 400) -> str:
    """Collapse log-sourced text to one line so it can't forge package lines."""
    return " ".join(text.split())[:cap]


def _known_fix(diag: dict, matches: list[dict]) -> dict | None:
    """The top match's fix PR, claimed only when identity agrees with this failure.

    Only the highest-scoring match is considered, and when the diagnosis knows
    its dag/task the match must share one; a token-overlap score alone must not
    attach an unrelated incident's PR (a two-word query can score 1.0).
    """
    if not matches:
        return None
    top = matches[0]
    if not top.get("fix_pr") or top.get("score", 0) < _KNOWN_FIX_MIN_SCORE:
        return None
    ident = diag.get("identity") or {}
    dag, task = ident.get("dag_id"), ident.get("task_id")
    identities_known = (dag or task) and (top.get("dag") or top.get("task"))
    if identities_known and top.get("dag") != dag and top.get("task") != task:
        return None
    return top


def build_troubleshooting(
    diag: dict, matches: list[dict] | None = None, repo_paths: dict[str, list[str]] | None = None
) -> str:
    """Paste-ready write-up: BLUF, problem, solution with any known fix PR, code links."""
    matches = matches or []
    root = diag.get("root_signature") or {}
    lines = [build_report(diag), "", "Problem"]
    problem = _one_line(
        (root.get("likely_cause") or "").strip()
        or (diag.get("root_error") or "").strip()
        or "Unclassified; see the log tail."
    )
    lines.append(problem)
    if root.get("matched_on"):
        lines.append(f'Matched on: "{_one_line(root["matched_on"], 120)}"')

    lines += ["", "Solution"]
    known = _known_fix(diag, matches)
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
        lines += [f"- {url}" for url, _ in links]
        linked_paths = {repo for _, repo in links}
        for f in fix_files:
            if f not in linked_paths:
                lines.append(f"- {GITHUB_AIRFLOW_TI}/{f} (fixed by {known['inc']})")

    perf = diag.get("perf_profile") or {}
    if perf.get("findings"):
        lines += ["", "Perf profile (event log)"]
        lines += [f"- [{f['impact']}] {f['title']} - {f['fix']}" for f in perf["findings"]]
    elif perf.get("error"):
        lines += ["", f"Perf profile unavailable: {perf['error']}"]

    if matches:
        lines += [
            "",
            "Similar: " + ", ".join(f"{m.get('inc')}({m.get('score')})" for m in matches),
        ]
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
