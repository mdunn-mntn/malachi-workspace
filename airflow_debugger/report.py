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

from .masks import detect as detect_mask
from .masks import note as mask_note

DBX_HOST = "https://1262887251702944.4.gcp.databricks.com"
DATAPROC_CONSOLE = "https://console.cloud.google.com/dataproc/batches/us-central1"
VERTEX_CONSOLE = "https://console.cloud.google.com/vertex-ai/locations/{loc}/pipelines/runs"
AIRFLOW_UI = "https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/d6bdvmnl/dags"
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


def fix_line(root: dict) -> str | None:
    """What to change. The signature's own remedy when it has one, else the category."""
    return (root.get("remedy") or "").strip() or _FIX_ACTION.get(root.get("programmatic_fix"))


def _confidence(diag: dict) -> str:
    return "high" if diag.get("root_signature") else "low"


def _short_cause(diag: dict) -> str:
    """Short root-cause tag: the bracketed error code (Spark failures) else the class."""
    root = diag.get("root_signature") or {}
    err = diag.get("root_error") or ""
    m = re.search(r"\[([A-Z0-9_]{4,}(?:\.[A-Z0-9_]+)*)\]", err)
    if m and not diag.get("orchestration_only"):
        return m.group(1)
    if not root:
        walked = ((diag.get("upstream_walk") or {}).get("root") or {}).get("signature") or {}
        if walked.get("sig_class"):
            return f"{walked['sig_class']} (upstream)"
        if diag.get("no_error_text"):
            return "no error text in log"
    return root.get("sig_class") or "unclassified"


def _link(diag: dict) -> str | None:
    if diag.get("engine") == "databricks" and diag.get("dbx_run_id"):
        jid, rid = diag.get("job_id"), diag["dbx_run_id"]
        return f"{DBX_HOST}/jobs/{jid}/runs/{rid}" if jid else f"{DBX_HOST} run {rid}"
    if diag.get("engine") == "dataproc" and diag.get("batch_id"):
        return f"{DATAPROC_CONSOLE}/{diag['batch_id']}?project={GCP_PROJECT}"
    if diag.get("engine") == "vertex" and diag.get("vertex_run_id"):
        console = VERTEX_CONSOLE.format(loc=diag.get("vertex_location") or "us-central1")
        return f"{console}/{diag['vertex_run_id']}?project={diag.get('vertex_project')}"
    spark = diag.get("spark") or {}
    if spark.get("engine") == "external_task" and spark.get("run_id"):
        return f"{AIRFLOW_UI}/{spark['dag_id']}/runs/{spark['run_id']}"
    return None


def _no_output_note(
    ti_state: str | None,
    culprits: list[str] | None = None,
    poke_target: str | None = None,
    poke_count: int = 0,
    rescheduled: bool = False,
) -> str:
    """An empty log means different things for a failed task and an upstream_failed one."""
    if ti_state == "upstream_failed":
        if culprits:
            named = ", ".join(f"`{c}`" for c in culprits[:3])
            more = f" (+{len(culprits) - 3} more)" if len(culprits) > 3 else ""
            return f"The task never ran. The failure is {named}{more}; diagnose that."
        return "The task never ran; diagnose the upstream task that failed."
    if ti_state == "failed" and poke_target and not rescheduled:
        # No reschedule line means the sensor never handed control back, so something killed it (INC-025).
        return (
            f"The process was killed mid-poke while watching {poke_target}; the log stops with no "
            "exception and no reschedule. Nothing here is the cause. Check for a control-plane or "
            "infra event at the last log timestamp before looking at the DAG."
        )
    if ti_state == "failed" and poke_target:
        return (
            f"A reschedule-mode sensor polled {poke_target} {poke_count} time(s) and never saw it. "
            "This try holds no timeout line because the sensor gave up in a different try; the "
            "question is whether the object ever landed, not why this log looks healthy."
        )
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
    resolved = resolved_cause(diag)
    likely = (resolved[0] if resolved else root.get("likely_cause") or "").strip()
    if likely:
        lines.append(likely if likely.endswith(".") else likely + ".")
        cause_idx = len(lines) - 1
    fix = (resolved[1] if resolved and resolved[1] else None) or fix_line(root)
    if fix:
        lines.append(fix)
    link = _link(diag)
    if link:
        lines.append(link)
    walked = walked_cause(diag)
    if walked:
        lines += [walked[0], walked[1]]
    else:
        stated = stated_condition(diag)
        if stated:
            lines.append(stated)
    mask = detect_mask(_verdict_text(diag))
    if mask:
        lines.append(mask_note(mask))
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


def _downstream_text(diag: dict) -> str:
    """Error text from a layer below Airflow, when the task's own log carries none."""
    spark = diag.get("spark") or {}
    return " ".join(
        str(x)
        for x in (diag.get("root_error"), spark.get("error_text"), spark.get("state_message"))
        if x
    ).strip()


def resolved_cause(diag: dict) -> tuple[str, str] | None:
    """(why, how) from a resolver that settled the signature's open fork, when one did.

    A signature names a class; most classes still hide a fork the reader would otherwise resolve
    by hand. When the evidence settles it, the settled answer replaces the conditional remedy.
    """
    res = diag.get("resolution")
    if not res:
        return None
    why = f"{res['verdict']} ({res['evidence']})"
    how = " ".join(f"{i}. {s}" for i, s in enumerate(res.get("solutions") or [], 1))
    return why, (how or None)


def walked_cause(diag: dict) -> tuple[str, str] | None:
    """(why, how) from the upstream walk's root, or None when the walk reached nothing.

    This outranks the stub verdict: "the task never ran, diagnose X" is a pointer, and the walk
    already followed it. Reporting the pointer once the destination is known wastes the trip.
    """
    walked = diag.get("upstream_walk") or {}
    root = walked.get("root")
    if not root:
        return None
    hops = walked.get("hops") or []
    who = f"{root.get('dag_id')}.{root.get('task_id')}"
    trail = ""
    if len(hops) > 1:
        trail = " via " + " -> ".join(h["task_id"] for h in hops[:-1])
    sig = root.get("signature") or {}
    if sig.get("likely_cause"):
        why = f"Root cause is {who}{trail}: {_one_line(sig['likely_cause'], 300)}"
        how = fix_line(sig) or f"Fix {who}; this task never ran."
    else:
        why = f"Root cause is {who}{trail}, which raised: {_one_line(root.get('error') or '', 240)}"
        how = f"Fix {who}. This task never ran, so nothing here needs changing."
    return why, how


def stated_condition(diag: dict) -> str | None:
    """The named condition behind a failure no signature matched, or None when there is not one.

    Both the report and the Slack post must stand behind the same sentence. Keeping the wording in
    one place is what stops the channel saying "no cause found" about a failure the report has
    already resolved to a named upstream task.
    """
    if diag.get("root_signature"):
        return None
    if diag.get("no_error_text"):
        if diag.get("ti_state") != "upstream_failed" and _downstream_text(diag):
            # The log is empty but the evidence is not; "the worker died" would overwrite it.
            return None
        return _no_output_note(
            diag.get("ti_state"),
            diag.get("upstream_failed_tasks"),
            diag.get("poke_target"),
            diag.get("poke_count", 0),
            bool(diag.get("reschedule_count")),
        )
    return _pod_startup_note(diag)


def walk_note(diag: dict) -> str | None:
    """Why the upstream walk did not reach a root. Silence would read as "there was nothing"."""
    walked = diag.get("upstream_walk") or {}
    if walked.get("root") or not walked.get("note"):
        return None
    return f"Could not follow the chain: {walked['note']}."


def stated_next_step(diag: dict) -> str:
    """Where to go next for a stated condition. The cause is rarely in this task's own log."""
    note = walk_note(diag)
    if diag.get("ti_state") == "upstream_failed":
        step = "Diagnose the upstream task named above; this one never started."
        return f"{step} {note}" if note else step
    if diag.get("pod_deleted"):
        return "Check node capacity and image-pull time for that pod, not the task's code."
    if diag.get("poke_target"):
        return "Check whether the awaited object landed, and for a control-plane event at the last timestamp."
    return "Check whether it already retried; the worker died before the task could raise."


def _pod_startup_note(diag: dict) -> str | None:
    """A KubernetesPodOperator that gave up waiting raises with an EMPTY message.

    The log looks like it has an error line and carries no error at all, so nothing classifies.
    The evidence is structural: the operator announced a startup budget, then deleted the pod.
    """
    if not (diag.get("pod_deleted") and diag.get("pod_wait_seconds")):
        return None
    if (diag.get("root_error") or "").strip():
        return None
    pod = diag.get("pod_name") or "the pod"
    return (
        f"The pod {pod} did not reach Running inside its {diag['pod_wait_seconds']}s budget, so the "
        "operator deleted it and raised with an empty message. Nothing in this log is the cause: "
        "check node capacity and image-pull time for that pod, not the task's code."
    )


def _verdict_text(diag: dict) -> str:
    """The error the report is about to stand behind, whatever layer it came from."""
    spark = diag.get("spark") or {}
    return " ".join(
        str(x)
        for x in (
            diag.get("root_error"),
            spark.get("error_text"),
            spark.get("state_message"),
            (diag.get("root_signature") or {}).get("matched_on"),
        )
        if x
    )


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
    fix = fix_line(root)
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
