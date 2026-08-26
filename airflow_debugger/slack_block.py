"""One fixed Slack shape for every RCA: What / Where / Why / How.

On-call learns one layout, so the eye lands in the same place every time. That constraint is the
product — a post whose structure varies with the failure is a second thing to read, not a summary.

Five sources fill Why, in strict precedence: the deterministic classifier, the root the upstream
walk reached, the condition the log structure states outright (the task never ran, the pod never
started), the LLM, then an honest statement of where the chain stopped. Which one spoke is always
labelled, because a model's guess and a matched signature are not the same evidence and must never
look the same. Both walked and stated outrank the LLM for that reason: they are read off logs and
API state, not inferred.

How is a POINTER, never a patch: file, line range, permalink. Auto-PR was ruled out and a diff
pasted into a channel is the same act with extra steps.
"""

from __future__ import annotations

import os

from .masks import detect as detect_mask
from .report import (
    _link,
    _one_line,
    code_links,
    fix_line,
    resolved_cause,
    stated_condition,
    stated_next_step,
    walked_cause,
)

MAX_BLOCK = 2900  # Slack hard-caps a section block at 3000 chars
_ASTRO_UI = (os.environ.get("AIRFLOW_API_BASE") or "").rstrip("/").removesuffix("/api/v2")

WHY_DETERMINISTIC = "signature"
WHY_RESOLVED = "resolved"
WHY_WALKED = "walked"
WHY_STATED = "stated"
WHY_LLM = "llm"
WHY_GAP = "gap"


def _astro_run_url(dag_id: str | None, run_id: str | None) -> str | None:
    """Deep link to the failing run, or None rather than a URL that 404s."""
    if not (_ASTRO_UI and dag_id and run_id):
        return None
    return f"{_ASTRO_UI}/dags/{dag_id}/runs/{run_id}"


def why(diag: dict, llm_cause: str | None = None) -> tuple[str, str]:
    """(text, source) for the Why line. Evidence outranks opinion, opinion outranks silence."""
    root = diag.get("root_signature") or {}
    resolved = resolved_cause(diag)
    if resolved:
        return _one_line(resolved[0], 600), WHY_RESOLVED
    if root.get("likely_cause"):
        return _one_line(root["likely_cause"], 600), WHY_DETERMINISTIC
    walked = walked_cause(diag)
    if walked:
        return _one_line(walked[0], 600), WHY_WALKED
    stated = stated_condition(diag)
    if stated:
        return _one_line(stated, 600), WHY_STATED
    if llm_cause:
        return _one_line(llm_cause, 600), WHY_LLM
    mask = detect_mask(
        " ".join(
            str(x)
            for x in (diag.get("root_error"), (diag.get("spark") or {}).get("error_text"))
            if x
        )
    )
    if mask:
        return f"The chain stopped on a masking error; it hides {mask.hides}.", WHY_GAP
    return "No signature matched and the LLM did not resolve it.", WHY_GAP


def how(diag: dict, source: str) -> str:
    """The next action. On a gap that is the next hop to read, never a guessed fix."""
    if source == WHY_RESOLVED:
        return resolved_cause(diag)[1] or fix_line(diag.get("root_signature") or {}) or ""
    if source == WHY_WALKED:
        return walked_cause(diag)[1]
    if source == WHY_STATED:
        return stated_next_step(diag)
    if source == WHY_GAP:
        mask = detect_mask(
            " ".join(
                str(x)
                for x in (diag.get("root_error"), (diag.get("spark") or {}).get("error_text"))
                if x
            )
        )
        if mask:
            return f"Read {mask.next_hop}."
        return "Open the task log; this class is not yet in the taxonomy."
    root = diag.get("root_signature") or {}
    remedy = (root.get("remedy") or "").strip()
    if remedy and root.get("programmatic_fix") == "yes":
        return f"{remedy} Verify against the linked lines before changing anything."
    return remedy or fix_line(root) or "No remedy on record; diagnose from the log tail."


def render(diag: dict, llm_cause: str | None = None, repo_paths: dict | None = None) -> str:
    """The post body. Same four labels, same order, every time."""
    ident = diag.get("identity", {})
    dag_id, task_id = ident.get("dag_id"), ident.get("task_id")
    who = "/".join(filter(None, [dag_id, task_id])) or "unknown task"
    root = diag.get("root_signature") or {}
    cause_text, source = why(diag, llm_cause)

    walked_sig = ((diag.get("upstream_walk") or {}).get("root") or {}).get("signature") or {}
    klass = root.get("sig_class") or {
        WHY_WALKED: walked_sig.get("sig_class") or "upstream/root-cause-walked",
        WHY_STATED: "no-cause-in-log",
    }.get(source, "unclassified")
    what = f"*{who}* — {klass}"

    where = [f"`{who}`"]
    run_url = _astro_run_url(dag_id, ident.get("run_id"))
    if run_url:
        where.append(f"<{run_url}|Airflow run>")
    engine_url = _link(diag)
    if engine_url:
        where.append(f"<{engine_url}|{diag.get('engine') or 'engine'} job>")
    for url, path in code_links(diag, repo_paths):
        where.append(f"<{url}|{path}>")

    label = {
        WHY_DETERMINISTIC: "matched signature",
        WHY_RESOLVED: "settled from evidence",
        WHY_WALKED: "walked upstream",
        WHY_STATED: "no cause in this log",
        WHY_LLM: "LLM, unverified",
        WHY_GAP: "no cause found",
    }[source]

    body = "\n".join(
        [
            f"*What*  {what}",
            f"*Where*  {' · '.join(where)}",
            f"*Why*  ({label}) {cause_text}",
            f"*How*  {how(diag, source)}",
        ]
    )
    return body if len(body) <= MAX_BLOCK else body[: MAX_BLOCK - 1].rstrip() + "…"
