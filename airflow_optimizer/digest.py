"""Render the daily sweep as a team-facing digest: what's new, what's chronic, what's blind.

Separate from `crawl.render_crawl`, which is the full per-log backlog. A digest answers
"what should we look at today" in a screenful: it leads with the delta from the ledger,
links every job to its Airflow page, and states outright which DAGs could not be profiled.

Delivery is deliberately not implemented here. MNTN retired local Slack apps and API keys
on 2026-06-10, so this writes text; posting it belongs to an approved server-side path.
"""

from __future__ import annotations

import os


def _ui_base() -> str:
    """The deployment's own DAG URL template, or "" when there is nothing to link to.

    Resolved from the environment rather than hardcoded, so a dev or staging deployment links
    to itself instead of to prod. Airflow sets AIRFLOW__API__BASE_URL (3.x) or
    AIRFLOW__WEBSERVER__BASE_URL (2.x) in every task; OPTIMIZER_AIRFLOW_UI overrides both for a
    run outside Airflow. When none is set the digest degrades to plain DAG names, which is the
    right failure: a link to the wrong deployment is worse than no link.
    """
    override = os.environ.get("OPTIMIZER_AIRFLOW_UI", "").strip()
    if override:
        return override
    for var in ("AIRFLOW__API__BASE_URL", "AIRFLOW__WEBSERVER__BASE_URL"):
        base = os.environ.get(var, "").strip()
        if base:
            return f"{base.rstrip('/')}/dags/{{dag_id}}"
    return ""


AIRFLOW_UI = _ui_base()
SEV = {"high": "HIGH", "medium": "MED", "low": "LOW"}
_RANK = {"high": 0, "medium": 1, "low": 2}


def dag_link(dag_id: str, base: str = AIRFLOW_UI, known: set | None = None) -> str:
    """Slack link to the DAG page, but only when the DAG is one coverage actually saw.

    Spark app names are not always dag_ids, so linking unconditionally ships dead links.
    """
    if not base or (known is not None and dag_id not in known):
        return f"`{dag_id}`"
    return f"<{base.format(dag_id=dag_id)}|{dag_id}>"


def _line(entry: object, base: str, known: set | None = None) -> str:
    sev = SEV.get(getattr(entry, "impact", ""), "LOW")
    dag = getattr(entry, "dag_id", "") or "unknown"
    title = getattr(entry, "title", "")
    bits = []
    streak = getattr(entry, "streak", 0)
    if streak > 1:
        bits.append(f"day {streak}")
    dcu = getattr(entry, "dcu_h", None)
    if dcu:
        bits.append(f"{dcu:,.0f} DCU-h/day")
    state = getattr(entry, "state", "")
    if state in ("owner_notified", "wont_fix"):
        bits.append(state.replace("_", " "))
    tail = f"  _{', '.join(bits)}_" if bits else ""
    return f"- *{sev}* {dag_link(dag, base, known)} — {title}{tail}"


def _section(title: str, entries: list, base: str, known: set | None = None,
             cap: int = 8) -> list[str]:
    if not entries:
        return []
    ordered = sorted(entries, key=lambda e: (
        _RANK.get(getattr(e, "impact", ""), 3),
                             -(getattr(e, "exec_h", None) or getattr(e, "dcu_h", None) or 0)))
    lines = [f"*{title}*"] + [_line(e, base, known) for e in ordered[:cap]]
    if len(ordered) > cap:
        lines.append(f"- _…{len(ordered) - cap} more in the full backlog_")
    return lines + [""]


def _worst_first(entries: list) -> list:
    """Entries ordered by impact, then by the executor-hours the run held."""
    return sorted(entries, key=lambda e: (_RANK.get(getattr(e, "impact", ""), 3),
                                          -(getattr(e, "exec_h", None) or 0)))


def by_dag(entries: list, cap: int = 3) -> list:
    """The `cap` worst DAGs, each with its own findings worst-first.

    Ranking per finding lets one bad DAG fill the page: the 2026-08-21 digest opened with
    eight consecutive `fangorn_score_monitor` lines and showed nothing else.
    """
    groups: dict = {}
    for e in entries:
        groups.setdefault(getattr(e, "dag_id", "") or "unknown", []).append(e)
    ranked = sorted(groups.items(),
                    key=lambda kv: (_RANK.get(_worst_first(kv[1])[0].impact, 3),
                                    -max((getattr(e, "exec_h", None) or 0 for e in kv[1]),
                                         default=0),
                                    -len(kv[1])))
    return [(dag, _worst_first(rows)) for dag, rows in ranked[:cap]]


def _blocks(dag: str, rows: list, base: str, known: set | None) -> list[str]:
    """One DAG as What / Where / Why / How, the same four blocks every day."""
    worst = rows[0]
    others = len(rows) - 1
    what = getattr(worst, "title", "")
    if others:
        what += f" (+{others} more finding{'s' if others != 1 else ''} on this DAG)"
    why = f"{SEV.get(getattr(worst, 'impact', ''), 'LOW')} impact"
    dcu = getattr(worst, "dcu_h", None)
    # Per RUN, not per finding: every finding on a DAG shares the one run's hours.
    hours = max((getattr(e, "exec_h", None) or 0 for e in rows), default=0)
    if dcu:
        why += f", {dcu:,.0f} DCU-h/day"
    elif hours:
        why += f", {hours:,.0f} executor-hours held on this run"
    streak = getattr(worst, "streak", 0)
    if streak > 1:
        why += f", firing {streak} sweeps running"
    return [f"  *What*  {what}",
            f"  *Where* {dag_link(dag, base, known)} · `{getattr(worst, 'app_id', '')}`",
            f"  *Why*   {why}",
            f"  *How*   {getattr(worst, 'fix', '') or 'See the backlog for the fix.'}",
            ""]


def render(delta: object, scanned: int, findings: int, high: int, date: str,
           coverage: object | None = None, backlog_path: str = "",
           base: str = AIRFLOW_UI) -> str:
    """The digest. Headline, then only the parts that changed, then the blind spots."""
    head = (f"*Spark optimizer — {date}*\n"
            f"{scanned} Spark job{'s' if scanned != 1 else ''} scanned, "
            f"{findings} finding{'s' if findings != 1 else ''}, {high} high.")
    if coverage is not None:
        head += f" {coverage.unprofiled_line()}"

    known = {d.dag_id for d in getattr(coverage, "dags", [])} or None if coverage else None
    out = [head, ""]
    actionable = (list(getattr(delta, "fix_not_working", []))
                  + list(getattr(delta, "new", [])) + list(getattr(delta, "chronic", [])))
    for dag, rows in by_dag(actionable):
        out += _blocks(dag, rows, base, known)
    out += _section("With the owner", getattr(delta, "notified", []), base, known)
    resolved = getattr(delta, "resolved", [])
    if resolved:
        names = ", ".join(sorted({getattr(e, "dag_id", "") for e in resolved}))
        out += [f"*Stopped firing* — {names}", ""]

    if not any(getattr(delta, k, [])
               for k in ("new", "chronic", "notified", "resolved", "fix_not_working")):
        out += ["No change since the last sweep.", ""]

    if backlog_path:
        out.append(f"Full backlog: `{backlog_path}`")
    if coverage is not None and coverage.unprofiled:
        out.append(f"Not scanned: `{coverage.report_path}`")
    return "\n".join(out).rstrip() + "\n"


def render_plain(text: str) -> str:
    """Strip Slack link markup for a file or terminal reader."""
    import re

    return re.sub(r"<([^|>]+)\|([^>]+)>", r"\2 (\1)", text).replace("*", "")
