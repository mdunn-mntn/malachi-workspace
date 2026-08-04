"""One-call Spark optimization report from a single event log.

Parses the event log (all 7 surfaces) and runs every detector - the plan-text ones
(`analyze_plan`, over the SQL physical plan) AND the metric ones (`analyze_run`) - then
renders a BLUF report grouped by recommendation type: CODE (query/PR), INFRA (cores/memory),
FAILURE (route). One event log in, an engineer-ready optimization backlog out.
"""

from __future__ import annotations

from .eventlog import parse_eventlog
from .optimizations import OptFinding, analyze_plan, analyze_run

_TYPE_ORDER = ["failure", "infra", "code"]
_TYPE_LABEL = {"code": "CODE / query-PR", "infra": "INFRA / compute", "failure": "FAILURE / route"}


def analyze_eventlog(eventlog_path: str) -> tuple:
    """Parse an event log and return (SparkRun, ranked findings) - plan + metric detectors."""
    run = parse_eventlog(eventlog_path)
    findings = analyze_run(run)
    for s in run.sql:
        if s.plan_text:
            findings += analyze_plan(s.plan_text)
    return run, _dedup_rank(findings)


def optimize_run(eventlog_path: str) -> list[OptFinding]:
    """Parse an event log and return all optimization findings (plan + metrics), ranked."""
    return analyze_eventlog(eventlog_path)[1]


def _dedup_rank(findings: list[OptFinding]) -> list[OptFinding]:
    seen, out = set(), []
    for f in findings:
        if (f.key, f.title) not in seen:
            seen.add((f.key, f.title))
            out.append(f)
    rank = {"high": 0, "medium": 1, "low": 2}
    out.sort(key=lambda f: rank.get(f.impact, 3))
    return out


def render_report(findings: list[OptFinding]) -> str:
    """BLUF report: headline finding, then grouped CODE / INFRA / FAILURE sections."""
    if not findings:
        return "No optimization findings."
    top = findings[0]
    lines = [f"Optimization: {len(findings)} findings. Top [{top.impact}]: {top.title}", ""]
    for rec in _TYPE_ORDER:
        group = [f for f in findings if f.rec_type == rec]
        if not group:
            continue
        lines.append(f"## {_TYPE_LABEL[rec]}")
        for f in group:
            lines.append(f"- [{f.impact}] {f.title}")
            lines.append(f"    why: {f.evidence}")
            lines.append(f"    fix: {f.fix}")
        lines.append("")
    return "\n".join(lines).rstrip()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("usage: python -m airflow_debugger.optimize <spark_eventlog>")
        raise SystemExit(2)
    print(render_report(optimize_run(sys.argv[1])))
