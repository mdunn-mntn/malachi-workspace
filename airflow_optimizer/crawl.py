"""Fleet crawl - optimize every job in a set of event logs, rank a cross-job backlog.

The "check every DAG/task" mode: point it at a directory of Spark event logs (or the GCS
event-log prefix once enablement lands) and it runs the full analyzer on each, then ranks the
worst offenders first so an owner triages the biggest wins across the fleet. Works on succeeded
jobs too - a slow-but-green job is the optimization target.
"""

from __future__ import annotations

import glob
import os
from dataclasses import dataclass, field

from .optimize import analyze_eventlog


@dataclass
class JobReport:
    """One job's optimization findings, with a rollup for ranking."""

    source: str
    findings: list = field(default_factory=list)
    error: str | None = None
    app_name: str | None = None  # from spark.app.name - the human label for the job
    exec_h: float = 0.0

    @property
    def n_high(self) -> int:
        """Count of high-impact findings (the ranking driver)."""
        return sum(1 for f in self.findings if f.impact == "high")

    @property
    def n_medium(self) -> int:
        """Count of medium-impact findings (the ranking tiebreak)."""
        return sum(1 for f in self.findings if f.impact == "medium")

    @property
    def score(self) -> tuple:
        """Rank key: high-impact count, then medium, then total."""
        return (self.n_high, self.n_medium, len(self.findings))


def _event_logs(paths: list[str]) -> list[str]:
    """Expand dirs/globs to event-log paths (v2 rolling dirs, `.zstd` files, plain JSON).

    A dir is ONE rolling log only when it is named eventlog_v2_* or carries the appstatus_*
    marker - merely containing events_* files is not enough (a flat download dir with loose
    parts would otherwise be hijacked into one cross-job chimera). Other dirs expand
    recursively; .inprogress logs are surfaced so the crawl can SHOW them as skipped.
    """
    out = []
    for p in paths:
        if os.path.isdir(p):
            base = os.path.basename(p.rstrip("/"))
            if base.startswith("eventlog_v2_") or glob.glob(os.path.join(p, "appstatus_*")):
                out.append(p)
            else:
                kids = glob.glob(os.path.join(p, "*"))
                out += _event_logs([c for c in kids if os.path.isdir(c)])
                out += [c for c in kids
                        if c.endswith((".zstd", ".json", ".inprogress"))]
        else:
            out += glob.glob(p)
    return sorted(set(out))


def executor_hours(run: object) -> float:
    """Executor-hours the run held, billed whether or not a task was running.

    A killed or still-rolling app writes no ApplicationEnd and releases no executors, so its
    cost is measured to the last event the log carries. Without that fallback the largest
    runaway in the fleet costs 0.0 and sorts last, which is the opposite of the truth.
    """
    end = getattr(run, "app_end_ts", None) or getattr(run, "last_event_ts", None)
    if not end:
        return 0.0
    spans = [(e.removed_ts or end) - e.added_ts
             for e in getattr(run, "executors", [])
             if getattr(e, "added_ts", None) is not None]
    return sum(max(s, 0) for s in spans) / 3_600_000


def crawl(paths: list[str]) -> list[JobReport]:
    """Run the optimizer on every event log; return per-job reports ranked worst-first."""
    reports = []
    for log in _event_logs(paths):
        base = os.path.basename(log.rstrip("/"))
        if log.endswith(".inprogress"):
            reports.append(JobReport(
                source=base, error="in-progress log (job still running or crashed mid-write)"))
            continue
        try:
            run, findings = analyze_eventlog(log)
            # A truncated download parses clean, which is a wrong answer, not a missing one.
            if not getattr(run, "jobs", None) and not getattr(run, "stages", None):
                reports.append(JobReport(
                    source=base,
                    error="log parsed but contains no jobs or stages (truncated or empty)"))
                continue
            reports.append(JobReport(source=base, findings=findings,
                                     app_name=run.app_name, exec_h=executor_hours(run)))
        except Exception as e:  # a bad log must not sink the crawl
            reports.append(JobReport(source=base, error=str(e)[:120]))
    reports.sort(key=lambda r: r.score, reverse=True)
    return reports


def render_crawl(reports: list[JobReport]) -> str:
    """Cross-job backlog: fleet summary + each job's top findings, worst-first."""
    scored = [r for r in reports if not r.error]
    total = sum(len(r.findings) for r in scored)
    lines = [f"Fleet optimization: {len(scored)} job{'s' if len(scored) != 1 else ''} scanned, "
             f"{total} finding{'s' if total != 1 else ''}, "
             f"{sum(r.n_high for r in scored)} high-impact.", ""]
    for r in reports:
        if r.error:
            lines.append(f"- {r.source}: SKIPPED ({r.error})")
            continue
        if not r.findings:
            lines.append(f"- {r.source}: clean")
            continue
        top = r.findings[0]
        by_type = ", ".join(sorted({f.rec_type for f in r.findings}))
        # keep the source filename: recurring jobs share an app_name across many runs
        label = f"{r.app_name} ({r.source})" if r.app_name else r.source
        lines.append(f"- {label} [{r.n_high} high, {len(r.findings)} total; {by_type}] "
                     f"-> top: {top.title}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("usage: python -m airflow_optimizer.crawl <event_log_dir_or_glob> ...")
        raise SystemExit(2)
    print(render_crawl(crawl(sys.argv[1:])))
