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

    @property
    def n_high(self) -> int:
        """Count of high-impact findings (the ranking driver)."""
        return sum(1 for f in self.findings if f.impact == "high")

    @property
    def score(self) -> tuple:
        """Rank key: most high-impact findings first, then total count."""
        return (self.n_high, len(self.findings))


def _event_logs(paths: list[str]) -> list[str]:
    """Expand dirs/globs to event-log paths (v2 rolling dirs, `.zstd` files, plain JSON)."""
    out = []
    for p in paths:
        if os.path.isdir(p):
            # a v2 rolling-log dir is itself one log; a dir OF logs expands to its children
            if glob.glob(os.path.join(p, "events_*")) or glob.glob(os.path.join(p, "appstatus_*")):
                out.append(p)
            else:
                out += [c for c in glob.glob(os.path.join(p, "*"))
                        if c.endswith(".zstd") or c.endswith(".json") or os.path.isdir(c)]
        else:
            out += glob.glob(p)
    return sorted(set(out))


def crawl(paths: list[str]) -> list[JobReport]:
    """Run the optimizer on every event log; return per-job reports ranked worst-first."""
    reports = []
    for log in _event_logs(paths):
        base = os.path.basename(log.rstrip("/"))
        try:
            run, findings = analyze_eventlog(log)
            reports.append(JobReport(source=base, findings=findings, app_name=run.app_name))
        except Exception as e:  # a bad log must not sink the crawl
            reports.append(JobReport(source=base, error=str(e)[:120]))
    reports.sort(key=lambda r: r.score, reverse=True)
    return reports


def render_crawl(reports: list[JobReport]) -> str:
    """Cross-job backlog: fleet summary + each job's top findings, worst-first."""
    scored = [r for r in reports if not r.error]
    total = sum(len(r.findings) for r in scored)
    lines = [f"Fleet optimization: {len(scored)} jobs scanned, {total} findings, "
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
        label = r.app_name or r.source
        lines.append(f"- {label} [{r.n_high} high, {len(r.findings)} total; {by_type}] "
                     f"-> top: {top.title}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("usage: python -m airflow_optimizer.crawl <event_log_dir_or_glob> ...")
        raise SystemExit(2)
    print(render_crawl(crawl(sys.argv[1:])))
