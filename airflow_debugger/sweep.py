"""Full-corpus sweep: identity + classification rates over every real Airflow log.

Offline only. Reuses `parse_log`/`parse_log_file` for identity and `classify` for the
signature; never calls `orchestrate.investigate`, which hits live GCP per log.

The filename convention is the identity oracle: `<HHMMSS>__<dag>__<task>[__map<N>]__
try<N>__<state>.log` is written by the acquisition layer from the Airflow API, so a
body-derived dag/task that disagrees with it is a parser defect.

Rates are reported per outcome. A blended number is meaningless when most of the corpus
is green, and an `upstream_failed` stub carries no error to classify at all.

    python3 -m airflow_debugger.sweep [<glob>] [--out <path>]
"""

from __future__ import annotations

import glob as globlib
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass

from .parse import _FILENAME_RE, has_error_text, parse_log, parse_log_file
from .signatures import classify

DEFAULT_GLOB = "on-call/airflow_logs/*/*.log"
_CLUSTER_SAMPLE = 240  # chars of fingerprint text kept for the cluster label

# Ranked most-specific-first: the first hit labels the cluster. These are recognition
# probes for grouping unclassified logs, NOT a taxonomy; a cluster that earns a verdict
# graduates to signatures.py.
_CLUSTER_PROBES: list[tuple[str, str]] = [
    ("vertex code-9 pipeline step", r"The failed tasks are: \[|Job failed with:(\\n|\s)+code: 9"),
    ("batch-id attach trap", r"Batch with given id already exists|Attaching to the job"),
    ("slack notifier channel_not_found", r"channel_not_found"),
    ("impersonated-credentials 503", r"Unable to acquire impersonated credentials"),
    ("worker died, no exception", r"No exception message found"),
    ("dbt runtime error in model", r"Runtime Error in model|Database Error in model"),
    ("dataproc agent boilerplate only", r"Dataproc Agent reports job failure"),
    ("kubernetes pod api error", r"ApiException|kubernetes\.client"),
]


@dataclass
class LogResult:
    """One log's sweep outcome."""

    path: str
    outcome: str
    dag_file: str | None
    task_file: str | None
    dag_body: str | None
    task_body: str | None
    dag_final: str | None
    task_final: str | None
    run_id: str | None
    engine: str
    job_id: str | None
    signature: str | None
    has_error: bool

    @property
    def identity_ok(self) -> bool:
        """The production path (`parse_log_file`) resolved dag+task to the filename truth."""
        return self.dag_final == self.dag_file and self.task_final == self.task_file

    @property
    def body_fired(self) -> bool:
        """Body parsing alone yielded a dag and task (no filename fallback needed)."""
        return self.dag_body is not None and self.task_body is not None

    @property
    def body_disagrees(self) -> bool:
        """Body parsing fired but contradicts the filename: a real parser defect."""
        return self.body_fired and (
            self.dag_body != self.dag_file or self.task_body != self.task_file
        )


def _outcome(path: str) -> str:
    m = re.search(r"__try\d+__(\w+)\.log$", os.path.basename(path))
    return m.group(1) if m else "unknown"


def _cluster(text: str, job_id: str | None = None) -> str:
    """Label an unclassified log by its error shape."""
    if job_id:
        # A job id outranks any text probe: the cause is in the engine, and callback noise in the
        # Airflow log (a failed Slack notify) would otherwise mislabel a perfectly routable failure.
        return "no local cause, routes to engine RCA (job id present)"
    for label, pat in _CLUSTER_PROBES:
        if re.search(pat, text, re.IGNORECASE):
            return label
    for line in text.splitlines():
        if re.search(r"\[error\]|ERROR -", line, re.IGNORECASE):
            body = re.sub(r"^\S*\s*", "", line.strip())
            body = re.sub(r"\d{4}-\d\d-\d\dT[\d:.]+Z?", "<ts>", body)
            if job_id:
                # The Airflow log is textually identical to a healthy run; the cause is in
                # the engine. diagnose() routes on the job id, so this is not a taxonomy gap.
                return "no local cause, routes to engine RCA (job id present)"
            return f"other: {body[:_CLUSTER_SAMPLE]}"
    return "other: no error line"


def sweep_one(path: str) -> tuple[LogResult, str]:
    """Sweep one log; returns its result and the raw text (for clustering)."""
    with open(path, encoding="utf-8", errors="replace") as f:
        text = f.read()
    p = parse_log(text)
    final = parse_log_file(path)
    m = _FILENAME_RE.search(os.path.basename(path))
    sig = classify(text)
    return (
        LogResult(
            path=path,
            outcome=_outcome(path),
            dag_file=m.group("dag") if m else None,
            task_file=m.group("task") if m else None,
            dag_body=p.dag_id,
            task_body=p.task_id,
            dag_final=final.dag_id,
            task_final=final.task_id,
            run_id=final.run_id,
            engine=p.engine,
            job_id=p.batch_id or (str(p.dbx_run_id) if p.dbx_run_id else None),
            signature=sig.key if sig else None,
            has_error=has_error_text(text),
        ),
        text,
    )


def sweep(paths: list[str]) -> tuple[list[LogResult], dict[str, list[str]]]:
    """Sweep every path; returns results and unclassified-cluster label -> paths."""
    results: list[LogResult] = []
    clusters: dict[str, list[str]] = defaultdict(list)
    for path in sorted(paths):
        res, text = sweep_one(path)
        results.append(res)
        if res.signature is None and res.has_error:
            clusters[_cluster(text, res.job_id)].append(path)
    return results, dict(clusters)


def _table(rows: list[tuple]) -> list[str]:
    head = (
        "| Outcome | Logs | Identity resolved | Body alone | Body disagrees | "
        "Run id | Classified | No error text |"
    )
    out = [head, "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return out


def render(results: list[LogResult], clusters: dict[str, list[str]]) -> str:
    """Markdown sweep report, every table ranked descending."""
    by_outcome: dict[str, list[LogResult]] = defaultdict(list)
    for r in results:
        by_outcome[r.outcome].append(r)

    lines = [
        f"# AUDI-1191 corpus sweep — {len(results)} logs",
        "",
        "Identity oracle is the filename convention written by the acquisition layer.",
        "`Classified` counts logs with a signature; a log with no error text has nothing",
        "to classify and is counted separately, not as a taxonomy gap.",
        "",
    ]
    rows = []
    for outcome, group in sorted(by_outcome.items(), key=lambda kv: -len(kv[1])):
        rows.append(
            (
                outcome,
                len(group),
                sum(r.identity_ok for r in group),
                sum(r.body_fired for r in group),
                sum(r.body_disagrees for r in group),
                sum(r.run_id is not None for r in group),
                sum(r.signature is not None for r in group),
                sum(not r.has_error for r in group),
            )
        )
    lines += _table(rows)

    failures = [r for r in results if r.outcome in ("failed", "upstream_failed")]
    diagnosable = [r for r in failures if r.has_error]
    hit = sum(r.signature is not None for r in diagnosable)
    routable = sum(r.signature is None and r.job_id is not None for r in diagnosable)
    greens = sum(r.signature is not None for r in results if r.outcome == "success")
    lines += [
        "",
        "## Headline",
        "",
        f"- Identity: {sum(r.identity_ok for r in results)}/{len(results)} resolved by"
        " `parse_log_file`; "
        f"{sum(r.body_fired for r in results)} from the log body alone, "
        f"{sum(r.body_disagrees for r in results)} contradicting the filename.",
        f"- Diagnosable failures (failed + upstream_failed, with error text): {len(diagnosable)}"
        f" of {len(failures)}.",
        f"- Classified: {hit}/{len(diagnosable)}"
        f" ({100 * hit // max(len(diagnosable), 1)}%) of diagnosable failures.",
        f"- Routable without a signature (job id present): {routable}."
        " These carry no cause in the Airflow log and are resolved by the engine RCA,"
        " so they are not taxonomy gaps.",
        f"- Fires on a green run: {greens}"
        " (a signature firing on a success log is a false positive unless the mechanism"
        " genuinely occurred).",
        "",
        "## Signatures fired",
        "",
    ]
    for key, n in Counter(r.signature for r in failures if r.signature).most_common():
        lines.append(f"- {n} — `{key}`")

    lines += ["", "## UNCLASSIFIED clusters (with error text)", ""]
    for label, paths in sorted(clusters.items(), key=lambda kv: -len(kv[1])):
        lines.append(f"- **{len(paths)}** — {label}")
        lines.append(f"  - e.g. `{paths[0]}`")

    mismatches = [r for r in results if not r.identity_ok or r.body_disagrees]
    if mismatches:
        lines += ["", "## Identity disagreements", ""]
        for r in mismatches[:20]:
            lines.append(
                f"- `{os.path.basename(r.path)}` body=({r.dag_body}/{r.task_body}) "
                f"file=({r.dag_file}/{r.task_file})"
            )
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> None:
    """CLI: sweep a glob of Airflow logs and print or write the report."""
    out = None
    if "--out" in argv:
        i = argv.index("--out")
        out = argv[i + 1]
        argv = argv[:i] + argv[i + 2 :]  # else the value reads as the glob
    args = [a for a in argv if not a.startswith("--")]
    pattern = args[0] if args else DEFAULT_GLOB
    paths = [p for p in globlib.glob(pattern) if not p.endswith(".rca.md")]
    if not paths:
        raise SystemExit(f"no logs matched {pattern}")
    results, clusters = sweep(paths)
    text = render(results, clusters)
    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"wrote {out} ({len(results)} logs)")
    else:
        print(text)


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
