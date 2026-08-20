"""One sweep: crawl the downloaded logs, record the ledger, check coverage, write the digest.

The shell script owns acquisition (GCS + PHS downloads); everything after the bytes land
happens here, so the ordering and the failure behaviour are testable in Python rather than
spread across bash.

Three files come out, all under the ticket's outputs/:
    optimizer_backlog_<date>.md    every finding, per log, worst first
    optimizer_coverage_<date>.md   which active DAGs could not be profiled, and why
    optimizer_digest_<date>.md     what CHANGED - the thing a person actually reads
plus an append to optimization_ledger.jsonl.

Coverage and the ledger are best-effort: neither may sink a sweep that produced findings.
"""

from __future__ import annotations

import argparse
import os
import subprocess

from . import coverage as cov_mod
from . import digest as digest_mod
from . import ledger as ledger_mod
from .crawl import crawl, render_crawl

OUTDIR = "tickets/audi_1194_optimizer_efficiency_crawler/outputs"
# gsutil, not `gcloud storage`: the same decompressive-transcoding gatekeeper that corrupts
# a .zstd download also has to be bypassed on the way back up for anything compressed.
_GSUTIL = ["gsutil", "-o", "GSUtil:check_hashes=never"]


def _dag_ids(reports: list, known: set | None = None) -> set:
    """The normalised job names this sweep produced findings for."""
    return {ledger_mod._dag_id(r, known) for r in reports if not r.error and r.findings}


def publish(files: list[str], gcs_prefix: str) -> list[str]:
    """Copy the sweep's artifacts to GCS. Returns what landed; never raises.

    A runner has no repo to commit to and should not have one - keeping its GitHub identity
    read-only is the point. GCS is where the outputs live for anyone downstream.
    """
    if not gcs_prefix:
        return []
    landed = []
    for f in files:
        if not f or not os.path.exists(f):
            continue
        dest = f"{gcs_prefix.rstrip('/')}/{os.path.basename(f)}"
        r = subprocess.run([*_GSUTIL, "cp", f, dest], capture_output=True, timeout=300)
        if r.returncode == 0:
            landed.append(dest)
        else:
            print(f"[sweep] upload failed {dest}: {r.stderr.decode()[:160]}")
    return landed


def run(paths: list[str], date: str, source: str = "", airflow_base: str = "",
        outdir: str = OUTDIR, ledger_path: str = ledger_mod.LEDGER,
        gcs_prefix: str = "") -> dict:
    """Crawl, record, report. Returns the paths written and the headline counts."""
    reports = crawl(paths)
    scored = [r for r in reports if not r.error]
    findings = sum(len(r.findings) for r in scored)
    high = sum(r.n_high for r in scored)
    os.makedirs(outdir, exist_ok=True)

    backlog = os.path.join(outdir, f"optimizer_backlog_{date}.md")
    with open(backlog, "w") as fh:
        fh.write(f"# Spark fleet optimizer backlog — {date}\n\n")
        if source:
            fh.write(f"Source: {source}\n\n")
        fh.write(render_crawl(reports) + "\n")

    # Coverage runs FIRST: its active-DAG set is what lets the ledger tell a run index
    # (materialize_mntn_select_16) from a data-source id (ipdsc_ds_67).
    cov, known = None, None
    if airflow_base:
        try:
            cov = cov_mod.collect(airflow_base, date)
            known = {d.dag_id for d in cov.dags} or None
        except Exception as e:
            print(f"[sweep] coverage skipped: {str(e)[:160]}")
            cov = None

    entries, delta = [], ledger_mod.Delta()
    try:
        entries = ledger_mod.record(reports, date, path=ledger_path, known=known)
        delta = ledger_mod.delta(entries)
    except Exception as e:  # a ledger fault must not lose the backlog
        print(f"[sweep] ledger skipped: {str(e)[:160]}")

    if cov is not None:
        cov.report_path = os.path.join(outdir, f"optimizer_coverage_{date}.md")
        with open(cov.report_path, "w") as fh:
            fh.write(cov_mod.render(cov, _dag_ids(reports, known)))

    text = digest_mod.render(delta, scanned=len(scored), findings=findings, high=high,
                             date=date, coverage=cov, backlog_path=backlog)
    digest_path = os.path.join(outdir, f"optimizer_digest_{date}.md")
    with open(digest_path, "w") as fh:
        fh.write(digest_mod.render_plain(text))

    published = publish([backlog, digest_path, cov.report_path if cov else "", ledger_path],
                        gcs_prefix)

    return {
        "backlog": backlog, "digest": digest_path,
        "coverage": cov.report_path if cov else "",
        "scanned": len(scored), "findings": findings, "high": high,
        "ledger_entries": len(entries), "slack": text, "published": published,
    }


def main() -> None:
    """CLI entry point - the daily cron calls this once the downloads are on disk."""
    ap = argparse.ArgumentParser(description="Run one optimizer sweep over downloaded logs.")
    ap.add_argument("paths", nargs="+", help="event-log dirs or globs")
    ap.add_argument("--date", required=True)
    ap.add_argument("--source", default="", help="provenance line for the backlog header")
    ap.add_argument("--airflow-base", default="",
                    help="Airflow API base ending in /api/v2; omit to skip coverage")
    ap.add_argument("--outdir", default=OUTDIR)
    ap.add_argument("--ledger", default=ledger_mod.LEDGER)
    ap.add_argument("--gcs-prefix", default=os.environ.get("OPTIMIZER_GCS_PREFIX", ""),
                    help="gs://... to publish the artifacts to; omit to keep them local only")
    args = ap.parse_args()

    out = run(args.paths, args.date, args.source, args.airflow_base, args.outdir,
              args.ledger, args.gcs_prefix)
    print(f"Fleet optimization: {out['scanned']} jobs scanned, {out['findings']} findings, "
          f"{out['high']} high-impact.")
    print(f"[sweep] backlog  {out['backlog']}")
    print(f"[sweep] digest   {out['digest']}  ({out['ledger_entries']} ledger entries)")
    if out["coverage"]:
        print(f"[sweep] coverage {out['coverage']}")
    for dest in out["published"]:
        print(f"[sweep] published {dest}")


if __name__ == "__main__":
    main()
