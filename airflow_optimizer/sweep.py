"""One sweep: crawl the downloaded logs, record the ledger, check coverage, write the digest.

Acquisition (the GCS and PHS downloads) happens before this is called, so ordering and failure
behaviour live in one testable place rather than being spread across the caller.

Three files come out, all under `outdir`:
    optimizer_backlog_<date>.md    every finding, per log, worst first
    optimizer_coverage_<date>.md   which active DAGs could not be profiled, and why
    optimizer_digest_<date>.md     what CHANGED - the thing a person actually reads
plus an append to the ledger.

Coverage and the ledger are best-effort: neither may sink a sweep that produced findings. What
they may NOT do is publish a confident wrong answer, so a partial sweep resolves nothing and a
sweep that lost coverage declines to write ledger rows it cannot key correctly.
"""

from __future__ import annotations

import argparse
import os
import subprocess

from . import coverage as cov_mod
from . import digest as digest_mod
from . import ledger as ledger_mod
from . import notify as notify_mod
from .crawl import crawl, render_crawl

OUTDIR = os.environ.get("OPTIMIZER_OUTDIR", "optimizer_out")
# gsutil, not `gcloud storage`: decompressive transcoding corrupts compressed objects.
_GSUTIL = ["gsutil", "-o", "GSUtil:check_hashes=never"]


def _rendered_dags(entries: list, delta: object, scored: list, known: set | None) -> set:
    """Every name the digest can print, so coverage judges the same strings the reader sees.

    Keying the two surfaces differently let the digest render a job unlinked while coverage's
    own unresolved list never mentioned it, which read as the tool disagreeing with itself.
    """
    names = {getattr(e, "dag_id", "") for e in entries}
    for k in ("new", "chronic", "notified", "resolved", "fix_not_working"):
        names |= {getattr(e, "dag_id", "") for e in getattr(delta, k, [])}
    names |= {ledger_mod._dag_id(r, known) for r in scored}
    return names - {""}


def _databricks_report() -> str:
    """The Databricks section, or "" when no warehouse is configured or the reads failed."""
    try:
        from .databricks import report

        return report()
    except Exception as e:  # the Spark half must ship even when Databricks is unreachable
        print(f"[sweep] databricks skipped: {str(e)[:160]}")
        return ""


def _dag_ids(reports: list, known: set | None = None) -> set:
    """The normalised job names this sweep produced findings for."""
    return {ledger_mod._dag_id(r, known) for r in reports if not r.error and r.findings}


def _gcs_dest(path: str, gcs_prefix: str) -> str:
    """The object this file is uploaded to."""
    return f"{gcs_prefix.rstrip('/')}/{os.path.basename(path)}"


def _published_ref(path: str, gcs_prefix: str, landed: list[str]) -> str:
    """What to cite: the GCS copy once it has landed, else the local path, marked unpublished."""
    if not gcs_prefix:
        return path
    dest = _gcs_dest(path, gcs_prefix)
    return dest if dest in landed else f"{path} (upload failed, local to the run)"


def publish(files: list[str], gcs_prefix: str) -> list[str]:
    """Copy the sweep's artifacts to GCS. Returns what landed; never raises, including on timeout.

    A runner has no repo to commit to and should not have one - keeping its GitHub identity
    read-only is the point. GCS is where the outputs live for anyone downstream.
    """
    if not gcs_prefix:
        return []
    landed = []
    for f in files:
        if not f or not os.path.exists(f):
            continue
        dest = _gcs_dest(f, gcs_prefix)
        try:
            r = subprocess.run([*_GSUTIL, "cp", f, dest], capture_output=True, timeout=300)
        except subprocess.TimeoutExpired:
            print(f"[sweep] upload timed out {dest}")
            continue
        if r.returncode == 0:
            landed.append(dest)
        else:
            print(f"[sweep] upload failed {dest}: {r.stderr.decode()[:160]}")
    return landed


def run(paths: list[str], date: str, source: str = "", airflow_base: str = "",
        outdir: str = OUTDIR, ledger_path: str = ledger_mod.LEDGER,
        gcs_prefix: str = "", complete: bool = True) -> dict:
    """Crawl, record, report. Returns the paths written and the headline counts.

    `complete=False` says the acquisition step did not deliver the whole fleet (some downloads
    failed). The sweep still reports what it found, but it will not conclude that anything
    stopped firing, because on a partial sweep absence is not evidence.
    """
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

    # Runs first: the ledger resolves job names against the DAG ids it enumerates.
    cov, known = None, None
    if airflow_base:
        try:
            cov = (cov_mod.collect_local(date) if airflow_base == "local"
                   else cov_mod.collect(airflow_base, date))
            known = cov.dag_ids_including_paused or None
        except Exception as e:
            print(f"[sweep] coverage skipped: {str(e)[:160]}")
            cov = None

    entries, delta = [], ledger_mod.Delta()
    ledger_note = ""
    if airflow_base and known is None:
        ledger_note = ("coverage unavailable, so without the DAG-id set the same job keys "
                       "differently and would read as new")
        print(f"[sweep] ledger skipped: {ledger_note}")
    else:
        # Ids the ledger already keyed hold a job steady whenever coverage's set is short.
        known = (known or set()) | {e["dag_id"] for e in ledger_mod.read(ledger_path)
                                    if e.get("dag_id")}
        try:
            entries = ledger_mod.record(reports, date, path=ledger_path, known=known,
                                        complete=complete)
            delta = ledger_mod.delta(entries)
        except Exception as e:  # a ledger fault must not lose the backlog
            ledger_note = f"ledger step failed: {str(e)[:160]}"
            print(f"[sweep] {ledger_note}")

    coverage_path = ""
    if cov is not None:
        coverage_path = os.path.join(outdir, f"optimizer_coverage_{date}.md")
        with open(coverage_path, "w") as fh:
            fh.write(cov_mod.render(cov, _dag_ids(reports, known),
                                    _rendered_dags(entries, delta, scored, known)))

    dbx_path = ""
    dbx = _databricks_report()
    if dbx:
        dbx_path = os.path.join(outdir, f"optimizer_databricks_{date}.md")
        with open(dbx_path, "w") as fh:
            fh.write(dbx)

    savings_path, savings_note = "", ""
    if ledger_note == "":
        try:
            usd_rate = float(os.environ.get("OPTIMIZER_USD_PER_EXEC_H", ""))
        except ValueError:
            usd_rate = None
        s = ledger_mod.savings(ledger_path, today=date, usd_per_exec_h=usd_rate)
        savings_path = os.path.join(outdir, "optimizer_savings.md")
        with open(savings_path, "w") as fh:
            fh.write(ledger_mod.render_savings(s))
        if s["rows"]:
            savings_note = ledger_mod.savings_headline(s)

    # The digest cites the other files, so they are uploaded before it is written.
    published = publish([backlog, coverage_path, ledger_path, dbx_path, savings_path], gcs_prefix)
    if cov is not None:
        cov.report_path = _published_ref(coverage_path, gcs_prefix, published)

    notes = []
    if not complete:
        notes.append("Partial sweep: some event logs could not be downloaded, so nothing is "
                     "reported as resolved this run.")
    if ledger_note:
        notes.append(f"No change tracking this run: {ledger_note}.")
    if dbx_path:
        notes.append(f"Databricks cost: `{_published_ref(dbx_path, gcs_prefix, published)}`")
    if savings_note:
        notes.append(f"{savings_note}. Log: "
                     f"`{_published_ref(savings_path, gcs_prefix, published)}`")

    text = digest_mod.render(delta, scanned=len(scored), findings=findings, high=high, date=date,
                             coverage=cov,
                             backlog_path=_published_ref(backlog, gcs_prefix, published))
    for note in notes:
        text += f"\n\n_{note}_"
    digest_path = os.path.join(outdir, f"optimizer_digest_{date}.md")
    with open(digest_path, "w") as fh:
        fh.write(digest_mod.render_plain(text))

    published += publish([digest_path], gcs_prefix)
    parent, replies = digest_mod.blocks(
        delta, scanned=len(scored), findings=findings, high=high, date=date, coverage=cov,
        backlog_path=_published_ref(backlog, gcs_prefix, published), notes=tuple(notes))
    delivery = notify_mod.deliver_thread(parent, replies)
    if delivery.get("error"):
        print(f"[sweep] slack post failed: {delivery['error']}")

    return {
        "backlog": backlog, "digest": digest_path,
        "coverage": coverage_path, "databricks": dbx_path,
        "scanned": len(scored), "findings": findings, "high": high,
        "ledger_entries": len(entries), "slack": text, "published": published,
        "complete": complete, "ledger_note": ledger_note, "delivery": delivery,
    }


def main() -> None:
    """CLI entry point - the daily cron calls this once the downloads are on disk."""
    ap = argparse.ArgumentParser(description="Run one optimizer sweep over downloaded logs.")
    ap.add_argument("paths", nargs="+", help="event-log dirs or globs")
    ap.add_argument("--date", required=True)
    ap.add_argument("--source", default="", help="provenance line for the backlog header")
    ap.add_argument("--airflow-base", default="",
                    help='Airflow API base ending in /api/v2, or "local" to parse the DAG '
                         "bundle in-process; omit to skip coverage")
    ap.add_argument("--outdir", default=OUTDIR)
    ap.add_argument("--ledger", default=ledger_mod.LEDGER)
    ap.add_argument("--partial", action="store_true",
                    help="acquisition was incomplete; record findings but resolve nothing")
    ap.add_argument("--gcs-prefix", default=os.environ.get("OPTIMIZER_GCS_PREFIX", ""),
                    help="gs://... to publish the artifacts to; omit to keep them local only")
    args = ap.parse_args()

    out = run(args.paths, args.date, args.source, args.airflow_base, args.outdir,
              args.ledger, args.gcs_prefix, complete=not args.partial)
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
