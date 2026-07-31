#!/usr/bin/env python3
"""bq_verify.py — the provenance / trust card: "how did you get this number?"

Given a ticket, a label substring, or a sql_sha256 prefix, print the perf-log record(s) that
produced it: the exact SQL fingerprint + preview, the BigQuery job_id (which recovers the FULL SQL
via `bq show`), the git commit the repo was at, the cost, and when it ran.

READ-ONLY: reads knowledge/bq_perf_log.jsonl only — runs no BigQuery, bills nothing, changes nothing.
This is the "pin-and-show" answer to the trust problem — it does NOT re-run and assert (BQ tables
mutate via TTL / late data / SQLMesh rebuilds, so re-run-and-match would false-alarm constantly).

Usage:
  bq_verify.py <ticket | label-substring | sql_sha256-prefix> [--limit N]     # default N=5, newest first
"""

import argparse, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.normpath(os.path.join(HERE, "..", "..", "knowledge", "bq_perf_log.jsonl"))


def load(q):
    ql = q.lower()
    hits = []
    if not os.path.exists(LOG):
        return hits
    with open(LOG, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            tkt = str(r.get("ticket") or "").lower()
            lbl = str(r.get("label") or "").lower()
            sha = str(r.get("sql_sha256") or "")
            if ql in tkt or ql in lbl or (q and sha.startswith(q)):
                hits.append(r)
    return hits


def card(r):
    ts = r.get("timestamp") or r.get("ts") or "—"
    job = r.get("job_id") or "—"
    bare = job.split(":", 1)[1] if ":" in job else job
    gb_b = r.get("gb_billed")
    gb_p = r.get("gb_processed")
    tables = ", ".join(r.get("sql_tables") or []) or "—"
    out = []
    out.append(f"── {r.get('ticket') or '—'}  ·  {r.get('label') or '—'} ──")
    out.append(f"  when      : {ts}")
    out.append(
        f"  cost      : {gb_b if gb_b is not None else '—'} GB billed"
        f" ({gb_p if gb_p is not None else '—'} processed)"
        f"  cache={r.get('cache_hit')}  phase={r.get('phase') or '—'}"
    )
    out.append(f"  git commit: {r.get('git_commit') or '— (pre-provenance run)'}")
    out.append(f"  sql sha256: {r.get('sql_sha256') or '— (pre-provenance run)'}")
    out.append(
        f"  sql        : {r.get('sql_preview') or '— (not captured; use full-SQL cmd below)'}"
    )
    out.append(f"  tables     : {tables}")
    out.append(f"  job_id     : {job}")
    out.append(
        f"  FULL SQL   : bq show --format=prettyjson -j {bare} | jq -r '.configuration.query.query'"
    )
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", help="ticket | label substring | sql_sha256 prefix")
    ap.add_argument("--limit", type=int, default=5)
    a = ap.parse_args()
    hits = load(a.query)
    if not hits:
        print(
            f"no perf-log record matches {a.query!r} (ticket / label / sql_sha256). "
            f"Was the query run through bq_run.sh?",
            file=sys.stderr,
        )
        return 1
    print(f"# {len(hits)} match(es) for {a.query!r} — showing newest {min(a.limit, len(hits))}\n")
    print("\n\n".join(card(r) for r in hits[-a.limit :][::-1]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
