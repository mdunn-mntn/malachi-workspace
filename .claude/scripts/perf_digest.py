#!/usr/bin/env python3
"""Deterministic aggregation over knowledge/bq/bq_perf_log.jsonl for the perf-analyst.

READ-ONLY: prints GitHub-markdown tables (ranked, most-costly on top). Never edits docs — the
perf-analyst agent reads this output and decides what to append where. A script does the counting;
a model does the judgement.

Usage:
  perf_digest.py [--log PATH] [--since YYYY-MM-DD] [--table ds.table] [--top N]
                 --mode {by-table,offenders,repeats,phase-accuracy,all}
"""

import argparse
import collections
import json
import os


def _clean(ref):
    """SQLMesh physical `sqlmesh__ds.ds__table__hash` -> clean `ds.table`; pass others through.
    The table itself can contain `__` (agg__daily_sum_by_campaign), so it is everything between the
    first segment (schema) and the last (fingerprint)."""
    ds, _, tbl = ref.partition(".")
    if ds.startswith("sqlmesh__"):
        parts = tbl.split("__")
        if len(parts) >= 3:
            return f"{parts[0]}.{'__'.join(parts[1:-1])}"
        if len(parts) == 2:
            return f"{parts[0]}.{parts[1]}"
    return ref


def load(log, since, only_table):
    rows = []
    if not os.path.exists(log):
        return rows
    with open(log, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            # Normalize the workspace perf-log schema -> the fields this tool reads, and derive
            # sql_tables from referenced_tables for historical records that predate the sql_tables field.
            if not r.get("ts"):
                r["ts"] = r.get("timestamp", "")
            if r.get("wall_ms") is None:
                r["wall_ms"] = r.get("elapsed_ms")
            if not r.get("sql_tables"):
                r["sql_tables"] = list(
                    dict.fromkeys(_clean(t) for t in (r.get("referenced_tables") or []))
                )
            if since and (r.get("ts", "") < since):
                continue
            if only_table and only_table not in (r.get("sql_tables") or []):
                continue
            rows.append(r)
    return rows


def pctl(vals, p):
    vals = sorted(v for v in vals if isinstance(v, (int, float)))
    if not vals:
        return None
    k = (len(vals) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(vals) - 1)
    return round(vals[lo] + (vals[hi] - vals[lo]) * (k - lo), 3)


def by_table(rows, top):
    agg = collections.defaultdict(list)
    for r in rows:
        for t in r.get("sql_tables") or []:
            agg[t].append(r)
    out = [
        "## Cost by table",
        "",
        "| table | n | gb_billed p50 | gb_billed p90 | slot_s p50 | cache-hit % | prune ratio | last_seen |",
        "|---|--:|--:|--:|--:|--:|--:|---|",
    ]
    ranked = sorted(
        agg.items(), key=lambda kv: -(pctl([x.get("gb_billed") for x in kv[1]], 90) or 0)
    )
    for t, rs in ranked[:top]:
        gb = [x.get("gb_billed") for x in rs]
        slot = [(x.get("slot_ms") or 0) / 1000.0 for x in rs]
        ch = sum(1 for x in rs if x.get("cache_hit"))
        # prune ratio = actual/estimate; <1 means dry-run over-estimated (good pruning), ~1 means it scanned what it predicted
        ratios = [
            (x["gb_billed"] / x["est_gb"])
            for x in rs
            if isinstance(x.get("gb_billed"), (int, float)) and x.get("est_gb")
        ]
        pr = round(sum(ratios) / len(ratios), 2) if ratios else None
        out.append(
            f"| `{t}` | {len(rs)} | {pctl(gb, 50)} | {pctl(gb, 90)} | {pctl(slot, 50)} | "
            f"{round(100 * ch / len(rs))} | {pr if pr is not None else '—'} | {max(x.get('ts', '') for x in rs)} |"
        )
    if len(agg) == 0:
        out.append("| _(no queries logged yet)_ | | | | | | | |")
    return "\n".join(out)


def offenders(rows, top):
    ok = [r for r in rows if isinstance(r.get("gb_billed"), (int, float))]
    ok.sort(key=lambda r: -r["gb_billed"])
    out = [
        "## Top offenders (single most expensive runs)",
        "",
        "| gb_billed | slot_s | tables | ticket | label | ts |",
        "|--:|--:|---|---|---|---|",
    ]
    for r in ok[:top]:
        out.append(
            f"| {r['gb_billed']} | {round((r.get('slot_ms') or 0) / 1000, 1)} | "
            f"{', '.join('`' + t + '`' for t in (r.get('sql_tables') or [])) or '—'} | "
            f"{r.get('ticket') or '—'} | {(r.get('label') or '—')[:48]} | {r.get('ts', '')} |"
        )
    if not ok:
        out.append("| _(none)_ | | | | | |")
    return "\n".join(out)


def repeats(rows, top):
    # identical SQL run more than once with at least one cache miss = a materialization candidate
    agg = collections.defaultdict(list)
    for r in rows:
        # the wrapper logs sql_sha256 (older/legacy records have no sha and fall under "" -> dropped by the `if h` guard below)
        agg[r.get("sql_sha256", "")].append(r)
    cands = [
        (h, rs)
        for h, rs in agg.items()
        if h and len(rs) > 1 and any(not x.get("cache_hit") for x in rs)
    ]
    cands.sort(key=lambda kv: -(len(kv[1]) * (pctl([x.get("gb_billed") for x in kv[1]], 50) or 0)))
    out = [
        "## Repeated queries (materialization candidates)",
        "",
        "| runs | cache-miss | gb_billed p50 | tables | sample label | sha256 |",
        "|--:|--:|--:|---|---|---|",
    ]
    for h, rs in cands[:top]:
        miss = sum(1 for x in rs if not x.get("cache_hit"))
        out.append(
            f"| {len(rs)} | {miss} | {pctl([x.get('gb_billed') for x in rs], 50)} | "
            f"{', '.join('`' + t + '`' for t in (rs[0].get('sql_tables') or [])) or '—'} | "
            f"{(rs[0].get('label') or '—')[:40]} | {h[:12]} |"
        )
    if not cands:
        out.append("| _(none)_ | | | | | |")
    return "\n".join(out)


def phase_accuracy(rows, top):
    # pair a sample run with its full run by (ticket,label); how well did sample predict full?
    by_key = collections.defaultdict(dict)
    for r in rows:
        key = (r.get("ticket", ""), r.get("label", ""))
        ph = r.get("phase")
        gb = r.get("gb_billed")
        # only REAL scans predict cost: skip cache hits (0-byte) and zero/non-numeric bytes.
        # otherwise a cached 0-byte re-run overwrites the true full scan (last-write-wins) and
        # the ratio reads 0.0 — the most-recent real run per phase should win instead.
        if (
            ph in ("sample", "full")
            and isinstance(gb, (int, float))
            and gb > 0
            and not r.get("cache_hit")
        ):
            by_key[key][ph] = r
    out = [
        "## Sample→full accuracy (did the sample predict the full run?)",
        "",
        "| ticket | label | sample gb | full gb | full/sample |",
        "|---|---|--:|--:|--:|",
    ]
    pairs = [(k, v) for k, v in by_key.items() if "sample" in v and "full" in v]
    pairs.sort(key=lambda kv: -(kv[1]["full"]["gb_billed"]))
    for (tk, lb), v in pairs[:top]:
        s, fu = v["sample"]["gb_billed"], v["full"]["gb_billed"]
        ratio = round(fu / s, 1) if s else "—"
        out.append(f"| {tk or '—'} | {(lb or '—')[:40]} | {s} | {fu} | {ratio} |")
    if not pairs:
        out.append("| _(no matched sample/full label pairs)_ | | | | |")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    here = os.path.dirname(os.path.abspath(__file__))
    ap.add_argument(
        "--log", default=os.path.join(here, "..", "..", "knowledge", "bq_perf_log.jsonl")
    )
    ap.add_argument("--since", default="")
    ap.add_argument("--table", default="")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument(
        "--mode",
        required=True,
        choices=["by-table", "offenders", "repeats", "phase-accuracy", "all"],
    )
    a = ap.parse_args()
    rows = load(os.path.normpath(a.log), a.since, a.table)
    blocks = {
        "by-table": by_table,
        "offenders": offenders,
        "repeats": repeats,
        "phase-accuracy": phase_accuracy,
    }
    print(
        f"<!-- perf_digest {a.mode} · {len(rows)} records"
        f"{' since ' + a.since if a.since else ''}{' · ' + a.table if a.table else ''} -->\n"
    )
    order = ["by-table", "offenders", "repeats", "phase-accuracy"] if a.mode == "all" else [a.mode]
    print("\n\n".join(blocks[m](rows, a.top) for m in order))


if __name__ == "__main__":
    main()
