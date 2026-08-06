#!/usr/bin/env python3
"""PS-8572 Task C: classify the 2,290 matchback orders against the CRM-exclusion timeline.

Subcommands:
  gen-sql   read matchback CSV -> write 4 membership pull queries (queries/ps_8572_05*.sql)
  merge     parse the 4 bq prettyjson result files -> outputs/ps_8572_matchback_ip_membership.csv
  classify  join orders to membership flags -> classified CSV + summary JSON
"""
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

TICKET_DIR = Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ps_8572_lovepop_repeat_customers")
MATCHBACK_CSV = TICKET_DIR / "outputs" / "ps_8572_matchback.csv"
MEMBERSHIP_CSV = TICKET_DIR / "outputs" / "ps_8572_matchback_ip_membership.csv"
CLASSIFIED_CSV = TICKET_DIR / "outputs" / "ps_8572_matchback_classified.csv"
SUMMARY_JSON = TICKET_DIR / "outputs" / "ps_8572_matchback_classification_summary.json"

SNAPSHOTS = [
    ("ds4_0630", 4, "2026-06-30", "05a"),
    ("ds47_0702", 47, "2026-07-02", "05b"),
    ("ds47_0717", 47, "2026-07-17", "05c"),
    ("ds47_0804", 47, "2026-08-04", "05d"),
]

# Exclusion timeline boundaries (UTC), verified from audience_segment_archives
CLAUSE_ADDED = datetime(2026, 6, 30, 2, 8, 18)      # 32697 first attached (DS4), P0 -> P1
GRACE1_END = CLAUSE_ADDED + timedelta(days=3)        # 2026-07-03 02:08:18
T_28594_ADDED = datetime(2026, 7, 16, 18, 17, 2)     # 28594 first attached, P1 -> P2
GRACE2_END = T_28594_ADDED + timedelta(days=3)       # 2026-07-19 18:17:02
EDT_OFFSET = timedelta(hours=4)                      # timestamps in CSV are EDT = UTC-4

SAMPLE_ORDERS = [
    "12181567668297", "12173057753161", "12175650553929", "12186375454793",
    "12197698535497", "12202725376073", "12189170991177", "12163229089865",
    "12181317353545", "12206853849161",
]


def load_orders():
    with open(MATCHBACK_CSV) as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["ip"] = r["ip"].strip()
        r["impression_utc"] = datetime.strptime(r["impression_time"], "%m-%d-%Y %I:%M:%S %p") + EDT_OFFSET
    return rows


def distinct_ips(rows):
    return sorted({r["ip"] for r in rows})


def gen_sql():
    ips = distinct_ips(load_orders())
    arr = ",".join(f"'{ip}'" for ip in ips)
    for tag, ds, dt, prefix in SNAPSHOTS:
        sql = (
            f"-- PS-8572 Task C membership pull: DS{ds} dt='{dt}' for {len(ips)} matchback conversion IPs\n"
            f"-- dt + data_source_id are hive partition keys; literals only. Pre-approved partition scan.\n"
            f"SELECT\n"
            f"  t.ip,\n"
            f"  LOGICAL_OR(dscid.element = 28594) AS m28594,\n"
            f"  LOGICAL_OR(dscid.element = 32697) AS m32697\n"
            f"FROM `dw-main-bronze.external.ipdsc__v1` t,\n"
            f"  UNNEST(t.data_source_category_ids.list) AS dscid\n"
            f"WHERE t.data_source_id = {ds}\n"
            f"  AND t.dt = '{dt}'\n"
            f"  AND dscid.element IN (28594, 32697)\n"
            f"  AND t.ip IN UNNEST([{arr}])\n"
            f"GROUP BY 1\n"
        )
        out = TICKET_DIR / "queries" / f"ps_8572_{prefix}_membership_{tag}.sql"
        out.write_text(sql)
        print(f"wrote {out} ({len(ips)} ips)")


def _to_bool(v):
    if isinstance(v, bool):
        return v
    return str(v).lower() == "true"


def merge():
    flags = defaultdict(dict)
    for tag, _ds, _dt, prefix in SNAPSHOTS:
        path = TICKET_DIR / "outputs" / f"ps_8572_{prefix}_membership_{tag}.json"
        data = json.loads(path.read_text())
        for row in data:
            flags[row["ip"]][f"{tag}_28594"] = _to_bool(row["m28594"])
            flags[row["ip"]][f"{tag}_32697"] = _to_bool(row["m32697"])
    ips = distinct_ips(load_orders())
    cols = ["ip"] + [f"{tag}_{u}" for tag, _, _, _ in SNAPSHOTS for u in ("28594", "32697")]
    with open(MEMBERSHIP_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for ip in ips:
            w.writerow([ip] + [int(flags[ip].get(c, False)) for c in cols[1:]])
    n_any = sum(1 for ip in ips if any(flags[ip].values()))
    print(f"wrote {MEMBERSHIP_CSV}: {len(ips)} ips, {n_any} with membership at >=1 snapshot")


def classify():
    orders = load_orders()
    mem = {}
    with open(MEMBERSHIP_CSV) as f:
        for row in csv.DictReader(f):
            mem[row["ip"]] = {k: v == "1" for k, v in row.items() if k != "ip"}

    out_rows = []
    for r in orders:
        m = mem[r["ip"]]
        imp = r["impression_utc"]
        m28594_any = any(m[f"{t}_28594"] for t, _, _, _ in SNAPSHOTS)
        m32697_any = any(m[f"{t}_32697"] for t, _, _, _ in SNAPSHOTS)
        member_0702 = m["ds47_0702_28594"] or m["ds47_0702_32697"]
        member_0717 = m["ds47_0717_28594"] or m["ds47_0717_32697"]
        member_0804 = m["ds47_0804_28594"] or m["ds47_0804_32697"]
        any_member = m28594_any or m32697_any
        note = ""

        if imp < CLAUSE_ADDED:
            bucket = "A_pre_exclusion"
        elif imp < T_28594_ADDED:  # P1
            if m28594_any and not m32697_any:
                bucket = "B_28594_gap"
            elif imp > GRACE1_END and m["ds47_0702_32697"]:
                bucket = "B2_32697_candidate"
            elif not any_member:
                bucket = "D_unmatched"
            elif not member_0702:
                bucket = "P1_nonmember"
                later = []
                if m["ds4_0630_28594"] or m["ds4_0630_32697"]:
                    later.append("ds4_0630")
                if member_0717:
                    later.append("ds47_0717")
                if member_0804:
                    later.append("ds47_0804")
                note = "later_matched:" + "+".join(later) if later else ""
            else:
                bucket = "other_unclassified"
                note = "P1_member_0702_no_rule (e.g. 32697 member, imp inside 3d grace, or both-list member)"
        else:  # P2
            if imp > GRACE2_END and member_0717:
                bucket = "C_post_attach_candidate"
            elif imp > GRACE2_END and not member_0717 and member_0804:
                bucket = "C_later_joined"
            elif imp <= GRACE2_END and member_0717:
                bucket = "C_in_grace"
            elif not any_member:
                bucket = "D_unmatched"
            else:
                bucket = "other_unclassified"
                note = "P2_member_somewhere_but_no_rule (not member at 0717; member only at earlier snapshot or in-grace+0804-only)"

        exp28 = m["ds47_0702_28594"] and not m["ds4_0630_28594"]
        exp32 = m["ds47_0702_32697"] and not m["ds4_0630_32697"]
        out_rows.append({
            "order_id": r["order_id"],
            "ip": r["ip"],
            "impression_time_edt": r["impression_time"],
            "impression_time_utc": imp.strftime("%Y-%m-%d %H:%M:%S"),
            "conversion_time_edt": r["conversion_time"],
            "bucket": bucket,
            "note": note,
            **{f"{t}_{u}": int(m[f"{t}_{u}"]) for t, _, _, _ in SNAPSHOTS for u in ("28594", "32697")},
            "m28594_any": int(m28594_any),
            "m32697_any": int(m32697_any),
            "final_0804_28594": int(m["ds47_0804_28594"]),
            "final_0804_32697": int(m["ds47_0804_32697"]),
            "ds47_expansion_evidence_28594": int(exp28),
            "ds47_expansion_evidence_32697": int(exp32),
        })

    with open(CLASSIFIED_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)

    n = len(out_rows)
    buckets = defaultdict(list)
    for r in out_rows:
        buckets[r["bucket"]].append(r)
    bucket_counts = {b: {"n": len(v), "pct": round(100 * len(v) / n, 2)} for b, v in sorted(buckets.items(), key=lambda kv: -len(kv[1]))}

    ips = sorted(mem)
    fs = {
        "n_distinct_ips": len(ips),
        "member_28594_at_0804": sum(mem[ip]["ds47_0804_28594"] for ip in ips),
        "member_32697_at_0804": sum(mem[ip]["ds47_0804_32697"] for ip in ips),
        "member_both_at_0804": sum(mem[ip]["ds47_0804_28594"] and mem[ip]["ds47_0804_32697"] for ip in ips),
        "member_either_at_0804": sum(mem[ip]["ds47_0804_28594"] or mem[ip]["ds47_0804_32697"] for ip in ips),
        "member_neither_at_0804": sum(not (mem[ip]["ds47_0804_28594"] or mem[ip]["ds47_0804_32697"]) for ip in ips),
        "member_neither_at_any_snapshot": sum(not any(mem[ip].values()) for ip in ips),
    }

    per_snapshot = {}
    for tag, _, dt, _ in SNAPSHOTS:
        per_snapshot[tag] = {
            "dt": dt,
            "28594": sum(mem[ip][f"{tag}_28594"] for ip in ips),
            "32697": sum(mem[ip][f"{tag}_32697"] for ip in ips),
            "either": sum(mem[ip][f"{tag}_28594"] or mem[ip][f"{tag}_32697"] for ip in ips),
        }

    order_idx = {r["order_id"]: r for r in out_rows}
    samples = {oid: ({k: order_idx[oid][k] for k in ("bucket", "note", "ip", "impression_time_utc", "final_0804_28594", "final_0804_32697", "m28594_any", "m32697_any")} if oid in order_idx else "NOT_IN_MATCHBACK_CSV") for oid in SAMPLE_ORDERS}

    rep_fields = ["order_id", "ip", "impression_time_utc", "conversion_time_edt", "note",
                  "ds4_0630_28594", "ds4_0630_32697", "ds47_0702_28594", "ds47_0702_32697",
                  "ds47_0717_28594", "ds47_0717_32697", "final_0804_28594", "final_0804_32697"]
    representatives = {b: [{k: r[k] for k in rep_fields} for r in v[:5]] for b, v in buckets.items()}

    exp_summary = {
        "ips_28594_ds47_0702_not_ds4_0630": sum(1 for ip in ips if mem[ip]["ds47_0702_28594"] and not mem[ip]["ds4_0630_28594"]),
        "ips_28594_both_ds4_0630_and_ds47_0702": sum(1 for ip in ips if mem[ip]["ds47_0702_28594"] and mem[ip]["ds4_0630_28594"]),
        "ips_32697_ds47_0702_not_ds4_0630": sum(1 for ip in ips if mem[ip]["ds47_0702_32697"] and not mem[ip]["ds4_0630_32697"]),
        "ips_32697_both_ds4_0630_and_ds47_0702": sum(1 for ip in ips if mem[ip]["ds47_0702_32697"] and mem[ip]["ds4_0630_32697"]),
        "caveat": "DS4 sampled only at dt=2026-06-30; DS47-only vs also-DS4 is distinguishable only against that snapshot. 32697 ingested 2026-06-30 02:06 UTC so the 6/30 DS4 partition may pre-date its first build.",
    }

    summary = {
        "task": "PS-8572 Task C matchback order classification",
        "n_orders": n,
        "n_distinct_ips": len(ips),
        "boundaries_utc": {
            "clause_added_32697": CLAUSE_ADDED.isoformat(),
            "grace1_end": GRACE1_END.isoformat(),
            "28594_added": T_28594_ADDED.isoformat(),
            "grace2_end": GRACE2_END.isoformat(),
        },
        "bucket_counts": bucket_counts,
        "final_state_membership_at_0804": fs,
        "per_snapshot_membership_of_2154_ips": per_snapshot,
        "graph_expansion_evidence": exp_summary,
        "client_flagged_samples": samples,
        "representative_rows_per_bucket": representatives,
    }
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2))
    print(f"wrote {CLASSIFIED_CSV} ({n} rows) and {SUMMARY_JSON}")
    print(json.dumps({"bucket_counts": bucket_counts, "final_state": fs}, indent=2))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"gen-sql": gen_sql, "merge": merge, "classify": classify}.get(cmd, lambda: sys.exit("usage: gen-sql|merge|classify"))()
