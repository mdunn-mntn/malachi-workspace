"""Pull eligibility metrics for one advertiser at their last-active window.

Two-step by design: BigQuery cannot prune partitions on a date derived from a
subquery, so step 1 resolves the window and step 2 substitutes it as a literal.
Collapsing these into one statement scans the whole table.

  python3 audi_xxx_run_metrics.py <advertiser_id> [--window-end YYYY-MM-DD]

--window-end pins the window instead of deriving it, which is how the fork is
regression-tested against a live advertiser already in INCR-75.
"""
import argparse
import csv
import io
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

TICKET = Path(__file__).resolve().parents[1]
BQ_RUN = TICKET.parents[1] / ".claude" / "scripts" / "bq_run.sh"

HISTORY_FLOOR = "2024-01-01"   # sum_by_advertiser_by_day floor
CIL_FLOOR = "2023-10-01"       # cost_impression_log floor, no TTL
WINDOW_DAYS = 30
REACH_DAYS = 56
SPEND_HIST_DAYS = 365
MAX_SCAN_GB = 25.0  # a 56d single-advertiser CIL slice is ~5GB even with clustering


def run_sql(sql, label, dry_run=False):
    args = [str(BQ_RUN), "--ticket", "AUDI", "--label", label,
            "--project_id=dw-main-silver", "--use_legacy_sql=false"]
    args += ["--dry_run"] if dry_run else ["--format=csv"]
    args.append(sql)
    p = subprocess.run(args, capture_output=True, text=True)
    if p.returncode != 0:
        sys.exit(f"query failed ({label}):\n{p.stdout}\n{p.stderr}")
    return p.stdout


def scan_gb(sql, label):
    out = run_sql(sql, label + " [dry]", dry_run=True)
    for tok in out.split():
        if tok.isdigit() and len(tok) > 6:
            return int(tok) / 1e9
    return 0.0


def render(path, **kw):
    sql = (TICKET / "queries" / path).read_text()
    for k, v in kw.items():
        sql = sql.replace("{{%s}}" % k, str(v))
    return sql


def csv_rows(out):
    lines = [l for l in out.splitlines() if l and not l.startswith(("+", "|", "Waiting"))]
    body = "\n".join(lines)
    cut = body.find("\n\n--- BQ Performance")
    if cut > 0:
        body = body[:cut]
    return list(csv.DictReader(io.StringIO(body)))


def resolve_window(aid, pinned_end):
    if pinned_end:
        end = date.fromisoformat(pinned_end)
        print(f"[info] window pinned to {end} (regression mode)")
        return end, None

    sql = render("audi_xxx_last_active.sql",
                 ADVERTISER_ID=aid, HISTORY_FLOOR=HISTORY_FLOOR,
                 TODAY=date.today().isoformat())
    rows = csv_rows(run_sql(sql, f"last-active day for advertiser {aid}"))
    if not rows or not rows[0].get("last_active_day"):
        sys.exit(f"advertiser {aid} has no delivering day since {HISTORY_FLOOR} — "
                 "nothing to screen on")
    r = rows[0]
    end = date.fromisoformat(r["last_active_day"])
    print(f"[info] last active {end} | first active {r['first_active_day']} | "
          f"{r['delivering_days']} delivering days | lifetime spend ${float(r['lifetime_spend']):,.0f}")
    lapsed = (date.today() - end).days
    print(f"[info] lapsed {lapsed} days ({lapsed/30.4:.1f} months) as of {date.today()}")
    return end, lapsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("advertiser_id", type=int)
    ap.add_argument("--window-end", default=None,
                    help="pin the window end (regression-test mode)")
    ap.add_argument("--window-start", default=None,
                    help="pin the window start too; reproduces an exact prior run")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    end, lapsed = resolve_window(a.advertiser_id, a.window_end)
    win_start = (date.fromisoformat(a.window_start) if a.window_start
                 else end - timedelta(days=WINDOW_DAYS - 1))
    win56_start = end - timedelta(days=REACH_DAYS - 1)
    hist_start = max(end - timedelta(days=SPEND_HIST_DAYS),
                     date.fromisoformat(HISTORY_FLOOR))

    if win56_start < date.fromisoformat(CIL_FLOOR):
        sys.exit(f"56d reach window starts {win56_start}, before the CIL floor {CIL_FLOOR}")

    sql = render("audi_xxx_lapsed_advertiser_metrics.sql",
                 ADVERTISER_ID=a.advertiser_id,
                 WIN_START=win_start, WIN_END=end,
                 WIN56_START=win56_start, SPEND_HIST_START=hist_start)

    gb = scan_gb(sql, f"metrics advertiser {a.advertiser_id}")
    print(f"[info] metrics window {win_start}..{end} | 56d reach from {win56_start} "
          f"| spend history from {hist_start}")
    print(f"[info] dry run: {gb:.2f} GB")
    if gb > MAX_SCAN_GB:
        sys.exit(f"aborting: {gb:.2f} GB exceeds the {MAX_SCAN_GB} GB ceiling")

    out = run_sql(sql, f"metrics advertiser {a.advertiser_id}")
    rows = csv_rows(out)
    if not rows:
        sys.exit(f"no rows for advertiser {a.advertiser_id} in {win_start}..{end}")

    dest = Path(a.out) if a.out else TICKET / "outputs" / f"audi_xxx_metrics_{a.advertiser_id}.csv"
    with open(dest, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"[ok] wrote {dest}")

    r = rows[0]
    print(f"\n{r['advertiser_name']} ({r['advertiser_id']}) — {r['vertical_buckets']}")
    print(f"  IVR  {float(r['p_visit'])*100:.2f}%   ({r['visiting_ips_30d']} visiting IPs)")
    print(f"  CVR  {float(r['p_cvr'])*100:.3f}%  ({r['converting_ips_30d']} converting IPs)")
    print(f"  CPM  ${float(r['cpm']):.2f}   imps/IP {float(r['imps_per_ip']):.1f}")
    print(f"  typical active month ${float(r['typical_active_month_spend']):,.0f} "
          f"over {r['active_months_count']} months")
    if int(r["visiting_ips_30d"]) < 100:
        print("  [warn] under 100 visiting IPs — IVR too unstable to quote (INCR-75 MIN_VISITING_IPS)")


if __name__ == "__main__":
    main()
