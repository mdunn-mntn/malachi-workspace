"""AUDI-1210 — advertisers spending with no measurable site visits, for pixel triage."""
import csv
import sys

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

T = "/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1210_zero_visit_rate_advertisers"
SRC = "/Users/malachi/Developer/work/mntn/workspace/tickets/incr_75_eligible_advertisers/outputs/incr_75_advertiser_metrics.csv"
GENERATED = "2026-08-19"


def f(r, k):
    try:
        return float(r[k])
    except (TypeError, ValueError):
        return 0.0


rows = list(csv.DictReader(open(SRC)))


def flag(r):
    vis = int(r["visiting_ips_30d"])
    vr = f(r, "p_visit")
    if vis == 0:
        return "No visits at all"
    if vr < 0.001:
        return "Under 0.1%"
    return "0.1% to 0.5%"


sub = [r for r in rows if f(r, "p_visit") < 0.005]
sub.sort(key=lambda r: -f(r, "spend_30d"))
df = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "What we see": flag(r),
    "30-day spend": f(r, "spend_30d"),
    "Impressions": int(f(r, "impressions_30d")),
    "Served IPs": int(f(r, "distinct_ips_30d")),
    "Visiting IPs": int(r["visiting_ips_30d"]),
    "Visit rate": f(r, "p_visit"),
    "Converting IPs": int(r["converting_ips_30d"]),
    "Industry": (r["vertical_buckets"] or "").split(" | ")[0] or None,
} for r in sub])

n0 = sum(1 for r in sub if int(r["visiting_ips_30d"]) == 0)
spend0 = sum(f(r, "spend_30d") for r in sub if int(r["visiting_ips_30d"]) == 0)
big = [r for r in sub if f(r, "spend_30d") >= 10_000]
tot = sum(f(r, "spend_30d") for r in sub)

df_sum = pd.DataFrame([
    {"What we see": "No visits at all", "Advertisers": n0,
     "30-day spend": spend0,
     "Over $10k": sum(1 for r in sub if int(r["visiting_ips_30d"]) == 0 and f(r, "spend_30d") >= 10_000)},
    {"What we see": "Under 0.1%",
     "Advertisers": sum(1 for r in sub if 0 < f(r, "p_visit") < 0.001),
     "30-day spend": sum(f(r, "spend_30d") for r in sub if 0 < f(r, "p_visit") < 0.001),
     "Over $10k": sum(1 for r in sub if 0 < f(r, "p_visit") < 0.001 and f(r, "spend_30d") >= 10_000)},
    {"What we see": "0.1% to 0.5%",
     "Advertisers": sum(1 for r in sub if 0.001 <= f(r, "p_visit") < 0.005),
     "30-day spend": sum(f(r, "spend_30d") for r in sub if 0.001 <= f(r, "p_visit") < 0.005),
     "Over $10k": sum(1 for r in sub if 0.001 <= f(r, "p_visit") < 0.005 and f(r, "spend_30d") >= 10_000)},
])

FM = {"30-day spend": FMT.USD, "Impressions": FMT.INT, "Served IPs": FMT.INT,
      "Visiting IPs": FMT.INT, "Visit rate": FMT.PCT2, "Converting IPs": FMT.INT,
      "Advertiser ID": "0"}

wb = MntnWorkbook(
    title="Advertisers With No Measurable Visits",
    ticket="AUDI-1210",
    subtitle="Live advertisers whose served IPs almost never show a site visit, ranked by spend",
    period="Trailing 30 days to 2026-08-19",
    generated=GENERATED,
)

wb.table(
    "Check these first", df[df["30-day spend"] >= 10_000],
    finding=f"{len(big)} advertisers spent $10k or more in 30 days and show almost no site visits",
    method="Visit rate is the share of served IPs seen visiting the advertiser's site in the same 30 days. Ranked by spend. See Read me.",
    formats=FM, heat={"30-day spend": "high"}, kind="headline",
    toc="The ones worth checking: $10k or more in spend",
    query="audi_1210_zero_visit_rate_advertisers.sql",
)

wb.table(
    "Summary", df_sum,
    finding=f"{len(df)} live advertisers sit under a 0.5% visit rate, together spending ${tot:,.0f} over 30 days",
    method="Three bands of severity. An advertiser appears in exactly one.",
    formats={"Advertisers": FMT.INT, "30-day spend": FMT.USD, "Over $10k": FMT.INT},
    kind="data", toc="How many advertisers, and how much spend",
)

wb.table(
    "Full list", df,
    finding=f"All {len(df)} advertisers under a 0.5% visit rate, largest spender first",
    method="Every live, non-test advertiser that served an impression in the trailing 30 days and sits under 0.5%.",
    formats=FM, kind="data", toc="The full list",
    query="audi_1210_zero_visit_rate_advertisers.sql",
)

wb.glossary(
    "Read me",
    intro="One question: these advertisers are spending, so why do we almost never see anyone visit their site?",
    rows=[
        ("Served IPs", "Distinct IP addresses that received at least one impression in the last 30 days."),
        ("Visiting IPs", "Of those, the ones later seen on the advertiser's own website."),
        ("Visit rate", "Visiting IPs divided by served IPs. The eligible-cohort median is about 2%."),
        ("How a visit is recorded", "A pixel on the advertiser's site fires and writes a row to clickpass_log, keyed to their advertiser id. No pixel row, no visit, regardless of what actually happened on their site."),
        ("", ""),
        ("What to check", ""),
        ("No visits at all", "Zero visiting IPs against real impressions. The pixel is almost certainly not reporting."),
        ("Under 0.1%", "A trickle. Consistent with a pixel on one page only, or one that fires on a fraction of sessions."),
        ("0.1% to 0.5%", "Low but not impossible. Some verticals genuinely convert offline. Included so the cut is not arbitrary."),
        ("Converting IPs", "Same idea for conversions. Zero visits AND zero conversions points at the pixel rather than at performance."),
    ],
)

wb.notes(
    "Method & caveats",
    intro="What this list is and is not.",
    blocks=[
        ("This is a measurement flag, not a performance verdict",
         "A zero here says we cannot see visits, not that the advertising failed. Several of these are utilities, grocery and healthcare brands whose customers may never transact on a tracked page."),
        ("The likeliest cause is the pixel",
         "Visits come only from the advertiser's own site pixel, keyed to their advertiser id. If it is missing, blocked, or firing under a different id, the visit rate reads zero no matter what happened."),
        ("Low spend is included but is not the point",
         "The cut is the visit rate, not spend. Small advertisers with a handful of impressions can read zero by chance. Start with the $10k-and-up sheet."),
        ("Why it matters beyond reporting",
         "A zero visit rate makes an advertiser unmeasurable for incrementality: it cannot be screened for a lift test and it cannot be shown a result. 479 advertisers were dropped from the lift-test screen for exactly this reason."),
    ],
)

wb.sql_dir("Queries", f"{T}/queries",
           note="The query behind this list. It runs standalone against BigQuery.")

wb.cover(takeaways=[
    f"{len(big)} advertisers spent $10k or more in the last 30 days and show almost no site visits.",
    f"{n0} show no visits at all, against ${spend0:,.0f} of spend.",
    "Visits are only recorded by the advertiser's own site pixel, so the likeliest cause is pixel reporting.",
])

path = wb.save_drive("AUDI-1210", "Advertisers With No Measurable Visits")
print("wrote", path)
