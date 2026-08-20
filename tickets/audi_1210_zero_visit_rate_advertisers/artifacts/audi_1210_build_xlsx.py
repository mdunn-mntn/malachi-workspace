"""AUDI-1210 — the share of each advertiser's own site traffic MNTN gets credit for."""
import csv
import sys

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

T = "/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1210_zero_visit_rate_advertisers"
GENERATED = "2026-08-19"
SPEND_FLOOR = 10_000
PEER_CUT = 0.25


def f(r, k):
    try:
        return float(r[k])
    except (TypeError, ValueError):
        return 0.0


rows = [r for r in csv.DictReader(open(f"{T}/outputs/audi_1210_share_of_site_visits.csv"))
        if r.get("advertiser_id", "").isdigit()]
rows.sort(key=lambda r: -f(r, "spend_30d"))

scored = [r for r in rows if r["coverage"] == "Scored"]
dark = [r for r in rows if r["coverage"] == "Pixel reported nothing"]
quiet = [r for r in rows if r["coverage"] == "Site too quiet to score"]
flagged = [r for r in scored
           if f(r, "site_visit_share_percentile_vs_peers") < PEER_CUT and f(r, "spend_30d") >= SPEND_FLOOR]
never = sum(1 for r in dark if f(r, "raw_visits_12mo") == 0)
flag_spend = sum(f(r, "spend_30d") for r in flagged)
dark_spend = sum(f(r, "spend_30d") for r in dark)

# Peer groups are labelled with the site-traffic range they cover, so the reader can see why an
# advertiser was compared to the accounts it was compared to without opening the Read me.
SIZE = {"1": "Under 25K visits", "2": "25K to 120K visits", "3": "120K to 350K visits",
        "4": "350K to 1.4M visits", "5": "Over 1.4M visits"}

df_flag = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "30-day spend": f(r, "spend_30d"),
    "Their site visits": int(f(r, "raw_visits_30d")),
    "Our visits": int(f(r, "verified_visits_30d")),
    "Share of site visits": f(r, "share_of_site_visits"),
    "Similar sites we beat": f(r, "site_visit_share_percentile_vs_peers"),
    "Compared to sites with": SIZE.get(r["site_size_quintile"]),
} for r in flagged])

df_dark = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "30-day spend": f(r, "spend_30d"),
    "Visits in 12 months": int(f(r, "raw_visits_12mo")),
    "Last visit seen": r["last_day_with_a_visit"] or None,
    "Tracking history": "Never tracked" if f(r, "raw_visits_12mo") == 0 else "Tracked, then stopped",
} for r in dark])

df_all = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "30-day spend": f(r, "spend_30d"),
    "Their site visits": int(f(r, "raw_visits_30d")),
    "Our visits": int(f(r, "verified_visits_30d")),
    "Share of site visits": f(r, "share_of_site_visits") or None,
    "Similar sites we beat": f(r, "site_visit_share_percentile_vs_peers") if r["coverage"] == "Scored" else None,
    "Compared to sites with": SIZE.get(r["site_size_quintile"]),
} for r in rows])

FM = {"30-day spend": FMT.USD, "Their site visits": FMT.INT, "Our visits": FMT.INT,
      "Share of site visits": FMT.PCT2, "Similar sites we beat": FMT.PCT0, "Advertiser ID": "0",
      "Visits in 12 months": FMT.INT}

wb = MntnWorkbook(
    title="Share of Site Visits by Advertiser",
    ticket="AUDI-1210",
    subtitle="How much of each advertiser's own site traffic MNTN gets credit for, against similar accounts",
    period="Trailing 30 days to 2026-08-19",
    generated=GENERATED,
)

wb.table(
    "Reporting nothing", df_dark,
    finding=f"{len(df_dark)} advertisers reported no site visits in 30 days, but only one stopped from real volume",
    method=f"{never} have never reported a visit in 12 months. The rest reported some and stopped, mostly from single-digit annual volume. Together they spent ${dark_spend:,.0f}.",
    formats=FM, kind="headline",
    toc="Start here: advertisers reporting no visits at all",
    query="audi_1210_share_of_site_visits.sql",
)

wb.table(
    "Check these first", df_flag,
    finding=f"{len(df_flag)} advertisers spending ${flag_spend / 1e6:.1f}M get credit for less of their site traffic than three quarters of similar accounts",
    method="Share of site visits is our visits over theirs. Each advertiser is compared only to others whose sites get about as much traffic. See Read me.",
    formats=FM, heat={"30-day spend": "high"}, kind="data",
    toc="Accounts short of similar advertisers, $10k or more in spend",
    query="audi_1210_share_of_site_visits.sql",
)

wb.table(
    "Every advertiser", df_all,
    finding=f"All {len(df_all):,} live advertisers that served in the last 30 days, largest spender first",
    method=f"The {len(quiet)} whose sites are too quiet to score and the {len(dark)} reporting nothing have no share figure.",
    formats=FM, kind="detail", toc="The full audit trail",
    query="audi_1210_share_of_site_visits.sql",
)

wb.glossary(
    "Read me",
    intro="One question: of everyone who visited the advertiser's site, how many did we get credit for?",
    rows=[
        ("Their site visits", "Every visit the advertiser's own pixel reported in 30 days, whether or not MNTN was involved."),
        ("Our visits", "MNTN verified visits over the same period: clicks plus views plus competing views. The client-facing Reporting figure."),
        ("Share of site visits", "Our visits over their site visits."),
        ("Similar sites we beat", "Of advertisers whose sites get about as much traffic, the share we do better than. 23% means we reach a bigger slice of the audience than 23 of every 100 similar advertisers, and a smaller slice than the other 77."),
        ("Compared to sites with", "The 30-day site-visit range of the advertisers this one was measured against. Comparison stays inside a band because a busier site gives any one channel a smaller slice."),
        ("No share shown", "The advertiser reported fewer than 1,000 site visits, so the ratio would be noise."),
        ("Tracking history", "For advertisers reporting nothing now: whether they have EVER reported a visit in the last 12 months. Never tracked points at a pixel that was never installed; tracked then stopped points at one that broke."),
    ],
)

wb.notes(
    "Method & caveats",
    intro="Read the first block before treating any row as a defect.",
    blocks=[
        ("This is a flag, not a verdict",
         "A low share can come from campaign configuration, audience, flight length or budget. It says the account is worth opening, not that anything is broken."),
        ("Every advertiser is compared only to others with similarly busy sites",
         "The bigger a site gets, the smaller a slice any one channel holds: the median share runs 1.09% for sites under 25K monthly visits and 0.39% for those over 1.4M. Comparing across the whole base would flag large sites and nothing else, so each advertiser is measured inside its own traffic band."),
        ("Our visits are attribution-credited, not a clean reach count",
         "A verified visit requires an impression to have been served and credited. So a low share mixes reaching few of their visitors with being credited for few, and the two cannot be separated here."),
        ("Visit rate on its own was the wrong measure",
         "An earlier version of this list ranked on matched visits over served IPs. That mostly tracks campaign audience against site size: Maurices matches 3.2% of served IPs but gets credit for 0.40% of its site traffic, while Re-Bath Cherry Hill matches 0.13% and gets 1.27%."),
        ("Tracking opt-out was checked and does not explain the zeros",
         "The opt-out flag on the advertiser record is set for one of these accounts. It is also not backfilled: it reads false for every advertiser created before 2022. These accounts were all created 2024 or later, so it should be readable. And an opt-out cannot produce an account that reported visits and then stopped."),
        ("The 30-day window under-detects recent breakage",
         "An advertiser whose pixel went dark ten days ago still carries three weeks of earlier visits and stays off the flag."),
    ],
)

wb.sql_dir("Queries", f"{T}/queries",
           note="The one query behind every sheet. It runs standalone against BigQuery.")

wb.cover(takeaways=[
    f"{len(df_flag)} advertisers spending ${flag_spend / 1e6:.1f}M over 30 days get credit for less of their site traffic than similar accounts.",
    f"The {len(dark)} reporting nothing at all spent ${dark_spend:,.0f} between them, and only one stopped from real volume.",
    "Busier sites give any channel a smaller slice, so each advertiser is measured against others in its own traffic band.",
])

# Filename is kept as-is on purpose: the link is already circulating with Johnny and Imani.
# The workbook title inside is what changed.
print("wrote", wb.save_drive("AUDI-1210", "Advertisers With No Measurable Visits"))
