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

SIZE = {"1": "Smallest fifth", "2": "Second fifth", "3": "Middle fifth",
        "4": "Fourth fifth", "5": "Largest fifth"}

df_flag = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "30-day spend": f(r, "spend_30d"),
    "Their site visits": int(f(r, "raw_visits_30d")),
    "Our visits": int(f(r, "verified_visits_30d")),
    "Share of site visits": f(r, "share_of_site_visits"),
    "Rank vs peers": f(r, "site_visit_share_percentile_vs_peers"),
    "Site size group": SIZE.get(r["site_size_quintile"]),
} for r in flagged])

df_dark = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "30-day spend": f(r, "spend_30d"),
    "Visits in 12 months": int(f(r, "raw_visits_12mo")),
    "Last visit seen": r["last_day_with_a_visit"] or None,
    "Reading": "Never tracked" if f(r, "raw_visits_12mo") == 0 else "Tracked, then stopped",
} for r in dark])

df_all = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "30-day spend": f(r, "spend_30d"),
    "Their site visits": int(f(r, "raw_visits_30d")),
    "Our visits": int(f(r, "verified_visits_30d")),
    "Share of site visits": f(r, "share_of_site_visits") or None,
    "Rank vs peers": f(r, "site_visit_share_percentile_vs_peers") if r["coverage"] == "Scored" else None,
    "Site size group": SIZE.get(r["site_size_quintile"]),
} for r in rows])

FM = {"30-day spend": FMT.USD, "Their site visits": FMT.INT, "Our visits": FMT.INT,
      "Share of site visits": FMT.PCT2, "Rank vs peers": FMT.PCT0, "Advertiser ID": "0",
      "Visits in 12 months": FMT.INT}

wb = MntnWorkbook(
    title="Share of Site Visits by Advertiser",
    ticket="AUDI-1210",
    subtitle="How much of each advertiser's own site traffic MNTN gets credit for, against similar accounts",
    period="Trailing 30 days to 2026-08-19",
    generated=GENERATED,
)

wb.table(
    "Check these first", df_flag,
    finding=f"{len(df_flag)} advertisers spending ${flag_spend / 1e6:.1f}M get credit for less of their site traffic than three quarters of similar accounts",
    method="Share of site visits is our verified visits over the advertiser's own reported visits, ranked within a site-size group. See Read me.",
    formats=FM, heat={"30-day spend": "high"}, kind="headline",
    toc="Start here: short of similar accounts, $10k or more in spend",
    query="audi_1210_share_of_site_visits.sql",
)

wb.table(
    "Reporting nothing", df_dark,
    finding=f"{len(df_dark)} advertisers reported no site visits in 30 days, but only one stopped from real volume",
    method=f"{never} never tracked a visit in 12 months. The rest stopped, mostly from single-digit annual volume. Together they spent ${dark_spend:,.0f}.",
    formats=FM, kind="data",
    toc="Advertisers reporting nothing, and whether they ever did",
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
        ("Rank vs peers", "Where that share sits against advertisers with similarly sized sites. 20% means four in five peers do better."),
        ("Site size group", "Advertisers split into five equal groups by their own site visits. The share falls as sites get larger, so it is only comparable within a group."),
        ("No share shown", "The advertiser reported fewer than 1,000 site visits, so the ratio would be noise."),
    ],
)

wb.notes(
    "Method & caveats",
    intro="Read the first block before treating any row as a defect.",
    blocks=[
        ("This is a flag, not a verdict",
         "A low share can come from campaign configuration, audience, flight length or budget. It says the account is worth opening, not that anything is broken."),
        ("Compared within a size group, because the share shrinks with site size",
         "Median share runs 1.09% at the smallest fifth of sites and 0.39% at the largest. Ranking on the raw figure would flag large sites and nothing else."),
        ("Our visits are attribution-credited, not a clean reach count",
         "A verified visit requires an impression to have been served and credited. So a low share mixes reaching few of their visitors with being credited for few, and the two cannot be separated here."),
        ("Visit rate on its own was the wrong measure",
         "An earlier version of this list ranked on matched visits over served IPs. That mostly tracks campaign audience against site size: Maurices matches 3.2% of served IPs but gets credit for 0.40% of its site traffic, while Re-Bath Cherry Hill matches 0.13% and gets 1.27%."),
        ("The 30-day window under-detects recent breakage",
         "An advertiser whose pixel went dark ten days ago still carries three weeks of earlier visits and stays off the flag."),
    ],
)

wb.sql_dir("Queries", f"{T}/queries",
           note="The one query behind every sheet. It runs standalone against BigQuery.")

wb.cover(takeaways=[
    f"{len(df_flag)} advertisers spending ${flag_spend / 1e6:.1f}M over 30 days get credit for less of their site traffic than similar accounts.",
    f"The {len(dark)} reporting nothing at all spent ${dark_spend:,.0f} between them, and only one stopped from real volume.",
    "Share of site visits is only comparable within a site-size group: it runs 1.1% at the smallest sites and 0.4% at the largest.",
])

# Filename is kept as-is on purpose: the link is already circulating with Johnny and Imani.
# The workbook title inside is what changed.
print("wrote", wb.save_drive("AUDI-1210", "Advertisers With No Measurable Visits"))
