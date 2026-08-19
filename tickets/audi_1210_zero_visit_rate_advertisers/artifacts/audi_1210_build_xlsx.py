"""AUDI-1210 — advertisers spending with almost no attributed site visits, split by site traffic."""
import csv
import sys

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

T = "/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1210_zero_visit_rate_advertisers"
GENERATED = "2026-08-19"

READING = {
    "Traffic exists, we are not matching it": "Traffic, no match",
    "Site traffic is tiny": "Quiet site",
    "Pixel reported nothing": "Pixel silent",
}


def f(r, k):
    try:
        return float(r[k])
    except (TypeError, ValueError):
        return 0.0


rows = list(csv.DictReader(open(f"{T}/outputs/audi_1210_low_visit_rate_advertisers.csv")))
rows.sort(key=lambda r: -f(r, "spend_30d"))

# Where each match rate sits in the whole live base, so nobody reads 0.2% as alarming on its own.
BASE = "/Users/malachi/Developer/work/mntn/workspace/tickets/incr_75_eligible_advertisers/outputs/incr_75_advertiser_metrics.csv"
base_rates = sorted(f(r, "p_visit") for r in csv.DictReader(open(BASE)) if f(r, "distinct_ips_30d") >= 1000)


def pctile(x):
    lo = sum(1 for v in base_rates if v < x)
    return lo / len(base_rates)


def frame(rs):
    return pd.DataFrame([{
        "Advertiser": r["advertiser_name"],
        "Advertiser ID": int(r["advertiser_id"]),
        "Reading": READING[r["reading"]],
        "30-day spend": f(r, "spend_30d"),
        "Their site visits": int(f(r, "raw_visits_30d")),
        "Visits we matched": int(f(r, "visiting_ips_30d")),
        "Match rate": f(r, "visit_rate"),
        "Base percentile": pctile(f(r, "visit_rate")),
        "Served IPs": int(f(r, "served_ips_30d")),
        "Their conversions": int(f(r, "raw_conversions_30d")),
        "Days with a visit": int(f(r, "days_with_any_visit")),
        "Industry": None,
    } for r in rs])


mism = [r for r in rows if r["reading"] == "Traffic exists, we are not matching it"]
quiet = [r for r in rows if r["reading"] == "Site traffic is tiny"]
dark = [r for r in rows if r["reading"] == "Pixel reported nothing"]
mism_big = [r for r in mism if f(r, "spend_30d") >= 10_000]

df_mism_big = frame(mism_big)
df_all = frame(rows)
df_dark = frame(dark)


def band(rs):
    return {"Advertisers": len(rs), "30-day spend": sum(f(r, "spend_30d") for r in rs),
            "Over $10k": sum(1 for r in rs if f(r, "spend_30d") >= 10_000)}


df_sum = pd.DataFrame([
    {"Reading": "Traffic, no match", "What it means":
     "Their site gets real traffic and we attribute almost none of it", **band(mism)},
    {"Reading": "Quiet site", "What it means":
     "Under 1,000 site visits in 30 days, so there is little to attribute", **band(quiet)},
    {"Reading": "Pixel silent", "What it means":
     "Their pixel reported no visits at all in 30 days", **band(dark)},
])

FM = {"30-day spend": FMT.USD, "Their site visits": FMT.INT, "Visits we matched": FMT.INT,
      "Match rate": FMT.PCT2, "Served IPs": FMT.INT, "Their conversions": FMT.INT,
      "Days with a visit": FMT.INT, "Advertiser ID": "0", "Base percentile": FMT.PCT0}

wb = MntnWorkbook(
    title="Advertisers We Attribute Almost No Visits For",
    ticket="AUDI-1210",
    subtitle="Live advertisers under a 0.5% match rate, split by whether their site has traffic at all",
    period="Trailing 30 days to 2026-08-19",
    generated=GENERATED,
)

wb.table(
    "Check these first", df_mism_big,
    finding=f"{len(mism_big)} advertisers spent $10k or more and have real site traffic we are matching almost none of",
    method="Their site visits come from the advertiser's own pixel. Visits we matched are served IPs later seen on their site. See Read me.",
    formats=FM, heat={"30-day spend": "high"}, kind="headline",
    toc="Start here: real traffic, no match, $10k or more",
    query="audi_1210_zero_visit_rate_advertisers.sql",
)

wb.table(
    "Three readings", df_sum,
    finding=f"Of {len(rows)} advertisers under a 0.5% match rate, {len(mism)} have traffic we are missing and only {len(dark)} have a silent pixel",
    method="Every advertiser falls in exactly one reading. Split on their own reported site visits over the same 30 days.",
    formats={"Advertisers": FMT.INT, "30-day spend": FMT.USD, "Over $10k": FMT.INT},
    kind="headline", toc="How the 542 split, and which group matters",
)

wb.table(
    "Pixel silent", df_dark,
    finding=f"{len(dark)} advertisers reported no site visits at all, together spending ${sum(f(r, 'spend_30d') for r in dark):,.0f}",
    method="Zero reported visits over 30 days. Small in number and in spend, but the clearest setup question.",
    formats=FM, kind="data", toc="The advertisers reporting nothing at all",
)

wb.table(
    "Full list", df_all,
    finding=f"All {len(df_all)} advertisers under a 0.5% match rate, largest spender first",
    method="Every live, non-test advertiser that served an impression in the trailing 30 days and matched under 0.5%.",
    formats=FM, kind="data", toc="The full list",
    query="audi_1210_zero_visit_rate_advertisers.sql",
)

wb.glossary(
    "Read me",
    intro="Two visit numbers sit side by side here, and the gap between them is the question. One is the advertiser's own site traffic. The other is how much of it we could tie back to an ad we served.",
    rows=[
        ("Their site visits", "Every visit the advertiser's pixel reported in 30 days, whether or not MNTN was involved. This is their site traffic."),
        ("Visits we matched", "Served IPs we later saw on their site. Always a subset of their site visits, so a small traffic number caps it arithmetically."),
        ("Match rate", "Matched visits divided by served IPs, over 30 days. The live-base median is 2.0%."),
        ("Base percentile", "Where this advertiser's match rate falls against all 1,798 live advertisers. 25% means three quarters of the base matches better."),
        ("A note on scale", "This is a 30-day cumulative rate, not a daily one. Per campaign per day the median is 0.33%. So 0.2% daily is ordinary; 0.2% over 30 days is not."),
        ("Days with a visit", "How many of the 30 days reported at least one visit. A handful of days points at a page that is rarely reached, not a dead pixel."),
        ("", ""),
        ("The three readings", ""),
        ("Traffic, no match", "1,000 or more site visits and still under a 0.5% match rate. Their pixel works and something between the impression and the visit is not connecting. This is the group worth investigating."),
        ("Quiet site", "Under 1,000 site visits in 30 days. Nothing to attribute. Not a defect, and worth knowing before anyone chases it."),
        ("Pixel silent", "No reported visits at all. The clearest setup question, and the smallest group."),
    ],
)

wb.notes(
    "Method & caveats",
    intro="What changed from the first version of this list, and what it does and does not show.",
    blocks=[
        ("The first version of this list led with the wrong advertisers",
         "It ranked purely on match rate and named Real Techniques, Food Lion and Valvoline as the likeliest pixel defects. Their own pixels report 55, 20 and 70 visits in 30 days. Those are quiet sites, not broken ones. Johnny Chen made the point that raw visits belong next to attributed ones, and he was right."),
        ("Attributed visits cannot exceed site visits",
         "A matched visit requires the advertiser's pixel to have reported that visit in the first place. So a low site-traffic number caps the match arithmetically and a zero match rate on 20 visits a month means nothing."),
        ("The group that matters is the third one",
         f"{len(mism)} advertisers have 1,000 or more reported site visits and still match under 0.5%, {len(mism_big)} of them spending $10k or more. EcoATM is the clearest: 8.4M site visits, 6,938 matched, 0.29%."),
        ("This is a measurement flag, not a performance verdict",
         "A low match rate says we cannot see the connection, not that the advertising failed. Identity matching, pixel placement, and cross-device traffic all sit between an impression and an attributed visit."),
        ("Why it matters beyond reporting",
         "An advertiser with no measurable visit rate cannot be screened for an incrementality lift test and cannot be shown a result. This was the largest single cut in the AUDI-1209 screening funnel, at 479 of 1,859 advertisers."),
        ("The 0.5% cut is a soft edge, not an outlier line",
         "496 of 1,798 live advertisers sit under it, so this list is the bottom 28% of the base rather than a small tail. That is why the first sheet conditions on real site traffic and $10k of spend instead of on the rate alone. Read the base percentile column before treating any single rate as abnormal."),
        ("The 30-day window under-detects recent breakage",
         "An advertiser whose pixel went dark ten days ago still carries three weeks of earlier visits and stays off this list. A shorter trailing window would surface those, at the cost of more advertisers reading zero by chance."),
    ],
)

wb.sql_dir("Queries", f"{T}/queries",
           note="The query behind this list. It runs standalone against BigQuery.")

wb.cover(takeaways=[
    f"{len(mism_big)} advertisers spent $10k or more, have real site traffic, and we match under 0.5% of it.",
    f"Only {len(dark)} of the {len(rows)} have a genuinely silent pixel; {len(quiet)} simply have quiet sites.",
    "EcoATM is the clearest case: 8.4M site visits in 30 days, 6,938 matched.",
])

print("wrote", wb.save_drive("AUDI-1210", "Advertisers With No Measurable Visits"))
