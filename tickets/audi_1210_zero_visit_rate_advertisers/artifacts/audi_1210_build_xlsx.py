"""AUDI-1210 — how much of each advertiser's own site traffic MNTN touched, and who is short of peers."""
import csv
import statistics as st
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


rows = [r for r in csv.DictReader(open(f"{T}/outputs/audi_1210_share_of_voice.csv"))
        if r.get("advertiser_id", "").isdigit()]
rows.sort(key=lambda r: -f(r, "spend_30d"))

scored = [r for r in rows if r["coverage"] == "Scored"]
quiet = [r for r in rows if r["coverage"] == "Site too quiet to score"]
dark = [r for r in rows if r["coverage"] == "Pixel reported nothing"]
never = sum(1 for r in dark if f(r, "raw_visits_12mo") == 0)
flagged = [r for r in scored
           if f(r, "sov_percentile_vs_peers") < PEER_CUT and f(r, "spend_30d") >= SPEND_FLOOR]

SIZE_LABEL = {"1": "Smallest fifth", "2": "Second fifth", "3": "Middle fifth",
              "4": "Fourth fifth", "5": "Largest fifth"}


def frame(rs, peers=True):
    out = []
    for r in rs:
        d = {
            "Advertiser": r["advertiser_name"],
            "Advertiser ID": int(r["advertiser_id"]),
            "30-day spend": f(r, "spend_30d"),
            "Their site visits": int(f(r, "raw_visits_30d")),
            "Our verified visits": int(f(r, "verified_visits_30d")),
            "Matched IPs": int(f(r, "matched_ips_30d")),
            "Share of voice": f(r, "share_of_voice") or None,
            "Match rate": f(r, "match_rate"),
            "Served IPs": int(f(r, "served_ips_30d")),
        }
        if peers:
            d["Site size group"] = SIZE_LABEL.get(r["site_size_quintile"], None)
            d["Rank vs peers"] = f(r, "sov_percentile_vs_peers") if r["coverage"] == "Scored" else None
        else:
            d["Visits in 12 months"] = int(f(r, "raw_visits_12mo"))
            d["Last visit seen"] = r["last_day_with_a_visit"] or None
            d["Reading"] = ("Never tracked" if f(r, "raw_visits_12mo") == 0
                            else "Tracked, then stopped")
        out.append(d)
    return pd.DataFrame(out)


df_flag = frame(flagged)
df_all = frame(rows)
df_dark = frame(dark, peers=False)

# The size effect, stated as a table so the peer grouping is not a black box.
by_q = []
for q in ("1", "2", "3", "4", "5"):
    g = [r for r in scored if r["site_size_quintile"] == q and f(r, "share_of_voice") > 0]
    if not g:
        continue
    by_q.append({
        "Site size group": SIZE_LABEL[q],
        "Advertisers": len(g),
        "Median site visits": int(st.median([f(r, "raw_visits_30d") for r in g])),
        "Median share of voice": st.median([f(r, "share_of_voice") for r in g]),
        "Median match rate": st.median([f(r, "match_rate") for r in g]),
        "Median spend": st.median([f(r, "spend_30d") for r in g]),
    })
df_size = pd.DataFrame(by_q)

# The two accounts Johnny used to make the point, side by side.
def pick(aid):
    return next((r for r in rows if r["advertiser_id"] == aid), None)


df_pair = pd.DataFrame([{
    "Advertiser": r["advertiser_name"],
    "Advertiser ID": int(r["advertiser_id"]),
    "Their site visits": int(f(r, "raw_visits_30d")),
    "Our verified visits": int(f(r, "verified_visits_30d")),
    "Match rate": f(r, "match_rate"),
    "Share of voice": f(r, "share_of_voice"),
    "Rank vs peers": f(r, "sov_percentile_vs_peers"),
} for r in (pick("66784"), pick("39510")) if r])

FM = {"30-day spend": FMT.USD, "Their site visits": FMT.INT, "Our verified visits": FMT.INT,
      "Matched IPs": FMT.INT,
      "Share of voice": FMT.PCT2, "Match rate": FMT.PCT2, "Served IPs": FMT.INT,
      "Rank vs peers": FMT.PCT0, "Advertiser ID": "0", "Visits in 12 months": FMT.INT}

wb = MntnWorkbook(
    title="Share of Voice by Advertiser",
    ticket="AUDI-1210",
    subtitle="How much of each advertiser's own site traffic MNTN touched, and who falls short of similar accounts",
    period="Trailing 30 days to 2026-08-19",
    generated=GENERATED,
)

wb.table(
    "Check these first", df_flag,
    finding=f"{len(df_flag)} advertisers spent $10k or more and touched less of their own site traffic than three quarters of similar accounts",
    method="Share of voice is matched visits over the advertiser's own site visits, compared within a site-size group. See Read me.",
    formats=FM, heat={"30-day spend": "high"}, kind="headline",
    toc="Start here: short of peers on share of voice, $10k or more",
    query="audi_1210_share_of_voice.sql",
)

wb.table(
    "Why match rate alone misleads", df_pair,
    finding="Re-Bath has a 25x worse match rate than Maurices and reaches 3x more of its site's audience (1.27% against 0.40%)",
    method="Share of voice ranks Re-Bath at the median of its size peers. Match rate mostly tracks campaign audience against site size. See Method & caveats.",
    formats=FM, kind="headline",
    toc="The two accounts that reframed this list",
)

wb.table(
    "Share of voice by site size", df_size,
    finding="Share of voice falls from 1.1% at the smallest sites to 0.4% at the largest, so it is only comparable within a size group",
    method="All 1,649 scorable advertisers, split into five equal groups by their own site visits. Medians within each group.",
    formats={"Advertisers": FMT.INT, "Median site visits": FMT.INT,
             "Median share of voice": FMT.PCT2, "Median match rate": FMT.PCT2,
             "Median spend": FMT.USD},
    kind="data", toc="Why peers are matched on site size",
)

wb.table(
    "Pixel reported nothing", df_dark,
    finding=f"Of {len(df_dark)} advertisers reporting nothing in 30 days, {never} never tracked in 12 months and the rest tracked and stopped",
    method="An opt-out never reports a visit; a broken pixel reports and then stops. Only Dura Guard Roofing stopped from real volume. See Method & caveats.",
    formats=FM, kind="data", toc="Advertisers reporting nothing at all",
)

wb.table(
    "Every advertiser", df_all,
    finding=f"All {len(df_all):,} live advertisers that served in the last 30 days, largest spender first",
    method=f"Includes the {len(quiet)} whose sites are too quiet to score and the {len(dark)} reporting nothing; both have no share-of-voice figure.",
    formats=FM, kind="detail", toc="The full audit trail",
    query="audi_1210_share_of_voice.sql",
)

wb.glossary(
    "Read me",
    intro="Two ratios sit side by side here and they answer different questions. One asks how much of what we served came back as a visit. The other asks how much of the advertiser's whole audience we touched at all.",
    rows=[
        ("Their site visits", "Every visit the advertiser's own pixel reported in 30 days, whether or not MNTN was involved."),
        ("Our verified visits", "MNTN verified visits over the same 30 days: clicks plus views plus competing views. This is the client-facing Reporting figure."),
        ("Matched IPs", "Distinct served IPs later seen on their site. Shown for context; it is smaller than verified visits because one household visiting repeatedly counts once."),
        ("Match rate", "Matched visits over IPs we served. Low mostly means we served a small audience against a large site."),
        ("Share of voice", "Our verified visits over their total site visits. The share of their traffic that came through MNTN."),
        ("Rank vs peers", "Where this share of voice sits against advertisers with similarly sized sites. 20% means four in five peers reach more of their audience."),
        ("", ""),
        ("How to read a low number", ""),
        ("A low match rate is usually not a fault", "Maurices matches 3.2% and Re-Bath Cherry Hill 0.13%, yet Re-Bath reaches 1.27% of its site's audience against Maurices' 0.40%, and sits at the median of its size peers."),
        ("Compare within a size group", "Share of voice runs 1.1% at the smallest sites and 0.4% at the largest, so a raw comparison across the base would just select big sites."),
        ("Not scored", "An advertiser needs at least 1,000 reported site visits for the ratio to mean anything. Quieter ones are listed with the figure left blank."),
    ],
)

wb.notes(
    "Method & caveats",
    intro="This list has been recut twice. Both times an incoming point was right and changed which advertisers appear.",
    blocks=[
        ("First cut ranked on visit rate alone and named the wrong accounts",
         "It led with Real Techniques, Food Lion and Valvoline as likely pixel defects. Their own pixels report 55, 20 and 70 visits in 30 days. Those are quiet sites, not broken ones."),
        ("Second cut added their site traffic, which was Johnny Chen's point",
         "Matched visits are a subset of reported visits, so a zero match on 20 visits a month means nothing. That reclassified 171 advertisers as quiet sites."),
        ("Third cut is share of voice, also his point",
         "A low match rate reflects campaign audience against site size, not measurement. Share of voice asks the question that matters: of everyone who reached their site, how many came through us."),
        ("Peers are matched on site size because share of voice shrinks with it",
         "Correlation of log site visits to log share of voice is -0.24, and median share of voice falls from 1.09% to 0.39% across the size range. Ranking on the raw figure would flag large sites and nothing else."),
        ("Only one advertiser reporting nothing looks like real breakage",
         f"{never} of the {len(dark)} never tracked a visit in 12 months, consistent with an opt-out or a pixel never installed. The rest stopped, but on 1 to 151 visits all year, so the stop date is indistinguishable from a quiet site. Dura Guard Roofing is the exception: 7,338 visits, last seen 2026-04-28."),
        ("Pixel opt-out was checked and does not explain the group",
         "advertisers.conv_pixel_opt_out is set for 1 of these advertisers, against 3.4% across the live base, and 38 of them carry the same tracking status nearly every live advertiser has. That field covers the conversion pixel, so a separate visit-tracking setting may exist that this table does not carry."),
        ("This is still a flag, not a verdict",
         "A low share of voice against peers can come from campaign configuration, audience quality, flight length or budget. It says the account is worth opening, not that anything is broken."),
        ("The share-of-voice definition, settled",
         "An earlier version divided distinct matched IPs by site visits and read Re-Bath Cherry Hill at 0.29% against Johnny's 1.25%. Testing both numerators on that account resolved it: verified visits give 1.269% and matched IPs give 0.291%. Verified visits is the client-facing figure and is what this file now uses."),
        ("The 30-day window under-detects recent breakage",
         "An advertiser whose pixel went dark ten days ago still carries three weeks of earlier visits and stays off the flag. A shorter trailing window would surface those, at the cost of more advertisers reading zero by chance."),
    ],
)

wb.sql_dir("Queries", f"{T}/queries",
           note="The query behind every sheet. It runs standalone against BigQuery and returns the full base.")

wb.cover(takeaways=[
    f"{len(df_flag)} advertisers spent $10k or more and reach less of their own site audience than three quarters of similar accounts.",
    f"Only {len(dark)} of {len(rows):,} report nothing at all, and just one of those stopped from real volume.",
    "Share of voice is compared within a site-size group, and uses verified visits, the client-facing figure.",
])

print("wrote", wb.save_drive("AUDI-1210", "Advertisers With No Measurable Visits"))
