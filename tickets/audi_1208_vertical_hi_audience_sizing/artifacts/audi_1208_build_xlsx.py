import csv
import statistics as s
import sys

import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import FMT, MntnWorkbook  # noqa: E402

T = "/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1208_vertical_hi_audience_sizing"
OUT = f"{T}/outputs"


def summarize(vals):
    v = sorted(vals)
    q = s.quantiles(v, n=4, method="inclusive")  # linear interpolation; matches Spark percentile()
    return dict(n=len(v), mean=round(s.mean(v)), median=round(s.median(v)),
                q1=round(q[0]), q3=round(q[2]), mn=v[0], mx=v[-1])


verts = [r for r in csv.reader(open(f"{OUT}/audi_1208_vertical_sizes_2026_08_17.csv")) if len(r) >= 5]
v6 = [(int(r[0]), r[2], int(r[4])) for r in verts if r[1] == "6"]
v3 = [(int(r[0]), r[2], int(r[4])) for r in verts if r[1] == "3"]

hi = {}
for r in csv.reader(open(f"{OUT}/audi_1208_hi_by_campaign_2026_08_17.csv")):
    hi[int(r[2])] = dict(all_ips=int(r[3]), hi=int(r[4]), pp=int(r[5]))
flags = {}
for r in csv.reader(open(f"{OUT}/audi_1208_campaign_exclusion_flags_2026_08_17.csv")):
    if len(r) >= 9:
        flags[int(r[2])] = dict(
            mm=r[7].strip().lower() == "true",
            excl=r[8].strip().lower() == "true",
            kw="19" in {x.strip() for x in (r[10] if len(r) > 10 else "").split(",")},
        )
funnel = {}
for r in csv.reader(open(f"{OUT}/audi_1208_campaign_funnel_levels.csv")):
    if len(r) >= 4:
        funnel[int(r[0])] = dict(funnel=int(r[1]), tmpl=int(r[3]))
keys = set(hi) & set(flags) & set(funnel)
camps = [dict(cid=c, **hi[c], **flags[c], **funnel[c]) for c in keys]
# prospecting_join flattens household_score to 10000 for any campaign that is not
# template 10 / funnel 1-2, which would enter the 8001-10000 band as fake High Intent.
mm = [c for c in camps if c["mm"] and c["funnel"] == 1]
assert not [c for c in mm if c["hi"] == c["all_ips"]], "flat-10000 leaked into the prospecting cohort"

rows_v = []
for label, data in (("Verticals (subindustry)", v6), ("Buckets (industry)", v3)):
    st = summarize([x[2] for x in data])
    rows_v.append({"Grain": label, "Count": st["n"], "Mean IPs": st["mean"],
                   "Median IPs": st["median"], "Q1": st["q1"], "Q3": st["q3"],
                   "Smallest": st["mn"], "Largest": st["mx"]})
df_v = pd.DataFrame(rows_v)

# Rows mirror the requester's two sub-asks verbatim. Prospecting only; the wider funnel reading
# lives on Method & caveats as one sentence, not as a second set of rows.
rows_h = []
assert not [c for c in mm if c["hi"] == c["all_ips"]], "flat-10000 leaked into a cohort"
for label, data in (
    ("HI, audiences with no exclusions", [c for c in mm if not c["excl"]]),
    ("HI, all MM audiences (incl. exclusions)", mm),
    ("HI, only audiences with exclusions", [c for c in mm if c["excl"]]),
    ("HI, only audiences that run keywords", [c for c in mm if c["kw"]]),
):
    st = summarize([c["hi"] for c in data])
    rows_h.append({"Cohort": label, "Audiences": st["n"], "Mean IPs": st["mean"],
                   "Median IPs": st["median"], "Q1": st["q1"], "Q3": st["q3"],
                   "Smallest": st["mn"], "Largest": st["mx"]})
df_h = pd.DataFrame(rows_h)

wider = summarize([c["hi"] for c in camps if c["mm"] and c["funnel"] in (1, 2)])

df_v_all = pd.DataFrame(
    [{"Vertical": n, "ID": i, "IPs": p} for i, n, p in sorted(v6, key=lambda x: -x[2])]
)
df_b_all = pd.DataFrame(
    [{"Bucket": n, "ID": i, "IPs": p} for i, n, p in sorted(v3, key=lambda x: -x[2])]
)

st_all = summarize([c["all_ips"] for c in mm])
df_ctx = pd.DataFrame([
    {"Band": "High Intent", "Score range": "8001-10000",
     "Mean IPs": summarize([c["hi"] for c in mm])["mean"],
     "Median IPs": summarize([c["hi"] for c in mm])["median"]},
    {"Band": "Peak Performance", "Score range": "6666-8000",
     "Mean IPs": summarize([c["pp"] for c in mm])["mean"],
     "Median IPs": summarize([c["pp"] for c in mm])["median"]},
    {"Band": "Any score", "Score range": "1-10000",
     "Mean IPs": st_all["mean"], "Median IPs": st_all["median"]},
])

INT = {"Count": FMT.INT, "Audiences": FMT.INT, "Largest": FMT.INT, "Mean IPs": FMT.INT, "Median IPs": FMT.INT,
       "Q1": FMT.INT, "Q3": FMT.INT, "Smallest": FMT.INT, "Largest": FMT.INT, "IPs": FMT.INT}

wb = MntnWorkbook(
    title="Vertical and HI Audience Sizes",
    ticket="AUDI-1208",
    subtitle="Mean and quartile sizes for MNTN verticals and the High Intent pool per MM prospecting audience",
    period="Snapshot 2026-08-17",
    generated="2026-08-18",
)

wb.table(
    "Vertical sizes", df_v,
    finding="The average vertical holds 9.5M IPs, the median 6.6M; the middle half spans 4.0M to 12.0M",
    method="Distinct IPs per DS13 category, 2026-08-17. Vertical = 6-digit id, bucket = its 3-digit parent. See Read me.",
    formats=INT, kind="headline",
    toc="Answer to part 1: how big is a vertical",
    query="audi_1208_vertical_sizes.sql",
)

wb.table(
    "HI pool sizes", df_h,
    finding="With no exclusions the mean HI pool is 4.5M IPs (median 3.5M); across all MM audiences it is 4.8M (median 3.6M)",
    method="Distinct IPs scoring 8001-10000 per active prospecting audience, 2026-08-17. Counted BEFORE exclusions are applied. See Method & caveats.",
    formats=INT, kind="headline",
    toc="Answer to part 2: HI pool, no-exclusion vs all",
    query="audi_1208_hi_subset_by_audience.sql",
)

wb.table(
    "All verticals", df_v_all,
    finding="Current Affairs is the largest vertical at 76.3M IPs, 83x the smallest at 0.9M",
    method="Every one of the 148 DS13 verticals, distinct IPs, 2026-08-17. Ranked largest first.",
    formats=INT, heat={"IPs": "high"}, kind="data",
    toc="All 148 verticals, ranked",
    query="audi_1208_vertical_sizes.sql",
)

wb.table(
    "All buckets", df_b_all,
    finding="B2B Software & Services is the largest bucket at 88.8M IPs, Apparel second at 82.3M",
    method="All 37 DS13 buckets, distinct IPs, 2026-08-17. A bucket is the parent of its verticals; the two do not sum.",
    formats=INT, heat={"IPs": "high"}, kind="data",
    toc="All 37 buckets, ranked",
    query="audi_1208_vertical_sizes.sql",
)

wb.table(
    "Score bands", df_ctx,
    finding="High Intent is a mean 4.8M of the mean 51.3M scored IPs a prospecting audience reaches",
    method="Mean and median across the same 2,063 prospecting audiences, by score band, 2026-08-17. Bands do not sum to Any score.",
    formats={"Mean IPs": FMT.INT, "Median IPs": FMT.INT}, kind="data",
    toc="HI in context of the other score bands",
    query="audi_1208_hi_subset_by_audience.sql",
)

wb.glossary(
    "Read me",
    intro="Every number here counts IP addresses on a single day, 2026-08-17.",
    rows=[
        ("The two questions", ""),
        ("Vertical", "A subindustry targeting category, e.g. Food Products. 148 exist. Identified by a 6-digit id."),
        ("Bucket", "The industry parent of a vertical, e.g. Food & Beverage. 37 exist. Identified by a 3-digit id."),
        ("Audience", "One active prospecting campaign and its targeting expression. 2,063 were live and intent-scored on the day."),
        ("Size", "Distinct IP addresses. Not households and not people; one home can hold several IPs over time."),
        ("", ""),
        ("Score bands", ""),
        ("High Intent", "An IP that sits in the advertiser's vertical AND matches the campaign's keywords. Scores 8001-10000."),
        ("Peak Performance", "In the vertical but matching no keyword. Scores 6666-8000. Shown for context only."),
        ("", ""),
        ("Reading the numbers", ""),
        ("Mean vs median", "Both are given because the spread is wide. The mean sits well above the median, so a few very large entries pull it up."),
        ("Quartiles", "Q1 and Q3 bound the middle half: half of all entries fall between them, a quarter below Q1, a quarter above Q3. Computed by linear interpolation, the same convention the daily vertical size monitor uses."),
        ("Exclusion", "A clause telling the platform to leave a group out, e.g. existing customers. 721 of 2,063 audiences had one."),
        ("Before exclusions", "Every High Intent figure counts the pool the platform could reach before any exclusion is subtracted. The after-exclusion pool is a separate number, not shown."),
        ("Keyword layer", "The campaign-level keyword targeting. High Intent requires it. An audience without it can still reach the band below, but never High Intent."),
    ],
)

wb.notes(
    "Method & caveats",
    intro="Read the first two blocks before quoting the High Intent numbers.",
    blocks=[
        ("These pools are counted BEFORE exclusions are applied",
         "For audiences that carry no exclusion that is the whole answer. For the 721 that do carry one, the after-exclusion pool is a different and smaller number that is not in this workbook. It is one query away; ask and it can be added."),
        ("Why: exclusions never reach the scoring step",
         "The pipeline reads the campaign-category file only to look up a campaign's funnel level, and discards the include/exclude flag before scoring. Exclusions are enforced later, when the bidder decides what to buy. So do not read the small gap between the cohorts as the cost of excluding."),
        ("The gap between the cohorts is which vertical they sell into",
         "Advertisers that run an exclusion sit in verticals 27% larger at the median, which more than accounts for their 7% larger pool. Comparing only within the same vertical the two cohorts are a coin flip: the larger side wins in 12 of 25 verticals."),
        ("Averages are the wrong summary here on their own",
         "Both distributions are strongly right-skewed. The mean vertical is 9.5M against a 6.6M median; the mean HI pool is 4.8M against a 3.6M median. The quartiles describe the portfolio, the mean describes its largest members."),
        ("Verticals and buckets do not sum",
         "A bucket is a vertical's parent, and an IP can sit in several verticals at once. Adding categories double-counts IPs. Every figure is a distinct count within its own category."),
        ("One day, not an average of days",
         "2026-08-17 for every figure; totals drift about 2% day to day, so treat these as a current snapshot. Vertical counts come from the same daily file that feeds the existing vertical size monitor, and a cross-check against the downstream copy for the prior day agreed to a median 1.9%."),
        ("HI counts are approximate by design",
         "One day of scores is 251.6 billion rows, so distinct IPs are counted with an approximate method accurate to roughly 1%. That is far inside the spread being reported."),
        ("The zero is real: 156 audiences cannot reach High Intent",
         "High Intent means an IP is both in the advertiser's vertical AND matches the campaign's keywords. 156 of the 2,063 run no keyword layer, so by design they top out one band lower and score exactly zero High Intent. The match is exact, 156 of 156, with no exceptions either way."),
        ("The answer holds if you widen it past prospecting",
         "These rows are prospecting audiences. Including the next stage down the funnel adds 1,418 more and barely moves the answer: the mean goes from 4.77M to 4.76M and the median from 3.55M to 3.56M. Nothing here turns on where that line is drawn."),
        ("Later-stage campaigns are excluded, and must be",
         "The pipeline flattens the score to 10000 for anything past the prospecting stage, which would enter the High Intent band as its entire audience. On the day, 1,426 such campaigns scored 100% High Intent. Dropping them cuts the mean from 18.3M to 4.8M."),
    ],
)

wb.sql_dir(
    "Queries", f"{T}/queries",
    order=["audi_1208_vertical_sizes.sql", "audi_1208_hi_subset_by_audience.sql"],
    collapse_aids=False,
    headers={
        "audi_1208_vertical_sizes.sql": "-- audi_1208_vertical_sizes.sql - vertical + bucket sizes, distinct IPs",
        "audi_1208_hi_subset_by_audience.sql": "-- audi_1208_hi_subset_by_audience.sql - HI pool per MM audience + exclusion split",
    },
    note="Both queries read the scoring output straight from cloud storage through an inline table definition, so the numbers do not depend on any intermediate table.",
)

wb.cover(takeaways=[
    "Verticals: mean 9.5M IPs, median 6.6M, middle half 4.0M to 12.0M, across all 148.",
    "HI pool: 4.5M mean with no exclusions, 4.8M across all MM audiences; medians 3.5M and 3.6M.",
"Every HI figure is counted before exclusions are applied; the after-exclusion pool is not in this workbook.",
])

print(wb.save_drive("AUDI-1208", "Vertical and HI Audience Sizes"))
