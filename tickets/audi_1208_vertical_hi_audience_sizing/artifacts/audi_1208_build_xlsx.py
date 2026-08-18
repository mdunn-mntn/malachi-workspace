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
    q = s.quantiles(v, n=4)
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
        flags[int(r[2])] = dict(mm=r[7].strip().lower() == "true", excl=r[8].strip().lower() == "true")
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

# Rows mirror the requester's two sub-asks verbatim, then repeat them one stage wider so the
# sensitivity to the stage boundary is visible on the same tab.
rows_h = []
for stage_label, stages in (("Prospecting", (1,)), ("Prospecting + MT-S2", (1, 2))):
    coh = [c for c in camps if c["mm"] and c["funnel"] in stages]
    assert not [c for c in coh if c["hi"] == c["all_ips"]], "flat-10000 leaked into a cohort"
    for label, data in (
        ("HI, audiences with no exclusions", [c for c in coh if not c["excl"]]),
        ("HI, all MM audiences (incl. exclusions)", coh),
        ("HI, only audiences with exclusions", [c for c in coh if c["excl"]]),
    ):
        st = summarize([c["hi"] for c in data])
        rows_h.append({"Cohort": label, "Stage": stage_label, "Audiences": st["n"],
                       "Mean IPs": st["mean"], "Median IPs": st["median"],
                       "Q1": st["q1"], "Q3": st["q3"], "Largest": st["mx"]})
df_h = pd.DataFrame(rows_h)

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
    method="Distinct IPs scoring 8001-10000 per active MM audience, 2026-08-17. Widening a stage barely moves it. Read Method & caveats before quoting.",
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
        ("Audience", "One active Stage-1 prospecting campaign and its targeting expression. 2,063 were live and prospecting-scored."),
        ("Size", "Distinct IP addresses. Not households and not people; one home can hold several IPs over time."),
        ("", ""),
        ("Score bands", ""),
        ("High Intent", "An IP that sits in the advertiser's vertical AND matches the campaign's keywords. Scores 8001-10000."),
        ("Peak Performance", "In the vertical but matching no keyword. Scores 6666-8000. Shown for context only."),
        ("", ""),
        ("Reading the numbers", ""),
        ("Mean vs median", "Both are given because the spread is wide. The mean sits well above the median, so a few very large entries pull it up."),
        ("Quartiles", "Q1 and Q3 bound the middle half. Half of all entries fall between them, a quarter below Q1, a quarter above Q3."),
        ("Exclusion", "A clause telling the platform to leave a group out, e.g. existing customers. 721 of 2,063 audiences had one."),
    ],
)

wb.notes(
    "Method & caveats",
    intro="Read the first two blocks before quoting the High Intent numbers.",
    blocks=[
        ("Exclusions are not subtracted from these HI pools",
         "The scoring pipeline never applies them. It reads the campaign-category file only to look up a campaign's funnel level, and discards the include/exclude flag before scoring. Exclusions are enforced later, when the bidder decides what to buy."),
        ("So the cohort split is a correlation, not an effect",
         "Both cohorts report their pool before any exclusion is removed. That is why audiences with exclusions look slightly larger, not smaller: larger accounts are likelier to run an exclusion. Do not read the gap as the cost of excluding."),
        ("Averages are the wrong summary here on their own",
         "Both distributions are strongly right-skewed. The mean vertical is 9.5M against a 6.6M median; the mean HI pool is 4.8M against a 3.6M median. The quartiles describe the portfolio, the mean describes its largest members."),
        ("Verticals and buckets do not sum",
         "A bucket is a vertical's parent, and an IP can sit in several verticals at once. Adding categories double-counts IPs. Every figure is a distinct count within its own category."),
        ("One day, not an average of days",
         "2026-08-17 for every figure. Vertical totals drift about 2% day to day, so treat these as a current snapshot rather than a stable constant."),
        ("Source of the vertical counts",
         "The same daily file that feeds the existing vertical size monitor. A cross-check against the downstream copy of the same data for the prior day agreed to a median 1.9%, consistent with one day of growth."),
        ("HI counts are approximate by design",
         "One day of scores is 251.6 billion rows, so distinct IPs are counted with an approximate method accurate to roughly 1%. That is far inside the spread being reported."),
        ("Later-stage campaigns are excluded, and must be",
         "The pipeline flattens the score to 10000 for anything past the prospecting stage, which would enter the High Intent band as its entire audience. On the day, 1,426 such campaigns scored 100% High Intent. Dropping them cuts the mean from 18.3M to 4.8M."),
        ("What is counted as an audience",
         "The 2,063 Stage-1 campaigns the pipeline actually prospecting-scored on the day. All 2,063 carry MNTN Matched targeting, so no non-MM comparison group exists here."),
        ("Smallest HI pool is genuinely zero",
         "Some active audiences reached no High Intent IPs at all on the day. Those are kept in the counts rather than dropped, so the minimum reads 0."),
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
    note="Both queries read GCS through an inline external table definition; the registered BigQuery table for scores cannot see partitions after mid-July 2026.",
)

wb.cover(takeaways=[
    "Verticals: mean 9.5M IPs, median 6.6M, middle half 4.0M to 12.0M, across all 148.",
    "HI pool: 4.5M mean with no exclusions, 4.8M across all MM audiences; medians 3.5M and 3.6M.",
    "Exclusions are applied at bid time, not in scoring, so both cohorts are pre-exclusion pools.",
])

print(wb.save_drive("AUDI-1208", "Vertical and HI Audience Sizes"))
