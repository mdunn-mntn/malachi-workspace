import sys, csv
sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
import pandas as pd
from lib.mntn_xlsx import MntnWorkbook, FMT

BASE = "/Users/malachi/Developer/work/mntn/workspace/tickets/audi_1083_mm_classifying_view"
sheet = {int(float(r["cgid"])): r for r in csv.DictReader(open(f"{BASE}/data/audi_1083_bad_cpa_tab.csv"))}
lab = {int(r["cgid"]): r for r in csv.DictReader(open(f"{BASE}/outputs/audi_1083_pp_labels_raw.csv"))}

PPV = {"v2_fangorn": "v2 (Fangorn)", "v1": "v1"}
RESTRICT = {"none": "None", "geo": "Geo", "audience": "Audience", "geo+audience": "Geo + audience"}

rows = []
for cgid, s in sheet.items():
    l = lab[cgid]
    spend, cpa = float(s["spend"]), float(s["cpa"])
    goal = float(s["goal_value"])
    kw = l["has_keyword_layer"] == "true"
    pp = l["pp_label"] == "peak_performance"
    ver = PPV.get(l["pp_version"] or "", "")
    rows.append({
        "Advertiser": s["advertiser"],
        "Campaign": s["campaign_name"],
        "Campaign group id": cgid,
        "Spend": spend,
        "Actual CPA": cpa,
        "CPA goal": goal,
        "Actual CPA vs goal": (cpa / goal) if goal and cpa else None,
        "Peak Performance": ver if pp else ("Not in the classifier" if l["pp_label"] == "not_in_view" else "No"),
        "Keyword layer": ("Yes" if kw else "No") if pp else "",
        "Can reach top intent band": ("No" if (l["pp_version"] == "v2_fangorn" and not kw) else "Yes") if pp else "",
        "Targeting restriction": RESTRICT.get(l["restriction_levels"], l["restriction_levels"]),
        "Intent gate set": {"true": "Yes", "false": "No", "": ""}[l["any_hhst_gated"]],
        "Stage 1 campaigns in group": int(l["n_stage1_campaigns"]) if l["n_stage1_campaigns"] else None,
        "Claim status": s["already spelunked? yes/not a good candidate"] or "",
    })

df = pd.DataFrame(rows).sort_values("Spend", ascending=False)
claim = df[(df["Peak Performance"].isin(["v1", "v2 (Fangorn)"])) & (df["Claim status"] == "")].copy()

FMTS = {"Spend": FMT.USD0, "Actual CPA": FMT.USD, "CPA goal": FMT.USD,
        "Actual CPA vs goal": FMT.MULT, "Stage 1 campaigns in group": FMT.INT}

wb = MntnWorkbook(
    title="Peak Performance campaigns missing CPA",
    ticket="AUDI-1083",
    subtitle="The 8/21 Insight Spelunking bad-CPA list, labelled by audience setup",
    period="Spend and CPA: 2026-07-01 to 2026-08-19",
    generated="2026-08-19",
)

wb.table(
    "Claim list", claim,
    finding="159 of the 251 bad-CPA campaigns run Peak Performance and are unclaimed",
    method="Peak Performance = a DS13 (v1) or DS46 (v2) leaf in the group's Stage-1 audience expression. Rows already spelunked or marked not a good candidate sit on the next tab.",
    formats=FMTS,
    heat={"Spend": "high", "Actual CPA vs goal": "high"},
    kind="headline",
    toc="Unclaimed Peak Performance campaigns, worst spend first",
)

wb.table(
    "All 251 rows", df,
    finding="The sheet's peak perf column agrees with the classifier on 250 of 251 rows",
    method="The one disagreement is LifeVac (group 123213), marked peak perf but running the keyword layer alone with no vertical anchor. One group has no Live Stage-1 campaign to label.",
    formats=FMTS,
    kind="data",
    toc="Every row on the bad-CPA tab, including claimed and excluded",
)

wb.glossary(
    "Read me",
    intro="What each column means and where the labels come from.",
    rows=[
        ("Peak Performance", "The vertical-anchor product inside MNTN Matched. v1 is the legacy categorical version; v2 is Fangorn, which scores continuously. A campaign runs one or the other, never both."),
        ("Keyword layer", "Whether the campaign also targets MNTN Matched keywords alongside the vertical anchor."),
        ("Can reach top intent band", "A v2 campaign without the keyword layer tops out at the 8000 band, so its delivery can never come from the highest intent band. Verified 2026-07-08."),
        ("Targeting restriction", "Whether geo, audience, both, or neither narrows the campaign beyond its base audience."),
        ("Intent gate set", "Whether a household score threshold is set. With no threshold the bidder bids on everyone, scored or not, and the intent scoring has no effect."),
        ("Actual CPA vs goal", "Actual CPA divided by the CPA goal. 3.0x means the campaign is paying three times its target per conversion."),
        ("Campaign group id", "The client-facing campaign, the cgid column on the source tab."),
        ("Claim status", "Copied from the source tab. Blank means unclaimed."),
    ],
)

wb.sql("Query", open(f"{BASE}/queries/audi_1083_pp_label_bad_cpa.sql").read(),
       note="Run through .claude/scripts/bq_run.sh against dw-main-silver.")

wb.notes(
    "Method & caveats",
    blocks=[
        ("Scope of the classifier",
         "audience.mm_campaign_classifier covers Live campaigns at funnel level 1 (Stage-1 prospecting) only. A paused campaign, or one whose group has no Stage-1 campaign, gets no label. That is not evidence it is not Peak Performance."),
        ("Grain",
         "The source tab is keyed on the client-facing campaign group; the classifier is keyed on the internal Stage-1 campaign. Ten of the 251 groups have a child group, so each group was expanded to itself plus its children before the audience flags were rolled up."),
        ("The obvious filter is wrong",
         "tiers_reachable LIKE '%PP%' looks like the Peak Performance filter and is not: one of the tier labels reads 'HI-MI-MaxReach (no PP)' and matches the pattern, returning 6,226 campaigns against a true 2,651. Use has_ds13 OR has_ds46, and coalesce both to false because they are nullable."),
        ("Snapshot",
         "The classifier rebuilds daily and was last rebuilt 2026-08-19 00:13 UTC. Audience expressions are edited continuously, so a label can move between the rebuild and the session."),
        ("Live footprint",
         "2,651 Live Stage-1 campaigns across 1,087 advertisers carry Peak Performance as of 2026-08-19."),
    ],
)

wb.cover(takeaways=[
    "161 of the 251 bad-CPA campaigns run Peak Performance, 128 of them on v2; 159 are still unclaimed.",
    "The sheet's own peak perf column checks out, agreeing with the classifier on 250 of 251 rows.",
    "22 of the Peak Performance campaigns carry no keyword layer, and 18 of those can never reach the top intent band.",
])

print(wb.save_drive("AUDI-1083", "Peak Performance Bad CPA Claim List"))
