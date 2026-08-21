"""Build the AUDI-1215 branded workbook: ElevenLabs CGID 122748 lift pre/post the 6/30 audience change."""
import sys
import pandas as pd

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from lib.mntn_xlsx import MntnWorkbook, FMT

T = "tickets/audi_1215_elevenlabs_lift_post_audience_change"
PCT4 = "0.0000%"
wb = MntnWorkbook("ElevenLabs Incrementality: Pre vs Post the June 30 Audience Change", "AUDI-1215")

itt = pd.DataFrame([
    ["Pre (6/23-6/30)", "Visits", 4200746, 441242, 0.009161, 0.008243, 0.00091834, 0.1114, 6.38, "1.7e-10", "Yes"],
    ["Post (7/11-8/13)", "Visits", 6639322, 672152, 0.001578, 0.001355, 0.00022310, 0.1646, 4.70, "2.6e-06", "Yes"],
    ["Pre (6/23-6/30)", "Conversions", 4200746, 441242, 0.000179, 0.000161, 0.0000181, 0.1125, 0.90, "0.37", "No"],
    ["Post (7/11-8/13)", "Conversions", 6639322, 672152, 0.0000401, 0.0000298, 0.0000103, 0.3465, 1.45, "0.15", "No"],
], columns=["Period", "Outcome", "Reached IPs", "Holdout IPs", "Reached rate", "Holdout rate", "Abs lift", "Rel lift", "z", "p", "Significant"])
wb.table("Lift Pre Vs Post", itt,
    finding="Visit lift positive and significant both periods: +11.1% pre, +16.5% post; conversion lift not significant in either.",
    method="Randomized ghost-bid holdout, intent-to-treat, campaign group 122748. IPs counted at first bid; outcomes within 7 days. Blackout 7/1-7/10 (multiple overlapping changes) excluded.",
    formats={"Reached rate": PCT4, "Holdout rate": PCT4, "Abs lift": PCT4, "Rel lift": FMT.PCT2},
    kind="headline", toc="The headline lift table, both periods", query="audi_1215_ghost_itt_prepost.sql")

delta = pd.DataFrame([
    ["Visits, relative lift", "+11.1%", "+16.5%", "+1.21", "0.23", "No", "No detectable change at these counts."],
    ["Visits, incremental volume", "0.092%", "0.022%", "-4.59", "4.4e-06", "Yes", "Baseline visit rate fell 6x; same relative lift, ~4x fewer incremental visits per reached IP."],
    ["Conversions, relative lift", "+11%", "+35%", "+0.73", "0.47", "No", "Underpowered at the 0.06% conversion base."],
    ["Conversions, holdout check", "+249%", "+123%", "-8.98", "2.6e-19", "Yes", "Fell 36%; attribution carryover understates the decline."],
], columns=["Comparison", "Pre", "Post", "z", "p", "Significant", "Read"])
wb.table("Did Lift Change", delta,
    finding="Relative visit lift did not detectably change; incremental volume fell ~4x and the powered conversion read fell 36%.",
    method="Post minus pre. Relative changes tested on the log risk ratio, absolute on the rate difference. The ghost-bid change test is underpowered for a 36% swing, so its null is not stability.",
    kind="headline", toc="Did the audience change move lift?", widths={"Read": 52})

hold = pd.DataFrame([
    ["Pre (6/1-6/30)", 13311, 6520, 3601, 1554, 2.492, "+230% to +269%", "Yes"],
    ["Post (7/11-7/31)", 3595, 2409, 1223, 779, 1.229, "+106% to +142%", "Yes"],
    ["Post vs pre change", None, None, None, None, -0.361, "-30% to -42%", "Yes (decline)"],
], columns=["Window", "Reached conv", "Reached converters", "Holdout conv", "Holdout converters", "Converter lift", "95% CI", "Significant"])
wb.table("Conversion Holdout Check", hold,
    finding="Conversion lift fell 36% after the changes: +249% over holdout pre vs +123% post (p=2.6e-19).",
    method="Fixed 10% holdout lineage, membership static across periods. Post is 100% this campaign group. 27.8% of post conversions attach to pre impressions via the 43-day lookback, flattering post.",
    formats={"Converter lift": FMT.PCT2},
    kind="data", toc="The well-powered conversion instrument", query="audi_1215_holdout_prepost.sql")

freq = pd.DataFrame([
    ["1 bid", 4707336, 498260, 0.0933, "1.3e-04", "Yes"],
    ["2-3 bids", 4487450, 395507, 0.1786, "3.4e-08", "Yes"],
    ["4-10 bids", 4817601, 419458, 0.2034, "2.6e-08", "Yes"],
    ["11+ bids", 2842252, 435178, -0.1773, "6.6e-06", "Yes (negative)"],
], columns=["Exposure band", "Reached IPs", "Holdout IPs", "Visit lift", "p", "Significant"])
wb.table("Frequency Vs Lift", freq,
    finding="Lift peaks at 2-10 exposures (+18-20%) and turns significantly negative at 11+ (-17.7%).",
    method="Ghost-bid results by bid-count stratum, all dates 6/22-8/20. 70% of households see 3 or fewer impressions; the 11+ band absorbs spend at negative lift.",
    formats={"Visit lift": FMT.PCT2},
    kind="data", toc="Where frequency helps and where it hurts", query="audi_1215_gold_strata.sql")

wk = pd.read_csv(f"{T}/outputs/audi_1215_ghost_itt_weekly.csv")
wb.table("Weekly Lift Trend", wk,
    finding="Weekly visit lift holds at +7% to +23% across the change; no week shows the collapse attributed metrics show.",
    method="Same cohorts by ISO week of first bid. Weeks 27-28 straddle the blackout; the period table is authoritative. Week 29 holdout share 0.088 is under band, so its +20% reads high.",
    kind="data", toc="Week by week trajectory", query="audi_1215_ghost_itt_prepost.sql")

tl = pd.DataFrame([
    ["2026-05-04 21:47", "Audience 77883 'Agents Targeting - Growth' attached (the pre state)"],
    ["2026-06-30 15:57", "Exclusion lookbacks widened 30d to 90d (conversion + pageview blocks)"],
    ["2026-06-30 15:58", "AUDIENCE SWAP to 88532 'MNTN-suggested precision audience changes': include collapses from ShareThis(4 cats) + MNTN Matched keywords(33) + LiveRamp(112 segments) to LiveRamp-only 6 AI/ML + B2B segments; geo widens to US-wide"],
    ["2026-07-01 20:32", "CRM identity-graph customer suppression added to excludes"],
    ["2026-07-09 14:07", "Geo restored to a 28-state list (US-wide ran only inside the blackout)"],
    ["2026-07-16 20:48", "Three custom ElevenLabs LiveRamp segments added (intent / job function / industry)"],
    ["2026-07-24 21:55", "MNTN Campaigns segment added to include"],
    ["2026-07-29 20:34", "All six campaigns' expressions rewritten; MNTN Taxonomy source added"],
    ["2026-08-20 14:10", "Campaign group paused (status change; the $770K pause)"],
], columns=["When (UTC)", "What changed"])
wb.table("What Changed", tl,
    finding="The 6/30 swap was one of six targeting changes through 7/29, so the post period averages four audience states.",
    method="Recovered from the platform change archives; every version is recoverable. The 10% holdout construction was unchanged throughout, so both periods rest on the same assignment.",
    kind="detail", toc="Exact change timeline from the archives", widths={"What changed": 72},
    query="audi_1215_audience_change_timeline.sql")

panel = pd.DataFrame([
    ["Attributed visitor rate per unique", 0.032886, 0.012034, -0.634],
    ["Attributed CVR per unique", 0.001595, 0.000598, -0.625],
    ["Visits per 1,000 impressions", 5.28, 2.67, -0.495],
    ["Distinct households reached (index, flat imps)", 1.00, 1.126, 0.126],
], columns=["Attributed metric (not incrementality)", "Pre", "Post", "Change"])
wb.table("Attributed Panel", panel,
    finding="Attributed visit and conversion rates fell ~60% at flat delivery; 13% more households reached, responding far less.",
    method="Campaign-group totals, pre 6/1-6/30 vs post 7/11-8/10, blackout excluded. Attributed metrics credit exposure; context only, never the lift verdict. The slide begins days before 6/30.",
    formats={"Pre": "0.0000", "Post": "0.0000", "Change": FMT.PCT2},
    kind="detail", toc="What the dashboards show (attributed, context only)", query="audi_1215_daily_panel.sql")

wb.notes("How Ghost Bidding Works", [
    ("Why attribution is not enough", "Attribution credits a visit to an ad whenever the ad touched it; it cannot say whether the visit would have happened anyway. Incrementality answers the harder question: how many visits happened only because the ad ran. It adds to attribution rather than replacing it."),
    ("Random split", "Every audience is randomly split by household: roughly 90% eligible, 10% holdout. Same standard of evidence as a clinical trial."),
    ("Same auctions", "The bidder values every auction identically for both groups. Same campaigns, same targeting, same bidding logic."),
    ("One difference", "For the eligible group the campaign serves as normal. For the holdout, the bid the bidder would have placed is recorded and never sent: the ghost bid. No ad is ever shown and no media is spent on the control group."),
    ("Compare", "Lift = (served-group visit rate minus holdout visit rate) divided by the holdout visit rate. The gap is the effect the ads caused, with a confidence interval on every estimate."),
    ("Always on, at full scale", "Runs inside the bidder across prospecting campaigns with no test setup and no geo split. Live since June 2026; 1,100+ advertisers have measurable ghost-bid lift data."),
    ("Conservative by design", "Results are reported only where coverage and statistical checks pass, and visits count for only seven days after each ad opportunity, which understates lift rather than inflating it."),
    ("Reading a lift number", "A +12% lift means: per 100 visits the holdout generates on its own, the served group generates 112. The 12 extra visits are the ones the ads caused."),
], intro="The measurement behind every lift number in this workbook. Print version: the ghost bidding one-pager PDF filed with this ticket (artifacts/audi_1172_incrementality_one_sheeter.pdf).", toc="How the measurement works")

wb.glossary("Read Me", [
    ("Incrementality vs attribution", "Incrementality = extra visits or conversions caused by ads, measured against a randomized holdout. Attribution = credit assigned after exposure; it always reads far higher."),
    ("Ghost-bid holdout", "10% of IPs are randomly held out per advertiser; the bidder logs the bid it WOULD have made. Comparing reached vs held-out IPs gives a clean randomized lift read."),
    ("Intent-to-treat (ITT)", "Compares everyone we bid on vs the holdout, whether or not the ad won. Avoids win-selection bias; slightly understates the effect of ads actually served."),
    ("Relative vs absolute lift", "Relative: percent above the holdout rate. Absolute: extra visits per reached IP (shown in percentage points). Relative can hold while absolute collapses if the audience's base rate drops."),
    ("Converter lift", "Reached converter rate versus holdout converter rate in the fixed-holdout instrument, shown as a percentage above holdout; +249% means reached IPs converted 3.5 times as often."),
    ("Blackout (7/1-7/10)", "Window with multiple overlapping campaign and audience changes, excluded from both sides per Matt Brorby's convention."),
    ("Exposure band", "IPs grouped by how many times we bid for them; a proxy for ad frequency."),
    ("Why two instruments", "The ghost-bid test is the cleanest design but thin on pre-period days; the fixed-holdout lineage has full months and power on conversions. Agreement across both is the standard of evidence."),
], intro="One page of terms; every number sheet names its source query on the Queries tab.")

wb.sql_dir("Queries", f"{T}/queries",
    order=["audi_1215_ghost_itt_prepost.sql", "audi_1215_holdout_prepost.sql", "audi_1215_gold_strata.sql"],
    note="All queries ran read-only on the us-central1 reservation via bq_run.sh.")

wb.notes("Method And Caveats", [
    ("The verdict standard", "Every number here was independently reproduced by a second fresh query pass (exact match) and adversarially reviewed before shipping. The review verdict reshaped the headline: lead with the decline evidence, not the flat relative-lift read."),
    ("Pre-window is thin", "The ghost-bid table starts 6/22, so pre = 8 anchor days. Early-day cohorts skew toward returning, higher-propensity IPs, inflating both arms' pre base rates. Randomization keeps each period's lift valid; cross-period ABSOLUTE comparisons carry this composition caveat."),
    ("Holdout depletion", "Post-period holdout share sits at the valid band floor (0.092; week 29 below at 0.088). Direction of bias: post lift reads HIGH. This strengthens, not weakens, the no-improvement conclusion."),
    ("Power on the change test", "The ghost-bid change test can only detect large swings in the lift ratio at these counts; the 36% conversion-lift decline the holdout instrument measured sits inside its blind spot. Its null is 'cannot see', never 'no change'."),
    ("Attribution carryover flatters post", "27.8% of post-window conversions in the holdout instrument attach to impressions served before 6/30 under the 43-day lookback, so the measured 36% decline is a lower bound."),
    ("One bundle, not one change", "Six targeting changes landed 6/30-7/29. Every pre/post number measures the bundle; nothing here isolates the audience swap alone."),
    ("Customer's 'no lift' claim", "Their 6-week multi-angle analysis predates these changes and reads conversions, where the clean test is underpowered at their 0.06% B2B base rate (TI-1044: ~$2M/mo needed to detect 5%). Visits show real, significant lift both periods; conversions cannot be resolved at this spend either way."),
], intro="Instruments: randomized ghost-bid ITT (silver enriched lift tables, Beeswax leg) and the fixed 10% MD5 holdout lineage (gold reporting lift views).")

wb.cover(takeaways=[
    "Visit lift stayed significant (+11% pre, +16% post) but incremental volume fell ~4x: the new audience rarely visits at baseline.",
    "Conversion lift fell 36% on the one powered instrument (3.5x to 2.2x, p<1e-18); the clean test is unpowered at the 0.06% B2B base.",
    "Lift peaks at 2-10 exposures and turns negative at 11+; 70% of households see 3 or fewer impressions.",
])
print(wb.save_drive("AUDI-1215", "ElevenLabs Lift Pre Post Audience Change"))
