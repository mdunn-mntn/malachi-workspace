---
doc_type: ticket
title: "AUDI-1141: MM vs 3P prospecting performance by sales vertical (6mo)"
status: done
date: 2026-07-21
summary: "Sales request: compare MNTN Matched vs 3P-segment prospecting performance across the 8 sales verticals, trailing 6 months."
result: "MM (gated) beats 3P ~6x on visit rate and ~4x on cost-per-visit for the median advertiser, in all 8 verticals. Un-gated MM and audience-restricted MM both fall well below gated MM. 3P only looks competitive when impression-pooled, which is ~39% one account."
keywords: ["mm vs 3p", "sales vertical", "prospecting", "intent gate", "cpv", "ivr", "roas", "scorecard", "audi-1141", "advertiser-weighted median"]
---

## TL;DR

**Q:** MM vs 3P prospecting performance across the 8 sales verticals, trailing 6 months (AUDI-1141).

**A:** MM (gated) beats 3P ~6.6x on visit rate and ~4x on cost-per-visit for the median advertiser in all 8 verticals; un-gated and audience-restricted MM both fall below gated MM; 3P only looks competitive impression-pooled, which is ~39% one account.

**How:** Classified S1 prospecting campaigns (objective_id=1, funnel_level=1, delivered in trailing 180d) from the latest targeted segment into MM / MM restricted / 3P / Neither via the TI-999 Pass 26 LCA tree-walk (OR-additive vs AND-narrowing) plus geo-narrowing detection; split broad MM by the HHST intent gate; rolled up to 8 sales verticals via an fpa_advertiser_verticals crosswalk; reported advertiser-weighted median (20k-impression floor) as headline with impression-pooled as cross-check.

**Tables:** `audience_segments`, `fpa_advertiser_verticals`, `household_score_threshold_archives`

**Learned:**
- MM (gated) IVR 0.46% beats 3P IVR 0.07% ~6.6x and ~4x on CPV, in all 8 verticals; MM (all) blended IVR 0.28% still beats 3P ~4x IVR / ~3x CPV / ~2.3x ROAS.
- 3P only looks competitive impression-pooled (3P pooled IVR 0.62%) because ~39% of 3P impressions come from one large account (advertiser 31357); advertiser-weighted median is the correct lens.
- The intent gate is the single biggest lever: MM (no gate) 0.13% and MM restricted 0.18% both sit below MM (gated) 0.46%.

**Reuse when:** MM vs 3P performance by vertical; is gated MM a cherry-picked / perfect-scenario average; does score-limited MM behave like 3P; cost-per-visit MM vs 3P; ROAS MM vs 3P prospecting.

---

# AUDI-1141: MM vs 3P prospecting performance by sales vertical

Jira: https://mntn.atlassian.net/browse/AUDI-1141
Requested via Slack (Sales), 2026-07-20.
Status: analysis complete, shareable workbook drafted.

## 1. Introduction

Sales asked for a MNTN Matched (MM) vs 3P-segment prospecting comparison, filterable by the 8 sales
verticals, over the trailing 6 months. "MM vs 3P" is not straightforward: what counts as MM, how to
treat modified/geo-restricted campaigns, and how to treat score-limited MM all change the answer. This
ticket resolves those empirically and delivers a per-vertical scorecard.

8 sales verticals: ProServ, Education, Retail/Ecom, Gaming/Entertainment, Telco & Tech,
Restaurants/Dining, CPG & Health, Auto Travel & Hospitality.

## 2. The problem

A naive "campaigns using MM vs campaigns using 3P" split is wrong. About 85% of campaigns that carry a
3P segment join it with OR (additive reach), leaving MM to do the scoring underneath. Only an AND-required
3P (or a narrow geo) actually restricts the audience. The comparison has to separate additive 3P
(effectively MM) from restrictive 3P, and control for the intent score gate.

## 3. Method

### Cohort
- S1 prospecting campaigns (objective_id=1, funnel_level=1, deleted=FALSE) that delivered (impressions>0)
  in the trailing 180 days.
- Classified from the latest bidder-facing targeted segment (audience.audience_segments,
  expression_type_id=2, is_targeted=TRUE, most recent per campaign).
- 8,202 campaigns after dropping the "Neither" group; 7,138 in the scored MM/3P groups.

### Buckets (AND vs OR semantics, per TI-999 Pass 26 LCA tree-walk)
- MM signal = DS13/19/38/46 positive. 3P = DS17/18/35 positive.
- A JS UDF walks the categories op-tree and finds the lowest common ancestor of each MM and 3P positive
  clause: LCA = OR means additive, LCA = AND means intersecting/narrowing.
- Narrow geo = zip (location_type_id=7) or city (6) in the geos INCLUDE block (before the first
  "op":"not"), or a geo_radii clause. Broad = national or DMA-tier.
- Groups:
  - MM: has MM, 3P (if any) is OR-additive, geo broad. Split by intent gate into MM (gated) and MM (no gate).
  - MM restricted: MM narrowed by an AND-include 3P (or mixed), or by narrow geo.
  - 3P: 3P positive, no MM.
  - Neither: CRM/1P/geo-only (excluded from the comparison).
- Spend mix: MM 51%, MM restricted 24%, 3P 15%, Neither 10%. Among MM+3P campaigns, 85% OR-additive,
  15% AND/mixed (matches TI-999: intersective 3P is rare).

### Intent gate
- household_score_threshold_archives, threshold>0 = intent gate on. capped = gate on for the majority of
  in-window writes. The gate is the norm on MM prospecting (median gated-fraction 0.99).

### Vertical rollup
- advertiser -> fpa_advertiser_verticals type=0 parent (37 canonical parents) -> 8 sales buckets via a
  crosswalk in the SQL. 3 orphans (News & Politics, Non-Profits, Holidays & Events) -> Other/Unmapped.
  Crosswalk is interim and needs RevOps sign-off.

### Metrics (all rates over impressions, per TI-999 Pass 26)
- IVR = visits/impressions (visits = views+clicks). CVR = conversions/impressions. CTR = clicks/impressions.
- CPV = spend/visits. CPM = spend/impressions*1000. ROAS = revenue/spend.
- Advertiser-weighted median is the headline (each advertiser one vote, 20k-impression floor); pooled
  (impression/spend-weighted) reported as a cross-check.

## 4. Findings

Headline (advertiser-weighted median, all verticals):

| Group | n adv | n camp | IVR (median) | CPV (median) | ROAS (median)* |
|---|---|---|---|---|---|
| MM (all) | 2,560 | 6,004 | 0.28% | $12.52 | 0.92 |
| MM (gated) | 1,262 | 2,248 | 0.46% | $9.13 | 0.92 |
| MM (no gate) | 144 | 289 | 0.13% | $21.84 | 0.56 |
| MM restricted | 1,393 | 3,467 | 0.18% | $19.02 | 0.94 |
| 3P | 438 | 1,134 | 0.07% | $37.18 | 0.40 |

- Two lenses (Jon requested both): **MM (all)** blends every MM campaign = the realistic average;
  **MM (gated)** is the best-configured subset. Both beat 3P clearly, which answers the "is gated a
  perfect-scenario average?" concern: MM (all) still beats 3P ~4x IVR / ~3x CPV / ~2.3x ROAS.
- Gated MM beats 3P ~6.6x on visit rate and ~4x on CPV. Both restrictions hurt: MM (no gate) 0.13% and
  MM restricted 0.18% sit below MM (gated) 0.46%; the intent gate is the single biggest lever.
- MM leads IVR and CPV in all 8 verticals under both lenses.
- **Intent gate clarified (for the requester):** an MM campaign only bids on model-scored high-intent
  IPs when the score threshold is >0. Some advertisers run it at 0 (Max Reach; many short flights that
  lower it for deliverability; or an over-narrowed HI pool exhausted quickly), which bypasses the model
  and bids broadly like a 3P segment. The gate is the threshold setting, not the scoring model.

Pooled cross-check: impression-pooled 3P IVR (0.62%) exceeds MM-gated pooled, but ~39% of 3P impressions
come from one large account (advertiser 31357) with an unusually high visit rate. Per-advertiser (median)
is the correct lens and favors MM decisively.

ROAS is directional only: prospecting/last-touch, revenue concentrates in retargeting (excluded), and
some cells carry pixel artifacts (one ProServ advertiser shows >800x). Use median, never mean.

## 5. Deliverables
- outputs/audi_1141_mm_vs_3p_scorecard.xlsx: shareable workbook (upload to Google Sheets). Tabs: Read me,
  MM vs 3P by vertical (blended MM-all vs 3P; IVR/CPV/ROAS + IVR-advantage + ROAS-advantage + counts),
  MM gated vs 3P by vertical (best case, same columns), Full scorecard (adds CPA = cost per conversion),
  Overall, Campaign detail (advertiser names, pivotable), Queries. Rebuild: artifacts/audi_1141_build_xlsx.py.
  CPA is the only metric added from the TI-1037 Mode dashboard set (reach/frequency intentionally omitted).
- outputs/audi_1141_campaign_grain.csv, audi_1141_scorecard_overall.csv, audi_1141_scorecard_by_vertical.csv,
  audi_1141_scorecard2_overall.csv, audi_1141_scorecard2_by_vertical.csv, audi_1141_advertiser_names.csv
- artifacts/audi_1141_chart_overall.png, audi_1141_chart_ivr_by_vertical.png, audi_1141_chart_cpv_by_vertical.png
- artifacts/audi_1141_findings.md
- queries/audi_1141_cohort_scorecard.sql, artifacts/audi_1141_aggregate.py, artifacts/generate_charts.py

### Drive
- Live workbook synced to `My Drive/Tickets/AUDI-1141/AUDI-1141 MM vs 3P Scorecard.xlsx` (via the local
  Drive mount). Convention: folder = ticket number only; file = `<ticket> <description>.<ext>`. Rebuild
  `build_xlsx.py` then copy over the same path to update in place.

## 6. Questions answered
- MM vs 3P by vertical, 6mo: MM (gated) wins IVR and CPV in every vertical; scorecard delivered.
- Does score-limited MM behave like 3P: un-gated MM (0.13%) falls toward 3P (0.07%); gated MM is 0.46%.
- Does narrowing MM hurt: yes, MM restricted (0.18%) is well below broad MM (0.46%).
- Is 3P ever better: only impression-pooled, which is ~39% one account.

## 7. Data documentation updates
- 3P bought-interest DS set = DS17/18/35.
- AND vs OR classification via TI-999 Pass 26 LCA tree-walk (additive 3P stays MM; only AND-include narrows).
- Geo detection: location_type_id 7=zip, 6=city, 5=state, 3/4=DMA, 2=country; INCLUDE block = geos before
  first "op":"not"; geo_radii = radius targeting.
- Rate conventions (TI-999 Pass 26): IVR/CVR/CTR are all over impressions.
- Intent gate is the norm on MM prospecting.

## 8. Open items
- Vertical crosswalk needs RevOps sign-off (B2B, Food & Beverage are judgment calls; 3 orphans).
- Confirm KPI/attribution lens (currently default non-competing, prospecting-only).

---

## 9. CPA follow-up for the pitch deck (2026-08-20, Jon Zucker via Slack)

**Ask:** add a CPA comparison for non-revenue-driving customers (e.g. B2B), for the new pitch deck.

**CPA was already delivered.** `CPA (median)` and `CPA (pooled)` are on the **Full scorecard** and
**Overall** tabs, per vertical, and defined in Read me. What is missing is the CPA column on the two
headline tabs ("MM vs 3P by vertical", "MM gated vs 3P by vertical"), which carry IVR / CPV / ROAS only.
That is a one-column edit to `compare_tab()` in `artifacts/audi_1141_build_xlsx.py`.

**The finding that matters: the CPA advantage does not survive the cohort Jon asked for.**
Recomputed from `outputs/audi_1141_campaign_grain.csv` (same 20k-impression floor, same
advertiser-weighted median; the all-advertiser row reproduces the shipped workbook exactly —
315.12 / 252.74 / 318.65 / 431.73 / 657.87). Full table: `outputs/audi_1141_cpa_nonrevenue_b2b.csv`.

| Cut | MM (all) CPA | 3P CPA | MM advantage |
|---|---:|---:|---:|
| All advertisers (what the deck has today) | $315.12 | $657.87 | **2.09x** |
| Non-revenue advertisers (conversions, no revenue) | $584.28 | $679.24 | **1.16x** |
| B2B Software & Services vertical | $1,187.71 | $1,255.46 | **1.06x** |
| B2B **and** non-revenue | $1,534.71 | $1,566.35 | **1.02x — a tie** |

Non-revenue = advertiser with zero tracked revenue across the window. `B2B Software & Services` is a
`vertical_name`, NOT one of the 8 sales verticals; the crosswalk folds it in and still needs RevOps
sign-off (§8), so "B2B" is not a cut the workbook exposes today.

**Three reasons this is not deck-safe as a CPA slide:**

1. **The gate inverts.** In B2B, MM (no gate) median CPA $724.71 BEATS MM (gated) $1,043.73 — the
   opposite of the deck's core "the intent gate is the biggest lever" line, which holds on IVR/CPV.
   n=6 advertisers, so it is noise, not a counter-finding; but it is noise that contradicts the claim.
2. **Median and pooled disagree by ~2.5x.** B2B pooled: MM $128.58 vs 3P $330.60 = 2.6x. B2B median:
   1.06x. Same data, same cohort. On IVR the two lenses agreed; on CPA they do not, because conversion
   counts are pixel-dependent and whale-skewed.
3. **Small n and unequal pixel coverage.** B2B rests on 22 3P advertisers vs 115 MM. Only **53.8%** of
   qualifying 3P advertisers have any conversion at all vs **78.0%** of MM (42.5% vs 64.2% on the
   non-revenue cut) — advertisers with no conversion pixel drop out of CPA entirely. That selection
   flatters 3P (its zero-conversion half is excluded), so the MM win is conservative; but it also means
   the CPA population is not the IVR/CPV population and the two slides are not the same advertisers.

**Rebuilt 2026-08-20 on the shared format.** `artifacts/audi_1141_build_xlsx.py` now builds on
`lib/mntn_xlsx.MntnWorkbook` instead of hand-rolled openpyxl (the module was generalized FROM this
builder on 2026-07-21, so the original predated the standard). Sheets: Overview / MM vs 3P by vertical /
MM gated vs 3P by vertical / **CPA on non-revenue accounts** (new) / Full scorecard / Overall / Campaign
detail / Read me / Queries / Method & caveats. CPA and a CPA-advantage column were added to both
by-vertical tabs. `artifacts/audi_1141_aggregate.py` now also emits
`outputs/audi_1141_scorecard_nonrevenue.csv`. Numbers are unchanged (same Jul-20 campaign-grain CSV);
the all-advertiser row still reproduces the shipped figures exactly.

**Drive now holds TWO files** in `My Drive/Tickets/AUDI-1141/`: the new
`AUDI-1141 MM vs 3P Scorecard.xlsx` (convention-named) and the original `MM vs 3P Scorecard.xlsx`
(Jul 21 build, opened 2026-08-20 08:26). The old one was left in place deliberately — it may be linked
from elsewhere. Delete it only once nothing points at it.

**Recommendation:** ship CPA on the all-advertiser cut where the 2.09x holds, and do NOT put a B2B or
non-revenue CPA slide in the deck on this data. If the B2B claim is needed, it is a new pull, not a
column: a B2B-specific cohort with RevOps-signed verticals, a conversion-pixel coverage screen, and a
larger 3P n.

**Two process flags raised with the requester:**
- **Lens switching.** Selecting gated vs ungated per slide by which reads better is cherry-picking in an
  external artifact. Both lenses exist because Jon asked for both (§4); the honest use is to pick one and
  hold it across the whole deck, labelled. Note the switch would run the other way on B2B CPA.
- **Staleness and the copy.** The workbook was built 2026-07-21 over a trailing-180d window ending
  2026-07-20 (~2026-01-21 → 2026-07-20); it is one month old. The link Jon circulated
  (`1m5RKXYN219eGH_JiYwPdL9pmzwmerunJ`) does not resolve against this Drive account, so it is a separate
  copy, not `My Drive/Tickets/AUDI-1141/MM vs 3P Scorecard.xlsx` — rebuilding the source will not update it.

**Open follow-up (Alex Knorr, Slack 2026-08-20 13:54): "how do we tell which campaigns in the sheet are
peak performance?"** This workbook cannot answer that. It was built 2026-07-21 with its own inline
MM/3P classification; `dw-main-silver.audience.mm_campaign_classifier` (AUDI-1083) only went live
2026-07-24. The workbook's "MM" bucket is DS-presence plus restriction, NOT engine — Peak Performance is
`mm_engine IN ('peak_performance_v1','fangorn_v2')` on the classifier view. Adding an engine column to
the Campaign detail tab is a small join on `campaign_id`, not a re-run. Not done: which sheet Alex meant
was not confirmed.

---

## 10. Data refresh 2026-08-20 (window moved to 2026-02-21 → 2026-08-20)

Re-ran `queries/audi_1141_cohort_scorecard.sql` (2.29 GB scanned, 23s), refreshed
`outputs/audi_1141_advertiser_names.csv` from `dw-main-bronze.integrationprod.advertisers`, re-ran
`aggregate.py` and `build_xlsx.py`. Previous pull kept at `outputs/audi_1141_campaign_grain_2026_07_20.csv`.
8,120 campaigns (was 8,202); 7,045 non-Neither. **MM still wins IVR and CPV in all 9 vertical rows on
both lenses**, so the headline claim holds.

**Two SQL/runner gotchas found:** (a) the file's first line cannot start with `--` or `bq` parses it as a
flag — the filename header that `mntn_xlsx` needs for query deep-links now sits at the END of the file as
`-- source: audi_1141_cohort_scorecard.sql`; (b) this query needs `--nouse_legacy_sql` (it opens with
`CREATE TEMP FUNCTION`) and an explicit `--project_id=dw-main-silver`, or `bq` picks up
`mntn-coredw-prod` from the gcloud default and fails on `bigquery.jobs.create`.

**Headline movement (advertiser-weighted median):**

| | Jul-20 window | Aug-20 window |
|---|---|---|
| MM (gated) IVR | 0.46% | 0.43% |
| MM (no gate) IVR | 0.13% | 0.14% |
| 3P IVR | 0.07% | 0.07% |
| Gated-vs-3P IVR advantage | 6.6x | 6.1x |

**The CPA cohort finding got STRONGER, not weaker:**

| Cut | MM (all) CPA | 3P CPA | MM advantage |
|---|---:|---:|---:|
| All advertisers | $338.82 | $732.50 | **2.16x** |
| Non-revenue advertisers | $644.29 | $793.81 | **1.23x** |
| B2B Software & Services | $1,267.15 | $1,290.66 | **1.02x** |
| B2B **and** non-revenue | $1,984.21 | $1,533.66 | **0.77x — 3P is CHEAPER** |

On the exact cohort the pitch deck wants (B2B, no revenue to compute ROAS from), MM's CPA is now
**worse** than 3P on the median advertiser. n is small (19 3P advertisers with a conversion vs 86 MM) and
pooled runs the other way (MM $371.78 vs 3P $1,129.04), which is itself the point: the cohort is too thin
and too lens-sensitive to carry an external claim. The §9 recommendation stands and hardens — ship CPA on
the all-advertiser cut, do not build a B2B or non-revenue CPA slide on this data.

## 11. The shipped 2026-07-21 workbook had HAND EDITS the first rebuild silently dropped

Read the old `My Drive/Tickets/AUDI-1141/MM vs 3P Scorecard.xlsx` cell-by-cell only AFTER rebuilding
(should have been before). Its tabs match the builder's, but four things in it were not builder output:

1. **Verticals were re-sorted descending by IVR advantage**, not alphabetical. The standing rank-descending
   rule ([[feedback_rank_desc_always]]) applies and the first rebuild broke it. Fixed in `compare()`.
2. **An inline footnote on the Education row: "*most education does not optimize towards a ROAS."**
   Carried forward as a Method & caveats block ("Education ROAS is not a target").
3. **Two ROAS-advantage cells were hand-blanked to "N/A"** — Restaurants/Dining (MM 0.62 vs 3P 0.67) and
   Telco & Tech (MM 0.289 vs 3P 0.350): the only two verticals where MM LOSES on ROAS. Deliberately NOT
   carried forward. Suppressing the cells where the claim fails is the same cherry-picking pattern flagged
   in §9; the real ratio now shows. Education's absurd 3,753x ROAS advantage was left visible in the old
   sheet while the two losses were hidden, which is the tell.
4. Read me detail dropped in the first rebuild and now restored: the 3P vendor names (ShareThis,
   Dstillery, LiveRamp), the CVR/CTR/CPM definitions, "Restricted is expected for local" (Auto and ProServ
   buy local by design), and "Pooled 3P is one account" (~39% of 3P impressions).

**Lesson:** a shared workbook is not reproducible from its builder alone once anyone has touched it in
Drive. Read the live file before regenerating over it. The old file is still in place next to the new one.

## 12. Correction to §11, and a spec divergence found in the original Slack thread (2026-08-20)

**Correction — §11 item 3 overreached.** The full Slack thread shows Malachi **made Jon an editor** on
`1m5RKXYN219eGH_JiYwPdL9pmzwmerunJ` on 2026-07-22 ("I'll make you an editor as well just in case"), and
the ROAS-advantage column existed because **Jon asked for it**. So the hand edits (re-sort, the Education
footnote, the two "N/A" cells) are as likely Jon's as anyone's, and the Education caveat was one Malachi
had already given him verbally. §11 called the pattern a cherry-picking "tell"; that reading is not
supported. Both cells still show their real ratio in the rebuild, which is the right default, but the
motive claim is withdrawn.

**Spec divergence — Jon's zip rule was never implemented as stated.** On 2026-07-20 Jon said: *"Lets omit
zip codes for all verticals except auto and professional services."* The shipped SQL does something
different: zip/city/radius narrowing routes a campaign into **MM restricted** in EVERY vertical, with no
Auto/ProServ exemption. That is a defensible design (isolate rather than drop, and expose the group), but
it was never reconciled with him, and it matters because MM restricted IS folded into **MM (all)** — the
exact lens Jon says the deck is using for the "realistic story".

Applying Jon's rule literally (drop geo-narrowed MM campaigns outside Auto and ProServ) on the
2026-08-20 window:

| MM (all) | As shipped | Jon's zip rule | 3P |
|---|---:|---:|---:|
| Advertisers | 2,299 | 1,514 | 378 |
| IVR (median) | 0.26% | **0.34%** | 0.07% |
| CPV (median) | $12.90 | **$11.36** | $36.23 |
| CPA (median) | $338.82 | **$295.06** | $732.50 |

Drops 2,592 of 5,966 MM campaigns (43%), $26.0M spend. **The shipped MM (all) understates MM against
Jon's own spec**: IVR advantage 3.5x → 4.7x, CPA advantage 2.16x → 2.48x. Not added to the workbook — it
is a third lens and §9's "pick one and hold it" applies to it too. Raise it with Jon and let him choose
which MM (all) the deck means.

## 13. Final shipped state (2026-08-20) — supersedes the tab list in §9 and §11

**The "CPA on non-revenue accounts" tab described in §9 was BUILT AND THEN CUT.** Two reasons, both
raised by Malachi in review:

1. **Jon never asked for it.** His ask was "add a CPA comparison... we'll need it for non-revenue-driving
   customers, e.g. B2B" — a request for the CPA *column*, because ROAS is meaningless for those accounts.
   The cohort split was analyst-initiated scope.
2. **The cohort was mislabelled and did not mean what it said.** "No revenue tracked" was really "zero
   revenue attributed to Stage 1 prospecting in this window". Retargeting is excluded from the cohort, so
   ecommerce advertisers whose revenue lands there fell into it. Of 2,542 qualifying advertisers, 1,624
   had zero prospecting revenue: **980 fire a conversion pixel with no order value, 644 fire none at all.**
   Not a clean B2B proxy.

**The correct screen for CPA is conversions, not revenue** — which is what the CPA median always did
(`conv > 0` only). Revenue now backs only the ROAS columns. Both comparison tabs carry
`MM with conversions` / `3P with conversions`, the advertiser counts each CPA rests on, so a thin cell is
visible at the point of use (Restaurants/Dining's 6.6x CPA advantage sits on 8 3P advertisers).

**§9's finding still stands and is still worth carrying to Jon** — the CPA advantage narrows sharply on
B2B and non-revenue accounts — but it belongs in conversation and in the spike, not as a tab whose cohort
definition cannot be defended.

**Shipped tabs (8):** Overview · MM vs 3P by vertical · MM gated vs 3P by vertical · Full scorecard ·
Campaign detail · Read me · Queries · Method & caveats. The `Overall` tab was cut as derivable from
Full scorecard.

**Also fixed in this pass:**
- **All four groups are now defined in the Read me.** The first rebuild defined only `MM restricted` and
  `Intent gate`; `MM (gated)`, `MM (no gate)` and `MM (all)` had no definition anywhere in the workbook,
  a regression against the 2026-07-21 sheet, which defined all of them. Caught in review.
- Duplicate Method blocks on conversion coverage merged into one.
- The B2B caveat was rewritten as "The vertical crosswalk is interim" — it had been describing a B2B
  split that no longer ships.
- `Campaign detail` gained `Peak Performance`, `Scoring engine` and `Intent tiers reachable` from
  `dw-main-silver.audience.mm_campaign_classifier` (answers Alex Knorr, 2026-08-20). 40% read
  `Not classified`: delivered in-window but no longer active, so the live view has no row.
- A thin-vertical caveat: Restaurants/Dining reads 27.8x on visit rate off 15 3P advertisers.

**Still open with the requester:** (a) which `MM (all)` the deck means, given §12's unimplemented zip
rule; (b) Jon's link points at the 2026-07-21 file, not this one.
