---
name: reference_goal_attainment_report
description: "The live \"Campaign Groups Hitting Goal Percentage\" Mode report + the bae goal/feature views it runs on — answers \"what % of customers hit their goal?\""
metadata: 
  node_type: memory
  type: reference
  originSessionId: d9227a5c-c9fc-4a1c-9193-e0db375cb6e5
doc_type: memory
keywords: [goal attainment, hitting goal percentage, Mode report, v_daily_goal_by_campaign_group, v_campaign_feature_date, bae gold views, sum_by_campaign_by_day, ROAS CostPerVisit CPCV, 63% hitting goal, goal_type_id]
domain: [data-catalog, business, audience-scoring]
lifecycle: active
last_verified: 2026-07-21
---
**"What % of our customers hit their goal?" already has a live answer.** Mode report **"Campaign Groups
Hitting Goal Percentage"** (`app.mode.com/mntn/reports/30fb4d3f8447`, runs daily, BQ data source 48787).
Reuse before rebuilding. Full table list = the `GOAL-ATTAINMENT` .xlsx (My Drive/Tickets/GOAL-ATTAINMENT/).

**Spine (BAE gold views, discovered 2026-07-21):**
- `dw-main-gold.bae.v_daily_goal_by_campaign_group` — as-of-day goal per cg (day, advertiser_id,
  campaign_group_id, goal_type_id, goal_type_name, goal_value, current_record). **Prefer over live
  `campaign_groups.goal_value`.** Join to perf on campaign_group_id + day.
- `dw-main-gold.bae.v_campaign_feature_date` — cg → targeting feature over [start_date,end_date); `feature_type`
  ∈ {KW=BUK keywords, PP=Peak Performance, KW+PP}. Join where start_date ≤ day < COALESCE(end_date,'2100-01-01').
- Perf = `dw-main-gold.summarydata.sum_by_campaign_by_day` (GOLD) trailing-3-day rolling; tier from
  `sum_by_advertiser_by_day` monthly spend (SMB <$25k / Mid Market $25-65k / Upper Mid Market ≥$65k).

**Scoring:** ROAS(1) order_value/spend hit if ≥ goal; CostPerVisit(13)/CPA(16) spend/visits|conv hit if ≤ goal;
**CostPerCompletedView(14) hard-coded hit=1**. Active = 3-day spend ≥ $100. Visits/conv/rev include
`competing_*` → industry_standard attr for everyone (ignores per-advertiser reporting_style).

**Current (2026-07-20):** ~**63%** active cgs hitting goal — UMM 71% / MM 66% / SMB 58%; by feature KW 57% /
KW+PP 58% / PP 56%.

**Caveats (where AUDI adds value):** (1) CPCV auto-passes, ~25% of cgs-with-a-goal → inflates headline;
(2) campaign-GROUP grain, not advertiser — "% of customers" needs roll-up; (3) 3-day rolling = live status,
not whole-flight; (4) industry_standard attr for all. 4 scored goal types cover 94.5% of cgs-with-a-goal
(only Efficiency/reach id 9 dropped). Detail in `knowledge/data_knowledge.md` "Hitting-goal %" +
`data_catalog.md` bae section. See [[reference_mode_dashboard_porting]], [[reference_xlsx_master_format]].
