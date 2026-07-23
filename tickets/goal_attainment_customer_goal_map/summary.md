---
doc_type: ticket
title: "GOAL-ATTAINMENT: Customer Goal Data Map"
status: done
date: 2026-07-23
summary: "Shareable data map (6-tab Drive .xlsx) identifying the tables that answer 'what % of customers hit their goal', anchored on the existing live BAE Mode report; goals from bae.v_daily_goal_by_campaign_group vs actuals from summarydata.sum_by_campaign_group_by_day."
result: "The question is already answered by a daily Mode report (30fb4d3f8447): ~63% of active campaign groups hit goal (Upper Mid 71 / Mid 66 / SMB 58). Delivered the map .xlsx; caveats documented (CPCV auto-passes ~25%, campaign-group grain, trailing-3-day)."
keywords: [goal attainment, hitting goal, v_daily_goal_by_campaign_group, sum_by_campaign_group_by_day, mode report 30fb4d3f8447, goal_type_id, costpercompletedview auto-pass, campaign group, smb mid upper mid]
---

## TL;DR

**Q:** Which tables tell us what % of our customers hit their goal? (GOAL-ATTAINMENT customer goal data map)

**A:** A live BAE Mode report already answers this: "Campaign Groups Hitting Goal Percentage" (app.mode.com/mntn/reports/30fb4d3f8447, daily), showing ~63% of active campaign groups hit goal (Upper Mid 71 / Mid 66 / SMB 58). Goals are stored, not guessed: bae.v_daily_goal_by_campaign_group holds the as-of-day goal_type + goal_value per campaign group per day, compared against actuals from summarydata.sum_by_campaign_group_by_day. The deliverable is a 6-tab Drive .xlsx data map (Overview, Table map of 18 tables x role/grain/join keys, 16-type goal_type_id decode, Already-built report + caveats, Join map, How to answer it). Caveats: CostPerCompletedView auto-passes (~25% of goals); grain is campaign-group not advertiser; status is trailing-3-day.

**How:** Assembled a shareable data map (not a Jira TI ticket; Drive key GOAL-ATTAINMENT) identifying the tables that answer the goal-attainment question, anchored on the existing live Mode report. Content captured in artifacts/goal_attainment_data.json and rendered to xlsx via python3 artifacts/goal_attainment_build_xlsx.py through lib/mntn_xlsx.py (MntnWorkbook), which overwrites the Drive copy so it re-inherits the format standard on each run.

**Tables:** bae.v_daily_goal_by_campaign_group, summarydata.sum_by_campaign_group_by_day

**Learned:**
- A live Mode report ('Campaign Groups Hitting Goal Percentage', token 30fb4d3f8447, daily) already answers '% of customers hitting goal': ~63% (Upper Mid 71 / Mid 66 / SMB 58)
- Goals are stored per campaign-group per day in bae.v_daily_goal_by_campaign_group (as-of-day goal_type + goal_value); actuals come from summarydata.sum_by_campaign_group_by_day
- Caveats: CostPerCompletedView auto-passes (~25% of goals), grain is campaign-group not advertiser, status is trailing-3-day
- Deliverable is a 6-tab xlsx data map reproducible from artifacts/goal_attainment_data.json via lib/mntn_xlsx.py

**Reuse when:**
- A stakeholder asks what % of customers/campaign groups hit their goal
- You need the authoritative stored goal per campaign group as of a date
- Building or citing the goal-attainment Mode report
- Mapping which tables answer a goal/performance question

---

# GOAL-ATTAINMENT — Customer Goal Data Map

**What:** a shareable data map answering "which tables tell us what % of our customers hit their goal?"
Built for a stakeholder ask. Not a Jira TI ticket; the Drive folder key is `GOAL-ATTAINMENT`.

**Deliverable (Drive):** `My Drive/Tickets/GOAL-ATTAINMENT/GOAL-ATTAINMENT Customer Goal Data Map.xlsx`
— 6 tabs: Overview, Table map (18 tables x role/grain/join keys), Goal types (16-type `goal_type_id`
decode), Already built (the live Mode report + caveats), Join map, How to answer it.

**Content (the substance):** a live Mode report already answers this ("Campaign Groups Hitting Goal
Percentage", `app.mode.com/mntn/reports/30fb4d3f8447`, daily) — ~63% of active campaign groups hit goal
(Upper Mid 71 / Mid 66 / SMB 58). Goals are stored, not guessed: `bae.v_daily_goal_by_campaign_group`
(as-of-day goal_type + goal_value per group per day) vs actuals from `summarydata.sum_by_campaign_group_by_day`.
Caveats: CostPerCompletedView auto-passes (~25% of goals); grain is campaign-group not advertiser;
trailing-3-day status. See memory [[reference_goal_attainment_report]].

**Reproduce / re-style:** `python3 artifacts/goal_attainment_build_xlsx.py`. Content is captured in
`artifacts/goal_attainment_data.json` (committed); the builder reads it through `lib/mntn_xlsx.py`
(`MntnWorkbook`) and overwrites the Drive copy, so it re-inherits the current format standard on every run.
