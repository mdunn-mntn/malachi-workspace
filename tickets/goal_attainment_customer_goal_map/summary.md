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
