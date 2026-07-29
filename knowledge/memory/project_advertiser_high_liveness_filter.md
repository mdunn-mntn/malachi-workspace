---
name: project-advertiser-high-liveness-filter
description: Victor Savitskiy owns adding a live + soon-to-launch advertiser filter to airflow-ti advertiser_high.py — fixes 60-80x scoring fanout
metadata: 
  node_type: memory
  type: project
  originSessionId: e524c303-522f-4ac1-90e4-5a36c61816cc
doc_type: memory
keywords: [advertiser_high liveness filter, victor savitskiy, airflow-ti, advertiser_high.py, scoring fanout, advertiser_verticals, ryan kleck, zach schoenberger, cold start, campaign start_date]
domain: [audience-scoring, project]
lifecycle: active
last_verified: 2026-05-27
---
**Open work item:** `SteelHouse/airflow-ti` → `spark/audience_intent/advertiser_high.py` currently scores every IP × every advertiser with a `type=1` vertical mapping (~25K) instead of just the ~300-400 live advertisers. Victor Savitskiy is taking the fix; Ryan Kleck handed it off 2026-05-26 while OOO for 3 days.

**Why:** Surfaced by Zach Schoenberger in Slack 2026-05-26 — single IPs were returning ~20k advertiser_high score rows, which doesn't match the ~3-400 actually-live advertisers. ~60-80x wasted compute/storage downstream.

**How to apply:**
- If anyone asks why advertiser_high output is so large, this is the known cause — no liveness filter on the `advertiser_verticals` join.
- The fix needs to include not just currently-active campaigns but also advertisers with campaigns scheduled to launch in the next N days (cold-start avoidance — Zach was emphatic about this).
- Likely join path: `core.campaigns` / `core.campaign_groups` for active status + future `start_date`.
- Don't suggest a naive "filter to live advertisers" — that breaks day-1 new-campaign performance.

See [[reference_audience_platform_authority]] for Zach's authority on this domain and [[reference_airflow_ti]] for the pipeline repo.

Full gotcha documented in workspace `knowledge/data_knowledge.md` under "advertiser_high scoring fanout — no liveness filter."
