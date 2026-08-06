---
doc_type: ticket
title: "PS-8572: lovepop repeat customers"
status: in_progress
date: 2026-08-06
summary: "Lovepop (58797) claims 983 repeat customers hit their prospecting campaign despite CRM + site-converter exclusions; validate exclusion at serve time and explain the matchback pattern"
result: "in progress"
question: "Were any post-2026-06-29 impressions on campaign 614193 served to IPs that were on Lovepop's CRM exclusion lists (uploads 28594/32697) at serve time?"
framing_state: locked
---

# PS-8572: lovepop repeat customers

**Jira:** https://mntn.atlassian.net/browse/PS-8572
**Status:** backlog
**Date Started:** 2026-08-06
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Were any post-2026-06-29 impressions on campaign 614193 served to IPs that were on Lovepop's CRM exclusion lists (uploads 28594/32697) at serve time — and if not, what mechanism produces the 983 "repeat customers" in their matchback?
- **Goal (why / the decision):** PS-8572 routing decision — Audience squad either fixes a real exclusion failure or the ticket goes back to the reporter with the mechanism explained (conversion window + stage retargeting + match gap). Alice/Richie/Alyson waiting; retention-side ask (Kale: revenue retention).
- **Objective (done-when):** A Jira comment draft with the y/n answer plus the per-order classification of the samples, and a Slack reply draft for Alyson — both traceable to queries in `queries/` and outputs in `outputs/`.
- **Approach (how):** Parse the matchback xlsx; verify config/expression polarity/windows from integrationprod + audience_segments; reconstruct the 10 chains from ui_conversions + clickpass_log; stage-aware serving-after-conversion test from cost_impression_log; campaign-wide serve-time join against ipdsc__v1 DS4/DS47 with 3d propagation grace; classify orders into served-pre-upload / propagation / match-gap / true-failure / IP-drift.
- **What would change the answer:** Post-grace on-list impressions >0.1% of window impressions or ≥25 distinct IPs → exclusion IS broken; any chain violating the 180d VV / 30d conversion windows → attribution defect instead.

## 1. Introduction
PS-8572 (support, reporter Alec Gorsse, assignee Bryce Wagg / Audience squad). Lovepop (advertiser_id 58797) runs a live prospecting campaign (campaign_id 614193, campaign_group_id 124087) with CRM exclusion lists (audience uploads 28594 + 32697, uploaded 2026-06-29) and site-converter pixel exclusion. Exclusion audience_id 95073. DS47 (CRM identity-graph generated) released 2026-07-01, two days after the upload. Alyson asked Malachi for prior learnings (TI-650/TI-644/TI-1037 territory); Richie asked PRO to validate the matchback pattern at pipeline level.

## 2. The Problem
Lovepop claims 983 repeat customers showed up tied to the prospecting campaign despite the exclusions. Evidence supplied: 10 sample order IDs (with days-since-last-purchase 1-2d and lifetime orders 3-88) + a matchback export (2,290 orders, Jun 1-Aug 4: order_id, impression time, verified visit time, conversion time, IP).

Richie (support) pre-checked: block_conversion / block_first_party / block_prospecting all TRUE at 180d lookback; audience 95073 correctly references both uploads; match rates climbed 9-11pts since upload (~24-26% residual unmatched). His manual cross-reference: 6 of 10 samples share an identical impression+visit pair with other orders (worst: 1 impression 5/26 + 1 visit 7/9 carrying 5 orders over 17 days), and no fresh impression near any repeat conversion.

Malachi's 5-possibility frame (Slack): (1) verify blocking configs actually on; (2) client definition of "repeat" vs our 30d conversion-window default; (3) S2/S3 within prospecting DO retarget converters/viewers by design (remedy: zero S2/S3 spend); (4) S1 keeps serving an IP until conversion (a DS exists for 1-imp-max); (5) IP drift between CRM list emails and live household IPs.

## 3. Plan of Action
Approved plan at `~/.claude/plans/i-got-this-jira-mossy-clock.md`. Pre-registered verdict thresholds in §0.
1. Parse attachments; profile matchback (DONE, see `outputs/ps_8572_matchback_profile.md`).
2. Fan-out checks 1a-1e (windows/blocks/expression polarity/upload metadata/ipdsc observability) + Step 2 (reconstruct 10 chains from ui_conversions+clickpass_log) + Step 3 (stage-aware serving-after-conversion for 16 sample/cluster IPs). Workflow wf_46f18446-656.
3. Step 4: campaign-wide serve-time exclusion test (CIL x ipdsc DS4/47, weekly dt snapshots, 3d propagation grace).
4. Step 5: classify matchback orders: (a) pre-upload impression / (a') propagation / (b) match gap / (c) true failure / (d) IP drift.
5. Step 6: Jira comment draft + Slack reply draft structured by the 5 possibilities; /capture; commit.

### Early findings (client's own export)
- Two-clock compliance in THEIR data: 0 of 2,290 orders exceed either window; max visit-to-conversion exactly 30.0d (the conversion-window bound is visible); max impression-to-visit 60d (well under 180d).
- 49.5% of orders (1,134) have impressions PRE-dating the 6/29 upload; 1,156 post-upload need membership adjudication.
- 100 impression+visit pairs carry >1 order (224 orders; up to 9 on one pair); 29 of those pairs span 2-3 IPs (household IP drift visible in their own report).
3. ...

## 4. Investigation & Findings

### 4.1 The headline: the main CRM list was not in the bidder exclusion until 2026-07-16
From `audience_segment_archives` version history for campaign 614193 (queries `ps_8572_01c_*`, outputs `ps_8572_expr_archive_history.json`):
- Before 2026-06-30 02:08 UTC: NO CRM exclusion clause existed at all (segment versions v1-v6).
- 2026-06-30 02:08 (v7): CRM clause first added, as DS4 [32697] ONLY, i.e. only the small "0-364 - Customer List - 6.29 Upload" (341K entries).
- 2026-07-01 10:40 (v8): migrated DS4 -> DS47 [32697] (DS47 release day).
- 2026-07-16 18:17 (v10): 28594 ("Customer List - 4.15 upload", 3.62M entries, created 2026-04-15 not 6/29) FIRST added -> DS47 [28594, 32697].
- 2026-07-31 18:11: whole audience swapped 79847 -> 95073 "Aug_Geo_Holdout" (same CRM excludes, includes DS13->DS46, geo change).
So members of the 3.6M-entry main customer list were targetable in prospecting for 17.3 days after the client believed exclusions were live, and no CRM exclusion existed at all in June. Current expression is CORRECT: DS47 [28594, 32697] at negative polarity (expr.py XOR-of-not verified), DS21 [58797] 180d + DS34 [58797] 90d + DS2 behavioral excludes (seg 635121 = UserNumConversions 1-50, seg 682481 = UserNumPageViews 2-50), 10% holdout, DS14 gate.

### 4.2 Sample chains: matchback validated exactly, zero window violations (Step 2)
All 10 sample orders reproduced from ui_conversions + clickpass_log with 0-second deltas on all 30 timestamps (matchback is EDT). All chains satisfy impression->visit <= 180d (max 44.55d) and visit->conversion <= 30d (max 20.27d). The 5-order cluster is confirmed: 5 conversions = 5 distinct orders on ONE ad_served_id (e4b97b34-b10d-457d-bc21-010d6a9140d3). Full 2,290-order matchback: 0 orders violate either window; max visit->conversion exactly 30.0d. Files: `outputs/ps_8572_sample_chains.csv`, `outputs/ps_8572_matchback_profile.md`.
Gotchas found: matchback "Impression Time" = ui_conversions.impression_time (FIRST qualifying impression), which differs from clickpass_log.impression_time on the same ad_served_id in 10/10 rows (up to +34.5d; e.g. the 5/26 "impression" actually anchors to a fresh 6/29 clickpass impression, 10.2d before the visit, inside the 14d PRO VV window). 6/10 visits sit on the S2/S3 campaigns (614191/614192) while the conversion is credited to 614193. 3/10 visit_ip != conversion_ip (cross-device/household).

### 4.3 Serving after conversion: zero prospecting-S1 impressions (Step 3)
For the 16 sample/cluster IPs (200 CIL impressions, 2026-05-01..2026-08-05): 0 post-conversion S1 impressions on either anchor (attribution time or true event time). All 147 post-conversion impressions were S2 (6) / S3 (52) / standalone retargeting cg 129046 (89). The behavioral converter suppression (block_conversion + DS21/DS2) is working in S1. The "ads after converting" experience is S2/S3 within the prospecting group + the standalone RT group, both by-design retargeting. 9 post-6/29 S1 impressions exist to 3 IPs, all PRE-conversion (adjudicated against list membership in step 4). Files: `outputs/ps_8572_serving_after_conv*.csv`.

### 4.4 Config facts (Steps 1a/1b/1d/1e)
- Windows (58797): PRO VV window 14d, RT 7d, conversion window 30d (all variants), unchanged all 2026. NOT 180d; Richie's "180-day lookback" = conversion_lookback_window in advertiser_configurations, a different field, and it was 90d for the whole complaint window (changed 90->180 on 2026-08-04, the day the ticket was filed).
- Blocks: block_conversion / block_first_party / block_prospecting all TRUE continuously since 2026-04-14 (one same-second FALSE blip on 8/4, a save transaction artifact). page_view_lookback 90d.
- Uploads: 28594 entry_count 3,618,989, match_rate 0.629 (residual 37.1%); 32697 entry_count 341,383, match_rate 0.669 (residual 33.1%). data_source_category_id = audience_upload_id confirmed. No match-rate history table exists in BQ (the claimed 9-11pt climb is unverifiable here).
- ipdsc: both uploads observable under DS4 AND DS47; DS47 membership ~2.2-2.4x DS4 (graph expansion; 28594: 13.87M vs 5.87M IPs at 7/15). DS21/DS34 NOT in ipdsc (site-converter exclusion only testable behaviorally, which 4.3 does). DS47 partitions predate the 7/1 release (release was enforcement-side). 6/29 upload resolved in ipdsc by 6/30 (~1 day latency).

### 4.6 Claims audit (every claim in the ticket description + comments, double-checked)
| Claim (who) | Verdict | Evidence |
|---|---|---|
| Blocks all TRUE, "blocking enabled correctly" (Richie) | TRUE | TRUE continuously since 2026-04-14 (1b archives) |
| "180-day lookback" (Richie) | MISLEADING | conversion_lookback_window was 90d for the entire complaint window; changed 90->180 on 8/4 20:43 UTC, the day the ticket was filed. Richie read the post-change value. Separately, the DS21 clause lookback doubled 90d->180d on 6/26 (v6) |
| Audience 95073 correctly excludes 28594+32697 (Richie) | TRUE today, FALSE historically | Current expression correct (DS47 both lists, negative polarity). But 95073 exists only since 7/31; predecessor 79847 had no CRM clause before 6/30, 32697-only until 7/16, 28594 added 7/16 |
| Match rates climbed 9-11pts (Richie) | UNVERIFIABLE in BQ | No match-rate history/archive table exists; current 62.9%/66.9% only |
| "~24-26% residual unmatched" (Richie) | FALSE | Residuals are 37.1% (28594) and 33.1% (32697) |
| "6 of 10 samples share an impression+visit pair" (Richie) | UNDERCOUNT | Actually 7 of 10 (he missed 12181317353545) |
| 5-order cluster table (Richie) | EXACT | All 5 order IDs, conversion times, IPs match; confirmed as 5 conversions on ONE ad_served_id in ui_conversions |
| Two other examples 12173057753161/12175650553929 (Richie) | EXACT | Partners 12161181777993 / 12174612693065 confirmed same pair, same IP |
| "No fresh impression near repeat conversions" (Richie) | TRUE in export, FALSE at serve level | Attribution never re-anchored (true), but the household IPs DID receive fresh impressions: 147 post-conversion imps in sample, e.g. 107.115.29.35 got 8 RT imps 7/16-7/30 while cluster conversions ran 7/27-7/29. All S2/S3/RT, none S1 |
| "CRM list uploaded 6/29" (Alec) | HALF-TRUE | 32697 yes (6/29); 28594 is the 4/15 upload (3.62M entries), the main list |
| Stage-3 theory: convert, stay eligible, "receive prospecting for 30 days" (Alec) | MECHANISM RIGHT, DETAILS WRONG | S2/S3 do retarget converters (52/52 S3 imps post-conversion in sample) but S1 stops: 0 post-conversion S1 imps. The "30 days" is the conversion window in attribution, not a serving window |
| Order 12181567668297 story: ad 6/10, visit 6/25, conv 7/9, prior conv 1d before under other household IP (Alec) | EXACT | Chain reproduced 0s deltas; household partner 98.242.67.136 first conv 7/8 16:29 UTC |
| "DS47 doesn't apply to old audience uploads", re-upload could help (Alec) | FALSE | The April upload 28594 IS in DS47: 13.87M IPs at 7/15 (2.4x its DS4 count). DS47 covers old uploads; no re-upload needed (Richie's conclusion right, reasoning moot) |
| "983 repeat customers" (client) | UNVERIFIABLE as stated | Matchback = 2,290 orders with no repeat flag; only the 10 samples have days-since-last-purchase. Classification of all 2,290 in flight |

### 4.5 Exclusion timeline periods (used by steps 4-5)
P0 2026-06-01..06-30 02:08 UTC: no CRM exclusion existed. P1 06-30 02:08..07-16 18:17: 32697 excluded, 28594 NOT. P2 07-16 18:17..08-04: both excluded (DS47). Grace 3d after attach.

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
