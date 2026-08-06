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
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

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
