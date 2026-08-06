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
Brief context: what system/feature/data is involved, and why this ticket exists.

## 2. The Problem
What exactly is broken, unclear, or needed? Include:
- Symptoms observed
- Who reported it / who it affects
- Impact (data quality, revenue, user experience, etc.)

## 3. Plan of Action
Numbered steps of the approach taken. Updated as the plan evolves.
1. Step one
2. Step two
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
