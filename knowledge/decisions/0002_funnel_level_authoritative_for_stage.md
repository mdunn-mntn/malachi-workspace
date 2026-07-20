---
doc_type: decision
title: "0002 — funnel_level is authoritative for campaign stage, not objective_id"
summary: "Use campaigns.funnel_level for stage; objective_id is unreliable for stage (use it only for prospecting = IN (1,5,6))"
status: accepted
date: 2026-07-19
last_verified: 2026-07-19
keywords: [funnel_level, objective_id, stage, prospecting, retargeting, campaigns, funnel]
supersedes: null
tags: [funnel, campaign]
---

# 0002 — funnel_level is authoritative for campaign stage, not objective_id

## Context
Two columns describe where a campaign sits in the funnel: `campaigns.funnel_level` and
`campaigns.objective_id`. `objective_id` is frequently used as a stage proxy but drifts from actual
stage (legacy values, re-mapped campaigns, PSA/test rows), producing mis-stated prospecting vs
retargeting splits.

## Decision
- **Campaign STAGE** → read `campaigns.funnel_level` (authoritative).
- **`objective_id`** → use only for the coarse prospecting filter: **prospecting = `objective_id IN
  (1,5,6)`** (1=Prospecting, 5=MT-S2, 6=MT-S3); 4=Retargeting, 7=Ego. Do not treat it as fine-grained stage.

## Alternatives considered
- **objective_id as stage** — rejected: unreliable for stage; disagrees with funnel_level on a
  material fraction of campaigns.
- **Infer stage from campaign_group product/naming** — rejected: indirect and lossier than the
  first-class `funnel_level` field.

## Consequences
- Any prospecting/retargeting cohort split, tiered-rollout inclusion table, or funnel report keys off
  `funnel_level`; `objective_id` is a filter, not the stage.
- `campaign_group_id` = client-facing campaign; `campaign_id` = internal funnel-stage sub-campaigns —
  stage lives at the campaign level.
- **Affected knowledge docs:** [`../bq/integrationprod/campaigns.md`](../bq/integrationprod/campaigns.md), [`../bq/integrationprod/objectives.md`](../bq/integrationprod/objectives.md), [`../glossary.md`](../glossary.md) (funnel_level / objective_id entries). Full nuance: `data_knowledge.md` § Advertising Concepts & Domain Logic.
