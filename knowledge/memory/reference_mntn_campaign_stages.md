---
name: reference_mntn_campaign_stages
description: MNTN campaign stages (Prospecting/Multi-Touch/Multi-Touch Plus) — canonical defs + the S1→S3-direct + per-stage-VV-window nuances; read before labeling stages
metadata: 
  node_type: memory
  type: reference
  originSessionId: 11ede3de-0ba9-4e9a-8849-688f05b49869
doc_type: memory
keywords: [mntn_campaign_stages, mntn, campaign, stages, prospecting, multi, touch, plus]
domain: [reference]
lifecycle: active
last_verified: 2026-07-22
---
MNTN campaigns run three stages (a campaign_group bundles one campaign per stage). Canonical defs: `knowledge/mntn_business.md` §"Campaign Stage Definitions" (via Tofer):
- **Stage 1 — Non-Engaged / Prospecting:** the audience the client targets in the UI. `funnel_level=1`.
- **Stage 2 — Engaged:** households that have SEEN a campaign ad (exposed, not necessarily visited). `funnel_level=2`.
- **Stage 3 — Engaged + VV ("Multi-Touch Plus"):** seen an ad AND registered a Verified Visit. `funnel_level=3`.

**Do NOT call these generic "mid/lower funnel"** — they are VV-gated retargeting stages, not an awareness→decision funnel (I mislabeled them once). **In stakeholder deliverables, label stages by the plain audience condition (user preference, AUDI-1148): Stage 1 = "Prospecting", Stage 2 = "Exposed to Prior Ad", Stage 3 = "Has a Prior VV"** — not invented funnel tiers, and not the raw DB names (Multi-Touch / Multi-Touch Plus). Plainer beats jargon. **NOT a strict 1→2→3 chain:** a household that saw the Stage-1 ad and then visited can enter Stage 3 directly — a Stage-3 VV traces to a prior Stage-2 VV (preferred) OR straight to a Stage-1 VV (fallback); ~39% skip Stage 2 (TI-650 v3: 53,961 of 138,317). **Each stage has its own VV/lookback window** (per-advertiser via `advertiser_configs`; audit default ~120d, WGU 210d).

**Stage key:** `funnel_level` vs `objective_id` is contested — on Beeswax CTV campaigns all carry `objective_id=1` so `funnel_level` 1/2/3 separates the stages (TI-650); broadly TI-1037/AUDI-1070 prefer `objective_id` (1=Prosp/4=RT/5=MT-S2/6=MT-S3/7=Ego) since funnel_level is reused as a sub-tier inside all-retargeting groups. Prospecting on CTV (`channel_id=8`), Multi-Touch stages often on display (`channel_id=1`). Full mechanics: `tickets/ti_650_stage_3_vv_audit/summary.md` §3/§7. Incrementality/HI-exclusion + ghost-bid holdout apply to **Stage 1 only**.
