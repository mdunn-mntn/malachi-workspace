---
name: project_audi_1173_freq_cap_bandit
description: "AUDI-1173 frequency-cap bandit: RFD finalized (+ claude.ai artifact) + sprint-ready implementation_plan.md + Jira card synced to 'ready to build' (status still Backlog); multi-agent adversarial-gated; RCT designed (3-arm control/cap-8/cap-3, total-visit-COUNT primary via household bootstrap, 5% relative NI margin, MD5 16-hex buckets 100-999 disjoint from 0-99 holdout, ~10-12wk); needs small @SteelHouse/rtb bidder feature (arms not config-only); leakage = capability gap (advertiser_frequency_caps empty), magnitude retracted; pending owner review + circulation"
metadata: 
  node_type: memory
  type: project
  originSessionId: 2f8d4ec8-78d6-419a-9c3f-4da329f3c216
doc_type: memory
keywords: [audi_1173_freq_cap_bandit, audi, 1173, freq, cap, bandit, frequency, finalized]
domain: [project]
lifecycle: active
last_verified: 2026-07-28
---
**AUDI-1173 — frequency-cap bandit scoping + RCT design + decision RFD.** Status: **decision-ready RFD drafted and adversarially gated** (multi-agent: implementer → 2 fresh-context reviewers → fixer); pending **owner review (@SteelHouse/rtb) + circulation**.

**RCT design (locked):** 3-arm household-randomized — **control / cap-8 / cap-3**. Primary metric = **total-visit COUNT** (guid_log page-views deduped to visit-days per `(advertiser_id, ip, date)`, [[reference_total_visit_signal]]), inference via **household cluster bootstrap**. Non-inferiority framing with a **5% RELATIVE margin** (relative, not absolute pp — coverage-invariant under arm-symmetric cross-device miss; absolute pp would be anti-conservative). Assignment hash = **MD5(AID:ip) 16-hex mod 1000**, treatment **buckets 100-999 disjoint from the 0-99 platform holdout**. Run length ~**10-12 weeks**. Expect a **non-inferiority-shaped readout** (TI-835: total traffic ~0% platform lift → sensitive to "safe cap," insensitive for superiority).

**Capability findings:** the cap arms need a **small @SteelHouse/rtb bidder feature** — arms are NOT config-only (`CampaignModel` has no per-household cap field; fcap counter key is always the IP). **Leakage is a CAPABILITY GAP, not a mis-set knob** — `advertiser_frequency_caps` is EMPTY (0 rows), no advertiser rollup exists. **Leakage-$ magnitude RETRACTED** — per-`(ip,advertiser)` estimates are shared-IP confounded (~76% of "leaked" imps on IPs shared by 5+ advertisers). Ownership: fcap crate = @SteelHouse/rtb (snowsignal/rogusdev/RockyGitHub), NOT Zach/Jordan; [[reference_frequency_capping]].

**What the adversarial orchestration caught (before shipping):** a hash blocker (BQ 16-hex form + disjoint bucket range), a metric bias (attributed VV mechanically inflated by frequency → switched to total visits), a leakage-magnitude confound (shared-IP → retracted the sized $), and a cost trap (avoid full-scanning 366TB guid_log; partition-prune / Databricks). See [[reference_frequency_capping]], [[reference_total_visit_signal]], [[reference_hhst_efficiency_sizing]] (sequence frequency bandit first, HHST second).

**Deliverables (2026-07-28):** sprint-ready `artifacts/audi_1173_implementation_plan.md` (BLUF / Problem / Solution / ordered work-list / Impact / Expected-improvement / ready-to-sprint checklist) + `audi_1173_rfd_draft.md` (buy-in decision doc, also rendered as a claude.ai artifact `a5cd4a66-2d0d-4159-b121-c81a5aa851e4` — republish the same HTML file path to update) + design / prereg / sizing / leakage / ownership / bandit docs. Jira card synced (terse Objective/Task/Done-when; summary = "ready to build"; status stays Backlog). **Impact model:** frequency capping is **MNTN-revenue-neutral** under CPM + fixed budgets (redistributes impressions to fresh reach, not removed spend) — value = advertiser efficiency / incremental ROAS → retention; attributed IVR shifts on the capped tail (reporting artifact). Deliverable shape per [[sprint_ready_plan]].
