---
name: incrementality-experiment-ber-2250
description: Q2 2026
metadata: 
  node_type: memory
  type: project
  originSessionId: 9c582365-7ebc-49fa-9d1e-6d93ac47841b
doc_type: memory
keywords: [incrementality_experiment, incrementality, experiment, 2026]
domain: [project]
lifecycle: active
last_verified: 2026-07-24
---
BER-2250 "Incrementality Overhaul" is the highest-leverage initiative for Q2 2026.

**CURRENT STATE (2026-07-24) — read first; the April content below is historical.**
- **Measurement ownership moved to the INCR project / First Ascent team.** Matt Brorby owns the ghost-bid lift pipeline; Ryan Kleck owns the bidder/holdout. We *consume* the measurement, we don't rebuild it. See [[project_bidder_level_ghost_bidding_approved]].
- **Ghost-bid lift is productionized:** gold `dw-main-gold.reporting.lift__ghost_bid_{results,rollup}` (time-boxed AUDI-1148, accumulates no-TTL). Query gated, aggregate per-campaign `abs_itt` with **inverse-variance weights, never a naive count pool** (that gives a Simpson-confounded no_score +29%; IVW → ~0).
- **The persuadables gradient (refreshed 2026-07-24, holds on the wider window):** Mid +9.2% · MaxReach +6.6% · PP +1.8% · High +1.7% · no_score +0.2% (~dead). Mid-intent carries the lift; top-intent + untargeted reach are incrementally dead. Raw-visit rank is ~INVERTED vs incremental-lift rank.
- **AUDI-789 (RTC/Fangorn scoring) is the go-forward targeting vehicle** — a visit/spend-optimized scorer de-optimizes incrementality unless lift is a target/guardrail.
- **User steer 2026-07-24:** treat these as old/reassigned work — don't keep extending BER-2250/AUDI-789 unprompted. See [[feedback-dont-extend-old-tickets]].

**Kale's direction (2026-04-08):** "The most valuable thing right now is getting this incrementality thing out. Solving this would be HUGE and would dramatically change growth and retention." Everything regresses to incrementality / incremental ROAS.

**The core problem:** MNTN likely looks bad on third-party incrementality platforms (LiftLab, Kochava) because everything is optimized toward the visit. Internal metrics (clickpass_log) overstate true incrementality. External vendors measure something closer to total business impact (guid_log-like).

**TI-835 observational finding confirms this:** guid_log shows ~0% lift (no net new traffic from CTV ads). clickpass_log shows 2-8x lift (attribution capture). The gap between internal and external measurement is the problem.

**Strategic shift:**
- Shutter internal incrementality dashboards → move to approved third-party vendors
- OKR: Run 5 experiments with external vendors
- Change targeting methodology to optimize for incrementality, not just visits
- Customer-driven: ask advertisers what they want (reach, performance, incrementality) → tailor experience
- Need a dedicated LiftLab liaison/DS
- CPM pricing → incrementality changes don't directly hit profit, but IVR will suffer
- **Incremental ROAS** is the top metric, not incremental visits

**Key external vendors:**
- **LiftLab** — primary, keeps coming up
- **Kochava** — another option
- Possibly more

**Three workstreams (Alex Bloore, 2026-04-08):**
1. Intent Score Shuffling Experiment (product brief — TI-837/839/842)
2. Population Split / Deciles (TI-831) — random A/B for customer testing
3. Observational Analysis (TI-835) — baseline using 10% holdout (DONE)

**Tickets:**
- BER-2250: Parent initiative
- TI-831: Audience Deciles for Advertiser Experimentation
- TI-835: Control group design and measurement methodology (3 SP) — **ANALYSIS COMPLETE**
- TI-837: Implementation plan for intent score shuffling (5 SP)
- TI-839: Measure incrementality results (5 SP)
- TI-842: Present results to broader audience (3 SP)

**Product brief:** https://mntn.atlassian.net/wiki/external/NTM1ZmViMzc1YzczNDQ0YjgzZDVlMjdkNTk2ZGY4NmY

**Why:** Existential for the business model. If we can't show up well on third-party incrementality, advertisers will shift budget away. Solving this = competitive moat vs Meta/Google.

**How to apply:** BER-2250 tickets are Tier 1. When external vendor experiments come up, prioritize immediately. Frame all incrementality work in terms of incremental ROAS, not incremental visits. When presenting findings, explicitly connect internal metrics gap to external vendor measurement gap.
