---
name: design-ti-999-3p-scoring-at-hhst
description: Design decision for scoring IPs identified only by 3P segments at HHST threshold points — move unscored 3P-only IPs from max-reach to mid-intent band (3333-6665).
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [ti_999, 3p segment scoring, hhst, mid_intent, unscored ips, mm+3p or additive, interest segment, alex knorr, alice wu, matt brorby, alyson lefkowitz]
domain: [project, audience-scoring, bidding]
lifecycle: active
last_verified: 2026-08-28
---

**Design Decision (2026-08-28 meeting: Alice Wu, Alex Knorr, Matt Brorby, Alyson Lefkowitz):** Score unscored IPs identified ONLY by 3P interest segments at mid-intent (HHST 3333-6665) as a baseline, instead of leaving them in max-reach.

**Why:** When campaigns use MM + 3P segments joined by OR (additive, not narrowing), IPs matching only 3P segments (no MM keywords/Fanghorn) stay unscored/max-reach. When HHST is set to mid-intent or lower, these IPs are unreachable because max-reach IPs are filtered. The customer problem: can't target 3P segment IPs when spending is constrained to mid-intent or below.

**Key constraints & assumptions:**
- **3P quality scores are segment-level, not IP-level** — Alex's segment_quality_utils ranks which segments are good/bad, not individual IPs. Cannot be directly applied to per-IP scoring (contradicts initial hypothesis).
- **Applies only to MM+3P OR-additive campaigns** — when segments are AND-joined with MM, the MM scoring already handles the IP; when 3P is a pure alternate (no MM), all 3P IPs are unscored anyway.
- **Only IPs with NO MM keywords/Fanghorn score** — if an IP matches both MM and 3P, it's already scored by MM (HI or Peak Performance).
- **1P MM is better than 3P interest** — empirically confirmed (2.1x better conversion for no-3P vs fresh-LiveRamp 3P, TI-999 headline); 3P-only IPs should not exceed peak performance tier.
- **HHST context is risky** — when HHST is 0 (max-reach, goal = volume), segment quality/confidence is dubious; this is a business trade-off (better reach via 3P now, risk signal quality loss).
- **Interest segment NOT conditions are already handled** — NOT clauses exclude IPs from the audience at evaluation time, so bidder drops them regardless of score.

**Three implementation options (from most to least preferred):**
1. **Flat mid-intent score** — all 3P-only IPs → mid-intent band. Simplest, no IP-level differentiation.
2. **Distribute within mid-intent range** — score 3P-only IPs based on # of matched segments (more matches = higher within the band). Same engineering lift, slightly more nuanced.
3. **High-intent for high-quality segments** — apply segment-quality scores to identify top-performing segments, elevate their IPs to high-intent. Requires segment quality validation; risk of false-positive quality on high-stakes tier (future iteration).

**Open questions / next steps:**
- **Experiment gate:** only needed if >10% of campaigns affected (Alice Wu to determine % of campaigns with MM+3P spending into mid-intent).
- **Score range spike:** Alyson to lead spike on what the exact score range should be (e.g., 3333-6665 only, or 3333-9999 including into max-reach?).
- **Coordination:** product ops, data monitoring, pacing teams must be notified of score-range changes (Alyson to lead).
- **Timeline:** Alice to draft product brief with assumptions/approach/risks; likely prioritized next quarter (Black Friday consideration noted).
- **Segment quality iteration:** if high-quality segment elevation (option 3) is pursued, needs triple-confirmation and aligned handoff to Alex/Kelly before implementation.

**Cross-refs:** [[project-ti-999-strategic-goal]] (end-goal: improve 3P segment performance via curation/ranking) · [[reference-segment-quality-framework-ds-agnostic]] (Alex's segment scores are framework; not IP-ready yet) · [[project-ti-999-interest-segment-sizing]] (TI-999 empirical context: 3P affects 34.6% of prospecting spend).
