---
name: reference_within_hi_vr_discriminator
description: "within-HI visit rate is THE discriminator between the two prospecting-decline failure modes: gate-removal (pool healthy, within-HI VR holds/rises) vs over-scaling (pool exhausted, within-HI VR falls with spend). AUDI-1070 5-advertiser tally: 4/5 = gate, only Caraway = over-scaling."
metadata:
  node_type: memory
  type: reference
  originSessionId: 06681997-5cb8-4f58-ba1a-517bc7ce83ae
doc_type: memory
keywords: [within-HI visit rate, prospecting decline, gate removal, over-scaling, HHST gate, AUDI-1070, household_score, Caraway, Bouqs, Kindred, HI-share]
domain: [audience-scoring, bidding]
lifecycle: active
last_verified: 2026-07-02
---
**When a prospecting advertiser's VR/ROAS falls YoY, the decisive test is WITHIN-HI visit rate** = HI-visits ÷ HI-served-imps, monthly as spend scales (household_score>=8001; RTC-excl-if-present; scores from 2025-05-06). Two finite-HI-pool modes look alike on aggregate but need OPPOSITE fixes:
- **GATE-REMOVAL/THRASH:** HI-share collapses but **within-HI VR HOLDS/RISES** → served-HI pool HEALTHY, delivery just LEFT it. Fix = restore & hold the gate. (Bouqs: HI-share 55→4% while within-HI VR ROSE 0.30→2.40. Kindred: +65% spend but within-HI VR held ~0.7-1.1%, Pearson r(spend,VR) POSITIVE.)
- **OVER-SCALING/SATURATION:** HI-share HOLDS but **within-HI VR FALLS with spend** → finite HI pool exhausted. Fix = pace / widen pool. (Caraway: stayed 82-99% HI, within-HI VR -69% at +191% spend.)

**AUDI-1070 tally (n=5): 4 of 5 declines = the GATE (HexClad, Avon, Bouqs, Kindred); only Caraway = true over-scaling.** "MM is degrading" is FALSE on all 5 — the dominant lever is the HHST gate (config/campaign-management), not the audience model. **DON'T assume high spend-growth = over-scaling — verify with within-HI VR** (Kindred scaled +65% and was still gate, not saturation).

**Two traps to avoid:**
1. **Gate controls COMPOSITION, not blended VR.** At monthly grain HI-share and overall VR need not co-move (Bouqs monthly corr = -0.45). Decisive gate evidence = HI-share tracks the gate + within-HI VR healthy. Score is BINARY (~all 10000) → avg score is blind; within-HI VR is the only lens that separates the modes.
2. **PORTFOLIO/MIX:** MT2/MT3 (obj=5/6) companions are unscored BY DESIGN (funnel-stage, not gate targets) → they inflate the "% unscored" headline. Split obj=1 stage-1 (gateable) from obj=5/6 MT before blaming a gate. (Bouqs "71% unscored" was ~47% MT-by-design; only 595017 obj=1 49% is a real gate.) Do NOT gate the MT campaigns.
3. **Re-gate ramp:** on gate restore, HI-share (composition) recovers OVERNIGHT but within-HI VR (performance) ramps ~4 weeks (TI-780). Present a re-gate as composition-proof, not performance-overnight.

Knowledge `data_knowledge.md` ("within-HI visit rate is THE discriminator"). Related [[reference_hhst_pacing_lever]], [[reference_stable_hi_not_stable_roas]].
