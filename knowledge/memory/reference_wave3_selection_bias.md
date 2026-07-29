---
name: wave3-selection-bias
description: "Fangorn Wave 3 (Tier 5, the permanent holdout used as control in TI-961) is NOT a random holdout — advertisers were specifically selected for having delivery concerns. DiD against Tier 5 has a known structural selection-bias issue."
metadata: 
  node_type: memory
  type: reference
  originSessionId: e28957cb-95c7-4eeb-a32b-fd1c38ef16fb
doc_type: memory
keywords: [wave3 selection bias, fangorn, tier 5, permanent holdout, TI-961, DiD, parallel trends, non-random control, CausalImpact, Angi, CVR]
domain: [experimentation, audience-scoring]
lifecycle: active
last_verified: 2026-06-10
---
Per the Confluence "Fangorn Advertiser Tiered Rollout" page (3566862508), Tier 5 / Wave 3 was selected by SPECIFIC delivery-risk criteria, NOT by random assignment:

> "Wave 3 — Hold for Manual Review (399 advertisers). Score < 0.70 with at least one blocking flag."

**Blocking flags used to put advertisers in Wave 3 (Tier 5):**

| Flag | Count | What it means |
|---|---|---|
| HHST low (pool grows — mitigated) | 198 | Low serving threshold but Fangorn would grow audience |
| HHST concern + shrinks | 138 | Low serving threshold AND Fangorn would shrink audience |
| audience shrinks > 70% | 42 | Fangorn cuts pool to < 30% of current size |
| audience grows > 5x | 34 | Pool expansion unusually large |
| no impressions yet | 7 | Campaign hasn't launched |

**Implication for TI-961 analysis:** Tier 5 advertisers have *structurally different* audience-quality and delivery characteristics than Tiers 1-3. Using them as a DiD control violates parallel-trends because the selection criteria correlate with baseline IVR/CVR/ROAS/CPA. DiD-against-Tier-5 comparisons are conservative-to-biased depending on direction:

- **Tier 5 advertisers tend to have anomalously HIGH CVRs** at the cohort level — multi-touch-attribution-heavy verticals (casual dining, hotels, swim schools, home services like Angi) pull the pool up to ~6.5% vs treated tiers at ~2-4%. This makes treated DiD CVR comparisons look artificially WORSE than they are.
- **Tier 5 advertisers tend to have anomalously LOW IVRs** in some segments (huge advertisers like Ancient Nutrition at 0.43% IVR). This makes treated DiD IVR comparisons look artificially BETTER than they should.

**How to apply:**

- **For TI-961 deck framing:** explicitly note that Tier 5 is a non-random "manually-flagged-for-review" cohort, NOT a random holdout. Lean on CausalImpact (which builds its counterfactual from the treated tier's own pre-period structure) over DiD (which is more sensitive to the structural baseline difference).
- **Identified leverage advertisers (cross-reference these with the actual Tier 5 list to confirm):** Angi (32766, adv_cvr 207.59% — anomalous attribution), Cheddar's (34834), Mountain Mike's Pizza (31297), Station Casinos (59584), SpotHero (35872), Goldfish Swim School (45921), Ancient Nutrition (31455), Gainbridge (49868), Northern Tool (40563).
- **For the NEXT major rollout:** the experimental design doc must explicitly forbid Wave-3-style selection criteria for the holdout. Permanent holdouts must be STRATIFIED RANDOM, never "the advertisers we have concerns about." See [[bootstrap-must-match-design]] and `documentation/docs/feature_rollout_experimental_design.md`.

**Discovered 2026-06-10** when investigating Tier 5 control composition for TI-961. Pool CVR diagnostic showed control CVR was 6.51% vs treated tiers' 2-4%, which the BQ-leverage analysis traced to specific high-CVR advertisers (Angi, casual dining chains, multi-touch attribution-heavy verticals).
