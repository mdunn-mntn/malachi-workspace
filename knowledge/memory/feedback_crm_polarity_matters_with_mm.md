---
name: feedback-crm-polarity-matters-with-mm
description: "CRM include vs exclude matters when combined with MM (or any positive targeting layer) — exclude is hygiene, include narrows MM scoring eligibility"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [crm polarity, DS4, DS8, DS47, MM scoring, include vs exclude, hygiene, eligibility intersection, TI-999, prospecting]
domain: [audience-scoring, bidding]
lifecycle: active
last_verified: 2026-05-29
---
When CRM (DS4/DS8/DS47) appears alongside MM (or any positive audience-targeting axis) in the same prospecting campaign, **polarity changes the meaning fundamentally at the scoring layer**:

- **CRM-exclude** (`op:not` wrapping the DS4/8/47 clause): hygiene only. Suppresses known customers from prospecting. Doesn't change MM scoring — eligibility-IP pool is just narrowed to "MM-scored IPs that aren't already customers." Standard prospecting practice.
- **CRM-include** (positive `op:any` clause with DS4/8/47): real positive targeting layer. The eligibility intersection becomes "MM-scored IPs ∩ CRM-list IPs," so MM scoring now ranks over the customer-list cohort only. Effectively a customer-list-seeded MM prospecting motion (per Zach: this is the intended use of CRM in prospecting, not retargeting — see [[reference-audience-platform-authority]]).

**Why:** the bidder's scoring waterfall applies to whichever IPs survive the eligibility filter. Excluding customers leaves MM-scored IPs untouched; including the customer list as a positive clause intersects the MM-scored set with the CRM IPs, dramatically reshaping who gets scored.

**How to apply:** Any TI-999 bucket math or deck framing that lumps "CRM-touching" together hides this. Empirically: 78% of CRM-touching prospecting spend is exclusion-only (hygiene); only ~16% is include-only and ~5% is both — so the meaningful "CRM as positive scoring constraint" cohort is ~318 campaigns / $1.57M / 4.9% of all prospecting (vs the 37.9% headline for CRM-touching). When analyzing MM scoring behavior or doing per-bucket lift analysis, split CRM by polarity. When reporting headline category coverage, "CRM-touching" with the 78% hygiene caveat is fine.
