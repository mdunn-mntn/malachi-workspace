---
name: id164-ip-quality-scoring
description: "ID-164 (Identity, Q3) — toxic-hub / shared-IP confidence scoring, PR open; overlaps any per-IP value/quality work (Jack Barbey, elena-tpm)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 11120755-d5de-4ee7-83cd-aef7c4761482
doc_type: memory
keywords: [id164_ip_quality_scoring, id164, quality, scoring, identity, toxic, shared, confidence]
domain: [reference]
lifecycle: active
last_verified: 2026-07-09
---
ID-164 "IP Quality Scoring" (Q3 Identity Research roadmap, PR open as of 2026-07-09): shared-IP / "toxic hub"
confidence scoring — hub IPs inflate visit rates up to 46×; 65% of verified visits arrive via shared IPs;
hub IPs can map to up to 500 households. Open design question on the ticket: surface as bid modifier,
reporting flag, or both. Owners: Identity team — Jack Barbey / elena-tpm.

Existing infra in SteelHouse/idg: `ipHouseholdOwnership.scala` (distinct households per IP, hub flag at
maxHHsPerIP=500), `SharedIpAttachmentFunctions.scala` (ConfidenceScore = least(1.0, IDHistory × IDStability)
per IP→household link).

**How to apply:** any per-IP value/quality scoring idea (e.g. weighting vendor-supplied IPs by advertiser
diversity — Malachi's AUDI-1089 idea) should extend/consume ID-164's score, not build a parallel pipeline.
Loop in the Identity team first. Related: [[ddp-valuation-framework]].
