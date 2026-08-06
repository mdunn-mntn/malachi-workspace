---
name: reference_crm_exclusion_serve_time
description: CRM exclusion serve-time reality — bidder evaluates DS47 only (NOT a per-IP superset of DS4; the 2026-07-01 migration dropped 86% of direct matches) and the CRM clause lives on S1 only (S2/S3/RT carry none)
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [DS47, DS4, crm exclusion, exclusion migration, audience_segments, s1 only, stage campaigns, ipdsc, PS-8572, Lovepop, 58797, converter suppression, repeat customers]
domain: [audience-scoring, bidding, identity]
lifecycle: active
last_verified: 2026-08-06
---
Two serve-time facts that decide any "excluded customer still saw ads" escalation (PS-8572, Lovepop 58797, verified 2026-08-06):

1. **DS47 is NOT a per-IP superset of DS4.** DS47 (CRM Identity Graph Generated) is 2.2-2.4x larger in aggregate, but covers DIFFERENT IPs: 1,300 of 2,154 converting IPs were DS4 exact-matches (ipdsc dt 6/30) while only 281 were DS47 members at 7/02 — overlap 182, i.e. 86% of direct matches dropped. The bidder evaluates DS47 ONLY since the 2026-07-01 release, so the platform DS4→DS47 exclusion-clause migration removed most direct-matched customer IPs from live exclusions. Verify per-IP overlap in `ipdsc__v1` at the relevant `dt`; never assume "bigger set = broader exclusion."
2. **CRM exclusion clauses attach to S1 (prospecting) only.** S2/S3 stage campaigns carry NO CRM/DS21/DS34/DS2 excludes in ANY archived segment version (Lovepop segments 738123/738128, 5 versions each, zero CRM hits) — just DS16 stage-progression includes + DS14 gate + holdout. Standalone RT groups get no CRM clause by design (Zach rule: CRM unusable in retargeting). Converter suppression downstream of S1 relies entirely on S1 gating DS16 entry; IPs already in S2/S3 pools keep getting served after a late exclusion.

**How to apply:** split any serve-time leak test by stage (S1 vs S2/S3/RT) and adjudicate membership against the DS the bidder actually evaluates (DS47) at the serve-date `dt`. Full detail: data_knowledge.md "CRM Upload Flow (DS 4)" + `tickets/ps_8572_lovepop_repeat_customers/summary.md` §4.7-4.9. Related: [[feedback_crm_excluded_from_prospecting]], [[feedback_crm_polarity_matters_with_mm]].
