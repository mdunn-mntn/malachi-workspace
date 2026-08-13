---
name: reference_geo_category_boolean_logic
description: "Geo is ALWAYS AND-joined to the category/audience block in an expression (never OR, by design). For EXCLUDES that AND-of-NOTs equals a union — NOT IN (RT or CRM) AND NOT IN GEO == NOT IN (RT or CRM or GEO) — so an excluded geo is excluded regardless of list membership. The audience-builder UI showed excludes joined by AND (reading as an intersection, the opposite of engine behavior); PRO-636, display fixed 2026-08-12."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [geo AND categories, geo OR categories, geo exclusion boolean, exclusion union, De Morgan, audience expression boolean, audience builder UI, mission control UI boolean, PRO-636, geos where op not, include block, exclude block, category targeting, RT exclusion, CRM exclusion, Zach Schoenberger, Paulo, Mike Dolt, PRO team]
domain: [audience-scoring, data-catalog]
lifecycle: active
last_verified: 2026-08-12
---
Source: `#targeting`-adjacent Slack thread 2026-08-12 (Paulo → Zach Schoenberger → Mike Dolt). Person's word, not a code read — Zach is the audience-platform authority ([[reference_audience_platform_authority]]), so treat the engine semantics as high-confidence but unverified in source.

**1. Geo is always AND-joined to the category/audience block. There is no OR-with-geo, by design.** Zach: "we've never supported doing any version of category targeting where the categories are OR'd with geo. in almost all cases it is not what they would want anyways... basically almost no one would use it correctly." Matches the stored structure already documented in `data_knowledge.md`: `geos.where = {op:and, value:[ <include or-block>, {op:not, <exclude or-block>} ]}`, with the geos block AND'd to the categories block.

**2. For EXCLUDES, that AND-of-NOTs is a union, not an intersection.** Engine behavior (Zach): `NOT IN (RT or CRM) AND NOT IN GEO`, which is identical to `NOT IN (RT or CRM or GEO)`. An IP in an excluded geo is dropped **whether or not** it also hits an excluded list, and vice versa. De Morgan: an AND of NOTs across axes = a NOT of the OR. This is what the customer in the thread actually wanted (RT or CRM or GEO) and what the platform was already doing.

**3. The defect was the UI label, not the engine.** The audience-builder UI joined the exclusion rows with **AND**, which reads as `NOT IN ((RT or CRM) AND GEO)` — exclude only IPs in a list *and* in the geo, a strictly narrower exclusion than what runs. Paulo (VP): "this UX is basically saying the opposite of whats happening, we gotta change that AND to an OR when its exclusions." Zach agreed: "exclude should show as OR to be correct." Ticket already existed with the PRO team as **PRO-636** (surfaced by Mike Dolt); **display fixed same day, 2026-08-12** (Paulo: "its fixed"). Includes still display AND, correctly.

**How to apply:**
- Never infer exclusion semantics from a UI screenshot or a buyer's UI-derived description, especially any dated before 2026-08-12 — it understates the exclusion (intersection where the engine unions). Parse the stored expression (`audience.audience_segments` targeted segment / TPA expression JSON) instead.
- When counting what an exclusion removes, sum the union across axes (geo excludes ∪ category/list excludes), not the overlap. Same direction as the single-attribute rule already in `data_knowledge.md` item 7b (stacked exclude clauses OR, so you inherit every provider's flags).
- A buyer complaint of the form "I want A or B or GEO excluded and the UI won't let me" was a labeling problem, not a capability gap. The mirror-image ask on the **include** side (categories OR geo) is genuinely unsupported and intentionally so.

Related: [[feedback_crm_polarity_matters_with_mm]] (include vs exclude changes meaning at the scoring layer), [[reference_aud22_geo_reporting_sync]] (the geo *data* mismatch class, unrelated to this boolean/display issue).
