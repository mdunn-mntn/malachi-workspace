---
name: feedback-crm-excluded-from-prospecting
description: "For prospecting analyses, exclude campaigns using DS4/8/47 in POSITIVE clauses only. Negative-clause CRM is CRM-suppression (still prospecting). Polarity-aware per TI-999 Finding 15 Pass 2 (2026-05-28 PM)."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 790f6279-052b-404e-8970-f70d7eb62991
doc_type: memory
keywords: [crm, prospecting, DS4, DS8, DS47, polarity, positive clause, suppression, TI-999, ast parse]
domain: [audience-scoring, bidding]
lifecycle: active
last_verified: 2026-05-28
---
For any analysis framed as "prospecting" at MNTN, exclude campaigns whose audience expression references CRM-style list uploads **as a positive clause**:
- `DS4 CRM`
- `DS8 IP List`
- `DS47 CRM Identity Graph Generated`

**Why polarity matters:** TI-999 Finding 15 Pass 2 (2026-05-28) showed that 92% of MM_plus_1P campaigns (296 of 320) use 1P **only in negative clauses** — the classic CRM-suppression-from-prospecting pattern ("don't bid on customers we already converted"). Excluding those campaigns from "prospecting" treats them as retargeting, which is wrong — they're prospecting with hygiene filters.

Only campaigns with 1P-family DS in **positive (`op:any` outside `op:not`)** clauses are list-style retargeting and should be excluded from prospecting analyses. CRM in negative clauses is prospecting infrastructure.

**How to apply:** Use a polarity-aware AST parse (see `tickets/ti_999_interest_segment_sizing/queries/ti_999_clause_polarity_ast.sql`) to classify each DS reference. Filter:
```sql
WHERE NOT EXISTS (
  SELECT 1 FROM UNNEST(parsed_cats) c
  WHERE c.data_source_id IN (4, 8, 47) AND c.polarity = 'positive'
)
```

The earlier non-polarity-aware regex (filter on `data_source_id ∈ {4,8,47}` anywhere in the expression) dropped 296 MM-prospecting campaigns ($1.88M / 30d) that belong in the prospecting universe.

**Do NOT exclude DS21 (MNTN Conversion) or DS34 (MNTN Pageview)** — these are commonly used in negative clauses within prospecting for past-visitor suppression.

**Impact reference (TI-999 Finding 15 corrected universe):** prospecting universe is wider than the original Finding 11 cut because 1P-negative-only campaigns are correctly included.

**Why initial version was non-polarity-aware:** TI-999 Findings 1-11 used regex extraction that captured DS references regardless of polarity. That regex approach was always known to be loose (deferred in §8.D.2 of TI-999 summary as a "medium-complexity refinement"). Pass 2 of Finding 15 implemented the AST parse and quantified the methodology fix.

Related: [[reference_bidder_scoring_reality]], [[project_ti_999_interest_segment_sizing]].
