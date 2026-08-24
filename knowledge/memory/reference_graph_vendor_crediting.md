---
name: reference_graph_vendor_crediting
description: Graph vendor crediting — Vendor List 1/2/3 taxonomy, per-team ownership, the translation-signal tables, and the four measured defects in the DS63 crediting leg.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [graph crediting, vendor list 1, vendor list 2, vendor list 3, ds63, ds47, crm inclusions, graph_translation_signal, auction_translation_signal, identity_targeted_signal, translation_timestamp, deepsync, ddp_crm_graph_cpm, divisor, audi-694, id-407, identity-crediting]
domain: [identity, pricing, data-catalog]
lifecycle: active
last_verified: 2026-08-19
---

Under the identity graph, one impression's provenance splits into **three** vendor lists (Jack Barbey's
nomenclature, #dev-mntn-id 2026-08-18). This is the map that resolves "who owns what" in graph crediting.

| list | credits | needed for | owner | table |
|---|---|---|---|---|
| **List 1 (DDP)** | who put the ID into a targeted segment | **MNTN ID only, NOT DS63** | AUDI | `gs://mntn-data-archive-prod/signals/identity_targeted_signal` (AUDI-953) |
| **List 2 (graph, encoding)** | who translated the ID into a household when building the audience | MNTN ID + DS63 | Identity | `dw-main-silver.identity.graph_translation_signal` |
| **List 3 (graph, decoding)** | who let the household be targeted in an auction | MNTN ID + DS63 | Identity | `dw-main-silver.identity.auction_translation_signal` |

**DS63 needs no List 1** — its segment is the advertiser's own CRM upload, so no DDP vendor sits behind it.
**DS47 needs nothing at all** — exclusion-only, zero impressions in `enriched_impressions`, nothing served
means nothing to credit. Together these closed AUDI-694 with no AUDI work required.

**Running it:** AP (Maya Triman) runs the monthly DDP crediting script from the August 2026 payout; BAE
supplies the updated script; the crediting *rules* are decided in #identity-crediting, not by whoever runs it.
**Ownership drift (Maya's word, Slack 2026-08-24, vs the 08-19 decision above that AUDI owns Vendor List 1):**
AP reviewed the DS63-update crediting script with Jack and Wei on 08-24 and Jack agreed to AP's proposal, with
AUDI not in the room; Maya did not know AUDI-1145 existed. Hypothesis: rule-making has consolidated into AP.
Settling check: the 08-24 meeting recording (requested) or an explicit ownership call in #identity-crediting.

**Gotchas that cost time:**
- The timestamp column is **`translation_timestamp`**, never `translation_date`. `bae-sql-utility#24` reads
  the latter and cannot compile. `targeted_id_data_sources` (the design doc's List 1 column) does not exist.
- Both signals are views over a **CRM-only** sqlmesh table with no UNION branches, so `log_translation` output
  from airflow-ti has no reader yet.
- Each daily partition is a **full snapshot**, ~335-437 GB/day. Answer questions from
  `dw-main-gold.reporting.ddp_crm_graph_cpm` instead (44 MB, impression join already done, `leg1`/`leg2` split).

**Four measured defects in the DS63 leg** (AUDI-694, dt 2026-08-06..08-12): the divisor excludes free logs
before dividing, paying deepsync 4.7x what the MM leg's rule would (259x vs full preemption); 39.3% of
in-scope impressions get no crediting row because there is no `$0` filler analog; 33Across is counted as two
vendors (DS40 credits to 28); variable-CPM partners bill $0 while still taking a divisor slot.

Kale ruled the graph leg should use the same fractional math as DDP crediting today. Contract-side, Andy
Everson favours moving deepsync to fixed-cost. Unresolved as of 2026-08-19.

Related: [[reference_ddp_billing_logic]] [[project_audi_1089_ddp_evals]] [[project_bae_4923_ddp_claim_validation]]
