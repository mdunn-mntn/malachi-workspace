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
**Ownership resolved (Maya's word, Slack 2026-08-24, supersedes the drift hypothesis from earlier that day):**
from September 2026 Mike Dolt + Jaime own the crediting logic; AP only helped with the August updates (DS63 is
part of the August change, so AP updated the script with Jack and Wei, Jack agreed to the proposal; no recording
exists). Maya's crediting examples, DS4 tab plus planned DS63 approach:
https://docs.google.com/spreadsheets/d/1yZXxU7RNf0TUgiZxXYJjD351qsVKwLyuQwMoks_7-0o/edit?gid=552399317
Sharper read (2026-08-24, from the table above): the meeting was the DS63 update, and DS63 needs no List 1,
so Maya's statement covers the graph legs (Lists 2/3) plus script-running; it does not displace the 08-18 call
that List 1 rule-making is AUDI's. AUDI-1145 (List 1 rules) likely stays with Malachi.
**Ownership is UNSETTLED — three claims on record, all 2026-08-24 or earlier, none authoritative:**
- Jack Barbey's map (08-18, table above): List 1 AUDI, Lists 2/3 Identity.
- Maya Triman (Slack 08-24): from September Mike Dolt + Jaime own the crediting logic; AP helped for August.
- Mike Dolt (Slack 08-24): "the other way around" — his side owns the monthly script, and they planned Maya's
  team owns the graph part; he does not know who owns the logic and asked Malachi to start a clarifying thread
  with him/Jaime + ID folks + Maya. Settling mechanism: that thread (venue: #identity-crediting, where the
  rules are decided). Until it lands, do not treat any single claim as the ownership answer.

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
