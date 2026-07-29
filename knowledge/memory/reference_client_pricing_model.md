---
name: client-pricing-model
description: Client billing is config-driven per campaign — pricing_model_type fixed_cpm vs custom_margin (cost-plus on variable win price); SQLMesh advertiser_attributes is the lookup. INTERNAL-ONLY.
metadata: 
  node_type: memory
  type: reference
  originSessionId: 11120755-d5de-4ee7-83cd-aef7c4761482
doc_type: memory
keywords: [client_pricing_model, client, pricing, model, billing, config, driven, campaign]
domain: [reference]
lifecycle: active
last_verified: 2026-07-13
---
From SteelHouse/sqlmesh CIL pipeline (cil__impression_info.sql → cil__spend_calcs.sql), 2026-07-09:
- Underlying win price (`media_cost`) is always per-impression variable (RTB second-price, from
  `win_cost_micros_usd`).
- What the client is billed depends on a per-campaign `has_cpm` flag (target_cpm/cpi configured at
  campaign-group, advertiser-channel, or channel level):
  - `has_cpm=TRUE` → **fixed_cpm**: flat negotiated CPM regardless of auction cost (e.g. Stagwell;
    audit_13_fixed_cpm.sql monitors impression caps).
  - `has_cpm=FALSE` → **custom_margin** (cost-plus): flat margin % applied on the variable win price —
    same campaign, different impressions bill differently.
- Canonical lookup: `pricing_model_type` in SQLMesh `advertiser_attributes.sql`.

**INTERNAL-ONLY** — the formulas involve take rates, which are sensitive: [[take-rates-sensitive]].
No universal flat rate across clients for the same impression volume.

**Blended margin color (user, AUDI-1089 meeting 2026-07-13, INTERNAL):** CPM upcharge ~10-30%;
large clients ~10%, average ~20%; blended media CPM $10-30 leaning ~$10. Public company — specifics
not disclosed. Use 15/20/30% ladder for WTP math; never in shareable docs.
