---
name: take-rates-sensitive
description: "Take rates / client billing margins are sensitive-private (ray) — cost analyses use base media/data cost only, never platform_spend or billed math in shareable artifacts"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 11120755-d5de-4ee7-83cd-aef7c4761482
doc_type: memory
keywords: [take rates, margins, media_spend, data_spend, platform_spend, billing, confidential, AUDI-1089, willingness to pay, ray]
domain: [pricing, workflow]
lifecycle: active
last_verified: 2026-07-15
---
ray (#data, 2026-07-09, answering Malachi's billing-rate question for AUDI-1089): take rates are a
"very sensitive/private topic." The sanctioned pattern for cost-related analysis: **evaluate against base
costs** — `media_spend` (raw media cost, what MNTN pays for the impression; advertiser-agnostic,
inventory/deal-based) and `data_spend`. There IS a ~static markup on all impressions, but do not build or
share analyses exposing it.

**Why:** margin structure is confidential even internally; an analysis circulating take-rate math creates risk.

**How to apply:** any vendor-value / cost analysis shared beyond self (decks, Jira, Slack, docs) uses
media_spend + data_spend lenses only. No platform_spend, no billed=media+data+platform, no take-rate or
margin formulas in shareable artifacts. Client pricing mechanics stay internal-only: [[reference_client_pricing_model]].

**Companion guidance (ray, same thread):** "we get paid no matter what" — billed/cost dollars are NOT the
vendor-value metric; end-to-end value to the customer (performance → retention) is. Vendor-value cases lead
with performance metrics (e.g. VR of impressions to vendor-sole IPs vs same-band multi-source); cost lenses
serve only as willingness-to-pay anchors. Malachi adopted this 2026-07-09 (AUDI-1089).

**Shareable query packages (2026-07-15):** MANIFEST/guide copies that leave the team must
genericize margin multipliers ("× internal margin parameters — values in fill_template.py")
— validators verify query outputs, not pricing policy. Queries themselves never carry margins.
