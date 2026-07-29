---
name: reference_audience_intent_scoring_dag
description: "AUTHORITATIVE prospecting-intent scoring model (Ryan Kleck audience_intent DAG): HI 10K = in Vertical(DS13) AND in Keywords(DS19), BOTH required; vertical is advertiser-supplied so DS19-only still reaches HI"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 604faaf9-ab5f-4b71-bb07-1a88aa0b430e
doc_type: memory
keywords: [audience_intent dag, prospecting intent scoring, HI 10K, DS13 vertical, DS19 keywords, ryan kleck, PP 8K, MI unscored, advertiser_intent, vertical keyword intersection]
domain: [audience-scoring, bidding]
lifecycle: active
last_verified: 2026-07-22
---
**Authoritative source for how prospecting intent scores are assigned** (Ryan Kleck's Confluence page,
`audience_intent` DAG in airflow-ti, `dags/audience_intent/audience_intent.py`, daily batch ~3–7 AM UTC).
Two products; **we use PROSPECTING scores**:

- **prospecting_intent** (per `ip, advertiser_id, campaign_group_id, campaign_id`; `gs://household-scoring-prod/output/scoring/prospecting_intent`):
  - **HI 10K = in Vertical (DS13) AND in Keywords (DS19) — BOTH required**
  - PP 8K = in vertical, no keyword
  - MI 3333–6665 = in bucket, not vertical (ranked by page views)
  - Unscored (prev Max Reach) = outside bucket/vertical but INSIDE keywords
- **advertiser_intent** (per `ip, advertiser_id`; `…/advertiser_intent`; sibling `household_scoring__advertiser_intent__v1`):
  pre-batch fallback so a new campaign has scores before the batch runs. **HI = in Vertical only (no keyword split); PP = N/A.** Not really used.

**The key reconciliation (why DS19-only reaches HI despite HI needing both):** the **Vertical (DS13) is the
ADVERTISER's vertical**, always fed into the prospecting score; **Keywords (DS19) are present only when the
campaign carries a DS19 leaf.** That asymmetry means a keyword-only config still intersects its keyword IPs
with the advertiser's vertical → HI, while a vertical-only config (no DS19) has no keyword set → every
in-vertical IP is vertical-no-keyword → PP, 0 HI. Empirically DS19-only delivers 69% HI / ~1% PP / 7% MI /
~22% MR-unscored — the 22% is exactly Ryan's "outside vertical but inside keywords" row, confirming the model.

**Corrects the earlier "independent axes / HI needs only DS19" framing** — HI is the vertical∩keyword
intersection, not an independent keyword axis. Terminology: Bucket = industry (3-digit DS13), Vertical =
subindustry (6-digit DS13); DS19 = keywords. Related: [[reference_mm_component_taxonomy]],
[[reference_bidder_scoring_reality]], [[reference_fangorn_tier_assignment]], [[project_audi_1083_mm_classifier]].
