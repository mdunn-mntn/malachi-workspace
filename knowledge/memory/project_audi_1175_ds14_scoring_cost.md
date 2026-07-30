---
name: project_audi_1175_ds14_scoring_cost
description: "DS14 scoring-cost optimization — backlog, sprint-ready, gate-safe, ~$2-11k/mo"
metadata: 
  node_type: memory
  type: project
  originSessionId: 90ae114b-824a-4a09-8ae2-53026431ded6
doc_type: memory
keywords: [audi_1175, ds14, scoring cost optimization, audience_intent, dataproc, ipdsc, prospecting_keywords, hhst threshold, idso bos, dataproc billing, audi_1176]
domain: [project, audience-scoring, bigquery]
lifecycle: active
last_verified: 2026-07-29
---
DS14 ("MNTN Global Data") is an **availability gate** ANDed onto ~every audience expression — limits bids/counts to IPs seen in ~8 days (per-IP serving TTL in membership-db `data_source_ttls['14']=8`; built daily from augmentor_log 1d + bidder_auction_events 1d + guid_log 4d; materialized in `ipdsc__v1` data_source_id=14). Born 2025-08-13 / prod 2025-12-11 (AUDI-369), **post-MM** — not a pre-MM recency filter. **Source code (GitHub SteelHouse org, verified 2026-07-30):** lookback constants `augmentor_log_lookback_days = 1` / `guid_log_lookback_days = 4` in `mntn_global_data` (and `airflow-ti`) `spark/create_mntn_global_data_pyspark.py`; materialized by `airflow-ti` `spark/data_source/populate_data_source.py::_get_data_source_14_df` (L933-953); 8d TTL in `membership-db` `server/config/config.yml` `data_source_ttls['14']=8` (enforced `server/src/config/mod.rs`).

**The optimization:** `audience_intent` scores the full **31-day** IP universe, but only the ~8-day DS14 slice is biddable → **69% (DS19 MM Core) / 39% (DS13 verticals) of scored IPs are non-biddable**, recomputed daily for nothing. Gate the scoring input to DS14-addressable → **~$1.3k–11k/mo (~up to $130k/yr)** vs a ~$39k/mo Dataproc-serverless DAG. Biggest lever = `prospecting_keywords` (DS19, 34% of DAG). Zero coverage loss (returning IPs scored the day they re-enter DS14; RTC covers intra-day).

**Tickets (both stay in Backlog per user directive 2026-07-28):**
- **AUDI-1175** (spike) — ANSWERED: sizing + cost + full consumer/safety audit. Backlog by choice.
- **AUDI-1176** (impl) — sprint-ready self-contained work order (BLUF/problem/solution/implementation/impact/gain in `summary.md`). Blocked-by AUDI-1175.
- **RFD published to Confluence TAR 2026-07-29** — https://mntn.atlassian.net/wiki/spaces/TAR/pages/3722346650 (tiny /x/moDe3Q). Source `artifacts/audi_1175_rfd_draft.md`; adversarially hardened against 2 reviews (`audi_1175_rfd_adversarial_review.md`). Ask = approve work (gated on 3 confirms: gate=bidder predicate, billing confirms bill drops, shadow-run parity) + name audience_intent co-owner + heads-up DDM/Devon. **$ is order-of-magnitude & unfirmed** — Gate-B billing pull is IAM-walled for the analyst (dataproc.batches denied, no reachable GCP billing export); ready query in `queries/audi_1175_dataproc_billing_probe.sql` for whoever has billing IAM.

**Key safety finding (why it's safe):** the prod HHST threshold recommender does NOT couple to the scored universe. Its population is **auction-scoped** — chain = camperbid **v3/v4** compute → CoreDB `performance.optimized_intent_thresholds` → **`SteelHouse/idso` BOS** hourly-cron upsert (sole writer of `dso.household_score_thresholds`, PK campaign_id, ~2K PTV camps/day). Only the fenced DDM `test_hhst_campaigns` pilot reads `prospecting_intent`, and it never writes the applied table. Applied-threshold distribution: 65% of 32,550 campaigns already at Max Reach (threshold=0). Full detail lives in git `knowledge/data_knowledge.md` (DS14-opt block) + the ticket summaries.

See [[reference_hhst_efficiency_sizing]], [[reference_frequency_capping]], [[reference_causal_impact_pattern]].
