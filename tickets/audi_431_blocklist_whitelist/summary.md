---
doc_type: ticket
title: "AUDI-431: Make Changes to Blocklist or Whitelist"
status: in_progress
date: 2026-08-10
summary: "Re-assess most-common missing domains for whitelist/blocklist adds + wcv vertical corrections"
result: "in progress"
question: "Which of the top most-common missing domains (28d volume) belong on the ecommerce whitelist vs blocklist, and which top-traffic wcv domains carry a wrong vertical?"
framing_state: locked
---

# AUDI-431: Make Changes to Blocklist or Whitelist

**Jira:** https://mntn.atlassian.net/browse/AUDI-431
**Status:** backlog
**Date Started:** 2026-08-10
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Which of the top most-common missing domains (28d `missing_domains` volume) belong on the ecommerce whitelist vs blocklist, and which top-traffic wcv domains carry a wrong vertical? Answered when every top-N domain has a designation or an explicit manual-review flag, and corrections carry agreeing double-LLM verdicts.
- **Goal (why / the decision):** Ryan Kleck decides which domains to deploy to the two lists (untouched since 2025-09-23) and which vertical overrides to apply — stops futile re-scoring of high-volume domains and grows DDP vertical coverage. Tier 3 infra: DS13 verticals feed MM 2.0 scoring states (PP 8000 / HI 10000), supporting targeting quality and revenue retention. Reopened by Bryce 2026-07-30 per Malachi.
- **Objective (done-when):** Branded xlsx decision workbook in Drive + format-identical list files (`audi_431_ecommerce_blocklist.csv` merged replacement, `audi_431_whitelist_additions.csv`) + vertical-corrections tab; confident bands auto-designated with per-row explainability, ambiguous band blank for Malachi; hygiene checks pass (0 overlaps, additions-only diff); Slack handoff draft ready. Binary: files + workbook exist and clear those bars, or not done.
- **Approach (how):** Candidates = 28d of `gs://mntn-data-archive-prod/vertical_categorizations/missing_domains/` (local pandas, ~70 MiB), junk-tiered, overlap-gated vs current lists + wcv. Adjudication = prod `ecommerce_score` aggregates per domain from `ddp_url_verticals` (7d closed window, BQ external tables via bq_run.sh, us-central1) banded by TGT-4016 P90/P10; LLM QC cross-check on auto bands; TI-200's ~140 Unsure re-checked. Corrections leg = double-LLM judgment over top-traffic wcv domains vs `tpa.dim_vertical` roster, keep agreeing "wrong" only. Assumptions to resolve first: prod missing_domains excludes the lists (PR #102); `is_in_vertical_mapping` exists in ddp parquet; P90/P10 re-derivable from tgt_4016 notebook.
- **What would change the answer:** Overlap gate nonzero (missing_domains does NOT exclude current lists) → stop, re-read prod code before adjudicating. Score coverage sparse (<50% of top-N with ≥30 scored URLs) → bands collapse to manual, deliverable becomes signal-sheet only. P90/P10 not re-derivable → fall back to prod's flat 0.4 with a wider manual band.

## 1. Introduction
The vertical categorization pipeline (DS13) processes `site_visit_signal` URLs: domain → ecommerce blocklist check (stop) → whitelist check (= ecommerce) → else URL-only ecommerce model @0.4 → if ecomm, vertical lookup in `website_crawl_verticals` (wcv). Domains absent from wcv get no vertical. The `missing_domains` dbt model (TI-253, daily, `gs://mntn-data-archive-prod/vertical_categorizations/missing_domains/`) tracks svs domains not in wcv. Periodically the most-common ones must be re-adjudicated into the whitelist or blocklist so they stop being processed futilely (TI-200 was the Sep-2025 pass).

## 2. The Problem
- Both lists untouched since 2025-09-23 (blocklist 1,464 domains; whitelist 3.31M); wcv last refreshed 2025-11-07.
- High-volume unknown domains are re-scored daily with no outcome; ecommerce domains missing from wcv get no vertical (coverage gap in DS13 → MM 2.0 states).
- TI-200 left ~140 domains 'Unsure'; Ryan also wants misclassified wcv domains flagged (vertical corrections).

## 3. Plan of Action
1. Verify prod dbt models (missing_domains_df, ddp_url_verticals) vs vendored TI-253 copies; pull list files + wcv; schema-probe parquet; re-derive TGT-4016 P90/P10.
2. Build 28d candidate frame locally (total_count, days_seen, junk tiers); overlap gate vs lists + wcv (kill criterion).
3. BQ (bq_run.sh, us-central1, external tables): Query A per-candidate `ecommerce_score` aggregates; Query B traffic-ranked wcv domains for corrections.
4. Band adjudication (auto-WL ≥P90, auto-BL ≤P10 or junk; ambiguous blank for Malachi); LLM QC cross-check on auto bands; TI-200 Unsure revisit; double-LLM corrections fan-out.
5. Impact sizing + hygiene (0 overlaps, additions-only diff).
6. Deliverables: branded xlsx to Drive, list files, Jira comment, Slack handoff draft to Ryan; /capture.

## 4. Investigation & Findings

### Phase 1: prod behavior verified (2026-08-10)
- **Current prod `missing_domains.py` (SteelHouse/dbt main) anti-joins whitelist AND blocklist before the wcv anti-join** (PR #102 logic is live). Candidate universe = svs domains net of all three lists. The TI-253 vendored copy predates this. Copies of current prod models in `artifacts/prod_models/`.
- **`ddp_url_verticals.py` (current prod)**: scores EVERY URL with MLflow `{env}.ml.ecommerce_classifier@champion` (no blocklist filter; whitelist is a LEFT join flag `is_whitelist`, not a filter). Output cols: `ip, domain, uid, time, vertical_id, bucket_id, vertical_name, is_ecommerce, is_in_vertical_mapping, data_source_id, input_timestamp, url, ecommerce_score, is_whitelist, dt`. `ECOMMERCE_THRESHOLD = 0.4`. `is_in_vertical_mapping` confirmed — Query A can filter in-scan without joining wcv.
- **TGT-4016 thresholds re-derived from `tgt_4016_thresholds.ipynb` outputs**: P90 = 0.9181 (ecommerce-confident), P10 = 0.0002 (non-ecommerce-confident); URL-level distribution, n = 251.7M, 2025-05-12 snapshot. Prod's flat 0.4 sits between them.
- Both list files in GCS dated 2025-09-23 (untouched since TI-200); wcv last refreshed 2025-11-07 (~1.42M domains).
- `missing_domains` GCS: daily partitions 2025-11-02 → yesterday, ~2.5 MiB/day; model emits `domain, count` per dt (2-day read window per run, so adjacent partitions double-count a day; dedupe by (domain, dt) at read).

### Phase 2-5: candidates, scores, bands (2026-08-10)
- **Overlap gate 0/0/0** (candidates ∩ blocklist / whitelist / wcv) — kill criterion passed; prod missing_domains is net of all three lists as coded.
- 28d window (2026-07-13 → 08-09): 1,072,286 distinct missing domains, 16.05B rows volume. Extremely head-heavy: 408 clean domains = 80% of volume.
- Junk: 34,361 domains / 1.19B vol, almost all `trailing_dot` tldextract artifacts (`comhttps.` 793M, `android-app.` 231M). 24 stable ones (≥1M vol, ≥14d) = 7.1% of all missing volume → Blocklist adds per the `localhost.` precedent.
- `aol.com`/`yahoo.com` absent from candidates because they ARE in the existing blocklist CSV (ddp_url_verticals still scores them daily — it consumes no blocklist).
- **Query A** (7d ddp scores, 3,000 candidates, 109s): 100% coverage. Bands: auto_WL 15 (med≥0.9181 & pct_ge_04≥0.9), auto_BL 1,617 (med≤0.05 & pct_ge_04≤0.05 — <5% of URLs would clear prod's 0.4 gate), manual 1,368 (40.1% vol, blank designation for hand review, volume-sorted).
- **Query B** (7d wcv-classified traffic, 15s): top-traffic wcv domains carry blatant vertical errors — yahoo.com→"Dating & Relationships" (2.33B urls/7d), google.com→"Security Software", outbrain.com→"B2B - Sales & Marketing", msn/foxnews/weather→"Current Affairs" (plausible) — top 500 sent to double-LLM corrections audit.
- **TI-200 Unsure revisit self-resolved**: all 149 are now IN wcv (2025-11-07 crawl refresh categorized them); 9 also in blocklist, 24 in whitelist. Nothing to adjudicate.
- **List files + hygiene**: 1,641 BL adds + 15 WL adds (pre-QC) = 54.3% of 28d missing volume resolved; merged blocklist 3,105 domains; dedupe/disjoint/additions-only checks pass.
- Hand-check: news/UGC brands correctly auto-BL (nytimes, huffpost, people, tumblr, live.com, allrecipes); .store-TLD content farms correctly BL; 3 of 15 auto_WL look like model over-scores on news/blog sites (proactiveinvestors.co.uk, immigrationnewscanada.ca, melhoresreceitas.blog) — QC workflow will adjudicate.

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
