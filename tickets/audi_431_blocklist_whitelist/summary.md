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
**What these domains are, in plain terms:** the sites people visit that MNTN's pipeline can't do anything with. When a visit signal arrives (someone hit nike.com/shoes), we extract the domain and try to categorize it. The domains in this ticket fail every step — not in the blocklist ("known not-ecommerce, skip"), not in the whitelist ("known ecommerce"), and not in the crawled domain→vertical map (wcv). Each one is re-scored by the ecommerce model every day and still yields no vertical for the IP: wasted compute, zero signal. This ticket ranks the most common of them and adjudicates each into blocklist (not a store), whitelist (is a store), or manual review.

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

### Phase 4b: QC + corrections workflow (45 agents, 2026-08-10)
- **Adversarial QC**: auto_BL sample 0/100 disputed (band holds at 0%); auto_WL 5/15 disputed → demoted to manual (dogzonline.com.au breeder directory, adeecodedlife.com blog, proactiveinvestors.co.uk financial news, immigrationnewscanada.ca news, melhoresreceitas.blog recipes). All 3 pre-flagged suspects confirmed; the model over-scores content sites with commerce-shaped URL structures.
- **Taxonomy constraint (defect caught by Malachi 2026-08-10, fixed same session):** the first corrections pass asked for a replacement vertical "in plain words" and supplied no roster, so suggestions were free text. Audit: of 55 named suggestions, **16 were invented** (e.g. "Media & Entertainment", "Books & Literature", and "Learning & Education Technology" — the *corrected* spelling of prod's real typo). Re-ran enum-constrained against the 152-name wcv roster (`outputs/audi_431_vertical_taxonomy.csv`); all 76 now validate, 55 named + 21 "NONE - not verticalizable". Free-text values retained as `suggested_vertical_freetext` for audit. **Lesson: any LLM step proposing a value that must JOIN to prod data has to be enum-constrained to the real roster, not free text.**
- **Vertical corrections (double-LLM, judge + defend, wrong only on agreement)**: **76 of the top 500 wcv domains agreed-wrong**, 12 unsure. Head: yahoo.com→Dating & Relationships (2.33B urls/7d), google.com→Security Software, facebook.com→B2B Sales & Marketing, cnn.com→"Learning & Eduction Technology" (taxonomy typo is real), yahoo.net→Men's Health, myshopify.com→Family Planning, bing.com→Theatre/Dance/Films, timeanddate.com→Emergency Preparedness. Most suggested fix: "not verticalizable (portal/search/adtech/webmail/infrastructure)".

## 5. Solution
- **Deliverables** (all in `outputs/`, workbook in Drive `Tickets/AUDI-431/`):
  - `audi_431_decision_sheet.csv` — 3,024 adjudicated rows, per-row band rule + designation source
  - `audi_431_blocklist_additions.csv` (1,641) / `audi_431_whitelist_additions.csv` (10) — shipped format (headerless bare domains)
  - `audi_431_ecommerce_blocklist.csv` — full merged replacement (3,105 = existing 1,464 + adds)
  - `audi_431_vertical_corrections.csv` — top 500 wcv domains, 76 agreed-wrong with suggested verticals
  - `AUDI-431 Blocklist Whitelist Reassessment.xlsx` — branded workbook (Decisions / Manual review / adds / Junk / Corrections / TI-200 / Impact / Queries / Read me)
- **Impact**: auto-adds resolve **54.2% of 28d uncategorized visit volume**; manual band holds 40.2% (1,373 rows, volume-sorted, blank designation for hand review).
- **Handoff**: Slack draft at `artifacts/audi_431_slack_handoff.md` — deploy mechanism + corrections mechanism are Ryan's call. Nothing deployed from this ticket.
- **Manual-review loop (added 2026-08-10)**: every manual-band row now carries an **advisory AI verdict** (ecommerce / not ecommerce / unsure + confidence + one-line reason; 28-batch workflow, 1,372 of 1,373 covered, `thesprucepets.com` ships blank) plus a **"Your call" dropdown** (Whitelist / Blocklist / Skip, Excel data validation on K5:K1377). Verdict split: 1,230 not ecommerce, 106 unsure, 36 ecommerce. `artifacts/audi_431_ingest_reviews.py` reads the filled dropdown back out of the Drive workbook, writes `outputs/audi_431_human_calls.csv`, and re-runs the list builder so human calls flow into the additions files with `designation_source = human`.

## 6. Questions Answered
- **Q:** Which most-common missing domains belong on which list?
  **A:** 1,641 confidently non-ecommerce → blocklist (incl. 24 stable parse-garbage strings); 10 confidently ecommerce → whitelist; 1,373 ambiguous ship blank, volume-sorted.
- **Q:** Does prod missing_domains already exclude the lists (kill criterion)?
  **A:** Yes — overlap gate 0/0/0; PR #102 anti-joins are live in prod.
- **Q:** Which top-traffic wcv domains are misclassified?
  **A:** 76 of top 500 (double-LLM agreement), led by yahoo.com/google.com/facebook.com carrying consumer verticals they shouldn't.
- **Q:** What happened to TI-200's 149 'Unsure' rows?
  **A:** All categorized by the 2025-11-07 crawl refresh; nothing left to adjudicate.

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
Routed via /capture 2026-08-10: missing_domains GCS path + semantics, ddp_url_verticals schema + no-blocklist gotcha, list staleness dates, wcv misclassification findings.

## 8. Open Items / Follow-ups
- **Malachi**: hand-fill the head of the Manual review tab (1,373 rows, volume-sorted) before shipping the lists.
- **Ryan**: confirm deploy mechanism (bucket drop vs PR) and the corrections mechanism (vertical_manual_overrides/ vs is_manual_override) — Slack draft ready in artifacts/.
- Whitelist adds carry no wcv vertical until the next crawl refresh — nominated as the backfill seed.
- improvements_backlog: quarterly list-refresh cadence (pipeline now scripted end-to-end); wcv crawl backfill seeded with whitelist adds.
