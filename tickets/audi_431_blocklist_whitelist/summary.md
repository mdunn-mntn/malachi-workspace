---
doc_type: ticket
title: "AUDI-431: Make Changes to Blocklist or Whitelist"
status: in_progress
date: 2026-08-10
summary: "Re-assess most-common missing domains for whitelist/blocklist adds + wcv vertical corrections"
result: "94.2% of uncategorized volume resolved; 2,912 BL + 102 WL adds, 76 real stores rescued; pending Ryan deploy"
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
- **Vertical corrections (double-LLM, judge + defend, wrong only on agreement)**: **76 of the top 500 wcv domains agreed-wrong**, 12 unsure. Head: yahoo.com→Dating & Relationships (2.33B urls/7d), google.com→Security Software, facebook.com→B2B Sales & Marketing, myshopify.com→Family Planning, bing.com→Theatre/Dance/Films. Most suggested fix: NONE - not verticalizable.
- **Impact split (corrected 2026-08-10 after reading `ip_vertical_associations.py`):** that model filters `is_ecommerce OR is_whitelist`, requires non-null vertical, then **anti-joins the blocklist** — so blocklisted domains never reach IP↔vertical pairs. Of the 76 wrong verticals, **40 are blocklisted (inert) and only 36 are live pollution** (413M urls/7d, led by facebook.com 85M, nextmillmedia.com 51M, smilewanted.com 49M). **An earlier claim in this ticket that yahoo.com's wrong vertical pollutes IP associations at 2.33B urls/wk was WRONG** — yahoo.com is blocklisted. The `Impact` column on the corrections tab now carries Live vs Inert.
- **New defect found in the existing lists: 362 domains are in BOTH the whitelist and the blocklist** (`outputs/audi_431_whitelist_blocklist_conflict.csv`), including google.com, yahoo.com, myshopify.com. Pre-existing in the 2025-09-23 files, not introduced here. Blocklist is checked first at every consumer, so it wins and the whitelist entry is dead weight. Flagged for Ryan at deploy.

## 5. Solution
- **Deliverables** (all in `outputs/`, workbook in Drive `Tickets/AUDI-431/`):
  - `audi_431_decision_sheet.csv` — 3,024 adjudicated rows, per-row band rule + designation source
  - `audi_431_blocklist_additions.csv` (1,641) / `audi_431_whitelist_additions.csv` (10) — shipped format (headerless bare domains)
  - `audi_431_ecommerce_blocklist.csv` — full merged replacement (3,105 = existing 1,464 + adds)
  - `audi_431_vertical_corrections.csv` — top 500 wcv domains, 76 agreed-wrong with suggested verticals
  - `AUDI-431 Blocklist Whitelist Reassessment.xlsx` — branded workbook (Decisions / Manual review / adds / Junk / Corrections / TI-200 / Impact / Queries / Read me)
- **Impact (updated 2026-08-11 after AI-verified promotion)**: **2,657 blocklist + 15 whitelist adds resolve 90.7% of 28d uncategorized visit volume**; only **352 rows (3.7% of volume)** remain for hand review. Merged blocklist = 4,121 domains.
- **Promotion pass (2026-08-11, per Malachi: "classify all we're confident in, leave the unsures to me")**: AI verdicts at **confidence >= 0.7** promoted from advisory to real designations, after adversarial verification. Confidence proved well-calibrated by verdict: `not_ecommerce` median 0.85, `ecommerce` median **0.55** (the model itself doubts most shop calls), `unsure` max 0.65.
  - **Whitelist: 8 candidates -> 3 killed -> 5 promoted.** Refuters fetched the live sites. `bookru.shop` is a Spanish content-farm blog (the .shop TLD fooled the classifier), `cardcluster.de` is a Yu-Gi-Oh deck builder whose per-card price widgets faked a 0.71 median, `exileeditions.com` showed 45,074 URLs for a ~320-title publisher (140x mismatch) and the domain refused connections.
  - **`buyrexroth.com` is the counter-case worth keeping:** med_score 0.33 with 0.03% of URLs over the cutoff, yet verifiably a direct-sales Rexroth parts store with prices, cart and order history. A classifier miss that the whitelist exists to correct.
  - **Blocklist: 1,016 promoted, 0 disputes across a 150-row two-lens stratified sample** (top-50 by volume + 100 random). Band held.
  - Remaining 352 = 106 AI-unsure + 214 low-confidence not-ecommerce + 31 low-confidence ecommerce + 3 refuted whitelist candidates + 1 unreviewed (`thesprucepets.com`).

### Phase 7: live-site fetch of every on-the-fence domain (2026-08-11, per Malachi)
Malachi asked whether verdicts were name-based or knowledge-based. **Honest answer: the first AI pass had NO web access** — it used prior knowledge for recognised sites and domain-name semantics + score signals otherwise, which is exactly why 106 came back unsure. Standing rule he then set: **fetch them all; when a domain is not clearly a shop, side with blocklist.**
- Two workflows, 31 agents, ~950 live fetches. Every verdict cites what was seen on the page. Whitelist requires corroboration (2 agreeing lenses, or fetch + independent confirm); not-a-shop / unreachable / disagreement -> Blocklist.
- **342 of 352 resolved: 11 real shops, 331 blocklist.** 10 left blank (nothing fetchable at all).
- **`pct_ge_04` near 100% is NOT evidence of a shop — at any median.** Of the 41 high-share holdouts, only 2 were real stores. A prediction made in this ticket that the 12 with median >= 0.80 would be genuine catalogs was WRONG (1 of 12); one real shop (`seranova.com`) sat at median 0.44. The population is content farms, MFA article sites, video/stream players, scraper tools, classifieds and dead domains.
- **We are scoring domains that do not exist.** `cootlogix.com` (88M rows/28d, rank 19) returns a Wix "ConnectYourDomain Error" 404 on every path — no site is connected. `o11.tech` has no A record. 9 of the 41 high-share domains were dead/unreachable.
- Real shops found by fetching that no score band would have caught: `buytavio.com`, `callascleaneats.com`, `docsdiesel.com`, `homeviable.com`, `mynuora.com`, `onuia.com`, `pixelframe.design`, `saxon-brands.com`, `seranova.com`, `telcom-data.com`, `tryrovina.com`.

### Final state (superseded by the Phase 9 sweep below)
2,988 blocklist + 26 whitelist adds; 94.2% of volume; 10 rows remaining.

### Phase 8: blocklist audit — the promoted rows needed checking too (2026-08-11, Malachi's call)
Malachi asked whether the confidence-promoted rows should also be verified. Exposure: **2,483 of the 2,988 blocklist adds had never been individually checked** — 866 AI-promoted (basis = a no-web-access verdict + a 150-row sample) and 1,617 score-band (basis = the model itself, med<=0.05 & pct_ge_04<=0.05).
Stratified fetch-audit, 300 sampled per tier (top-50 by volume + 250 random), 39 agents, ~1,500 fetches:

| Tier | Sampled | Real stores found | Rate | 95% upper | Implied misses in tier |
|---|---|---|---|---|---|
| AI-promoted (no web access) | 300 of 866 | 8 | 2.7% | 5.2% | up to 45 |
| Score-band med<=0.05 | 300 of 1,617 | 1 | 0.3% | 1.9% | up to 30 |

**Both tiers breach the 1% sweep threshold, so the answer to "should we check these too" is empirically yes.** Up to ~75 real retailers would otherwise have been silently discarded.
- **The systematic blind spot: content sites with a store attached.** Every AI-tier miss is a blog running its own WooCommerce/Shopify shop — `dimitrasdishes.com` and `keviniscooking.com` (recipe blogs selling spice rubs), `hearthookhome.com` (224 crochet-pattern products), `homesteadandchill.com` (handmade skincare), `modernmrsdarcy.com`, `thehawaiivacationguide.com` (itineraries), `stotranidhi.com` (26-title imprint), `musescore.com` (sheet music). A no-web-access reviewer sees recipes, calls it a blog, and never checks `/shop`.
- **7.2% of audited domains (43 of 600) are unreachable** — dead domains still being scored daily.
- Exhaustive sweep of the remaining 1,883 launched, with the blind spot written into the prompt (check `/shop`, `/store`, shop subdomain, and the nav before calling anything not-ecommerce).

### Pi-hole DNS contamination check (Malachi's catch, 2026-08-11)
Malachi asked whether his own Pi-hole was making live domains look dead to the fetch agents. Tested per-domain rather than disabling it: `dig` local vs `@8.8.8.8` across all 186 domains any agent called unreachable.
- **27 were Pi-hole false negatives** (local `0.0.0.0`, real public A record); 122 resolve on both resolvers and failed for other reasons (closed ports, 403, Cloudflare); 37 have no DNS anywhere and are genuinely dead.
- Re-fetched all 27 via `curl --resolve <d>:443:<public_ip>`: **13 returned HTTP 200, and ZERO had any cart/checkout/WooCommerce/Shopify signal.** They are adult sites, Indonesian piracy/manga readers, video-viral pages and adtech. All belong on the blocklist regardless, so **the contamination changed no outcome here** — but it would in any task whose domain set is not junk.
- `cootlogix.com` stays confirmed dead: even via public DNS it returns a Wix "ConnectYourDomain Error" 404.
- Durable lesson captured: memory `reference_pihole_dns_contaminates_fetch` (never accept "unreachable" from a fetch agent on this Mac without the DNS diff). Files: `outputs/audi_431_dns_check.csv`, `outputs/audi_431_pihole_recheck.csv`.

### Phase 9: exhaustive sweep + FINAL STATE (2026-08-11)
All 1,883 remaining unverified blocklist rows fetched. **128 agents, 5,734 live fetches, 0 errors, ~2h.** Combined with the audit sample, **2,484 proposed blocklist rows were individually checked**.
- **76 real stores rescued from the blocklist to the whitelist — a 3.06% false-blocklist rate**, carrying 156M rows/28d that would have been permanently discarded. 91 were claimed, 15 rejected by the independent confirm fetch (affiliate-only, off-domain storefronts, content subscriptions).
- **The blind spot held exactly as predicted: creator blogs with an attached shop.** Recipe/craft/travel blogs selling their own goods on a shop subdomain or `/shop`: `keviniscooking.com`, `dimitrasdishes.com`, `hearthookhome.com`, `homesteadandchill.com`, `pantrymama.com`, `amigurumicorner.com`, `butterwithasideofbread.com`, `joyfilledeats.com`, `aspicyperspective.com`. Plus mainstream publishers with first-party Shopify stores: `bonappetit.com`, `newyorker.com`, `lemonde.fr` (boutique.lemonde.fr), `sfchronicle.com`, `mysanantonio.com`, `goheels.com`.
- The confirm stage earned its place by catching the near-misses: `theatlantic.com` and `fextralife.com` redirect to third-party retailers (zazzle, creator-spring) so they stay blocklisted, and `harpersbazaar.com`/`popularmechanics.com` sell only magazine subscriptions.
- **165 unreachable** across the sweep (6.6%), net of the Pi-hole correction above.

**FINAL: 2,922 blocklist + 102 whitelist adds resolve 94.4% of 28d uncategorized visit volume.** Merged blocklist = 4,386 domains. **Zero rows undecided.**
Tail resolution (2026-08-11): of the last 10, six were dead on inspection (3 no DNS on any resolver, 3 returning 404 via public DNS) so they blocklisted under the standing rule; the final 4 sat behind Cloudflare bot walls and **Malachi called them all non-ecommerce** (`onechicday.com` a fashion/news blog, plus `prettyinpink.ru`, `watchluna.com`, `streetscan.co.uk`), recorded as `designation_source: human`. The Manual review tab is now omitted from the workbook rather than shipped empty.
**Verification standard achieved: every shipped designation is either score-banded with adversarial QC, or backed by a live page fetch citing what was seen; every whitelist entry is corroborated twice.**

### Defect: the workbook silently shipped a stale view (caught by Malachi, fixed 2026-08-11)
`build_workbook.py` re-read `audi_431_decision_sheet.csv` and applied ONLY the QC demotions, while `build_lists.py` applied the full overlay chain. Every rebuild after the promotion pass therefore wrote a fresh file with three-passes-stale contents — Whitelist tab showing 10 rows instead of 102, Manual review showing 1,373 instead of 10 — and I reported it as updated each time. The file mtime moved, so nothing looked wrong.
**Root-cause fix:** `artifacts/audi_431_common.py` `load_designated_sheet()` is now the single resolver of designations (QC demotions -> ai-verified -> site-fetch -> sweep-fetch -> human, in order); both builders import it, so a new adjudication pass is registered once in `OVERLAYS` and cannot reach one output but not the other. Also fixed: the Blocklist adds tab filtered to `band == auto_blocklist` and showed 1,581 of the 2,912 shipping rows; it now shows all of them with a Source column.
**Verified after fix:** workbook tabs match the shipped files exactly (Whitelist 102 = `audi_431_whitelist_additions.csv` 102; Blocklist 2,912 = `audi_431_blocklist_additions.csv` 2,912; Manual review 10).
**Lesson:** when two deliverables derive from the same state, they must share one resolver function — not two copies of the transform. A regenerated file is not evidence of a regenerated result; assert the output against the source of truth.
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
