---
doc_type: runbook
title: "Ecommerce blocklist/whitelist quarterly refresh"
date: 2026-08-11
summary: "End-to-end repeatable process for re-adjudicating the most-common uncategorized domains into the ecommerce blocklist/whitelist. Built and proven in AUDI-431."
keywords: [ecommerce blocklist, ecommerce whitelist, missing_domains, website_crawl_verticals, wcv, vertical categorization, DS13, quarterly refresh, domain adjudication, AUDI-431]
domain: [audience-scoring, data-catalog]
---

# Ecommerce blocklist/whitelist refresh — quarterly runbook

**Read this first if you are picking this up cold.** Everything here was executed end-to-end on
2026-08-11 under AUDI-431; the scripts referenced are committed and re-runnable. Budget half a day
of wall-clock, most of it unattended agent time.

## 1. What problem this solves

MNTN's DDP pipeline categorizes site-visit domains so an IP can be given a *vertical*. Flow:

```
site_visit_signal URL
  -> registrable domain (tldextract eTLD+1)
  -> in ecommerce_blocklist.csv?  -> STOP, never categorized
  -> in ecommerce_whitelist.csv.gz? -> treat as ecommerce
  -> else score URL with the ecommerce classifier (threshold 0.4)
  -> if ecommerce: look up domain in website_crawl_verticals (wcv) -> assign vertical to the IP
```

A domain in **none** of those three lists is re-scored every single day and still yields nothing.
`missing_domains` (a daily prod job) is the list of exactly those domains. Left alone the lists rot:
before AUDI-431 they had not been touched since **2025-09-23**, and ~16B rows / 28 days of visit
traffic was hitting domains that could never produce a vertical.

**This runbook decides, for the most-common of those domains: blocklist (not a store) or
whitelist (is a store).**

## 2. Inputs (all GCS, all readable today)

| What | Path |
|---|---|
| Candidates | `gs://mntn-data-archive-prod/vertical_categorizations/missing_domains/dt=YYYY-MM-DD/` (daily, ~2.5 MiB/day, cols `domain, count`; `dt` is directory-only) |
| Blocklist (prod) | `gs://mntn-data-archive-prod/vertical_categorizations/ecommerce_domain_whitelist/ecommerce_blocklist.csv` |
| Whitelist (prod) | `.../ecommerce_domain_whitelist/ecommerce_whitelist.csv.gz` |
| Domain -> vertical | `.../website_crawl_verticals/*.parquet` |
| Prod model scores | `.../ddp_url_verticals/dt=.../` (~113 GB/day; per-URL `ecommerce_score`, `is_whitelist`, `is_in_vertical_mapping`) |
| Vertical overrides | `.../vertical_manual_overrides/*.parquet` |

**Use `gcloud storage cp`, never `gsutil -m cp`** — the latter hangs on this Mac.

## 3. Source code (GitHub)

- [missing_domains.py](https://github.com/SteelHouse/dbt/blob/main/ml_squad/models/vertical_categorization/missing_domains.py) — builds the candidate list. Anti-joins whitelist **and** blocklist **and** wcv, so candidates are net of all three.
- [ddp_url_verticals.py](https://github.com/SteelHouse/dbt/blob/main/ml_squad/models/vertical_categorization/ddp_url_verticals.py) — scores every URL via MLflow `prod.ml.ecommerce_classifier@champion`, threshold 0.4. **Consumes no blocklist**, so blocklisted domains are still scored daily.
- [ip_vertical_associations.py](https://github.com/SteelHouse/dbt/blob/main/ml_squad/models/vertical_categorization/ip_vertical_associations.py) — writes IP↔vertical pairs. Filters `is_ecommerce OR is_whitelist`, then **anti-joins the blocklist**, so a blocklisted domain never reaches an IP.
- [update_website_verticals.py](https://github.com/SteelHouse/airflow-ti/blob/main/spark/vertical_classification/update_website_verticals.py) — rebuilds wcv. Merges prior + newly classified + manual overrides, keeping one row per domain ordered `desc(is_manual_override), desc(last_modified_ts)`: **a manual override always beats the classifier.**
- [submit_html_content.py](https://github.com/SteelHouse/airflow-ti/blob/main/spark/vertical_classification/submit_html_content.py) — how verticals are actually assigned: Common Crawl homepage HTML posted to the **OpenAI batch API**.

## 4. Scripts (this repo, in order)

All under [tickets/audi_431_blocklist_whitelist/artifacts/](https://github.com/mdunn-mntn/malachi-workspace/tree/main/tickets/audi_431_blocklist_whitelist/artifacts):

| Step | Script | Does |
|---|---|---|
| 1 | `audi_431_build_candidates.py` | 28d of missing_domains -> per-domain volume/days_seen, junk tiers, **overlap gate** |
| 2 | *(BQ)* `queries/audi_431_qa_score_aggregates.sql` | per-candidate `ecommerce_score` aggregates from ddp_url_verticals |
| 3 | `audi_431_adjudicate.py` | score bands -> auto-whitelist / auto-blocklist / manual |
| 4 | `audi_431_apply_qc.py` | applies the adversarial QC pass |
| 5 | `audi_431_extract_ai_review.py` / `audi_431_promote.py` | AI verdicts -> promotions, after refutation |
| 6 | `audi_431_apply_fetch.py` / `audi_431_apply_sweep.py` | live-site fetch verdicts, rescue confirmed stores |
| 7 | `audi_431_common.py` | **single resolver** of designations — both builders import it |
| 8 | `audi_431_build_lists.py` | emits additions + both deploy-ready prod files |
| 9 | `audi_431_validate_deploy.py` | **pre-deploy gate — must exit 0** |
| 10 | `audi_431_build_workbook.py` | branded xlsx to Drive |

## 5. The process

1. **Pull inputs** (`gcloud storage cp`), 28 days of `missing_domains` + both lists + wcv.
2. **Build candidates.** Run step 1. **The overlap gate is the kill criterion**: candidates ∩ blocklist / whitelist / wcv must all be **0**. Nonzero means prod's anti-joins changed — stop and re-read the model.
3. **Score aggregates.** BQ external table over `ddp_url_verticals`, 7-day **closed** window ending `dt <= today-2` (the daily run overwrites today and yesterday). Use `bq_run.sh --location=us-central1` and a def-file with `hivePartitioningOptions: AUTO`.
4. **Band it.** Auto-whitelist `med_score >= 0.9181` (TGT-4016 P90) and `pct_ge_04 >= 0.9`; auto-blocklist `med_score <= 0.05` and `pct_ge_04 <= 0.05`; everything else manual.
5. **Fetch everything ambiguous.** This is the step that matters — see Lessons below.
6. **Validate** (step 9) then **deploy**: back up the live objects to a dated path first, upload, then **re-download and verify** SHA + that the original content is an exact prefix.
7. **Workbook** to Drive `Tickets/<KEY>/`, update `summary.md`, Jira comment.

## 6. Lessons that cost real time — do not relearn them

- **Never judge a domain without fetching it.** The first AI pass had no web access and judged from domain names plus scores. The exhaustive fetch sweep then found a **3.06% false-blocklist rate — 76 real stores** we were about to discard permanently.
- **The systematic miss is a content site with a shop attached.** Recipe/craft/travel blogs selling their own goods at `/shop` or on a `shop.<domain>` subdomain. Thousands of article URLs bury a handful of product pages, so the median score says "blog". Always check `/shop`, `/store`, `/products`, `/collections` and the nav before calling anything not-ecommerce.
- **`pct_ge_04` near 100% is NOT evidence of a store, at any median.** Of 41 such domains only 2 were real shops; the rest were content farms and template-generated URL sets scoring uniformly just above the cutoff.
- **A wrong whitelist entry is worse than a wrong blocklist entry.** Require corroboration (two independent fetches) for whitelist; default to blocklist when not clearly a shop.
- **Constrain any LLM-suggested vertical to the real 152-name roster** (`SELECT DISTINCT vertical_name` off wcv) via a JSON-schema `enum`, and reproduce prod typos verbatim (`Learning & Eduction Technology`). Free text produced 16 invented categories that join to nothing.
- **Pi-hole makes live domains look dead.** Local DNS returns `0.0.0.0` for blocked domains. Diff `dig +short <d>` vs `dig +short @8.8.8.8 <d>` and re-fetch with `curl --resolve` before believing "unreachable".
- **`find -newermt` is broken here** (find is `bfs`). Use `.claude/scripts/stall_monitor.sh` for background-task liveness.
- **Two deliverables from one state need one resolver function.** The workbook shipped a three-passes-stale view for hours because it applied a different overlay chain than the list builder.

## 7. Deploy safety

- Strictly **additive**: original content must remain an exact prefix of the new file. No reordering, no deletions.
- **362+ domains are in BOTH lists** (pre-existing). Blocklist is checked first everywhere, so it wins. Do not assert "disjoint"; assert "no *new unreviewed* conflict".
- Back up to `.../ecommerce_domain_whitelist/backup_pre_<ticket>_<date>/` before every upload. Rollback = copy those objects back.
- Verify from the **live** object after upload, never from the local copy.

## 8. Prior art

- **AUDI-431** (2026-08-11) — this build. 2,931 blocklist + 102 whitelist adds, 94.4% of uncategorized volume, 76 stores rescued. [summary.md](https://github.com/mdunn-mntn/malachi-workspace/blob/main/tickets/audi_431_blocklist_whitelist/summary.md)
- **TI-200** (2025-09) — the manual predecessor (Google Sheet, no reproducible SQL).
- **TGT-4016** — where the P90/P10 thresholds come from.
