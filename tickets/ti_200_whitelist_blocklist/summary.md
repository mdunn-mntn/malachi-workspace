---
doc_type: ticket
title: "TI-200: Adding Domains to Whitelist / Blocklist"
status: done
date: 2026-05-06
summary: "Maintenance: manual overrides of ecommerce domain classifier via whitelist/blocklist"
result: "Delivered updated ecommerce whitelist and blocklist domain lists (CSV exports)"
---

# TI-200: Adding Domains to Whitelist / Blocklist

**Jira:** https://mntn.atlassian.net/browse/TI-200
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Maintenance ticket for the ecommerce domain whitelist and blocklist. Domains are added to the whitelist (confirmed ecommerce) or blocklist (confirmed non-ecommerce/excluded) to override classifier results and improve targeting precision.

---

## 2. The Problem

Classifier alone is imperfect — some domains are misclassified or need manual override. This ticket managed additions to the whitelist and blocklist.

---

## 3. Plan of Action

1. Review domains flagged for whitelist/blocklist action
2. Validate domain classifications
3. Export updated lists
4. Apply changes

---

## 4. Investigation & Findings

Multiple domain list exports in `outputs/domain_lists/`:
- `domain_list.csv` — combined list
- `ecomm_blocklist_export.csv` — blocklist export
- `ecommerce_blocklist.csv` — blocklist
- `ecommerce_whitelist.csv` — whitelist
- `whitelist_blocklist_domains.csv` — combined
- `vertical_categorizations_ecommerce_whitelist.csv` — categorized whitelist

Additional outputs (local, gitignored):
- `outputs/ti_200_ecommerce_whitelist_2.csv.gz` — updated whitelist version
- `outputs/ti_200_ecommerce_blocklist_2.csv` — updated blocklist version

> **Canonical vs duplicates (2026-07-20 audit note).** The `_2` files above are the **latest/blessed**
> versions (this is a maintenance ticket — the newest export is what shipped). The blocklist appears three
> times **byte-identical** (`ecomm_blocklist_export.csv` = `ecommerce_blocklist.csv` = `_2.csv`, all 22,351 B),
> and `whitelist_2.csv.gz` is just the gzipped `ecommerce_whitelist.csv`. **All are untracked/gitignored** —
> none are in the repo, so there was nothing to prune here; the copies are harmless local history. If tidying
> locally, keep the `_2` pair and the categorized whitelist; the other blocklist copies are exact duplicates.

Analysis notebook: `artifacts/ti_200_output.ipynb`

---

## 5. Solution

Delivered updated whitelist and blocklist domain lists.

---

## 6. Questions Answered

- **Q:** Which domains should be whitelisted/blocklisted?
  **A:** See domain list CSVs in `outputs/domain_lists/`.

---

## 7. Data Documentation Updates

None specific to BQ tables.

---

## 8. Open Items / Follow-ups

None known.

---

## Drive Files

📁 `Tickets/TI-200 Adding Domains to Whitelist   Blocklist/`
- `[TI-200] - Whitelist Blocklist.gsheet` — tracking spreadsheet
- `_TI_200__Add_more_domains__Whitelist__Blocklist.csv` — domain additions
