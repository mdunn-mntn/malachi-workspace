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

Analysis notebook: `artifacts/output.ipynb`

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
