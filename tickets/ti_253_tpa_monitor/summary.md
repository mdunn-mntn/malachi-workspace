# TI-253: TPA Monitor — Missing Domains

**Jira:** https://mntn.atlassian.net/browse/TI-253
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Investigation into missing domains in the TPA (Third-Party Audience) pipeline. The TPA pipeline maps IP addresses to domain verticals — domains missing from the pipeline represent targeting gaps.

---

## 2. The Problem

Some domains were not appearing in the TPA targeting pipeline. Needed to identify which domains were missing, why they were absent, and how to remediate.

---

## 3. Plan of Action

1. Identify domains present in DDP URL data but absent from TPA vertical mappings
2. Understand the missing domain patterns
3. Build monitoring script to catch future gaps
4. Implement IP-to-vertical association fix

---

## 4. Investigation & Findings

- Missing domains identified via `artifacts/ti_253_ddp_url_verticals.py`
- Missing domain processing script: `artifacts/ti_253_missing_domains.py`
- Missing domains list persisted in `artifacts/ti_253_missing_domains.yml`
- Monitor script built: `artifacts/ti_253_monitor_missing_domains.py`
- IP-vertical association creation: `artifacts/ti_253_create_ip_verticals_associations.py`

---

## 5. Solution

Built monitoring pipeline for missing TPA domains. Created IP-vertical associations to fill gaps.

---

## 6. Questions Answered

- **Q:** Which domains are missing from TPA vertical coverage?
  **A:** See `artifacts/ti_253_missing_domains.yml` for the tracked list.

---

## 7. Data Documentation Updates

- `tpa.membership_updates_logs` — contains IP audience membership flags
- IP → vertical → TPA pipeline confirmed as key data flow

---

## 8. Open Items / Follow-ups

- Monitoring script (`ti_253_monitor_missing_domains.py`) may need periodic re-runs.
- Similar code in `documentation/code_snippets/` (tpa_ipdsc_export.py, ip_vertical_associations.py).

---

## Drive Files

- (None found in Drive for TI-253)
