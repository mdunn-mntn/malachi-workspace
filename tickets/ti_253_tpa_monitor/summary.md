---
doc_type: ticket
title: "TI-253: TPA Monitor — Missing Domains"
status: done
date: 2026-03-04
summary: "Find domains missing from the TPA IP-to-vertical pipeline and monitor future gaps"
result: "Built a monitor for missing TPA domains; created IP-vertical associations to fill gaps"
keywords: [TPA, third-party audience, missing domains, IP-to-vertical, membership_updates_logs, vertical mapping, DDP URL, monitor]
---

## TL;DR

**Q:** Produce the TL;DR card for ti_253_tpa_monitor and extract any durable delta_facts not already in the knowledge docs.

**A:** TI-253 (TPA Monitor — Missing Domains, done 2026-03-04): built monitoring for domains missing from the TPA (Third-Party Audience) IP-to-vertical pipeline, and created IP-vertical associations to fill identified gaps. Some domains present in DDP URL data were absent from TPA vertical mappings (targeting gaps); these were identified, a monitor script was built to catch future gaps, and IP-to-vertical associations were created as the fix. The tracked missing-domain list lives in artifacts/ti_253_missing_domains.yml. Deliverable artifacts (Python scripts + yml) are in the ticket's artifacts/ folder; the monitor may need periodic re-runs.

**How:** Read tickets/ti_253_tpa_monitor/summary.md in full (outputs/ and queries/ dirs do not exist for this ticket). Reported only what the Findings/Solution/Questions-Answered sections state. Grepped knowledge/data_catalog.md, data_knowledge.md, mntn_business.md, experimentation.md for the summary's named facts.

**Tables:** tpa.membership_updates_logs

**Learned:**
- TI-253 mapped IP addresses to domain verticals via the TPA pipeline; domains missing from vertical mappings represent targeting gaps.
- The summary's two named facts (tpa.membership_updates_logs and the IP->vertical->TPA data flow) are already documented in data_catalog.md and data_knowledge.md, so no delta facts.

**Reuse when:**
- Investigating TPA missing-domain gaps or IP-to-vertical associations
- Reviving or re-running the ti_253 monitor script for future TPA vertical-coverage gaps


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
