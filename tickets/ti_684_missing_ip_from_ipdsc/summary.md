---
doc_type: ticket
title: "TI-684: Missing IPs from IPDSC"
status: in_progress
date: 2026-03-04
summary: "Trace why some expected IPs are absent from the IPDSC targeting pipeline"
result: "in progress — added ipdsc__v1 catalog entry; root cause not yet determined"
keywords: [ipdsc, ipdsc__v1, missing ips, ip data source category, ti-684, hem resolution, ip ttl, geo exclusion]
---

## TL;DR

**Q:** Why are some expected IPs missing from the IPDSC (IP Data Source Category) targeting pipeline?

**A:** In progress — root cause not yet determined. The ticket investigates IPs expected to be in IPDSC (dw-main-bronze.external.ipdsc__v1) but absent, which would mean audience members are not reached. Candidate causes listed (not yet confirmed): pipeline failures (HEM → IP resolution not completing), IP TTL expiration, geo exclusions filtering IPs out, or incorrect partition-date queries. No findings, solution, or root cause concluded yet. Only documentation output so far: a full data_catalog.md entry for ipdsc__v1 (added 2026-03-03, from TI-644 work). Prior related work: TI-644 (ipdsc__v1 schema/query patterns) and TI-34 (identity sync freshness — stale syncs could cause missing IPs).

**How:** Ticket summary only; investigation ongoing. Plan (not executed to conclusion): compare expected vs actual IPDSC contents, trace IPs through pipeline stages, determine root cause vs expected data lifecycle, recommend fix. Outputs folder holds two JSON exports (ti_684_export.json, ti_684_export_2.json); no findings written into the summary.

**Tables:** dw-main-bronze.external.ipdsc__v1

**Learned:**
- IPDSC = IP Data Source Category, maps IPs to audience segments for targeting; missing IPs mean audience members not reached
- TI-684 root cause not yet determined; candidate causes are pipeline failures (HEM->IP resolution), IP TTL expiration, geo exclusions, or incorrect partition-date queries
- TI-644 established ipdsc__v1 schema and query patterns; TI-34 investigated identity sync freshness as a possible missing-IP cause

**Reuse when:**
- investigating missing IPs from IPDSC
- tracing IP dropout through the targeting pipeline
- ipdsc__v1 data lifecycle / TTL / freshness questions

# TI-684: Missing IPs from IPDSC

**Jira:** https://mntn.atlassian.net/browse/TI-684
**Status:** In Progress
**Date Started:** ~2026-03 (estimate)
**Date Completed:** TBD
**Assignee:** Malachi

---

## 1. Introduction

Investigation into IPs that are missing from the IPDSC (IP Data Source Category) pipeline. IPDSC maps IPs to audience segments for targeting. Missing IPs mean potential audience members are not being reached.

---

## 2. The Problem

Some IPs expected to be in IPDSC (`dw-main-bronze.external.ipdsc__v1`) are absent. This could result from:
- Pipeline failures (HEM → IP resolution not completing)
- IP TTL expiration
- Geo exclusions filtering IPs out
- Incorrect partition date queries

---

## 3. Plan of Action

1. Identify which IPs are missing (compare expected vs. actual IPDSC contents)
2. Trace IPs through pipeline stages to find where they drop out
3. Determine root cause (pipeline failure vs. expected data lifecycle)
4. Recommend fix

---

## 4. Investigation & Findings

Work in progress. See `queries/`, `outputs/`, and `artifacts/` for investigation files.

Relevant prior work:
- TI-644 established `ipdsc__v1` schema and query patterns — see TI-644 `artifacts/ti_644_complete_context.md`
- TI-34 investigated identity sync freshness — stale syncs could cause missing IPs

---

## 5. Solution

TBD.

---

## 6. Questions Answered

TBD.

---

## 7. Data Documentation Updates

- `knowledge/data_catalog.md` now has full entry for `dw-main-bronze.external.ipdsc__v1` (added 2026-03-03)
  including GCS path, unnest pattern, and key facts from TI-644 investigation.
- Update this section with freshness/TTL findings when investigation completes.

---

## 8. Open Items / Follow-ups

- Investigation ongoing.

---

## Drive Files

- (No Drive folder found for TI-684)
