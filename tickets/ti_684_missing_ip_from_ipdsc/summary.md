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
- TI-644 established `ipdsc__v1` schema and query patterns — see TI-644 `artifacts/complete_context.md`
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
