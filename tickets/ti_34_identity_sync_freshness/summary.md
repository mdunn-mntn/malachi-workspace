---
doc_type: ticket
title: "TI-34: Identity Sync Freshness — IP Blocklist Freshness Measures"
status: done
date: 2026-03-03
summary: "Freshness monitoring for the IP identity graph and blocklist sync pipeline"
result: "Delivered freshness SQL monitoring max(update_time) on tpa.membership_updates_logs"
keywords: [identity sync freshness, ip blocklist, tpa.membership_updates_logs, update_time, staleness threshold, identity graph, ti-34, membership sync]
---

## TL;DR

**Q:** TI-34 Identity Sync Freshness: how was IP identity graph / blocklist sync staleness detected, and what freshness signal was delivered?

**A:** TI-34 (done, 2026-03-03) built freshness monitoring for the IP identity graph and blocklist sync pipeline, addressing the absence of any mechanism to detect a lagging sync (stale exclusion lists keep targeting fresh converters; stale inclusion lists miss new members). Delivered: freshness monitoring SQL that watches max(update_time) on the Greenplum table tpa.membership_updates_logs versus the expected sync cadence (sync should run daily). update_time is the key recency column; datastream_metadata.source_timestamp must not be used as a proxy for it. Caveat: the SQL file ti_34_identity_sync_freshness.sql actually contains NTB email-prevalence analysis (conversion_log.email / email_data, threshold 0.5), a likely mismatch with the file's stated purpose — the canonical freshness findings live in the Drive gdoc/gsheet. Related follow-up: TI-684 (missing IPs from IPDSC), since freshness issues can cause missing IPs.

**How:** Investigated Greenplum timestamp columns for sync-recency signals, landed on max(update_time) on tpa.membership_updates_logs compared to expected daily cadence as the staleness detector. Freshness monitoring SQL and staleness thresholds were delivered; canonical findings documented in Drive.

**Tables:** tpa.membership_updates_logs, conversion_log, ti_34_identity_sync_freshness.sql

**Learned:**
- Stale IP identity-graph sync is detected by monitoring max(update_time) on Greenplum tpa.membership_updates_logs against the expected (daily) sync cadence
- update_time is the freshness column on tpa.membership_updates_logs; datastream_metadata.source_timestamp is NOT a valid proxy for it
- The file ti_34_identity_sync_freshness.sql actually holds NTB email-prevalence analysis, likely a mismatch; canonical freshness findings are in the Drive documents

**Reuse when:**
- Building or auditing freshness/staleness monitoring for an IP identity graph or blocklist sync pipeline
- Detecting a lagging Greenplum tpa membership sync
- Investigating missing IPs (e.g. TI-684 / IPDSC) that could stem from stale sync

---


# TI-34: Identity Sync Freshness — IP Blocklist Freshness Measures

**Jira:** https://mntn.atlassian.net/browse/TI-34
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Established freshness monitoring for the IP identity graph and blocklist pipeline. The identity graph maps IPs to audiences — stale data means IPs are being targeted or excluded based on outdated membership. This ticket built measures to detect staleness.

---

## 2. The Problem

No existing mechanism to detect when the IP identity graph or blocklist sync had fallen behind. A delayed sync could mean:
- Fresh converters still being targeted (exclusion list stale)
- New audience members not yet enrolled (inclusion list stale)

---

## 3. Plan of Action

1. Identify timestamp columns that reflect sync recency
2. Build freshness monitoring query
3. Define staleness thresholds
4. Validate against known sync schedules

---

## 4. Investigation & Findings

**Query:** `queries/ti_34_identity_sync_freshness.sql`

Key tables investigated for freshness signals (Greenplum):
- `tpa.membership_updates_logs` — IP audience membership timestamps
- Identity graph sync timestamps

**Content note:** The SQL file `ti_34_identity_sync_freshness.sql` actually contains NTB (New-to-Brand)
email prevalence analysis (querying `conversion_log.email` and `email_data` columns, threshold 0.5
for qualifying advertisers). This may be a mismatch between the file's original purpose and final
content. The Drive documents are the canonical freshness findings.

**Note (Drive):** Drive has two additional documents:
- `ID-34 Establish Freshness Measure for IP Blocklist.gdoc` — written findings
- `ID-34 Establish Freshness Measure for IP Blocklist.gsheet` — data
- `Quality and Identity Graph.gdoc` — broader identity graph quality doc

---

## 5. Solution

Delivered freshness monitoring SQL and documented staleness thresholds.

---

## 6. Questions Answered

- **Q:** How do we detect a stale IP identity graph sync?
  **A:** Monitor `max(update_time)` on `tpa.membership_updates_logs` vs. expected sync cadence.

---

## 7. Data Documentation Updates

- `tpa.membership_updates_logs`: key freshness column is `update_time`; sync should run daily.
- `datastream_metadata.source_timestamp ≠ update_time` — use `update_time` for recency checks (confirmed in data_knowledge.md).

---

## 8. Open Items / Follow-ups

- TI-684 (missing IPs from IPDSC) is related — freshness issues can cause missing IPs.

---

## Drive Files

📁 `Tickets/ID-34 Establish Freshness Measures for IP Blocklists/`
- `ID-34 Establish Freshness Measure for IP Blocklist.gdoc` — written findings
- `ID-34 Establish Freshness Measure for IP Blocklist.gsheet` — data
- `Quality and Identity Graph.gdoc` — identity graph quality documentation
