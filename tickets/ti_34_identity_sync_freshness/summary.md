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
