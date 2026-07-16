# AUDI-1111: Vendor Data Quality & Valuation (EPIC)

**Jira:** https://mntn.atlassian.net/browse/AUDI-1111
**Status:** In Progress
**Date Started:** 2026-07-16
**Assignee:** Malachi

---

## 1. Introduction

Epic created out of the 2026-07-16 AUDI-1089 stakeholder readout. Decision: **don't drop all
vendors — set a data-quality bar and establish the true price we're willing to pay.** All
evidence base lives in `tickets/audi_1089_ddp_vendor_evaluations/` (Done, standalone).

## 2. Children

| Ticket | What | Owner | Folder |
|---|---|---|---|
| [AUDI-1093](https://mntn.atlassian.net/browse/AUDI-1093) | Free-log credit preemption — investigate + spec (In Progress; re-parented into this epic) | Malachi | (pre-epic, work in audi_1089 runbook) |
| [AUDI-1113](https://mntn.atlassian.net/browse/AUDI-1113) | Implement free-log credit preemption in billing ($273,671/yr stake) | TBD (Sean Yang's team?) | — |
| [AUDI-1114](https://mntn.atlassian.net/browse/AUDI-1114) | Vendor data-quality outreach — 5 asks (33Across webmail/bots, Sovrn malformed URLs, Justuno user_agent, 5x5 outbrain iframes, ShareThis/Predactiv adult) | TBD (Alyson?) | — |
| [AUDI-1115](https://mntn.atlassian.net/browse/AUDI-1115) | True willingness-to-pay CPM per vendor — 3 lenses | Malachi | `audi_1115_wtp_cpm/` |
| [AUDI-1116](https://mntn.atlassian.net/browse/AUDI-1116) | RTC × free logs — feed, timing, hourly-grain check | Malachi | (folder on start) |
| [AUDI-1117](https://mntn.atlassian.net/browse/AUDI-1117) | DS14 availability gate vs site_visit_signal overlap | Malachi | (folder on start) |

## 3. Meeting decisions driving this epic (2026-07-16 readout)

- Flow filter for free-log coverage credit: applies to **both** free logs (no same-day credit —
  the bid stream is circular; lagged 30d window).
- Outreach = one ticket, 5-vendor checklist (Bryce one-spike convention).
- Non-Malachi tickets created unassigned; owners settled at grooming.
- WTP = 3 CPM numbers per vendor: all-ingested / actually-used (flow-filtered unique) /
  bid-and-won. Effective CPM today AND WTP ceiling per lens.
- Meeting transcript: `../audi_1089_ddp_vendor_evaluations/meetings/audi_1089_02_stakeholder_readout_2026_07_16.txt`

## 4. Cross-cutting facts

- DS14 = "MNTN Global Data" availability gate, auto-added to every audience expression;
  restricts to IPs recently in guid_log/aug_log; computed at bid time, not in IPDSC.
  Docs disagree on windows (guid ~4d + aug ~1d vs ~7d aug) — AUDI-1117 resolves.
- RTC = realtime_conquest_score=10000, recent site visitors, hourly batch, first check in
  bidder waterfall; independent pipeline from MM batch scoring. AUDI-1116 traces its feed.

## 5. Open Items

- [ ] AUDI-1113/1114 owner assignment at grooming
- [ ] Flat-fee amounts from Maya (carried from AUDI-1089)
- [ ] Data Eng ingestion costs (Sean Yang's team, carried from AUDI-1089)
