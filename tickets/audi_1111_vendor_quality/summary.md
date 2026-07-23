---
doc_type: epic
title: "AUDI-1111: Vendor Data Quality & Valuation"
status: in_progress
date: 2026-07-17
summary: "Epic: set a 3P data-quality bar and true willingness-to-pay price per vendor"
result: "No metered vendor breaks even at $0.50; RTC vendor-independent; preemption $274K/yr"
keywords: [audi-1111, vendor quality, willingness to pay, wtp cpm, rtc, ds14, svs, free log preemption]
---

## TL;DR

**Q:** For AUDI-1111 vendor data-quality & valuation, what is the true willingness-to-pay price per 3P vendor and do the platform's real-time layers (RTC, DS14 gate) actually need the vendors?

**A:** No metered vendor breaks even at the $0.50 contract on any WTP lens (L0 break-even e.g. 33Across $0.086-0.257, closest is 33A API $0.127-0.381; effective cost 1.3x-7.4x over ceiling). Free-union coverage under the flow-filter rule is 44.1% (vs 59.4% same-day). RTC is effectively vendor-independent: 99.99% of RTC-fired imps are free-covered, vendor-only 0.01%, because vendor logs arrive 2.4-8.6h stale vs free logs at 0 min. The DS14 gate is real but soft: only 36.1% of 301.5M svs IPs are in-gate; recommendation drafted to widen free-log windows before paying vendors for reach. Cross-cutting: AUDI-1113 free-log credit preemption is worth $273,671/yr and needs no vendor cooperation (billing is self-reported).

**How:** Three measured, adversarially-verified analyses: (1) AUDI-1115 WTP as 3 CPM lenses per vendor (all-ingested / flow-filtered actually-used / bid-and-won) giving effective CPM and break-even ceiling; (2) AUDI-1116 RTC vendor-share plus an svs ULID ingest-latency instrument comparing free-log vs vendor arrival times; (3) AUDI-1117 DS14 availability-gate vs site_visit_signal overlap pool math on 301.5M svs IPs.

**Tables:** `guid_log`, `aug_log`, `IPDSC`, `svs`, `MembershipDB`

**Learned:**
- No metered vendor breaks even at $0.50 on any lens; free-union flow-filter coverage 44.1% vs 59.4% same-day
- RTC effectively vendor-independent: 99.99% of RTC-fired imps free-covered, vendor-only 0.01%; vendors arrive 2.4-8.6h stale vs free logs at 0 min
- DS14 gate soft: only 36.1% of 301.5M svs IPs in-gate; widen free-log windows before paying vendors for reach
- AUDI-1113 free-log credit preemption worth $273,671/yr, needs no vendor cooperation because billing is self-reported

**Reuse when:**
- valuing a 3P data vendor / willingness-to-pay CPM per vendor
- assessing whether RTC or DS14 real-time layers depend on vendor logs
- free-log credit preemption / self-reported billing questions

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

- **Scope directive: research for a proposal** — implementation is a separate workstream;
  epic description must say so (it does).
- **Routing:** consolidated proposal → pre-read for Alyson → Mike + Kale (department leads) →
  then push for discussion with Paulo.
- Flow filter for free-log coverage credit: applies to **both** free logs (no same-day credit —
  the bid stream is circular; lagged 30d window). (Malachi self-flagged this on the call:
  "make sure there's a trailing day for the last 30 days" — AUDI-1115 L2 implements it.)
- Outreach = one ticket, 5-vendor checklist (Bryce one-spike convention).
- Non-Malachi tickets created unassigned; owners settled at grooming.
- WTP = 3 CPM numbers per vendor: all-ingested / actually-used (flow-filtered unique) /
  bid-and-won. Effective CPM today AND WTP ceiling per lens. Sean's framing: renegotiate the
  rate (e.g. $0.50 → $0.20 CPM) rather than drop — vendors have value, just not at current price.
- Meeting transcript: `../audi_1089_ddp_vendor_evaluations/meetings/audi_1089_02_stakeholder_readout_2026_07_16.txt`

## 3b. Facts + concerns raised on the call (beyond the to-do list)

- **RTC = two pipelines** (→ AUDI-1116, also in data_knowledge §RTC): guid_log Kafka streaming
  (~real-time, Zach's) + TI-run HOURLY batch over svs-minus-guid — vendors DO drive RTC.
  Per-day analysis understates vendor timing effects (Sean).
- **DS14 filter lives at MembershipDB / audience-service level** as a global filter (Sean);
  "add other svs IPs to DS14" liked by Allison + Sean (→ AUDI-1117 option sizing).
- **Billing is self-reported**: we run targeted_signal compute (Sherwin's team) and tell
  vendors what we owe — no audit; preemption needs no vendor cooperation. Credit rule
  ambiguity (fractional vs Victor's "first reporter in date partition wins") — **meeting
  Monday 2026-07-20**.
- **Selection-bias / feedback-loop concern (Matt):** keyword/vertical sources may align with
  free logs → we score free-covered IPs higher → coverage numbers partly self-fulfilling
  ("house of cards" risk). Ryan's proposed test: remove 33Across from IPDSC input (DS13/DS19
  prep), wait ~30d, measure. **Candidate follow-up ticket — not yet created.**
- **Companion report ask:** how many campaigns/audiences actually use the keyword categories
  that shrink under free-only (e.g. 85M-IP category cut 78%)? Audience-size UI shrink WILL
  generate advertiser complaints (Allison has fielded these before). **Candidate follow-up —
  not yet created.**
- **LiveRamp is a separate pipeline** (separate bucket + ingestion; aggregates 33Across,
  Dstillery, etc. for interest segments) — svs vendor drops do NOT touch interest segments.
  Sean floated a LiveRamp-wide cost/value analysis as a future item; also the idea of asking
  LiveRamp/DeepSync to supply site-visit-type data instead of the small svs vendors.
- **ID-graph IP-quality idea (Allison):** use the identity graph to flag busy/unusable IPs
  arriving from vendors (ties to Identity ID-164 toxic-hub scoring — extend, don't rebuild).
- **Context:** vendor expansion era was Richard + Phil ("integrate 100 data providers");
  Proxima-style shopping/conversion data is the kind of 3P worth paying for (Ryan/Matt).
- **Vendor-ops list:** Alyson + Alex maintain a vendor list; check prepaid contracts before
  proposing drops.

## 4. Cross-cutting facts

- DS14 = "MNTN Global Data" availability gate, auto-added to every audience expression;
  restricts to IPs recently in guid_log/aug_log; computed at bid time, not in IPDSC.
  Docs disagree on windows (guid ~4d + aug ~1d vs ~7d aug) — AUDI-1117 resolves.
- RTC = realtime_conquest_score=10000, recent site visitors, hourly batch, first check in
  bidder waterfall; independent pipeline from MM batch scoring. AUDI-1116 traces its feed.

## 4b. Findings snapshot (all three analyses measured + adversarially verified, 2026-07-16/17)

**The proposal's one-line spine: the price, not the data, is the problem — and the platform's
real-time layers don't need the vendors at all.**

1. **AUDI-1115 (WTP):** no metered vendor breaks even at the $0.50 contract on ANY lens.
   L0 break-even (renegotiation number, vendor's own meter): 33Across $0.086–0.257,
   33A API $0.127–0.381 (closest), Sovrn $0.048–0.145, Justuno $0.024–0.072, Cybba
   $0.023–0.068. L2 (meeting's flow-filter rule) confirms: effective cost 1.3×–7.4× over
   ceiling. Free-union coverage under the flow rule: 44.1% (vs 59.4% same-day). Anchors
   exact vs deck_d1. Flat-fee bills pending Maya.
2. **AUDI-1116 (RTC):** effectively vendor-independent — 99.99% of RTC-fired imps
   free-covered; vendor-only 0.01%. Root cause measured with the new svs ULID latency
   instrument: free logs stream at 0 min; vendors arrive 2.4–8.6h stale (Predactiv to
   ~12h), matching the pipeline's CONFIGURED per-DS lag hours. Freshness SLA = second
   renegotiation lever.
3. **AUDI-1117 (DS14):** the gate is real but soft on CTV (87.8% upper bound, 4.3% outside
   both logs; display is a same-day-aug echo, not evidence). Pool math: only 36.1% of
   301.5M svs IPs are in-gate; expansion splits 97.0M free-stale (widen free windows —
   costs nothing) vs 95.7M vendor-only (and 50% of 33Across's IPs arrive non-biddable).
   Recommendation drafted: widen free-log windows before paying vendors for reach.

Cross-cutting: AUDI-1113 preemption ($273,671/yr) needs no vendor cooperation — billing is
self-reported (we run the meter). Full detail + caveats in each child's summary.md.

## 4c. Where everything lives (deliverables map)

| What | Where |
|---|---|
| **Consolidated workbook (ALL new tables, one file)** | `outputs/audi_1111_findings.xlsx` — 7 sheets: wtp_L0_renegotiation, L2_flow_coverage, rtc_vendor_share, ingest_latency, ds14_gate_by_cohort, ds14_overlap_sizing, queries (maps every sheet+chart to its query) |
| Full 4-lens WTP table (canonical) | `audi_1115_wtp_cpm/outputs/audi_1115_wtp_cpm.xlsx` (24 pending cells = Maya flat fees) |
| Charts (4) | `audi_1115_wtp_cpm/artifacts/audi_1115_wtp_vs_contract.png` + `audi_1115_flow_coverage_drop.png`; `audi_1116_rtc_free_logs/artifacts/audi_1116_ingest_latency.png`; `audi_1117_ds14_svs_overlap/artifacts/audi_1117_ds14_pool.png` (each with a `generate_charts.py` beside it) |
| Raw measured CSVs | each child's `outputs/` (L2 merged + 4 shards; hourly arrival; rtc share; 3 DS14 CSVs) |
| Queries (copy-paste runnable, headers with claims/windows/run blocks) | each child's `queries/` (7 SQL files) |
| Narratives + caveats (verified) | each child's `summary.md` §4; this file §4b for the snapshot |
| Jira | epic AUDI-1111 (comments 596088 + desc); results comments AUDI-1115 #596162, AUDI-1116 #596106, AUDI-1117 #596107 |
| Durable knowledge | `knowledge/data_knowledge.md`: §RTC (two pipelines + measured 0.01%), §svs (ULID latency instrument), §DS14 (MembershipDB + AUDI-1117 pointers), billing (self-reported meter) |

## 5. Open Items

- [ ] AUDI-1113/1114 owner assignment at grooming
- [ ] Monday 2026-07-20 meeting: billing credit-assignment rule (fractional vs first-reporter)
- [ ] Proposal routing: pre-read Alyson → Mike + Kale → Paulo (after analyses land)
- [ ] Candidate tickets to raise with Malachi: selection-bias/IPDSC-removal test;
      keyword-category campaign-usage companion report
- [ ] Flat-fee amounts from Maya (carried from AUDI-1089)
- [ ] Data Eng ingestion costs (Sean Yang's team, carried from AUDI-1089)
