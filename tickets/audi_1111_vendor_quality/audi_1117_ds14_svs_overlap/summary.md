---
doc_type: ticket
title: "AUDI-1117: DS14 Availability Gate vs site_visit_signal Overlap"
status: in_progress
date: 2026-07-17
summary: "How much vendor signal is biddable under DS14 gate; size the add-svs-to-DS14 pool option"
result: "In progress — expansion splits ~half free-stale (97M) vs vendor-only (95.7M IPs)"
---

# AUDI-1117: DS14 availability gate vs site_visit_signal overlap

**Jira:** https://mntn.atlassian.net/browse/AUDI-1117
**Status:** In Progress
**Date Started:** 2026-07-16
**Assignee:** Malachi

---

## 1. Introduction

DS14 ("MNTN Global Data") is auto-added to every audience expression and restricts bidding
to IPs recently seen in guid_log/augmentor_log — a global filter at MembershipDB /
audience-service level (Sean, 2026-07-16 readout). It explains why ~99% of biddable IPs come
from the free logs (Allison's question on the call). Parent epic: AUDI-1111.

## 2. The Problem

1. Internal docs disagree on the gate windows: guid ~4d + aug ~1d (DS taxonomy entry) vs
   "~7-day augmentor-log activity filter" (audience-expression decode). Which is real?
2. How much of each vendor's delivered signal is even biddable (inside the gate)?
3. Option floated on the call (liked by Allison + Sean): add other svs IPs to DS14 — how
   much would the pool grow, and how vendor-dependent is that growth?

## 3. Plan of Action

1. `audi_1117_ds14_gate_lag.sql` — for IPs served won imps on 2026-07-01, histogram
   (aug_lag, guid_lag) over an 11d svs lookback. Gate windows show up as hard edges.
2. After windows resolved: per-source share of svs IPs inside the gate (biddable) +
   pool-growth sizing for the add-svs-to-DS14 option. Query design depends on step 1.
3. Fix whichever data_knowledge line is wrong.

## 4. Investigation & Findings

### 4a. Q1 all-impressions lag histogram (MEASURED 2026-07-16 — `audi_1117_ds14_gate_lag.csv`)

Served IPs on 2026-07-01: 9,589,296 (54.6M imps). **No hard gate edge over ALL impressions:**

| Hypothesis | % served IPs | % imps |
|---|---|---|
| aug same-day | 68.6% | 82.9% |
| aug ≤ 1d | 77.0% | 87.2% |
| gate = aug(1d) OR guid(4d) | **85.5%** | **92.9%** |
| gate = aug(7d) OR guid(4d) | 92.7% | 96.4% |
| NEITHER free log in 11d | **5.1%** | 2.5% |

Lag distributions decay smoothly (no cliff at 1d/4d/7d). So DS14 is NOT a hard universal
filter at the documented windows across all delivery — either some paths bypass it
(retargeting = own visitor lists; display = cookie-based) or exemptions exist.

### 4b. Q2 cohort split (MEASURED 2026-07-16 — `audi_1117_ds14_gate_lag_by_cohort.csv`)

Funnel mapping note (corrected by verify-pass): funnel_level 4 (retargeting) EXISTS but
delivered only 603 imps / 148 served IPs on 2026-07-01 (98.5% in-gate) — far too small to
test any bypass hypothesis, which is why it fell below the table's 100K-imp floor. The
material funnels are 1/2/3 (prospecting / stage-2 / stage-3).

| Cohort | Imps (07-01) | aug≤1 OR guid≤4 | neither log 11d |
|---|---|---|---|
| display (all funnels) | 20.2M | **100.00%** | 0.00% |
| ctv / stage-3 | 1.2M | 98.4% | 0.25% |
| ctv / stage-2 | 4.6M | 91.7% | 2.5% |
| ctv / prospecting | 28.6M | **87.8%** | **4.3%** |

**Display is a same-day echo, not gate evidence:** 100.00% of display imps have a SAME-DAY
augmentor row — aug_log mirrors the display bid stream by construction ("we can only bid on
what's in the augmentor_log" is literally true for display). The DS14 gate question is a CTV
question, and there the edge is SOFT: 12.2% of CTV-prospecting imps land outside
aug(1d)|guid(4d), 4.3% outside both logs entirely (11d).

Candidate mechanisms for the CTV soft edge (unresolved): household-graph expansion (gate
satisfied by a graph-sibling IP, serving IP differs), bid-time vs partition-day fuzz
(bounded small: aug≤2|guid≤4 only adds ~1.9pp), CTV IP churn between qualification and
serve. Needs MemDB/audience-service inspection or Zach/Sean to adjudicate. The "~7d
augmentor window" reading is NOT supported as a hard bound in any cohort.

Direction-of-bias note (verify-pass): same-day free-log rows can POSTDATE the impression
(ad served, visit later, still lag 0), so the 87.8% CTV-prospecting figure is an UPPER
bound on true at-bid-time gate coverage — which strengthens the soft-edge finding.

### 4c. Q3 overlap + pool-expansion sizing (MEASURED — `audi_1117_ds14_overlap_sizing.csv`, 30d, gate ref 2026-07-01)

- **svs 30d universe: 301.5M IPv4 IPs; only 108.8M (36.1%) are in-gate** (biddable under the
  documented aug-1d|guid-4d proxy) — the "what's in svs that's not in DS14" answer: 192.7M
  IPs (63.9%).
- **The expansion splits almost exactly in half:**
  - `expansion_free_stale` = **97.0M** — out-of-gate IPs the FREE logs delivered within 30d.
    This growth needs NO vendors — just widen the free-log gate windows.
  - `expansion_vendor_only` = **95.7M** — only vendors delivered them in 30d; growth that
    actually requires paying vendors.
- **Per-source biddable share of delivered IPs** (in-gate %): Cybba 81.6, Klickly 78.5,
  Sovrn 72.7, augmentor 70.2, Predactiv 66.8, Justuno 60.0, 33A API 59.6, guid 54.8,
  5x5 52.2, **33Across 50.0** — half of what our biggest vendor sends is not even biddable
  under today's gate (the meeting's "vendors don't seem very fresh" hypothesis, quantified;
  also the answer to "what are the other ~40/50%?" — IPs absent from the recent auction
  stream).
- Caveats: snapshot at one reference day (gate membership churns daily); stock-vs-flow —
  the 30d IP stock naturally exceeds any recency-gated pool; IPv4 only.
- Invariant check (verify-pass): expansion_free_stale + expansion_vendor_only =
  97,010,109 + 95,654,923 = 192,665,032 = expansion_all_out_of_gate EXACTLY — no
  unlisted-data_source_id leakage through the maskless CASE.

**Chart:** `artifacts/audi_1117_ds14_pool.png` (per-source biddable share + expansion split;
regenerate via `artifacts/audi_1117_generate_charts.py`). Also in
`../outputs/audi_1111_findings.xlsx`.

## 5. Solution

*(analysis complete pending verification pass; recommendation draft below)*

**If audience size is the concern, widen the free-log gate windows before paying vendors:**
half the possible pool growth (97.0M of 192.7M IPs) is already in the free logs, just
outside the current 1d/4d windows. The vendor-only half (95.7M) is dominated by IPs that are
demonstrably stale (2.4–8.6h delivery lag, AUDI-1116) and 50% non-biddable at arrival
(33Across). Pool expansion via vendors buys mostly low-liveness inventory.

## 6. Questions Answered

*(pending)*

## 7. Data Documentation Updates

*(pending — the DS14 window line correction lands here)*

## 8. Open Items / Follow-ups

- [ ] Step-2 overlap/option-sizing query after windows resolve
