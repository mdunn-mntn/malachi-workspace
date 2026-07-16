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

### 4a. Gate-lag scan in flight (launched 2026-07-16 ~17:00)

Expected read: if gate = aug(1d) OR guid(4d), (nearly) all served IPs show aug_lag ≤ 1 OR
guid_lag ≤ 4 (±1 day partition fuzz). Mass at aug_lag 2–7 with guid_lag > 4 would support
the ~7d augmentor reading instead.

## 5. Solution

*(pending)*

## 6. Questions Answered

*(pending)*

## 7. Data Documentation Updates

*(pending — the DS14 window line correction lands here)*

## 8. Open Items / Follow-ups

- [ ] Step-2 overlap/option-sizing query after windows resolve
