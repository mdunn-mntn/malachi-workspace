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

### 4b. Q2 cohort split in flight (launched 2026-07-16 ~17:25)

Same histogram per channel (ctv/display) × funnel (prospecting/retargeting) via
public_campaigns (funnel_level authoritative). If the gate is real, CTV-prospecting should
show a hard edge and the bypass cohorts should absorb the Q1 residual.

## 5. Solution

*(pending)*

## 6. Questions Answered

*(pending)*

## 7. Data Documentation Updates

*(pending — the DS14 window line correction lands here)*

## 8. Open Items / Follow-ups

- [ ] Step-2 overlap/option-sizing query after windows resolve
