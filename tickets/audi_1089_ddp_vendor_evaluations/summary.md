# AUDI-1089: [SPIKE] DDP Vendor Data Evaluations — Renewal Pass/Play per Vendor

**Jira:** https://mntn.atlassian.net/browse/AUDI-1089
**Status:** In Progress — Klickly (DS39) first, due **2026-07-13** (renewal live)
**Date Started:** 2026-07-09
**Assignee:** Malachi
**Supersedes:** AUDI-1051 (closed with pointer). 5x5 (DS25) already evaluated in TI-1027 → KEEP.

---

## 1. Introduction

Contract renewal season. Paulo (Slack 2026-07-09) asked which MM site-visit data vendors to keep; Kale:
"do what we did for 5x5 [TI-1027] for Klickly"; Paulo: **Klickly pass/play by Monday 2026-07-13** — their
renewal is live. Renewal schedule for the rest incoming from Paulo. Per Bryce (PMO): ONE spike ticket,
per-vendor outcomes marked off in the ticket description as completed. Workspace: one subfolder per vendor.

## 2. The Problem

Per vendor: is the data worth its cost? Deliver a pass/play verdict + implied max defensible fee band,
convincing from data alone, using the TI-1027 playbook generalized to the IP grain at the 30-day window.

## 3. Plan (per vendor — the playbook)

1. **Liveness + cost structure** — registry (CDC-dedup), GCS delivery, lineage blast radius (non-MM consumers).
2. **Scale + freshness** — per-day rows/IPs/domains/pairs (30d); recency sole-or-freshest per (ip,domain).
3. **Uniqueness (30d)** — IP/domain/pair sole + net-new-vs-free (DS23/30) + classified (wcv).
4. **Quality** — delivered score-tier mix; served-rate sole-vs-shared (junk check); IPv6 share.
5. **Value anchor (media/data-cost lens ONLY — take rates are sensitive/private, per ray in #data)** —
   impressions + media_spend + data_spend to vendor-touched / vendor-sole IPs, tiered:
   T1 = HHST-gated + scored (HS≥6666, not AHS, non-RTC) to sole IPs (floor: "could not have served without");
   T2 = all imps to sole IPs; T3 = all touched (ceiling, transparency only).
   Check A: share r of delivered scored IPs with zero svs signal → report T1×(1−r).
6. **Verdict** — fee band: floor = T1 × data-CPM lens, ceiling = T2 × media lens, peer anchor $0.50 CPM;
   PASS (drop) / PLAY (keep) / renegotiate once actual fee arrives.

**Canonical windows:** signal svs `dt 2026-06-02 → 2026-07-01` (30d targeting lookback) · valuation week CIL
`2026-07-02 → 2026-07-08` (strictly after — temporal ordering) · soleness on the 37d union.
**Union scans compute all 10 DS at once** → cross-vendor outputs land in ticket-root `outputs/`; each vendor
folder holds its interpretation, vendor-specific queries, and verdict.

## 4. Vendor checklist (mirrors Jira description — mark off as completed)

| Vendor | DS | Billing | Prior (TI-1027) | Status | Verdict |
|---|---|---|---|---|---|
| Klickly | 39 | flat_fee | 132 unique classified domains (7d), score 36, REVIEW | **IN PROGRESS — due 07-13** | — |
| Justuno | 24 | $0.50 CPM | 4,823 unique classified, 84% unique, KEEP-efficient | pending | — |
| Predactiv | 26 | flat_fee | #1 unique (164,627), KEEP; rich metadata dropped; broken registry SCD | pending | — |
| 33Across | 28 | $0.50 CPM | 9,277 unique (30%), REVIEW; ~38.6% redundant vs augmentor (AUDI-647) | pending | — |
| Sovrn | 33 | $0.50 CPM | 293 unique (1.6%), DROP-CANDIDATE | pending | — |
| Cybba | 36 | $0.50 CPM | 309 unique (5.7%), REVIEW | pending | — |
| 33Across API | 40 | $0.50 CPM | 2,802 unique (3.2%), DROP-CANDIDATE; ~13.5% match (AUDI-647) | pending | — |

## 5. Constraints & context (from the Slack thread, 2026-07-09)

- **Take rates sensitive/private (ray):** shareable artifacts use base cost only — media_spend
  (advertiser-agnostic, inventory/deal-based) + data_spend. No platform_spend / billed / take-rate math
  in anything shared.
- **Performance over cost as the value metric (ray, 2nd reply — adopted):** "we get paid no matter what…
  the value is whether the end-to-end generated value to the customer, so that they keep paying us —
  lean towards the performance metrics, not the raw/net costs." → the per-vendor case LEADS with
  performance (VR of impressions to vendor-sole IPs vs same-score-band multi-source IPs); the media/data
  cost lenses remain as the willingness-to-pay anchor ("how much would we pay for these IPs"), not the
  headline. Quality ≈ does the vendor's unique signal find IPs that perform.
- **Paulo:** believes payment is "waterfall style on usage basis" — registry says Klickly `flat_fee`;
  reconcile against the renewal schedule when it lands. He'll act on our recs, including keep-alls.
  Redundancy may have validity value (his point) → priced via the recency tied-share (coverage-if-down).
- **Ryan Kleck (via Matt Brorby):** DDP-vs-augmentor_log redundancy — 33Across ~38.6% IP+URL match,
  33Across API ~13.5% (corroboration for those two verdicts; AUDI-647).
- **ID-164** (Identity: toxic-hub IP scoring, PR open, Jack Barbey/elena-tpm): overlaps the per-IP value idea —
  follow-up connection after the renewal wave, not in scope here.

## 6. Investigation & Findings

*(per-vendor findings live in each vendor subfolder's summary.md; cross-vendor results summarized here)*

## 7. Data Documentation Updates

*(running list)*

## 8. Open Items

- Renewal schedule + per-vendor fees ← Paulo (verdicts are bands until then).
- Klickly "waterfall usage" vs registry `flat_fee` — reconcile.
- Does scoring's event ingest exclude DS23 like the classification consumer? ← Sean/Ryan (run soleness both ways if unanswered).
- 5x5 contract: data still flowing past end-of-June contract end with no recorded renewal — confirm signed (flagged in TI-1027).
