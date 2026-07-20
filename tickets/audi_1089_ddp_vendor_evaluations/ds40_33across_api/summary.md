---
doc_type: ticket
title: "AUDI-1089: 33Across API (DS40) Renewal Pass/Play"
status: done
date: 2026-07-10
summary: "Renewal eval of 33Across API (DS40) metered 3P data vendor at $0.50/1k CPM"
result: "NEGOTIATE: cap/re-paper meter to <=$10-40K/yr else DROP; no lens breaks even at usage"
---

# AUDI-1089 / 33Across API (DS40) — Renewal Pass/Play

**Status:** DONE 2026-07-10 — verdict below (§6)
**Billing:** `fixed_cpm` **$0.50/1,000** per registry — usage-METERED, unlike the flat-fee vendors; monthly
usage-email pipeline confirmed in code (§1) but the metered basis + actual bill are NOT reconciled vs an invoice.
**Prior evidence (TI-1027, 7d):** 2,802 unique classified domains (**3.2% unique**) · scorecard 32 →
**DROP-CANDIDATE**; per-IP depth 1.87 unique pairs/IP (§4.16: "redundant for classification, useful for per-IP
features") · Ryan Kleck (AUDI-647): ~13.5% IP+URL match vs augmentor_log.

## Verdict frame

Metered vendor → the question is not "is the fee worth it" (fee scales with usage) but "what metered bill would
the data justify." Floor = T1 (gated+scored non-RTC imps to DS40-sole IPs) × data lens; ceiling = domain lens
(sole classified × TI-1027's $3–13/yr) + T2 sole-imps × $0.50 peer CPM. The one thing that could move the prior:
the 30d pair-grain picture (45.3% sole) is far better than the 7d domain-grain picture (3.2%) — §3 resolves
which grain the decision rides on.

## Supporting artifacts

- Evidence tables: `../outputs/audi_1089_*.csv` (all 10 DS; windows: svs 30d 2026-06-02→07-01, CIL valuation
  week 07-02→07-08, soleness on the 37d union; method validated via DS25 cross-check — recency 69.3% vs
  TI-1027's 69.8% ✓)
- Cross-vendor chart: `../artifacts/audi_1089_chart_sole_classified_domains.png` (DS40's 2,780 in context)
- Delivery liveness: verified for all 8 vendors 2026-07-08 (svs partitions present 07-07/08) — cited, not re-checked

## Findings

### 1. Liveness + cost structure + lineage blast radius — DONE 2026-07-10

**Registry** (`tpa.direct_data_partners`, CDC-deduped → 1 row): billing_type **fixed_cpm**, fixed_cpm **0.5**,
enabled, used_in_mntn_match=true, used_in_interests=false, type=mntn_matched, valid_from **2025-07-01**
(≈ July-1 contract anniversary, same as Klickly/others), valid_to null, **notes NULL — no fee terms in our data.**

**Metering pipeline EXISTS (new finding, fee-reality upgrade):** `SteelHouse/bae-sql-utility/ddp/
ddpmonthlyusageemail-33Across.py` sends a monthly usage report from partnerbilling@mountain.com to
33across.com contacts (allison.dewey / paul.bell / mike), CC accountspayable, covering **BOTH ds 28 and 40**
in one email: per-domain SUM(impressions) + SUM(usage $) from `dw-main-bronze.coredw.usage_reporting_data`
(schema has data_source_id, impressions, usage, tv_cpm, domains, reporting_month — cataloged in olympus).
→ billing is invoice-by-usage on an impressions meter (corroborates Paulo's "waterfall on usage basis").
**The metered basis (WHICH impressions count) and the monthly $ were not pulled here** (out of eval scope —
no invented cost); reconciling one month of `usage_reporting_data` vs the invoice is the decisive open item.

**Delivery — LIVE, streaming, and it DOMINATES the pixel topic.** DS40 is NOT a batch file-drop partner (not in
`ENABLED_DSIDS=[23,25,26,28,30,36]`); it arrives via the `pixel-page-view-signal` Kafka topic → hourly parquet
`gs://mntn-data-archive-prod/pixel_page_view_signal/` → `site_visit_signal/data_source_id=40`.
**89.5K of 110K rows in a sampled hour ≈ 81% of the whole pixel topic is DS40** (Klickly, by contrast, ~850 rows).
At ~363M rows/day, the Kafka/parquet/svs carrying + processing cost of DS40 is nonzero **beyond** the data fee;
dropping it shrinks the pixel-page-view pipeline ~5×. (Not dollarized here.)

**Lineage blast radius (org-wide sweep: `dsid40`, `"data_source_id = 40"`, `data_source_id=40`, `dsid_40`,
`33across_api`, `33across-api`, `33across`):**
- **MM site-visit path** — the only real consumer (svs → vertical classification → DS19/DS13 signal builds).
- **BUK training enrichment** — the known DSID-agnostic feature-store rollup (source_weight=0.05, 5% stratified
  sample); no hard DS40 dependency, degrades gracefully.
- **Passive:** billing/usage email above; audit/monitoring dashboards (would just show DS40 → 0).
- **DS40 backfill DAG exists but is DISABLED**: `gcp_pixel_page_view_signal_33across_dsid40_backfill_workflow.py`
  is listed in `airflow-ti/dags/.airflowignore` (alongside the Justuno/Klickly/Sovrn ones) — backfill-only, ignored.
- **Verified NON-consumers:** identity graph, attribution, bidder/serving, interests (zero dsid40 hits anywhere).
  The one 33Across non-MM consumer found — `airflow/dags/targeting/device_id_33across_signal.py` (device-ID/IFA
  extraction into `device_id_signal`) — is **DS28-only** (reads `fpa_dsid28_log.device_ids`, inserts
  data_source_id=28, schedule=None/manual). **DS40 does not feed it.**
- **DS28 ≠ DS40:** DS28 "33Across" = batch file drop (S3 `mntn-data-partner-33across`, spark
  `dsid28_33across_processing.py`, in ENABLED_DSIDS) with a device-ID side-channel; DS40 "33Across API" =
  pixel/streaming, page-views only. Same parent company + one billing email covering both → dropping DS40 does
  NOT end the relationship; DS28 is evaluated separately.
- **Conclusion: no hard dependency rescues DS40.** Off-switch is vendor-side / pixel-event-signal-service config,
  not a DAG change.

### 2. Scale + freshness (30d: 2026-06-02 → 07-01) — DONE
- Delivers **every day, no gaps**: **~363M rows/day** (10.9B rows/30d — the biggest streaming feed), ~34.6M
  IPs/day, ~110K domains/day, 26.0% of URLs carry paths. IPv6 0.01% (IPv4-only method immaterial).
- 30d window reach: **121.0M IPs · 356.6K domains · 1.23B (ip,domain) pairs** — top-3 on every scale axis
  (pairs behind only augmentor 2.79B and DS28 2.33B).
- Recency (pair grain): **45.3% sole**, 0.8% freshest, **39.2% tied**, 14.7% stale; **67.3% net-new vs the free
  internal sources** (DS23/30). Sole-or-freshest-or-tied 85.3%.

### 3. Uniqueness (30d) — DONE — **the 7d-vs-30d "discrepancy" resolved: it's a GRAIN split, not a window artifact**
- **Domain grain (what MM's classifier consumes): 30d confirms the 7d prior almost exactly.** 353,850 domains →
  **7,076 sole (2.0%)** → **2,780 sole+classified** (7d: 2,802 @ 3.2%). Longer window did NOT rehabilitate it.
  For scale: Predactiv 226.8K, 5x5 86.1K, augmentor 47.5K, DS28 6.8K sole classified — DS40 is ~1/31 of 5x5.
- **Pair grain: the 45.3% sole / 67.3% net-new-vs-free IS real** (543M sole pairs) — much higher than the 7d
  redundancy narrative (13.5% match vs augmentor) implied. Both are true: DS40 sees **different IPs on domains
  everyone already has** (TI-1027 §4.16 called this exact case: per-IP depth 1.87, domain-unique 3.2% —
  "redundant for classification, useful for per-IP features").
- **Which grain does the renewal ride on? Domain grain.** MM's value chain is domain→vertical classification →
  scores; a sole pair on an already-classified common domain adds no new classification, only marginal behavioral
  depth to a shared IP's score. That depth channel is (a) unpriced by this framework, (b) unmeasured (would need
  an ablation/holdout), and (c) its only empirical proxy fails: where DS40's signal is the ONLY input (sole IPs),
  scores essentially don't materialize (§4) and VR sits at/below the no-signal baseline (§5). Sole-IP metrics are
  the DEPENDENCY bound, not the whole story — but DS40's shared-IP story is depth-on-classified-domains, not
  domain coverage, so the classifier-grain number (2.0%) is the honest value read.

### 4. Quality (delivered score tiers, junk check) — DONE
- DS40-touched IPs look normal (127.0M touched → 20.7% delivered, 22.3% HI) — **pure co-occurrence**; every big
  vendor touches the same delivered households.
- **DS40-SOLE IPs (its unique contribution): 8.46M IPs → 19,120 delivered (0.2%), 97.6% unscored, 21 IPs at HI**
  — the worst unscored share of all 10 sources (Klickly 91.4%, 5x5 94.8%). Its unique reach is adversely
  selected: effectively unscorable despite 543M sole behavioral pairs landing on those IPs — the strongest
  single fact against the "pair depth = value" rescue.
- Check A (svs-necessity, global): 99.95% of scored delivered IPs have svs signal (r=0.05%) — "no site-visit
  signal → no score" holds; T1 needs no discount.

### 5. Value anchor (media/data-cost lens, tiered; CIL week 07-02 → 07-08) — DONE
| Tier | Imps/week | Media $/week | Meaning |
|---|---:|---:|---|
| T3 all touched (ceiling, co-occurrence) | 386.3M | $3.20M | meaningless for credit — shared IPs; shown for transparency |
| T2 all imps to sole IPs | **96,497** | **$1,126.62** | upper bound of real delivery dependency |
| T1 scored (≥6666) non-RTC to sole IPs | **523** | $5.59 | "could not have served without DS40" |
- **Performance (the headline, per ray):** sole-IP VR **0.0155%** (15 visits / 96,494 imps) — **at/below the
  no-svs unscored baseline ~0.02% and ~17× below guid_log-sole 0.26%**. The 10000-band sole cell: 30 imps,
  0 visits. DS40's unique signal does not find IPs that perform.
- Sole-imps peer-CPM lens: 96.5K/wk ≈ 5.0M/yr × $0.50 CPM ≈ **$2.5K/yr**. T1 lens: 523/wk ≈ **$14/yr**.
- Domain lens (generous): **2,780 sole classified × $3–13/yr ≈ $8.3–36.1K/yr** (TI-1027 per-unit anchor).
- BUK training upside: 0.05 weight × 5% sample — negligible per-vendor.

### 6. Verdict — **NEGOTIATE: cap/re-paper the meter to ≤ ~$10–40K/yr, else DROP**
- **Implied max defensible bill: ~$10–40K/yr** = domain lens ($8.3–36.1K) + sole-impression lens (~$2.5K).
  At $0.50/1,000 that is **~1.7–6.7M metered impressions/month**. Any invoice above that fails break-even.
- **Scale check on the meter:** if the metered basis resembles vendor-touched impressions (386M/wk), the
  CPM-equivalent is ~$10M/yr — 250×+ over the band. The basis is unknown (§1); **reconcile one month of
  `coredw.usage_reporting_data` (ds=40) against the actual invoice before renewal** — that single number decides
  between "cheap insurance" and "worst $/value in the portfolio."
- Why not KEEP: 2.0% sole domains (30d-confirmed, not a window artifact), sole IPs 97.6% unscored, sole VR at/
  below no-signal baseline, ~13.5% augmentor match (Ryan, AUDI-647) — no lens lands above ~$40K/yr.
- Why not DROP outright: unlike Klickly (~$0 band), DS40 has a real $10–40K band, a genuinely large net-new pair
  base (543M sole pairs, 67.3% net-new-vs-free) with unpriced-but-possible shared-IP depth value, and a tied
  share (39.2%) that carries Paulo's coverage-if-down validity value. If 33Across reprices into the band, the
  option is worth holding — with an ablation experiment to price the depth channel if we keep paying.
- **Hidden cost sweetens a drop:** DS40 is ~81% of the pixel-page-view topic (~363M rows/day) — Kafka/parquet/svs
  carrying cost is real and un-dollarized; a drop also shrinks that pipeline ~5×.
- No dependency blocks a drop (§1): MM path + negligible BUK enrichment only; the DS28 relationship (and its
  device-ID side-channel) is unaffected and judged separately.
- Caveats: metered basis + actual bill unconfirmed vs invoice (never invented here); shared-IP depth value
  unmeasured (sole-IP proxy confounded by adverse selection); 15 visits → wide VR uncertainty; one-week
  valuation window; soleness on the 37d union (temporal ordering held: signal precedes serve).
