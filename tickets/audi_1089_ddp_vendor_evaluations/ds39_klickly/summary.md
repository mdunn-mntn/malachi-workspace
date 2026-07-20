---
doc_type: ticket
title: "AUDI-1089: Klickly (DS39) DDP Renewal Evaluation"
status: done
date: 2026-07-10
summary: "Renewal pass/play evaluation of Klickly (DS39) Shopify product-view data vendor."
result: "Drop / do not renew unless ~free — value ~$0-4K/yr; any DDP fee fails break-even."
---

# AUDI-1089 / Klickly (DS39) — Renewal Pass/Play

**Status:** IN PROGRESS — verdict due **2026-07-13** (renewal is live; Paulo needs pass/play Monday)
**Billing:** `flat_fee` per registry (Paulo believes "waterfall usage-based" — reconcile with renewal schedule)
**Prior evidence (TI-1027, 7d windows):** 4.8M rows/day · 1.1M IPs/day · ~200 domains/day · 100% URLs with path ·
**132 unique classified domains** · 77.7% domain-unique (of a tiny base) · delivered IPs skew HI (52-57%) on
small volume · scorecard 35.8 → REVIEW ("negligible — too small to matter")

## Verdict plan

Fee band: floor = T1 (HHST-gated + scored, non-RTC, HS-not-AHS impressions to Klickly-sole IPs) × data-CPM
lens; ceiling = T2 (all impressions to sole IPs) × media lens; peer anchor $0.50 CPM. Expected shape from
priors: band near $0 — decision-ready even before the actual fee arrives. The only possible rescue: a non-MM
consumer of DS39 (lineage blast-radius check) or a 30d uniqueness picture materially better than the 7d one.

## Supporting artifacts

- Charts (also attached to AUDI-1089 in Jira; generator `../artifacts/audi_1089_generate_charts.py`, data
  from `../outputs/*.csv`):
  - `../artifacts/audi_1089_chart_sole_classified_domains.png` — cross-vendor: Klickly's 126 vs Predactiv's 226,826
  - `../artifacts/audi_1089_chart_klickly_dependency_ladder.png` — 224M touched → 3,674 sole → 26 gated
  - `../artifacts/audi_1089_chart_klickly_adverse_selection.png` — sole IPs: 0.2% delivered, 91% unscored
- Evidence tables: `../outputs/` CSVs (all 10 sources — reused by the other six evals)
- SQL: `../queries/audi_1089_q1..q5*.sql`

## Findings

*(filled as steps complete — steps per ticket-root summary.md §3)*

### 1. Liveness + cost structure + lineage blast radius — DONE 2026-07-09

**Registry** (`tpa.direct_data_partners`, CDC-deduped → 1 row): billing_type **flat_fee** (fixed_cpm null),
enabled, used_in_mntn_match=true, used_in_interests=false, type=mntn_matched, **valid_from 2025-07-01**
(≈ contract anniversary July 1 — consistent with Paulo's "they're up right now"), valid_to null,
**notes NULL — no fee amount anywhere in our data; must come from Paulo's renewal schedule.**

**Delivery — LIVE, streaming.** Klickly is NOT a file-drop partner (nothing under `gs://mntn-data-partners/`).
Path: vendor server-side sends → `pixel-page-view-signal` Kafka topic → hourly parquet
`gs://mntn-data-archive-prod/pixel_page_view_signal/dt=/hh=/` (all DSIDs commingled) →
`site_visit_signal/data_source_id=39` (landing hourly through today 2026-07-09).
Volume: **~850 of 110K rows in a sampled hour (<1% of the pixel topic; DS40 dominates it).**

**Raw richness — thinnest possible.** DS39 payload = IP + URL + timestamp ONLY: referer/user_agent/query_str
0% filled, mobile always false (server-side send), advertiser_id 100% NULL in svs.
**93.4% of DS39 URLs are `*.myshopify.com` product pages** (~155 distinct shops in sample: pulsetto, draxe,
ekster…) → Klickly = a Shopify-network product-page-view feed, mostly long-tail shop domains.

**Lineage blast radius (org-wide code sweep, file-level evidence):**
- **MM site-visit path** (vertical classification → DS19/DS13 signal builds → MM reporting/tpa_export) — the known consumer.
- **BUK training enrichment** (the ONLY real non-MM consumer): feature-store site-visit rollups
  (`site_visit_signal_advertiser_id_dsc_id`, excludes only DS23) feed the Bottom-Up Keywords ALS training
  pipeline at **source_weight=0.05 and a 5% stratified sample** — DSID-agnostic, degrades gracefully; no
  hard DS39 dependency.
- Passive monitoring/audit dashboards (would just show DS39 → 0).
- **Verified NON-consumers:** identity graph (idg: zero hits), attribution (page-view topic → analytics only;
  no data_source_id propagation to guidv2), bidder/serving/interests.
- **Conclusion: no hard dependency rescues a drop.** Off-switch is vendor-side (streaming), not a DAG change —
  DS39 isn't in ENABLED_DSIDS (that's the batch DAG); ingestion stops when their pixel stops sending.

### 2. Scale + freshness (30d: 2026-06-02 → 07-01) — DONE
- Delivers **every day, no gaps**: ~4.1M rows/day, ~1.0M IPs/day, **~160 domains/day**. 100% of URLs carry paths.
- 30d window reach: **12.4M IPs · 257 domains · 13.3M (ip,domain) pairs.** IPv6 share 0.02% (exclusion immaterial).
- Recency: 98.0% of its pairs are sole-in-window, 99.6% sole-or-freshest — maximally "unique," but on a
  domain base of 257 (the uniqueness is an artifact of nobody else covering long-tail myshopify shops).

### 3. Uniqueness (30d) — DONE
- Domains: 257 total → 176 sole → **126 sole+classified**. That is ~0.03% of MM's classified-domain universe
  (5x5 for scale: 86,084 sole classified). 98.7% of pairs are net-new vs internal — again, tiny base.
- **Method validated**: DS25 cross-checks pass (69.3% pairs sole vs TI-1027's 69.8%; sole-or-freshest 95.7%
  vs 95.4%) — same pipeline, trustworthy numbers.

### 4. Quality (delivered score tiers, junk check, IPv6) — DONE
- Klickly-touched IPs look great (29.6% delivered, 37.1% HI) — **pure co-occurrence**: those IPs are
  multi-source; every big vendor "touches" the same delivered households.
- **Klickly-SOLE IPs (its actual unique contribution): 338K IPs → 666 delivered (0.2%), 91.4% unscored,
  3 IPs at HI.** Its unique reach is adversely selected — effectively unbiddable/unscorable.
- Check A (svs-necessity): 99.95% of scored delivered IPs have svs signal (r=0.05%) — the causal chain
  "no site-visit signal → no score" holds; T1 needs no discount.

### 5. Value anchor (media/data-cost lens, tiered; CIL week 07-02 → 07-08) — DONE
| Tier | Imps/week | Media $/week | Meaning |
|---|---:|---:|---|
| T3 all touched (ceiling, co-occurrence) | 224.2M | $1.63M | meaningless for credit — 99.98% shared IPs |
| T2 all imps to sole IPs | **3,674** | **$42.40** | upper bound of real dependency |
| T1 scored (≥6666) non-RTC to sole IPs | **26** | $0.25 | "could not have served without Klickly" |
- **Performance (the headline, per ray):** 1 visit across all 3,674 sole impressions all week (VR 0.03%);
  the 10000-band sole cell has THREE impressions. There is no measurable performance because there is no
  volume — which is itself the verdict.
- BUK training upside: 0.05 source weight × 5% sample of <1% of the data — negligible.

### 6. Verdict — **PASS (do not renew) unless effectively free**
- **Implied max defensible fee: ~$0.1-1.5K/yr** (generous): 126 sole classified domains × TI-1027's $3-13/yr
  per net-new classified domain ≈ $0.4-1.6K/yr; sole-impression lens ≈ $8/mo at the $0.50 peer CPM;
  T1 lens < $1/mo. Any typical DDP flat fee (tens of $K/yr) fails break-even by 10-100×.
- Both value lenses (domain coverage AND unique delivered impressions) independently land at ~zero —
  Klickly has neither 5x5's domain breadth (86K sole classified) nor any unique deliverable reach.
- No dependency rescue (§1): MM path + negligible BUK enrichment only; off-switch is vendor-side.
- **If keeping relationships matters:** counter at a $0.50 CPM on impressions to Klickly-sole IPs
  (≈ $100/yr at current volumes) — i.e., renew only if ~free.
- Caveats stated: fee unknown (registry notes NULL — compare band against Paulo's renewal schedule);
  one-week valuation window; sole-set judged on 37d union (temporal ordering held: signal precedes serve).


## Dependency-ceiling valuation (2026-07-10, second lens — runbook/dependency_valuation.md)

Stock -> flow -> performance -> dollars, valuation week Jul 2-8 x52:
- Stock: 324,019 sole usable IPs (2.6% of its usable footprint). Flow: 3,674 sole won bids/wk ->
  ~191K/yr (envelope 95K-287K); ~5.5 wins per delivered IP; yield 0.59 wins/sole-IP/yr.
- Performance on those IPs: 1 visit/wk -> ~52/yr (Poisson 95% CI ~1-290); VR 0.0272% vs 0.0223%
  no-svs baseline — no detectable lift.
- Dollars: eCPM $11.54 -> T2 base $2,205/yr (envelope $882-$3,969); T1 provable floor $13/yr;
  break-even margin 11%; **WTP at realistic 30-50% margins: $418-859/yr; absolute ceiling ~$4.0K/yr.**
- Verdict unchanged, now double-confirmed: **drop unless ~free** (fee-band lens said $0.1-1.5K/yr;
  this lens says a flat fee above ~$4K/yr guarantees a net loss even at 100% margin and maximal
  attribution). Charts: ../runbook/charts/q9c_dependency_ceiling.png, q9c_klickly_ladder.png.
