---
doc_type: ticket
title: "AUDI-1089: 33Across (DS28) DDP Renewal Evaluation"
status: done
date: 2026-07-10
summary: "33Across DS28 vendor renewal eval: defensible value vs the $0.50/CPM usage meter"
result: "NEGOTIATE cap-or-drop: value ~$30-100K/yr; renew only if metered cost caps ≤$100K/yr"
keywords: [33across, ds28, ddp renewal, fixed_cpm, usage meter, sole classified domains, audi-1089, value-per-metered-unit, targeted_signal, usage_reporting_data]
---

## TL;DR

**Q:** AUDI-1089: verdict for 33Across (DS28) DDP renewal evaluation?

**A:** NEGOTIATE (cap-or-drop). DS28 defensible value ~$30-100K/yr, dominated by 6,849 sole classified domains. Renew only if actual metered cost lands ≤ ~$100K/yr (rate cut or meter cap); else drop. Break-even on the $0.50 CPM meter is ~5-17M credited imps/month; sole impressions run only ~1.9M/mo (~$11.6K/yr), but the implemented meter also credits shared IPs fractionally, and 33Across touches 27.2M delivered IPs (most of any source including internal), so the metered base plausibly runs orders above break-even. Billing is fixed_cpm $0.50/1,000; metering basis identified at script level (credit-split MM impressions joined to external.targeted_signal data_source_id IN (13,19) on IP, 30-day lookback, split 1/N among co-matching MM vendors, landed in coredw.usage_reporting_data, emailed monthly) but NOT yet confirmed against an invoice. Action: pull actual metered dollars, reconcile vs invoice, then keep/renegotiate/drop (off-switch: remove 28 from ENABLED_DSIDS, Sean owns). No hard dependency rescues a drop. Worst value-per-metered-unit (18x worse than Justuno on same rate); 54.7% of pairs redundant vs free internal feeds DS23/30; 41.68M-IP sole reach 97% unscored at no-signal baseline. DS40 (33Across API) is a separate feed/contract despite shared parent. Status: DONE 2026-07-10.

**How:** 30d cross-vendor CIL/svs scans (svs 2026-06-02→07-01, CIL valuation week 07-02→07-08, soleness on 37d union); org-wide code sweep of SteelHouse repos for the meter script and lineage. Metering basis is script-level evidence, not invoice-confirmed; actual usage dollars not queried (out of allowed-scan scope).

**Tables:** tpa.direct_data_partners · dw-main-bronze.external.targeted_signal · dw-main-bronze.coredw.usage_reporting_data · site_visit_signal · augmentor_log · guid_log

**Learned:**
- Meter runs on shared IPs (fractional 1/N credit-split among co-matching MM vendors), not just sole impressions; 33Across's fractional base is largest in roster because it touches the most delivered IPs (27.2M).
- DS28 raw feed is 32 columns; MNTN keeps only 4 (page categories+keywords); geo, device hints, GPP consent, and a device_ids (GAID/IDFA pipe-delimited) column dropped at site_visit_signal.
- DS28 is the least domain-efficient feed per metered unit: 2,946 sole classified domains per B 30d pairs (18x worse than Justuno on the same $0.50 rate).
- 33Across sole-IP base of 41.68M is the largest sole-IP base of any vendor, but 97% unscored and performs at the no-signal VR baseline (0.026%).
- Legacy AWS DAG device_id_33across_signal.py (schedule=None, manual only) explodes device_ids into data_archive.device_id_signal; no live GCP-side consumer of DS28 device IDs.

**Reuse when:** evaluating a DDP vendor renewal on the $0.50 fixed_cpm meter · questions about 33Across DS28 value/meter/drop safety · how the MM DDP usage meter credits shared vs sole IPs · which pipelines consume 33Across / DS28 data.

---

# AUDI-1089 / 33Across (DS28) — Renewal Evaluation

**Status:** DONE 2026-07-10
**Billing:** `fixed_cpm` **$0.50 per 1,000** per registry — metering basis identified at script level (§1c, credit-split MM impressions) but **not yet confirmed against an invoice**
**Prior evidence (TI-1027, 7d):** biggest external feed (834M rows/day) · 9,277 unique classified domains (30.1%) · scorecard 46 → REVIEW
· shallowest unique depth of the majors (0.65 unique domains/IP, 27% pairs unique — §4.16) · ~38.6% IP+URL redundant vs
augmentor_log (Ryan Kleck, AUDI-647)
**Do not confuse with DS40 (33Across API)** — separate pixel/streaming feed, same parent company, billed on the same monthly
usage email but a separate line/contract; evaluated separately.

## Verdict summary

**NEGOTIATE (cap-or-drop).** The data's defensible value is **~$30–100K/yr** (dominated by 6,849 sole classified domains).
Break-even on the $0.50 CPM meter is **~5–17M credited impressions/month**; its sole impressions run only ~1.9M/mo (~$11.6K/yr),
but the implemented meter (§1c) also credits shared IPs fractionally — and 33Across touches **27.2M delivered IPs, the most of
any source including internal** — so the metered base plausibly runs orders above break-even. Renew only if the actual metered
cost lands ≤ ~$100K/yr (rate cut or meter cap); otherwise drop. The actual dollars are pullable (§1c) — pull before signing.

## Supporting artifacts

- Evidence tables: `../outputs/audi_1089_*.csv` (cross-vendor 30d scans, all 10 DS — windows: svs 2026-06-02→07-01,
  CIL valuation week 2026-07-02→07-08, soleness on the 37d union; method validated via DS25 cross-check, ticket-root §6)
- Metering-basis evidence (external repos, cited in §1c): `SteelHouse/bae-sql-utility` `ddp/usage reporting` +
  `ddp/ddpmonthlyusageemail-33Across.py`; `SteelHouse/tableau-datasources` `data-sources/ddp_monthly_usage_report.sql`

## Findings

### 1. Liveness + cost structure + lineage blast radius — DONE 2026-07-10

**1a. Registry** (`tpa.direct_data_partners`, CDC-deduped: 2 identical rows → 1): billing_type **fixed_cpm**, fixed_cpm **0.5**,
enabled, used_in_mntn_match=true, used_in_interests=false, type=mntn_matched, valid_from **2025-01-01**, valid_to null,
**notes NULL** (verbatim — no fee context in the registry).

**1b. Delivery — LIVE, batch.** DS28 is a file-drop partner in `ENABLED_DSIDS=[23,25,26,28,30,36]`
(`airflow-ti/dags/fpa/fpa_vendor_log_batch_ingestion_consolidated.py` → `spark/fpa/dsid28_33across_processing.py`); vendor S3
drop bucket provisioned in `cloudops` terraform (`mntn-data-partner-33across`). svs partitions present 2026-07-07/08
(liveness verified for all 8 vendors at ticket level — cited, not re-checked). Off-switch = remove `28` from ENABLED_DSIDS
(Sean owns) + vendor stops dropping. Raw feed is 32 columns; we keep 4 — page categories+keywords, geo, device hints, GPP
consent all dropped at site_visit_signal (TI-1027 §4.18). Raw also carries a `device_ids` column (GAID/IDFA, pipe-delimited)
— see 1d.

**1c. The meter (the cost question) — identified at script level, unconfirmed against an invoice.**
`SteelHouse/bae-sql-utility` `ddp/usage reporting` implements MM DDP usage billing:
- Served impressions are joined to `dw-main-bronze.external.targeted_signal` rows with `data_source_id IN (13,19)` (MM-targeted)
  on IP, with a **30-day signal lookback**; `ts.source_data_source_id` identifies which vendor's site-visit signal contributed.
- Winners are picked by CPM rank across DDP families; each MM-winning impression's credit is **split fractionally 1/N among the
  co-matching MM vendors** (`mm_dsid_count` + credit-divisor logic; `ddp_usage_report_ds28` block).
- Vendor usage = ceil(sum of fractional impressions) / 1,000 × $0.50, **reported per domain** (domains <1,000 imps rolled into
  an OTHER row), landed in `dw-main-bronze.coredw.usage_reporting_data` (`impressions`, `usage` columns, per `reporting_month`),
  and **emailed monthly to 33Across** (`ddpmonthlyusageemail-33Across.py`, from partnerbilling@, covering DS28 + DS40 as separate
  attachments with a combined total).
- **Implication: the meter runs on shared IPs too, not just sole ones.** Because 33Across touches more delivered IPs than any
  other source (27.2M — §4), its fractional-credit base is the largest in the roster. The actual metered dollars ARE in our
  data (`usage_reporting_data.usage`) — not queried here (outside this eval's allowed scans) and still needing invoice
  reconciliation — but this is the number to pull before any renewal signature.

**1d. Lineage blast radius (org-wide code sweep: `33across`, `dsid28`, `data_source_id = 28`, `device_id_signal`):**
- **MM site-visit path** (batch ingest → site_visit_signal/data_source_id=28 → vertical classification → MM) — the known consumer.
- **BUK training enrichment** — the DSID-agnostic feature-store rollup at source_weight=0.05 / 5% sample; degrades gracefully,
  no hard DS28 dependency.
- **Billing/audit only:** the §1c usage-reporting pipeline + Tableau `ddp_monthly_usage_report` + passive dashboards — these
  meter DS28 for paying the vendor; they are cost machinery, not value consumers.
- **Legacy AWS stack only (not live):** `SteelHouse/airflow` `fpa_ingestion_33across_dsid28.py` and
  `site_visit_signals_ds_id_28.py` (superseded by the GCP path), and `device_id_33across_signal.py` — a targeting DAG that
  explodes the raw feed's `device_ids` into `data_archive.device_id_signal` (Athena/Redshift) — **schedule=None, manual-trigger
  only**; every `device_id_signal` consumer found lives in the legacy AWS repos (`airflow` importer DAGs, `aws-importer`).
  No GCP-side consumer of DS28 device IDs.
- **Verified NON-consumers (current GCP stack):** identity graph, attribution, bidder/serving, interests
  (registry used_in_interests=false).
- **Conclusion: same shape as Klickly — no hard dependency rescues a drop.** MM path + negligible BUK weight; the only other
  live DS28-specific pipeline is the one that pays them.

### 2. Scale + freshness (30d: 2026-06-02 → 07-01) — DONE
- **The largest external feed by rows and pairs:** 30.37B rows/30d (~1.01B/day), ~48.5M IPs/day, ~155–205K domains/day,
  66–72% of URLs carry paths. One delivery dip: 2026-06-20 at 86.0M rows (~8% of typical); otherwise daily, no gaps.
- 30d window reach: **149.9M IPs · 496,724 domains · 2.325B (ip,domain) pairs** — #1 external pair base (internal augmentor:
  2.789B), #3 by IPs (behind guid_log 195.0M, 5x5 157.2M). Frequency 15.5 pairs/IP — pair-grain DEPTH, i.e., repeat visits.
- IPv6: 8.19% of rows — the IPv4-only method modestly undercounts it (far less than Justuno's 19.6%).
- Recency (37d union): **30.8% of pairs sole**, 5.2% freshest, **54.3% tied** (the roster's #2 tied share after Sovrn's 80.1%),
  9.8% stale. **Net-new vs the free internal feeds (DS23/30): 45.3%** — i.e., 54.7% of its pairs are already covered free.
  Corroboration (independent method): Ryan Kleck measured **~38.6% IP+URL match vs augmentor_log alone** (AUDI-647) — same
  direction, half-or-more redundant vs data we already own.

### 3. Uniqueness (30d) — DONE
- Domains: 499,034 total → 109,375 sole (21.9%) → **6,849 sole+classified** (23.8% of its domains classify).
- Cross-vendor rank: far below the flat-fee majors (Predactiv 226.8K, 5x5 86.1K, free augmentor 47.5K) but **the highest among
  the $0.50-CPM cohort** (Justuno 4,605, 33Across API 2,780, Cybba 362, Sovrn 181).
- **Uniqueness per metered unit — the killer stat for a per-use vendor:** sole classified domains per billion 30d pairs:
  **33Across 2,946** vs Justuno 53,175 (18×) vs 5x5 116,240 (39×) vs Predactiv 444,689 (151×). It is the least domain-efficient
  feed per unit of volume in the roster — and volume is exactly what a usage meter charges for. This is TI-1027 §4.16's
  pair-depth-vs-domain-value distinction, confirmed at 30d: its bulk is repeat visits to common domains, not new coverage.

### 4. Quality (delivered score tiers, junk check, IPv6) — DONE
- Touched IPs look great (166.1M touched, 27.2M delivered = 16.4%, 21.8% HI) — **pure co-occurrence**; it touches more delivered
  IPs than any source in the roster (27.2M > augmentor 27.0M > guid 25.5M), because it swims in the same households as everyone.
- **33Across-SOLE IPs (its actual unique contribution): 41.68M IPs — the largest sole-IP base of any vendor — of which only
  99,041 delivered (0.2%), 97.0% unscored, 92 IPs at HI.** Its enormous unique reach is adversely selected: effectively
  unbiddable/unscorable inventory.
- Check A (svs-necessity): r = 0.05% of scored delivered IPs lack svs signal — the "no signal → no score" chain holds;
  T1 needs no discount.

### 5. Value anchor (media/data-cost lens, tiered; CIL week 07-02 → 07-08) — DONE
| Tier | Imps/week | Media $/week | Meaning |
|---|---:|---:|---|
| T3 all touched (ceiling, co-occurrence) | 393.5M | $3.48M | meaningless for credit — shared IPs; data_spend on these: $325.3K/wk (all vendors combined) |
| T2 all imps to sole IPs | **446,628** | **$5,186** | upper bound of real dependency; largest external T2 (5x5: 99.3K) |
| T1 scored (≥6666) non-RTC to sole IPs | **3,048** | $33 | "could not have served without 33Across" |
- **Performance (the headline, per ray):** sole-IP VR = **0.026%** (116 visits / 446.6K imps) — statistically at the no-svs
  unscored baseline (~0.022%) and **10× below internal guid_log sole IPs (0.263%)**. Its unique reach performs like IPs with no
  signal at all. (10000-band sole cell: 125 imps, 1 visit — n too small to read.)
- **Dependency bound, not the whole story:** for a domain-value vendor, sole-IP metrics floor the value; MM value lives in
  domain→vertical coverage feeding scores on shared IPs (TI-1027). For 33Across that lens gives 6,849 sole classified domains —
  real but small, and the least efficient per metered unit in the roster (§3).
- BUK training upside: 0.05 weight × 5% sample — negligible.

### 6. Verdict — **NEGOTIATE: renew only with the meter capped ≤ ~$100K/yr, else drop**
- **Defensible value ≈ $30–100K/yr:** domain lens 6,849 sole classified × TI-1027's $3–13/yr = **$21–89K/yr**; sole-impression
  lens 446.6K/wk × $0.50 peer CPM ≈ **$11.6K/yr**; T1 lens < $100/yr. Domain lens dominates.
- **Break-even meter = ~5–17M credited impressions/month** ($30–100K/yr ÷ $0.50/1,000). What the meter would run under each
  plausible basis (**all unconfirmed against an invoice — never sign off these as the actual cost**):
  - raw rows: 30.4B/30d → **~$15.2M/mo** — implausible on its face;
  - (ip,domain) pairs: 2.33B/30d → **~$1.16M/mo** — implausible;
  - all touched impressions: ~1.71B/mo → **~$853K/mo** — implausible (exceeds the $325K/wk TOTAL data spend on its touched imps);
  - **credit-split MM winners (the implemented basis, §1c): dollars unknown here, but the base includes fractional credit on
    shared IPs across the roster's largest touched footprint — plausibly orders above the 5–17M/mo break-even;**
  - sole impressions only: ~1.9M/mo → **~$970/mo (~$11.6K/yr)** — the only basis under which the bill lands inside the value band.
- **Why not KEEP:** it is the roster's worst value-per-metered-unit (2,946 sole classified domains per B pairs — 18× worse than
  Justuno on the same $0.50 rate), half-plus redundant vs free internal feeds (54.7% of pairs covered by DS23/30; AUDI-647's
  independent ~38.6% IP+URL match vs augmentor corroborates), and its 41.7M-IP unique reach is 97% unscored performing at the
  no-signal baseline (0.026% VR).
- **Why not DROP outright:** most sole classified domains of the $0.50-CPM cohort (6,849), largest external sole delivered reach
  (99K IPs / 447K imps/wk), and the roster's #2 tied share (54.3%) — non-trivial coverage-if-down insurance (Paulo's redundancy
  point) IF the price is right.
- **Action:** pull actual DS28 metered dollars from `dw-main-bronze.coredw.usage_reporting_data` (usage, by reporting_month) /
  the partnerbilling@ monthly emails, reconcile against the invoice, then: cost ≤ ~$100K/yr → keep; above → renegotiate the rate
  or cap the meter; no movement → drop (off-switch: ENABLED_DSIDS, Sean).
- Caveats: metering basis is script-level evidence, not an invoice; actual usage dollars not queried in this eval (out of
  allowed-scan scope); one-week valuation window; sole-set judged on the 37d union (temporal ordering held); IPv4-only method
  undercounts its 8.2% IPv6 share (direction: slightly understates scale, not value rank); 2026-06-20 one-day delivery dip
  (~8% of typical volume) included in 30d totals; DS40 (33Across API) is a separate feed/verdict despite the shared parent
  and shared usage email.
