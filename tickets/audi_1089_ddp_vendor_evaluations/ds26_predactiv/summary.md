---
doc_type: ticket
title: "AUDI-1089: Predactiv (DS26) Renewal Evaluation"
status: done
date: 2026-07-10
summary: "Renewal evaluation of the Predactiv (DS26) data vendor for MNTN Matched signal"
result: "KEEP/renew — #1 classified-domain source (226,826 sole), domain value $0.68M-$2.95M/yr"
---

# AUDI-1089 / Predactiv (DS26, registry `sharethis_predactiv`) — Renewal Evaluation

**Status:** DONE 2026-07-10
**Billing:** `flat_fee` per registry (fixed_cpm NULL; **no fee amount anywhere in our data** — must come from Paulo's renewal schedule)
**Prior evidence (TI-1027, 7d windows):** **#1 unique contributor — 164,627 unique classified domains (60.1% unique)**,
scorecard 80 → KEEP · raw feed = 26 cols (hashed emails, geo, firmographics, concepts/keywords) mostly dropped at
ingest · registry SCD broken (4 conflicting is_current rows)

## Verdict summary

**KEEP (renew).** Predactiv is the single largest external source of MM-usable classified-domain signal — 226,826
sole classified domains in 30d, 2.6x 5x5 and 2.2x ALL other external vendors combined. Domain-lens value band
$0.68M–$2.95M/yr (per-unit anchor $3–13/yr × sole classified domains); any typical DDP flat fee
(tens-to-low-hundreds $K/yr) clears break-even with ~5–30x margin. Unlike every other vendor in this wave, DS26
also has a **hard non-MM consumer** (hourly hashed-email→IP signal feeding CRM/identity audience resolution).
Negotiate from the metadata we already receive but don't use; lock price before the CPM-vendor drops are visible.

## Supporting artifacts

- Evidence tables: `../outputs/` CSVs (cross-vendor 30d scans; DS26 rows) — windows: svs 2026-06-02→07-01 (30d),
  CIL valuation week 2026-07-02→07-08, soleness on the 37d union. Method validated (DS25 cross-check: 69.3% vs
  TI-1027's 69.8% ✓).
- Cross-vendor chart: `../artifacts/audi_1089_chart_sole_classified_domains.png` (Predactiv's 226,826 is the top bar).
- Lineage: SteelHouse GitHub sweep 2026-07-10 (files cited inline in §1).

## Findings

### 1. Liveness + cost structure + lineage blast radius — DONE 2026-07-10

**Registry** (`tpa.direct_data_partners`, queried 2026-07-10 — 6 physical rows, CDC-dupes ×2 over 3 logical rows;
**SCD confirmed BROKEN: 4 physical rows carry valid_to NULL**):
- **Current-intent row** (valid_from **2025-10-17**, valid_to NULL, ×2): billing_type **flat_fee**, fixed_cpm NULL,
  enabled, **type=mntn_matched**, used_in_mntn_match=true, used_in_interests=false, **notes NULL**.
- **Orphan open row** (valid_from **2025-01-01**, valid_to NULL, ×2): **type=crm**, used_in_mntn_match=false,
  notes verbatim: *"we provide report but only impression counts- unknown if this was shared with th customer"*.
  Its `mntn_matched` sibling row WAS closed correctly (valid_to 2025-10-16 12:59:59, ×2); the `crm` branch was
  never closed → two conflicting "current" logical rows (mntn_matched vs crm). Dedupe rule: take the max
  valid_from mntn_matched row. Fix lives in the seed — the registry is a SQLMesh seed
  (`SteelHouse/sqlmesh:seeds/tpa__direct_data_partners.csv`).
- The stale `crm`-type row is not pure noise: it foreshadows the real CRM/identity usage found below.
- **No fee amount in registry notes or fixed_cpm** — flat-fee amount unknown; do not invent a monthly cost.

**Delivery — LIVE, batch file-drop.** DS26 is in `ENABLED_DSIDS=[23,25,26,28,30,36]`
(`airflow-ti:dags/fpa/fpa_vendor_log_batch_ingestion_consolidated.py`). Ingest
(`airflow-ti:spark/fpa/dsid26_predactiv_processing.py`, task `dsid26_predactiv_processing` in
`fpa_site_visit_batch_serverless`): reads hourly drops `gs://mntn-data-partners/partners/predactiv/dt=YYYYMMDDHH/*.parquet`,
then (stage 1) writes the **FULL payload** to `gs://mntn-data-archive-prod/fpa_vendor_log/data_source_id=26/` and
(stage 2) a thin projection to `site_visit_signal/data_source_id=26` — `user_agent`/`query_parameters`/`advertiser_id`
explicitly nulled; only ip/url/time survive to svs. svs partitions present 2026-07-07/08 (liveness verified for all
8 vendors in the ticket-root pass — cited, not re-checked).

**Lineage blast radius (org-wide sweep: "predactiv" 19 hits, "dsid26" 6 hits, "hashed_email_signal" 25 hits):**
- **MM site-visit path** (svs → vertical classification → DS19/DS13 → MM) — the primary consumer.
- **HARD NON-MM CONSUMER (DS26-exclusive DAG): `hashed_email_ds_26_signals`**
  (`airflow-ti:dags/targeting/hashed_email_ds_26_signals.py`, hourly, severity=1, start_date 2024-09-17):
  explodes `hem_sha256` from DS26's fpa_vendor_log, dedupes (ip,hashed_email), privacy caps ≤10 IPs/email and
  ≤100 emails/IP, writes `signals/hashed_email_signal/.../data_source_id=26/hash_type=sha256`.
- **Downstream of that:** `HEMSignalReader` (`airflow-ti:spark/data_source/hem_signal_reader.py`) **hardcodes DS26
  as a delta source** in its inventory {21, 22=Experian, 23=guid_log, 26=Predactiv, 29=Deepsync} and feeds
  `tpa_export/populate_data_source` + `ipdsc_resolution_strategies.py` — the hashed-email→IP resolution used to
  build CRM/identity-matched audiences (the DS47 CRM-IDG rollout's risk analysis attributes IP sources incl.
  predactiv: `dsanalysis:project-ds47-rollout/`). **Predactiv is the ONLY MM site-visit DDP among the five HEM
  sources** — dropping DS26 removes one of just three external hashed-email suppliers (with Experian, Deepsync).
- **This corrects TI-1027 §4.18** ("hashed emails dropped"): `hem_sha256` IS consumed, hourly. Still dropped:
  geo, `domain_industries`/firmographics, concepts/keywords, user_agent (retained in fpa_vendor_log parquet,
  consumed by nothing).
- Known DSID-agnostic BUK feature-store rollup (0.05 weight, 5% sample) + passive audit dashboards — degrade
  gracefully, as for all vendors.
- **Commercial context:** the vendor relationship spans TWO feeds. `bae-sql-utility:ddp/ddpmonthlyusageemail-Sharethis.py`
  emails a monthly usage report (impressions × tv_cpm from `coredw.usage_reporting_data`) for **DS17 = ShareThis
  interests** (NOT DS26) to margaret@/platformops@sharethis.com + tiffini@/sheldon@predactiv.com — matching the
  registry note verbatim. DS26 is the flat-fee MM arm of a broader ShareThis/Predactiv relationship.
- **Conclusion: unlike Klickly, a drop is NOT free** — it breaks a severity-1 hourly DAG and thins the CRM/identity
  HEM pool, in addition to the MM domain-coverage loss quantified below.

### 2. Scale + freshness (30d: 2026-06-02 → 07-01) — DONE
- Delivers **every day, 30/30, no gaps**: mean **74.1M rows/day · 15.3M IPs/day · ~220K domains/day** (2.22B rows
  in 30d); 74.6% of URLs carry paths; **IPv6 7.9%** (IPv4-only method modestly undercounts it; flag, not fatal).
- 30d window reach: **88.2M IPs · 849.0K domains · 510.1M (ip,domain) pairs** — the largest domain universe of any
  vendor (5x5: 524.5K domains; nobody else above 500K).
- **Recency (pair grain — its weak axis): 23.1% sole · 8.3% freshest · 42.7% tied · 25.9% stale**;
  sole-or-freshest-or-tied 74.1%; 51.6% net-new vs the free internal sources (DS23/30). Lowest sole-pair share of
  the flat-fee vendors (5x5 69.3%, guid_log 94.4%).

### 3. Uniqueness (30d) — DONE: the pair-vs-domain tension, reconciled
- Domain grain: **851,448 domains → 482,951 sole (56.7%) → 226,826 sole+classified** (47.0% of its sole domains
  classify to a vertical; 367,287 = 43.1% of all its domains are classified).
- **#1 by a wide margin: 226,826 sole classified = 2.6x 5x5 (86,084), 4.8x internal augmentor (47,463), 2.2x ALL
  other external vendors combined (100,987), and ~69% of total external sole-classified supply.**
- **Reconciling weak pairs (23.1% sole) vs dominant domains (56.7% sole):** MM's value chain consumes
  domain→vertical classification — the classifier strips URLs to domains and needs only ONE vendor to observe a
  domain; which IPs carried the observation is irrelevant to classification. Predactiv's (ip,domain) PAIRS are
  heavily co-observed (77% covered elsewhere in some form → weak as unique *reach/recency*), but its DOMAINS are
  not: cut DS26 and 482,951 domains (226,826 classified) leave the observable universe entirely. **Domains > pairs
  for a classification-feeding vendor — the pair number measures redundancy of reach, the domain number measures
  irreplaceability of signal.** The 42.7% tied share is Paulo's validity/insurance value: same-pair corroboration
  if another feed goes down.

### 4. Quality (score tiers, junk check, IPv6) — DONE
- Touched IPs (co-occurrence, not credit): 95.6M vendor IPs → 21.9M delivered (**22.9%**), 24.2% HI, 51.3%
  unscored — indistinguishable from the other big vendors; the household score is a property of the household.
- **Sole IPs (its actual unique reach): 6.28M → 5,623 delivered (0.1%), 96.6% unscored, 12 IPs at HI** — adversely
  selected, same pattern as every vendor in this wave (§6 ticket-root early read). Unique *reach* is not what
  anyone is buying from Predactiv.
- Check A (svs-necessity): r=0.05% of scored delivered IPs lack svs signal — "no site-visit signal → no score"
  holds; T1 needs no discount.

### 5. Value anchor (media/data-cost lens, tiered; CIL week 07-02 → 07-08) — DONE
| Tier | Imps/week | Media $/week | Meaning |
|---|---:|---:|---|
| T3 all touched (ceiling, co-occurrence) | 349.6M | $2.84M | meaningless for credit — shared IPs |
| T2 all imps to sole IPs | **27,323** | **$319.26** | upper bound of unique-REACH dependency (≈$16.6K/yr media) |
| T1 gated+scored (HS≥6666, non-RTC) sole | **216** | $2.37 | "could not have served without" — ≈$123/yr media |
- **Performance (sole-IP dependency bound):** 5,623 sole delivered IPs, 27,322 imps, 4 visits → **VR 0.0146%** —
  below the no-svs unscored baseline (~0.0223%) and 18x below guid_log-sole (0.2626%); 10000-band sole cell = 20
  imps, 0 visits. **Explicitly: sole-IP metrics are the DEPENDENCY bound, not the value story.** Predactiv is a
  domain-value vendor — its 226.8K sole classified domains feed vertical coverage that scores IPs on SHARED
  inventory, where MM value actually lives (TI-1027 finding). The correct value lens is the domain lens below.
- **Domain lens (the headline): 226,826 sole classified domains × $3–13/yr per net-new classified domain
  (TI-1027 §4.14 anchor) = $0.68M–$2.95M/yr defensible value band.**
- Sole-impression lens at the $0.50 peer CPM: 27,323/wk ≈ 1.42M imps/yr ≈ **$710/yr** — negligible for every
  vendor in this wave; not decision-relevant here.
- HEM lens (unpriced): only external site-visit DDP feeding hashed_email_signal → CRM/identity resolution; no
  per-unit anchor exists — qualitative KEEP-weight, quantify only if the renewal turns contentious.

### 6. Verdict — **KEEP (renew)** + negotiation posture
1. **Break-even:** flat-fee amount unknown (registry NULL). Domain-lens value $0.68M–$2.95M/yr → a typical DDP
   flat fee (tens-to-low-hundreds $K/yr; 5x5's fair band was $150–600K for 2.6x LESS sole-classified signal)
   clears break-even by roughly **5–30x**. Even a 5x5-sized fee at the top of its band ($600K) sits under
   Predactiv's value floor.
2. **Negotiation lever at zero vendor cost:** we already receive 26 columns; only ip/url/time (MM) + hem_sha256
   (identity) are consumed. Geo, firmographics/domain_industries, concepts/keywords, user_agent land in
   `fpa_vendor_log/data_source_id=26/` parquet and rot. Do NOT pay for any "enrichment" upsell — activation is a
   pipeline change on data already delivered. (Precedent: hem_sha256 was "dropped" in TI-1027's June audit and is
   now a severity-1 hourly production signal.)
3. **Concentration risk (renewal posture):** if the redundant CPM vendors are dropped per their priors
   (Sovrn 181 / Cybba 362 / 33Across-API 2,780 sole classified — jointly <1.5% of Predactiv's contribution),
   Predactiv becomes ~70% of external sole-classified supply. Its leverage rises the moment those drops become
   visible. **Sequence matters: lock Predactiv's renewal (multi-year or price-cap) BEFORE or concurrent with the
   CPM-vendor terminations, not after.** Also: its 42.7% tied-pair share is our insurance AGAINST other feeds —
   which cuts both ways; a Predactiv outage after the drops leaves materially less same-pair corroboration.
4. **Drop cost is not just MM:** severity-1 `hashed_email_ds_26_signals` breaks; HEM pool loses 1 of 3 external
   suppliers; MM loses its #1 classified-domain source. No scenario in the data supports a drop.

## Caveats
- **Fee unknown:** flat-fee amount is not in our data (fixed_cpm NULL, notes NULL on the current row); band-vs-fee
  comparison waits on Paulo's renewal schedule. The $0.50 CPM peer rate is a metering basis NOT yet confirmed
  against an invoice.
- One-week valuation window (07-02→07-08); soleness judged on the 37d union (temporal ordering held).
- **IPv6 7.9%** — IPv4-only soleness modestly undercounts DS26 (far below Justuno's 19.6%; direction: undercount,
  so the KEEP case only strengthens).
- $3–13/yr per-domain anchor inherits TI-1027's calibration (B2B-weighted MM value attribution); the band is an
  order-of-magnitude instrument, not a price quote — but the verdict survives the bottom of the band.
- Per-domain value is not strictly linear in domain count (marginal domains skew longer-tail than 5x5's); even at
  a 50% haircut the floor is ~$340K/yr.
- Registry SCD broken (4 valid_to-NULL physical rows) — anyone consuming `direct_data_partners` naively
  double-counts DS26 or reads it as `crm`; fix in the SQLMesh seed.
