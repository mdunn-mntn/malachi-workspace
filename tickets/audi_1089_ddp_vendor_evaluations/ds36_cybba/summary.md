# AUDI-1089 / Cybba (DS36) — Renewal Pass/Play

**Status:** DONE 2026-07-10
**Billing:** `fixed_cpm` **$0.50 per 1,000** per registry — per-use billed (unlike flat-fee Klickly/5x5/Predactiv).
Metering surface FOUND in code (§1) but the metered amount is NOT reconciled against an invoice — never quote a
monthly cost from signal volume.
**Prior evidence (TI-1027, 7d windows):** 1.8M rows/day · 0.55M IPs/day · ~5K domains/day · 81.9% URLs with path ·
**309 unique classified domains (5.7% unique)** · raw feed genuinely thin (ip/url/time only, §4.18) · scorecard
22 → REVIEW ("6% unique, per-use billed")

## Verdict — **DROP unless effectively free (metered bill ≤ ~$1–5K/yr)**

- **Max defensible fee ~$1.1–4.7K/yr:** 362 sole classified domains × TI-1027's $3–13/yr per net-new classified
  domain ≈ $1.1–4.7K/yr. Sole-impression lens ≈ **$121/yr** (4,668 sole imps/wk × $0.50 CPM); T1 lens ≈ **$0.86/yr**
  (33 gated sole imps/wk). Both lenses — domain coverage AND unique deliverable reach — land at ~zero, independently.
- **Break-even vs the $0.50 CPM:** the defensible band funds only ~185–780K metered units/month at $0.50/1,000.
  Actual metered usage = `SUM(usage)` in `dw-main-bronze.coredw.usage_reporting_data` WHERE data_source_id=36
  (the table behind the monthly usage email we send Cybba, §1) — one query for billing to pull. If the monthly
  usage total exceeds ~$90–390/mo, Cybba fails break-even. (For scale ONLY, not a bill: the raw feed is ~53M
  rows/mo; metering is NOT confirmed to be on ingested rows.)
- **Because it's per-use billed, dropping realizes immediate savings** — no waiting for a renewal date, unlike
  flat-fee vendors. Off-switch is OURS: Sean removes `36` from `ENABLED_DSIDS` (DAG change, §1) + Cybba stops
  file drops; also retire `ddpmonthlyusageemail-Cybba.py`.
- No dependency rescue (§1): MM path + negligible DSID-agnostic BUK enrichment + billing/audit reporting only.
- **If keeping the relationship matters:** keep only at a usage bill ≤ ~$5K/yr — i.e. effectively free.

## Supporting artifacts

- Evidence tables: `../outputs/` cross-vendor CSVs (all 10 DS; windows: svs 30d 2026-06-02→07-01, CIL valuation
  week 2026-07-02→07-08, soleness on the 37d union). Method validated (DS25 recency 69.3% vs TI-1027's 69.8% ✓).
- SQL: `queries/audi_1089_ds36_registry.sql` (registry row; scans reused from ticket-root `../queries/`).

## Findings

### 1. Liveness + cost structure + lineage blast radius — DONE 2026-07-10

**Registry** (`tpa.direct_data_partners`, CDC-deduped 2 identical rows → 1): billing_type **fixed_cpm**,
**fixed_cpm = 0.5**, enabled=true, used_in_mntn_match=true, used_in_interests=false, type=mntn_matched,
valid_from **2025-01-01**, valid_to null, **notes NULL** (verbatim — no fee amount, no metering basis in registry).

**Delivery — LIVE, batch file-drop.** Cybba IS a batch partner: `gs://mntn-data-partners/partners/cybba/date=/hour=/`
→ `fpa_site_visit_batch_serverless` DAG (`airflow-ti/dags/fpa/fpa_vendor_log_batch_ingestion_consolidated.py`,
`ENABLED_DSIDS = [23, 25, 26, 28, 30, 36]`, dsid36 lag 2h, script `spark/fpa/dsid36_cybba_processing.py`) →
`site_visit_signal/data_source_id=36`. svs partitions present 2026-07-07/08 (verified once for all 8 vendors — cited,
not re-checked). **Dropping requires a DAG change (Sean Yang — DAG failure alerts route to syang@) — unlike
streaming Klickly, whose off-switch was vendor-side.**

**Lineage blast radius (org-wide code sweep: `cybba`, `dsid36`, `data_source_id = 36`, `data_source_id=36`):**
- **MM site-visit path** (site_visit_signal → vertical classification → DS19/DS13 builds → MM) — the known consumer.
- **BUK training enrichment** — DSID-agnostic feature-store rollup at 0.05 source-weight / 5% sample; Cybba is
  ~0.07% of svs rows (1.75M of ~2.57B/day) → negligible, degrades gracefully.
- **Billing/usage reporting (Cybba-specific find):** `bae-sql-utility/ddp/ddpmonthlyusageemail-Cybba.py` —
  monthly usage email from partnerbilling@mountain.com to Cybba (Tom.McKeon@, Jeff.Ellis@cybba.com), attaching
  per-domain `SUM(impressions)` + `SUM(usage)` ($) from `dw-main-bronze.coredw.usage_reporting_data`
  WHERE data_source_id=36 for the prior month. **This is the metering surface for the $0.50 CPM** — the actual
  monthly bill total exists in that table. Semantics of its `impressions` counter (what qualifies an impression
  as Cybba usage) not verified against an invoice here (out of scope; billing owns).
- Legacy predecessor DAGs in old `SteelHouse/airflow` (`fpa_ingestion_cybba_dsid36.py`,
  `site_visit_signals_ds_id_36.py`) — superseded by airflow-ti consolidated DAG; registry seed (sqlmesh),
  GCS-bucket terraform (mntn-devops), data-source dim seeds (pixel-event-signal-service, datasource_migration) —
  passive/config, not data consumers.
- **Verified NON-consumers: identity graph, attribution, bidder/serving, interests — zero hits.**
- **Conclusion: no hard dependency rescues a drop.** Same blast radius as Klickly, plus a billing email to retire.

### 2. Scale + freshness (30d: 2026-06-02 → 07-01) — DONE
- Delivers **every day, no gaps**: avg **1.75M rows/day** (range 1.3–2.3M), **~536K IPs/day**, **~6.0K domains/day**,
  78.0% of URLs carry paths. IPv6 share 0.06% (IPv4-only method immaterial here).
- 30d window reach: **8.77M IPs · 26.1K domains · 12.6M (ip,domain) pairs** — the **smallest external IP and pair
  base of all 8 vendors** (next-smallest: Klickly 12.4M IPs; vs Justuno 47.5M, 33Across 149.9M).
- Recency (37d union, 11.71M pairs): **68.6% sole** · 0.9% freshest · 21.2% tied · 9.2% stale; 70.3% net-new vs
  free internal (DS23/30); 90.8% sole-or-freshest-or-tied.
- **Absolute-first discipline (the Klickly lesson): 68.6% sole is the 2nd-highest external share — and trivial.**
  8.03M sole pairs = **1.6% of 5x5's** 513.6M and **1.1% of 33Across's** 717.8M sole pairs. High percentage on the
  smallest base ≈ nobody else covers its handful of pages, not meaningful unique coverage.

### 3. Uniqueness (30d) — DONE
- Domains: 26,242 total → **1,475 sole (5.6%)** → **362 sole+classified** ≈ **0.09% of MM's classified-domain
  universe** (~387K, TI-1027 denominator). Rank 8 of 10 — above only Sovrn (181) and Klickly (126); Predactiv
  226,826 is **627×** larger, 5x5 86,084 is 238×, even Justuno (fellow $0.50-CPM) 4,605 is 13×.
- **68.3% of Cybba's domains are classified — the highest classification rate of all 10 sources** (5x5 32.3%,
  Predactiv 43.1%). Cybba sees quality commercial head-domains — which is exactly why only 5.6% are sole:
  everyone else already covers them. Quality without uniqueness = redundancy.
- 30d improves the 7d prior only marginally (309 → 362 sole classified); no rescue in the wider window.

### 4. Quality (delivered score tiers, junk check, IPv6) — DONE
- Cybba-**touched** IPs look the best of ALL 10 vendors: **33.7% delivered, 37.4% HI** — the co-occurrence trap at
  its most extreme. A retargeting/cart-abandonment vendor watches advertiser sites, so it "touches" exactly the
  multi-source households MNTN already delivers to. Zero credit.
- **Cybba-SOLE IPs (its actual unique contribution): 397.7K IPs → 739 delivered (0.19%), 91.7% unscored,
  8 IPs at HI.** Same adverse selection as every vendor's sole set — effectively unbiddable/unscorable.
- Check A (svs-necessity): r = 0.05% (5,016 of 9.28M scored delivered IPs lack svs) — "no site-visit signal →
  no score" holds; T1 needs no discount.

### 5. Value anchor (media/data-cost lens, tiered; CIL week 07-02 → 07-08) — DONE
| Tier | Imps/week | Media $/week | Meaning |
|---|---:|---:|---|
| T3 all touched (ceiling, co-occurrence) | 205.5M | $1.48M | meaningless for credit — shared IPs |
| T2 all imps to sole IPs | **4,668** | **$53.90** | upper bound of real dependency |
| T1 scored (≥6666) non-RTC to sole IPs | **33** | $0.34 | "could not have served without Cybba" |
- **Performance (the headline, per ray): 0 visits across all 4,666 sole impressions all week (VR 0.00%).**
  Benchmarks: no-svs unscored baseline ~0.02%, guid_log sole 0.26%, Klickly sole 0.03% (1 visit). The 10000-band
  sole cell has SEVEN impressions, 0 visits. No volume → no measurable performance → that is the verdict.
- **Dependency-bound framing:** sole-IP metrics bound the dependency, not the whole value story — for domain-value
  vendors, MM value lives in domain→vertical coverage feeding scores on shared IPs (TI-1027). But Cybba's
  domain-side contribution is also ~zero (362 sole classified, §3), so **both lenses independently land at ~zero.**
- BUK training upside: 0.05 weight × 5% sample of ~0.07% of svs — negligible.

### 6. Verdict — **DROP unless effectively free**
- Fee band + break-even: see Verdict block at top. Max defensible ~$1.1–4.7K/yr; break-even at ~185–780K metered
  units/mo at $0.50/1,000; actual bill lives in `coredw.usage_reporting_data` (reconcile before executing).
- **vs Klickly (the closest comp):** same magnitude story — hundreds of sole classified domains (362 vs 126),
  a few thousand sole imps/wk (4,668 vs 3,674), T1 in the tens (33 vs 26), ≤1 visit/wk (0 vs 1). Two differences
  cut opposite ways: (1) **per-use billing → dropping Cybba saves real metered dollars immediately**, Klickly's
  flat fee only saves at renewal; (2) **off-switch needs OUR DAG change** (remove 36 from ENABLED_DSIDS — Sean),
  Klickly's was vendor-side. Neither changes the value math.
- Caveats: metered $ NOT reconciled against an invoice (metering mechanism found in code; `impressions` counter
  semantics unverified — do NOT price the drop off signal-row volume); one-week valuation window; soleness on the
  37d union (temporal ordering held: signal precedes serve); IPv4-only (Cybba IPv6 0.06%, immaterial);
  ±2-unit differences between value_tiers (739 IPs/4,668 imps) and vr_sole (738/4,666) tables — immaterial;
  0 visits on 7 HI-band sole imps carries no statistical power — the absence of volume is itself the finding.
