# AUDI-1089 / Sovrn (DS33) — Renewal Pass/Play

**Status:** DONE 2026-07-10 — verdict below (§6)
**Billing:** `fixed_cpm` **$0.50 per 1,000** per registry — the only per-use MM vendor evaluated so far where we
also found the internal metering pipeline (§1). Paulo's "waterfall usage-based" description is CORRECT for this
vendor — the waterfall is in our own code.
**Prior evidence (TI-1027, 7d windows):** 293 unique classified domains · **1.6% domain-unique** (2nd-worst of 8) ·
per-IP depth **0.10 unique domains/IP (worst of all 10 sources)** · scorecard **12 (last place)** → DROP-CANDIDATE
("Sovrn ≈ fully redundant; the per-use $0.50-CPM vendors are where the waste is").

## Supporting artifacts

- Evidence tables: `../outputs/audi_1089_*.csv` (cross-vendor, all 10 DS; windows: svs 30d 2026-06-02→07-01,
  CIL valuation week 07-02→07-08, soleness on the 37d union). Method validated via DS25 cross-check
  (69.3% pairs sole vs TI-1027's 69.8%).
- Metering code (lineage sweep finds): `SteelHouse/bae-sql-utility` → `ddp/usage reporting` (waterfall SQL),
  `ddp/ddpmonthlyusageemail-Sovrn.py` (monthly self-report email to Sovrn);
  `SteelHouse/db_repo` → `coredw/lds/functions/populate_usage_reporting_data.sql`.

## Findings

### 1. Liveness + cost structure + lineage blast radius — DONE 2026-07-10

**Registry** (`tpa.direct_data_partners`, CDC-deduped 2 identical rows → 1): billing_type **fixed_cpm**,
**fixed_cpm 0.5**, enabled, used_in_mntn_match=true, used_in_interests=false, type=mntn_matched,
valid_from **2025-05-28**, valid_to null, **notes NULL**.

**Delivery — LIVE, streaming** (cited from the 8-vendor liveness check 2026-07-09: svs partitions
2026-07-07/08 carry DS33). Sovrn is a pixel/streaming vendor — server-side sends → `pixel-page-view-signal`
Kafka topic → `site_visit_signal/data_source_id=33`. Not in the batch DAG's ENABLED_DSIDS (expected — that list
is file-drop vendors 23,25,26,28,30,36). Off-switch is vendor-side (stop the stream), not a DAG change.

**Lineage blast radius (org-wide code sweep: `sovrn`, `dsid33`, `"data_source_id = 33"`):**
- **MM site-visit path** (svs → vertical classification → DS19/DS13 builds) — the known consumer.
- **BUK training enrichment** — the DSID-agnostic feature-store rollup at 0.05 source weight / 5% sample;
  degrades gracefully, no hard DS33 dependency (same as Klickly).
- Passive docs/monitoring only: olympus data docs (`fpa_dsid33__v1.md` etc.), sqlmesh external table
  `fpa_dsid33__v1.sqlx`, Mode "Monitoring Null IP Rate in Pixel Page View", two ingest **backfill** DAGs
  (`pixel_page_view_signal_sovrn_dsid33_backfill_workflow.py` in airflow + airflow-ti).
- **Verified NON-consumers:** identity graph, attribution, bidder/serving, interests — zero hits (matches
  the Klickly sweep result).
- **NEW — the billing metering pipeline is itself a DS33 consumer (cost side, not value side):**
  `bae-sql-utility/ddp/usage reporting` implements the DDP usage waterfall: MM-targeted impressions
  (`cost_impression_log`, DS 13/19) are joined to `targeted_signal` on IP over a **30-day lookback**; every
  vendor `source_data_source_id` (incl. 33) that touched the impression's IP gets into the winner/credit-split
  logic at its `fixed_cpm`; results land in `dw-main-bronze.coredw.usage_reporting_data` (per-domain
  impressions + usage $ per month) and `ddpmonthlyusageemail-Sovrn.py` emails the report from
  partnerbilling@mountain.com to cchumley@sovrn.com / dataaccountsreporting@sovrn.com monthly.
  **Implication 1:** billing is metered on impressions to IPs Sovrn *touched* — shared/tied IPs earn credit,
  not just sole contribution. **Implication 2:** the actual monthly Sovrn bill IS in our data
  (`coredw.usage_reporting_data`, data_source_id=33) — one query, out of this eval's allowed scans; pull it
  before the renewal call.
- **Separate relationship, do not conflate:** Sovrn (FMX) is also a **PMP inventory vendor** in gary-ql
  (`core.partners` id 68, prefix `svr`) — supply-side deals, untouched by dropping the DS33 data feed.

### 2. Scale + freshness (30d: 2026-06-02 → 07-01) — DONE
- Delivers **every day, no gaps**: ~56.1M rows/day (range 46.4–68.0M), ~7.8M IPs/day, **~32.5K domains/day**,
  91.9% of URLs carry paths. IPv6 0.07% (IPv4-only method immaterial here; contrast Justuno 19.6%).
- 30d window reach: **59.9M IPs · 175,045 domains · 135.9M (ip,domain) pairs.** Real web-wide breadth —
  ~680× Klickly's domain count — but breadth ≠ uniqueness (§3).
- **Recency (37d union, 84.1M pairs): 12.5% sole · 2.2% freshest · 80.1% tied · 5.2% stale.**
  **80.1% tied is the highest same-day-redundancy share of all 10 sources** (next: 33Across 54.3%).
  Sole-or-freshest-or-tied 94.8% — i.e., when Sovrn has a pair, someone else almost always has it the same day.
- Direction of the redundancy: **net-new-vs-free = 99.7%** — the free internal sources (guid/augmentor) do NOT
  carry Sovrn's pairs. Its tied cover comes from the other **paid** feeds (33Across DS28 / 33Across API DS40
  commons). Sovrn is same-day insurance *for the 33Across feeds*, not incremental signal over internal.

### 3. Uniqueness (30d) — DONE
- Domains: 175K total → **1,614 sole (0.9%, worst of 8 external)** → **181 sole+classified** (total classified
  35,759 = 20.5%). For scale: Predactiv 226.8K sole classified, 5x5 86.1K, Klickly 126.
- Pairs: 10.5M sole of 84.1M (12.5%). The 30d window does NOT rescue the 7d DROP-CANDIDATE prior
  (1.6% unique @7d → 0.9% domain-sole @30d).

### 4. Quality (delivered score tiers, junk check, IPv6) — DONE
- Sovrn-touched IPs look excellent (25.7% delivered, 26.2% HI of delivered) — **pure co-occurrence**; every
  big vendor touches the same delivered households.
- **Sovrn-SOLE IPs (its actual unique contribution): 2.71M IPs → 866 delivered (0.03%), 95.0% unscored,
  2 IPs at HI.** The most adversely-selected sole cohort measured so far (Klickly sole was 0.2% delivered).
  Its unique reach is effectively unbiddable/unscorable.
- Check A (svs-necessity, cross-vendor): r=0.05% — "no site-visit signal → no score" holds; T1 needs no discount.

### 5. Value anchor (media/data-cost lens, tiered; CIL week 07-02 → 07-08) — DONE
| Tier | Imps/week | Media $/week | Meaning |
|---|---:|---:|---|
| T3 all touched (ceiling, co-occurrence) | 334.4M | $2.81M | meaningless for credit — 80.1% tied signal |
| T2 all imps to sole IPs | **4,380** | **$51.07** | upper bound of real dependency |
| T1 scored (≥6666) non-RTC to sole IPs | **36** | $0.40 | "could not have served without Sovrn" |
- **Performance (the headline, per ray): 0 visits on all 4,380 sole impressions all week (VR 0.000%)** — below
  even the no-svs unscored baseline ~0.02% (expected ~0.9 visits; observed 0) and nowhere near guid_log sole
  0.26%. The 10000-band sole cell has TWO impressions. No measurable performance because no volume — which is
  itself the verdict.
- **Dependency bound vs domain-value story:** sole-IP metrics are the dependency bound, not the whole value
  story — MM value lives in domain→vertical coverage feeding scores on shared IPs (TI-1027). But Sovrn fails
  that lens too: 181 sole classified domains means its 175K-domain breadth is almost entirely the commons other
  vendors already classify. Both lenses land at ~zero.
- **The per-use problem (TI-1027's exact waste pattern):** the metering (§1) credits DS33 on MM impressions to
  IPs it touched within 30d — and 80.1% of its signal is same-day-duplicated by other paid feeds. We pay
  per-use for signal that changes nothing: remove Sovrn and the same IPs stay MM-targetable via 33Across/
  33Across API/internal the same day.
- **The insurance framing (Paulo's redundancy-has-validity-value point), priced:** if Sovrn went dark,
  87.5% of its pairs remain covered in-window by another source (80.1% tied + 5.2% fresher-elsewhere + 2.2%
  freshest-but-shared); the loss = 12.5% sole pairs → 1,614 domains → 181 classified → 866 deliverable IPs →
  0 visits/week. That is the entire insured risk — and it only materializes if the 33Across feeds ALSO fail
  (net-new-vs-free 99.7% means internal doesn't cover the commons). Coverage-if-down is an option contract;
  options are priced as small flat fees, not per-impression premiums that accrue every month the insurance
  is NOT needed. Per-use billing is structurally the wrong instrument for redundancy value.

### 6. Verdict — **DROP (do not renew per-use) — keep only if re-papered to a nominal flat insurance fee**
- **Implied max defensible value: ~$0.1–2.4K/yr** (generous): 181 sole classified domains × TI-1027's $3–13/yr
  per net-new classified domain ≈ $0.5–2.4K/yr; sole-impression CPM lens ≈ **$114/yr** (4,380/wk × 52 ×
  $0.50/1000); T1 lens ≈ $1/yr.
- **Break-even at $0.50 CPM:** metered volume must stay ≤ ~4.8M credited imps/yr to clear even the generous
  $2.4K ceiling — but the waterfall credits co-touched IPs (334M touched imps/wk), so the actual bill is
  near-certainly orders of magnitude above the defensible band. **The actual number exists:**
  `coredw.usage_reporting_data` DS33 monthly grand total (already emailed to Sovrn monthly) — pull it and the
  break-even check is arithmetic.
- Both value lenses (domain coverage AND unique deliverable reach) independently land at ~zero, its sole
  cohort is the most adversely selected measured (0.03% delivered, 0 visits), AND unlike the flat-fee vendors
  the cost meter keeps running on redundant volume. Weakest keep-case of the vendors evaluated so far.
- No dependency rescue (§1): MM path + negligible BUK enrichment + passive dashboards; billing pipeline is a
  cost, not a value, consumer. PMP/inventory Sovrn relationship is separate and unaffected.
- **If Paulo wants the redundancy as validity insurance:** counter by converting to a **flat fee ≤ ~$1–2K/yr
  with metered billing removed** (the domain-lens ceiling), or by re-scoping the meter to sole-contribution
  impressions (~$114/yr at current volumes). If Sovrn declines, drop — 33Across (DS28, 2.3B pairs, 30.8% sole)
  and internal sources carry the delivery-relevant coverage.
- Caveats: (a) $0.50-per-1,000 metering basis not confirmed against an actual invoice — the internal usage
  report is self-metered and the exact credit-split shares in the waterfall weren't traced end-to-end;
  (b) actual monthly bill not pulled (out of allowed scans for this eval) — flagged as the immediate next
  query; (c) one-week valuation window; (d) soleness judged on the 37d union (temporal ordering held);
  (e) 0-visit VR is a small-n bound (4,380 imps), not a precise rate — but the smallness IS the finding.
