# AUDI-1089 / Justuno (DS24) — Renewal Evaluation

**Status:** DONE 2026-07-10
**Billing:** `fixed_cpm` **$0.50 CPM** per registry (per-use). A live metering + vendor-billing pipeline EXISTS
(see §1) — Paulo's "waterfall usage basis" is real for DS24 — but the actual monthly bill was not pulled here
(out of this eval's query scope) and the metering basis is not yet reconciled against an invoice.
**Prior evidence (TI-1027, 7d windows):** 4,823 unique classified domains, **84.3% unique** — the most-unique
vendor by ratio; scorecard 64 → KEEP (efficient). 30d verdict below: the uniqueness prior HOLDS.

## Verdict (summary)

**KEEP — monitor the metered bill.** The unique-domain claim survives the 30d window (4,605 sole classified
domains, 91.6% of 86.9M pairs sole, 95.0% net-new vs free internal sources). Domain-coverage lens values DS24 at
**~$14–60K/yr** (4,605 × TI-1027's $3–13/yr per net-new classified domain). At $0.50 CPM that is a break-even of
**~2.3–10M billed impressions/month**; pull the actual number from the existing monthly usage report
(`coredw.usage_reporting_data`, ds=24) and keep iff the bill sits inside the band. Sole-IP dependency bound is
tiny (~$0.3K/yr) — but for a domain-value vendor that is the floor, not the value story (§5).

## Supporting artifacts

- Evidence tables: `../outputs/` cross-vendor CSVs (all 10 DS; windows: svs 30d 2026-06-02→07-01, CIL valuation
  week 07-02→07-08, soleness on the 37d union; method validated via DS25 cross-check 69.3% vs TI-1027 69.8%).
- Cross-vendor chart: `../artifacts/audi_1089_chart_sole_classified_domains.png` (Justuno = 4,605 bar).
- Lineage evidence: GitHub org sweep (SteelHouse), files cited inline in §1.

## Findings

### 1. Liveness + cost structure + lineage blast radius — DONE 2026-07-10

**Registry** (`tpa.direct_data_partners`, CDC-deduped 2 identical rows → 1): billing_type **fixed_cpm**,
**fixed_cpm 0.5**, enabled=true, used_in_mntn_match=true, used_in_interests=false, type=mntn_matched,
**valid_from 2025-01-01**, valid_to null, **notes NULL** — no contract terms in our data.

**Delivery — LIVE** (cited from the ticket-root liveness check: svs partitions 2026-07-07/08 carry DS24; scale
CSV shows rows every day of the 30d window, no gaps).

**Ingest path (differs from the "pixel/streaming" prior).** Current code shows a dedicated hourly **S3 file-drop**
pipeline, not the pixel topic: vendor SFTP-drops to `s3://mntn-data-partner-justuno/<dt>/<hh>/` (AWS Transfer
Family + bucket in `SteelHouse/cloudops` terraform) → hourly DAG `justuno_dsid24_ingestion`
(`SteelHouse/airflow` `dags/fpa/fpa_ingestion_justuno_dsid24.py`, start 2025-01-01 — matches registry
valid_from) → EMR Spark `justuno_process.py` → `fpa_vendor_log/data_source_id=24` (Athena `fpa_dsid24_log`) →
triggers `site_visit_signals_ds_id_24` → `site_visit_signal/data_source_id=24`. The
`pixel_page_view_signal_justuno_dsid24_backfill_workflow` files (airflow + airflow-ti; listed in airflow-ti
`.airflowignore`) are backfill/legacy for the earlier pixel-topic route. DS24 is NOT in the batch-consolidated
ENABLED_DSIDS ([23,25,26,28,30,36]) — it has its own DAG. **Off-switch = pause `justuno_dsid24_ingestion`**
(or vendor stops dropping files); one DAG, no shared-topic entanglement.

**Lineage blast radius (org-wide code sweep: "justuno" 18 hits, "dsid24" 4 hits, "data_source_id = 24" 5 hits):**
- **MM site-visit path** — the known consumer (svs → vertical classification → DS19/DS13 builds → MM).
- **BUK training enrichment** — the DSID-agnostic feature-store rollup at source_weight=0.05 / 5% sample;
  degrades gracefully, no hard DS24 dependency (same as Klickly's sweep).
- Passive monitoring/audit dashboards.
- **Billing/metering (new find, unique to per-use vendors):** `SteelHouse/bae-sql-utility`
  `ddp/ddpmonthlyusageemail-Justuno.py` — monthly job queries
  `dw-main-bronze.coredw.usage_reporting_data` WHERE `data_source_id = 24`, aggregates **impressions + usage $
  per domain**, and emails the CSV report to travis@justuno.com from partnerbilling@mountain.com
  (CC accountspayable). Table carries `tv_cpm / impressions / usage` columns (populated from
  `lds.ext_usage_reporting_data` via `db_repo` `populate_usage_reporting_data.sql`; upstream attribution rule
  not in db_repo). Sibling scripts exist for Cybba/Sovrn/33Across/ShareThis/LiveRamp/Deepsync + a Tableau
  `ddp_monthly_usage_report.sql`. **This is a metering consumer, not a data-product consumer — it confirms
  usage-based billing is operational for DS24 and gives the exact place to read the real monthly bill.**
- **Verified NON-consumers:** no identity-graph, attribution, bidder/serving, or interests hits for
  justuno/dsid24 tokens; remaining hits are registry seeds (`sqlmesh`, `pixel-event-signal-service`,
  `datasource_migration`) and backfill workflows.
- **Conclusion: no hard dependency beyond svs→MM.** Same blast-radius shape as Klickly, plus the billing meter.

### 2. Scale + freshness (30d: 2026-06-02 → 07-01) — DONE
- Delivers **every day, no gaps**: **18.9M rows/day** avg (16.8–21.4M), **3.28M IPs/day**, **~5,931 domains/day**
  (5,652–6,217). **90.7% of URLs carry paths** (rich URLs — though the MM classifier strips to domain).
- 30d window reach: **47.5M IPs · 9,337 domains · 86.6M (ip,domain) pairs.**
- Recency (37d union, 86.9M pairs): **91.6% sole**, 0.5% freshest, 7.6% tied, 0.2% stale →
  **99.8% sole-or-freshest-or-tied**. **95.0% of pairs net-new vs the free internal sources (DS23/30).**
  Redundancy-insurance value is ~nil in both directions: almost nothing it carries is covered elsewhere.
- **IPv6 = 19.61% of the feed (111.2M of 566.7M rows; daily 16.8→21.7%, trending up) — the highest of all 10
  sources by far** (next: 33Across 8.2%, Predactiv 7.9%; every other source ≤0.07%). The IPv4-only method
  measures only 80.4% of DS24's rows → its **true footprint ≈ measured × 1.244**. Direction of bias:
  **against Justuno** — IP reach, pair counts, touched/sole delivered value, and every cross-vendor ranking
  above understate DS24 by ~20% while barely touching peers; domain counts are least affected (domains recur
  across the IPv4 rows). All KEEP-side numbers here are therefore conservative.

### 3. Uniqueness (30d) — DONE
- Domains (37d union): **9,555 total → 6,869 sole (71.9%) → 4,605 sole+classified** (67.0% of sole domains
  classify; 69.1% of all its domains classify — the second-highest classified rate of the 10 sources).
- **The TI-1027 7d prior (4,823 unique classified, 84.3%) HOLDS at 30d** — uniqueness is real signal, not a
  window artifact (contrast Klickly, whose 98% pair-soleness is an artifact of a 257-domain base).
- Rank among externals (sole classified, 30d): Predactiv 226.8K > 5x5 86.1K > 33Across 6.8K > **Justuno 4.6K**
  > 33Across API 2.8K > Cybba 362 > Sovrn 181 > Klickly 126. Justuno ≈ **1.1% of MM's classified-domain
  universe** — mid-tier in absolute terms, top-tier per unit of feed volume (18.9M rows/day vs 33Across's ~1.04B).

### 4. Quality (delivered score tiers, junk check, IPv6) — DONE
- Justuno-touched IPs: 54.4M, 17.0% delivered, 30.9% of delivered at HI, 46.1% in the high band — pure
  **co-occurrence** (multi-source households; every vendor "touches" the same delivered pool).
- **Justuno-SOLE IPs (its unique reach): 5.24M IPs → 2,400 delivered (0.046%), 92.5% unscored, 35 IPs at HI.**
  Like every vendor in this pass, its unique reach is adversely selected — effectively unbiddable. Sole reach
  is NOT where its value sits.
- Check A (svs-necessity, cross-vendor): r = 0.05% → "no site-visit signal → no score" holds; T1 needs no discount.

### 5. Value anchor (media/data-cost lens, tiered; CIL week 07-02 → 07-08) — DONE

| Tier | Imps/week | Media $/week | Meaning |
|---|---:|---:|---|
| T3 all touched (ceiling, co-occurrence) | 270.7M | $2.05M | meaningless for credit — shared IPs |
| T2 all imps to sole IPs | **11,964** | **$143.89** | upper bound of real delivery dependency |
| T1 scored (≥6666) non-RTC to sole IPs | **145** | $1.59 | "could not have served without Justuno" |

- **Performance (headline, per ray):** sole-IP VR **0.0084%** (1 visit / 11,919 sole imps) — **below** the
  no-svs unscored baseline ~0.02% and ~31× below guid_log sole 0.26%. The 10000-band sole cell has 44 imps,
  0 visits — volumes too small for a stable VR read; the honest statement is "no measurable performance on
  sole IPs."
- **Stated explicitly: sole-IP metrics are the DEPENDENCY bound, not the value story.** Justuno is a
  domain-value vendor — its 4,605 sole classified domains feed domain→vertical coverage that scores IPs on
  SHARED households, which is where MM value lives (TI-1027 finding). The dependency lenses price only the
  incremental reach: T2 at the $0.50 peer CPM ≈ **$311/yr** (× 1.244 IPv6 correction ≈ $387/yr); T2 media
  ceiling ≈ $7.5K/yr; T1 ≈ $4/yr.
- BUK training upside: 0.05 weight × 5% sample — negligible.

### 6. Verdict — **KEEP; verify the metered bill against the break-even band**
- **Defensible value: ~$14–60K/yr**, dominated by the domain lens (4,605 sole classified × $3–13/yr per
  net-new classified domain). Dependency lenses add ~$0.3–7.5K/yr on top at most.
- **Break-even at $0.50 CPM: ~2.3–10M billed impressions/month** ($1.15–5.0K/mo). Unlike the flat-fee vendors,
  cost self-scales with use — the verdict is conditional on metered volume, not on an unknown lump sum.
- **The bill is readable today:** monthly usage reports for DS24 already ship to the vendor from
  `coredw.usage_reporting_data` (impressions + usage $ per domain; §1). Action: pull last month's grand total —
  if ≤ ~$5K/mo, KEEP outright; if materially above, renegotiate the rate or metered scope (its per-use billing
  makes over-metering the only realistic failure mode).
- **Why keep vs the other $0.50-CPM vendors:** Justuno is the efficiency case — 91.6% pair-sole / 95.0%
  net-new vs free (vs Sovrn 80.1% tied, 33Across API 3.2%-unique prior). Its billed units are the least
  redundant of the per-use cohort.
- Caveats: (1) fixed_cpm metering basis not reconciled against an invoice — the usage pipeline exists but the
  attribution rule upstream of `ext_usage_reporting_data` wasn't traced, and no monthly $ was computed here;
  (2) one-week valuation window; soleness on the 37d union (temporal ordering held: signal precedes serve);
  (3) all volumes IPv4-only → DS24 understated ~20% (§2), direction favors KEEP; (4) sole-IP VR is on 1 visit —
  a power statement, not a performance estimate; (5) domain-lens $ band inherits TI-1027's per-domain anchor
  ($3–13/yr), which is a framework estimate, not a market price.
