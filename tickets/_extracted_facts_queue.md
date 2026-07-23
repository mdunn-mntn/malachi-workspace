# Extracted Facts Queue (human review → knowledge/)

Append-only queue of durable facts surfaced during the context pass. A human reviews
each and merges into the named home doc. Do NOT auto-merge into knowledge/.

## data_knowledge.md

- **[audi_1089_ddp_vendor_evaluations/ds24_justuno]** DS24 (Justuno) IPv6 traffic is 19.61% of the feed (111.2M of 566.7M rows, daily trend 16.8→21.7%), the highest of all 10 DDP sources by far (next 33Across 8.2%, Predactiv 7.9%, every other source ≤0.07%); IPv4-only measurement captures only 80.4% of DS24 rows so true footprint ≈ measured × 1.244.
  - source_line: "IPv6 = 19.61% of the feed (111.2M of 566.7M rows; daily 16.8→21.7%, trending up) — the highest of all 10 sources by far (next: 33Across 8.2%, Predactiv 7.9%; every other source ≤0.07%). The IPv4-only method measures only 80.4% of DS24's rows → its true footprint ≈ measured × 1.244."

- **[audi_1089_ddp_vendor_evaluations/ds26_predactiv]** DS17 (ShareThis interests) and DS26 (Predactiv, the flat-fee MM arm) are two feeds of one ShareThis/Predactiv vendor relationship; the monthly usage email (ddpmonthlyusageemail-Sharethis.py, impressions x tv_cpm from coredw.usage_reporting_data) covers DS17 NOT DS26 and goes to margaret@/platformops@sharethis.com + tiffini@/sheldon@predactiv.com.
  - source_line: "the vendor relationship spans TWO feeds. `bae-sql-utility:ddp/ddpmonthlyusageemail-Sharethis.py` emails a monthly usage report (impressions × tv_cpm from `coredw.usage_reporting_data`) for **DS17 = ShareThis interests** (NOT DS26) ... DS26 is the flat-fee MM arm of a broader ShareThis/Predactiv relationship."

- **[audi_1089_ddp_vendor_evaluations/ds26_predactiv]** The hashed_email_ds_26_signals DAG applies privacy caps of <=10 IPs per email and <=100 emails per IP when writing hashed_email_signal/.../data_source_id=26/hash_type=sha256.
  - source_line: "explodes `hem_sha256` from DS26's fpa_vendor_log, dedupes (ip,hashed_email), privacy caps ≤10 IPs/email and ≤100 emails/IP, writes `signals/hashed_email_signal/.../data_source_id=26/hash_type=sha256`."

- **[audi_1089_ddp_vendor_evaluations/ds26_predactiv]** HEMSignalReader's 5-source HEM inventory is {21, 22=Experian, 23=guid_log, 26=Predactiv, 29=Deepsync}; DS26 is the only MM site-visit DDP among them, so dropping it removes 1 of just 3 external hashed-email suppliers.
  - source_line: "`HEMSignalReader` ... **hardcodes DS26 as a delta source** in its inventory {21, 22=Experian, 23=guid_log, 26=Predactiv, 29=Deepsync} ... **Predactiv is the ONLY MM site-visit DDP among the five HEM sources** — dropping DS26 removes one of just three external hashed-email suppliers (with Experian, Deepsync)."

- **[audi_1089_ddp_vendor_evaluations/ds28_33across]** DS28 33Across raw feed is 32 columns; MNTN keeps only 4 (page categories+keywords); geo, device hints, and GPP consent are dropped at site_visit_signal. Raw also carries a device_ids column (GAID/IDFA, pipe-delimited).
  - source_line: "Raw feed is 32 columns; we keep 4 - page categories+keywords, geo, device hints, GPP consent all dropped at site_visit_signal (TI-1027 4.18). Raw also carries a `device_ids` column (GAID/IDFA, pipe-delimited)"

- **[audi_1089_ddp_vendor_evaluations/ds28_33across]** Legacy AWS DAG device_id_33across_signal.py (schedule=None, manual-trigger only) explodes the DS28 raw feed's device_ids into data_archive.device_id_signal (Athena/Redshift); every device_id_signal consumer lives in legacy AWS repos, no GCP-side consumer of DS28 device IDs.
  - source_line: "`device_id_33across_signal.py` - a targeting DAG that explodes the raw feed's `device_ids` into `data_archive.device_id_signal` (Athena/Redshift) - schedule=None, manual-trigger only; every `device_id_signal` consumer found lives in the legacy AWS repos"

- **[audi_1089_ddp_vendor_evaluations/ds28_33across]** 33Across DS28 has the largest sole-IP base of any vendor at 41.68M IPs, but only 99,041 (0.2%) delivered, 97.0% unscored, sole-IP VR 0.026% (at the no-signal baseline, 10x below internal guid_log sole IPs at 0.263%).
  - source_line: "33Across-SOLE IPs (its actual unique contribution): 41.68M IPs - the largest sole-IP base of any vendor - of which only 99,041 delivered (0.2%), 97.0% unscored, 92 IPs at HI"

- **[audi_1089_ddp_vendor_evaluations/ds39_klickly]** DS39 (Klickly) feeds BUK ALS training enrichment (the only non-MM consumer) at source_weight=0.05 and a 5% stratified sample via feature-store site-visit rollups (site_visit_signal_advertiser_id_dsc_id, excludes only DS23); DSID-agnostic, no hard DS39 dependency.
  - source_line: "BUK training enrichment (the ONLY real non-MM consumer): feature-store site-visit rollups (`site_visit_signal_advertiser_id_dsc_id`, excludes only DS23) feed the Bottom-Up Keywords ALS training pipeline at **source_weight=0.05 and a 5% stratified sample** — DSID-agnostic, degrades gracefully; no hard DS39 dependency."

## data_catalog.md

- **[audi_1089_ddp_vendor_evaluations/ds26_predactiv]** DS26 batch ingestion set is ENABLED_DSIDS=[23,25,26,28,30,36]; the ingest reads hourly drops gs://mntn-data-partners/partners/predactiv/dt=YYYYMMDDHH/*.parquet, writes the full payload to gs://mntn-data-archive-prod/fpa_vendor_log/data_source_id=26/ and a thin ip/url/time projection to site_visit_signal (user_agent/query_parameters/advertiser_id nulled).
  - source_line: "DS26 is in `ENABLED_DSIDS=[23,25,26,28,30,36]` ... reads hourly drops `gs://mntn-data-partners/partners/predactiv/dt=YYYYMMDDHH/*.parquet`, then (stage 1) writes the **FULL payload** to `gs://mntn-data-archive-prod/fpa_vendor_log/data_source_id=26/` and (stage 2) a thin projection to `site_visit_signal/data_source_id=26`."

<!-- batch 2 appended -->

## data_catalog.md

- **fact:** The DS30 svs feeder (dsid30_augmentor_log_processing.py) builds site visits from BOTH the page and referrer columns (referrer timestamped 1s earlier), normalizes URLs by prepending http://, requires non-empty ip, and first-touch dedups per (ip, url).
  - source ticket: `audi_1091_augmentor_full_source`
  - source_line: Builds site visits from BOTH `page` and `referrer` (referrer timestamped 1s earlier), normalizes URLs (prepends `http://`), requires non-empty `ip`, first-touch dedup per (ip, url).
- **fact:** The DS23 svs feeder (dsid23_guid_log_processing.py) reads guid_log, left-anti-joins pixel-isolation blocked advertisers, uses URL = product_referer, distinct.
  - source ticket: `audi_1091_augmentor_full_source`
  - source_line: `dsid23_guid_log_processing.py` — guid_log, left-anti-joins pixel-isolation blocked advertisers, URL = `product_referer`, distinct.
- **fact:** The BQ gold DDP table family (dw-main-gold.reporting): ddp_all_matches_cpm[_YYYYMM] (all matched paths, per-category, segment_name + tv_cpm), ddp_mm_winners_imp[_YYYYMM] (MM slice, mm_dsids_winner), ddp_mm_winners_domains[_YYYYMM], _w_select variants; monthly since ~2025-09/10; ddp_mm_winners_imp keyed on ad_served_id.
  - source ticket: `audi_1111_vendor_quality/audi_1115_wtp_cpm`
  - source_line: A full gold table family exists (BQ-migrated): ddp_all_matches_cpm[_YYYYMM]... ddp_mm_winners_imp[_YYYYMM] (MM slice with mm_dsids_winner), ddp_mm_winners_domains[_YYYYMM], _w_select variants; monthly since ~2025-09/10... keyed on ad_served_id.

## data_knowledge.md

- **fact:** DS40 (33Across API) is ~81% of the whole pixel-page-view Kafka topic (89.5K of 110K rows in a sampled hour; ~363M rows/day); dropping it shrinks the pixel-page-view pipeline ~5x.
  - source ticket: `audi_1089_ddp_vendor_evaluations/ds40_33across_api`
  - source_line: 89.5K of 110K rows in a sampled hour approx 81% of the whole pixel topic is DS40 (Klickly, by contrast, ~850 rows). At ~363M rows/day ... dropping it shrinks the pixel-page-view pipeline ~5x.
- **fact:** device_id_33across_signal.py (airflow/dags/targeting/, device-ID/IFA extraction into device_id_signal) is DS28-only (reads fpa_dsid28_log.device_ids, inserts data_source_id=28); DS40 does not feed it.
  - source ticket: `audi_1089_ddp_vendor_evaluations/ds40_33across_api`
  - source_line: airflow/dags/targeting/device_id_33across_signal.py (device-ID/IFA extraction into device_id_signal) is DS28-only (reads fpa_dsid28_log.device_ids, inserts data_source_id=28, schedule=None/manual). DS40 does not feed it.
- **fact:** The DDP monthly usage email ddpmonthlyusageemail-33Across.py covers BOTH ds28 and ds40 in one email to 33across.com contacts, CC accountspayable.
  - source ticket: `audi_1089_ddp_vendor_evaluations/ds40_33across_api`
  - source_line: sends a monthly usage report from partnerbilling@mountain.com to 33across.com contacts ... covering BOTH ds 28 and 40 in one email
- **fact:** AUDI-1091 verdict (2026-07-22): ingesting the full augmentor_log beyond the DS30 BANNER slice already in svs adds only ~+1.2% more site-visit rows and <=+16% more IPs (upper bound, pre-overlap) - NO-GO because the extra VIDEO volume is URL-less CTV.
  - source ticket: `audi_1091_augmentor_full_source`
  - source_line: Net-new site-visit signal is ~1% of rows and <=16% of IPs (upper bound, pre-overlap), versus ~2 sprints of Data Eng ingestion
- **fact:** RTC = two pipelines: guid_log Kafka streaming (near real-time) + TI-run hourly batch over svs-minus-guid; vendors do drive RTC, so per-day analysis understates vendor timing effects
  - source ticket: `audi_1111_vendor_quality`
  - source_line: L54-56: RTC = two pipelines (guid_log Kafka streaming + TI-run HOURLY batch over svs-minus-guid)
- **fact:** svs ingest latency measured with a ULID instrument: free logs stream at 0 min, vendors arrive 2.4-8.6h stale (Predactiv to ~12h), matching configured per-DS lag hours
  - source ticket: `audi_1111_vendor_quality`
  - source_line: L103-105: measured with the new svs ULID latency instrument: free logs stream at 0 min; vendors arrive 2.4-8.6h stale
- **fact:** DS14 availability-gate filter lives at MembershipDB / audience-service level as a global filter, computed at bid time not in IPDSC
  - source ticket: `audi_1111_vendor_quality`
  - source_line: L57-58: DS14 filter lives at MembershipDB / audience-service level as a global filter
- **fact:** Vendor billing is self-reported: MNTN runs targeted_signal compute and tells vendors what is owed, no audit; free-log credit preemption needs no vendor cooperation
  - source ticket: `audi_1111_vendor_quality`
  - source_line: L59-62: Billing is self-reported: we run targeted_signal compute and tell vendors what we owe — no audit
- **fact:** L0f fractional per-won-impression media CPM is ~$10.7 (media_cpm_frac $10.74 ~= media_cpm_elig_full $10.68, CIL join 99.999%, grain-robust); break-even vendor CPM = media CPM x margin = ~$1.0-3.3 for every vendor, essentially vendor-independent because it is just MNTN's CTV media rate.
  - source ticket: `audi_1111_vendor_quality/audi_1115_wtp_cpm`
  - source_line: Per-credited-impression media CPM ~$10.7 is TRUSTWORTHY and grain-robust (CIL join 99.999%, no double-count; media_cpm_frac $10.74 ~ media_cpm_elig_full $10.68 = weights cancel...). So break-even vendor CPM = media CPM x margin = ~$1.0-3.3 for EVERY vendor - because it's just MNTN's CTV media rate, essentially vendor-independent.
- **fact:** l0f is a PRICING lens for the post-preemption residual, NOT a keep/drop test: it over-credits (attributes full impression media incl. impressions we'd win anyway) and would greenlight the current deal; the marginal/drop value is the AUDI-1089 solo cohort (~$60K/mo for 33Across vs l0f's $217K/mo, 3.6x gap, entirely denominator/grain).
  - source ticket: `audi_1111_vendor_quality/audi_1115_wtp_cpm`
  - source_line: CRITICAL CAVEAT (steelman): l0f is a PRICING lens, NOT a keep/drop test. It attributes full impression media (fractionally) to the vendor, valuing impressions we'd win anyway... The marginal/drop value is the AUDI-1089 solo cohort (~$60K/mo for 33Across vs l0f's $217K/mo - 3.6x gap...).
- **fact:** Free-log winners preempt ~88-97% of every vendor's won impressions (33Across 90.5%) at impression grain, higher than the 52.5% visit-day grain because impression volume concentrates on live IPs free logs almost always carry.
  - source ticket: `audi_1111_vendor_quality/audi_1115_wtp_cpm`
  - source_line: ~88-97% of every vendor's won impressions have a free-log winner (33Across 90.5%) - the preemption gap, impression-grain (higher than the 52.5% visit-day grain because impression volume concentrates on live IPs free logs almost always carry).
- **fact:** Free co-hold share per vendor (deck_d1): 33Across 52.5%, 33A API 23.8%, Cybba 28.2%, Justuno 4.9%, Sovrn 0.2% - small vendors' credit is junk/unique not overlap, so preemption barely helps them.
  - source ticket: `audi_1111_vendor_quality/audi_1115_wtp_cpm`
  - source_line: | 33Across | 52.5% | 400.8M | ... | 33A API | 23.8% ... | Sovrn | 0.2% ... | Justuno | 4.9% ... | Cybba | 28.2% ... the other three stay far under (their co-hold is tiny - their credit is junk/unique, not overlap, so preemption barely helps them).
- **fact:** Applying the flow-filter (free log earns credit for an IPxdomain on day D only if it delivered that pair in [D-30, D-1]) drops free-union coverage from 59.36% same-day to 44.09% prior-30d; augmentor alone 38.63%, guid alone 5.83%; vendor flow-unique vs same-day-unique moves both directions.
  - source ticket: `audi_1111_vendor_quality/audi_1115_wtp_cpm`
  - source_line: free-union coverage drops 59.36% (same-day credit) -> 44.09% (prior-30d credit only); augmentor alone 38.63%, guid alone 5.83%. Vendor flow-unique vs same-day-unique moves BOTH directions...
- **fact:** DS14 (MNTN Global Data) is auto-added to every audience expression and restricts bidding to IPs recently seen in guid_log/augmentor_log at the MembershipDB/audience-service level; it explains why ~99% of biddable IPs come from the free logs.
  - source ticket: `audi_1111_vendor_quality/audi_1117_ds14_svs_overlap`
  - source_line: DS14 ("MNTN Global Data") is auto-added to every audience expression and restricts bidding to IPs recently seen in guid_log/augmentor_log — a global filter at MembershipDB / audience-service level (Sean, 2026-07-16 readout). It explains why ~99% of biddable IPs come from the free logs
- **fact:** The documented DS14 gate windows are not a hard universal filter: over all impressions on 2026-07-01 lag distributions decay smoothly with no cliff at 1d/4d/7d; aug(1d) OR guid(4d) covers 85.5% of served IPs and 5.1% appear in neither free log within 11d.
  - source ticket: `audi_1111_vendor_quality/audi_1117_ds14_svs_overlap`
  - source_line: Lag distributions decay smoothly (no cliff at 1d/4d/7d). So DS14 is NOT a hard universal filter at the documented windows across all delivery
- **fact:** Display delivery is a 100.00% same-day augmentor echo by construction (aug_log mirrors the display bid stream), so DS14 gate evidence must come from CTV; CTV-prospecting has a soft edge (12.2% of imps outside aug(1d)|guid(4d), 4.3% outside both logs in 11d).
  - source ticket: `audi_1111_vendor_quality/audi_1117_ds14_svs_overlap`
  - source_line: Display is a same-day echo, not gate evidence: 100.00% of display imps have a SAME-DAY augmentor row... The DS14 gate question is a CTV question, and there the edge is SOFT: 12.2% of CTV-prospecting imps land outside aug(1d)|guid(4d), 4.3% outside both logs entirely (11d).
- **fact:** Of the 30d svs universe of 301.5M IPv4 IPs, only 108.8M (36.1%) are in-gate/biddable under the aug-1d|guid-4d proxy; 192.7M (63.9%) are out-of-gate.
  - source ticket: `audi_1111_vendor_quality/audi_1117_ds14_svs_overlap`
  - source_line: svs 30d universe: 301.5M IPv4 IPs; only 108.8M (36.1%) are in-gate (biddable under the documented aug-1d|guid-4d proxy) — the "what's in svs that's not in DS14" answer: 192.7M IPs (63.9%).
- **fact:** Adding svs IPs to DS14 would split the 192.7M out-of-gate pool almost exactly in half: 97.0M free-stale IPs (free logs delivered them in 30d, needs no vendors) vs 95.7M vendor-only IPs.
  - source ticket: `audi_1111_vendor_quality/audi_1117_ds14_svs_overlap`
  - source_line: expansion_free_stale = 97.0M — out-of-gate IPs the FREE logs delivered within 30d... expansion_vendor_only = 95.7M — only vendors delivered them in 30d
- **fact:** Per-source biddable in-gate share of delivered svs IPs (30d, gate ref 2026-07-01): Cybba 81.6, Klickly 78.5, Sovrn 72.7, augmentor 70.2, Predactiv 66.8, Justuno 60.0, 33A API 59.6, guid 54.8, 5x5 52.2, 33Across 50.0.
  - source ticket: `audi_1111_vendor_quality/audi_1117_ds14_svs_overlap`
  - source_line: Per-source biddable share of delivered IPs (in-gate %): Cybba 81.6, Klickly 78.5, Sovrn 72.7, augmentor 70.2, Predactiv 66.8, Justuno 60.0, 33A API 59.6, guid 54.8, 5x5 52.2, 33Across 50.0
- **fact:** The exact CHAPI graph query for a 30-day advertiser metric runs against summarydata.all_facts_by_day_ramp_combined (daily, ClickHouse Distributed, no FINAL) with a half-open GMT literal predicate day >= timestamp '<30d-ago>' AND day < timestamp '<today 00:00>' (30d ending yesterday, not today()-30); graph.spend and graph.impressions are SUMmed, graph.usersreached is uniqArrayMergeState/uniqArrayMerge over uniques_arr (cross-day distinct merge, not a SUM). aid maps to WHERE advertiser_id IN (...), sum=advertiserinfo.id to GROUP BY advertiser_id.
  - source ticket: `ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill`
  - source_line: Table = `summarydata.all_facts_by_day_ramp_combined` (daily grain, ClickHouse `Distributed`; no `FINAL`). Time column `day`; predicate is **half-open literal GMT timestamps** `day >= timestamp '<30d-ago>' AND day < timestamp '<today 00:00>'` (30d ending yesterday; not `today()-30`).
- **fact:** There is no CHAPI /apidata debug/explain/sql/dryrun param; the literal executed SQL is captured either from the INFO service log 'Built SQL Command' (DataService.kt:143, logged unconditionally per request) or from ClickHouse system.query_log pinned on the table plus advertiser_id IN (aid).
  - source ticket: `ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill`
  - source_line: there is **no** curl/debug param... (1) **easiest — service logs:** every request logs it unconditionally at INFO — `DataService.kt:143` `log.info("Built SQL Command: {} | Params: {}", cmd, ...)` ... (2) **ClickHouse `system.query_log`**
- **fact:** Mixed CTV+display advertisers cannot get cross-channel-deduped served-IP reach via a channel split: WGU CTV-IP 12.79M + display-IP 7.93M = 20.71M summed vs 15.61M distinct = 5.10M (33%) cross-channel overlap (1 in 4 served IPs see both channels). CTV-only advertisers can use a channel_id=8 filter since that leg is already IP-keyed.
  - source ticket: `ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill`
  - source_line: WGU CTV-IP 12.79M + display-IP 7.93M = 20.71M summed vs 15.61M distinct -> **5.10M (33%) cross-channel overlap** (1 in 4 served IPs see both).
- **fact:** WGU IVR reconciliation at denominator 15.61M: all-verified-visitors (graph site_visitors) 1.922M = 12.31%; impression-in-window (visit_facts__base) 1.690M = 10.83%; visiting-AND-served-in-window (CIL intersect) 1.672M = 10.71% (matches the standalone calculator). Exact parity is unreachable from the graph layer because the in-window restriction needs impression_hour/day_number, which live only in ber_stg.visit_facts__base and are grouped away before visit_facts/all_facts.
  - source ticket: `ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill`
  - source_line: A. all verified visitors (graph `site_visitors`) | 1.922M | **12.31%** ... C. visiting-AND-served-in-window (CIL intersect) | 1.672M | **10.71%** | = our standalone 10.70% ... the in-window restriction needs `impression_hour`/`day_number`, which live only in `ber_stg.visit_facts__base`

## experimentation.md
- **fact:** TI-884 MDE calculator self-test anchor: Lewis-Rao two-proportion z-test at p=0.05, N=10k, no variance reduction yields MDE_rel 17.27%.
  - source ticket: `ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis`
  - source_line: Self-tested against Lewis-Rao hand calc (p=0.05, N=10k, no var reduction → MDE_rel=17.27%).
- **fact:** Cross-validation of TI-884 MDE against Lauren's completed tests: GLD (0.67% reported lift vs 3.12% raw / 1.86% stack MDE), Ownerly (0.72% vs 5.92% / 3.53%), Boll & Branch (1.00%, paused/no traffic, 88.4% / 52.6% MDE) all reported lifts 4.7x-8.2x below the MDE, statistically indistinguishable from zero.
  - source ticket: `ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis`
  - source_line: the 3 measurable cases all reported lifts well below detection threshold.

## data_catalog.md
- **fact:** The airflow-ti feature-store output aug_log_ip can substitute for raw bronze.raw.augmentor_log as the biddability filter in Spark lift runs (much smaller, same biddability filter).
  - source ticket: `ber_2250_incrementality_overhaul/ti_933_select_lift_analysis`
  - source_line: substitute the airflow-ti `aug_log_ip` feature-store output for raw `augmentor_log` (much smaller; same biddability filter)

## experimentation.md
- **fact:** MNTN Select drives pooled incremental lift: +2.055 pp visit-rate (95% CI [+2.011, +2.100]) and +0.140 pp conversion-rate (95% CI [+0.133, +0.147]), both significant, over a 7-day holdout window across 23 active Select advertisers (TI-933). Select lift sits between TI-917's all-campaigns (+3.12 pp) and prospecting-only (+0.78 pp) baselines.
  - source ticket: `ber_2250_incrementality_overhaul/ti_933_select_lift_analysis`
  - source_line: Pooled MNTN Select visit-rate lift = +2.055 pp (95% CI [+2.011, +2.100]), conversion-rate lift = +0.140 pp (95% CI [+0.133, +0.147]). Both significant. Select is incremental.

## mntn_business.md
- **fact:** Active MNTN Select advertisers run entirely prospecting/awareness campaigns with zero retargeting; no single Select advertiser has the volume to be individually powered for visit-rate lift, so pooling is required.
  - source ticket: `ber_2250_incrementality_overhaul/ti_933_select_lift_analysis`
  - source_line: zero retargeting campaigns across all 38 - confirms Kale's framing that Select is purely awareness/prospecting

## data_knowledge.md
- **fact:** A bid is gated by three inputs: (1) score (GCS to consumer), (2) segments (MembershipDB), (3) HHST threshold; to safely drop data you need no-threshold AND no-score, OR no-segments.
  - source ticket: `ti_1016_memdb_bidder_cache_optimization`
  - source_line: Three inputs gate a bid: (1) score (GCS→consumer), (2) segments (MembershipDB), (3) HHST threshold. To safely drop data you need: no threshold AND no score, OR no segments. The clean, safe cut is no-segments → don't write intent score.
- **fact:** The membership consumer's intent-score write path does not currently check whether an IP has any segments before writing the score (an IP with no segments still gets a score written); Abbas and Ryan agreed it probably should.
  - source ticket: `ti_1016_memdb_bidder_cache_optimization`
  - source_line: The membership consumer's intent-score write path does not currently check whether the IP has any segments before writing the score. Abbas + Ryan both agreed it "probably should" — an IP with no segments effectively doesn't exist for bidding, so writing a score for it is wasted storage/writes.

## data_catalog.md (batch 5)
- **fact:** The full all-IP MM scoring universe household_scoring.prospecting_intent_daily is ~19.4 TB/day to scan; use delivered household_score in cost_impression_log as the cheap realized-score substitute.
  - source ticket: `ti_1027_5x5_data_evaluation`
  - source_line: Joined each vendor's site-visit IPs to delivered MM household_score (cost_impression_log, 7d; the cheap realized-score source — full scoring universe household_scoring.prospecting_intent_daily is 19.4 TB/day, not scanned).

## Batch 6

### experimentation.md
- **[ti_1044_elevenlabs_ctv_incrementality]** The guid-based total-visit holdout has a cross-device IP-matching limitation: it matches the CTV-impression IP (TV/home router) to the web-visit IP (phone/laptop), so cross-device/cellular/away visits have a different IP and are missed, undercounting the served arm's visits and biasing measured visit lift DOWNWARD (asymmetric, not symmetric loss); the device-agnostic geo test is cleaner and the fix is to rebuild the total-visit holdout with household/identity-graph matching (IP->household->all visits).
  - source_line: The guid join matches the CTV-impression IP (TV/home router) to the web-visit IP (phone/laptop); cross-device / cellular / away visits have a different IP -> missed, so the absolute visit rate (2.83%) is an undercount, and (unlike a symmetric loss) it preferentially drops ad-induced cross-device visits from the served arm -> can bias the measured visit lift downward. ... Refinement: rebuild the total-visit holdout with household/identity-graph matching (IP->household->all visits) to remove the cross-device undercount.
- **[ti_1044_elevenlabs_ctv_incrementality]** A ghost-win simulation forming the served-counterfactual by sampling ghost bids at the per-bid win rate w=0.27 (10.96M imps / 40.79M real bids) moved conversions +32%->+26% and visits +35%->+33%; the frequency correction is only ~2-6pp, showing the served-vs-ghost ATT bias is value-selection (winning impressions for the households bid highest, who visit/convert anyway), not frequency, and uniform win-rate sampling cannot remove value-selection (only randomized ITT / IV-TOT do, both ~0).
  - source_line: Ghost-win ATT - simulated ghost wins by sampling ghost bids at the per-bid win rate w=0.27 (10.96M imps / 40.79M real bids), frequency-weighting the control: visits +35%->+33%, conversions +32%->+26%. The frequency correction is small (~2-6pp) => the ATT bias is value-selection, not frequency (we win impressions for the households we bid highest on, who visit/convert anyway).

### data_knowledge.md
- **[ti_1058_ds13_ds19_pipeline_map]** The DS13 vertical leg is a separate OpenAI batch from DS19 and its code is located at airflow-ti/spark/vertical_classification/{distinct_site_visit_signal_domains,prepare_html_content,submit_html_content,fetch_vertical_response,update_website_verticals}.py, airflow-ti/dags/targeting/fetch_common_crawl.py, airflow-ti/dags/vertical_classification/*, and SteelHouse/dbt ml_squad/models/vertical_categorization/*.
  - source_line: DS13-leg code located (§4): airflow-ti/spark/vertical_classification/{...}.py, airflow-ti/dags/targeting/fetch_common_crawl.py, airflow-ti/dags/vertical_classification/*, and SteelHouse/dbt ml_squad/models/vertical_categorization/*. It is a separate OpenAI batch from DS19.


<!-- batch 7 appended -->

## data_catalog.md
- **fact:** tpa.membership_updates_logs (Greenplum) tracks IP audience membership timestamps; its freshness column is update_time and the sync should run daily, so staleness is detected by comparing max(update_time) against expected daily cadence.
  - source ticket: `ti_34_identity_sync_freshness`
  - source_line: "- `tpa.membership_updates_logs`: key freshness column is `update_time`; sync should run daily."
