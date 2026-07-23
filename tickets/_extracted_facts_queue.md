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
