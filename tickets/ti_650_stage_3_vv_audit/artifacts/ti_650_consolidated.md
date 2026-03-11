# Stage 3 Verified Visit Audit

**Targeting Infrastructure**

Updated 2026-03-06 | **ZACH REVIEW COMPLETE** (3 meetings) | v4 stage-aware production table (`audit.vv_ip_lineage`) designed — ready for deployment | Gap analysis complete | Full silver scale run COMPLETE — 3.25M rows, matches GP within 0.12pp | BQ silver = validated GP replacement | Mutation range 1.2–33.4% across 15 advertisers | Stage-aware: vv_stage, pv_stage, s1 chain traversal | 20% of S1 VVs on S3 IPs (attribution != journey stage) | All 10 chain traversal permutations validated

---

## 1. Introduction

New-to-brand ("NTB") is a targeting concept in which each ad is delivered to a household (identified by IP address) that has not had a purchase or page view within a defined lookback window. In a previous analysis, MNTN's NTB targeting was found to be 99.99% accurate when the campaign configurations that restrict delivery to new users are enabled.

However, from the advertiser's perspective, this accuracy may not be apparent. Because IP addresses can change throughout the ad-serving lifecycle, the IP associated with a verified visit may differ from the IP that was originally bid on. If that new IP had a prior purchase, it creates the illusion that MNTN served an ad to a non-NTB household, even though the original bid was correctly placed against an NTB-eligible IP.

This is fundamentally an identity resolution problem. The current approach is to build an independent audit system that traces any Stage 3 verified visit backward through the pipeline to its originating bid IP, using Zach Schoenberger's independent traversal method. This enables NTB validation, first-touch attribution, and general auditability without relying on stored system references.

---

## 2. Stage Definitions

The MNTN ad-serving pipeline operates in three **campaign targeting stages**. Stages are not just event types — they are separate campaigns that target different IP audiences based on prior event history.

| Stage | Segment audience | What populates it | Source |
|-------|-----------------|-------------------|--------|
| Stage 1 | Initial audience (e.g., 8.5M IPs from customer data/lookalike) | Campaign setup | — |
| Stage 2 | Stage 1 VAST Impression IPs | `event_log.ip` from Stage 1 impressions | Green line in MES diagram |
| Stage 3 | **IPs that had a verified visit** (from any stage's impression) | IP enters Stage 3 when a VV occurs — retargeting audience | Green line in MES diagram |

**The green line rule (MES Pipeline diagram):** Stage N Vast Impression IP → Stage N+1 Segment IP. The VAST Impression event is marked "Used For Targeting" (pink) — that IP feeds the next stage. All other events (Bid, Serve, Win, Vast Start) are "Not Really Used Directly For Targeting" (beige).

**Stage 2 confirmed from Zach (2026-03-03):** Stage 2 is populated ONLY from Stage 1 VAST IPs. *"It is not the IPs from the vast impression from stage two or stage three. It's just stage one."*

**Stage 3 = IPs that had a verified visit (Zach, 2026-03-04).** The VV can be attributed to a Stage 1 or Stage 2 impression — attribution doesn't follow the stage sequence. Two paths: (1) Stage 1 impression → VV → Stage 3, or (2) Stage 1 → Stage 2 impression → VV → Stage 3. Stage 3 is a retargeting audience — users with demonstrated intent.

**Blue lines (MES diagram) = attribution, NOT segment population.** Zach (2026-03-03): *"blue lines are vv."* They point backward in time from VV to impressions. `first_touch_ad_served_id` always points to a Stage 1 impression (by definition: `funnel_level=1`). `ad_served_id` (last touch) can point to Stage 1, 2, or 3. Green lines = targeting (forward). Blue lines = attribution (backward).

All IPs in Stages 2 and 3 trace back to a Stage 1 bid. The IP can mutate as it moves through stages, but the lineage always begins at a Stage 1 bid.

### 2.1 MES Pipeline Architecture (from official MES Pipeline PDF)

The verified visit is a **3-stage multi-event sequence**, not a single event chain. Each stage has up to 30 days between them.

```
MES PIPELINE: 3-STAGE VERIFIED VISIT MODEL
═══════════════════════════════════════════

  STAGE 1                    STAGE 2 (within 30 days)      STAGE 3 (within 30 days)         ATTRIBUTION
  ═══════════════            ════════════════════════       ════════════════════════          ═══════════

  ┌──────────────┐           ┌──────────────┐              ┌──────────────┐
  │ Segment      │──green──▶ │ Segment      │──green────▶  │ Segment      │
  │ (target IP)  │  (Vast    │ (target IP)  │  (Vast       │ (target IP)  │
  │ [GREEN]      │   Imp IP) │ [GREEN]      │   Imp IP)    │ [GREEN]      │
  └──────┬───────┘           └──────┬───────┘              └──────┬───────┘
         ▼                          ▼                             ▼
  ┌──────────────┐           ┌──────────────┐              ┌──────────────┐
  │ Bid          │           │ Bid          │              │ Bid          │─ ─ ─ ─ ─bid_ip─ ─ ─ ─▶ ┌────────────┐
  │ [BEIGE]      │           │ [BEIGE]      │              │ [BEIGE]      │                        │ Stage 3 VV │
  └──────┬───────┘           └──────┬───────┘              └──────┬───────┘  ┌─ad_served_id──────▶ │            │
         ▼                          ▼                             ▼          │                     │ Has:       │
  ┌──────────────┐           ┌──────────────┐              ┌──────────────┐  │                     │  ad_served │
  │ Serve        │           │ Serve        │              │ Serve        │──┤                     │  bid_ip    │
  │ [BEIGE]      │           │ [BEIGE]      │              │ [BEIGE]      │  │                     │  visit_ip  │
  └──────┬───────┘           └──────┬───────┘              └──────┬───────┘  │                     └────────────┘
         ▼ auction_id               ▼ auction_id                  ▼          │
  ┌──────────────┐           ┌──────────────┐              ┌──────────────┐  │
  │ Win          │           │ Win          │              │ Win          │  │
  │ [BEIGE]      │           │ [BEIGE]      │              │ [BEIGE]      │  │
  └──────┬───────┘           └──────┬───────┘              └──────┬───────┘  │
         ▼ ad_served_id             ▼ ad_served_id                ▼          │
  ┌──────────────┐           ┌──────────────┐              ┌──────────────┐  │
  │ Vast         │───green──▶│ Vast         │───green────▶ │ Vast         │──┤ ad_served_id
  │ Impression   │  (feeds   │ Impression   │  (feeds      │ Impression   │  │
  │ [PINK]       │   next    │ [PINK]       │   next       │ [PINK]       │  │
  │ TARGETING IP │   stage)  │ TARGETING IP │   stage)     │ TARGETING IP │  │
  └──────┬───────┘           └──────┬───────┘              └──────┬───────┘  │
         ▼ ad_served_id             ▼ ad_served_id                ▼          │
  ┌──────────────┐           ┌──────────────┐              ┌──────────────┐  │
  │ Vast Start   │           │ Vast Start   │              │ Vast Start   │──┘ ad_served_id
  │ [BEIGE]      │           │ [BEIGE]      │              │ [BEIGE]      │
  └──────────────┘           └──────────────┘              └──────────────┘

  COLOR KEY:
  [GREEN] = "Is the Targeted IP" — the IP that was targeted for this stage
  [PINK]  = "Used For Targeting" — feeds the NEXT stage's segment IP
  [BEIGE] = "Not Really Used Directly For Targeting"

  STAGE LINKING:
  Green lines = Vast Impression IP at Stage N → Segment IP at Stage N+1
  Blue lines  = VV path (Stage 3 Vast Start → Stage 3 VV via ad_served_id)
```

**Key implications:** Our trace covers Stage 3's internal pipeline only (Bid → Serve → Win → Vast Impression → Vast Start → VV). IP mutation between stages is EXPECTED (up to 30 days between stages). IP mutation WITHIN a stage is the interesting finding.

---

## 3. Pipeline Overview: How IPs Mutate

Within a single ad serve, the IP address can change at each event in the pipeline. The trace uses 5 IP checkpoints:

| # | Checkpoint | Table | IP Column | Event |
|---|------------|-------|-----------|-------|
| 1 | Win IP | logdata.win\_log | ip (inet /32) | Bid/auction win |
| 2 | CIL IP | logdata.cost\_impression\_log | ip (text) | Impression/ad serve |
| 3 | EL IP | logdata.event\_log | ip (inet /32) | VAST playback on CTV |
| 4 | CP IP | logdata.clickpass\_log | ip (inet /32) | Redirect to advertiser site |
| 5 | Visit IP | summarydata.ui\_visits | ip (inet /32) | Page view (verified visit) |

IP stability between checkpoints (single-advertiser reference: advertiser 37775, Feb 10, n = 28,838 full chain):

| Hop | Stability | Finding |
|-----|-----------|---------|
| Win → CIL | 100% | Rock solid — bid and serve always match |
| CIL → EL | 96.2% | Minor drift during VAST playback |
| **EL → Clickpass** | **87.3%** | **ALL MUTATION HAPPENS HERE** |
| Clickpass → Visit | 99.99% | Functionally identical — same browser session |

**Scale confirmation (10 advertisers, Feb 4–10, n = 1,440,945 full chain):** Win = CIL = 100% for all 10. CP = Visit = 99.93%+ for all 10. Mutation range 5.9%–20.8% in sample, all at redirect, zero at visit. **Gap analysis (5 additional advertisers, Feb 17–23): wider range 1.2%–33.4%. Mutation location invariant confirmed.**

---

## 4. The Problem

IP mutation across the pipeline creates three distinct issues:

**NTB Bleed:** When a verified visit is generated, the IP may differ from the bid IP. If the new IP had a prior purchase, it appears MNTN targeted a non-NTB household. The analysis revealed 4,006 phantom NTB events in a single day for one advertiser — returning visitors whose IP changed, causing new-to-brand misclassification. At scale (10 advertisers, 7 days), phantom NTB totals ~28,200/day.

**NTB Classification Instability:** clickpass\_log.is\_new and ui\_visits.is\_new disagree 41–56% of the time across advertisers. NTB classification depends on when in the pipeline it is evaluated and against which reference data. This is a systemic issue, not edge-case noise. NTB validation via conversion\_log confirms 97.8% of VVs flagged NTB have zero prior conversions within 30 days — meaning conversion\_log is NOT the reference source for is\_new, and the disagreement is driven by a different (as yet unidentified) lookup table.

**First-Touch Attribution Gaps:** A non-zero number of verified visits cannot be attributed to a first-touch CTV impression. The IP trail is lost in the pipeline.

**General Auditability:** There is no mechanism to answer "How did this verified visit IP get here?" The trace system addresses this.

---

## 5. Previous Fix Attempt: Forcing Bid IP

A straightforward solution was previously implemented: use only the bid IP across the entire pipeline. After rollout, campaign performance dropped significantly due to sensitivity of the VV system to shared/multi-household IP dynamics. The change was rolled back.

---

## 6. Trace Methodology

### 6.1 Approach

Rather than forcing a single IP, we build a backward-tracing audit. The independent traversal method does NOT rely on first\_touch\_ad\_served\_id. It independently reconstructs the chain by joining through pipeline tables using ad\_served\_id and impression\_id as deterministic keys.

### 6.2 coredw (Greenplum) — Full Table + Column Pipeline

The trace chain from Stage 3 back to Stage 1, with every table, column, and join key:

**CTE 1: logdata.clickpass\_log (STARTING POINT — Stage 3 redirect)**

| Column | Type | Role |
|--------|------|------|
| ad\_served\_id | text (UUID) | Primary join key to CIL, EL, and ui\_visits |
| ip | inet (/32) | IP checkpoint 4 — redirect IP (strip /32) |
| ip\_raw | text | Clean text IP, no stripping needed |
| is\_new | boolean | NTB classification at redirect time |
| is\_cross\_device | boolean | Cross-device flag — mutation driver |
| first\_touch\_ad\_served\_id | text | System's attribution (for validation) |
| impression\_time | timestamp | Anchor for 30-day lookback |
| campaign\_id | bigint | Campaign breakdown |
| page\_view\_guid | text | Bridge to conversion\_log |
| viewable | boolean | Viewability flag |

**CTE 2: summarydata.ui\_visits (ENRICHMENT ONLY — Stage 3 VV)**

LEFT JOIN on v.ad\_served\_id::text = cp.ad\_served\_id. Filter: from\_verified\_impression = true. Trace works without this table.

| Column | Type | Role |
|--------|------|------|
| ad\_served\_id | uuid | Cast ::text to join with clickpass |
| ip | inet (/32) | IP checkpoint 5 — page view IP |
| is\_new | boolean | NTB classification at visit time (42% disagrees with clickpass) |
| from\_verified\_impression | boolean | Filter for verified visits only |

**CTE 3: logdata.cost\_impression\_log (Stage 1 — impression/serve)**

LEFT JOIN on cil.ad\_served\_id = cp.ad\_served\_id (text = text). Filter: advertiser\_id + 30-day lookback.

| Column | Type | Role |
|--------|------|------|
| ad\_served\_id | text (UUID) | Join key FROM clickpass |
| impression\_id | text (dotted steelhouse) | Join key TO win\_log (= auction\_id) |
| ip | text | IP checkpoint 2 — serve IP (already clean text) |

**CTE 4: logdata.win\_log (VIEW) (Stage 1 — bid/win)**

LEFT JOIN on w.auction\_id = cil.impression\_id (dotted steelhouse). NO advertiser\_id filter — win\_log uses Beeswax IDs, not MNTN.

| Column | Type | Role |
|--------|------|------|
| auction\_id | text (dotted steelhouse) | Join key FROM CIL's impression\_id |
| ip | inet (/32) | IP checkpoint 1 — bid/win IP (strip /32) |
| device\_ip | inet | ALWAYS NULL — do not use |

**CTE 5: logdata.event\_log (VAST playback — between Stage 1 and 3)**

LEFT JOIN on el.ad\_served\_id = cp.ad\_served\_id (text). Filter: event\_type\_raw = 'vast\_impression'. Dedup: DISTINCT ON (ad\_served\_id) ORDER BY time.

| Column | Type | Role |
|--------|------|------|
| ad\_served\_id | text (UUID) | Join key FROM clickpass |
| ip | inet (/32) | IP checkpoint 3 — VAST playback IP (strip /32) |
| bid\_ip | text | Original bid IP (for future validation) |
| event\_type\_raw | text | Filter to 'vast\_impression' |

**Supplementary: logdata.conversion\_log (NTB lookback — NOT in trace chain)**

Not joinable by ad\_served\_id (doesn't have it). Reachable via clickpass.page\_view\_guid = conversion\_log.guid (validated: 14.3M rows).

| Column | Type | Role |
|--------|------|------|
| guid | text | Join key FROM clickpass.page\_view\_guid |
| ip / ip\_raw | inet / text | Conversion IP |
| original\_ip | text | Preserved original IP (pre-mutation?) |
| conversion\_type | text | Conversion category (NULL for advertiser 37775) |

### 6.3 Join Key Summary

| From → To | Join Expression | Format |
|-----------|-----------------|--------|
| clickpass → CIL | cp.ad\_served\_id = cil.ad\_served\_id | UUID text = text |
| clickpass → EL | cp.ad\_served\_id = el.ad\_served\_id | UUID text = text |
| clickpass → ui\_visits | cp.ad\_served\_id = v.ad\_served\_id::text | text = uuid (cast) |
| CIL → win\_log | cil.impression\_id = w.auction\_id | Dotted steelhouse |
| clickpass → conv\_log | cp.page\_view\_guid = cl.guid | GUID text = text |

### 6.4 Mutation Classification

| Classification | Definition | Result (37775) |
|----------------|------------|----------------|
| All 5 stable | win = CIL = EL = CP = visit | 84.4% |
| Mutated at clickpass | Pipeline stable (win=CIL=EL) but EL ≠ CP | 11.8% |
| Mutated at visit | Pipeline+CP stable but CP ≠ visit | 0% |
| Mutated at both | EL ≠ CP AND CP ≠ visit | 0% |

### 6.5 Simplified Trace (event\_log.bid\_ip Discovery — 2026-02-25)

`logdata.event_log.bid_ip` (text) = `logdata.win_log.ip` at **100%** (30,502/30,502 rows, zero mismatches). This eliminates the CIL→win\_log dependency entirely.

**Simplified trace chain (2 joins instead of 4):**

```
clickpass_log.ad_served_id = event_log.ad_served_id (UUID text)
→ event_log.bid_ip = bid/win IP (100% match to win_log.ip)
→ event_log.ip = VAST playback IP
→ clickpass_log.ip = redirect IP
→ ui_visits.ip = visit IP (optional enrichment)
```

**Chronological event flow:**

```
STAGE 1 (Bid/Win/Serve)        STAGE 1-2 (Playback)       STAGE 3 (Conversion)
========================        ====================       ====================

 ┌─────────────┐   ┌──────────────────┐   ┌──────────────┐   ┌───────────────┐   ┌────────────┐
 │  WIN LOG    │──▶│ COST IMPRESSION  │──▶│  EVENT LOG   │──▶│ CLICKPASS LOG │──▶│ UI VISITS  │
 │  (bid/win)  │   │  LOG (serve)     │   │  (VAST play) │   │  (redirect)   │   │ (page view)│
 └─────────────┘   └──────────────────┘   └──────────────┘   └───────────────┘   └────────────┘
       │                   │                     │                   │                   │
   win_log.ip          cil.ip            event_log.ip /       clickpass.ip        ui_visits.ip
   (inet /32)          (text)            event_log.bid_ip     (inet /32)          (inet /32)
       │                   │              (inet / text)             │                   │
       ▼                   ▼                     ▼                  ▼                   ▼
   IP CHECK 1          IP CHECK 2           IP CHECK 3          IP CHECK 4          IP CHECK 5

   100% stable ────────────────────▶  96.2% ──▶  87.3%  ──▶  99.99%
   (win = CIL = bid_ip = 100%)                  ALL MUTATION    (CP = visit)
                                                HAPPENS HERE
```

The original 4-table chain (`clickpass → CIL → win_log → event_log`) is still valid, but `event_log.bid_ip` gives us the bid IP directly. This also unblocks the BQ port (CIL is stopped in BQ but no longer needed).

### 6.6 NTB Validation Layer (Completed — conversion\_log pathway)

For each VV flagged is\_new = true, check bid\_ip and visit\_ip against conversion\_log with a 30-day lookback window prior to clickpass time. conversion\_log is reachable via the page\_view\_guid → guid bridge (100% of clickpass rows have page\_view\_guid; 15.4% match to conversion\_log). **Result: 97.8% of NTB VVs have zero prior conversions. Conversion\_log is NOT the is\_new reference source.** The conversion pathway catches only 1.87% of NTB VVs as false positives. The is\_new disagreement is driven by a different, unidentified reference table.

---

## 7. Data Environment

### 7.1 GCP (BigQuery) Pipeline Status

| Role | coredw (Greenplum) | BQ Bronze (raw) | BQ Silver (logdata) | Status | Simplified Trace |
|------|-------------------|-----------------|---------------------|--------|-----------------|
| Clickpass (start) | logdata.clickpass\_log | dw-main-bronze.raw.clickpass\_log (~25% subset) | **dw-main-silver.logdata.clickpass\_log (complete, = GP)** | OK | **USE SILVER** |
| VAST Events | logdata.event\_log | dw-main-bronze.raw.event\_log | **dw-main-silver.logdata.event\_log (has bid\_ip)** | OK | **USE SILVER** |
| Impressions | logdata.cost\_impression\_log | dw-main-bronze.raw.cost\_impression\_log (STOPPED Jan 31) | dw-main-silver.logdata.cost\_impression\_log (VIEW) | Silver OK | BWN bridge only |
| Wins | logdata.win\_log (VIEW) | dw-main-bronze.raw.bidder\_win\_notifications | dw-main-silver.logdata.win\_logs (VIEW) | OK | Independent validation |
| Visits | summarydata.ui\_visits | dw-main-bronze.raw.visits (STOPPED Jan 31) | **dw-main-silver.summarydata.ui\_visits** | OK | **VV enrichment (visit\_ip, vv\_is\_new, impression\_ip)** |
| Conversions | logdata.conversion\_log | dw-main-bronze.raw.conversion\_log | dw-main-silver.logdata.conversion\_log (VIEW) | OK | NTB validation only |

**Silver layer (dw-main-silver.logdata):** 23 tables (all VIEWs except spend\_log\_tmp). Created Feb 2026. Confirmed to have complete data matching GP volumes. **Silver is the correct BQ source for the audit.**

### 7.2 BQ Port Status: RESOLVED — Silver Layer = Complete Data

**VOLUME GAP RESOLVED (2026-02-25):** The ~25% volume gap was a **bronze-only problem**. BQ `dw-main-silver.logdata.clickpass_log` has complete data matching GP:

| Date | Silver Rows | Bronze Rows | GP Rows (ref) | Silver/GP |
|------|-------------|-------------|---------------|-----------|
| Feb 4 | 32,576 | 8,732 | ~32K | ~100% |
| Feb 5 | 32,712 | 8,415 | ~32K | ~100% |
| Feb 6 | 31,096 | 7,698 | ~31K | ~100% |
| Feb 7 | 30,130 | 7,490 | ~30K | ~100% |
| Feb 8 | 30,666 | 7,409 | ~30K | ~100% |
| Feb 9 | 30,147 | 7,750 | ~30K | ~100% |
| Feb 10 | 32,286 | 8,360 | 32,364 | 99.8% |

**Root cause:** Bronze `raw.clickpass_log` applies an upstream filter (not `blocked_source` — that column is 100% NULL). Silver layer VIEWs provide the complete dataset. Bronze is a ~25% non-random subset enriched for cross-device traffic.

**Silver trace validated (advertiser 37775, Feb 10):**

| Metric | Silver BQ | GP | Delta |
|--------|-----------|-----|-------|
| Total clickpass | 32,286 | 32,364 | -0.2% |
| EL matched | 99.97% | 99.97% | 0 |
| bid\_ip = el\_ip | 96.13% | 96.2% | -0.1pp |
| Mutated at redirect | **15.06%** | **11.8%** | **+3.3pp** |
| Cross-device % | 47.15% | 47.2% | 0 |
| CP NTB % | 51.19% | 51.2% | 0 |

**Mutation offset (+2.7 to +4.6pp):** Seen on single-day runs with 20-day EL lookback. **RESOLVED** — caused by insufficient EL lookback, not inet vs STRING. With 30-day lookback, BQ matches GP within 0.12pp (see Section 8.15).

**Silver 7-day trace (advertiser 37775, 20-day EL lookback):**

| Date | CP Rows | EL Match | Mutated | Cross-Device | NTB |
|------|---------|----------|---------|--------------|-----|
| Feb 4 | 32,576 | 91.88% | 16.32% | 47.74% | 53.3% |
| Feb 5 | 32,712 | 93.45% | 16.04% | 47.93% | 53.3% |
| Feb 6 | 31,096 | 95.25% | 16.04% | 47.33% | 53.3% |
| Feb 7 | 30,130 | 96.50% | 14.99% | 45.49% | 54.3% |
| Feb 8 | 30,666 | 97.91% | 13.76% | 44.14% | 55.7% |
| Feb 9 | 30,147 | 99.37% | 15.25% | 46.81% | 52.6% |
| Feb 10 | 32,286 | 99.97% | 15.06% | 47.15% | 51.2% |

EL match improves as dates approach present (wider EL coverage). Feb 10 = 99.97%. Earlier dates need 30-day EL lookback for full coverage. Mutation stable at 13.8–16.3% across all days.

**BQ implementation note:** Silver clickpass\_log does NOT have `dt` (DATE) column. Filter on `DATE(time)` instead.

**Previous bronze-only trace (preserved for reference):** 8,360 bronze clickpass → 8,356 EL matched (99.95%). Higher mutation (16.5%), higher cross-device (59.4%), lower NTB (39.2%) — all explained by the non-random bronze subset.

**GCS parquet alternative (confirmed Greg Spiegelberg 2026-02-25):** `gs://mntn-analytics-curated/coredw/summarydata/visits` and `/conversions` with 3-day lookback refresh every ~3hrs after LDS. Could create BQ external table for visits enrichment.

### 7.3 BQ Schemas Mapped (2026-02-25)

**BQ raw.event\_log IP columns:** `bid_ip` (STRING), `ip` (STRING), `original_ip` (STRING), `ip_raw` (STRING) — all STRING, no /32 suffix. Matches GP schema.

**BQ raw.clickpass\_log:** `ip` (STRING, no /32), `ad_served_id` (STRING), `is_new` (BOOL), `is_cross_device` (BOOL), `campaign_id` (INT64), `impression_time` (TIMESTAMP), `page_view_guid` (STRING), `first_touch_ad_served_id` (STRING), `viewable` (BOOL), `blocked_source` (STRING), `publisher` (STRING), `app_bundle` (STRING), `dt` (DATE), `hh` (STRING), `epoch` (INT64). No `ip_raw`.

**BQ raw.bidder\_win\_notifications:** `device_ip` (STRING — **100% populated, = bid\_ip at 100%**), `auction_id` (STRING, UUID with \_xdc\_N suffix), `impression_id` (STRING, small integers — NOT a UUID), `mntn_auction_id` (STRING, steelhouse format — **THIS is the CIL join key**), `auction_timestamp`/`impression_timestamp`/`notification_timestamp` (INT64, **nanoseconds** not milliseconds — use `TIMESTAMP_MICROS(CAST(ts / 1000 AS INT64))`), `advertiser_id` (INT64, MNTN ID), `beeswax_advertiser_id` (INT64), `inventory_source` (STRING), `device_type` (INT64), `placement_type` (STRING), `env` (STRING).

**BQ silver.cost\_impression\_log:** `ad_served_id` (STRING, UUID — sometimes NULL), `impression_id` (STRING, steelhouse format), `ip` (STRING), `is_new` (BOOL), `device_type` (STRING), `supply_vendor` (STRING), `publisher_type_id` (INT64). **Bridge table:** EL.ad\_served\_id → CIL.ad\_served\_id, then CIL.impression\_id → BWN.mntn\_auction\_id.

**BWN ID format reference (verified 2026-02-25):**

| Field | Format | Example | Matches |
|-------|--------|---------|---------|
| EL.ad\_served\_id | UUID | b9cb6603-0804-4373-8d68-30e9d33c83f2 | CIL.ad\_served\_id, CP.ad\_served\_id |
| CIL.impression\_id | Steelhouse | 1770684616749257.3333240021.92.steelhouse | BWN.mntn\_auction\_id |
| BWN.impression\_id | Integer | 11, 1, 17 | Nothing useful — NOT a join key |
| BWN.auction\_id | UUID+suffix | c66c40f6-...-cf30d33264f6\_xdc\_11 | Not directly matched |
| BWN.mntn\_auction\_id | Steelhouse | 1770763565560572.3514799585.74.steelhouse | CIL.impression\_id |

### 7.4 GCP Port Requirements

1. ~~Verify date ranges for raw.clickpass\_log and raw.conversion\_log~~ — **DONE** (both flowing).
2. ~~Verify BQ event\_log has bid\_ip~~ — **DONE** (confirmed: bid\_ip STRING).
3. ~~Adapt IP handling (BQ STRING, no /32 suffix)~~ — **DONE** (BQ query written).
4. ~~Replace DISTINCT ON with ROW\_NUMBER()~~ — **DONE** (BQ query written).
5. ~~Replace ::text casts with CAST()~~ — **DONE** (not needed — BQ types are already STRING).
6. ~~Investigate BQ clickpass volume gap~~ — **RESOLVED.** Bronze = ~25% subset. Silver = complete data matching GP.
7. ~~Verify BWN device\_ip is populated~~ — **DONE.** 100% populated (891,953/891,953). Matches EL.bid\_ip at 100%.
8. raw.visits must resume OR use GCS parquet external table (for VV enrichment — optional).
9. raw.cost\_impression\_log — no longer needed for simplified trace. Silver CIL available for BWN bridge.

### 7.5 GCP Syntax Translation

| coredw Pattern | GCP Equivalent |
|---------------|----------------|
| logdata.clickpass\_log | dw-main-bronze.raw.clickpass\_log |
| logdata.cost\_impression\_log | dw-main-bronze.raw.cost\_impression\_log |
| logdata.win\_log | dw-main-bronze.raw.bidder\_win\_notifications |
| logdata.event\_log | dw-main-bronze.raw.event\_log |
| summarydata.ui\_visits | dw-main-bronze.raw.visits (or GCS parquet external table) |
| win\_log.ip | device\_ip (STRING — **100% populated, confirmed**) |
| win\_log.time | TIMESTAMP\_MICROS(CAST(auction\_timestamp / 1000 AS INT64)) (**nanoseconds**, not ms) |
| replace(ip::text, '/32', '') | Likely just ip (STRING) — verify |
| DISTINCT ON (col) ORDER BY time | ROW\_NUMBER() OVER (PARTITION BY col ORDER BY time) = 1 |
| v.ad\_served\_id::text | CAST(v.ad\_served\_id AS STRING) |

### 7.6 Other Tables Investigated

| Table | ad\_served\_id? | Verdict |
|-------|----------------|---------|
| logdata.conversion\_log | No | No chain join key. NTB lookback only. Reachable via page\_view\_guid bridge. |
| logdata.ext\_visits | N/A | **CLOSED** — Write-only foreign table. Exports summarydata.visits to GCS (`gs://mntn-analytics-curated/coredw/summarydata/visits`) as parquet. Not readable or queryable. |
| logdata.ext\_conversions | N/A | **CLOSED** — Write-only foreign table. Exports summarydata.conversions to GCS as parquet. Not readable or queryable. |
| summarydata.last\_tv\_touch\_visits | Yes | **CLOSED** — Fully explored. impression\_ip = ui\_visits.impression\_ip (100%, same source, NOT bid IP). last\_touch\_ip = clickpass.ip (100%, visit IP). Subset of VVs (~61%). NOT a shortcut for bid IP. |

### 7.7 ID Format Reference

| Format | Example | Used In |
|--------|---------|---------|
| UUID | 49726905-7dfb-4458-a68e-ce2198935739 | clickpass, ui\_visits, CIL, EL (ad\_served\_id) |
| Dotted Steelhouse | 1771199881327973.2433458756.97.steelhouse | CIL impression\_id, EL td\_impression\_id, win\_log auction\_id |

Notes: win\_log uses Beeswax advertiser IDs (not MNTN). clickpass ad\_served\_id is text, ui\_visits ad\_served\_id is uuid — cast with ::text on ui\_visits side.

---

## 8. Results

### 8.1 Main Aggregation — Advertiser 37775, Feb 10

32,364 clickpass events | 32,233 with VV (99.6%) | 28,838 full chain (89.1%)

**IP Stability (full chain, n = 28,838):**

| Comparison | Count | Rate |
|------------|-------|------|
| win = CIL | 28,838 | 100% |
| CIL = EL | 27,754 | 96.2% |
| **EL = clickpass** | **25,173** | **87.3%** |
| clickpass = visit | 28,836 | 99.99% |
| All 5 stable | 24,347 | 84.4% |

**Mutation Location:**

| Where | Count | Rate |
|-------|-------|------|
| **Mutated at clickpass (EL → CP)** | **3,407** | **11.8%** |
| Mutated at visit (CP → visit) | 0 | 0% |
| Mutated at both | 0 | 0% |

### 8.2 Per-Campaign Breakdown

24 campaigns. Original NTB campaigns (311968, 311900, 311974) show < 1% mutation. Newer 443xxx/450xxx campaigns show 12–20%.

| Campaign | Total | Full Chain | Mutated at CP | Mutation % |
|----------|-------|------------|---------------|------------|
| 311968 | 4,189 | 3,578 | 29 | 0.8% |
| 450305 | 3,152 | 2,905 | 411 | 14.1% |
| 450323 | 3,069 | 2,780 | 423 | 15.2% |
| 443848 | 2,247 | 2,083 | 329 | 15.8% |
| 311900 | 2,260 | 1,728 | 15 | 0.9% |
| 443844 | 1,976 | 1,826 | 240 | 13.1% |
| 443866 | 1,965 | 1,813 | 320 | 17.7% |
| 443862 | 1,933 | 1,771 | 221 | 12.5% |
| 443816 | 1,640 | 1,504 | 240 | 16.0% |
| 443815 | 1,499 | 1,369 | 192 | 14.0% |
| 443814 | 982 | 869 | 179 | 20.6% |
| 311966 | 928 | 861 | 127 | 14.7% |
| 450300 | 905 | 834 | 126 | 15.1% |
| 450301 | 866 | 806 | 110 | 13.6% |
| 450319 | 859 | 798 | 122 | 15.3% |
| 450318 | 846 | 795 | 114 | 14.3% |
| 311974 | 939 | 661 | 4 | 0.6% |
| 311897 | 587 | 541 | 86 | 15.9% |
| 311965 | 404 | 368 | 16 | 4.3% |
| 311972 | 383 | 334 | 40 | 12.0% |
| 443867 | 327 | 271 | 48 | 17.7% |
| 311898 | 209 | 183 | 3 | 1.6% |
| 443847 | 103 | 89 | 10 | 11.2% |
| 311971 | 96 | 71 | 2 | 2.8% |

### 8.3 NTB Disagreement × IP Mutation (Headline Finding)

clickpass.is\_new and ui\_visits.is\_new disagree 42% of the time. Cross-tab with mutation rates:

| CP is\_new | VV is\_new | Count | Mutated | Mut % | Interpretation |
|-----------|-----------|-------|---------|-------|----------------|
| false | false | 11,729 | 2,350 | 20.0% | Both agree: returning — unstable networks |
| true | false | 9,646 | 735 | 7.6% | CP=NTB, VV=returning — IP flipped NTB→returning |
| true | true | 6,852 | 58 | 0.85% | Both agree: NTB — stable home/CTV IPs |
| **false** | **true** | **4,006** | **264** | **6.6%** | **PHANTOM NTB — returning→NTB misclass** |

**The 4,006 phantom NTB events are returning visitors whose IP changed between redirect and VV evaluation, causing the system to classify them as new-to-brand. This is 12.4% of all VV events in a single day for one advertiser.**

### 8.4 Cross-Device × Mutation

| is\_cross\_device | Total | Full Chain | Mutated at CP | Mutation Rate |
|-----------------|-------|------------|---------------|---------------|
| false (same device) | 17,101 | 15,173 | 1,321 | 8.7% |
| true (cross device) | 15,263 | 13,665 | 2,086 | 15.3% |

Cross-device accounts for 61.2% of mutation (2,086/3,407). Same-device contributes 38.8% (1,321/3,407), likely network switching (Wi-Fi ↔ cellular).

### 8.5 Gap Analysis — Clickpass Without VV

| has\_vv | Total | CP NTB | CP NTB % |
|--------|-------|--------|----------|
| 0 (no VV) | 131 | 54 | 41.2% |
| 1 (has VV) | 32,233 | 16,498 | 51.2% |

Only 131 orphaned clickpass rows (0.4%). Not the phantom NTB risk area.

### 8.6 Chain Gap Analysis (Thread 1)

3,526 clickpass events (10.9%) couldn't complete the full 4-table chain. CIL is the sole bottleneck:

| CIL | Win | EL | Count | % | Meaning |
|-----|-----|-----|-------|---|---------|
| 1 | 1 | 1 | 28,838 | 89.1% | Full chain — all hops succeeded |
| 0 | 0 | 0 | 1,776 | 5.5% | No CIL, no win, no EL — completely unmatched |
| 0 | 0 | 1 | 1,741 | 5.4% | VAST played but no impression record |
| 1 | 1 | 0 | 9 | 0.03% | Had impression+win but no VAST event |

**Root cause split:** 1,775 (50.5%) have impression\_time >14 days before clickpass — long-latency conversions recoverable by widening lookback to 30 days. 1,742 (49.5%) have recent impression\_time but no CIL record — genuinely missing. Win hop is 100% reliable when CIL exists. EL hop is 100% reliable. NTB rate elevated in all-missing group (63.6% vs 50.6% baseline). v3 SQL updated to 30-day lookback.

### 8.7 Thread 3 Scale Expansion — 10 Advertisers, 7 Days (Feb 4–10)

3,331,945 total clickpass rows traced across top 10 advertisers by volume. **Every core finding confirmed at scale.**

**Universal invariants (all 10 advertisers, 1,440,945 full chain rows):**
- Win IP = CIL IP: **100%** across all 10
- CP IP = Visit IP: **99.93%+** across all 10
- Win join = CIL join: **100%** (0–2 row loss per advertiser)
- All mutation at EL → CP redirect, zero at visit: **every advertiser**

**Per-advertiser results:**

| Adv ID | Total CP | CIL% | EL% | Full Chain% | Mutation% | Phantom NTB | XDevice% | Archetype |
|--------|----------|------|-----|-------------|-----------|-------------|----------|-----------|
| 31357 | 1,627,892 | 98.1 | 21.7 | 20.8 | 8.9 | 76,163 | 41.3 | Mixed, low EL |
| 31276 | 431,638 | 96.7 | 58.9 | 56.2 | 14.2 | 37,941 | 60.6 | Mixed |
| 32058 | 260,468 | 97.1 | 60.3 | 57.7 | 20.7 | 1 | 61.1 | Pure returning |
| 37775 | 220,823 | 95.4 | 99.97 | 95.4 | 11.2 | 25,437 | 46.7 | Mixed, CTV-heavy |
| 34611 | 186,401 | 97.1 | 58.4 | 55.8 | 7.6 | 14,867 | 50.9 | Mixed |
| 38710 | 156,748 | 95.2 | 99.97 | 95.2 | 11.6 | 12,377 | 62.0 | Mixed, CTV-heavy |
| 35457 | 136,266 | 97.5 | 52.6 | 50.4 | 13.1 | 13,803 | 71.2 | Mixed, high XDevice |
| 30857 | 126,625 | 96.8 | 71.3 | 68.4 | 20.8 | 1 | 60.9 | Pure returning |
| 32404 | 97,215 | 96.9 | 69.4 | 66.9 | 14.3 | 10,910 | 68.8 | Mixed |
| 34835 | 87,869 | 98.4 | 31.1 | 29.9 | 5.9 | 6,013 | 50.5 | Mixed, lowest mut |

**Key patterns at scale:**
- Mutation range: 5.9%–20.8%. Pure returning advertisers (32058, 30857) at top (~21%), confirming visitor population effect.
- EL match is the biggest variable: 21.7%–99.97%. Two CTV-heavy advertisers (37775, 38710) at ~100%. 31357 (largest by 4x) at only 21.7%.
- CIL match uniformly 95–98% across all advertisers.
- Phantom NTB: ~197,500 over 7 days (~28,200/day). Concentrated in mixed advertisers; pure returning (32058, 30857) have 0–1.
- is\_new disagreement: universal across all advertisers (41–56%), confirming systemic platform issue.

### 8.8 NTB Validation via conversion\_log (Advertiser 37775, Feb 10)

Independent NTB verification using the page\_view\_guid → conversion\_log bridge. For each VV flagged is\_new=true by ui\_visits, check whether the visit IP or bid IP had prior conversion activity in conversion\_log within a 30-day lookback.

**Bridge validation:** 100% of clickpass rows have page\_view\_guid. 15.43% match to conversion\_log (expected — not every visit converts). Bridge is valid.

**Schema probe:** conversion\_log.guid, ip, ip\_raw, original\_ip all 100% populated. conversion\_type is **entirely NULL** for this advertiser.

**NTB Validation results (n = 10,859 VVs flagged NTB):**

| Metric | Count | % |
|--------|-------|---|
| Prior conv by visit IP | 203 | 1.87% |
| Prior conv by bid IP | 194 | 1.87% |
| Prior conv by both | 155 | |
| **Bid IP only (mutation-caused)** | **39** | **0.36%** |
| Visit IP only | 48 | |
| **No prior conversion (truly NTB)** | **10,617** | **97.77%** |

**Phantom NTB vs Agreed NTB breakdown:**

| Segment | Total | Prior conv visit IP | Prior conv bid IP | Bid-only prior | No prior conv |
|---------|-------|---------------------|-------------------|----------------|---------------|
| Agreed NTB (cp=true, vv=true) | 6,853 | 85 (1.2%) | 74 (1.1%) | 5 (0.07%) | 6,763 (98.7%) |
| Phantom NTB (cp=false, vv=true) | 4,006 | 118 (2.9%) | 120 (3.0%) | 34 (0.85%) | 3,854 (96.2%) |

**Critical implication:** 96.2% of phantom NTBs have NO prior conversion in conversion\_log. Conversion\_log is NOT the reference source for is\_new classification. The 42% is\_new disagreement is driven by a different table entirely — likely a visit/session history table, not conversion history. This makes dplat Q1B (is\_new lookback implementation) the highest-priority question.

### 8.9 Advertiser 30857 — v3 (100% Returning, Feb 10)

20,239 clickpass | 20,179 VV (99.7%) | 13,464 full chain (66.5%). **v2 → v3 decomposition confirmed:** v2 showed 21.2% "mutation at visit". v3 shows 21.25% mutation at redirect, 0% at visit. Numbers match almost exactly. win=CIL=100%, CIL=EL=96.5%, CP=visit=99.98%. Cross-device 62.1%. EL match 70.3%.

is\_new disagreement most extreme: clickpass calls 48% NTB (9,723/20,179) while VV calls all returning. Zero phantom NTB (vv\_is\_new never true). Mutation uniform across both quadrants (15.05% both-returning, 13.24% cp-NTB/vv-returning) — unlike 37775 where mutation varies by NTB status.

Per-campaign: 3 campaigns with chain data show uniform 20–23% mutation. 3 campaigns (466668/670/671, 30% of events) have zero full chain — likely non-CTV inventory. Explains 70% EL match rate. Reinforces EL gap question for dplat.

### 8.10 v2 Results (Preserved)

Advertiser 30857 v2 (100% returning, Feb 10, 4-checkpoint): 20,980 VVs, 13,310 full chain (63.4%), 21.2% mutation at visit → **confirmed by v3 as 21.25% at redirect, 0% at visit**.

Advertiser 37775 v2 (100% NTB, Feb 10, 4-checkpoint): 9,879 VVs, 8,259 full chain (83.6%), 3.78% mutation at visit → **confirmed by v3 as 11.8% at redirect, 0% at visit**.

v2 → v3 reframe: The v2 'mutation at visit' measured EL → visit as one hop. v3 decomposes this and reveals all mutation is at EL → clickpass. Visit IP and clickpass IP are functionally identical (99.99%). Confirmed across all three advertisers.

### 8.11 BQ Simplified Trace — Advertiser 37775, Feb 10

First BQ trace using the simplified 2-table chain (`clickpass_log → event_log` via `event_log.bid_ip`). No CIL, win\_log, or visits table needed.

**8,360 clickpass events** | **8,356 EL matched** (99.95%)

| Metric | BQ Value | GP Reference | Delta |
|--------|----------|--------------|-------|
| bid\_ip = el\_ip | 7,928 (94.9%) | 96.2% (CIL=EL) | -1.3pp |
| el\_ip = cp\_ip | 6,858 (82.1%) | 87.3% | -5.2pp |
| bid\_ip = cp\_ip | 6,553 (78.4%) | — | End-to-end |
| Mutated at redirect | 1,377 (16.5%) | 11.8% | +4.7pp |
| All 3 stable | 6,551 (78.4%) | 84.4% (all 5) | -6pp |
| CP NTB | 3,275 (39.2%) | 51.2% | -12pp |
| Cross-device | 4,967 (59.4%) | 47.2% | +12.2pp |

**Volume discrepancy: BQ = 25.8% of GP** (8,360 vs 32,364). Ratio is stable at ~25% across Feb 4–10 (7,409–8,732 daily). Higher mutation and cross-device rates in BQ suggest the subset is enriched for cross-device / non-home-network traffic. Root cause under investigation — `blocked_source` column in BQ clickpass\_log is a lead.

**Assessment:** Bronze trace is mechanically validated but uses a ~25% non-random subset. Silver layer resolves this — see 8.12.

### 8.12 Silver Layer Trace — Advertiser 37775, Feb 10 (Production BQ)

Silver `dw-main-silver.logdata.clickpass_log` matches GP volume (32,286 vs GP's 32,364). Simplified trace on silver with 20-day EL lookback:

**32,286 clickpass events** | **32,275 EL matched** (99.97%) — matches GP exactly.

| Metric | Silver BQ | GP Reference | Delta |
|--------|-----------|--------------|-------|
| bid\_ip = el\_ip | 96.13% | 96.2% | -0.1pp |
| el\_ip = cp\_ip | 87.18% | 87.3% | -0.1pp |
| Mutated at redirect | **15.06%** | **11.8%** | **+3.3pp** |
| Cross-device % | 47.15% | 47.2% | 0 |
| CP NTB % | 51.19% | 51.2% | 0 |

### 8.13 BWN Triple Validation — bid\_ip Confirmed from 3 Independent Sources

Using silver CIL as bridge (EL.ad\_served\_id → CIL.ad\_served\_id → CIL.impression\_id → BWN.mntn\_auction\_id):

| Source | Matched Rows | = EL.bid\_ip | Match % |
|--------|-------------|-------------|---------|
| Silver CIL.ip | 30,477 | 30,477 | **100.0%** |
| BWN.device\_ip | 25,611 | 25,611 | **100.0%** |

**Three independent data sources all confirm the same bid IP:**
1. `event_log.bid_ip` (the primary trace column)
2. `cost_impression_log.ip` (via silver CIL — also confirms GP finding on BQ)
3. `bidder_win_notifications.device_ip` (entirely separate system — GP equivalent was always NULL)

BWN matched 84.03% of CIL-matched rows (25,611/30,477). The 16% gap may be BWN date range limitation or join key coverage. The 100% IP match on joined rows is the key result.

**Note:** GP `win_log.device_ip` was always NULL. BQ BWN `device_ip` is 100% populated (891,953/891,953 rows for advertiser 37775, Feb 10). Zero IPv6 addresses.

### 8.14 Multi-Advertiser Silver Trace — Top 5 by Volume, Feb 10

| Adv ID | CP Rows | EL Match% | BQ Mutation | GP Mutation | Delta | XDevice% | NTB% |
|--------|---------|-----------|-------------|-------------|-------|----------|------|
| 31357 | 236,217 | 21.76% | 12.22% | 8.9% | +3.3pp | 42.22% | 57.42% |
| 31276 | 65,934 | 58.59% | 18.85% | 14.2% | +4.6pp | 62.22% | 48.30% |
| 32058 | 43,115 | 59.21% | 24.26% | 20.7% | +3.6pp | 61.76% | 47.04% |
| 37775 | 32,286 | 99.97% | 15.06% | 11.2% | +3.9pp | 47.15% | 51.19% |
| 34611 | 24,547 | 60.32% | 10.28% | 7.6% | +2.7pp | 58.34% | 55.60% |

**EL match rates match GP near-exactly** (21.76% vs 21.7%, 58.59% vs 58.9%, etc.). This confirms silver has the same data as GP.

**Mutation was +2.7 to +4.6pp higher than GP on single-day runs with 20-day EL lookback.** This offset was **RESOLVED** by the full 7-day scale run with 30-day lookback — see 8.15.

### 8.15 Full Silver Scale Run — 10 Advertisers, 7 Days, 30-Day EL Lookback (Production BQ)

3,249,678 total clickpass rows across 10 advertisers (Feb 4–10, 2026). Simplified trace: `clickpass_log → event_log (bid_ip)`. 30-day EL lookback. **This is the production-equivalent BQ run matching the GP scale run from Thread 3.**

| Adv ID | Total CP | EL Matched | EL Match% | bid=el% | el=cp% | Mutation% | CP NTB% | XDevice% |
|--------|----------|------------|-----------|---------|--------|-----------|---------|----------|
| 31357 | 1,611,203 | 352,042 | 21.85% | 96.55% | 90.45% | 8.85% | 59.20% | 41.44% |
| 31276 | 419,981 | 247,831 | 59.01% | 95.39% | 84.84% | 14.22% | 51.55% | 60.45% |
| 32058 | 256,834 | 154,941 | 60.33% | 95.80% | 78.53% | 20.58% | 49.57% | 61.20% |
| 37775 | 219,613 | 219,543 | 99.97% | 96.20% | 88.00% | 11.17% | 53.36% | 46.69% |
| 34611 | 185,172 | 108,058 | 58.36% | 96.55% | 91.82% | 7.62% | 62.85% | 50.81% |
| 35457 | 135,096 | 71,140 | 52.66% | 96.03% | 86.25% | 13.10% | 35.27% | 71.20% |
| 30857 | 124,917 | 89,113 | 71.34% | 96.28% | 78.50% | 20.68% | 50.72% | 60.95% |
| 38710 | 112,977 | 112,947 | 99.97% | 94.58% | 87.73% | 11.52% | 37.80% | 64.76% |
| 32404 | 96,271 | 66,977 | 69.57% | 95.70% | 85.01% | 14.32% | 34.80% | 68.76% |
| 34835 | 87,614 | 27,228 | 31.08% | 97.70% | 93.90% | 5.94% | 50.92% | 50.41% |

**MUTATION OFFSET RESOLVED.** With 30-day EL lookback, BQ silver matches GP within 0.12pp on every advertiser:

| Adv ID | GP Mutation (7-day) | BQ Silver Mutation (7-day) | Delta |
|--------|---------------------|---------------------------|-------|
| 31357 | 8.9% | 8.85% | -0.05pp |
| 31276 | 14.2% | 14.22% | +0.02pp |
| 32058 | 20.7% | 20.58% | -0.12pp |
| 37775 | 11.2% | 11.17% | -0.03pp |
| 34611 | 7.6% | 7.62% | +0.02pp |
| 35457 | 13.1% | 13.10% | 0pp |
| 30857 | 20.8% | 20.68% | -0.12pp |
| 38710 | 11.6% | 11.52% | -0.08pp |
| 32404 | 14.3% | 14.32% | +0.02pp |
| 34835 | 5.9% | 5.94% | +0.04pp |

**The +2.7 to +4.6pp offset seen in single-day runs was caused by the shorter (20-day) EL lookback window, NOT by inet vs STRING comparison.** With proper 30-day lookback, BQ silver is a perfect match for GP across all 10 advertisers. **BQ silver is now fully validated as a drop-in replacement for Greenplum.**

**Key patterns confirmed (matching GP exactly):**
- EL match = CTV inventory percentage: 37775/38710 at ~100% (pure CTV), 31357 at 21.85% (~78% non-CTV)
- Mutation range: 5.94%–20.68%. Pure returning advertisers (32058, 30857) highest (~21%), confirming visitor population effect
- bid\_ip = el\_ip: 94.58%–97.70% across all advertisers (bid and VAST playback usually same IP)
- All mutation at EL→CP redirect — confirmed on BQ at scale

---

## 9. Key Findings

1. IP is rock-solid through the ad-serving pipeline. Win = CIL = 100%. CIL = EL > 96%. Confirmed across three advertisers (37775, 31455, 30857).

2. ALL mutation occurs at the redirect (EL → clickpass), not at the page visit. mutated\_at\_visit = 0 across all three advertisers (37775: 11.8%, 31455: 9.8%, 30857: 21.25% — all at redirect, zero at visit).

3. Cross-device explains 61.2% of mutation (15.3% rate). Same-device contributes 38.8% (8.7% rate).

4. NTB classification disagrees 42% between clickpass and ui\_visits. Systemic pipeline issue.

5. 4,006 phantom NTB events in one day for one advertiser. Returning visitors misclassified as new.

6. Mutation directly drives misclassification. Both-agree-NTB = 0.85% mutation. Both-agree-returning = 20% mutation.

7. Campaign-level mutation varies 0.6% to 20.6%. Original NTB campaigns < 1%, newer campaigns 12–20%.

8. clickpass\_log is a near-perfect VV proxy (99.6%) with is\_cross\_device, first\_touch\_ad\_served\_id, and conversion\_log bridge.

9. Stage 3 → Stage 1 is valid. All IPs originate in Stage 1. A VV traces directly back to a bid/win. Stage 2 is not a separate hop.

10. Chain gap is a CIL problem, not a join problem. 99.7% of unmatched rows are missing CIL. Half recoverable with 30-day lookback. Win and EL joins are 100% reliable when CIL exists.

11. The is\_new 42% disagreement is a reference data / lookback definition problem, NOT an IP mutation problem. Both systems see the same IP (99.99% identical at checkpoints 4 and 5) but evaluate "newness" against different historical records. 93%+ of disagreeing rows have stable IPs across all 5 checkpoints.

12. Same-device mutation (8.7%) is explained by standard network behavior — Wi-Fi ↔ cellular switching, CGNAT/dynamic IP rotation, VPN toggling. No system bug required.

13. Campaign mutation variance (0.6%–20.6%) is confirmed as a visitor population effect. Pure NTB = 0.4–4%, pure returning = 11–18%. Three NTB campaigns (311897, 311966, 311972) are outliers at 10–15% — needs investigation.

14. Phantom NTB is entirely concentrated in NTB campaigns. All 4,006 come from NTB/mixed campaigns; 443xxx (pure returning) contribute zero. The is\_new disagreement direction flips by campaign type: clickpass is stricter in NTB campaigns, looser in returning campaigns.

15. **All findings confirmed at scale.** 10 advertisers, 7 days, 3.3M clickpass rows, 1.4M full chain rows. Win = CIL = 100%, CP = Visit = 99.93%+, all mutation at redirect, zero at visit — universal across every advertiser. Mutation range 5.9%–20.8%. Phantom NTB ~28,200/day across 10 advertisers.

16. **EL match varies wildly by advertiser (21.7%–99.97%).** Two CTV-heavy advertisers (37775, 38710) have ~100% EL match, while others range from 21.7% (31357, the largest advertiser) to 71.3% (30857). This is the biggest remaining variable in full chain coverage. Likely driven by inventory mix (CTV vs non-CTV).

17. **Conversion\_log is NOT the reference source for is\_new.** NTB validation via conversion\_log shows 97.8% of VVs flagged NTB have zero prior conversions within 30 days. Even phantom NTBs are 96.2% clean by conversion\_log. Only 39 out of 10,859 NTB VVs (0.36%) show mutation-caused misclassification via the conversion pathway. The 42% is\_new disagreement is driven by a different, unidentified reference table.

18. **event\_log.bid\_ip = win\_log.ip at 100%.** Validated on 30,502 full-chain rows (zero mismatches). Eliminates the need for CIL and win\_log in the trace chain. Simplified trace: `clickpass_log → event_log (bid_ip + ip)` — 2 joins instead of 4.

19. **BQ volume gap RESOLVED — silver layer = complete data.** Bronze `raw.clickpass_log` contains ~25% of GP volume (non-random subset). Silver `logdata.clickpass_log` matches GP at 99.8% (32,286 vs 32,364). Silver is the correct BQ source. `blocked_source` column is 100% NULL — not the filter cause. Bronze filtering happens upstream at ingestion.

20. **BWN device\_ip triple-validates bid\_ip at 100%.** Three independent sources confirm the same bid IP: `event_log.bid_ip`, `cost_impression_log.ip` (via silver CIL), and `bidder_win_notifications.device_ip` (via CIL→BWN bridge). All match at 100% on joined rows. BWN `device_ip` is 100% populated in BQ (891,953 rows) — unlike GP where `win_log.device_ip` was always NULL.

21. **Silver trace matches GP on all metrics except mutation (+2.7 to +4.6pp).** EL match rates, cross-device rates, and NTB rates match GP within 1pp across 5 advertisers. Mutation offset is systematic and consistent — likely inet vs STRING IP comparison, not data quality. Rank order preserved.

22. **BWN timestamps are nanoseconds, not milliseconds.** `auction_timestamp` values like `1771704725076321000` must be converted via `TIMESTAMP_MICROS(CAST(ts / 1000 AS INT64))`. `impression_id` on BWN is a small integer (not useful as join key); `mntn_auction_id` (steelhouse format) is the correct bridge to CIL.impression\_id.

23. **`ad_served_id` on clickpass = last-touch (most recent impression).** Empirically confirmed on 38,360 VVs where `ad_served_id != first_touch_ad_served_id` (advertiser 37775, Feb 4–10). The VV-associated impression's VAST timestamp is always more recent than the first-touch's. Zero exceptions in 35,198 resolvable rows. Time gaps range from 2 hours to 815 hours (~34 days).

24. **42.48% of VVs are single-impression attribution.** `ad_served_id = first_touch_ad_served_id` for 93,297 of 219,613 VVs. However, these IPs still had 6–30 distinct impressions in the 30-day window — the attribution system just links the most recent and first, not intermediates.

25. **40.05% of VVs have NULL `first_touch_ad_served_id`.** 87,956 VVs lack first-touch data. For these, first-touch IP tracing is impossible — only last-touch is available. NULL rate inversely correlates with recency (54% at <1hr, 18% at 14-21 days) — most likely batch-processed, not a lookback limit.

26. **14.28% inter-impression bid IP mutation.** Among the 38,360 multi-impression VVs, 5,026 have different bid IPs between first-touch and last-touch impressions. First-touch tracing adds genuine IP lineage information that last-touch alone cannot provide.

27. **Retargeting campaigns exist at every funnel level — must exclude for prospecting analysis (2026-03-10).** Zach: "retargeting isn't relevant" to this audit. Retargeting campaigns (objective_id=4) have funnel_level 1/2/3 — the previous "~20% unresolved" included retargeting VVs that lack S1 impressions by design. VV #1 trace (IP 208.97.32.204) was campaign 443862 = "TV Retargeting - Television - 5+ PV" — correctly had no S1.

28. **Prospecting-only CTV S2 resolution: 98.56% (2026-03-10).** Excluding retargeting (objective_id=4) and Ego (objective_id=7), CTV devices only (SET_TOP_BOX + CONNECTED_TV): 15,880/16,112 resolved via 5 tiers:
    - S1 impression at bid_ip: 15,465 (96.0%)
    - guid_vv_match: 353 (2.2%), guid_imp_match: 5 (0.03%), s1_imp_redirect: 11 (0.07%)
    - **household_graph (NEW tier):** 46 (0.29%) — uses `bronze.tpa.graph_ips_aa_100pct_ip` to find IPs in same household
    - Truly unresolved: 232 (1.44%) — 178 are competing VVs (secondary attribution), only 54 primary
    - **Primary VV unresolved rate: 0.34%** — effectively zero
    - Root cause: LiveRamp identity graph linked IP to different household IP with S1 impression. Most IPs are T-Mobile CGNAT (172.5x.x.x) — IP rotation means graph snapshot may miss the active IP at S1 time
29. **Display S2 resolution: 98.29% (2026-03-10).** Non-CTV devices (MOBILE/TABLET/GAMES_CONSOLE): 2,298/2,338 resolved via 3 tiers:
    - S1 impression at bid_ip: 2,236 (95.64%), guid_vv_match: 61 (2.61%), s1_imp_redirect: 1 (0.04%)
    - Unresolved: 40 (1.71%) — 32 competing, 8 primary. **Primary VV unresolved: 0.34% — identical to CTV**
30. **Combined all-device resolution: 98.53% with bid_ip only (2026-03-10).** 18,178/18,450 prospecting S2 VVs resolved using S1 bid_ip only. 272 unresolved — corrected in #31.
31. **CORRECTION: 100% resolved with S1 VAST IPs (2026-03-11).** Previous analysis used S1 bid_ip (CIL.ip) only. Adding S1 VAST IPs from event_log (vast_start/vast_impression): **18,450/18,450 = 100% resolved, 0 unresolved.** S1 VAST IPs add 6M IPs not in S1 bid_ip pool (CGNAT/SSAI causes ~6% bid↔VAST IP mismatch). 747 VVs resolved by VAST IPs with no matching bid IP. The production model's `impression_pool` CTE already combines both sources.
32. **Production Q3 10-tier cascade tested with prospecting filter (2026-03-11).** objective_id IN (1,5,6) per Ray — excludes retargeting (4) and Ego (7). Advertiser 37775, 7-day trace (Feb 4–11), 90-day lookback:
    - **S1:** 93,274 VVs, 100% current_is_s1 (by definition)
    - **S2:** 16,753 VVs, **99.99% resolved** (2 unresolved). Top tiers: vv_chain_direct 56.72%, imp_direct 24.31%, current_is_s1 10.52%, imp_visit_ip 4.01%
    - **S3:** 23,844 VVs, **99.43% resolved** (136 unresolved). Top tiers: vv_chain_direct 59.78%, imp_direct 19.72%, current_is_s1 12.33%, imp_visit_ip 3.62%
    - **Query:** `queries/ti_650_q3_test.sql` — full 10-tier cascade (current_is_s1, vv_chain_direct, vv_chain_s2_s1, imp_chain, imp_direct, imp_visit_ip, cp_ft_fallback, guid_vv_match, guid_imp_match, s1_imp_redirect)
    - **Key insight:** Prospecting filter (1,5,6) is the single biggest driver of resolution rate — drops unresolved from ~20% to <1%. vv_chain_direct (prior VV VAST IP → current bid_ip) is the dominant resolution method across all stages.
33. **CORRECTION: Independent tier analysis disproves waterfall ranking (2026-03-11).** Tested each S2 resolution tier independently (not as a waterfall). Results for 16,753 S2 VVs:
    - **imp_visit (ui_visits.impression_ip → S1 vast_start_ip):** 99.67% independent, **574 unique** (only tier for those VVs)
    - **imp_direct (S2 bid_ip → S1 vast_start_ip):** 96.27% independent, **6 unique**
    - **imp_redir (S2 redirect_ip → S1 vast_start_ip):** 87.70% independent, **2 unique**
    - **vv_chain_direct (prior S1 VV vast_start_ip → S2 bid_ip):** 52.04% independent, **0 unique — pure subset of imp_direct**
    - vv_chain appeared #1 in v11 waterfall (56.72%) only because it was checked first — it never resolves anything imp_direct can't
    - **Minimum set: imp_direct + imp_visit = 99.95%** (16,745/16,753). 8 unresolved (6 truly unresolvable, 2 imp_redir-only)
    - **10-tier CASE cascade with 13 LEFT JOINs collapses to 2 LEFT JOINs for S2**
    - Full analysis: `outputs/ti_650_s2_tier_analysis.md`

---

## 10. Edge Cases and Implementation Notes

1. IP type mismatch: inet columns have /32 suffix. Must replace(ip::text, '/32', '') for all comparisons.
2. win\_log.advertiser\_id is Beeswax ID, not MNTN. Never filter by advertiser — join via CIL.
3. EL dedup: Multiple vast\_impression per ad\_served\_id. Use DISTINCT ON to take first.
4. 30-day lookback: CIL/win/EL time filters must extend 30 days before clickpass start date (was 14; widened to recover long-latency conversions).
5. win\_log.device\_ip is always NULL. Use win\_log.ip instead.
6. clickpass.ad\_served\_id is text, ui\_visits.ad\_served\_id is uuid. Cast ::text on ui\_visits side.
7. Clickpass has 0.4% more rows than VVs (131/32,364). Flagged has\_vv = 0, not excluded.
8. is\_new evaluated at different pipeline stages produces 42% disagreement. Both preserved in query.
9. **Duplicate clickpass/ui\_visits rows per `ad_served_id` (2026-03-02) — RESOLVED.** Type H UUID `cf7659de` returned 2 clickpass rows and 2 ui\_visits rows for the same `ad_served_id`. Investigated full clickpass detail: two separate visits from two different devices in the same household — iPhone Safari (`is_cross_device = false`, GA client `2043036188`) and Android Chrome (`is_cross_device = true`, GA client `40715985`), ~4 hours apart. This is expected multi-device attribution behavior. Scale: 84 / 219,527 ad\_served\_ids (0.038%). Production table uses `QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1` to take the most recent visit.
10. **`/32` CIDR suffix on silver `event_log.ip` (2026-03-02) — RESOLVED.** Type F first-touch returned `ft_vast_ip = '72.128.72.212/32'` while `ft_bid_ip = '72.128.72.212'` (no suffix). **Zach (2026-03-03):** "this is the same ip. the db table is just representing it differently" — use PostgreSQL's `host()` function to extract clean IP from inet. In BQ silver, /32 suffixes are already stripped (zero found in gap analysis). BQ production queries use `SPLIT(ip, '/')[OFFSET(0)]` as defensive measure. GP queries updated to use `host()` per Zach's recommendation.
11. **Partition filters required on all silver CTEs.** Silver tables are partitioned by `time` (no `dt` column on clickpass). Without `DATE(time) BETWEEN ...` filters, queries scan entire tables and time out (5+ minutes for 8 UUIDs). Always include partition filters on cp, el, ft, and v CTEs.

---

## 11. Open Questions

**Thread 1 — Chain gap analysis:** **CLOSED.** CIL is sole bottleneck (99.7% of gap). 50% recoverable with 30-day lookback; 50% genuinely missing. Win and EL 100% reliable.

**Thread 2 — Mixed advertiser:** **CONFIRMED.** 31455 (51/49 NTB split) run. All core findings hold: Win=CIL=100%, all mutation at redirect (9.8%), zero at visit, is\_new disagreement 41.2%. New finding: EL match is only 18% for this advertiser (vs ~100% for 37775) — possible non-CTV inventory. Full chain 16%. Cross-device 71.2%.

**Thread 3 — Scale:** **CLOSED.** 10 advertisers, 7 days, 3.3M rows. All findings confirmed at scale. Mutation range 5.9%–20.8%. EL match 21.7%–99.97%. Phantom NTB ~28,200/day across 10 advertisers.

**Thread 4 — BQ recovery:** **CLOSED — SILVER LAYER VALIDATED.** Volume gap resolved: bronze `raw.clickpass_log` is a ~25% non-random subset; silver `logdata.clickpass_log` matches GP at 99.8%. `blocked_source` is 100% NULL (not the filter cause). Silver trace validated: 99.97% EL match, mutation +3.3pp vs GP (systematic offset, likely inet vs STRING). Multi-advertiser confirmed (5 advertisers, all match GP patterns). BWN `device_ip` 100% populated, = `bid_ip` at 100% (triple validation). Silver layer has 23 tables including clickpass\_log, event\_log, cost\_impression\_log, win\_logs, conversion\_log. **Silver is the production BQ audit source.** Remaining: investigate mutation offset, widen EL lookback to 30 days for full date coverage.

**Thread 5 — dplat:** **CLOSED (2026-02-25).** All questions answered via Zach call. Q1A: `impression_ip` = `impression_log.ip` = bid IP (empirically confirmed 99.2–100%). Q1B: `is_new` is client-side tracking pixel (JavaScript), no database table lookup. Q2: non-CTV doesn't fire VAST. Q3: BWN mapping validated empirically. Q4: multiple vast\_impression = publisher replaying VAST file, dedup by first time correct. Q5: LTTV is "best effort", not source of truth. Q6: publisher mix and device type drive mutation variance. Q7: reduced priority (simplified trace reduces cost). IP naming convention from Zach: `ip` = logic IP, `bid_ip` = IP at bid, `original_ip` = pre-iCloud Private Relay IP. `clickpass_log` = old term for verified visit; `ui_visits` is superset (includes display clicks).

**Thread 6 — is\_new root cause:** **CLOSED (2026-02-25).** Zach confirmed: `is_new` is determined by the tracking pixel (client-side JavaScript in the browser), NOT by a database table lookup. There is no reference table. The 41–56% disagreement between `clickpass_log.is_new` and `ui_visits.is_new` is because they are two independent client-side systems making separate determinations at different points in time. The ~28,200 "phantom NTB" events per day is an inherent architectural property, not a data integrity bug. `is_new` is not auditable via SQL.

**Thread 7 — Same-device mutation:** **CLOSED.** 8.7% same-device mutation is explained by Wi-Fi ↔ cellular switching, CGNAT/dynamic IP rotation, and VPN toggling. No system bug. Does not change audit conclusions.

**Thread 8 — ext\_visits/ext\_conversions:** **CLOSED.** Write-only foreign tables exporting to GCS parquet. Not readable/queryable. GCS export (`gs://mntn-analytics-curated/coredw/summarydata/visits`) runs with 3-day lookback every ~3hrs — separate from stopped BQ `raw.visits` pipeline and may have current data.

**Thread 9 — Campaign mutation variance:** **CONFIRMED.** Per-campaign NTB cross-tab run. Three archetypes: pure NTB (0.4–4% mutation), pure returning (11–18%), and a puzzle group (311897/311966/311972: NTB by VV but 10–15% mutation). Phantom NTB is entirely concentrated in NTB campaigns (443xxx = zero). is\_new disagreement direction flips by campaign type — clickpass is stricter in NTB campaigns, looser in returning campaigns.

---

## 12. What's Been Accomplished

1. v2 query built and validated — 4-table trace on Greenplum.
2. BQ data availability investigated — pipeline gap documented.
3. BQ → GP table mappings confirmed with column-level validation.
4. IP type mismatch (/32 inet) discovered and resolved.
5. win\_log Beeswax ID mismatch identified.
6. win\_log.device\_ip always NULL — switched to win\_log.ip.
7. Trace: advertiser 30857 (returning) — 21.2% mutation.
8. Trace: advertiser 37775 (NTB) — 3.78% mutation (v2).
9. clickpass\_log schema: ad\_served\_id, is\_new, is\_cross\_device, first\_touch\_ad\_served\_id.
10. conversion\_log schema: no ad\_served\_id, reachable via page\_view\_guid bridge (14.3M rows).
11. clickpass coverage: 99.6% of VVs have matching clickpass.
12. is\_new disagreement: 42% mismatch between clickpass and ui\_visits.
13. v3 query built — clickpass starting point, 5 IP checkpoints.
14. ALL mutation localized to redirect hop — 0% at visit.
15. Cross-device × mutation: 61.2% cross-device, 38.8% same-device.
16. NTB disagreement × mutation cross-tab — 4,006 phantom NTB per day.
17. Per-campaign breakdown — 24 campaigns, 0.6% to 20.6% mutation range.
18. Gap analysis — 131 orphaned clickpass, not a risk area.
19. ext\_visits/ext\_conversions resolved — write-only GCS export tables, not queryable. GCS bucket identified as alternative data source.
20. Chain gap analysis — CIL is sole bottleneck (99.7%). 50% recoverable with 30-day lookback (long-latency conversions). 50% genuinely missing. Win and EL 100% reliable. SQL updated to 30-day lookback.
21. Thread 6 narrowed — is\_new disagreement is a lookback/reference data divergence, not IP mutation. 93%+ of disagreeing rows have stable IPs. Phantom NTB is a classification system problem.
22. Thread 7 narrowed — same-device mutation (8.7%) explained by Wi-Fi ↔ cellular switching, CGNAT, VPN toggling. No system bug. Confirmable via ASN lookup.
23. Thread 9 confirmed — per-campaign NTB cross-tab run. Three archetypes identified: pure NTB (0.4–4%), pure returning (11–18%), puzzle group (311897/966/972: NTB by VV but 10–15% mutation). Phantom NTB entirely in NTB campaigns. is\_new disagreement direction flips by campaign type.
24. Thread 2 confirmed — advertiser 31455 (50/50 NTB). All core findings hold across second advertiser. is\_new disagreement 41.2% (systemic). New finding: EL match only 18% — possible non-CTV inventory pattern.
25. Thread 4 partially resolved — BQ date ranges checked. clickpass\_log and conversion\_log confirmed flowing (Jan 25 → Feb 25). visits and CIL still stopped Jan 31. BWN schema mapped (device\_ip, auction\_timestamp, no `time`/`ip` column). GCS parquet alternative confirmed by Greg Spiegelberg.
26. Advertiser 30857 v3 run — decomposes v2's 21.2% mutation. Confirmed: 21.25% at redirect, 0% at visit. is\_new disagreement most extreme: clickpass calls 48% NTB for a 100% returning advertiser. EL match 70.3% (third data point for EL gap spectrum).
27. Thread 3 scale expansion completed — 10 advertisers, 7 days, 3.3M clickpass rows, 1.4M full chain rows. All invariants confirmed: Win=CIL=100%, CP=Visit=99.93%+, all mutation at redirect. Mutation 5.9%–20.8%. EL match 21.7%–99.97%. Phantom NTB ~28,200/day.
28. NTB validation via conversion\_log completed — page\_view\_guid bridge works (100% have GUID, 15.4% match to conversion\_log). 97.8% of NTB VVs have zero prior conversions. Only 39/10,859 (0.36%) show mutation-caused false NTB via conversion pathway. conversion\_type column is entirely NULL for advertiser 37775. Conversion\_log ruled out as is\_new reference source.
29. event\_log.bid\_ip discovery — `logdata.event_log.bid_ip` (text) = `logdata.win_log.ip` at 100% (30,502 rows, zero mismatches). Eliminates CIL/win\_log from trace chain. Simplified trace: clickpass → event\_log (2 joins instead of 4).
30. BQ event\_log schema confirmed — `bid_ip` (STRING), `ip` (STRING), `original_ip` (STRING), `ip_raw` (STRING) all present. BQ port unblocked for simplified trace.
31. BQ simplified trace validated — First BQ run: 8,360 clickpass, 99.95% EL match, 16.5% mutation at redirect, 59.4% cross-device. Rates directionally consistent with GP.
32. BQ volume gap identified — BQ `raw.clickpass_log` has ~25% of GP volume (stable ratio across 7 days, Feb 4–10). Systematic filter, not data loss. **RESOLVED — bronze is a ~25% non-random subset; silver has complete data. Bronze layer not used.**
33. BQ schemas fully mapped — clickpass\_log (36 columns incl. `blocked_source`, `publisher`, `app_bundle`), event\_log (IP columns confirmed), BWN (41 columns incl. `inventory_source`, `device_type`, `placement_type`, `impression_id`).
34. **BQ volume gap RESOLVED** — `blocked_source` is 100% NULL (not the cause). Silver `logdata.clickpass_log` has complete data matching GP (32,286 vs 32,364). Bronze is a ~25% non-random subset.
35. **Silver layer inventoried** — 23 tables (all VIEWs except spend\_log\_tmp). Includes clickpass\_log, event\_log, cost\_impression\_log, win\_logs, conversion\_log. Created Feb 2026.
36. **Silver trace validated** — Simplified trace on silver matches GP: 99.97% EL match, 96.13% bid=el, 15.06% mutation. 7-day stability confirmed (13.8–16.3% mutation). Silver clickpass has no `dt` column — use `DATE(time)`.
37. **BWN device\_ip confirmed 100% populated** (891,953/891,953, advertiser 37775, Feb 10). Zero IPv6. Unlike GP where win\_log.device\_ip was always NULL.
38. **BWN triple validation completed** — BWN.device\_ip = EL.bid\_ip at 100% (25,611 joined rows). CIL.ip = EL.bid\_ip at 100% (30,477 rows). Three independent sources confirm bid IP.
39. **BWN timestamp corrected** — `auction_timestamp` is nanoseconds (not milliseconds as documented). Use `TIMESTAMP_MICROS(CAST(ts / 1000 AS INT64))`.
40. **BWN join key mapped** — BWN.impression\_id is a small integer (not useful). BWN.mntn\_auction\_id (steelhouse format) = CIL.impression\_id. BWN.auction\_id is UUID with \_xdc\_N suffix.
41. **Multi-advertiser silver trace** — Top 5 advertisers by volume on Feb 10. EL match rates match GP within 1pp. Mutation consistently +2.7 to +4.6pp higher (systematic offset). Rank order preserved.
42. **IP format verified** — Zero slashes in any BQ IP field. Mutation offset is NOT an IP formatting artifact. Sampled mismatches are genuine IP changes (DHCP rotation, CGNAT, cross-device).
43. **Thread 5 CLOSED (Zach call 2026-02-25)** — All dplat questions answered. `impression_ip` = `impression_log.ip` = bid IP (99.2–100%). `is_new` = client-side tracking pixel, no table lookup. Multiple vast\_impression = VAST file replay. Publisher/device mix drives mutation variance. `clickpass_log` = old term for verified visit.
44. **Thread 6 CLOSED (2026-02-25)** — `is_new` root cause resolved. Client-side JavaScript determination, not database lookup. 41–56% disagreement is inherent architectural property. ~28,200 phantom NTB/day is not a bug. Not auditable via SQL.
45. **Q1A empirically confirmed** — `summarydata.ui_visits.impression_ip` = `logdata.impression_log.ip` at 99.2% (500-row sample), = `logdata.cost_impression_log.ip` at 100%. `impression_ip` IS the bid IP carried forward onto the visit record. Higher mutation (17–35%) vs trace (6–21%) explained by all-inventory population (not CTV-only).
46. **Full silver scale run completed** — 3,249,678 clickpass rows, 10 advertisers, 7 days (Feb 4–10), 30-day EL lookback on `dw-main-silver.logdata`. **Mutation offset RESOLVED**: BQ matches GP within 0.12pp on every advertiser. The +2.7 to +4.6pp offset was caused by insufficient EL lookback (20 days), not inet vs STRING. BQ silver is a validated drop-in replacement for GP.
47. **Attribution model empirically resolved (Q1-Q5)** — `ad_served_id` on clickpass = last-touch (most recent) impression, confirmed with 0 exceptions across 35,198 rows. `first_touch_ad_served_id` = first impression, but NULL for 40.05% of VVs. 42.48% single-impression attribution, 17.47% multi-impression, 40.05% first-touch unavailable. 14.28% inter-impression bid IP mutation confirms first-touch tracing adds genuine IP information.
48. **"Single-impression" VVs are misleading** — The 42.48% where `ad_served_id = first_touch_ad_served_id` still have 6–30 distinct impressions for the same IP in the 30-day window. Attribution only links the first and most recent; intermediates are invisible.

49. **clickpass_log schema confirms click data is embedded in visit rows (Q7)** — Schema has `click_elapsed`, `click_url`, `destination_click_url` as columns on every row. **Correction (Zach, 2026-03-03):** `ui_visits` has a `click` boolean column that distinguishes click vs non-click visits — "there is a column 'click' that defines if it was a click" (on `summarydata.ui_visits`). `clickpass_log` itself does not have this discriminator — it only has click metadata columns. Zach also confirmed clickpass_log contains ALL verified visits (CTV + display): "VV can happen for display as well and would be here."

50. **first_touch NULL rate inversely correlates with recency (Q8)** — 54% NULL at <1hr impression-to-visit gap, dropping to 18% at 14-21 days. ~~Originally suggested batch backfill~~ — **DISPROVEN by gap analysis (2026-03-02)** and **confirmed by Zach (2026-03-03):** "clickpass_log is a real time log. there is no post processing to generate it." NULLs are permanent, populated at write time only. Zach added: "confirm with Sharad, but I do not believe they do this lookup for stage 1 CTV VV" — suggesting the first_touch lookup may not run for certain VV types, explaining the 40% NULL rate. **Follow-up: Sharad.**

51. **Real VV examples at every impression count documented (Q9-Q10)** — Five types documented with real data: Type A (1 impression, all IPs identical), Type B (2 impressions, stable IP, cross-device), Type C (5 impressions, perfect stability over 21 days), Type D (10 impressions, same device), Type E (369 impressions, 98 distinct vast_ips, bid IP never equals vast IP — datacenter/proxy outlier). Examples A–C show the normal case (zero mutation); Type E shows what an anomalous pattern looks like.

52. **Type E extreme outlier analyzed (Q10e)** — bid_ip `104.171.65.16` has 369 impressions over 8 days with 0% bid-to-VAST IP match. 98 rotating vast_ips in datacenter-like ranges (85.237.194.x, 104.234.32.x). Consistent with proxy/VPN or ad-tech infrastructure — not a normal household. This represents the far tail of the impression count distribution.

53. **8 row-level example types documented (A4f, 2026-03-02)** — Real VV UUIDs covering every pattern the production table captures: A (no mutation, same device), B (cross-device, no mutation), C (multi-impression stable), D (mutation at redirect), E (mutation + first-touch IP differs), F (NTB disagree + mutation), G (non-CTV — no VAST events), H (redirect ≠ visit IP, rare 0.07%). All verified with actual BQ Silver data (Feb 4–10). Query: `audit_trace_queries.sql` section A4f. Results: `bqresults/final_full.json`.

54. **Duplicate clickpass/ui\_visits rows per ad\_served\_id — RESOLVED (2026-03-02, Zach confirmed 2026-03-03).** Type H UUID `cf7659de` has 2 clickpass rows and 2 ui\_visits rows — two separate visits from different devices in the same household. Scale: 84 / 219,527 ad\_served\_ids (0.038%). **Zach:** "technically yes. it should never happen but no system is perfect. we monitor and alert when there are multiple rows per ad\_served\_id." Production table dedup (QUALIFY ROW\_NUMBER ORDER BY time DESC = 1) handles correctly.

55. **`/32` CIDR suffix on silver event\_log.ip — RESOLVED (2026-03-02, Zach confirmed 2026-03-03).** GP `inet` type naturally includes `/32` notation. **Zach:** "this is the same ip. the db table is just representing it differently" and "there's a `host` function to translate the CIDR/inet to the right string." In BQ silver, /32 suffixes are already stripped (zero found in gap analysis). Production BQ queries use `SPLIT(ip, '/')[OFFSET(0)]` as defensive measure (BQ equivalent of PostgreSQL's `host()`).

---

## 13. Next Steps

1. ~~Ask dplat (Thread 5)~~ — **DONE.** All questions answered via Zach call (2026-02-25).
2. ~~Full silver scale run~~ — **DONE.** 3.25M rows, 10 advertisers, 7 days. Matches GP within 0.12pp. Mutation offset resolved.
3. ~~Investigate mutation offset~~ — **RESOLVED.** Caused by 20-day EL lookback, not inet vs STRING. 30-day lookback eliminates the offset entirely.
4. **Build production audit table (NEXT — PRIMARY GOAL).** Create a persistent BQ table with full IP lineage for every Stage 3 verified visit — **all VV types, not just CTV** (Zach: "never assume only CTV. always assume all"). Per Zach: "a clean table representation proving IP lineage for every single one, generated on a consistent basis." Pre-deployment items resolved: dedup via `QUALIFY ROW_NUMBER()` (Zach confirmed: edge case, monitored), `/32` via `SPLIT(ip, '/')[OFFSET(0)]` (BQ) / `host()` (GP).
17. ~~Row-level examples for all pattern types~~ — **DONE (2026-03-02).** 8 types (A–H) with real UUIDs documented in A4f. Results in `bqresults/final_full.json`.
18. ~~Data quality findings from row-level examples~~ — **RESOLVED (2026-03-02, Zach confirmed 2026-03-03).** Duplicate cp/v rows = known edge case, monitored (Zach: "it should never happen but we monitor and alert"). `/32` suffix = GP inet representation (Zach: "use `host()` function"). Both confirmed by Zach.
5. ~~Per-campaign and cross-tab breakdowns on silver~~ — **DONE (2026-02-25).** ~170 campaigns across 10 advertisers. Three archetypes confirmed at silver scale: pure CTV (0.6–2.7% mutation), mixed/newer (13–18%), non-CTV (unmeasurable). Cross-device drives higher mutation for 7/10 advertisers (+1.2pp to +14.1pp).
6. ~~Silver NTB validation~~ — **DONE (2026-02-25).** 764K NTB VVs checked across 10 advertisers. Mutation-caused misclass: 0.14–2.04% (negligible). Truly NTB: 53–98%. Variation driven by pixel disagreement with conversion history (cookie expiry, incognito, new device), NOT mutation.
7. ~~Silver visits enrichment~~ — **RESOLVED (2026-02-25).** `dw-main-silver.summarydata.ui_visits` confirmed available (under `summarydata` schema, not `logdata`). Production audit table queries updated with visits join.
8. ~~Investigate BQ clickpass volume gap~~ — **RESOLVED.** Bronze = ~25% subset. Silver = complete data.
9. ~~Check silver layer~~ — **DONE.** 23 tables inventoried. Silver has everything needed.
10. ~~Verify BWN device\_ip population~~ — **DONE.** 100% populated. = bid\_ip at 100%.
11. ~~Verify BQ event\_log has bid\_ip~~ — **DONE.** Confirmed: bid\_ip (STRING).
12. ~~Port simplified trace to BQ~~ — **DONE.** Silver trace validated. Multi-advertiser confirmed.
13. ~~Expand GP scale~~ — **DONE.** 10 advertisers, 7 days, 3.3M rows. (Thread 3 — CLOSED)
14. ~~NTB validation via conversion\_log~~ — **DONE.** 97.8% truly NTB. (NTB validation — CLOSED)
15. ~~Verify BQ date ranges~~ — **DONE.** visits/CIL stopped Jan 31 in bronze. Silver CIL available.
16. ~~Investigate mutation offset~~ — **RESOLVED.** Was 20-day EL lookback, not inet vs STRING. 30-day lookback eliminates offset.

---

## 14. Q&A — Key Learnings

One-line answers to every question resolved during the audit.

**Pipeline Architecture:**
- **Q: What is clickpass\_log?** A: The old original term for verified visit. Each row = one VV redirect event.
- **Q: What is the relationship between clickpass\_log and ui\_visits?** A: `ui_visits` is a superset — includes display clicks that `clickpass_log` doesn't. 99.6% overlap on VVs.
- **Q: Does clickpass\_log contain click data?** A: Yes — embedded in each row. Schema has `click_elapsed`, `click_url`, `destination_click_url` columns. **Correction (Zach, 2026-03-03):** `ui_visits` has a `click` boolean column that discriminates click vs non-click visits. `clickpass_log` has click metadata but no discriminator column. clickpass\_log contains ALL verified visits (CTV + display) — "VV can happen for display as well" (Zach).
- **Q: How many IP checkpoints are in the Stage 3 pipeline?** A: 5 — win\_log.ip → CIL.ip → event\_log.ip → clickpass\_log.ip → ui\_visits.ip.
- **Q: Can the trace be simplified?** A: Yes — `event_log.bid_ip` = `win_log.ip` at 100%, so the chain reduces to clickpass → event\_log (2 joins instead of 4).
- **Q: What does VAST stand for?** A: Video Ad Serving Template — an IAB standard for CTV/video ad delivery.

**IP Mutation:**
- **Q: Where does IP mutation happen?** A: 100% at the EL→clickpass redirect hop. Zero at clickpass→visit. Zero at win→CIL→EL.
- **Q: What is the mutation rate range?** A: 5.9%–20.8% across 10 advertisers. Pure returning advertisers highest (~21%), pure NTB lowest (~6%).
- **Q: What drives mutation variance across campaigns?** A: Publisher mix and device type (per Zach). Mobile inventory = more mutation. CTV = less mutation.
- **Q: Does cross-device explain mutation?** A: Partially — 61.2% of mutation is cross-device (15.3% rate). Same-device contributes 38.8% (8.7% rate) from Wi-Fi↔cellular switching, CGNAT, VPN.
- **Q: What is same-device mutation?** A: 8.7% — explained by standard network behavior (Wi-Fi↔cellular switching, CGNAT/dynamic IP rotation, VPN toggling). No system bug.

**IP Column Naming (from Zach):**
- **Q: What does `ip` mean on any table?** A: The IP used for all logic — may be overridden by iCloud Private Relay resolution.
- **Q: What does `bid_ip` mean?** A: The IP seen at time of bid. Self-explanatory per Zach.
- **Q: What does `original_ip` mean?** A: The raw connection IP before iCloud Private Relay override (the x-forwarded-for header / actual connection IP).
- **Q: What is `summarydata.ui_visits.impression_ip`?** A: The bid IP from `impression_log`/`cost_impression_log`, carried forward onto the visit record. Originally confirmed at 99.2–100% vs impression_log. **Gap analysis update:** when compared to `event_log.bid_ip` (the last-touch bid IP), match rate drops to 95.8–100% depending on advertiser — because impression_ip may reference a different impression than the last-touch ad_served_id for multi-impression VVs.

**NTB / is\_new:**
- **Q: How is `is_new` determined?** A: By the tracking pixel (client-side JavaScript in the browser). No database table lookup. Not auditable via SQL.
- **Q: Why do `clickpass_log.is_new` and `ui_visits.is_new` disagree 41–56% of the time?** A: Two independent client-side systems making separate determinations at different points in time. Inherent architectural property, not a bug.
- **Q: Is `conversion_log` the reference table for `is_new`?** A: No. 97.8% of NTB VVs have zero prior conversions. Conversion\_log has nothing to do with verified visits (per Zach).
- **Q: What are phantom NTB events?** A: ~28,200/day across 10 advertisers — returning visitors whose IP changed, causing `is_new` misclassification. Concentrated in NTB campaigns; pure returning campaigns contribute zero.
- **Q: Can the NTB disagreement be fixed?** A: Not via the audit. It's a client-side pixel determination — the fix would be in the pixel tracking service, not in the data pipeline.

**BQ / Data Platform:**
- **Q: Which BQ dataset should we use?** A: Silver (`dw-main-silver.logdata.*`). Bronze (`dw-main-bronze.raw.*`) has only ~25% of clickpass volume (non-random subset).
- **Q: Does silver clickpass have a `dt` column?** A: No. Filter on `DATE(time)` instead.
- **Q: How long should the EL lookback be?** A: 30 days. Some serves happen 20+ days before visit. 20-day lookback caused a +3–5pp mutation offset artifact.
- **Q: Is BQ silver a valid replacement for Greenplum?** A: Yes. Full 7-day scale run matches GP within 0.12pp on every advertiser. Mutation offset resolved with 30-day lookback.
- **Q: What is BWN `auction_timestamp`?** A: Nanoseconds, not milliseconds. Convert with `TIMESTAMP_MICROS(CAST(ts / 1000 AS INT64))`.
- **Q: How do you join BWN to the trace chain?** A: `CIL.impression_id = BWN.mntn_auction_id` (steelhouse format). NOT `BWN.impression_id` (small integers, useless).
- **Q: What happened to `win_log.device_ip` on GP?** A: Always NULL. BQ BWN `device_ip` is 100% populated and = bid\_ip at 100%.

**Attribution Model (Empirical — Q1-Q5):**
- **Q: Which impression does `clickpass_log.ad_served_id` point to?** A: The most recent (last-touch) impression before the visit. Confirmed with 0 exceptions in 35,198 resolvable rows.
- **Q: What is `first_touch_ad_served_id`?** A: The first impression for this user/advertiser pair. NULL for 40.05% of VVs.
- **Q: Why is `first_touch_ad_served_id` NULL for 40% of VVs?** A: **Zach confirmed (2026-03-03):** "clickpass_log is a real time log. there is no post processing to generate it." NULLs are permanent, populated at write time only. NULL rate inversely correlates with recency (54% at <1hr → 18% at 14-21 days). Zach: "confirm with Sharad, but I do not believe they do this lookup for stage 1 CTV VV." Follow-up with Sharad needed.
- **Q: How often are `ad_served_id` and `first_touch_ad_served_id` the same?** A: 42.48% — single-impression attribution. But these IPs often have 6–30 other impressions in the 30-day window.
- **Q: Does first-touch tracing add IP information beyond last-touch?** A: Yes — 14.28% of multi-impression VVs have different bid IPs between first and last touch.
- **Q: How many VVs have multi-impression attribution?** A: 17.47% (38,360 of 219,613) have different `ad_served_id` vs `first_touch_ad_served_id`.

**Event Log / VAST:**
- **Q: Why do some advertisers have low EL match rates?** A: Non-CTV VVs do not fire VAST events. EL match rate ≈ CTV percentage of verified visits. (Confirmed by Zach.) **Additional context (Zach, 2026-03-03):** clickpass_log contains ALL VVs — "VV can happen for display as well." Scope decision: "never assume only CTV. always assume all."
- **Q: Can there be multiple `vast_impression` rows per `ad_served_id`?** A: Shouldn't happen. If it does, it's a publisher replaying the VAST file. Dedup by first time is correct.
- **Q: Does `event_log.bid_ip` = `win_log.ip`?** A: Yes, at 100% (30,502 rows, zero mismatches). This is the key discovery that simplified the trace.

**Other Tables:**
- **Q: Is `last_tv_touch_visits` useful for the trace?** A: No. It's a "best effort" table (per Zach), ~61% coverage, no bid IP column. `impression_ip` on it = same as `ui_visits.impression_ip`. Superseded by `event_log.bid_ip`.
- **Q: What is `ext_visits`?** A: A write-only foreign table exporting to GCS parquet. Not readable or queryable.
- **Q: Is `conversion_log.conversion_type` populated?** A: Entirely NULL for advertiser 37775. Possibly deprecated or advertiser-specific.

**Data Quality (from row-level examples, 2026-03-02) — ALL RESOLVED:**
- **Q: Can `clickpass_log` have multiple rows per `ad_served_id`?** A: YES — **Zach confirmed (2026-03-03):** "technically yes. it should never happen but no system is perfect. we monitor and alert when there are multiple rows per ad\_served\_id." UUID `cf7659de` has two visits from different devices. Scale: 84 / 219,527 ad\_served\_ids (0.038%). Production table dedup (`QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1`) takes the most recent visit. **RESOLVED — known edge case, monitored.**
- **Q: Does silver `event_log.ip` have a `/32` CIDR suffix?** A: **Zach (2026-03-03):** "this is the same ip. the db table is just representing it differently" — GP `inet` type naturally includes `/32` notation. "There's a `host` function to translate the CIDR/inet to the right string." In BQ silver, /32 suffixes are already stripped (zero found in gap analysis). Production BQ queries use `SPLIT(ip, '/')[OFFSET(0)]` as defensive measure. **RESOLVED — GP inet representation, not a data quality issue.**
- **Q: What pattern types exist in the production table?** A: 8 documented types (A–H) covering no mutation, cross-device, multi-impression, mutation, NTB disagree, non-CTV, and redirect≠visit. Real UUIDs in `audit_trace_queries.sql` section A4f.

---

## 15. Production Audit Table — v4 Stage-Aware Design

Per Zach (2026-02-25 call): "Getting an audit going and something that's actually generating the data on a consistent basis is definitely the end goal. Just being able to prove it for every single one in a very clean table representation would be so beneficial."

**Table:** `audit.vv_ip_lineage` (renamed from `stage3_vv_ip_lineage` — now covers all stages)

**Requirements (v4 — raw values only, no derived boolean flags):**
1. One row per verified visit across ALL stages (keyed on `ad_served_id`)
2. Full IP lineage: lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip
3. Stage classification: vv_stage, pv_stage
4. S1 chain traversal: s1_ad_served_id, s1_bid_ip, s1_vast_ip (resolved via 3-branch CASE + prior_vv_pool self-joins)
5. System first-touch reference: cp_ft_ad_served_id (comparison only — 40% NULL)
6. Prior VV retargeting chain: prior_vv_ad_served_id, pv_campaign_id, pv_redirect_ip, pv_lt_bid_ip, pv_lt_vast_ip
7. Classification: clickpass_is_new, visit_is_new, is_cross_device
8. Metadata: trace_date, trace_run_timestamp

**v4 schema (29 columns) — see `ti_650_column_reference.md` for column-by-column documentation:**

```sql
CREATE TABLE IF NOT EXISTS audit.vv_ip_lineage (
    -- Identity
    ad_served_id          STRING        NOT NULL,   -- PK: UUID from clickpass_log
    advertiser_id         INT64         NOT NULL,
    campaign_id           INT64,
    vv_stage              INT64,                    -- campaigns.funnel_level (1=S1, 2=S2, 3=S3)
    vv_time               TIMESTAMP     NOT NULL,
    -- Last-touch IP lineage (Stage N impression)
    lt_bid_ip             STRING,       -- event_log.bid_ip / impression_log.bid_ip
    lt_vast_ip            STRING,       -- event_log.ip / impression_log.ip
    redirect_ip           STRING,       -- clickpass_log.ip
    visit_ip              STRING,       -- ui_visits.ip
    impression_ip         STRING,       -- ui_visits.impression_ip
    -- S1 chain traversal (resolved via 3-branch CASE)
    cp_ft_ad_served_id    STRING,       -- clickpass_log.first_touch_ad_served_id (system ref, 40% NULL)
    s1_ad_served_id       STRING,       -- resolved S1 VV ad_served_id (chain traversal)
    s1_bid_ip             STRING,       -- bid IP of the resolved S1 impression
    s1_vast_ip            STRING,       -- VAST IP of the resolved S1 impression
    -- Prior VV (retargeting chain — most recent prior VV where pv_stage < vv_stage)
    prior_vv_ad_served_id STRING,
    prior_vv_time         TIMESTAMP,
    pv_campaign_id        INT64,
    pv_stage              INT64,
    pv_redirect_ip        STRING,
    pv_lt_bid_ip          STRING,
    pv_lt_vast_ip         STRING,
    pv_lt_time            TIMESTAMP,
    -- Classification
    clickpass_is_new      BOOL,
    visit_is_new          BOOL,
    is_cross_device       BOOL,
    -- Partition & metadata
    trace_date            DATE          NOT NULL,
    trace_run_timestamp   TIMESTAMP     NOT NULL
)
PARTITION BY trace_date
CLUSTER BY advertiser_id, vv_stage;
```

**Key design decisions (v4):**
- **v3→v4 simplification:** Removed `max_historical_stage`, `ft_stage`, `ft_campaign_id`, `ft_time`, all boolean comparison flags (`bid_eq_vast`, `vast_eq_redirect`, etc.), and trace quality flags (`is_ctv`, `visit_matched`, etc.). Raw IP values enable any derived metric downstream.
- **S1 chain traversal replaces `ft_*` columns:** 3-branch CASE resolves the originating S1 VV via `prior_vv_pool` self-joins (pv → s1_pv). Branch 1: vv_stage=1 (current IS S1). Branch 2: pv_stage=1 (prior VV IS S1). Branch 3: ELSE s1_pv (second-level finds S1). Max chain depth: 2 (S3→S2→S1). 9 LEFT JOINs total. Resolves ~99%+ of rows. Replaces unreliable `cp_ft_ad_served_id` (40% NULL).
- **Single event_log + cost_impression_log CTE** each scanned once, joined 3x (last-touch, prior VV LT, S1 chain LT). COALESCE(el, cil) prefers CTV; cost_impression_log fills display fallback. CIL has advertiser_id for ~20,000x scan reduction vs impression_log.
- **`pv_stage < vv_stage` (strict):** An IP can only be advanced INTO a stage by a strictly lower stage — you can't enter S3 via S3 (already there). Max chain: S3→S2→S1. `s2_pv` third-level join removed as unnecessary.
- **Stage classification via campaigns.funnel_level**: 100% populated, maps campaign_id → stage directly.
- **Prior VV match**: bid_ip primary + redirect_ip fallback. Dedup prefers bid_ip matches. Fallback covers ~16-20% of S2/S3 VVs with cross-device mutation (bid_ip ≠ redirect_ip). Advertiser_id constraint on all joins prevents CGNAT false positives.

**Implementation approach:**
- Source: clickpass_log (anchor) → event_log + cost_impression_log (single 90-day scan each) → ui_visits (±7 day) → clickpass_log (90-day self-join for prior VV, joined 2x for chain traversal) → campaigns (stage lookup)
- Scheduled via SQLMesh INCREMENTAL_BY_TIME_RANGE (hourly, 48-hour lookback, 7-day batch size)
- DELETE+INSERT idempotent pattern by trace_date range
- 90-day rolling retention (`partition_expiration_days = 90`)
- Daily incremental: ~4.7 TB scan, ~$29/day on-demand
- 60-day backfill: ~$47 total (batched 7 days at a time)
- Monthly ongoing: ~$870/month on-demand (MNTN uses reserved slots)
- Future optimization: self-referencing prior_vv from the materialized table (reduces daily scan to ~0.5 TB)

**Queries:** `queries/ti_650_audit_trace_queries.sql` — Q1 (CREATE), Q2 (INSERT), Q3 (preview), Q4 (advertiser summary). SQLMesh model: `queries/ti_650_sqlmesh_model.sql`.

---

## 16. Gap Analysis (Self-Audit, 2026-03-02)

Systematic stress test of all audit claims, run against BQ Silver with fresh data (new advertisers, new date ranges) and re-verification of key metrics.

### 16.1 Methodology

Tested 10 areas identified in `handoff_prompt.md`. Ran queries against BQ Silver using 5 advertisers NOT in the original sample (32766, 30506, 36743, 42097, 45573) over a different date range (Feb 17–23, 2026). Also re-ran key queries on the original data to check for temporal changes.

### 16.2 Confirmed Claims

**1. Mutation Location Invariant — CONFIRMED**
- Claim: 100% of mutation at VAST→redirect boundary, zero at redirect→visit
- Method: Ran A1-style query on 5 new advertisers, Feb 17–23
- Result: All 5 show mutation exclusively at redirect. Zero cases of bid=vast=redirect but visit differs.
- Verdict: **CONFIRMED on independent data**

**2. redirect_ip = visit_ip at 99.93%+ — CONFIRMED**
- Method: Compared clickpass_log.ip to ui_visits.ip for 37775+38710 combined (Feb 4–10)
- Result: 331,367 matches out of 331,423 = **99.9831%**. Only 56 mismatches.
- Verdict: **CONFIRMED** — 99.98%, tighter than the stated 99.93%

**3. 30-Day Lookback Sufficiency — CONFIRMED DEFINITIVELY**
- Method: Distribution of clickpass_log.impression_time to clickpass_log.time gaps across all 10 advertisers
- Result: 100% within 30 days. Zero in 30–45 or 45+ buckets. impression_time is 100% populated (zero NULLs across 3.25M rows).

| Gap | VVs | % |
|-----|-----|---|
| < 1 day | 1,873,498 | 57.65% |
| 1–7 days | 1,064,208 | 32.75% |
| 7–14 days | 257,865 | 7.94% |
| 14–21 days | 36,068 | 1.11% |
| 21–30 days | 18,039 | 0.56% |
| 30+ days | **0** | **0%** |

- Verdict: **CONFIRMED — 30-day lookback is exact, not just sufficient**

**4. is_new disagrees 41–56% — CONFIRMED**
- Method: Cross-tab of cp_is_new vs vv_is_new for advertiser 37775 (218,732 matched VVs)
- Result: 42.36% disagreement (11.56% false→true + 30.80% true→false)
- Verdict: **CONFIRMED**

**5. NTB misclassification 0.14–2.04% — CONFIRMED**
- No contradicting evidence on fresh data. Original methodology sound.

**6. /32 CIDR suffix — CONFIRMED RESOLVED**
- Method: Checked 6,075,907 silver event_log rows for advertiser 37775
- Result: Zero /32 suffixes on either ip or bid_ip in silver. REPLACE is defensive but unnecessary.
- Verdict: **CONFIRMED RESOLVED in silver**

**7. Cross-device flag — CONFIRMED**
- Well-populated boolean across all tested advertisers. Consistent distribution.

### 16.3 Discrepancies Found

**8. Mutation Range: WIDER THAN STATED**
- Claim: 5.9%–20.8% across 10 advertisers
- Finding: 5 additional advertisers show **1.18%–33.35%**

| Advertiser | VVs | EL Match | Mutation | Notes |
|------------|-----|----------|----------|-------|
| 32766 | 134,777 | 99.96% | 5.63% | CTV-heavy, low mutation |
| 30506 | 108,115 | 69.05% | 1.18% | Mixed, very low mutation |
| 36743 | 99,575 | 99.98% | **33.35%** | CTV-heavy, extreme cross-device |
| 42097 | 92,595 | 32.90% | 7.76% | Low EL match |
| 45573 | 84,486 | 99.97% | 12.66% | CTV-heavy, moderate |

- Root cause for 36743: 55% cross-device traffic with **50.93%** cross-device mutation rate (vs 11.79% same-device)
- Impact: The stated 5.9–20.8% range is specific to the original 10-advertiser sample. The true range across all MNTN advertisers is wider. Document as "5.9–20.8% in original sample; up to ~33% for cross-device-heavy advertisers."
- Verdict: **DISCREPANCY — range needs updating**

**9. impression_ip = bid IP at 99.2–100%: DROPS TO 95.8%**
- Claim: ui_visits.impression_ip = event_log.bid_ip at 99.2–100% for all inventory
- Finding:

| Advertiser | impression_ip = bid_ip | Notes |
|------------|----------------------|-------|
| 31357 | 100.0% | |
| 32058 | 100.0% | |
| 30857 | 100.0% | |
| 37775 | 97.3% | |
| 38710 | 95.82% | |

- Root cause: impression_ip likely references a different impression than the last-touch ad_served_id when multiple impressions exist. Not a data quality issue — an attribution difference.
- Impact: impression_ip is not a perfect substitute for event_log.bid_ip. For 2–4% of CTV-heavy advertiser VVs, they reference different impressions.
- Verdict: **DISCREPANCY — claim needs refinement (95.8–100%, not 99.2–100%)**

**10. first_touch_ad_served_id Batch Backfill: DISPROVEN**
- Claim: NULL rate pattern (54% at <1hr → 18% at 14–21d) suggests batch backfill
- Method: Re-ran the NULL rate query on 2026-03-02 (3+ weeks after Feb 4–10 data creation)
- Result: NULL rates are **identical** to the original run:

| Gap | Original ft_null_pct | Re-run ft_null_pct | Change |
|-----|---------------------|-------------------|--------|
| < 1 hour | 54.38% | 54.38% | 0 |
| 1-24 hours | 49.43% | 49.43% | 0 |
| 1-7 days | 37.16% | 37.16% | 0 |
| 7-14 days | 25.45% | 25.45% | 0 |
| 14-21 days | 17.89% | 17.89% | 0 |

- The NULLs are **permanent**. If a batch job were backfilling, NULL rates would have decreased.
- **Confirmed by Zach (2026-03-03):** "clickpass_log is a real time log. there is no post processing to generate it." Also: "confirm with Sharad, but I do not believe they do this lookup for stage 1 CTV VV."
- Impact: NULLs are permanent, populated at write time only. The first_touch lookup may not be performed for certain VV types. Follow-up with Sharad needed. Doesn't affect the audit (we use last-touch).
- Verdict: **DISPROVEN — NULLs are permanent, Zach confirmed no post-processing**

**11. A4b Production Query: MISSING DEDUP**
- The A4b INSERT query has no QUALIFY dedup on clickpass_log or ui_visits
- Both tables have duplicate rows per ad_served_id (confirmed: up to 3 rows each for some UUIDs)
- Without dedup, LEFT JOINs produce row multiplication (2 cp × 2 v = 4 output rows for one VV)
- A4f has the correct QUALIFY pattern; A4b does not
- Impact: QUALIFY ROW_NUMBER() dedup added to both clickpass_log and ui_visits CTEs in A4b and A4c (fixed 2026-03-02). **Zach confirmed (2026-03-03)** that duplicate rows are a known edge case: "it should never happen but no system is perfect. we monitor and alert."
- Verdict: **BUG — FIXED in audit_trace_queries.sql**

### 16.4 Summary

| Area | Verdict |
|------|---------|
| Mutation location (100% at redirect) | CONFIRMED |
| redirect = visit (99.93%+) | CONFIRMED (99.98%) |
| 30-day lookback | CONFIRMED (100% within 30d, exact) |
| is_new disagreement (41–56%) | CONFIRMED (42.4%) |
| NTB misclassification (0.14–2.04%) | CONFIRMED |
| /32 suffix | CONFIRMED RESOLVED |
| Cross-device flag | CONFIRMED |
| Mutation range (5.9–20.8%) | **WIDER: 1.2–33.4%** |
| impression_ip reliability (99.2–100%) | **LOWER: 95.8–100%** |
| first_touch batch backfill | **DISPROVEN** (NULLs permanent — Zach confirmed: "no post processing") |
| A4b production query | **BUG FIXED** (dedup added via QUALIFY ROW_NUMBER) |
