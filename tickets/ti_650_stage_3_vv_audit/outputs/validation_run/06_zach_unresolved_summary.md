# TI-650: 2 Genuinely Unresolved S3 VVs — For Zach

**Context:** Validation run on 146,900 S3 VVs across 10 advertisers (Mar 16-22, 2026). 99.95% resolved with 365-day lookback. All-time clickpass scan resolved 13 more. 60 had no bid_ip (bid_logs TTL). Of the remaining 4 "truly unresolved," campaign creation date analysis showed 2 are likely lookback-window issues (campaign existed >365d). These **2 are genuinely unexplained** — the S1 campaign was created recently enough that our all-time scan should have found the prior VV.

---

## VV 1: Ferguson Home — campaign_group 106777

| Field | Value |
|-------|-------|
| **ad_served_id** | `8ae132b0-8566-406b-aaf3-e3a0b73423e6` |
| **advertiser** | Ferguson Home (31276) |
| **campaign_group** | 106777 — `fh_national_engagement_acquire_2573_none_brand_stv_midfunnelconsideration` |
| **campaign** | 521942 — `Multi-Touch - Plus` |
| **S3 VV time** | 2026-03-22 12:57:37 |
| **clickpass_ip** | 174.202.4.80 |
| **bid_ip** | 174.202.4.80 (same as clickpass — no IP mutation) |
| **S1 campaign created** | 2025-12-18 (95 days before VV) |
| **Max possible lookback** | 95 days |

**What we checked:** Searched clickpass_log ALL TIME for any prior S1 or S2 VV where `clickpass_ip = 174.202.4.80` in campaign_group 106777. No match found.

**Question for Zach:** This IP (174.202.4.80) had an S3 bid placed on it, meaning it was in the S3 targeting segment (tmul_daily). To enter the S3 segment, it MUST have had a prior S1 VAST impression. But no prior VV exists in clickpass_log for this IP + campaign_group. Did this IP have a VAST impression that never resulted in a VV? Or was the IP added to the segment through a path we're not tracking?

---

## VV 2: FICO — campaign_group 107447

| Field | Value |
|-------|-------|
| **ad_served_id** | `e87853c7-6e1c-4313-982b-6507cc2c539b` |
| **advertiser** | FICO (37056) |
| **campaign_group** | 107447 — `FY26_Croud_myFICO_US_Direct_MNTN_CTV_CTV_Mixed_3P_MM` |
| **campaign** | 525930 — `Beeswax Television Multi-Touch Plus` |
| **S3 VV time** | 2026-03-19 00:18:08 |
| **clickpass_ip** | 172.56.154.242 |
| **bid_ip** | 172.56.154.242 (same as clickpass — T-Mobile CGNAT range) |
| **S1 campaign created** | 2026-01-02 (80 days before VV) |
| **Max possible lookback** | 80 days |

**What we checked:** Searched clickpass_log ALL TIME for any prior S1 or S2 VV where `clickpass_ip = 172.56.154.242` in campaign_group 107447. No match found.

**Question for Zach:** bid_ip is 172.56.154.242 — T-Mobile CGNAT range. CGNAT IPs rotate across sessions. The prior S1 VV that put this IP into the targeting segment may have been on a different 172.56.x.x IP at the time of the S1 impression, and the IP rotated to 172.56.154.242 by the time the S3 bid was placed. Is this a known limitation of CGNAT targeting?

---

## Questions for Zach

### On the 2 genuinely unresolved VVs:
1. **Ferguson (174.202.4.80, campaign_group 106777):** This IP was bid on for S3, so it was in the targeting segment. The S1 campaign in this group was created 2025-12-18 (95 days before the VV). We searched clickpass_log all-time for any prior S1/S2 VV with this IP in this campaign_group — nothing. **Is it possible this IP had an S1 VAST impression that never resulted in a site visit (VV)?** If the user saw the ad but never visited the site at S1/S2, they'd be in the targeting segment but have no clickpass_log record.

2. **FICO (172.56.154.242, campaign_group 107447):** T-Mobile CGNAT IP. Same situation — bid_ip exists but no prior VV in clickpass_log. **Could CGNAT IP rotation explain this?** The IP that entered the S3 segment (via tmul_daily) might have been a different 172.56.x.x at the time of the S1/S2 VV.

### On NO_BID_IP (60 VVs):
3. **bid_logs TTL confirmed:** We tested 10 NO_BID_IP ad_served_ids — all have impression_log records but bid_logs records are gone (even with no time filter). impression_log.ip for these is internal 10.105.x.x NAT. **What is the actual bid_logs retention policy in Beeswax?** We documented 90 days but want to confirm.

4. **Is there another path to bid_ip?** Since bid_logs purges, is there any other table that stores the external IP for a given auction_id / ttd_impression_id? (win_logs.ip perhaps — but for these NO_BID_IP cases, do win_logs also get purged?)

### On the audit approach:
5. **The S3 targeting path assumption:** We assume every S3 VV's bid_ip must match a prior VV's clickpass_ip in the same campaign_group. **Is there any path into the S3 targeting segment that doesn't go through a prior site visit?** (e.g., direct segment upload, CRM match without a VV)

6. **Campaign creation date as max lookback:** We used `MIN(create_time)` from the S1 campaign in each campaign_group as the maximum possible lookback. **Is this correct — can an impression exist before the campaign was created?** (e.g., if a campaign was duplicated/migrated from another group)

---

## Google Sheets data

`outputs/validation_run/06_truly_unresolved_for_zach.csv` — 2 rows with all pipeline IPs, timestamps, campaign metadata, S1 campaign creation date. Paste directly into Sheets.

---

## Query to reproduce

`queries/validation_run/05_unresolved_s3.sql` — runs the full resolution logic and then does an all-time clickpass_log scan for these VVs. The two genuinely unresolved are classified as `TRULY_UNRESOLVED` in the output.

To check if these bid_ips have ANY event_log S1 impression (even without a resulting VV):

```sql
-- Check if bid_ip had a VAST impression in S1 campaign of same campaign_group
SELECT
    el.ad_served_id,
    el.campaign_id,
    c.funnel_level,
    c.campaign_group_id,
    SPLIT(el.ip, '/')[SAFE_OFFSET(0)] AS event_ip,
    el.event_type_raw,
    el.time
FROM `dw-main-silver.logdata.event_log` el
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = el.campaign_id
    AND c.deleted = FALSE AND c.is_test = FALSE
    AND c.funnel_level = 1
    AND c.objective_id IN (1, 5, 6)
WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
  AND el.time >= TIMESTAMP('2025-12-01')
  AND el.time < TIMESTAMP('2026-03-23')
  AND (
    (SPLIT(el.ip, '/')[SAFE_OFFSET(0)] = '174.202.4.80' AND c.campaign_group_id = 106777)
    OR
    (SPLIT(el.ip, '/')[SAFE_OFFSET(0)] = '172.56.154.242' AND c.campaign_group_id = 107447)
  )
ORDER BY el.time;
```
