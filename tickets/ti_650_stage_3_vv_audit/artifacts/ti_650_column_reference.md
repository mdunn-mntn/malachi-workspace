# VV IP Lineage — Column Reference

**Table:** `{dataset}.vv_ip_lineage`
**Rows:** One per verified visit (VV), all advertisers, all stages
**Partitioned by:** `trace_date` | **Clustered by:** `advertiser_id`, `vv_stage`
**Retention:** 90 days

Each row provides a complete audit trail for one VV, linking three impressions:
1. **Last-touch (Stage N)** — the impression that triggered this VV
2. **First-touch (Stage 1)** — the S1 impression that started the funnel for this IP
3. **Prior VV** — the most recent prior VV that advanced this IP into the current stage

---

## 1. Identity

| Column | Type | Description |
|--------|------|-------------|
| `ad_served_id` | STRING | UUID of the verified visit. Primary key. From `clickpass_log`. |
| `advertiser_id` | INT64 | Advertiser. |
| `campaign_id` | INT64 | Campaign that received credit for this VV (last-touch). |
| `vv_stage` | INT64 | Stage of `campaign_id` per `campaigns.funnel_level`. 1=S1, 2=S2, 3=S3. |
| `vv_time` | TIMESTAMP | When the verified visit was recorded (`clickpass_log.time`). |

---

## 2. Last-Touch Impression IPs (Stage N)

The impression that directly triggered this VV. IPs traced through each hop.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `lt_bid_ip` | STRING | `event_log.bid_ip` | IP at auction (RTB bid). |
| `lt_vast_ip` | STRING | `event_log.ip` | IP at VAST playback. **Mutation occurs here** — when `lt_vast_ip ≠ redirect_ip`, the IP changed between ad playback and site visit. |
| `redirect_ip` | STRING | `clickpass_log.ip` | IP at the redirect (clickpass pixel). |
| `visit_ip` | STRING | `ui_visits.ip` | IP recorded at site visit. |
| `impression_ip` | STRING | `ui_visits.impression_ip` | IP the visit system attributed to the impression. |

**Reading the IP chain:** `lt_bid_ip → lt_vast_ip → redirect_ip → visit_ip`
All four should match for a clean, same-device non-mutated visit. Any difference indicates cross-device, VPN switch, or CGNAT.

---

## 3. First-Touch Impression (Stage 1)

The Stage 1 impression that first served an ad to this IP — the start of the funnel.

Because the funnel is sequential (an IP cannot enter S2 until it has a VV from S1), the first impression for any IP is always Stage 1.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `ft_ad_served_id` | STRING | `clickpass_log.first_touch_ad_served_id` | The first-touch impression ID as recorded by the MNTN attribution system at VV time. |
| `ft_campaign_id` | INT64 | `event_log` (audit join) | Campaign ID of the first-touch impression. |
| `ft_stage` | INT64 | `campaigns.funnel_level` | Always 1. |
| `ft_bid_ip` | STRING | `event_log` (audit join) | IP at auction for the S1 impression — **our audit trail lookup**, not the clickpass-stored value. |
| `ft_vast_ip` | STRING | `event_log` (audit join) | IP at VAST playback for the S1 impression — **our audit trail lookup**. |
| `ft_time` | TIMESTAMP | `event_log` (audit join) | When the S1 impression was served. |

**Source distinction:** `ft_ad_served_id` is the system's stored value (written to `clickpass_log` at VV time). `ft_bid_ip`, `ft_vast_ip`, and `ft_time` are retrieved by joining `event_log` on that ID — our independent audit of the impression. When `ft_ad_served_id` is NULL, the system did not record a first-touch (~40% of VVs; see Known Limitations).

---

## 4. Prior VV (Stage Advancement Trigger)

The most recent prior VV whose redirect IP matches this VV's bid IP. This is the VV that advanced this IP into the current stage — e.g. for a Stage 3 VV, this is the Stage 2 VV that triggered S3 targeting.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `prior_vv_ad_served_id` | STRING | `clickpass_log` (self-join) | ad_served_id of the prior VV. NULL if no prior VV found in the 90-day lookback. |
| `prior_vv_time` | TIMESTAMP | `clickpass_log` | When the prior VV occurred. |
| `pv_campaign_id` | INT64 | `clickpass_log` | Campaign ID of the prior VV. |
| `pv_stage` | INT64 | `campaigns.funnel_level` | Stage of the prior VV. |
| `pv_redirect_ip` | STRING | `clickpass_log.ip` | Redirect IP of the prior VV (the IP that was added to the next-stage targeting segment). |
| `pv_lt_bid_ip` | STRING | `event_log` (audit join) | Bid IP of the prior VV's impression — **our audit trail lookup**. |
| `pv_lt_vast_ip` | STRING | `event_log` (audit join) | VAST IP of the prior VV's impression — **our audit trail lookup**. |
| `pv_lt_time` | TIMESTAMP | `event_log` (audit join) | When the prior VV's impression was served. |

**Prior VV match logic:** We match on `prior_vv_pool.ip = lt_bid_ip` — the prior VV's redirect IP against this VV's bid IP. ~94% accurate. The targeting system uses VAST IP to populate segments, but `redirect_ip ≈ lt_vast_ip` in 94% of cases.

**3 VVs → 3 rows:** If an IP had 3 VVs that advanced it through the funnel, each VV is its own row. The prior VV column on each row points to the most recent VV that came before it on the same IP.

---

## 5. Classification

Raw values. No derived comparisons.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `clickpass_is_new` | BOOL | `clickpass_log.is_new` | NTB flag per the clickpass pixel (client-side JavaScript). |
| `visit_is_new` | BOOL | `ui_visits.is_new` | NTB flag per the ui_visits pixel (independent client-side JavaScript). |
| `is_cross_device` | BOOL | `clickpass_log.is_cross_device` | Ad was served on one device, visit recorded on another. |

**NTB note:** Both `clickpass_is_new` and `visit_is_new` are client-side JavaScript determinations. They are not auditable via SQL and disagree 41–56% of the time. This is architectural, not a data quality issue.

---

## 6. Metadata

| Column | Type | Description |
|--------|------|-------------|
| `trace_date` | DATE | Partition key. Date of the VV (`DATE(vv_time)`). |
| `trace_run_timestamp` | TIMESTAMP | When this row was written. |

---

## Key Insight: Attribution Stage vs Journey Stage

| Column | Answers |
|--------|---------|
| `vv_stage` | Which campaign's budget paid for this impression? |
| `pv_stage` | What stage was the prior VV that advanced this IP here? |

For a complete journey trace on a single IP, query all rows for that `lt_bid_ip` ordered by `vv_time`.

---

## Known Limitations

- **`ft_ad_served_id` NULL (~40% of VVs):** The system did not record a first-touch impression. Written at VV time and cannot be backfilled. IP mutation is a contributing factor (~15% of NULLs) but not the primary driver.
- **Prior VV match uses `redirect_ip = bid_ip` (~94% accurate):** Targeting uses VAST IP to populate segments. `redirect_ip ≈ lt_vast_ip` in 94% of cases, so this is a close proxy.
- **Non-CTV VVs:** `lt_bid_ip` and `lt_vast_ip` will be NULL for display inventory (uses `impression_log`, not `event_log`).
- **`clickpass_is_new` / `visit_is_new`:** Client-side JavaScript. Not auditable via SQL.
- **90-day retention:** Partitions older than 90 days are automatically dropped.
