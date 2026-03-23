# TI-650: Full Trace Table Design — Row-Per-Stage with UUID Linking

**Date:** 2026-03-19
**Status:** Draft

---

## Design Overview

Redesign from v12's flat wide layout (1 row per VV, S3+S2+S1 columns side-by-side) to a
**row-per-stage** layout where each row represents one stage in the trace chain, linked by
a shared UUID.

**Why row-per-stage:**
- Each row has the SAME columns regardless of stage — no NULL S3/S2 blocks for S1 VVs
- Impression type (CTV, Viewable Display, Non-Viewable Display) is per-stage, not per-VV
- Natural for S3→S2→S1 chain traversal
- Simpler schema (fewer columns per row vs v12's 29+ wide)

---

## Trace Examples

**T1-resolved S3 VV (3 rows):**
```
trace_uuid=abc  stage=S3  role=origin_vv         resolution=T1  → S3 VV + 5-source IPs
trace_uuid=abc  stage=S2  role=s2_bridge_vv       resolution=T1  → S2 VV + 5-source IPs
trace_uuid=abc  stage=S1  role=s1_impression       resolution=T1  → S1 impression pool match
```

**T2-resolved S3 VV (2 rows):**
```
trace_uuid=def  stage=S3  role=origin_vv         resolution=T2  → S3 VV + 5-source IPs
trace_uuid=def  stage=S1  role=s1_direct_vv       resolution=T2  → S1 VV (clickpass_ip matched)
```

**Unresolved S3 VV (1 row):**
```
trace_uuid=ghi  stage=S3  role=origin_vv         resolution=unresolved  → S3 VV + 5-source IPs
```

---

## Column Schema

### Trace Identity
| Column | Type | Description |
|--------|------|-------------|
| `trace_uuid` | STRING | Generated UUID linking all rows in the same S3→S2→S1 chain |
| `stage` | INT64 | Funnel stage: 1, 2, or 3 |
| `stage_role` | STRING | Role in the trace chain (see values below) |
| `resolution` | STRING | How this S3 VV was resolved: `T1`, `T2`, `unresolved` |
| `impression_type` | STRING | `CTV`, `Viewable Display`, `Non-Viewable Display` |

**stage_role values:**
- `origin_vv` — the S3 VV being traced (always stage=3)
- `s2_bridge_vv` — the S2 VV in the T1 chain whose clickpass_ip = S3.resolved_ip
- `s1_direct_vv` — the S1 VV in T2 whose clickpass_ip = S3.resolved_ip
- `s1_impression` — the S1 impression in the T1 chain whose IP = S2.bid_ip

### VV/Impression Identity
| Column | Type | Description |
|--------|------|-------------|
| `ad_served_id` | STRING | UUID of the VV or impression |
| `advertiser_id` | INT64 | MNTN advertiser ID |
| `campaign_group_id` | INT64 | Campaign group |
| `campaign_id` | INT64 | Campaign |
| `vv_time` | TIMESTAMP | When the VV was recorded (NULL for s1_impression rows) |

### Visit IPs (from clickpass_log — VV rows only)
| Column | Type | Description |
|--------|------|-------------|
| `clickpass_ip` | STRING | Redirect/clickpass IP from clickpass_log.ip (CIDR-stripped) |

### 5-Source Impression IPs
| Column | Type | Description |
|--------|------|-------------|
| `event_log_ip` | STRING | VAST start/impression IP (CTV) |
| `viewability_ip` | STRING | Viewability log IP (display viewable) |
| `impression_ip` | STRING | impression_log IP (all display) |
| `win_ip` | STRING | win_logs IP (via auction_id bridge) |
| `bid_ip` | STRING | bid_logs IP (via auction_id bridge) |
| `resolved_ip` | STRING | COALESCE(bid_ip, win_ip, impression_ip, viewability_ip, event_log_ip) |
| `impression_time` | TIMESTAMP | When the impression was served |

### Dimension Lookups
| Column | Type | Description |
|--------|------|-------------|
| `advertiser_name` | STRING | From advertisers.company_name |
| `campaign_group_name` | STRING | From campaign_groups.name |
| `campaign_name` | STRING | From campaigns.name |
| `channel` | STRING | `CTV` or `Display` (from campaigns.channel_id) |

### Metadata
| Column | Type | Description |
|--------|------|-------------|
| `trace_date` | DATE | DATE(vv_time) — partition key |

---

## Impression Type Classification

Determined by which 5-source tables have data for this stage's ad_served_id:

| Impression Type | Condition | Trace Path |
|-----------------|-----------|------------|
| **CTV** | event_log_ip IS NOT NULL | clickpass → event_log → win_logs → impression_log → bid_logs |
| **Viewable Display** | event_log_ip IS NULL AND viewability_ip IS NOT NULL | clickpass → viewability_log → win_logs → bid_logs |
| **Non-Viewable Display** | event_log_ip IS NULL AND viewability_ip IS NULL AND impression_ip IS NOT NULL | clickpass → impression_log → win_logs → bid_logs |

---

## Query Structure

```
-- For each S3 VV, determine resolution (T1/T2/unresolved) and generate UUID
-- UNION ALL:
--   Row 1: S3 VV itself (always present)
--   Row 2: Matching S2 VV (T1 only)
--   Row 3: Matching S1 VV/impression (T1 or T2)
```

### Key CTEs
1. `all_clickpass` — 365d lookback, all stages, 5-source trace for S3 VVs
2. `s3_classified` — each S3 VV classified as T1/T2/unresolved + matching S2/S1 ad_served_id
3. `s2_trace` — 5-source trace for the matched S2 VVs (T1 only)
4. `s1_trace` — 5-source trace for the matched S1 VVs (T2) or S1 impressions (T1)
5. `trace_output` — UNION ALL of S3 + S2 + S1 rows with shared UUID

### Cost Optimization
- S3 5-source: ±30d window (display impressions served weeks before VV)
- S2 5-source: ±30d around S2 VV times (but S2 VVs span 365d → full range)
  - Optimization: only extract S2 traces for S2 VVs that match T1 S3s
- S1 5-source: only for matched S1 VVs/impressions (small set)
- Total estimate: ~5-8 TB (vs 21 TB monolithic)

---

## Open Questions

1. **S1 impression rows for T1:** Do we extract the specific S1 impression that matched S2.bid_ip,
   or just note that an S1 impression exists? Extracting the specific impression requires a
   different S1 pool structure (keeping ad_served_id, not just aggregating by IP).

2. **Multiple matches:** When S3.resolved_ip matches multiple S2 VVs, which one do we pick?
   Proposal: most recent (MAX(vv_time)) before the S3 VV.

3. **S1/S2 VV scope:** Only include within the audit window, or include all VVs in the lookback
   as potential matches? Current: matches can be from any time in the 365d lookback.
