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

## Quick Reference: Reading a S3 VV Row

For a Stage 3 VV, the three impression IDs and their IPs are:

| Slot | Impression ID column | Impression IP columns | Coverage |
|------|---------------------|-----------------------|---------|
| **S(N)** (this VV) | `ad_served_id` | `lt_bid_ip`, `lt_vast_ip` | ~100% |
| **Prior VV** | `prior_vv_ad_served_id` | `pv_lt_bid_ip`, `pv_lt_vast_ip` | ~95%+ (90-day window) |
| **S1 (chain traversal)** | `s1_ad_served_id` | `s1_bid_ip`, `s1_vast_ip` | ~99%+ (4-branch CASE) |
| **S1 (system shortcut)** | `cp_ft_ad_served_id` | — | ~60% (comparison ref only) |

> **Source clarity:** `cp_ft_ad_served_id` is what the MNTN attribution system stored in `clickpass_log` at VV time — retained as a comparison reference only (NULL ~40% of the time). `s1_ad_served_id` is resolved via in-query chain traversal (`prior_vv_pool` JOINs) — this is the primary audit-trail S1 field (~99%+ populated).

> `ad_served_id` in `clickpass_log` is the same UUID that appears in `event_log` and `impression_log` — it is the impression ID. One ad serve = one `ad_served_id` flowing through every downstream log.

> **All VV stages are anchor rows.** `cp_dedup` does not filter by stage. S1-only, S2→S1, S3→S3→S2→S1 chains are all present as rows.

---

## 1. Identity

| Column | Type | Description |
|--------|------|-------------|
| `ad_served_id` | STRING | UUID of the verified visit **and** the impression that triggered it. Same UUID appears in `clickpass_log`, `event_log`, and `impression_log`. Primary key. |
| `advertiser_id` | INT64 | Advertiser. |
| `campaign_id` | INT64 | Campaign that received credit for this VV (last-touch). |
| `vv_stage` | INT64 | Stage of `campaign_id` per `campaigns.funnel_level`. 1=S1, 2=S2, 3=S3. |
| `vv_time` | TIMESTAMP | When the verified visit was recorded (`clickpass_log.time`). |

---

## 2. Last-Touch Impression IPs (Stage N)

The impression that directly triggered this VV. IPs traced through each hop.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `lt_bid_ip` | STRING | `event_log.bid_ip` (CTV) or `impression_log.bid_ip` (display) | IP at auction (RTB bid). |
| `lt_vast_ip` | STRING | `event_log.ip` (CTV) or `impression_log.ip` (display) | IP at ad playback. **Mutation occurs here** — when `lt_vast_ip ≠ redirect_ip`, the IP changed between ad playback and site visit. |
| `redirect_ip` | STRING | `clickpass_log.ip` | IP at the redirect (clickpass pixel). |
| `visit_ip` | STRING | `ui_visits.ip` | IP recorded at site visit. |
| `impression_ip` | STRING | `ui_visits.impression_ip` | IP the visit system attributed to the impression. |

**Reading the IP chain:** `lt_bid_ip → lt_vast_ip → redirect_ip → visit_ip`
All four should match for a clean, same-device non-mutated visit. Any difference indicates cross-device, VPN switch, or CGNAT.

---

## 3. S1 (First-Touch) Impression — Chain Traversal Resolved

The Stage 1 impression that started this IP's funnel. Resolved via 4-branch CASE backed by `prior_vv_pool` chain traversal — NOT dependent on the system-written `cp_ft_ad_served_id` field.

**Resolution logic (4-branch CASE):**

| Branch | Condition | Source |
|--------|-----------|--------|
| 1 | `vv_stage = 1` | Current VV IS the S1 impression — use `ad_served_id` and `lt_*` |
| 2 | `pv.pv_stage = 1` | Prior VV IS S1 — use `prior_vv_ad_served_id` and `pv_lt_*` |
| 3 | `s1_pv.pv_stage = 1` | 2nd-hop join finds S1 — use `s1_pv.prior_vv_ad_served_id` |
| 4 | ELSE | 3rd-hop join (`s2_pv`, pv_stage=1 required) — use `s2_pv.prior_vv_ad_served_id` |

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `cp_ft_ad_served_id` | STRING | `clickpass_log.first_touch_ad_served_id` | System-recorded S1 impression ID at VV write time. NULL ~40% of VVs. **Retained as comparison reference only** — use `s1_ad_served_id` for analysis. |
| `s1_ad_served_id` | STRING | 4-branch chain traversal | S1 impression ID resolved via `prior_vv_pool` chain traversal. ~99%+ populated. NULL only for 4+ hop chains or impressions outside 90-day lookback. |
| `s1_bid_ip` | STRING | `event_log` (CTV) or `impression_log` (display) | IP at auction for the S1 impression — audit trail lookup on the resolved `s1_ad_served_id`. |
| `s1_vast_ip` | STRING | `event_log` (CTV) or `impression_log` (display) | IP at ad playback for the S1 impression. |

**Validation:** Where both `cp_ft_ad_served_id` and `s1_ad_served_id` are non-NULL, they should agree. Use this as a spot-check. The `s1_*` columns work independently of the system-recorded value.

---

## 4. Prior VV (Stage Advancement Trigger)

The most recent prior VV whose redirect IP matches this VV's bid IP. This is the VV that advanced this IP into the current stage — e.g. for a Stage 3 VV, this is the Stage 2 VV that triggered S3 targeting.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `prior_vv_ad_served_id` | STRING | `clickpass_log` (self-join) | Impression ID of the prior VV's last-touch impression (same UUID as `ad_served_id` on that row). NULL if no prior VV found in the 90-day lookback. |
| `prior_vv_time` | TIMESTAMP | `clickpass_log` | When the prior VV occurred. |
| `pv_campaign_id` | INT64 | `clickpass_log` | Campaign ID of the prior VV. |
| `pv_stage` | INT64 | `campaigns.funnel_level` | Stage of the prior VV. |
| `pv_redirect_ip` | STRING | `clickpass_log.ip` | Redirect IP of the prior VV (the IP that was added to the next-stage targeting segment). |
| `pv_lt_bid_ip` | STRING | `event_log` (CTV) or `impression_log` (display) | Bid IP of the prior VV's impression — **our audit trail lookup**. |
| `pv_lt_vast_ip` | STRING | `event_log` (CTV) or `impression_log` (display) | IP at ad playback for the prior VV's impression — **our audit trail lookup**. |
| `pv_lt_time` | TIMESTAMP | `event_log` or `impression_log` | When the prior VV's impression was served. |

**Prior VV match logic:** We match on `prior_vv_pool.ip = lt_bid_ip` (prior VV's redirect IP = this VV's bid IP, ~94% accurate) and `pv_stage <= vv_stage` (same stage or lower). This supports full chain traversal: a S3 VV can point to a previous S3 VV as its prior VV. The longest possible chain is S3 VV → S3 VV → S2 VV → S1 VV. Stage 3 is the terminal stage, so chains never grow beyond this. `pv_stage` on each row tells you the stage of that prior VV. When pv_stage=1 for a S3 row, `prior_vv_ad_served_id` and `cp_ft_ad_served_id` may point to the same impression UUID.

**Every VV is a row, chain is traversable:** Each VV for an IP is its own row. Following `prior_vv_ad_served_id` hops traces the full sequence of VVs for that IP backward in time, terminating at the S1 VV.

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

- **`cp_ft_ad_served_id` NULL (~40% of VVs):** The system did not record a first-touch impression. Written at VV time and cannot be backfilled. IP mutation is a contributing factor (~15% of NULLs) but not the primary driver. **Use `s1_ad_served_id` instead** — chain traversal resolves S1 for ~99%+ of rows regardless.
- **`s1_ad_served_id` NULL (rare, <1%):** Chains deeper than 3 hops (e.g. S3→S3→S3→S3→S1) fall through all CASE branches. Extremely rare in practice; the 4-branch CASE covers all validated permutations for advertiser 37775 over a 7-day window.
- **Prior VV match uses `redirect_ip = bid_ip` (~94% accurate):** Targeting uses VAST IP to populate segments. `redirect_ip ≈ lt_vast_ip` in 94% of cases, so this is a close proxy.
- **Display vs CTV sources:** CTV impressions are sourced from `event_log` (`event_type_raw = 'vast_impression'`). Non-viewable display impressions appear **only** in `impression_log` — they never generate a VAST event. The query joins both and prefers `event_log` via `COALESCE(el, il)`. CTV and display have no attribution preference — treated identically in the last-touch stack. (Confirmed with Sharad/ATT, 2026-03-06.) If both are NULL, the impression was not found in either log (rare edge case).
- **`clickpass_is_new` / `visit_is_new`:** Client-side JavaScript. Not auditable via SQL.
- **90-day retention:** Partitions older than 90 days are automatically dropped.
