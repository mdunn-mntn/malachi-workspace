# VV IP Lineage — Plain English Guide (v12)

Everything you need to understand `audit.vv_ip_lineage`, explain it to anyone, and answer every skeptical question about it.

> **Version note:** This document describes the **v12 target architecture** — stage-based column naming (`s3_*`/`s2_*`/`s1_*`), 2-link S1 resolution (`imp_direct` + `imp_visit`). The SQLMesh model is being updated from v11 (10-tier cascade) to v12. For the full column schema, see `ti_650_column_reference.md`.

---

## Part 1: What Actually Happens (5 events, 5 tables)

When MNTN serves an ad and a user later visits the advertiser's site, five things happen in sequence. Each event writes to a different table.

| # | Event | Table | Key field written |
|---|-------|-------|-------------------|
| 1 | **Bid wins** | `win_logs` | `auction_id`, `ip` (the bid IP) |
| 2 | **Ad is served / displayed** | `cost_impression_log` (CIL) | `ad_served_id`, `ip` (= bid IP) |
| 3 | **VAST fires** (CTV only) | `event_log` | `ad_served_id`, `bid_ip` (carried from bid), `ip` (CTV playback IP) |
| 4 | **User visits the site** | `clickpass_log` | `ad_served_id`, `ip` (redirect IP — the site visit IP) |
| 5 | **Page view recorded** | `ui_visits` | `ad_served_id`, `ip` (visit IP), `impression_ip` (bid IP carried forward) |

**The critical thread:** `ad_served_id` is the UUID that flows through every table. One ad serve = one `ad_served_id` that appears identically in CIL, event_log, clickpass_log, and ui_visits. This is how we trace one impression end-to-end.

**Display vs CTV:** Display ads do not fire VAST events. For display, step 3 uses `cost_impression_log` (CIL) instead of `event_log`. CIL.ip = bid_ip (100% validated). The query handles this with `COALESCE(event_log value, CIL value)` — CTV preferred, display (CIL) as fallback. CIL replaces `impression_log` — it has `advertiser_id` (impression_log does not), making it ~20,000x smaller for single-advertiser queries. Render IP (impression_log.ip) is lost — only differs from bid_ip 6.2% of the time (internal 10.x.x.x NAT).

**Where IP mutation happens:** 100% of IP changes happen between step 3 (VAST) and step 4 (clickpass redirect). The bid IP = VAST bid_ip = CIL IP in 100% of cases. If the IP changed, it changed at the VAST→redirect boundary. Cross-device and VPN switching cause this.

**How VVS links the visit to the impression (Sharad, confirmed):** The Verified Visit Service (VVS) does the attribution in two layers:
1. **Primary: IP match.** When a page view comes in, VVS looks for impressions served to the same IP as the page view IP. This is the main mechanism.
2. **Secondary: GA Client ID expansion.** Using the GA Client ID from the page view, VVS finds all IPs that Client ID has been seen with in the previous few days, then looks for impressions served to any of those IPs.

There are validations and filtering on each layer, but this is how a CTV ad served to one device gets linked to a site visit from another device — either they share the same IP (e.g. both on home WiFi), or the GA Client ID bridges across IPs. The `is_cross_device` flag in `clickpass_log` indicates when VVS detected the ad was served on a different device type than the visit. See: Nimeshi Fernando's "Verified Visit Service (VVS) Business Logic" Confluence doc.

---

## Part 2: The Three Stages

Stages are about **which targeting segment served this impression**, determined by the IP's event history at the time of the bid.

| Stage | What it means | What puts an IP into this segment |
|-------|---------------|----------------------------------|
| **S1** | First time this IP is in-audience for this advertiser | Campaign audience setup (customer data, lookalike, etc.) |
| **S2** | IP had a prior VAST impression (from an S1 impression) | IP's VAST event in event_log |
| **S3** | IP had a prior verified visit | IP had a clickpass_log entry (any stage) |

**Key rules from Zach:**
- S2 is populated ONLY from Stage 1 VAST IPs — not S2 or S3 impressions. S1 impression → VAST fires → IP enters S2.
- Stage 3 = "IPs that had a verified visit." That is literally the definition of Stage 3. The VV can be from any stage impression. Any VV puts the IP into S3.
- IPs are NEVER removed from prior stages. An IP in S3 is also in S2 AND S1. An IP in S2 is also in S1 but NOT necessarily S3. An IP can be in S1 without being in S2 at all — it just means it hasn't had a VAST impression yet. Stages only accumulate forward.
- Each stage is a separate campaign with separate budget. S1 = ~75-80% of budget. The bidder has no concept of stages — it just sees three independent campaigns.
- VV attribution uses the last-touch stack model: *"We put impressions on the stack. When a page view comes in, we check the top of the stack."* The most recent impression gets credit.

**Eligibility vs actual impression:** An IP being eligible for S3 (because it had a prior VV) does NOT mean it will receive an S3 impression. The bidder only knows `campaign_id`. At bid time, an IP eligible for all three stages is a candidate for all three campaigns. Which one actually wins depends on pacing, budget, and bidding factors. An IP that has reached S3 can still get served from an S1 campaign — and we can tell this happened by looking at `cost_impression_log.campaign_id` and joining to `campaigns.funnel_level`. The `vv_stage` column in the audit table reflects which campaign's impression got last-touch credit — not the IP's maximum stage.

**Attribution stage vs journey stage:** Because S1 has 75-80% of budget, a VV is frequently attributed to an S1 impression even when the IP has already progressed to S3. 20% of S1-attributed VVs are on IPs that have already reached S3.

---

## Part 3: What the Audit Table Is

**One row per verified visit. All advertisers. All stages.**

The table provides the complete IP and impression ID trace for every VV:
- The impression that triggered this VV (last touch, Stage N) — 5 IPs + timestamp + guid
- The S2 impression in this IP's funnel (cross-stage linked via bid_ip → VAST IP) — 5 IPs + timestamp + guid
- The S1 impression that started this IP's funnel (resolved via 2-link model) — 5 IPs + timestamp + guid

For a Stage 3 VV, this means three impression slots:

| Slot | Columns | What it is |
|------|---------|------------|
| **This VV's S3 impression** | `s3_bid_ip`, `s3_vast_start_ip`, `s3_vast_impression_ip`, `s3_serve_ip`, `s3_win_ip`, `s3_impression_time`, `s3_guid` | The S3 impression that triggered this VV. Linked via `ad_served_id` (deterministic). |
| **S2 impression** | `s2_ad_served_id`, `s2_bid_ip`, `s2_vast_start_ip`, `s2_vast_impression_ip`, `s2_serve_ip`, `s2_win_ip`, `s2_vv_time`, `s2_impression_time`, `s2_guid` | The prior S2 VV whose VAST IP matched this VV's `s3_bid_ip`. NULL when prior VV is S1 (no S2 step). |
| **S1 impression** | `s1_ad_served_id`, `s1_bid_ip`, `s1_vast_start_ip`, `s1_vast_impression_ip`, `s1_serve_ip`, `s1_win_ip`, `s1_impression_time`, `s1_guid`, `s1_resolution_method` | The S1 impression that started this IP's funnel — resolved via 2-link model (see Part 3b). |

> **Cross-stage linking is IP-only.** No deterministic ID links across stages. `s3_bid_ip` ≈ S2 `vast_start_ip` (the VAST IP that entered the S3 segment). `s2_bid_ip` ≈ S1 `vast_start_ip` (the VAST IP that entered the S2 segment). `first_touch_ad_served_id` links S3/S2→S1 directly but is only ~60% available. IP matching is the only reliable mechanism — same as the production bidder. See Finding #28.

> **All stages are anchor rows.** The table is not limited to S3 VVs. `cp_dedup` pulls ALL stages from `clickpass_log`. Zach confirmed: "if the dataset is still reasonable in size i think it would be good to have the info for all impressions/vv." A S1-only VV, a S2→S1 chain, or a S3→S2→S1 chain are all present as anchor rows.

> **Stage columns are stage-fixed, not VV-relative.** S3 columns are always NULL for S1/S2 VVs. S2 columns are always NULL for S1 VVs AND for S3 VVs whose prior VV is S1 (IP went S1→S3 directly, skipping S2). S1 columns are always attempted (~97%+ populated for S2/S3 VVs; structural ceiling from CRM/cross-device entry with no IP breadcrumbs).

**Does this answer Zach's exact audit request?**

Zach: *"we need an exact audit trail for a vv. that means every impression id/ip for each stage that lead to that vv."*

| Zach needs | Our table | Coverage |
|------------|-----------|----------|
| S(N) impression ID + IPs | `ad_served_id`, `s3_bid_ip`, `s3_vast_start_ip`, `s3_vast_impression_ip` | ~100% |
| S2 prior VV impression ID + IPs | `s2_ad_served_id`, `s2_bid_ip`, `s2_vast_start_ip`, `s2_vast_impression_ip` | ~95%+ in production (90-day lookback) |
| S1 impression ID (system-recorded shortcut) | `cp_ft_ad_served_id` | ~60% (system limitation — retained as comparison reference) |
| S1 impression ID via 2-link resolution | `s1_ad_served_id`, `s1_resolution_method` | S2: 99.95%, S3: 96.85% (see Part 3b) |
| S1 bid IP + VAST IPs | `s1_bid_ip`, `s1_vast_start_ip`, `s1_vast_impression_ip` | Same as `s1_ad_served_id` — populated when resolved |

**`cp_ft_ad_served_id`** is a direct shortcut to S1 that the attribution system stored at VV time — works ~60% of the time. Retained for comparison only. When NULL, `s1_ad_served_id` from the 2-link resolution fills in.

---

## Part 3b: S1 Resolution — 2-Link Model (v12)

The `s1_ad_served_id`, `s1_bid_ip`, `s1_vast_start_ip`, `s1_vast_impression_ip`, and related S1 columns are resolved via 2 LEFT JOINs. The `s1_resolution_method` column records which link succeeded.

**Why not just use `cp_ft_ad_served_id`?** It's NULL ~40% of the time and cannot be backfilled. The 2-link model resolves S2: 99.95%, S3: 96.85%.

**The 2 links + trivial case:**

| `s1_resolution_method` | Join Logic | Description | Coverage |
|------------------------|-----------|-------------|----------|
| `current_is_s1` | `vv_stage = 1` | VV itself is S1 — its impression IS the S1 impression. Trivial. | 100% of S1 VVs |
| `imp_direct` | `s1_by_vast_start.vast_start_ip = bid_ip` | S1 impression whose `vast_start_ip` matches the current VV's `bid_ip`. Primary cross-stage link. | Resolves ~95%+ of S2/S3 |
| `imp_visit` | `s1_by_vast_start.vast_start_ip = impression_ip` | S1 impression whose `vast_start_ip` matches `ui_visits.impression_ip`. Fallback — rescues 574 unique S2 + 900 unique S3. | Rescues ~4-5% |
| NULL | — | Unresolved. Structural ceiling: ~3.15% of S3 VVs (identity graph entry, no IP path exists). | — |

**Why only 2 links?** v11 had a 10-tier CASE cascade (`vv_chain_direct`, `vv_chain_s2_s1`, `imp_chain`, `imp_direct`, `imp_visit_ip`, `cp_ft_fallback`, `guid_vv_match`, `guid_imp_match`, `s1_imp_redirect`). Empirical analysis proved only `imp_direct` and `imp_visit` contribute unique resolutions — all other tiers were redundant (0 unique contributions). See `_archive/` for full tier analysis.

**How the S1 pool works:** `s1_by_vast_start` is the S1 impression pool dedup'd by `vast_start_ip` (earliest impression per IP). When a VV's `bid_ip` matches a `vast_start_ip` in this pool, we've found the S1 impression that put this IP into the targeting segment.

Zach also said: *"if there are 3 vv that put an ip in stage 3, that's 3 rows. if there's a vv in stage 3 for an ip that had 3 vv that got it into stage 3 we should use last touch."* This is exactly what the table does — one row per VV, with the most recent prior VV (strictly lower stage) selected via dedup.

---

## Part 4: Reading a Single Row

**Concrete example: a Stage 3 VV**

```
# ── 1. Identity ──
ad_served_id               = "003a01cf-5e87-40f6-..."    # S3 impression UUID
advertiser_id              = 37775
campaign_id                = 423501
vv_stage                   = 3
vv_time                    = 2026-02-04 04:58:54
vv_guid                    = "abc123..."                  # User/device cookie (persists across VVs)

# ── 2. VV Visit IPs ──
visit_ip                   = 172.59.192.138    # Page view IP (ui_visits)
impression_ip              = 172.59.192.138    # Attributed IP (ui_visits.impression_ip)
redirect_ip                = 172.59.192.138    # Site visit redirect IP (clickpass_log.ip)

# ── 3. S3 Impression IPs (this VV's impression) ──
s3_vast_start_ip           = 172.59.192.138    # CTV playback IP (last VAST callback)
s3_vast_impression_ip      = 172.59.192.138    # CTV load IP (first VAST callback)
s3_serve_ip                = 172.59.192.138    # Ad serve request IP (~93.6% = bid_ip)
s3_bid_ip                  = 172.59.192.138    # Targeting identity IP (= win_ip = segment_ip)
s3_win_ip                  = 172.59.192.138    # = bid_ip today (future-proofing)
s3_impression_time         = 2026-02-04 04:55:12
s3_guid                    = "abc123..."

# ── 4. S2 Impression IPs (prior VV at S2) ──
s2_ad_served_id            = "a4074373-..."    # Prior S2 VV's impression UUID
s2_vv_time                 = 2026-01-25 11:32:00
s2_bid_ip                  = 172.59.192.138    # S2 impression bid IP
s2_vast_start_ip           = 172.59.192.138    # S2 VAST start IP
s2_vast_impression_ip      = 172.59.192.138    # S2 VAST impression IP
s2_serve_ip                = 172.59.192.138    # S2 serve request IP
s2_win_ip                  = 172.59.192.138    # S2 win IP (= bid_ip)
s2_impression_time         = 2026-01-25 11:28:45
s2_redirect_ip             = 172.59.192.138    # S2 VV redirect IP (fallback match key)
s2_guid                    = "abc123..."

# ── 5. S1 Impression IPs (resolved via 2-link model) ──
s1_ad_served_id            = "a12e289d-..."    # S1 impression resolved via imp_direct
s1_bid_ip                  = 172.59.192.138    # Original S1 bid IP — end of the chain
s1_vast_start_ip           = 172.59.192.138    # S1 VAST start IP
s1_vast_impression_ip      = 172.59.192.138    # S1 VAST impression IP
s1_serve_ip                = 172.59.192.138    # S1 serve request IP
s1_win_ip                  = 172.59.192.138    # S1 win IP (= bid_ip)
s1_impression_time         = 2025-12-15 09:14:33
s1_guid                    = "abc123..."
s1_resolution_method       = "imp_direct"      # Resolved via S1 vast_start_ip = VV's bid_ip
cp_ft_ad_served_id         = NULL              # System shortcut — NULL ~40% of VVs
```

How to read it: This S3 VV was triggered by impression `003a01cf` on 2026-02-04. The IP `172.59.192.138` had a prior S2 VV (`a4074373`) on 2026-01-25. S1 was resolved via `imp_direct` — the S1 impression `a12e289d` has a `vast_start_ip` matching this VV's `s3_bid_ip`. The system's `cp_ft_ad_served_id` was NULL, but the 2-link resolution recovered the S1 impression. All IPs are the same in this example (no mutation) — when they differ, the table shows exactly where the IP changed.

---

## Part 5: Every Column Explained

> For the full schema reference (types, NULLability, exact source tables), see `ti_650_column_reference.md`. This section explains what each column **tells you** and answers common questions.

### 1. Identity — from `clickpass_log` (anchor)

**`ad_served_id`** — UUID of the impression that triggered this VV. Same UUID in event_log, CIL, clickpass_log, ui_visits. Primary key.
- *"Is this really the impression ID?"* Yes. Look at `event_log WHERE ad_served_id = this value AND event_type_raw = 'vast_impression'` — that's the exact impression.

**`advertiser_id`** — MNTN advertiser ID.

**`campaign_id`** — Campaign that received last-touch attribution for this VV.

**`vv_stage`** — Stage of the attributed campaign (`campaigns.funnel_level`). 1=S1, 2=S2, 3=S3. Set at campaign creation, never changes.

**`vv_time`** — When the verified visit was recorded (`clickpass_log.time`).

**`vv_guid`** — User/device cookie ID (`clickpass_log.guid`). Persists across multiple VVs for the same user. Matches `ui_visits.guid` 99.99%.

**`vv_original_guid`** — Pre-reattribution guid (`clickpass_log.original_guid`). Differs from `vv_guid` in ~16% of VVs (those that were reattributed). NULL when no reattribution.

**`vv_attribution_model_id`** — Attribution model used for this VV (`clickpass_log.attribution_model_id`). Values: 1-3 (non-competing), 9-11 (competing). See Part 12 for full reference.

---

### 2. VV Visit IPs — from `clickpass_log` and `ui_visits`

IPs recorded when the user visited the advertiser's site after seeing the ad.

**`visit_ip`** — Page view IP (`ui_visits.ip`). 99.93% equal to `redirect_ip` (same session).

**`impression_ip`** — The IP `ui_visits` attributed the visit to — carried forward from the impression at attribution time. Used as S1 fallback (`imp_visit` resolution tier).

**`redirect_ip`** — Clickpass redirect IP (`clickpass_log.ip`). Where IP mutation from VAST is observed. This is the IP the site visit server recorded.

---

### 3. S3 Impression IPs — this VV's impression (NULL for S1/S2 VVs)

The S3 impression that triggered this VV. Linked via `ad_served_id` (deterministic — zero IP joining).

**`s3_vast_start_ip`** — IP at VAST start callback. Fires AFTER vast_impression (last VAST callback). 99.85% = `s3_vast_impression_ip`. When differs: SSAI proxy. **Cross-stage key (merged pool: priority 1).**

**`s3_vast_impression_ip`** — IP at VAST impression callback. Fires BEFORE vast_start (first VAST callback). **Cross-stage key (merged pool: priority 2).**

**`s3_serve_ip`** — IP at ad serve request (`impression_log.ip`). 93.6% = bid_ip. When differs: 96.9% internal 10.x.x.x NAT, 3.1% AWS infra.

**`s3_bid_ip`** — Targeting identity. = win_ip = segment_ip (100% validated). The IP that entered the S3 targeting segment via `tmul_daily`. **Cross-stage link: should ≈ s2 `vast_start_ip`.**

**`s3_win_ip`** — = bid_ip today (100% validated). Kept for Mountain Bidder SSP future-proofing.

**`s3_impression_time`** — When the S3 impression was served.

**`s3_guid`** — Impression-side guid for the S3 impression.

---

### 4. S2 Impression IPs — prior VV's S2 impression (NULL for S1 VVs; NULL for S3 VVs that skipped S2)

The S2 impression in this IP's funnel. **Cross-stage link:** `s3_bid_ip` ≈ S2 `vast_start_ip` (the VAST IP that entered the S3 segment). Matched via `pv_pool_vast.match_ip = bid_ip` (merged pool: vast_start preferred, vast_impression fallback).

**`s2_ad_served_id`** — S2 VV's impression UUID. For S2 VVs: = `ad_served_id` (self). For S3 VVs: prior VV's UUID when prior is S2.

**`s2_vv_time`** — When the S2 VV occurred.

**`s2_bid_ip`** — S2 impression bid IP. **Cross-stage link to S1: should ≈ s1 `vast_start_ip`.**

**`s2_vast_start_ip`** / **`s2_vast_impression_ip`** — S2 impression VAST IPs.

**`s2_serve_ip`** / **`s2_win_ip`** — S2 impression serve request and win IPs.

**`s2_impression_time`** — When the S2 impression was served.

**`s2_campaign_id`** — S2 campaign.

**`s2_redirect_ip`** — S2 VV's redirect IP. Fallback match key for cross-device cases.

**`s2_guid`** — Impression-side guid for the S2 impression.

**`s2_attribution_model_id`** — Attribution model used for the S2 VV.

---

### 5. S1 Impression IPs — cross-stage linked (always attempted)

The Stage 1 impression that started this IP's funnel. Resolved via 2-link model (see Part 3b).

For S1 VVs: s1 columns are populated directly — the VV's own impression IS the S1 impression. S3 and S2 columns are NULL.

For S2/S3 VVs: resolved via `imp_direct` (bid_ip → S1 vast_start_ip) or `imp_visit` (impression_ip → S1 vast_start_ip). S2: 99.95% resolved. S3: 96.85% resolved.

**`s1_ad_served_id`** — S1 impression UUID. Resolved via the 2-link model — no dependency on the system's write-time `cp_ft_ad_served_id`.
- *"Why is this better than `cp_ft_ad_served_id`?"* `cp_ft_ad_served_id` is NULL ~40% of the time and cannot be recovered. The 2-link model fills in those gaps.

**`s1_bid_ip`** — The original S1 bid IP — the IP we targeted at the start of the funnel. **End of the chain.**

**`s1_vast_start_ip`** / **`s1_vast_impression_ip`** — S1 impression VAST IPs.

**`s1_serve_ip`** / **`s1_win_ip`** — S1 impression serve request and win IPs.

**`s1_impression_time`** — When the S1 impression was served.

**`s1_guid`** — Impression-side guid for the S1 impression.

**`s1_resolution_method`** — Which link resolved S1. Values: `current_is_s1`, `imp_direct`, `imp_visit`, or NULL (unresolved — structural ceiling). See Part 3b for definitions.

**`cp_ft_ad_served_id`** — System-recorded S1 shortcut from `clickpass_log.first_touch_ad_served_id`. NULL ~40%. **Retained as comparison reference only** — use `s1_ad_served_id` for audit work.

---

### 6. Classification — from `clickpass_log` and `ui_visits`

**`clickpass_is_new`** — NTB flag from clickpass JavaScript pixel. Client-side determination.

**`visit_is_new`** — NTB flag from ui_visits JavaScript pixel. Independent from `clickpass_is_new`.

**Critical note:** Both are client-side JavaScript checks of browser local storage/cookies. They disagree 41-56% of the time — this is not a bug. Two independent pixels, different implementations. Neither is auditable via SQL.

**`is_cross_device`** — Ad served on one device, visit on another. Set by VVS when it detects device type mismatch.

---

### 7. Metadata

**`trace_date`** — Partition key. `DATE(vv_time)`. Query must include this for partition pruning.

**`trace_run_timestamp`** — When this row was written (`current_timestamp()`).

---

## Part 6: Known Limitations — What the Table Cannot Do

**1. `cp_ft_ad_served_id` is NULL ~40% of the time — but `s1_ad_served_id` covers ~97%+**

`cp_ft_ad_served_id` (the system-written first-touch field) is NULL in ~40% of VVs overall, and higher (~60-74%) for S3 VVs specifically. It was not written at VV time and cannot be backfilled. This is a system limitation.

The `s1_ad_served_id` column was built to work around this: it uses the 2-link model (`imp_direct` + `imp_visit`) to resolve S1 for S2: 99.95%, S3: 96.85% of rows. `s1_ad_served_id` is NULL only when no IP path exists (structural ceiling — identity graph entry, CRM/cross-device cases with no IP breadcrumbs).

**`cp_ft_ad_served_id` is retained as a comparison reference only.** Users can validate by checking `s1_ad_served_id = cp_ft_ad_served_id` where both are non-NULL.

**2. Cross-stage matching uses merged VAST pool (vast_start preferred) + redirect_ip fallback**

Primary match: `pv_pool_vast.match_ip = bid_ip` (merged pool: vast_start_ip priority 1, vast_impression_ip priority 2, dedup'd by `(match_ip, stage)`). Fallback: `pv_pool_redir.redirect_ip = redirect_ip` (household identity — covers cross-device cases). Dedup prefers VAST matches over redirect, then last touch (most recent). Advertiser_id constraint prevents CGNAT false positives.

**3. `s2_bid_ip` is ~99%+ in production, lower in ad-hoc testing**

In production with a 90-day impression_pool window, almost all prior VV impressions are in range. In short-window ad-hoc queries, some prior VV impressions fall outside the window. When NULL, `s2_redirect_ip` is the reliable fallback (~94% equivalent).

**4. vast_start_ip vs vast_impression_ip as cross-stage key**

vast_start_ip is marginally better for cross-stage matching (99.937% vs 99.884%, 257 extra matches per 487K pairs). The merged VAST pool uses vast_start as priority 1, vast_impression as priority 2.

**5. NTB is not auditable via SQL**

`clickpass_is_new` and `visit_is_new` are JavaScript-determined and cannot be independently verified through log analysis. The 41-56% disagreement rate is expected and real.

**6. Display IPs vs CTV IPs**

CTV impressions: sourced from `event_log` (VAST events). Display impressions: sourced from `cost_impression_log` (CIL). CIL.ip = bid_ip (100% validated). All 5 IPs = bid_ip for display. CIL has `advertiser_id` for massive scan reduction. Render IP (impression_log.ip) is not available from CIL — differs from bid_ip only 6.2%, always internal 10.x.x.x NAT. The `impression_pool` CTE unions CTV + display into one shape.

---

## Part 7: The Join Architecture (v12)

**6 CTEs (was 9 in v10):**

```
1. el             — event_log (VAST events, pivoted to 1 row per ad_served_id)
2. cil            — cost_impression_log (display impressions)
3. impression_pool — UNION ALL of el + cil, dedup'd by ad_served_id
4. s1_by_vast_start — S1 impressions dedup'd by vast_start_ip (earliest per IP)
5. v_dedup        — ui_visits (visit_ip, impression_ip)
6. cp_dedup       — anchor VVs in target date range
```

**2 cross-stage LEFT JOINs (was 10 in v10/v11):**

```
cp_dedup (anchor)
  ├── impression_pool    (this VV's impression — deterministic ad_served_id link)
  ├── v_dedup            (visit IPs — ad_served_id link)
  ├── pv_pool_vast       (prior VV — merged VAST pool, match_ip = bid_ip)
  ├── pv_pool_redir      (prior VV — redirect_ip fallback for cross-device)
  ├── pv impression_pool (prior VV's impression — ad_served_id link)
  ├── s1_by_vast_start   (S1 via imp_direct — vast_start_ip = bid_ip)
  └── s1_by_vast_start   (S1 via imp_visit — vast_start_ip = impression_ip)
```

**impression_pool pattern:** CTV (event_log) and display (CIL) are unioned into `impression_pool` — one shape, one schema. CTV has distinct `vast_start_ip` and `vast_impression_ip`. Display has all IPs = CIL.ip (= bid_ip). The query never needs to branch on CTV vs display after this point.

**Important caveat:** BQ does NOT materialize CTEs — each reference re-scans the underlying table.

**Lookback:** 90 days. Ray's TTL context: the longest real-world chain (S1 impression → S3 VV) spans ~83 days. S2 and S3 audience TTLs are 30 days each (14+30+14+30 = 88 max). The 90-day window covers virtually all chains.

---

## Part 8: Does It Answer Zach's Question?

Zach's request: *"exact audit trail for a vv — every impression id/ip for each stage that led to that vv."*

**What we have:**

For every VV row:
- S(N) impression ID: `ad_served_id` — always populated
- S(N) impression IPs: `s3_bid_ip`, `s3_vast_start_ip`, `s3_vast_impression_ip`, `redirect_ip`, `visit_ip` — ~100% in production
- S2 prior VV impression ID + IPs: `s2_ad_served_id`, `s2_bid_ip`, `s2_vast_start_ip`, `s2_vast_impression_ip`, `s2_redirect_ip` — ~95%+ (90-day lookback)
- S1 impression ID (system-recorded): `cp_ft_ad_served_id` — ~60%, comparison reference only
- S1 impression ID (2-link resolution): `s1_ad_served_id`, `s1_resolution_method` — S2: 99.95%, S3: 96.85%
- S1 impression IPs: `s1_bid_ip`, `s1_vast_start_ip`, `s1_vast_impression_ip` — populated when resolved

**What is NOT achievable (honest answer):**

~3.15% of S3 VVs have NULL S1 columns. These are structural — the IP entered the targeting segment via the identity graph (CRM/LiveRamp), not via an IP path. No IP-based resolution can recover them. `cp_ft_ad_served_id` remains as an independent cross-check.

**Zach's specific clarification point:** *"its not super clear which ft values are coming from the table traversal vs what is coming from the clickpass logs attempt to look that data up."*

This is addressed by the naming convention:
- `cp_ft_ad_served_id` → the `cp_` prefix means this came directly from `clickpass_log.first_touch_ad_served_id` (what the system stored)
- `s1_ad_served_id`, `s1_bid_ip`, `s1_vast_start_ip` → the `s1_` prefix = our audit 2-link resolution via `s1_by_vast_start` pool

They are independent: `cp_ft_ad_served_id` is what the system wrote; `s1_ad_served_id` is what our resolution found. When both are non-NULL, they should agree. The `s1_` columns work when `cp_ft_ad_served_id` is NULL.

---

## Part 9: How to Use the Table — Common Questions, One-Line Answers

This section answers the most common questions using the table. Every answer is a single query against `audit.vv_ip_lineage`. No joins needed — everything is pre-computed.

### "What was the original bid IP for this verified visit?"

**Any stage, any VV, one column: `s1_bid_ip`.**

```sql
SELECT
  ad_served_id,
  vv_stage,
  redirect_ip,          -- IP at site visit (what you see today)
  s3_bid_ip,            -- IP we bid on for THIS impression
  s1_bid_ip,            -- IP we ORIGINALLY bid on at S1 (the start of the funnel)
  s1_ad_served_id,      -- the S1 impression ID
  s1_resolution_method  -- how S1 was resolved (imp_direct, imp_visit, etc.)
FROM audit.vv_ip_lineage
WHERE trace_date = '2026-02-04'
  AND advertiser_id = 37775
  AND ad_served_id = '77ddff0c-7f94-4d02-adfb-9c01b9598bf7'
```

Result: One row. `s1_bid_ip` = the IP we originally bid on at S1. `redirect_ip` = the IP at visit time. If they differ, mutation happened — but we targeted correctly at bid time.

**Works for every stage:**
- **S1 VV:** `s1_bid_ip` = `s3_bid_ip` (current impression IS the S1 impression)
- **S2 VV:** `s1_bid_ip` = the bid IP from the S1 impression that started this IP's funnel, resolved via `imp_direct` or `imp_visit`
- **S3 VV:** `s1_bid_ip` = same, resolved via the 2-link model through the S1 impression pool

### "Was this IP new-to-brand when we originally bid on it?"

**The NTB verification use case.** This is the core question: "if a VV's IP looks non-NTB today, was it NTB when we first bid on it?"

```sql
SELECT
  ad_served_id,
  vv_stage,
  vv_time,
  redirect_ip,              -- IP at visit time (might look "not new" now)
  s1_bid_ip,                -- IP we originally bid on at S1
  s1_ad_served_id,          -- the S1 impression that started this IP's funnel
  s1_resolution_method,     -- how S1 was resolved
  clickpass_is_new,         -- what the pixel said about this VV
  visit_is_new              -- what the other pixel said about this VV
FROM audit.vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-01' AND '2026-02-07'
  AND advertiser_id = 37775
  AND vv_stage = 3
  AND redirect_ip != s1_bid_ip   -- IP changed between S1 bid and S3 visit
LIMIT 100
```

**How to interpret:** If `redirect_ip != s1_bid_ip`, the IP changed. The visit IP may look "not new" because it's been seen before — but `s1_bid_ip` shows the IP we actually targeted. We bid on `s1_bid_ip` at S1 time. Any IP change after that is mutation (cross-device, VPN, CGNAT), not a targeting failure.

### "Show me all the IPs across the entire funnel for a single VV"

```sql
SELECT
  ad_served_id,
  vv_stage,
  -- S1 (origin of the funnel)
  s1_ad_served_id       AS s1_impression_id,
  s1_bid_ip             AS s1_original_bid_ip,
  s1_vast_start_ip      AS s1_playback_ip,
  s1_resolution_method,
  -- S2 (the VV that advanced this IP to current stage)
  s2_ad_served_id       AS s2_impression_id,
  s2_bid_ip             AS s2_bid_ip,
  s2_vast_start_ip      AS s2_playback_ip,
  s2_redirect_ip        AS s2_visit_ip,
  -- S3 / This VV (current stage)
  s3_bid_ip             AS current_bid_ip,
  s3_vast_start_ip      AS current_playback_ip,
  redirect_ip           AS current_visit_ip,
  visit_ip              AS current_page_view_ip,
  impression_ip         AS attributed_ip
FROM audit.vv_ip_lineage
WHERE trace_date = '2026-02-04'
  AND ad_served_id = '003a01cf-5e87-40f6-...'
```

**Reading the result:** Top-to-bottom = funnel journey. `s1_original_bid_ip` is where it started. `current_visit_ip` is where it ended. Every IP in between is visible. If they're all the same — no mutation. If they differ at the S2 or S3 stage — you can see exactly where and when the IP changed.

### "What % of S3 VVs have a different IP than their S1 origin?"

```sql
SELECT
  COUNT(*) AS total_s3_vvs,
  COUNTIF(redirect_ip != s1_bid_ip) AS ip_changed_from_s1,
  ROUND(COUNTIF(redirect_ip != s1_bid_ip) / COUNT(*) * 100, 2) AS mutation_pct,
  COUNTIF(s1_bid_ip IS NULL) AS s1_unresolved,
  ROUND(COUNTIF(s1_bid_ip IS NULL) / COUNT(*) * 100, 2) AS s1_unresolved_pct,
  COUNTIF(s1_resolution_method = 'imp_direct') AS via_imp_direct,
  COUNTIF(s1_resolution_method = 'imp_visit') AS via_imp_visit
FROM audit.vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-01' AND '2026-02-07'
  AND advertiser_id = 37775
  AND vv_stage = 3
```

### "For a specific IP, show me every VV and what stage it was at"

```sql
SELECT
  ad_served_id,
  vv_stage,
  vv_time,
  redirect_ip,
  s3_bid_ip,
  s2_ad_served_id,
  s1_bid_ip,
  s1_ad_served_id,
  s1_resolution_method
FROM audit.vv_ip_lineage
WHERE trace_date BETWEEN '2026-01-01' AND '2026-02-07'
  AND advertiser_id = 37775
  AND (redirect_ip = '100.34.227.166' OR s3_bid_ip = '100.34.227.166' OR s1_bid_ip = '100.34.227.166')
ORDER BY vv_time
```

**Reading the result:** Every VV involving IP `100.34.227.166` across all stages. You can see the full timeline: first S1 VV, then S2, then S3. Each row shows the chain back to S1. This is the IP's complete journey through the funnel.

---

## Part 10: Concrete Walkthrough — Cross-Device NTB Verification

**Real example from advertiser 37775, VV `77ddff0c` (2026-02-01):**

This S3 VV was served on a T-Mobile phone (bid IP `172.56.29.134`) but the site visit came from a home network (redirect IP `100.34.227.166`). Classic cross-device: CTV ad → phone → home WiFi website visit.

```
VV 77ddff0c (S3, 2026-02-01 22:35:17)
├── s3_bid_ip     = 172.56.29.134  (T-Mobile — phone that saw the ad)
├── redirect_ip   = 100.34.227.166 (home WiFi — where the site visit happened)
├── S2 prior VV   = 2c6a511d (2026-02-01 22:11:28)
│   ├── s2_bid_ip      = [S2 impression bid IP]
│   └── s2_redirect_ip = 100.34.227.166
├── S1 resolution: imp_direct (s3_bid_ip matched S1 vast_start_ip)
│   └── s1_ad_served_id = 305be134
│       ├── s1_bid_ip          = [original S1 bid IP]
│       └── s1_vast_start_ip   = [original S1 playback IP]
└── The IP we originally targeted (s1_bid_ip) was NTB at bid time.
    The visit IP (100.34.227.166) looks "not new" because the household
    had 17 prior VVs on the home network — but that's BECAUSE of successful
    targeting, not despite it.
```

**Why the cross-device fix matters here:** Without the redirect_ip fallback, `s2_ad_served_id` would be NULL for this VV. The bid IP (`172.56.29.134`, T-Mobile) has 5 prior VVs — but all are S3/S2, none are S1. The home IP (`100.34.227.166`) has 17 prior VVs including 4 S1 VVs. The redirect_ip fallback finds the household's VV history and resolves the chain all the way to S1.

**The NTB answer:** `s1_bid_ip` shows the IP we originally bid on. That IP was NTB at the time we bid. The fact that `redirect_ip` differs is cross-device mutation — the user watched the ad on their phone and visited on their laptop via home WiFi. Not a targeting failure. The table proves it.

---

## Part 11: Coverage Summary

**Validated coverage (20 advertisers, v3 bid_ip-only audit, 2026-03-10 to 2026-03-16):**

| Stage | VVs | Resolution | Method |
|-------|-----|------------|--------|
| **S1** | 93,274 | 100% | `ad_served_id` — deterministic, no IP matching needed |
| **S2** | 68,498 | 99.95% | `s2_bid_ip` → S1 impression pool, same campaign_group_id |
| **S3** | 36,388 | 96.85% | bid_ip cross-stage trace (T1: S3→S2, T2: S3→S1) |

**Coverage by column:**

| Column | S1 VVs | S2 VVs | S3 VVs | How |
|--------|--------|--------|--------|-----|
| `s3_bid_ip` / `s2_bid_ip` | ~100% | ~100% | ~100% | impression_pool — within 90-day window |
| `redirect_ip` | 100% | 100% | 100% | Directly from clickpass_log (anchor) |
| `s1_ad_served_id` | 100% | 99.95% | 96.85% | 2-link model (imp_direct + imp_visit) |
| `s1_bid_ip` | 100% | 99.95% | 96.85% | Same — populated when S1 resolves |
| `s2_ad_served_id` | NULL | = self | ~95%+ | 90-day merged VAST pool + redirect_ip fallback |

**NULL `s1_bid_ip` means:** No IP path exists to S1 — the IP entered the segment via the identity graph (CRM/LiveRamp), not via an impression IP. This is a structural ceiling, not a data quality issue. The 2-link model resolves everything that CAN be resolved via IP.

**The bottom line:** For any VV at any stage, `s1_bid_ip` tells you the IP we originally bid on. If the visit IP differs, it's mutation. The table proves targeting correctness.

### Historical: v10/v11 Tier Analysis (condensed)

The v12 2-link model replaced a 10-tier CASE cascade after empirical validation showed only 2 tiers (`imp_direct` + `imp_visit`) contributed unique resolutions. Key findings that drove the simplification:

**Critical scoping correction:** Previous "~20% unresolved" included retargeting campaigns. Zach confirmed retargeting is NOT relevant. Retargeting campaigns (objective_id=4) enter segments via LiveRamp/audience data, not S1 impressions.

**v11 10-tier cascade results (adv 37775, Feb 4-11, prospecting only):**

| Stage | Total VVs | Resolved | Resolution % |
|-------|-----------|----------|-------------|
| S1 | 93,274 | 93,274 | 100% |
| S2 | 16,753 | 16,751 | 99.99% |
| S3 | 23,844 | 23,708 | 99.43% |

Of the 10 tiers, `vv_chain_direct` (56.7%) and `imp_direct` (24.3%) dominated. All other tiers combined contributed <20%, and most were redundant once VAST IPs were added to the S1 pool. Adding S1 VAST IPs (vast_start/vast_impression from event_log) resolved 100% of S2 VVs — 747 VVs were recovered by VAST IPs alone.

**Root cause of truly unresolved VVs:** LiveRamp identity graph entry (IP_A → IP_B bridging via CRM), CGNAT IP rotation making IP_A no longer discoverable. A time-series IP→household mapping would close the gap.

Full v10/v11 tier analysis data is archived in `outputs/_archive/`.

---

## Part 12: VVS Determination Logic (How a Visit Becomes a Verified Visit)

Source: Nimeshi Fernando, "Verified Visit Service (VVS) Business Logic" (Confluence). This is the internal service that decides whether a page view on an advertiser's site gets attributed to an MNTN ad.

### The Flow: Ad Serve → Page View → VVS Decision

**Part 1 — Impression ingestion:**
1. Client calls ad service with a serve request
2. Ad service posts impression to Kafka (impression topic)
3. Ad service also sends VVS the impression → logged in `users.click_pass` (Scylla) where `viewable = false`
4. Ad service serves the ad to the client with an adcode
5. Client notifies event service that it received a viewable ad
6. Event service calls VVS → logs impression in `users.click_pass` where `viewable = true`
7. Event service also logs the impression to Kafka (impression topic)

**Part 2 — TRPX fires → VVS decides:**
1. User sees ad on TV, opens browser on a device, navigates to advertiser's site (which has TRPX tracking pixel installed)
2. TRPX fires on the page → record logged in `guidv2`
3. TRPX sends an HTTP POST request to VVS every time it fires (includes `ip`, `guid`, `gaid` (GA Client ID), `advertiserId`, UTM params, referrer, etc.)
4. VVS responds `true` or `false`:
   - `isSuccessful=true` + impression details = **Last Touch VV** (non-tamp, `attribution_model_id` 1-3)
   - `isSuccessful=false` = **Competing/First Touch VV** (tamp, `attribution_model_id` 9-11) or rejection
5. TRPX sends GA tracking data to attribution-consumer → attribution-consumer fires Measurement Protocol to advertiser's GA property → logged in `analytics_request_log`

### VVS Determination Logic (step-by-step)

This is the exact decision tree VVS runs on every TRPX POST request:

1. **Advertiser validation:** Is `advertiser_id` valid? If not → VV false.
2. **IP blocklist check** (`segmentation.ip_blocklist`): Is the page view IP on the blocklist?
   - Not on blocklist → proceed to cross-device check
   - On blocklist → proceed to guid whitelisting (stage 1)
   - *Blocklist populated from Oracle-Audience-Service, stored in Scylla*
3. **GUID blocklist check** (`segmentation.guid_blocklist`): Is the page view GUID on the blocklist?
   - On blocklist → VV false
   - Not on blocklist → proceed to cross-device check
4. **Cross-device config check:** Does advertiser have `crossdevice.config` and `advertisers.clickpass_enabled`?
   - `clickpass_enabled = false` → VV false
   - No cross-device config → same-device attribution only
   - Cross-device config enabled → same-device AND cross-device attribution
   - *Lookup table: `vvs.cross_device_config` in Aurora DB*
5. **GUID match (attribution_model_id = 1):** Does the page view GUID match a GUID that was served an impression within the VV window (14-45 days)?
   - Match found → GUID blocklist check (2nd stage) → eligibility check → **attribution_model_id = 1** (Last Touch - guid)
   - No match → proceed to IP match
6. **IP match (attribution_model_id = 2):** Does the page view IP match an IP served an impression?
   - Match found → was the IP served a CTV impression? (check `household_whitelist` in Scylla)
     - CTV impression → eligibility check → **attribution_model_id = 2** (Last Touch - ip)
     - No CTV impression → 2nd stage IP whitelisting (filter against `icloud_ipv4`)
       - Whitelisted → eligibility check
       - Not whitelisted → check GUID-to-IP count (if count > max → VV false; otherwise → eligibility check)
   - No match → proceed to GA Client ID match
7. **Viewable = false repeat:** Both GUID and IP matching are repeated with impressions where `viewable = false`
8. **GA Client ID match (attribution_model_id = 3):** Do any IPs associated with the GA Client ID (`cookie.gaid_ip_mapping`) match an impression IP?
   - Match found → eligibility check → **attribution_model_id = 3** (Last Touch - ga_client_id)
   - No match → **VV false** (final rejection)

### Eligibility Checks (run after a match is found)

9. **Duplicate check:** Was a VV already logged for the same user in this session? (TRPX fires on every page view, but only the first is eligible)
   - Already logged → VV false
10. **TTL / VV window check:** Is the user eligible for the next VV based on the impression's `clickpass_acquisition_ttl`? (Has this user/ip/guid/ga_client_id already been attributed in a prior visit — even a tampered one?)
    - Already attributed → VV false
11. **Advertiser TTL check:** Is the request within the advertiser's TTL window (45-day max, from `core-cache-service`)?
    - Within window → proceed to referral blocking
    - Outside window → VV false
12. **Referral blocking / tamp detection:** Does the referrer contain any substrings from the general tamp values list (`utm_source`, `utm_medium`, `utm_campaign`, `utm_content`, `gclid`, `cid`, `cmmmc`)?
    - Tamp substrings found → **VV false** (this is a paid search / competing channel visit)
    - No tamp substrings → **VV true** (`isSuccessful=true`)

### Attribution Model IDs (clickpass_log.attribution_model_id)

| ID | Model | Type |
|----|-------|------|
| 1 | Last Touch - guid | Non-competing |
| 2 | Last Touch - ip | Non-competing |
| 3 | Last Touch - ga_client_id | Non-competing |
| 4 | Last TV Touch - guid | Non-competing |
| 5 | Last TV Touch - ip | Non-competing |
| 6 | Last TV Touch - ga_client_id | Non-competing |
| 7 | Last TV Touch - Offline Attribution | Non-competing |
| 8 | Last Touch - Offline Attribution | Non-competing |
| 9 | Last Touch Competing - guid | Competing |
| 10 | Last Touch Competing - ip | Competing |
| 11 | Last Touch Competing - ga_client_id | Competing |
| 12 | Last TV Touch Competing - guid | Competing |
| 13 | Last TV Touch Competing - ip | Competing |
| 14 | Last TV Touch Competing - ga_client_id | Competing |
| 15 | Last Touch Impression - ip | Impression-based |
| 16 | Last TV Touch Impression - ip | Impression-based |

- **Non-competing** (1-8): `clickpass_log` rows where the visit passed tamp detection — genuine MNTN-driven visits
- **Competing** (9-14): Visit detected as coming from a competing channel (paid search, etc.) but still logged for reporting. Stored in `competing_vv` Kafka topic. Industry Standard visits skip tamp detection (step 12) and go directly to competing_vv
- **Impression-based** (15-16): Attribution based on impression alone (no direct visit linkage)

### PV_GUID_LOCK (Misattribution Prevention)

Handles the case where a user's IP changes during a single site visit:
- VVS stores both impression GUID and page view (PV) GUID
- **Impression GUID TTL:** unchanged (standard TTL)
- **PV GUID TTL:** 30 minutes of **inactivity** (resets every time TRPX fires with same PV GUID)
- This ensures the PV GUID remains eligible for VV consideration even if the IP changes mid-session
- Accounts for site visits longer than 30 minutes (TTL resets on each page view)
- Advertisers with this logic have `pv_guid_lock = true` in `advertiser_configs`

### Custom Attribution Settings

Advertisers can customize their referral blocking blocklist values. Stored in `vvs.blacklist_query_params`:
- `active = true` → substring is on the blocklist (will trigger tamp rejection)
- `active = false` → substring is off the blocklist (will not trigger rejection)

### Why This Matters for the Audit Table

The VVS determination logic explains several things visible in `audit.vv_ip_lineage`:
- **`is_cross_device` flag:** Set when VVS detects the ad was served on a different device than the visit (step 4-6 in the flow above)
- **IP mutation at VAST→redirect boundary:** VVS matches visits to impressions via IP/GUID/GA Client ID. The IP at match time (step 6) may differ from the IP at bid time because the user is on a different network
- **`clickpass_is_new` / `visit_is_new` disagreement:** Two independent client-side pixels with different implementations. VVS doesn't determine NTB — the TRPX pixel does, separately from VVS's attribution logic
- **`attribution_model_id` priority:** GUID match (1) → IP match (2) → GA Client ID match (3). The table's chain traversal uses IP matching (like VVS model 2), which is the most common match type for CTV attribution
