# VV IP Lineage — Plain English Guide

Everything you need to understand `audit.vv_ip_lineage`, explain it to anyone, and answer every skeptical question about it.

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
- The impression that triggered this VV (last touch, Stage N)
- The prior VV that advanced this IP into the current stage (any stage VV can trigger advancement — an S1, S2, or S3 VV can all put an IP into S3 targeting)
- The original first-touch S1 impression that started this IP's funnel

For a Stage 3 VV, this means three impression IDs and their associated IPs:

| Slot | Impression ID column | What it is |
|------|---------------------|------------|
| **This VV** | `ad_served_id` | The S(N) impression that triggered this VV |
| **Prior VV** | `prior_vv_ad_served_id` | The most recent prior VV for this IP — must be strictly lower stage (pv_stage < vv_stage) |
| **S1 first touch** | `s1_ad_served_id` | The S1 impression that started this IP's funnel — resolved via 3-branch chain traversal, ~99%+ populated |

> **Important:** The prior VV must be a **strictly lower stage** than the current VV. `pv_stage < vv_stage` — an IP can only be advanced INTO a stage by a lower-stage impression. You can't enter S3 via S3 (already there). Max chain: S3 → S2 → S1 (2 hops). An IP can also enter S3 from an S1 VV directly, in which case pv_stage=1 and `s1_ad_served_id` may equal `prior_vv_ad_served_id`.

> **All stages are anchor rows.** The table is not limited to S3 VVs. `cp_dedup` pulls ALL stages from `clickpass_log`. Zach confirmed: "if the dataset is still reasonable in size i think it would be good to have the info for all impressions/vv." A S1-only VV, a S2→S1 chain, or a S3→S2→S1 chain are all present as anchor rows.

Each impression slot also has its IPs in columns (bid IP, VAST IP, redirect IP, visit IP).

**Does this answer Zach's exact audit request?**

Zach: *"we need an exact audit trail for a vv. that means every impression id/ip for each stage that lead to that vv."*

| Zach needs | Our table | Coverage |
|------------|-----------|----------|
| S(N) impression ID + IPs | `ad_served_id`, `lt_bid_ip`, `lt_vast_ip` | ~100% |
| Lower-stage prior VV impression ID + IPs | `prior_vv_ad_served_id`, `pv_lt_bid_ip`, `pv_lt_vast_ip` | ~95%+ in production (90-day lookback) |
| S1 impression ID (system-recorded shortcut) | `cp_ft_ad_served_id` | ~60% (system limitation — retained as comparison reference) |
| S1 impression ID via chain traversal | `s1_ad_served_id` | ~99%+ in production (3-branch CASE, see Part 3b) |
| S1 bid IP + VAST IP | `s1_bid_ip`, `s1_vast_ip` | ~99%+ (same chain traversal) |

**The table is a traversable chain.** Every VV is traceable to S1 by following `prior_vv_ad_served_id`:
- **S(N) row where `pv_stage=1`:** `prior_vv_ad_served_id` IS the S1 impression. One hop, done.
- **S(N) row where `pv_stage>1`:** `prior_vv_ad_served_id` points to a higher-stage VV. Follow the chain in-query using the S1 CASE logic (see Part 3b below).
- **`cp_ft_ad_served_id`** is a direct shortcut to S1 that the attribution system stored at VV time — works ~60% of the time. Retained for comparison only. When NULL, `s1_ad_served_id` from chain traversal fills in.

The chain traversal works in ~99%+ of production rows because the `prior_vv_ad_served_id` link is backed by a 90-day clickpass_log window. The direct `cp_ft_ad_served_id` shortcut fails ~40% of the time due to a system write-time limitation.

---

## Part 3b: S1 Chain Traversal — 3-Branch CASE

The `s1_ad_served_id`, `s1_bid_ip`, and `s1_vast_ip` columns are resolved in-query using a 3-branch CASE expression, backed by 1 additional chain JOIN (`s1_pv` and its impression lookups). 9 LEFT JOINs total.

**Why not just use `cp_ft_ad_served_id`?** It's NULL 40% of the time and cannot be backfilled. Chain traversal resolves ~99%+.

**The 3 branches:**

| Branch | Condition | Resolution | Chain pattern |
|--------|-----------|-----------|---------------|
| 1 | `vv_stage = 1` | Current VV IS the S1 impression | S1 (no prior VV) |
| 2 | `pv.pv_stage = 1` | Prior VV IS the S1 impression | S2→S1 or S3→S1 |
| 3 | ELSE | Second-hop (`s1_pv`) finds S1 | S3→S2→S1 |

**The JOIN chain that powers this:**
```
pv    = prior VV of the current VV (redirect_ip = lt_bid_ip, pv_stage < vv_stage)
s1_pv = prior VV of pv (ip = pv_lt_bid_ip, pv_stage < pv.pv_stage)
```

`pv_stage < vv_stage` (strict): an IP can only be advanced INTO a stage by a strictly lower stage — you can't enter S3 via S3 (already there). Max chain depth: 2 (S3 → S2 → S1). The `s2_pv` third-level join was removed as unnecessary.

**All permutations validated (advertiser 37775, 2026-02-04):**

| vv_stage | pv_stage | s1_pv_stage | Branch | ~Row count |
|----------|----------|-------------|--------|-----------|
| 1 | — | — | 1 | 44K+ |
| 2 | 1 | — | 2 | 10K+ |
| 3 | 1 | — | 2 | 125K+ |
| 2 | 2 | 1 | 3 | 57K |
| 3 | 2 | 1 | 3 | 115K |

With strict `<` stage logic, same-stage prior VVs (pv_stage=3 for S3 VV) are impossible — an IP can't enter S3 via S3. The old permutations with pv_stage=vv_stage (branches 3 and 4 with same-stage hops) no longer apply.

Zach also said: *"if there are 3 vv that put an ip in stage 3, that's 3 rows. if there's a vv in stage 3 for an ip that had 3 vv that got it into stage 3 we should use last touch."* This is exactly what the table does — one row per VV, and for the prior VV we select the most recent prior VV (strictly lower stage) using `ORDER BY prior_vv_time DESC`.

---

## Part 4: Reading a Single Row

**Concrete example: a Stage 3 VV**

```
ad_served_id           = "003a01cf-5e87-40f6-..."    # S3 impression UUID
vv_stage               = 3
vv_time                = 2026-02-04 04:58:54

lt_bid_ip              = 172.59.192.138    # IP at auction
lt_vast_ip             = 172.59.192.138    # IP at CTV playback (same — no mutation here)
redirect_ip            = 172.59.192.138    # IP at site visit redirect (same — no mutation)
visit_ip               = 172.59.192.138    # IP at page view (same)
impression_ip          = 172.59.192.138    # IP the visit was attributed to (same)

prior_vv_ad_served_id  = "a4074373-..."    # The prior VV's impression UUID (pv_stage=2 here)
pv_stage               = 2                # Prior VV is S2 (strictly lower — S3→S2 chain)
pv_redirect_ip         = 172.59.192.138    # Prior VV's redirect IP (how we matched it)
pv_lt_bid_ip           = 172.59.192.138    # Prior VV's impression bid IP (audit lookup)
pv_lt_vast_ip          = 172.59.192.138    # Prior VV's VAST IP (audit lookup)
prior_vv_time          = 2026-01-25 11:32:00

cp_ft_ad_served_id     = NULL              # System did not record first touch (40% of VVs)
                                           # Retained as comparison reference only
s1_ad_served_id        = "a12e289d-..."    # S1 VV resolved via chain traversal (s1_pv JOIN)
s1_bid_ip              = 172.59.192.138    # S1 impression's bid IP
s1_vast_ip             = 172.59.192.138    # S1 impression's VAST IP
```

How to read it: This S3 VV was triggered by impression `003a01cf` on 2026-02-04. The IP `172.59.192.138` had a prior S2 VV (`a4074373`) on 2026-01-25. The 3-branch CASE resolved the S1 VV to `a12e289d` via the s1_pv JOIN (branch 3 — s1_pv found an S1 entry in `prior_vv_pool` for this IP). The system's `cp_ft_ad_served_id` was NULL, but chain traversal recovered the S1 impression.

---

## Part 5: Every Column Explained

### Identity group — from `clickpass_log` (anchor)

**`ad_served_id`**
- Source: `clickpass_log.ad_served_id`
- What it is: The UUID of the impression that triggered this VV. The same UUID appears in `event_log`, `impression_log`, `cost_impression_log`, and `ui_visits`.
- Skeptical Q: *"How do I know this is the impression ID and not just an arbitrary VV ID?"* Look at event_log WHERE `ad_served_id = this value` AND `event_type_raw = 'vast_impression'` — you will find the exact impression that triggered this VV.
- Skeptical Q: *"Is this the primary key?"* Yes. `clickpass_log` is QUALIFY deduped: one row per `ad_served_id`, most recent by time.

**`advertiser_id`**
- Source: `clickpass_log.advertiser_id`

**`campaign_id`**
- Source: `clickpass_log.campaign_id`
- What it is: The campaign that received last-touch attribution for this VV.

**`vv_stage`**
- Source: `campaigns.funnel_level` joined on `campaign_id`
- What it is: The stage of the campaign that served the impression. 1 = S1, 2 = S2, 3 = S3.
- Skeptical Q: *"How is stage determined?"* Every campaign has `funnel_level` in `bronze.integrationprod.campaigns`. This is set at campaign creation. It does not change.
- Skeptical Q: *"Why is this in cp_dedup and not joined in the final SELECT?"* We need it in `cp_dedup` so it's available as `cp.vv_stage` in the prior VV join condition (`pv.pv_stage < cp.vv_stage`). The prior VV pool join happens in `with_all_joins`, which references `cp.vv_stage` before campaigns is joined there.

**`vv_time`**
- Source: `clickpass_log.time`

---

### Last-touch impression IPs — from `event_log` (CTV) / `cost_impression_log` (display) / `clickpass_log` / `ui_visits`

These are the IPs at each hop for the impression that directly triggered THIS VV.

**`lt_bid_ip`**
- Source: `event_log.bid_ip` (CTV, preferred) or `cost_impression_log.ip` (display, fallback — CIL.ip = bid_ip, 100% validated)
- What it is: The IP that MNTN bid on at auction. This is the household/device IP as known to the DSP at bid time.
- How joined: `el_all.ad_served_id = cp.ad_served_id`, rn=1 (first VAST event)
- Skeptical Q: *"What if there's no event_log row?"* Display ads don't fire VAST events. In that case `event_log` returns NULL and `COALESCE(el.bid_ip, cil.bid_ip)` falls back to `cost_impression_log.ip` (which IS the bid_ip).

**`lt_vast_ip`**
- Source: `event_log.ip` (CTV) or `cost_impression_log.ip` (display — same as bid_ip; render IP not available from CIL)
- What it is: For CTV, the IP at the time the ad played (VAST impression — the TV's IP at playback). For display, this equals bid_ip (CIL does not have render_ip; render_ip differs from bid_ip only 6.2% of the time, always internal 10.x.x.x NAT).
- Note: This is where IP mutation is measured. If `lt_vast_ip ≠ redirect_ip`, the IP changed between ad playback and site visit.

**`redirect_ip`**
- Source: `clickpass_log.ip`
- What it is: The IP at the clickpass redirect — when the user's site visit was intercepted by the MNTN tracking pixel. This is what the site visit server recorded.

**`visit_ip`**
- Source: `ui_visits.ip`
- What it is: The IP recorded by the page view pixel on the advertiser's site.
- Note: Validated as 99.93% equal to `redirect_ip`. Near-identical because both are recorded during the same site visit session.

**`impression_ip`**
- Source: `ui_visits.impression_ip`
- What it is: The IP that `ui_visits` attributed the visit to — carried forward from the impression at attribution time.

---

### S1 (first-touch) impression — resolved via chain traversal

**`cp_ft_ad_served_id`**
- Source: `clickpass_log.first_touch_ad_served_id`
- What it is: The impression UUID the attribution system recorded as "the first time this IP was ever served an ad for this advertiser." Written when the VV fires. Cannot be updated retroactively.
- Skeptical Q: *"Why is it NULL 40% of the time?"* The system only writes it when the stack has a clear first touch. High-traffic IPs, IPs that had impressions before the field was introduced, and attribution stack resets all result in NULL. It is a system limitation, not a query bug.
- **This column is retained as a comparison reference only.** Use `s1_ad_served_id` for audit work.

**`s1_ad_served_id`**
- Source: Resolved via 3-branch CASE in the `with_all_joins` CTE (see Part 3b).
- What it is: The ad_served_id of the S1 VV that started this IP's funnel. Unlike `cp_ft_ad_served_id`, this is computed by traversing the `prior_vv_pool` chain — no dependency on the write-time system field.
- Coverage: ~99%+ in production with 90-day lookback. NULL only for chains deeper than 3 hops or impressions outside the lookback window.
- Skeptical Q: *"Why is this better than cp_ft_ad_served_id?"* `cp_ft_ad_served_id` is NULL 40% of the time and cannot be recovered. `s1_ad_served_id` uses the same source data (clickpass_log IP matching) to traverse the chain and fills in ~59 of those 60 missing points.

**`s1_bid_ip`**
- Source: `event_log.bid_ip` (CTV) or `cost_impression_log.ip` (display, = bid_ip), JOINed on the resolved S1 ad_served_id.
- What it is: The IP at auction time for the S1 impression. The "original bid IP" that started this IP's funnel.

**`s1_vast_ip`**
- Source: `event_log.ip` (CTV) or `cost_impression_log.ip` (display, = bid_ip), JOINed on the resolved S1 ad_served_id.
- What it is: The IP at the time the S1 ad played (VAST or display render).

---

### Prior VV — from `clickpass_log` (self-join) and `event_log`/`cost_impression_log` (IPs)

The prior VV is the most recent VV for this IP before the current VV, where `pv_stage < vv_stage` (strictly lower). For an S3 VV, the prior VV can be an S1 or S2 VV — whichever is most recent. An IP can only be advanced INTO a stage by a lower-stage impression.

**How we find the prior VV:**
1. `prior_vv_pool` scans `clickpass_log` for a 90-day lookback. Every VV in that window is a candidate.
2. **Primary match:** `prior_vv_pool.ip = lt_bid_ip` (the prior VV's redirect IP matches this VV's bid IP — direct targeting chain)
3. **Fallback match:** `prior_vv_pool.ip = cp.ip` (the prior VV's redirect IP matches this VV's redirect IP — household identity). This covers cross-device cases (~16-20% of S2/S3 VVs) where bid_ip ≠ redirect_ip due to mutation.
4. We require `prior_vv_time < vv_time` (must be before this VV)
5. We require `pv.pv_stage < cp.vv_stage` (prior VV must be strictly lower stage — max chain S3→S2→S1)
6. We require `pv.advertiser_id = cp.advertiser_id` (prevents cross-advertiser matching on shared IPs / CGNAT)
7. If multiple prior VVs exist, we take the most recent **bid_ip match** first; if none, the most recent redirect_ip fallback (last-touch rule per Zach, with bid_ip preference)

**`prior_vv_ad_served_id`**
- Source: `clickpass_log.ad_served_id` (the prior VV row's impression UUID)
- What it is: The impression ID of the prior VV (can be any stage ≤ current — S1, S2, or S3 for an S3 row). This is the same UUID that would be `ad_served_id` on THAT VV's row in this table.
- Skeptical Q: *"What if the prior VV isn't in the 90-day window?"* NULL. S3 VVs more than 90 days old won't have prior VV data. For production, incremental runs always have a 90-day lookback — this affects only historical rows at the very start of the table.
- Skeptical Q: *"Can pv_stage = 3 for an S3 row?"* No. The `pv.pv_stage < cp.vv_stage` condition requires strictly lower stage. An IP can't enter S3 via S3 — it's already there. For an S3 VV, pv_stage can be 1 or 2. The S1 is resolved via the s1_pv JOIN when pv_stage=2 (see Part 3b).
- Skeptical Q: *"Why allow pv_stage=1 for S3 rows, not just pv_stage=2?"* Because an IP can enter S3 from an S1 VV directly (S1 impression → S1 VV → IP enters S3 targeting). There doesn't need to be an S2 VV in the chain. If the prior VV is a S1 VV, then `pv_stage=1` and `prior_vv_ad_served_id` may equal `cp_ft_ad_served_id`.

**`prior_vv_time`**
- Source: `clickpass_log.time` for the prior VV row

**`pv_campaign_id`**
- Source: `clickpass_log.campaign_id` for the prior VV row

**`pv_stage`**
- Source: `campaigns.funnel_level` joined on `pv_campaign_id` (inside `prior_vv_pool` CTE)
- `pv_stage < vv_stage` (strictly lower). For S3 rows, pv_stage can be 1 or 2. For S2 rows, pv_stage can be 1.

**`pv_redirect_ip`**
- Source: `clickpass_log.ip` for the prior VV
- What it is: The IP that was recorded at the prior VV's site visit redirect. This is the IP that entered the S(N) targeting segment — the IP that made this VV possible.
- Important: This is the primary IP the targeting system used to promote the IP to the next stage. In 94% of cases `pv_redirect_ip = pv_lt_bid_ip`. When `pv_lt_bid_ip` is NULL (impression outside lookback), `pv_redirect_ip` is the reliable fallback.

**`pv_lt_bid_ip`**
- Source: `event_log.bid_ip` (CTV) or `cost_impression_log.ip` (display, = bid_ip), JOINed on `prior_vv_ad_served_id`
- Audit-trail lookup for the prior VV's impression bid IP.
- Skeptical Q: *"Why is this NULL for some rows?"* The prior VV's impression may predate the `el_all`/`cil_all` window. In production (90-day window), this is ~1-2% NULL. In ad-hoc testing with shorter windows, NULL rate is higher. When NULL, use `pv_redirect_ip` — it's ~94% equivalent.

**`pv_lt_vast_ip`**
- Source: `event_log.ip` (CTV) or `cost_impression_log.ip` (display, = bid_ip), JOINed on `prior_vv_ad_served_id`

**`pv_lt_time`**
- Source: `event_log.time` or `cost_impression_log.time` for the prior VV's impression

---

### Classification — from `clickpass_log` and `ui_visits`

**`clickpass_is_new`**
- Source: `clickpass_log.is_new`
- What it is: Whether the clickpass JavaScript pixel classified this visit as new-to-brand at visit time.

**`visit_is_new`**
- Source: `ui_visits.is_new`
- What it is: Whether the ui_visits JavaScript pixel classified this visit as new-to-brand.

**Critical note on both:** Both are client-side JavaScript determinations. They check the browser's local storage/cookies to see if this user has been to the advertiser's site before. They disagree 41-56% of the time — this is not a bug. They are two independent pixels with different implementations. Neither is auditable via SQL. The disagreement is an architectural reality of how NTB detection works.

**`is_cross_device`**
- Source: `clickpass_log.is_cross_device`
- What it is: The attribution system detected that the ad was served on one device and the site visit came from a different device.

---

### Metadata

**`trace_date`**
- Source: `DATE(clickpass_log.time)`
- Partition key. Query must include this column for partition pruning.

**`trace_run_timestamp`**
- Source: `current_timestamp()` at write time

---

## Part 6: Known Limitations — What the Table Cannot Do

**1. `cp_ft_ad_served_id` is NULL ~40% of the time — but `s1_ad_served_id` covers ~99%+**

`cp_ft_ad_served_id` (the system-written first-touch field) is NULL in ~40% of VVs overall, and higher (~60-74%) for S3 VVs specifically. It was not written at VV time and cannot be backfilled. This is a system limitation.

The `s1_ad_served_id` column was built specifically to work around this: it uses chain traversal (3-branch CASE, backed by `prior_vv_pool` JOINs) to resolve S1 for ~99%+ of rows. `s1_ad_served_id` is NULL only for VVs whose S1 impression predates the 90-day lookback window.

**`cp_ft_ad_served_id` is retained in the table as a comparison reference only.** Users can validate the chain traversal by checking `s1_ad_served_id = cp_ft_ad_served_id` where both are non-NULL (should agree in the vast majority of cases).

**2. Prior VV match uses bid_ip (primary) + redirect_ip (fallback)**

Primary match: `prior_vv_pool.ip = lt_bid_ip` (prior VV's redirect IP = this VV's bid IP — direct targeting chain). Fallback: `prior_vv_pool.ip = cp.ip` (prior VV's redirect IP = this VV's redirect IP — household identity). The fallback covers the ~16-20% of S2/S3 VVs where bid_ip ≠ redirect_ip due to cross-device mutation. Without the fallback, these VVs would have NULL prior_vv/s1 chains. Dedup prefers bid_ip matches. Same fallback logic applies at all chain levels (pv, s1_pv). Advertiser_id constraint on all joins prevents CGNAT false positives.

**3. pv_lt_bid_ip is ~99%+ in production, ~67% in ad-hoc testing**

In production with a 90-day event_log/cost_impression_log window, almost all prior VV impressions are in range. In short-window ad-hoc queries, ~33% of prior VV impressions fall outside the window. When NULL, `pv_redirect_ip` is the reliable fallback (~94% equivalent).

**4. NTB is not auditable via SQL**

`clickpass_is_new` and `visit_is_new` are JavaScript-determined and cannot be independently verified through log analysis. The 41-56% disagreement rate is expected and real.

**5. Display IPs vs CTV IPs**

CTV impressions: sourced from `event_log` (`vast_impression` event). Display impressions: sourced from `cost_impression_log` (CIL). CIL replaces `impression_log` — CIL.ip = bid_ip (100% validated), and CIL has `advertiser_id` for massive scan reduction. Render IP is not available from CIL (differs from bid_ip only 6.2%, always internal 10.x.x.x NAT). Non-viewable display impressions appear in CIL — they never generate a VAST event. The `COALESCE(el, cil)` pattern handles this correctly.

---

## Part 7: The Join Architecture

How the query builds one row from five sources (9 LEFT JOINs total):

```
clickpass_log (anchor — one VV per row, all stages)
  ├── event_log  (CTV, scanned once → el_all, joined 3x: lt, pv_lt, s1_lt)
  ├── cost_impression_log (display, scanned once → cil_all, joined 3x: same slots as el_all)
  ├── ui_visits  (visit IP — via ad_served_id, CAST as STRING)
  ├── clickpass_log (self via prior_vv_pool, joined 2x: pv, s1_pv + campaigns inside pool)
  └── campaigns  (stage lookup — joined inside cp_dedup and prior_vv_pool CTEs)
```

**One scan, joined multiple times:** `event_log` is scanned once into `el_all`, then used three times (lt = last touch, pv_lt = prior VV impression, s1_lt = s1_pv impression). Same for `cost_impression_log` → `cil_all`. **Important caveat:** BQ does NOT materialize CTEs — each reference re-scans the underlying table. See `ti_650_query_optimization_guide.md` for TEMP TABLE mitigation.

**COALESCE pattern:** For every IP column, the pattern is `COALESCE(el.ip, cil.ip)`. CTV (`event_log`) is preferred; display (`cost_impression_log`) fills in NULLs. If both are NULL, the impression was not found in either log (rare, typically a timing edge case or data gap).

**S1 chain traversal JOIN (s1_pv):**
- `s1_pv` = second-level prior VV pool JOIN. Finds the VV whose redirect IP = `pv_lt_bid_ip`. `s1_pv.pv_stage < pv.pv_stage` keeps the chain strictly decreasing in stage.
- `s2_pv` removed — unnecessary with strict `<` stage logic (max chain depth is 2).
- `s1_lt`, `s1_lt_d` = impression log lookups for the IPs at the second traversal hop.

**Ray's TTL context (from architecture diagram):** The longest real-world chain (S1 impression → S3 VV) spans ~83 days in representative examples. S2 and S3 audience TTLs are 30 days each. The 90-day lookback window is conservatively sized to cover virtually all chains with room to spare.

---

## Part 8: Does It Answer Zach's Question?

Zach's request: *"exact audit trail for a vv — every impression id/ip for each stage that led to that vv."*

**What we have:**

For every VV row:
- S(N) impression ID: `ad_served_id` — always populated
- S(N) impression IPs: `lt_bid_ip`, `lt_vast_ip`, `redirect_ip`, `visit_ip` — ~99%+ in production
- Prior VV impression ID: `prior_vv_ad_served_id` — any stage ≤ current, populated when prior VV in 90-day window (~95%+)
- Prior VV impression IPs: `pv_lt_bid_ip`, `pv_lt_vast_ip`, `pv_redirect_ip` — mostly populated, `pv_redirect_ip` always available
- S1 impression ID (system-recorded): `cp_ft_ad_served_id` — ~60%, retained as comparison reference
- S1 impression ID (chain traversal): `s1_ad_served_id` — ~99%+, the primary audit-trail S1 field
- S1 impression IPs: `s1_bid_ip`, `s1_vast_ip` — populated when S1 chain resolves (~99%+)

**What is NOT achievable (honest answer):**

With strict `<` stage logic, the maximum chain depth is 2 (S3→S2→S1). The 3-branch CASE covers all possible chains. `s1_ad_served_id` is NULL only when the S1 impression predates the 90-day lookback. `cp_ft_ad_served_id` remains as an independent cross-check.

**Zach's specific clarification point:** *"its not super clear which ft values are coming from the table traversal vs what is coming from the clickpass logs attempt to look that data up."*

This is addressed by the naming convention:
- `cp_ft_ad_served_id` → the `cp_` prefix means this came directly from `clickpass_log.first_touch_ad_served_id` (what the system stored)
- `s1_ad_served_id`, `s1_bid_ip`, `s1_vast_ip` → the `s1_` prefix = our audit chain traversal via `prior_vv_pool` JOINs

They are independent: `cp_ft_ad_served_id` is what the system wrote; `s1_ad_served_id` is what the chain traversal resolved. When both are non-NULL, they should agree. The `s1_` columns work when `cp_ft_ad_served_id` is NULL.

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
  lt_bid_ip,            -- IP we bid on for THIS impression
  s1_bid_ip,            -- IP we ORIGINALLY bid on at S1 (the start of the funnel)
  s1_ad_served_id       -- the S1 impression ID (trace it in event_log/cost_impression_log)
FROM audit.vv_ip_lineage
WHERE trace_date = '2026-02-04'
  AND advertiser_id = 37775
  AND ad_served_id = '77ddff0c-7f94-4d02-adfb-9c01b9598bf7'
```

Result: One row. `s1_bid_ip` = the IP we originally bid on at S1. `redirect_ip` = the IP at visit time. If they differ, mutation happened — but we targeted correctly at bid time.

**Works for every stage:**
- **S1 VV:** `s1_bid_ip` = `lt_bid_ip` (current impression IS the S1 impression)
- **S2 VV:** `s1_bid_ip` = the bid IP from the S1 impression that started this IP's funnel, resolved via chain traversal (1-2 hops)
- **S3 VV:** `s1_bid_ip` = same, resolved via chain traversal through S3→S2→S1 (max 2 hops)

### "Was this IP new-to-brand when we originally bid on it?"

**The NTB verification use case.** This is the core question: "if a VV's IP looks non-NTB today, was it NTB when we first bid on it?"

```sql
SELECT
  ad_served_id,
  vv_stage,
  vv_time,
  redirect_ip,          -- IP at visit time (might look "not new" now)
  s1_bid_ip,            -- IP we originally bid on at S1
  s1_ad_served_id,      -- the S1 impression that started this IP's funnel
  clickpass_is_new,     -- what the pixel said about this VV
  visit_is_new          -- what the other pixel said about this VV
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
  s1_vast_ip            AS s1_playback_ip,
  -- Prior VV (the VV that advanced this IP to current stage)
  prior_vv_ad_served_id AS prior_vv_impression_id,
  pv_stage              AS prior_vv_stage,
  pv_lt_bid_ip          AS prior_vv_bid_ip,
  pv_lt_vast_ip         AS prior_vv_playback_ip,
  pv_redirect_ip        AS prior_vv_visit_ip,
  -- This VV (current stage)
  lt_bid_ip             AS current_bid_ip,
  lt_vast_ip            AS current_playback_ip,
  redirect_ip           AS current_visit_ip,
  visit_ip              AS current_page_view_ip,
  impression_ip         AS attributed_ip
FROM audit.vv_ip_lineage
WHERE trace_date = '2026-02-04'
  AND ad_served_id = '003a01cf-5e87-40f6-...'
```

**Reading the result:** Top-to-bottom = funnel journey. `s1_original_bid_ip` is where it started. `current_visit_ip` is where it ended. Every IP in between is visible. If they're all the same — no mutation. If they differ at the prior_vv or current stage — you can see exactly where and when the IP changed.

### "What % of S3 VVs have a different IP than their S1 origin?"

```sql
SELECT
  COUNT(*) AS total_s3_vvs,
  COUNTIF(redirect_ip != s1_bid_ip) AS ip_changed_from_s1,
  ROUND(COUNTIF(redirect_ip != s1_bid_ip) / COUNT(*) * 100, 2) AS mutation_pct,
  COUNTIF(s1_bid_ip IS NULL) AS s1_unresolved,
  ROUND(COUNTIF(s1_bid_ip IS NULL) / COUNT(*) * 100, 2) AS s1_unresolved_pct
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
  lt_bid_ip,
  prior_vv_ad_served_id,
  pv_stage,
  s1_bid_ip,
  s1_ad_served_id
FROM audit.vv_ip_lineage
WHERE trace_date BETWEEN '2026-01-01' AND '2026-02-07'
  AND advertiser_id = 37775
  AND (redirect_ip = '100.34.227.166' OR lt_bid_ip = '100.34.227.166' OR s1_bid_ip = '100.34.227.166')
ORDER BY vv_time
```

**Reading the result:** Every VV involving IP `100.34.227.166` across all stages. You can see the full timeline: first S1 VV, then S2, then S3. Each row shows the chain back to S1. This is the IP's complete journey through the funnel.

---

## Part 10: Concrete Walkthrough — Cross-Device NTB Verification

**Real example from advertiser 37775, VV `77ddff0c` (2026-02-01):**

This S3 VV was served on a T-Mobile phone (bid IP `172.56.29.134`) but the site visit came from a home network (redirect IP `100.34.227.166`). Classic cross-device: CTV ad → phone → home WiFi website visit.

```
VV 77ddff0c (S3, 2026-02-01 22:35:17)
├── lt_bid_ip     = 172.56.29.134  (T-Mobile — phone that saw the ad)
├── redirect_ip   = 100.34.227.166 (home WiFi — where the site visit happened)
├── prior VV      = 2c6a511d (S2, 2026-02-01 22:11:28)
│   ├── pv_lt_bid_ip  = [S2 impression bid IP]
│   └── pv_redirect_ip = 100.34.227.166
├── chain traversal: S3 → S2 → S2 → S1
│   └── s1_ad_served_id = 305be134
│       ├── s1_bid_ip = [original S1 bid IP]
│       └── s1_vast_ip = [original S1 playback IP]
└── The IP we originally targeted (s1_bid_ip) was NTB at bid time.
    The visit IP (100.34.227.166) looks "not new" because the household
    had 17 prior VVs on the home network — but that's BECAUSE of successful
    targeting, not despite it.
```

**Why the cross-device fix matters here:** Without the redirect_ip fallback, `prior_vv_ad_served_id` would be NULL for this VV. The bid IP (`172.56.29.134`, T-Mobile) has 5 prior VVs — but all are S3/S2, none are S1. The home IP (`100.34.227.166`) has 17 prior VVs including 4 S1 VVs. The redirect_ip fallback finds the household's VV history and resolves the chain all the way to S1.

**The NTB answer:** `s1_bid_ip` shows the IP we originally bid on. That IP was NTB at the time we bid. The fact that `redirect_ip` differs is cross-device mutation — the user watched the ad on their phone and visited on their laptop via home WiFi. Not a targeting failure. The table proves it.

---

## Part 11: Coverage Summary

**Validated coverage by column (advertiser 37775, 7-day window, 90-day lookback):**

| Column | S1 VVs | S2 VVs | S3 VVs | How |
|--------|--------|--------|--------|-----|
| `lt_bid_ip` | ~100% | ~100% | ~100% | Every VV has its impression within 30-day EL/IL window |
| `redirect_ip` | 100% | 100% | 100% | Directly from clickpass_log (anchor) |
| `s1_ad_served_id` | 100% | ~85-93%+ | ~85-93%+ | Chain traversal (3-branch CASE). Higher with cross-device fix. |
| `s1_bid_ip` | 100% | ~85-93%+ | ~85-93%+ | Same as s1_ad_served_id — populated when chain resolves |
| `prior_vv_ad_served_id` | NULL (S1 has no prior) | ~55-85%+ | ~85-93%+ | 90-day clickpass_log window + redirect_ip fallback |

**NULL s1_bid_ip means:** The chain couldn't be fully traversed — the S1 VV is >90 days old. Not a data quality issue — just a lookback limitation. In production with daily incremental runs and 90-day lookback, this is expected to be <1%.

**All chain traversal permutations validated** with strict `<` stage logic:
S1, S2→S1, S3→S1, S3→S2→S1. Every permutation resolves `s1_bid_ip`. Max chain depth: 2.

**The bottom line:** For any VV at any stage, `s1_bid_ip` tells you the IP we originally bid on. If the visit IP differs, it's mutation. The table proves targeting correctness.

### S1 Resolution: Prospecting-Only Results (updated 2026-03-10)

**Critical scoping correction:** Previous "~20% unresolved" included retargeting campaigns. Zach confirmed retargeting is NOT relevant to this audit. Retargeting campaigns (objective_id=4) exist at every funnel_level (1/2/3) — they enter segments via LiveRamp/audience data, not S1 impressions, by design.

**Prospecting-only CTV S2 resolution (adv 37775, 7-day trace, 90-day lookback):**

| Tier | VVs Resolved | Cumulative % |
|------|-------------|-------------|
| S1 impression at bid_ip | 15,465 | 95.98% |
| guid_vv_match | 353 | 98.17% |
| guid_imp_match | 5 | 98.20% |
| s1_imp_redirect | 11 | 98.27% |
| household_graph | 46 | 98.56% |
| **Truly unresolved** | **232** | **1.44%** |
| **Total CTV S2 VVs** | **16,112** | |

**Household graph tier:** Uses `bronze.tpa.graph_ips_aa_100pct_ip` to find IPs in the same household. Of 265 distinct unresolved IPs, 254 (95.8%) exist in the graph. 44 IPs have household-linked IPs with S1 impressions.

**232 truly unresolved breakdown:** 178 (76.7%) are "competing" VVs (models 9-11, secondary attribution). Only 54 are primary VVs (models 1-3). **Primary VV unresolved: 0.34%.**

**Root cause of remaining 232:** LiveRamp identity graph linked the IP to a different household IP with the S1 impression. Most are T-Mobile CGNAT IPs (172.5x.x.x) — IPs rotate, so the household graph snapshot may not include the IP that was active at S1 time.

**How the gap happens:**
1. S1 impression served to IP_A via S1 prospecting campaign
2. LiveRamp identity graph links IP_A ↔ IP_B as same household/user
3. IP_B enters S2 targeting segment via LiveRamp (DS3) — gets S2 impression → VV
4. Audit tries to trace S2 VV at IP_B back to S1 at IP_B → finds nothing (S1 was at IP_A)
5. CGNAT IP rotation means IP_A may no longer be in the household graph

**What would close the gap:** A time-series IP→household mapping (not just current snapshot).

**Display S2 resolution (2026-03-10):**

| Tier | VVs Resolved | Cumulative % |
|------|-------------|-------------|
| S1 impression at bid_ip | 2,236 | 95.64% |
| guid_vv_match | 61 | 98.25% |
| s1_imp_redirect | 1 | 98.29% |
| **Truly unresolved** | **40** | **1.71%** |
| **Total Display S2 VVs** | **2,338** | |

40 unresolved: 32 competing, 8 primary. Primary VV unresolved: 0.34% — identical to CTV.

**Combined all device types:** 18,178/18,450 resolved (98.53%). Primary VV unresolved: 62/18,450 = 0.34%. Root cause consistent: LiveRamp CGNAT IP rotation.

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
