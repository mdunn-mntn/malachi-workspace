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

**Display vs CTV:** Display ads do not fire VAST events. For display, step 3 uses `impression_log` (not `event_log`). The `event_log` columns (`vast_impression`, `bid_ip`) are CTV-only. The query handles this with `COALESCE(event_log value, impression_log value)` — CTV preferred, display as fallback.

**Where IP mutation happens:** 100% of IP changes happen between step 3 (VAST) and step 4 (clickpass redirect). The bid IP = VAST bid_ip = impression_log IP in 100% of cases. If the IP changed, it changed at the VAST→redirect boundary. Cross-device and VPN switching cause this.

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
- IPs are NEVER removed from prior stages. An IP in S3 is also still in S1 and S2. All three campaigns can serve it; frequency capping prevents duplicate delivery.
- Each stage is a separate campaign with separate budget. S1 = ~75-80% of budget. The bidder has no concept of stages — it just sees three independent campaigns.
- VV attribution uses the last-touch stack model: *"We put impressions on the stack. When a page view comes in, we check the top of the stack."* The most recent impression gets credit.

**Attribution stage vs journey stage:** Because S1 has 75-80% of budget, a VV can be attributed to an S1 impression even if the IP has already reached S3. The `vv_stage` column records attribution stage, not the IP's deepest stage. 20% of S1-attributed VVs are on IPs that have already reached S3.

---

## Part 3: What the Audit Table Is

**One row per verified visit. All advertisers. All stages.**

The table provides the complete IP and impression ID trace for every VV:
- The impression that triggered this VV (last touch, Stage N)
- The prior VV that advanced this IP into the current stage (e.g., the S2 VV that put the IP into S3 targeting)
- The original first-touch S1 impression that started this IP's funnel

For a Stage 3 VV, this means three impression IDs and their associated IPs:

| Slot | Impression ID column | What it is |
|------|---------------------|------------|
| **This VV** | `ad_served_id` | The S3 impression that triggered this VV |
| **Prior VV** | `prior_vv_ad_served_id` | The S2 VV that advanced this IP into S3 targeting (last-touch rule) |
| **First touch** | `cp_ft_ad_served_id` | The S1 impression that started this IP's funnel |

Each impression slot also has its IPs in columns (bid IP, VAST IP, redirect IP, visit IP).

**Does this answer Zach's exact audit request?**

Zach: *"we need an exact audit trail for a vv. that means every impression id/ip for each stage that lead to that vv."*

| Zach needs | Our table | Coverage |
|------------|-----------|----------|
| S3 impression ID + IPs | `ad_served_id`, `lt_bid_ip`, `lt_vast_ip` | ~100% |
| S2 impression ID + IPs | `prior_vv_ad_served_id`, `pv_lt_bid_ip`, `pv_lt_vast_ip` | ~95%+ in production (90-day lookback) |
| S1 impression ID + IPs | `cp_ft_ad_served_id`, `ft_bid_ip`, `ft_vast_ip` | ~60% (system limitation — see Part 6) |

Zach also said: *"if there are 3 vv that put an ip in stage 3, that's 3 rows. if there's a vv in stage 3 for an ip that had 3 vv that got it into stage 3 we should use last touch."* This is exactly what the table does — one row per VV, and for the prior VV we select the most recent S(N-1) VV using `ORDER BY prior_vv_time DESC`.

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

prior_vv_ad_served_id  = "a4074373-..."    # The S2 VV's impression UUID
pv_stage               = 2                # Confirmed: this IS the S2 VV (vv_stage-1)
pv_redirect_ip         = 172.59.192.138    # Prior VV's redirect IP (how we matched it)
pv_lt_bid_ip           = 172.59.192.138    # Prior VV's impression bid IP (audit lookup)
pv_lt_vast_ip          = 172.59.192.138    # Prior VV's VAST IP (audit lookup)
prior_vv_time          = 2026-01-25 11:32:00

cp_ft_ad_served_id     = NULL              # System did not record first touch (40% of VVs)
ft_bid_ip              = NULL              # NULL because cp_ft_ad_served_id is NULL
ft_vast_ip             = NULL              # NULL because cp_ft_ad_served_id is NULL
```

How to read it: This S3 VV was triggered by impression `003a01cf` on 2026-02-04. The IP `172.59.192.138` was already in S3 targeting because it had the prior S2 VV (`a4074373`) on 2026-01-25. The original S1 first touch is unknown — system did not record it.

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
- Skeptical Q: *"Why is this in cp_dedup and not joined in the final SELECT?"* We moved it into `cp_dedup` so it's available for the prior VV join condition (`pv.pv_stage = cp.vv_stage - 1`), ensuring we always match the correct prior VV stage.

**`vv_time`**
- Source: `clickpass_log.time`

---

### Last-touch impression IPs — from `event_log` (CTV) / `impression_log` (display) / `clickpass_log` / `ui_visits`

These are the IPs at each hop for the impression that directly triggered THIS VV.

**`lt_bid_ip`**
- Source: `event_log.bid_ip` (CTV, preferred) or `impression_log.bid_ip` (display, fallback)
- What it is: The IP that MNTN bid on at auction. This is the household/device IP as known to the DSP at bid time.
- How joined: `el_all.ad_served_id = cp.ad_served_id`, rn=1 (first VAST event)
- Skeptical Q: *"What if there's no event_log row?"* Display ads don't fire VAST events. In that case `event_log` returns NULL and `COALESCE(el.bid_ip, il.bid_ip)` falls back to `impression_log.bid_ip`.

**`lt_vast_ip`**
- Source: `event_log.ip` (CTV) or `impression_log.ip` (display)
- What it is: The IP at the time the ad played (VAST impression). For CTV this is the TV's IP at playback. For display this is the device IP at impression render.
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
- What it is: The IP that `ui_visits` attributed the visit to — carried forward from `impression_log` at attribution time.

---

### First-touch impression — from `clickpass_log` (ID) and `event_log`/`impression_log` (IPs)

**Why there are TWO types of first-touch columns:**

The `cp_ft_` prefix means it comes directly from `clickpass_log.first_touch_ad_served_id` — the value the MNTN attribution SYSTEM stored when the VV was written. This is the system's recorded answer.

The `ft_` columns (without `cp_` prefix) are what we get by JOINing `event_log`/`impression_log` on that ID — our independent audit-trail lookup. They should agree; when `cp_ft_ad_served_id` is not NULL, the `ft_` columns look up the impression data FOR that ID.

These are not two competing answers to "what is the first touch" — they are the same impression viewed from two angles: the system's label vs. our raw log lookup.

**`cp_ft_ad_served_id`**
- Source: `clickpass_log.first_touch_ad_served_id`
- What it is: The impression UUID the attribution system recorded as "the first time this IP was ever served an ad for this advertiser." Written when the VV fires. Cannot be updated retroactively.
- Skeptical Q: *"How does the system know this?"* At VV time, the attribution stack contains all prior impressions for this IP. The system writes the bottom of the stack (first impression) into `first_touch_ad_served_id`.
- Skeptical Q: *"Why is it NULL 40% of the time?"* The system only writes it when the stack has a clear first touch. High-traffic IPs, IPs that had impressions before the field was introduced, and IPs with attribution stack resets all result in NULL. This is written at VV time and cannot be backfilled. It is a system limitation, not a query bug.

**`ft_campaign_id`**
- Source: `event_log.campaign_id` (CTV) or `impression_log.campaign_id` (display), retrieved by JOINing on `cp_ft_ad_served_id`

**`ft_stage`**
- Source: `campaigns.funnel_level`, joined on `ft_campaign_id`
- What it is: Should always be 1. The first-touch impression is by definition from a Stage 1 campaign (the IP had no prior impressions, so it could only have been in an S1 segment).

**`ft_bid_ip`**
- Source: `event_log.bid_ip` (CTV) or `impression_log.bid_ip` (display)
- Audit-trail lookup. Only populated when `cp_ft_ad_served_id` is not NULL.

**`ft_vast_ip`**
- Source: `event_log.ip` (CTV) or `impression_log.ip` (display)
- Audit-trail lookup. The IP at the time the S1 ad played.

**`ft_time`**
- Source: `event_log.time` or `impression_log.time`
- When the S1 impression was served.

---

### Prior VV — from `clickpass_log` (self-join) and `event_log`/`impression_log` (IPs)

The prior VV is the most recent VV that advanced this IP into the current targeting stage. For a S3 VV, this is the S2 VV that put the IP into S3 targeting. For a S2 VV, this is the S1 VV that put the IP into S2 targeting.

**How we find the prior VV:**
1. `prior_vv_pool` scans `clickpass_log` for a 90-day lookback. Every VV in that window is a candidate.
2. We match where `prior_vv_pool.ip = lt_bid_ip` (the prior VV's redirect IP matches this VV's bid IP — ~94% accurate)
3. We require `prior_vv_time < vv_time` (must be before this VV)
4. We require `pv.pv_stage = cp.vv_stage - 1` (the prior VV must be stage N-1 — for S3, we need the S2 VV; not just any prior VV)
5. If multiple S(N-1) VVs exist, we take the most recent (last-touch rule per Zach)

**`prior_vv_ad_served_id`**
- Source: `clickpass_log.ad_served_id` (the prior VV row's impression UUID)
- What it is: The impression ID of the S2 VV (for S3 rows). This is the same UUID that would be `ad_served_id` on THAT VV's row in this table.
- Skeptical Q: *"What if the prior VV isn't in the 90-day window?"* NULL. S3 VVs more than 90 days old won't have prior VV data. For production, incremental runs always have a 90-day lookback — this affects only historical rows at the very start of the table.
- Skeptical Q: *"What if pv_stage = 3 for an S3 row?"* That was a design bug (fixed). The `pv.pv_stage = cp.vv_stage - 1` condition ensures the prior VV is always the stage-advancement VV, not another same-stage VV. A S3 VV's prior_vv must have pv_stage=2.

**`prior_vv_time`**
- Source: `clickpass_log.time` for the prior VV row

**`pv_campaign_id`**
- Source: `clickpass_log.campaign_id` for the prior VV row

**`pv_stage`**
- Source: `campaigns.funnel_level` joined on `pv_campaign_id` (inside `prior_vv_pool` CTE)
- Should always equal `vv_stage - 1`.

**`pv_redirect_ip`**
- Source: `clickpass_log.ip` for the prior VV
- What it is: The IP that was recorded at the prior VV's site visit redirect. This is the IP that entered the S(N) targeting segment — the IP that made this VV possible.
- Important: This is the primary IP the targeting system used to promote the IP to the next stage. In 94% of cases `pv_redirect_ip = pv_lt_bid_ip`. When `pv_lt_bid_ip` is NULL (impression outside lookback), `pv_redirect_ip` is the reliable fallback.

**`pv_lt_bid_ip`**
- Source: `event_log.bid_ip` (CTV) or `impression_log.bid_ip` (display), JOINed on `prior_vv_ad_served_id`
- Audit-trail lookup for the prior VV's impression bid IP.
- Skeptical Q: *"Why is this NULL for some rows?"* The prior VV's impression may predate the `el_all`/`il_all` window. In production (90-day window), this is ~1-2% NULL. In ad-hoc testing with shorter windows, NULL rate is higher. When NULL, use `pv_redirect_ip` — it's ~94% equivalent.

**`pv_lt_vast_ip`**
- Source: `event_log.ip` (CTV) or `impression_log.ip` (display), JOINed on `prior_vv_ad_served_id`

**`pv_lt_time`**
- Source: `event_log.time` or `impression_log.time` for the prior VV's impression

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

**1. S1 traceability is ~60%, not 100%**

`cp_ft_ad_served_id` is NULL in ~40% of VVs overall, and higher (~60-74%) for S3 VVs specifically. When it's NULL, there is no S1 impression ID anywhere in the system. It was not written at VV time and cannot be backfilled. This is a system limitation, not a query limitation.

For the rows where it IS NULL: Zach's traversal algorithm (scan event_log for the first occurrence of `lt_bid_ip` in any S1 campaign within 30 days) can partially recover S1 IP information, but not the exact `ad_served_id`. This would be a separate lookup query, not a stored column.

**2. Prior VV match is ~94% accurate, not 100%**

We match `prior_vv_pool.ip = lt_bid_ip` (prior VV's redirect IP = this VV's bid IP). The targeting system uses VAST IP to populate segments, but `redirect_ip ≈ lt_vast_ip` in 94% of cases. The 6% miss rate is due to IP mutation between VAST and redirect — the same phenomenon we're measuring.

**3. pv_lt_bid_ip is ~99%+ in production, ~67% in ad-hoc testing**

In production with a 90-day event_log/impression_log window, almost all prior VV impressions are in range. In short-window ad-hoc queries, ~33% of prior VV impressions fall outside the window. When NULL, `pv_redirect_ip` is the reliable fallback (~94% equivalent).

**4. NTB is not auditable via SQL**

`clickpass_is_new` and `visit_is_new` are JavaScript-determined and cannot be independently verified through log analysis. The 41-56% disagreement rate is expected and real.

**5. Display IPs vs CTV IPs**

CTV impressions: sourced from `event_log` (`vast_impression` event). Display impressions: sourced from `impression_log`. Non-viewable display impressions appear ONLY in `impression_log` — they never generate a VAST event. The `COALESCE(el, il)` pattern handles this correctly.

---

## Part 7: The Join Architecture

How the query builds one row from five sources:

```
clickpass_log (anchor — one VV per row)
  ├── event_log (CTV, joined 3x as: lt, ft, pv_lt — via ad_served_id)
  ├── impression_log (display, joined 3x as: lt_d, ft_d, pv_lt_d — display fallback)
  ├── ui_visits (visit IP — via ad_served_id, CAST as STRING)
  ├── clickpass_log (self-join via prior_vv_pool — prior VV row)
  └── campaigns (stage lookup — joined inside CTEs)
```

**One scan, joined multiple times:** `event_log` is scanned once into `el_all`, then used three times (last-touch, first-touch, prior VV impression). Same for `impression_log` → `il_all`. This is an 8% cost saving vs three separate scans.

**COALESCE pattern:** For every IP column, the pattern is `COALESCE(el.ip, il.ip)`. CTV (`event_log`) is preferred; display (`impression_log`) fills in NULLs. If both are NULL, the impression was not found in either log (rare, typically a timing edge case or data gap).

---

## Part 8: Does It Answer Zach's Question?

Zach's request: *"exact audit trail for a vv — every impression id/ip for each stage that led to that vv."*

**What we have:**

For every VV row:
- S(N) impression ID: `ad_served_id` — always populated
- S(N) impression IPs: `lt_bid_ip`, `lt_vast_ip`, `redirect_ip`, `visit_ip` — ~99%+ in production
- S(N-1) impression ID: `prior_vv_ad_served_id` — populated when prior VV in 90-day window (~95%+)
- S(N-1) impression IPs: `pv_lt_bid_ip`, `pv_lt_vast_ip`, `pv_redirect_ip` — mostly populated, `pv_redirect_ip` always available
- S1 impression ID: `cp_ft_ad_served_id` — populated ~60% of the time (system-recorded, not audit lookup)
- S1 impression IPs: `ft_bid_ip`, `ft_vast_ip` — populated when S1 ID is not NULL

**What is NOT achievable (honest answer):**

Complete S3→S1 chain for every row is not possible. The `first_touch_ad_served_id` field in `clickpass_log` has a structural NULL rate of ~40% (higher for S3 VVs). This data was never written — it cannot be reconstructed.

**Zach's specific clarification point:** *"its not super clear which ft values are coming from the table traversal vs what is coming from the clickpass logs attempt to look that data up."*

This is addressed by the naming convention:
- `cp_ft_ad_served_id` → the `cp_` prefix means this came directly from `clickpass_log.first_touch_ad_served_id` (what the system stored)
- `ft_bid_ip`, `ft_vast_ip`, `ft_time` → no `cp_` prefix = these are our audit lookup from `event_log`/`impression_log` based on that ID
