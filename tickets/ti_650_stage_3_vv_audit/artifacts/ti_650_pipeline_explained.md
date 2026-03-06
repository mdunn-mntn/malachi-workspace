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
- The prior VV that advanced this IP into the current stage (e.g., the S2 VV that put the IP into S3 targeting)
- The original first-touch S1 impression that started this IP's funnel

For a Stage 3 VV, this means three impression IDs and their associated IPs:

| Slot | Impression ID column | What it is |
|------|---------------------|------------|
| **This VV** | `ad_served_id` | The S(N) impression that triggered this VV |
| **Prior VV** | `prior_vv_ad_served_id` | The most recent prior VV that advanced this IP into S(N) targeting — pv_stage <= vv_stage |
| **S1 first touch** | `s1_ad_served_id` | The S1 impression that started this IP's funnel — resolved via 4-branch chain traversal, ~99%+ populated |

> **Important:** The prior VV does not have to be exactly one stage lower. `pv_stage <= vv_stage` — same-stage prior VVs are allowed. An S3 VV's prior VV can be another S3 VV (e.g. S3→S3→S2→S1 chain). An IP can also enter S3 from an S1 VV directly, in which case pv_stage=1 and `s1_ad_served_id` may equal `prior_vv_ad_served_id`.

> **All stages are anchor rows.** The table is not limited to S3 VVs. `cp_dedup` pulls ALL stages from `clickpass_log`. Zach confirmed: "if the dataset is still reasonable in size i think it would be good to have the info for all impressions/vv." A S1-only VV, a S2→S1 chain, or a S3→S3→S2→S1 chain are all present as anchor rows.

Each impression slot also has its IPs in columns (bid IP, VAST IP, redirect IP, visit IP).

**Does this answer Zach's exact audit request?**

Zach: *"we need an exact audit trail for a vv. that means every impression id/ip for each stage that lead to that vv."*

| Zach needs | Our table | Coverage |
|------------|-----------|----------|
| S(N) impression ID + IPs | `ad_served_id`, `lt_bid_ip`, `lt_vast_ip` | ~100% |
| Lower-stage prior VV impression ID + IPs | `prior_vv_ad_served_id`, `pv_lt_bid_ip`, `pv_lt_vast_ip` | ~95%+ in production (90-day lookback) |
| S1 impression ID (system-recorded shortcut) | `cp_ft_ad_served_id` | ~60% (system limitation — retained as comparison reference) |
| S1 impression ID via chain traversal | `s1_ad_served_id` | ~99%+ in production (4-branch CASE, see Part 3b) |
| S1 bid IP + VAST IP | `s1_bid_ip`, `s1_vast_ip` | ~99%+ (same chain traversal) |

**The table is a traversable chain.** Every VV is traceable to S1 by following `prior_vv_ad_served_id`:
- **S(N) row where `pv_stage=1`:** `prior_vv_ad_served_id` IS the S1 impression. One hop, done.
- **S(N) row where `pv_stage>1`:** `prior_vv_ad_served_id` points to a higher-stage VV. Follow the chain in-query using the S1 CASE logic (see Part 3b below).
- **`cp_ft_ad_served_id`** is a direct shortcut to S1 that the attribution system stored at VV time — works ~60% of the time. Retained for comparison only. When NULL, `s1_ad_served_id` from chain traversal fills in.

The chain traversal works in ~99%+ of production rows because the `prior_vv_ad_served_id` link is backed by a 90-day clickpass_log window. The direct `cp_ft_ad_served_id` shortcut fails ~40% of the time due to a system write-time limitation.

---

## Part 3b: S1 Chain Traversal — 4-Branch CASE

The `s1_ad_served_id`, `s1_bid_ip`, and `s1_vast_ip` columns are resolved in-query using a 4-branch CASE expression, backed by up to 3 additional JOINs (s1_pv, s2_pv, and their impression lookups).

**Why not just use `cp_ft_ad_served_id`?** It's NULL 40% of the time and cannot be backfilled. Chain traversal resolves ~99%+.

**The 4 branches:**

| Branch | Condition | Resolution | Chain pattern |
|--------|-----------|-----------|---------------|
| 1 | `vv_stage = 1` | Current VV IS the S1 impression | S1 (no prior VV) |
| 2 | `pv.pv_stage = 1` | Prior VV IS the S1 impression | S2→S1 or S3→S1 |
| 3 | `s1_pv.pv_stage = 1` | Second-hop finds S1 | S2→S2→S1 or S3→S2→S1 or S3→S3→S1 |
| 4 | ELSE | Third-hop (`s2_pv`) finds S1 | S3→S3→S2→S1 or S3→S2→S2→S1 |

**The JOIN chain that powers this:**
```
pv    = prior VV of the current VV (redirect_ip = lt_bid_ip)
s1_pv = prior VV of pv (ip = pv_lt_bid_ip, pv_stage <= pv.pv_stage)
s2_pv = prior VV of s1_pv with pv_stage=1 (ip = s1_lt_bid_ip)
```

`s1_pv.pv_stage <= pv.pv_stage` keeps the chain monotonically non-increasing in stage, preventing spurious high-stage matches as intermediate hops. `s2_pv.pv_stage = 1` terminates the chain — it MUST be S1.

**S3→S3→S3→S1 (4+ hops) are theoretically possible but extremely rare.** `s1_ad_served_id` will be NULL for those — they fall through all branches. The 90-day lookback and `<= pv_stage` constraint reduce but do not eliminate this case.

**All permutations validated (advertiser 37775, 2026-02-04):**

| vv_stage | pv_stage | s1_pv_stage | Branch | ~Row count |
|----------|----------|-------------|--------|-----------|
| 1 | — | — | 1 | 44K+ |
| 2 | 1 | — | 2 | 10K+ |
| 3 | 1 | — | 2 | 125K+ |
| 2 | 2 | 1 | 3 | 57K |
| 3 | 2 | 1 | 3 | 115K |
| 3 | 3 | 1 | 3 | 151K |
| 2 | 2 | 2 | 4 | 56K |
| 3 | 2 | 2 | 4 | 108K |
| 3 | 3 | 2 | 4 | 141K |
| 3 | 3 | 3 | 4 | 211K |

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

prior_vv_ad_served_id  = "a4074373-..."    # The prior VV's impression UUID (pv_stage=3 here)
pv_stage               = 3                # Prior VV is same stage (S3→S3 chain)
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

How to read it: This S3 VV was triggered by impression `003a01cf` on 2026-02-04. The IP `172.59.192.138` had a prior S3 VV (`a4074373`) on 2026-01-25. The 4-branch CASE resolved the S1 VV to `a12e289d` via the s1_pv JOIN (branch 3 or 4 — s1_pv found an S1 entry in `prior_vv_pool` for this IP). The system's `cp_ft_ad_served_id` was NULL, but chain traversal recovered the S1 impression.

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

### S1 (first-touch) impression — resolved via chain traversal

**`cp_ft_ad_served_id`**
- Source: `clickpass_log.first_touch_ad_served_id`
- What it is: The impression UUID the attribution system recorded as "the first time this IP was ever served an ad for this advertiser." Written when the VV fires. Cannot be updated retroactively.
- Skeptical Q: *"Why is it NULL 40% of the time?"* The system only writes it when the stack has a clear first touch. High-traffic IPs, IPs that had impressions before the field was introduced, and attribution stack resets all result in NULL. It is a system limitation, not a query bug.
- **This column is retained as a comparison reference only.** Use `s1_ad_served_id` for audit work.

**`s1_ad_served_id`**
- Source: Resolved via 4-branch CASE in the `with_all_joins` CTE (see Part 3b).
- What it is: The ad_served_id of the S1 VV that started this IP's funnel. Unlike `cp_ft_ad_served_id`, this is computed by traversing the `prior_vv_pool` chain — no dependency on the write-time system field.
- Coverage: ~99%+ in production with 90-day lookback. NULL only for chains deeper than 3 hops or impressions outside the lookback window.
- Skeptical Q: *"Why is this better than cp_ft_ad_served_id?"* `cp_ft_ad_served_id` is NULL 40% of the time and cannot be recovered. `s1_ad_served_id` uses the same source data (clickpass_log IP matching) to traverse the chain and fills in ~59 of those 60 missing points.

**`s1_bid_ip`**
- Source: `event_log.bid_ip` (CTV) or `impression_log.bid_ip` (display), JOINed on the resolved S1 ad_served_id.
- What it is: The IP at auction time for the S1 impression. The "original bid IP" that started this IP's funnel.

**`s1_vast_ip`**
- Source: `event_log.ip` (CTV) or `impression_log.ip` (display), JOINed on the resolved S1 ad_served_id.
- What it is: The IP at the time the S1 ad played (VAST or display render).

---

### Prior VV — from `clickpass_log` (self-join) and `event_log`/`impression_log` (IPs)

The prior VV is the most recent VV that advanced this IP into the current targeting stage. For a S3 VV, this is the S2 VV that put the IP into S3 targeting. For a S2 VV, this is the S1 VV that put the IP into S2 targeting.

**How we find the prior VV:**
1. `prior_vv_pool` scans `clickpass_log` for a 90-day lookback. Every VV in that window is a candidate.
2. We match where `prior_vv_pool.ip = lt_bid_ip` (the prior VV's redirect IP matches this VV's bid IP — ~94% accurate)
3. We require `prior_vv_time < vv_time` (must be before this VV)
4. We require `pv.pv_stage <= cp.vv_stage` (prior VV can be same stage OR lower — enables S3→S3→S2→S1 chain traversal)
5. If multiple prior VVs exist, we take the most recent (last-touch rule per Zach)

**`prior_vv_ad_served_id`**
- Source: `clickpass_log.ad_served_id` (the prior VV row's impression UUID)
- What it is: The impression ID of the S2 VV (for S3 rows). This is the same UUID that would be `ad_served_id` on THAT VV's row in this table.
- Skeptical Q: *"What if the prior VV isn't in the 90-day window?"* NULL. S3 VVs more than 90 days old won't have prior VV data. For production, incremental runs always have a 90-day lookback — this affects only historical rows at the very start of the table.
- Skeptical Q: *"Can pv_stage = 3 for an S3 row?"* Yes. The `pv.pv_stage <= cp.vv_stage` condition allows same-stage prior VVs. An S3 VV's prior VV can be another S3 VV — this is the first hop in an S3→S3→S2→S1 chain. The S1 is then resolved via s1_pv/s2_pv JOINs (see Part 3b).
- Skeptical Q: *"Why allow pv_stage=1 for S3 rows, not just pv_stage=2?"* Because an IP can enter S3 from an S1 VV directly (S1 impression → S1 VV → IP enters S3 targeting). There doesn't need to be an S2 VV in the chain. If the prior VV is a S1 VV, then `pv_stage=1` and `prior_vv_ad_served_id` may equal `cp_ft_ad_served_id`.

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

**1. `cp_ft_ad_served_id` is NULL ~40% of the time — but `s1_ad_served_id` covers ~99%+**

`cp_ft_ad_served_id` (the system-written first-touch field) is NULL in ~40% of VVs overall, and higher (~60-74%) for S3 VVs specifically. It was not written at VV time and cannot be backfilled. This is a system limitation.

The `s1_ad_served_id` column was built specifically to work around this: it uses chain traversal (4-branch CASE, backed by `prior_vv_pool` JOINs) to resolve S1 for ~99%+ of rows. `s1_ad_served_id` is NULL only for chains deeper than 3 hops (extremely rare) or for VVs whose S1 impression predates the 90-day lookback window.

**`cp_ft_ad_served_id` is retained in the table as a comparison reference only.** Users can validate the chain traversal by checking `s1_ad_served_id = cp_ft_ad_served_id` where both are non-NULL (should agree in the vast majority of cases).

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

How the query builds one row from five sources (13 LEFT JOINs total):

```
clickpass_log (anchor — one VV per row, all stages)
  ├── event_log  (CTV, scanned once → el_all, joined 4x: lt, pv_lt, s1_lt, s2_lt)
  ├── impression_log (display, scanned once → il_all, joined 4x: same slots as el_all)
  ├── ui_visits  (visit IP — via ad_served_id, CAST as STRING)
  ├── clickpass_log (self via prior_vv_pool, joined 4x: pv, s1_pv, s2_pv + campaigns inside pool)
  └── campaigns  (stage lookup — joined inside cp_dedup and prior_vv_pool CTEs)
```

**One scan, joined multiple times:** `event_log` is scanned once into `el_all`, then used four times (lt = last touch, pv_lt = prior VV impression, s1_lt = s1_pv impression, s2_lt = s2_pv impression). Same for `impression_log` → `il_all`. This is significant cost savings vs four separate scans.

**COALESCE pattern:** For every IP column, the pattern is `COALESCE(el.ip, il.ip)`. CTV (`event_log`) is preferred; display (`impression_log`) fills in NULLs. If both are NULL, the impression was not found in either log (rare, typically a timing edge case or data gap).

**S1 chain traversal JOINs (s1_pv, s2_pv):**
- `s1_pv` = second-level prior VV pool JOIN. Finds the VV whose redirect IP = `pv_lt_bid_ip`. `s1_pv.pv_stage <= pv.pv_stage` keeps the chain monotonically non-increasing.
- `s2_pv` = third-level prior VV pool JOIN. Only active when `s1_pv` found a non-S1 intermediate VV. `s2_pv.pv_stage = 1` — this terminates the chain.
- `s1_lt`, `s1_lt_d`, `s2_lt`, `s2_lt_d` = impression log lookups for the IPs at each traversal hop.

**Ray's TTL context (from architecture diagram):** The longest real-world chain (S1 impression → S3 VV) spans ~83 days in representative examples. S2 and S3 audience TTLs are 30 days each. The 90-day lookback window is conservatively sized to cover virtually all chains with room to spare.

---

## Part 8: Does It Answer Zach's Question?

Zach's request: *"exact audit trail for a vv — every impression id/ip for each stage that led to that vv."*

**What we have:**

For every VV row:
- S(N) impression ID: `ad_served_id` — always populated
- S(N) impression IPs: `lt_bid_ip`, `lt_vast_ip`, `redirect_ip`, `visit_ip` — ~99%+ in production
- S(N-1) impression ID: `prior_vv_ad_served_id` — populated when prior VV in 90-day window (~95%+)
- S(N-1) impression IPs: `pv_lt_bid_ip`, `pv_lt_vast_ip`, `pv_redirect_ip` — mostly populated, `pv_redirect_ip` always available
- S1 impression ID (system-recorded): `cp_ft_ad_served_id` — ~60%, retained as comparison reference
- S1 impression ID (chain traversal): `s1_ad_served_id` — ~99%+, the primary audit-trail S1 field
- S1 impression IPs: `s1_bid_ip`, `s1_vast_ip` — populated when S1 chain resolves (~99%+)

**What is NOT achievable (honest answer):**

S1 chains deeper than 3 hops (e.g. S3→S3→S3→S3→S1) will have `s1_ad_served_id = NULL`. These are extremely rare. The 4-branch CASE covers all realistic chains. `cp_ft_ad_served_id` remains as an independent cross-check.

**Zach's specific clarification point:** *"its not super clear which ft values are coming from the table traversal vs what is coming from the clickpass logs attempt to look that data up."*

This is addressed by the naming convention:
- `cp_ft_ad_served_id` → the `cp_` prefix means this came directly from `clickpass_log.first_touch_ad_served_id` (what the system stored)
- `s1_ad_served_id`, `s1_bid_ip`, `s1_vast_ip` → the `s1_` prefix = our audit chain traversal via `prior_vv_pool` JOINs

They are independent: `cp_ft_ad_served_id` is what the system wrote; `s1_ad_served_id` is what the chain traversal resolved. When both are non-NULL, they should agree. The `s1_` columns work when `cp_ft_ad_served_id` is NULL.
