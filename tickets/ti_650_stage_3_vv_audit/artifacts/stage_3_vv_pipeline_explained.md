# How the Stage 3 Verified Visit Pipeline Works

A ground-up explanation of the MNTN ad-serving pipeline, how IPs move through it, and how we trace a verified visit back to its original bid.

---

## Part 1: Definitions

**IP Address** — The network address of the device (or household router) viewing the ad or visiting the site. This is the core identity signal in the pipeline. It can change between events if the user switches networks, uses a VPN, or visits from a different device.

**Verified Visit (VV)** — A page view on an advertiser's website that MNTN can attribute to a prior ad impression. The user saw an MNTN ad, and later visited the advertiser's site. This is the end goal — proof that the ad worked.

**New-to-Brand (NTB)** — A verified visit from a household that hasn't previously purchased from or visited the advertiser. MNTN's NTB targeting delivers ads only to these new households. NTB accuracy at bid time is 99.99%.

**`is_new`** — A boolean flag that says "this visitor is new to the brand." It's determined by a JavaScript tracking pixel in the browser — a client-side check, not a database lookup. This means it's not auditable via SQL.

**`ad_served_id`** — A UUID assigned when an ad is served. This is the single most important identifier in the pipeline. It follows the ad from serve through VAST playback through redirect through page view. It's the thread that ties everything together.

**`impression_id`** — A steelhouse-format ID (e.g., `1770684616749257.3333240021.92.steelhouse`) assigned at the impression/serve level. Used to link impressions to win notifications.

**VAST** — Video Ad Serving Template. An IAB standard for how video ads (especially on CTV/connected TV) are delivered. When a CTV device plays an ad, it fires VAST events: `vast_impression` (ad loaded), `vast_start` (playback began), `vast_firstQuartile`, etc. Non-video ads (display, mobile web) do NOT fire VAST events.

**CTV** — Connected TV. A TV connected to the internet (Roku, Fire TV, Apple TV, smart TVs). CTV ads are video ads delivered via VAST. CTV inventory has near-perfect traceability because VAST events are always fired.

**Cross-device** — When the ad is served on one device (e.g., the living room TV) but the verified visit happens on another device (e.g., a phone or laptop). The IP can change because the devices may be on different networks.

---

## Part 2: What Stages Actually Are

**Stages are campaign targeting stages, not just event types.** Stage 1, 2, and 3 are distinct campaign groups in the MES pipeline. Each stage targets a different IP audience based on prior events. The number of impressions doesn't determine the stage — what determines it is the IP's event history.

### The three campaign stages

| Stage | What Populates the Segment | Source event | Signal in data |
|-------|---------------------------|--------------|----------------|
| Stage 1 | Initial audience (customer data, lookalike, etc.) | Campaign setup | — |
| Stage 2 | **Stage 1 VAST Impression IP** | `event_log.ip` where Stage 1 impression fired VAST | Pink box in MES diagram = "Used For Targeting" |
| Stage 3 | **Stage 2 VAST Impression IP** | `event_log.ip` where Stage 2 impression fired VAST | Pink box in MES diagram = "Used For Targeting" |

**The green line rule (from the MES Pipeline diagram):** `Stage N VAST Impression IP → Stage N+1 Segment IP`. The VAST Impression event (pink boxes) is explicitly "Used For Targeting" — that IP feeds the next stage's targeting segment. Bid, Serve, Win, and Vast Start are beige ("Not Really Used Directly For Targeting") — they don't generate segment membership.

**Stage 2 is populated ONLY from Stage 1 VAST IPs.** Zach (2026-03-03): *"it's not the IPs from the vast impression from stage two or stage three. It's just stage one."* The same logic applies at Stage 3: Stage 3 is populated from Stage 2 VAST IPs only, not Stage 3 VAST IPs.

**The blue lines in the MES diagram are NOT segment population — they are the audit attribution chain (2026-03-03, Zach confirmed).** Zach: *"blue lines are vv"* and *"the lines are exactly how the data flows right now."* The blue lines show `ad_served_id` data flows: Stage 1 Vast Start → Stage 3 VV (via `first_touch_ad_served_id`), Stage 2 Vast Start → Stage 3 VV (via `first_touch_ad_served_id`), Stage 3 Vast Start → Stage 3 VV (via `ad_served_id`), and Stage 3 Bid → Stage 3 VV (via `bid_ip`). This is exactly what the `ft` and `cp` CTEs in A4f are tracing. The blue lines explain WHY `first_touch_ad_served_id` can point to a Stage 1 or Stage 2 impression.

The diagram is showing **two systems simultaneously**: the green side (targeting — how segments get built from VAST IPs) and the blue side (attribution — how the audit traces from the Stage 3 VV back through ad_served_id to the original bid).

### The mutation consequence for Stage 2 targeting

Because Stage 2 is built from Stage 1 VAST IPs, mutation at Stage 1 determines who ends up in Stage 2. If a Stage 1 impression bids on IP_a and VAST fires at IP_b (mutation), it is **IP_b** that gets added to Stage 2 — not IP_a. The segment is keyed on the VAST playback IP, not the bid IP.

When Stage 2 then serves an impression to IP_b and VAST fires at IP_c (another mutation), IP_c is NOT added to Stage 2. Stage 2 was already locked from Stage 1 VAST events. Zach: *"IP_c would not actually show up in the targetable audience of stage two at that point."* IP_c does not enter any targeting pool from that Stage 2 impression — it's simply lost.

### How the segment system works internally

**Important nuance from Zach (2026-03-03):** "there is no SQL being run" in the actual segment store — the data is stored as a key-value structure: for every (ip, datasource) key there is a value list of (category, timestamp). Events are upserts. The Spark job (`membership_updates_proto_generate.py`) generates the protobuf update payloads that get applied to this KV store:

```python
.groupBy('ip', 'data_source_id').agg(F.max(F.col('epoch')).alias('epoch'), ...)
```

**Zach's correction (2026-03-03):** "these events add the datasource/category data to the ip state. the segment expression then can evaluate to true. there is a difference between that and directly adding the ip to the segment." The system doesn't "add IPs to segments" directly — it updates IP state, and segment membership is derived. The `ad_served_id` is completely absent from segment membership.

From Zach (Slack, 2026-03-02):
> "stage 2 is as simple as it sounds: whatever ip we see in the vast_start/vast_impression event, we add it to that segment and target it. nothing else"

---

## Part 2.5: A Concrete Example — One IP Through the Full Pipeline

This is the thing that is hardest to reason about abstractly. Here it is with real numbers.

### Scale: what each stage looks like

Zach's example from the 2026-03-03 review meeting:

- Stage 1 audience: **8.5 million IPs** (from customer data, lookalike, etc.)
- Of those, ~10,000 get a Stage 1 impression served (only a fraction of the audience gets an ad)
- Those 10,000 are now in the **Stage 2 targeting audience** (via their VAST playback IPs)
- Of those 10,000, ~2,000 get a Stage 2 impression served
- Those 2,000 Stage 2 impressions can generate Stage 3 VVs

### The full journey for one IP — using the MES diagram IPs

The MES Pipeline diagram (shared by Zach's team) uses concrete IPs to show mutation at every hop. Here is the same example traced from Stage 1 through the Stage 3 VV.

```
STAGE 1 IMPRESSION
  Segment A targeted IP = 1.1.1.1  (the IP in the Stage 1 audience)
  Bid               IP = 1.1.1.1  (bid on the segment IP — matching, no mutation yet)
  Serve             IP = 1.1.1.2  (mutation at serve)
  Win               IP = 1.1.1.3  (mutation at win)
  Vast Impression   IP = 1.1.1.4  ← PINK "Used For Targeting"
  Vast Start        IP = 1.1.1.5

  → 1.1.1.4 (Stage 1 VAST IP) added to Stage 2 Segment B via green line

STAGE 2 IMPRESSION  (30 days later)
  Segment B targeted IP = 1.1.1.4  (= Stage 1 VAST Impression IP — the green line target)
  Bid               IP = 1.1.1.4
  Serve             IP = 1.1.1.6
  Win               IP = 1.1.1.7
  Vast Impression   IP = 1.1.1.8  ← PINK "Used For Targeting"
  Vast Start        IP = 1.1.1.9

  → 1.1.1.8 (Stage 2 VAST IP) added to Stage 3 Segment C via green line

STAGE 3 IMPRESSION  (30 days later) ← THIS IS WHAT OUR AUDIT TABLE CAPTURES
  Segment C targeted IP = 1.1.1.8  (= Stage 2 VAST Impression IP — the green line target)
  Bid               IP = 1.1.1.10  (mutation — bid-time IP differs from segment IP)
  Serve             IP = 1.1.1.11
  Win               IP = 1.1.1.12
  Vast Impression   IP = 1.1.1.13  ← PINK (would feed Stage 4 if it existed)
  Vast Start        IP = 1.1.1.14

  → Vast Start fires → user visits advertiser site → Stage 3 VV IP = 1.1.1.14

  Our audit table row:
    bid_ip           = 1.1.1.10   (from event_log.bid_ip — Stage 3 bid IP)
    vast_playback_ip = 1.1.1.13   (from event_log.ip — Stage 3 VAST Impression IP)
    redirect_ip      = 1.1.1.14   (from clickpass_log.ip — Stage 3 VV IP)
    bid_eq_vast         = false   (1.1.1.10 ≠ 1.1.1.13 — bid→VAST mutation)
    vast_eq_redirect    = false   (1.1.1.13 ≠ 1.1.1.14 — VAST→redirect mutation)
    mutated_at_redirect = false   (flag requires bid=vast AND vast≠redirect; bid≠vast here)
    el_matched          = true    (CTV — has VAST data)
```

**Key observations from the diagram:**
1. The Segment IP is the TARGETED IP (white box = "Is the Targeted IP") — but the actual Bid IP can differ. In Stage 3, the segment has 1.1.1.8 but the bid fires for 1.1.1.10.
2. Every single hop can have a different IP. The diagram deliberately shows unique IPs at every step to illustrate all possible mutation points.
3. The Vast Impression IP (pink) is the ONLY event that contributes to the next stage's targeting segment.
4. Our `mutated_at_redirect` flag catches the specific pattern where bid=VAST (CTV device stable) but VAST≠redirect (user switched devices/networks). The diagram's Stage 3 shows both hops mutated, so `mutated_at_redirect` would be false for this specific example — but `bid_eq_vast=false` catches the bid→VAST mutation.

### What mutation at Stage 1 means for Stage 2 targeting

```
Stage 1 impression: Segment IP = 1.1.1.1, Bid IP = 1.1.1.1
  VAST fires at:    1.1.1.4  (mutation — VAST IP differs from bid IP)
  → 1.1.1.4 added to Stage 2 targeting (the VAST IP, not the bid IP)

Stage 2 impression: MNTN targets 1.1.1.4
  Bid fires at 1.1.1.4, VAST fires at:  1.1.1.8  (another mutation)
  → 1.1.1.8 added to Stage 3 targeting (Stage 2 VAST IP)
  → 1.1.1.4 stays in Stage 2; 1.1.1.8 does NOT go back into Stage 2

If Stage 2 VAST had fired at the same IP as Stage 1 bid (1.1.1.1), the Stage 3 segment
would be 1.1.1.1 again. In practice, mutation means Stage 3 often contains IPs that differ
from what was originally bid at Stage 1.
```

### What we CAN and CANNOT see from our audit table

Our audit table (`audit.stage3_vv_ip_lineage`) gives us the IP lineage within the Stage 3 impression:

| What | How | Available |
|------|-----|-----------|
| Bid IP for the Stage 3 impression | `event_log.bid_ip` | Yes |
| VAST playback IP for the Stage 3 impression | `event_log.ip` | Yes (CTV only) |
| Redirect IP (the visit IP) | `clickpass_log.ip` | Yes |
| First-touch impression bid IP | `event_log.bid_ip` via `first_touch_ad_served_id` | Yes (when not NULL) |
| What stage the impression was served in | Not stored on clickpass_log | No — would need campaign_id → stage lookup |
| The IP's full history across Stage 1 and 2 | Not on the VV record | No — would require Zach's traversal method |

---

## Part 3: Two Separate Systems — Targeting vs. Attribution

This is the most important distinction in the pipeline, and the source of most confusion.

### System 1: Targeting (segment membership)

**Question it answers:** "Which IPs should we bid on next?"

This is the membership update system described above. It reads logs, groups by IP, and populates targeting segments. It operates at the **IP level** — no `ad_served_id`, no impression-level tracking. It just knows "this IP has had these types of events."

### System 2: Attribution (VV trace)

**Question it answers:** "This verified visit happened — which ad impressions get credit?"

**Clarification (confirmed):** Zach said "visits will include clicks if that matters." He then clarified on the call: "The click pass log is the log of verified visits" and "we don't put clicks into the stage three." The clickpass_log schema has `click_elapsed`, `click_url`, and `destination_click_url` as columns on every row — click metadata is embedded in the visit record. `ui_visits` has a `click` boolean column that distinguishes click visits from non-click visits. `ui_visits` is the superset that includes display clicks; `clickpass_log` does not include clicks. **Zach also confirmed (2026-03-03):** clickpass_log contains ALL verified visits — CTV and display — not just CTV. "VV can happen for display as well and would be here." Adding clicks to Stage 3 is a future improvement Zach identified.

When a verified visit occurs, the system records it in `clickpass_log` with two key fields:

| Field | What it stores | What it means |
|-------|---------------|--------------|
| `ad_served_id` | UUID of the **most recent** ad serve before the visit | Last-touch attribution — always the newest impression (confirmed: 0 exceptions in 38,360 rows) |
| `first_touch_ad_served_id` | UUID of the **first** ad serve for this user/advertiser | First-touch attribution — may be NULL for 40% of VVs |

These are the **only** impression-level links stored on the VV record. Everything in between (the 2nd, 3rd, 4th impressions if they exist) is not individually traceable from the clickpass record.

### Attribution coverage breakdown (advertiser 37775, Feb 4–10, n = 219,613 VVs)

| Scenario | Count | % | What it means |
|----------|-------|---|---------------|
| Same ID (ad_served_id = first_touch) | 93,297 | 42.48% | Single-impression attribution — the VV's impression was also the first touch |
| Different IDs | 38,360 | 17.47% | Multi-impression attribution — user saw multiple ads; VV links to the last one |
| `first_touch_ad_served_id` is NULL | 87,956 | 40.05% | First-touch data missing — only last-touch available |

The 40% NULL rate on `first_touch_ad_served_id` is a significant gap. For these VVs, first-touch IP tracing is impossible — only the last-touch (most recent) impression can be traced.

**RESOLVED (2026-03-03, Zach confirmed):** Zach stated: "clickpass_log is a real time log. there is no post processing to generate it." This confirms: first_touch_ad_served_id is populated at write time — no batch backfill. If the first-touch data isn't available when the clickpass row is created, the field stays NULL permanently. Our gap analysis (2026-03-02) independently proved this by showing NULL rates are identical 3+ weeks later. Zach also noted: "confirm with Sharad, but I do not believe they do this lookup for stage 1 CTV VV" — suggesting the first-touch lookup may not be performed for certain VV types, which would explain the 40% NULL rate.

### How the audit uses both

Our audit works entirely within the **attribution system**. We start from `clickpass_log` (a VV happened) and trace backward using the `ad_served_id` values to find the IPs at each point. We don't interact with the targeting/segment system at all — we just use the log tables that both systems write to.

---

## Part 4: The Internal Pipeline (What We're Tracing)

For any single ad serve (identified by one `ad_served_id`), these events happen in order:

```
1. BID        The auction happens. MNTN bids on this IP.
      ↓
2. SERVE      MNTN wins. The ad creative is served to the device.
      ↓
3. WIN        The win notification is recorded.
      ↓
4. VAST IMP   The video ad loads on the CTV device. VAST fires.
      ↓
5. VAST START The video begins playing.
      ↓
      ... time passes (seconds to days) ...
      ↓
6. REDIRECT   The user visits the advertiser's site.
               This is the "clickpass" — the verified visit redirect.
      ↓
7. PAGE VIEW  The advertiser's page loads. The visit is recorded.
```

Each step records an IP address. The IP *should* be the same throughout, but it can change — especially between step 5 (VAST playback on the TV) and step 6 (redirect on a phone/laptop), because those might be different devices on different networks.

**Steps 1–5 share one `ad_served_id`.** Step 6 (clickpass) references that same `ad_served_id`, linking the VV to its originating impression. Step 7 (ui_visits) also carries the `ad_served_id`.

### Step-by-step: table, IP column, and join key

| Step | Event | Table | IP Column | What IP It Is | Join to Next Step |
|------|-------|-------|-----------|---------------|-------------------|
| 1 | BID | `win_logs` | `ip`, `device_ip` | The IP MNTN bid on | `auction_id` = CIL's `impression_id` |
| 2 | SERVE | `cost_impression_log` (CIL) | `ip` | Serve-time IP (= bid IP at 100%) | `ad_served_id` (shared with EL, CP) |
| 3 | WIN | `win_logs` | (same row as BID) | (same as BID) | (same as BID) |
| 4 | VAST IMP | `event_log` | `bid_ip`, `ip` | `bid_ip` = bid IP carried forward (= win_logs.ip at 100%). `ip` = CTV playback IP. | `ad_served_id` (shared with CP) |
| 5 | VAST START | `event_log` | (same `ad_served_id`, different `event_type_raw` row) | Same IPs as step 4 | — |
| 6 | REDIRECT | `clickpass_log` | `ip` | User's IP when visiting the site | `ad_served_id` = `CAST(ui_visits.ad_served_id AS STRING)` |
| 7 | PAGE VIEW | `ui_visits` | `ip`, `impression_ip` | `ip` = visit IP (= redirect IP at 99.93%). `impression_ip` = bid IP carried forward from impression_log. | (terminal) |

### Why steps 1–3 collapse into one IP

`win_logs.ip`, `cost_impression_log.ip`, and `event_log.bid_ip` all record the same value — the bid IP. Validated at 100% across 30,502 rows with zero mismatches. Three tables, one IP. CIL and win_logs are redundant for the audit trace.

### The 3 distinct IP checkpoints

```
bid_ip (steps 1-4)  →  vast_playback_ip (step 4-5)  →  redirect_ip (step 6)  ≈  visit_ip (step 7)
event_log.bid_ip       event_log.ip                    clickpass_log.ip          ui_visits.ip
     ↑                      ↑                               ↑                       ↑
 = win/serve IP         CTV device IP                  user's browser IP       same session (99.93%)
 (always equal)         (≈96.5% = bid_ip)              (MUTATION POINT)
```

The only meaningful IP change is at the **VAST → redirect** boundary (steps 5→6). This is where cross-device visits, VPN switches, and network changes show up. Everything before that is the same IP; everything after is the same IP.

---

## Part 5: How the Audit Traces a VV Back to Its Bid IP

### What `clickpass_log` gives us

Each VV record has two attribution pointers:

| Field | Points to | What we get by joining to event_log |
|-------|----------|-------------------------------------|
| `ad_served_id` | The ad serve associated with this VV | bid_ip and vast_playback_ip for that impression |
| `first_touch_ad_served_id` | The very first impression | bid_ip and vast_playback_ip for the first touch |

**There are no fields for intermediate impressions.** If the user saw 5 ads before visiting, we can trace the first one and the VV-associated one. The 2nd, 3rd, and 4th are invisible from the clickpass record.

### RESOLVED: `ad_served_id` = the most recent impression before the visit

Empirically confirmed on 38,360 VVs where `ad_served_id != first_touch_ad_served_id` (advertiser 37775, Feb 4–10):

| Pattern | Count | % |
|---------|-------|---|
| `ad_served_id` VAST time is MORE RECENT than `first_touch` | 35,198 | 91.76% |
| `first_touch` has no VAST event (NULL) — can't compare | 3,149 | 8.21% |
| `ad_served_id` has no VAST event (NULL) — can't compare | 12 | 0.03% |
| `ad_served_id` VAST time is OLDER than `first_touch` | 0 | **0%** |

**Zero exceptions.** When both timestamps are available, `ad_served_id` is always the more recent impression. The 8.21% with NULL first-touch VAST times are cases where the first-touch impression didn't fire a VAST event (non-CTV inventory).

Time gaps between first touch and VV-associated impression range from 2 hours to 815 hours (~34 days). This confirms the 30-day attribution window.

### The simplified trace

```
clickpass_log  ──ad_served_id──▶  event_log
      │                              │
  redirect_ip                   bid_ip (= win IP at 100%)
  visit_ip (via ui_visits)      vast_playback_ip (= VAST IP)
```

Join `clickpass_log` to `event_log` on `ad_served_id`. The event_log row gives us:
- `bid_ip` — the IP at auction time for that specific ad serve
- `ip` — the IP during VAST playback

The clickpass row gives us:
- `ip` — the IP at redirect time (when the user visited the site)

Two tables, one join, full IP lineage for one ad serve.

### First-touch trace

Same mechanics, different join key:

```
clickpass_log  ──first_touch_ad_served_id──▶  event_log
```

This gives us the bid_ip and vast_playback_ip for the very first impression, which may be days or weeks earlier and may have a completely different IP.

---

## Part 6: Table Summary

| Table | BQ Silver Path | Key Columns | What It Records |
|-------|---------------|-------------|-----------------|
| `clickpass_log` | `dw-main-silver.logdata.clickpass_log` | `ad_served_id`, `first_touch_ad_served_id`, `ip`, `is_new`, `is_cross_device`, `campaign_id`, `advertiser_id`, `time` | One row per verified visit redirect. **Starting point of every trace.** |
| `event_log` | `dw-main-silver.logdata.event_log` | `ad_served_id`, `bid_ip`, `ip`, `event_type_raw`, `time` | VAST playback events. Has bid IP AND playback IP. Filter: `event_type_raw = 'vast_impression'`. Dedup: first row per `ad_served_id` by time. |
| `ui_visits` | `dw-main-silver.summarydata.ui_visits` | `ad_served_id` (needs CAST), `ip`, `is_new`, `impression_ip`, `from_verified_impression`, `time` | Page view records. Superset of clickpass (includes display clicks). Filter: `from_verified_impression = true`. |
| `cost_impression_log` | `dw-main-silver.logdata.cost_impression_log` | `ad_served_id`, `impression_id`, `ip` | Ad serve/impression records. Bridge to win_logs via `impression_id`. Optional — not needed for simplified trace. |
| `win_logs` | `dw-main-silver.logdata.win_logs` | `auction_id`, `ip`, `device_ip` | Auction win records. Join via CIL's `impression_id = auction_id`. Optional — independent validation only. |
| `conversion_log` | `dw-main-silver.logdata.conversion_log` | `guid`, `ip`, `original_ip`, `time` | Raw conversion events. Join via `clickpass.page_view_guid = guid`. NTB validation only — not part of IP trace. |

### Key IP columns across tables

| Column | Table | What IP it captures |
|--------|-------|-------------------|
| `bid_ip` | event_log | IP at bid/auction time for that ad serve |
| `ip` | event_log | IP during VAST playback (CTV device IP) |
| `ip` | clickpass_log | IP at redirect time (visit IP) |
| `ip` | ui_visits | IP at page load time (= clickpass IP at 99.93%) |
| `impression_ip` | ui_visits | Bid IP carried forward from impression_log (independent source; matches event_log.bid_ip at 95.8–100% depending on advertiser) |
| `ip` | cost_impression_log | IP at serve time (= bid IP at 100%) |
| `ip` | win_logs | IP at win time (= bid IP at 100%) |
| `original_ip` | event_log | Pre-iCloud Private Relay IP (raw connection IP) |

### Key join paths

| From → To | Join Key | Notes |
|-----------|---------|-------|
| clickpass → event_log | `cp.ad_served_id = el.ad_served_id` | VV-associated impression trace |
| clickpass → event_log | `cp.first_touch_ad_served_id = el.ad_served_id` | First-touch impression trace |
| clickpass → ui_visits | `cp.ad_served_id = CAST(v.ad_served_id AS STRING)` | Visit enrichment. Filter: `from_verified_impression = true` |
| clickpass → CIL | `cp.ad_served_id = cil.ad_served_id` | Optional bridge to win_logs |
| CIL → win_logs | `cil.impression_id = w.auction_id` | Steelhouse format IDs |
| clickpass → conversion_log | `cp.page_view_guid = cl.guid` | NTB validation only |

---

## Part 7: The Tables — Detailed

Each step in the pipeline has a corresponding database table. Here's what each one holds and what IP it captures.

### `clickpass_log` — The Redirect (Step 6)

**This is our starting point.** Every row is one verified visit redirect event. When a user visits the advertiser's site after seeing an ad, this table records it.

| Column | What it is |
|--------|-----------|
| `ad_served_id` | UUID — the thread connecting this VV to its associated ad serve |
| `first_touch_ad_served_id` | UUID — the very first impression in the attribution chain |
| `ip` | The IP at redirect time — the user's IP when they visited |
| `is_new` | Client-side pixel's opinion: is this a new visitor? |
| `is_cross_device` | Was the ad on one device and the visit on another? |
| `campaign_id` | Which campaign this VV belongs to |
| `time` | When the redirect happened |
| `advertiser_id` | Which advertiser's site was visited |

**Why we start here:** This is the definitive VV record. One row = one verified visit. Everything else is traced backward from here.

**BQ Silver location:** `dw-main-silver.logdata.clickpass_log`

---

### `event_log` — VAST Playback (Steps 4-5)

When a video ad plays on a CTV device, the VAST protocol fires events. Each event is a row in `event_log`. The most important event type is `vast_impression` — the moment the ad loaded and was viewable.

| Column | What it is |
|--------|-----------|
| `ad_served_id` | UUID — same as clickpass, this is how we join |
| `ip` | The IP of the device playing the ad (the CTV's IP) |
| `bid_ip` | **The IP at original bid time.** This is the gold column. |
| `event_type_raw` | Which VAST event: `vast_impression`, `vast_start`, etc. |
| `time` | When the VAST event fired |

**Why this table matters:** It has TWO IP columns:
- `ip` = the VAST playback IP (the CTV's current IP when the ad played)
- `bid_ip` = the original bid IP (the IP MNTN bid on)

The `bid_ip` column is the key discovery that simplified our entire trace. It gives us the bid IP for that specific ad serve directly, without needing to join through cost_impression_log and win_logs.

**Why we filter `event_type_raw = 'vast_impression'`:** A single ad playback fires multiple VAST events (impression, start, firstQuartile, midpoint, thirdQuartile, complete). They all share the same `ad_served_id` and the same IPs. We only need one row per ad serve, so we take the first `vast_impression`.

**Why we dedup:** Occasionally a publisher replays the VAST file, creating duplicate `vast_impression` events for the same `ad_served_id`. We use `ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time)` and take `rn = 1` to get the first one.

**Why non-CTV inventory has no event_log row:** Display and mobile web ads don't use VAST. No VAST = no `event_log` row = no `bid_ip` to trace. This is why `el_matched` varies from 21.7% to 99.97% across advertisers — it reflects the CTV percentage of their verified visits. **Note (Zach, 2026-03-03):** clickpass_log contains ALL verified visits — CTV and display — not just CTV. "VV can happen for display as well." The EL match rate measures what percentage of those VVs had a VAST event to trace through.

**Dual role in targeting:** The VAST event IP (`event_log.ip`) is also what the membership update system uses to populate Stage 2 segments. When this IP fires, it gets added to the targeting pool for the next campaign cycle.

**BQ Silver location:** `dw-main-silver.logdata.event_log`

---

### `ui_visits` — The Verified Visit Record (Step 7)

**Terminology note (Zach, 2026-03-03):** `ui_visits` is NOT a "page view." Zach explicitly: *"Page View is something different. That's the Google blog."* `ui_visits` is the verified visit record — a visit that MNTN has confirmed is attributable to an ad impression. Page views (Google Analytics) are a superset; `ui_visits` is a subset filtered to attribution-confirmed visits.

When the advertiser's page loads in the user's browser following a verified visit redirect, this table records it. It's a superset of clickpass — it includes display clicks and other visit types, not just CTV verified visits.

| Column | What it is |
|--------|-----------|
| `ad_served_id` | UUID (note: stored as a different type, needs CAST to join) |
| `ip` | The IP when the page loaded — the "visit IP" |
| `is_new` | A second, independent client-side pixel's opinion on new vs returning |
| `impression_ip` | The bid IP carried forward onto the visit record from impression_log |
| `click` | Boolean — distinguishes click visits from non-click visits (Zach: "there is a column 'click' that defines if it was a click") |
| `from_verified_impression` | Boolean — filter to `true` to get only VV-related visits |
| `time` | When the page view happened |

**Why this table is optional enrichment:** The clickpass redirect (step 6) and the page view (step 7) happen in the same browser session, milliseconds apart. Their IPs match 99.93%+ of the time. The main value of ui_visits is:
- A second `is_new` flag (to compare with clickpass's)
- `impression_ip` (an independent source of the bid IP)
- Confirmation that the visit actually completed

**BQ Silver location:** `dw-main-silver.summarydata.ui_visits` (note: `summarydata` schema, not `logdata`)

---

### `cost_impression_log` (CIL) — The Ad Serve (Step 2)

Records the impression/serve event. One row per ad served.

| Column | What it is |
|--------|-----------|
| `ad_served_id` | UUID — joins to clickpass and event_log |
| `impression_id` | Steelhouse-format ID — joins to win_logs |
| `ip` | The IP at serve time |

**Role in our trace:** Bridge table between `ad_served_id` (used by clickpass/event_log) and `impression_id` (used by win_logs). With the `bid_ip` discovery, this table is no longer needed for the simplified trace — but it's available for independent validation.

**BQ Silver location:** `dw-main-silver.logdata.cost_impression_log`

---

### `win_logs` — The Auction Win (Step 3)

Records that MNTN won the auction for this ad placement.

| Column | What it is |
|--------|-----------|
| `auction_id` | Steelhouse-format ID — joins to CIL's `impression_id` |
| `ip` | The IP at bid/win time — the original bid IP |
| `device_ip` | On BQ (BWN), this is 100% populated and = bid_ip. On GP, always NULL. |

**Role in our trace:** Independent validation only. We already have the bid IP from `event_log.bid_ip`, and they match at 100% (30,502 rows, zero mismatches). Win_logs confirms it but isn't required.

**BQ Silver location:** `dw-main-silver.logdata.win_logs`

---

## Part 8: Why `event_log.bid_ip` Is the Key

Before we discovered `bid_ip`, the trace required 4 joins:

```
clickpass → CIL → win_logs → event_log
              ↑        ↑
         ad_served_id  impression_id/auction_id
```

The `bid_ip` column on event_log stores the original bid IP directly. We validated that `event_log.bid_ip` = `win_logs.ip` at **100%** across 30,502 rows with zero mismatches. Three independent sources all confirm the same value:

1. `event_log.bid_ip` — the primary trace column
2. `cost_impression_log.ip` — serve-time IP (= bid IP at 100%)
3. `bidder_win_notifications.device_ip` — win notification IP (= bid IP at 100%)

Since `event_log` already has both the bid IP and the VAST playback IP, and we can join to it directly from clickpass via `ad_served_id`, the trace collapses to:

```
clickpass_log  ──ad_served_id──▶  event_log
      │                              │
  redirect_ip                   bid_ip (= win IP)
  visit_ip (via ui_visits)      vast_playback_ip (= VAST IP)
```

Two joins instead of four. Same result.

---

## Part 9: Examples (Real Data from BQ Silver)

All examples below use real data from advertiser 37775, Feb 4–10, 2026 (BQ Silver). See `bqresults/example_vvs_by_impression_count.json`, `timeline_type_a_1_impression.json` through `timeline_type_e_369_impressions_outlier.json`.

### Example A: Single impression, no mutation (Type A — real data)

**Source:** `bqresults/timeline_type_a_1_impression.json` — bid_ip `173.184.150.62`, 1 impression

A user sees exactly one MNTN ad. Two days later, they visit the advertiser's site from the same network.

```
Impression 1:  ad_served_id = c00f6066-5f0e-45e7-9cbb-64453676a8b3
               bid_ip  = 173.184.150.62
               vast_ip = 173.184.150.62
               time    = 2026-02-01 23:16:30 UTC

Verified Visit: redirect_ip = 173.184.150.62
                time        = 2026-02-04 00:00:11 UTC  (~2.5 days later)
```

**What our trace produces:**

```
bid_ip           = 173.184.150.62  (from event_log.bid_ip)
vast_playback_ip = 173.184.150.62  (from event_log.ip)
redirect_ip      = 173.184.150.62  (from clickpass_log.ip)

bid_eq_vast          = true     (same IP on the TV)
vast_eq_redirect     = true     (same IP for visit)
mutated_at_redirect  = false    (no mutation)
is_cross_device      = false    (same device)
cp_is_new            = true     (NTB)
```

**No mutation.** Same IP at every checkpoint. ad_served_id = first_touch_ad_served_id (single-impression attribution). This is the simplest case — one ad, one visit, one IP.

---

### Example B: Two impressions, stable IP, cross-device (Type B — real data)

**Source:** `bqresults/timeline_type_b_2_impressions.json` — bid_ip `16.98.111.49`, 2 impressions

A user sees two MNTN ads 20 days apart. Both have the same bid IP despite the gap. The visit is from a different device but the same network.

```
Impression 1:  ad_served_id = 62ea154b-ef6f-4fe8-851e-17f7db7dd0b5
               bid_ip  = 16.98.111.49
               vast_ip = 16.98.111.49
               time    = 2026-01-10 06:29:57 UTC

Impression 2:  ad_served_id = 4bfeeb10-b950-48c3-87a9-118c86f75431
               bid_ip  = 16.98.111.49
               vast_ip = 16.98.111.49
               time    = 2026-01-30 19:26:58 UTC

Verified Visit: redirect_ip = 16.98.111.49
                time        = 2026-02-04 00:00:19 UTC  (~5 days after last impression)
```

**What our trace produces:**

```
bid_ip           = 16.98.111.49    (from event_log.bid_ip — impression 2, the VV-associated one)
vast_playback_ip = 16.98.111.49    (from event_log.ip)
redirect_ip      = 16.98.111.49    (from clickpass_log.ip)

bid_eq_vast          = true     (same IP on the TV)
vast_eq_redirect     = true     (same IP for visit)
mutated_at_redirect  = false    (no mutation)
is_cross_device      = true     (different device — TV ad, phone/laptop visit)
cp_is_new            = false    (returning visitor)
first_touch_ad_served_id = NULL (40% NULL rate — batch processing hasn't caught up)
bid_ip_stable        = true     (both impressions share the same bid_ip)
```

**No mutation despite cross-device.** Cross-device doesn't guarantee mutation — the household has the same external IP on both devices (they're on the same home Wi-Fi). The first_touch_ad_served_id is NULL, consistent with the ~40% NULL rate we see globally.

---

### Example C: Five impressions over 21 days, perfectly stable (Type C — real data)

**Source:** `bqresults/timeline_type_c_5_impressions.json` — bid_ip `71.206.63.109`, 5 impressions

A user sees five MNTN ads over three weeks, all from the same IP. The bid IP is completely stable across all impressions.

```
Impression 1:  ad_served_id = eb387e84-d2e6-45f3-a152-b0731580b5be
               bid_ip = 71.206.63.109, vast_ip = 71.206.63.109
               time   = 2026-01-13 13:04:33 UTC

Impression 2:  bid_ip = 71.206.63.109, vast_ip = 71.206.63.109
               time   = 2026-01-14 00:24:12 UTC  (next day)

Impression 3:  bid_ip = 71.206.63.109, vast_ip = 71.206.63.109
               time   = 2026-01-27 22:34:28 UTC  (13 days later)

Impression 4:  bid_ip = 71.206.63.109, vast_ip = 71.206.63.109
               time   = 2026-01-29 19:52:29 UTC

Impression 5:  ad_served_id = 7905c7a9-1e4c-4404-ac19-1f7e9ead6b56
               bid_ip = 71.206.63.109, vast_ip = 71.206.63.109
               time   = 2026-02-03 23:15:04 UTC

Verified Visit: redirect_ip = 71.206.63.109
                time        = 2026-02-04 00:00:43 UTC  (~45 minutes after last impression)
```

**What our trace produces:**

```
bid_ip           = 71.206.63.109   (from impression 5 — the VV-associated one)
vast_playback_ip = 71.206.63.109
redirect_ip      = 71.206.63.109

bid_eq_vast          = true
vast_eq_redirect     = true
mutated_at_redirect  = false
is_cross_device      = false
cp_is_new            = true     (NTB)
ad_served_id = first_touch_ad_served_id  (single-impression attribution)
```

**Perfect stability over 21 days.** All 5 impressions have the same bid_ip and vast_ip. The ISP assigned a static IP to this household. Note: `ad_served_id = first_touch_ad_served_id` even though there are 5 impressions — the attribution system considers impression 5 both the "most recent" and the "first touch" for this VV. The other 4 impressions exist in event_log but aren't linked from the clickpass record.

---

### Example D: Mutation at redirect (Type D — real data)

**Source:** `bqresults/mutation.json` — UUID `a12c9b22-6ddc-475a-a494-528af5ee83a9`

This is the core finding of the audit. The bid IP and VAST IP match (the CTV device is consistent), but the redirect IP is different — the user visited the advertiser's site from a different network or device.

```
bid_ip           = 188.213.202.24    (from event_log — IP at auction)
vast_playback_ip = 188.213.202.24    (from event_log — IP at CTV playback)
redirect_ip      = 188.213.202.47    (from clickpass_log — IP at site visit)

bid_eq_vast          = true     (CTV device consistent)
vast_eq_redirect     = false    (IP changed between ad and visit)
mutated_at_redirect  = true     ← THIS IS THE MUTATION
el_matched           = true
```

**This is the pattern that drives 5.9–20.8% mutation across advertisers.** The IP changed between VAST playback (on the TV) and redirect (site visit, likely on phone/laptop). Note bid=VAST — the CTV device is stable. The mutation happens when the user switches to another device or network to visit the site. In this case the IPs are in the same /24 block (188.213.202.x), suggesting same ISP but different NAT assignment or device.

---

### Example E: Mutation + first-touch IP divergence (Type E — real data)

**Source:** `bqresults/mutation_first_touch.json` — UUID `34ca16b4-ce5f-4d13-9666-f7ce7f238e4c`

Like Example D, the redirect IP differs from the bid IP. But here, the first-touch impression also has a different bid IP than the last-touch — meaning the user's IP changed between their first ad exposure and their most recent one.

```
Last-touch bid_ip     = 172.56.16.39    (event_log, ad_served_id)
First-touch bid_ip    = 172.59.190.73   (event_log, first_touch_ad_served_id)
redirect_ip           = 73.168.42.52    (clickpass_log)

bid_eq_vast           = [from query]
mutated_at_redirect   = true
ft_bid_eq_lt_bid      = false    ← IP changed across impressions
```

**Three distinct IPs across the user's journey.** First touch was on 172.59.x, last touch on 172.56.x (both T-Mobile ranges — likely mobile IP reassignment), and the site visit on 73.168.x (likely home ISP). This illustrates why we trace both first-touch and last-touch: the user's IP changed even between ad exposures, not just at redirect.

---

### Example F: NTB disagree + mutation (Type F — real data)

**Source:** `bqresults/ntb_disagree.json` — UUID `4af3a55b-c1e7-42ec-bf3e-58f8e6095fe4`

This combines two phenomena: IP mutation at redirect AND disagreement between the two independent NTB (new-to-brand) signals.

```
bid_ip           = 72.128.72.212     (from event_log)
redirect_ip      = 76.39.91.199     (from clickpass_log)

mutated_at_redirect  = true
cp_is_new            = false    (clickpass pixel says: returning visitor)
vv_is_new            = true     (ui_visits pixel says: new visitor)
ntb_agree            = false    ← TWO INDEPENDENT PIXELS DISAGREE
```

**Why does `is_new` disagree?** Both `cp_is_new` and `vv_is_new` come from independent client-side JavaScript pixels — separate tracking calls on the advertiser's page. They can disagree because they use different cookie stores, different timing windows, or different device fingerprinting. This is NOT a data bug; it's the architectural reality of client-side NTB detection. Across the 10 audited advertisers, `is_new` disagrees 41–56% of the time. The mutation here is incidental — it doesn't cause the NTB disagreement.

---

### Example G: Non-CTV ad (no VAST — el_matched = false) (Type G — real data)

**Source:** `bqresults/non_ctv.json` — UUID `0755dd40-d480-4cb6-82cb-14437efcf54e` (advertiser 31357)

When an ad is display/mobile web (not CTV video), there's no VAST playback, and thus no event_log entry. This represents the ~78% of advertiser 31357's VVs that have no event_log row.

```
bid_ip           = NULL              (no event_log row)
vast_playback_ip = NULL              (no event_log row)
redirect_ip      = 16.98.11.206     (from clickpass_log)
is_cross_device  = true

bid_eq_vast          = NULL
vast_eq_redirect     = NULL
mutated_at_redirect  = NULL
el_matched           = false    (event_log join failed — no VAST event)
```

**We can't trace this one back to bid via event_log.** No VAST event means no event_log row, which means no bid_ip. The `impression_ip` from ui_visits may partially fill this gap (it carries the bid IP forward independently from impression_log), but through the event_log path, the lineage is broken. This is why EL match rates vary from 21.7% (31357, mostly non-CTV) to 99.97% (37775, nearly all CTV).

---

### Example H: Redirect ≠ visit IP (Type H — real data, rare)

**Source:** `bqresults/final_full.json` — UUID `cf7659de-81e9-450b-83f2-4894b6f323a6` (advertiser 37775, campaign 450301)

In 99.93%+ of VVs, the clickpass redirect IP equals the ui_visits page-load IP. This is one of the 0.07% where they differ — the user's IP changed between the redirect firing and the browser page fully loading.

```
bid_ip           = 172.58.135.22    (from event_log.bid_ip — IP at bid)
vast_ip          = 172.58.135.22    (from event_log.ip — VAST playback)
redirect_ip      = 172.58.131.114   (from clickpass_log — IP at redirect)
visit_ip         = 66.176.148.243   (from ui_visits — IP at page load)
impression_ip    = 172.58.135.22    (from ui_visits.impression_ip — confirms bid IP)
is_cross_device  = true

bid_eq_vast          = true    (same CTV session)
vast_eq_redirect     = false   (mutated at redirect — the usual place)
redirect_eq_visit    = false   ← RARE — IP changed AGAIN at page load
mutated_at_redirect  = true

ft_bid_ip        = 172.58.135.22    (first-touch bid IP — same as last-touch)
lt_bid_eq_ft_bid = true             (bid IP stable across impressions)
ft_matched       = true
```

**Full IP chain:**

```
172.58.135.22 → 172.58.135.22 → 172.58.131.114 → 66.176.148.243
   (bid)           (VAST)          (redirect)        (visit)
    ✓ match         ✗ mutated       ✗ mutated again
```

**What causes this?** The redirect and page load are nearly simultaneous but technically two separate HTTP requests. If the user's network changes between them — VPN toggling, mobile handoff, NAT reassignment — the IPs can diverge. The redirect IP is `172.58.x` (T-Mobile mobile range) while the visit IP is `66.176.x` (likely home ISP), consistent with a mobile-to-WiFi handoff as the page loaded.

**Additional finding — multi-device attribution (resolved):** This UUID has *multiple* clickpass and ui_visits rows (2 each). Investigation of the full clickpass detail revealed these are two separate verified visits from two different devices in the same household: one from iPhone Safari (`is_cross_device = false`, GA client `2043036188`) and one from Android Chrome (`is_cross_device = true`, GA client `40715985`), approximately 4 hours apart. This is expected multi-device attribution behavior — the same CTV ad generated visits from two different phones. At scale, only 84 out of 219,527 ad_served_ids (0.038%) have multiple rows. The production table deduplicates to 1 row using `QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1`.

**Why it doesn't matter for the audit:** This 0.07% rate confirms that clickpass and ui_visits are recording the same event from essentially the same network position. The redirect-to-visit leg is not where mutation happens — the VAST-to-redirect boundary is.

---

### Outlier: Extreme case — 369 impressions, datacenter/proxy pattern (real data)

**Source:** `bqresults/timeline_type_e_369_impressions_outlier.json` — bid_ip `104.171.65.16`, 369 impressions

This is an anomalous case. A single bid_ip receives 369 impressions over 8 days, but the VAST playback IP **never** matches the bid IP. 98 distinct VAST IPs rotate across the impressions.

```
bid_ip = 104.171.65.16 (CONSTANT across all 369 impressions)

Impression 1:   vast_ip = 85.237.194.150,  time = 2026-02-02 16:44:31 UTC
Impression 2:   vast_ip = 85.237.194.150,  time = 2026-02-02 16:50:52 UTC
Impression 3:   vast_ip = 85.237.194.150,  time = 2026-02-02 16:58:32 UTC
...
Impression 367: vast_ip = 104.234.32.158,  time = 2026-02-10 11:03:57 UTC
Impression 368: vast_ip = 104.234.32.189,  time = 2026-02-10 14:15:04 UTC
Impression 369: vast_ip = 85.237.194.246,  time = 2026-02-10 15:53:01 UTC

Summary:
  total_impressions  = 369
  distinct_vast_ips  = 98
  bid_eq_vast_count  = 0   (bid IP NEVER equals VAST IP)
  bid_ne_vast_count  = 369

Verified Visit: redirect_ip = 85.237.194.211
                time        = 2026-02-10 16:30:52 UTC
                is_cross_device = true
                cp_is_new       = false (returning)
```

**What this pattern means:**

- The bid IP (`104.171.65.16`) is stable — the auction always targets the same address
- The VAST playback IPs rotate across 98 distinct addresses in datacenter-like ranges (`85.237.194.x`, `104.234.32.x`)
- 369 impressions in 8 days = ~46/day — extremely high frequency
- bid_ip never equals vast_ip — **100% bid-to-VAST mutation**

This is consistent with a proxy/VPN scenario: the household's VPN exit node is `104.171.65.16` at bid time, but CTV playback resolves through different endpoints. Or it could be ad-tech infrastructure / verification traffic. Either way, this is an outlier — the vast majority of VVs look like Examples A–G above with far simpler patterns.

---

### What we CAN and CANNOT trace (multiple impressions)

From the Q10c example (5 impressions), the clickpass record stores:

```
first_touch_ad_served_id = 7905c7a9-...   (= ad_served_id — single-attribution)
ad_served_id             = 7905c7a9-...   (Impression 5 — the most recent, CONFIRMED)
ip                       = 71.206.63.109
```

**`ad_served_id` = most recent impression** is empirically confirmed with zero exceptions across 38,360 multi-impression VVs.

**What we CAN trace:**
- First touch bid IP: join event_log on `first_touch_ad_served_id` (when not NULL)
- Last-touch bid IP: join event_log on `ad_served_id`
- Visit IP: from clickpass_log
- Whether first-touch and last-touch bid IPs differ (**14.28% of multi-impression VVs have different bid IPs**)

**What we CANNOT trace:**
- Which impressions happened between first touch and last touch
- The IPs at each intermediate impression
- Whether the IP changed at a specific intermediate impression

**For the audit, this is fine.** We care about the endpoints: "what IP did we originally bid on?" (first touch) and "what IP visited the site?" (redirect). The intermediate impressions don't change the mutation measurement.

**Real-world scale:** Even VVs where `ad_served_id = first_touch_ad_served_id` (the 42.48% "single-impression" group) often have many other impressions to the same IP that just aren't linked to this VV. In the data, these IPs show 6–30 distinct `ad_served_id`s in event_log within the 30-day window. The attribution system picks the most recent one and records the first one — everything else is invisible from the clickpass record.

---

## Part 10: The Joins — Why These Columns

### Join 1: clickpass_log → event_log

```sql
ON el.ad_served_id = cp.ad_served_id
```

**Why `ad_served_id`:** This UUID is assigned at serve time and follows the ad through every downstream event. It's present in both clickpass_log and event_log as a text/STRING column. It's a 1:1 relationship — one ad serve produces one clickpass redirect and one set of VAST events.

**Why not `impression_id`:** Event_log doesn't have `impression_id`. Only CIL and win_logs use that format.

**Why not join on IP:** IPs can change (that's the whole point of this audit). Joining on IP would miss every mutated row.

**Why not join on `advertiser_id` + time range:** Too loose. Multiple ads are served to the same advertiser in the same time window. `ad_served_id` is precise — one row to one row.

### Join 2: clickpass_log → ui_visits

```sql
ON CAST(v.ad_served_id AS STRING) = cp.ad_served_id
AND v.from_verified_impression = true
```

**Why the CAST:** In the underlying data, clickpass stores `ad_served_id` as STRING while ui_visits stores it as a UUID/INT type. The CAST ensures they compare correctly.

**Why `from_verified_impression = true`:** ui_visits is a superset — it includes display clicks, organic visits, and other non-VV traffic. This filter restricts to only visits that came from a verified impression, matching the clickpass population.

### Join 3 (optional): CIL → win_logs

```sql
ON cil.impression_id = w.auction_id
```

**Why `impression_id` to `auction_id`:** These are both steelhouse-format IDs (e.g., `1770684616749257.3333240021.92.steelhouse`). CIL calls it `impression_id`, win_logs calls it `auction_id`, but they're the same value — the auction/impression identifier from the Beeswax ad exchange.

**Why not join win_logs directly to clickpass:** Win_logs doesn't have `ad_served_id`. The only path is clickpass → CIL (via `ad_served_id`) → win_logs (via `impression_id`/`auction_id`). This is why the simplified trace that uses `event_log.bid_ip` is so valuable — it skips this two-hop bridge entirely.

---

## Part 11: Where Mutation Happens and Why

From tracing 3.25 million verified visits across 10 advertisers over 7 days:

| Hop | Mutation Rate | Why |
|-----|--------------|-----|
| Bid → VAST Playback | ~3.5% | Minor — same device, slight network drift |
| **VAST Playback → Redirect** | **1.2%-33.4%** | **This is where all meaningful mutation occurs** |
| Redirect → Page View | <0.07% | Same browser session, milliseconds apart |

The original 10-advertiser sample (Feb 4–10, 3.25M VVs) showed 5.9%–20.8%. Gap analysis on 5 additional advertisers (Feb 17–23) found a wider range of 1.2%–33.4%. The upper bound is driven by advertisers with high cross-device rates — advertiser 36743 has 55% cross-device traffic and 50.93% cross-device mutation rate.

The redirect hop is where the user transitions from "watching the ad" to "visiting the site." If they watched the ad on their TV but visit the site on their phone (cross-device), the IP changes. Even on the same device, Wi-Fi to cellular switching, VPN toggling, or ISP-level IP rotation (CGNAT) can cause a change.

**Cross-device accounts for 61% of mutation.** Same-device network switching accounts for the remaining 39%.

---

## Part 12: The 30-Day Lookback Requirement

When we join clickpass_log to event_log, the VAST event might have happened days or weeks before the redirect. The MES pipeline allows up to 30 days between stages. So if a clickpass event is from February 10th, the corresponding VAST event could be from as far back as January 11th.

If we only look back 20 days in the event_log, we miss the older VAST events. Those missed rows show up as `el_matched = false` and inflate the apparent mutation rate (because the surviving matched rows are biased toward recent, same-session events). Using a 20-day lookback caused a +3-5 percentage point mutation offset. Extending to 30 days eliminated it entirely and matched Greenplum's results within 0.12pp.

**Always use a 30-day EL lookback.**

### Gap analysis confirmation (2026-03-02)

Verified definitively: across all 10 original advertisers (3.25M VVs, Feb 4–10), the `impression_time` to `time` gap on clickpass_log falls within 30 days for **100%** of rows:

| Gap | VVs | % |
|-----|-----|---|
| < 1 day | 1,873,498 | 57.65% |
| 1–7 days | 1,064,208 | 32.75% |
| 7–14 days | 257,865 | 7.94% |
| 14–21 days | 36,068 | 1.11% |
| 21–30 days | 18,039 | 0.56% |
| 30+ days | **0** | **0%** |

Zero VVs have an impression_time more than 30 days before the visit. The 30-day lookback is not just sufficient — it's exact. impression_time is also 100% populated (zero NULLs across 3.25M rows).

---

## Part 13: Putting It All Together — The Production Query

The audit query does this:

1. **Start with clickpass_log** — one row per verified visit in the date range
2. **LEFT JOIN event_log** (deduped to first `vast_impression` per `ad_served_id`, 30-day lookback) — gives us `bid_ip` and `vast_playback_ip`
3. **LEFT JOIN ui_visits** (filtered to `from_verified_impression = true`) — gives us `visit_ip`, `vv_is_new`, and `impression_ip`
4. **Compute flags** — compare IPs at each hop, check NTB agreement, flag mutation

The result is one row per verified visit with every IP at every step, NULL where a join didn't resolve, and boolean flags telling you exactly what happened.

```
┌─────────────────┐     ad_served_id      ┌─────────────────┐
│  clickpass_log   │─────────────────────▶ │   event_log     │
│                  │                       │                  │
│  redirect_ip ●   │                       │  ● bid_ip        │
│  cp_is_new   ●   │                       │  ● vast_ip       │
│  is_cross_dev ●  │                       └─────────────────┘
└────────┬─────────┘
         │
         │  ad_served_id (CAST)
         ▼
┌─────────────────┐
│   ui_visits      │
│                  │
│  visit_ip    ●   │
│  vv_is_new   ●   │
│  impression_ip ● │
└─────────────────┘
```

---

## Part 14: Resolved Questions — Attribution Model (Empirical, Q1-Q5)

These questions were answered empirically using BQ Silver data (advertiser 37775, Feb 4–10, n = 219,613 VVs). See `bqresults/attribution_breakdown.json` through `bqresults/inter_impression_bid_ip_mutation.json`.

### RESOLVED: Which impression does `clickpass_log.ad_served_id` point to?

**The most recent impression before the visit.** Confirmed with zero exceptions across 35,198 resolvable rows. When both `ad_served_id` and `first_touch_ad_served_id` have VAST timestamps, the `ad_served_id` impression is always more recent. Never older. This is last-touch attribution.

### RESOLVED: What is `first_touch_ad_served_id`?

**The first impression for this user/advertiser pair.** When both fields are populated and different, the first-touch VAST timestamp is always older. However, `first_touch_ad_served_id` is NULL for **40.05%** of VVs (87,956 of 219,613). This is a major gap — for nearly half of all VVs, first-touch tracing is impossible.

### RESOLVED: How often are the two fields the same?

**42.48%** of VVs have `ad_served_id = first_touch_ad_served_id`. This means the VV's most recent impression was also the first touch — single-impression attribution. But this does NOT mean the IP only had one impression. Q4 shows these IPs still had 6–30 distinct impressions in the 30-day window; they just aren't linked to the clickpass record.

### RESOLVED: Does first-touch tracing reveal additional IP information?

**Yes.** Among the 38,360 VVs where the two IDs differ (multi-impression attribution):

| bid IP comparison | Count | % |
|-------------------|-------|---|
| Same bid IP at both impressions | 30,172 | 85.72% |
| **Different bid IP** | **5,026** | **14.28%** |
| One or both NULL | 3,162 | — |

**14.28% of multi-impression VVs have different bid IPs between first touch and last touch.** This means first-touch tracing adds genuine IP lineage information that last-touch tracing alone cannot provide. However, this only applies to the 17.47% of VVs with different IDs — and only when the first-touch VAST event is resolvable.

### RESOLVED: Why is `first_touch_ad_served_id` NULL for 40% of VVs?

**Status: RESOLVED (Zach confirmed 2026-03-03).** "clickpass_log is a real time log. there is no post processing to generate it." The NULLs are permanent — populated at write time only. Zach suggested checking with Sharad: "I do not believe they do this lookup for stage 1 CTV VV."

**Disproven hypothesis: non-CTV inventory.** We tested whether NULL first_touch VVs have lower event_log match rates (which would indicate non-CTV). Result: all three groups have identical match (~99.97%). The NULL group is fully CTV with VAST events — the system just didn't populate `first_touch_ad_served_id`. (Note: advertiser 37775 is ~100% CTV, so this test should be repeated on a mixed-inventory advertiser like 31357 to fully rule out the inventory hypothesis.)

| ft_group | total | el_match_pct |
|----------|-------|--------------|
| different_id | 38,360 | 99.97% |
| ft_null | 87,956 | 99.98% |
| same_id | 93,297 | 99.96% |

**Disproven hypothesis: lookback window.** We tested whether NULL rate correlates with how recently the last impression happened before the visit (Q8):

| Impression → visit gap | Total | ft_null_pct |
|---|---|---|
| < 1 hour | 10,890 | **54.38%** |
| 1-24 hours | 85,375 | **49.43%** |
| 1-7 days | 82,573 | **37.16%** |
| 7-14 days | 24,405 | **25.45%** |
| 14-21 days | 16,299 | **17.89%** |
| 21+ days | 1 | **0.0%** |

The NULL rate is **highest for recent impressions** — the opposite of a lookback limit. ~~This pattern was originally interpreted as batch backfill~~ — **DISPROVEN.** Zach confirmed (2026-03-03): "clickpass_log is a real time log. there is no post processing." Gap analysis (2026-03-02) independently proved NULLs are permanent (identical rates 3+ weeks later). The recency correlation likely reflects that first-touch tracking data takes time to become available in the upstream systems — more time between impression and visit = higher chance the first-touch data was available when the clickpass row was written.

**Impact on the audit:** Low. The production table uses `ad_served_id` (last-touch) as the primary trace, which is always populated. `first_touch_ad_served_id` is optional enrichment — nice to have, not required.

---

## Part 15: Zach's Independent Traversal Method

Zach described a different trace method from our `ad_served_id`-based approach. His method traverses backward through the **targeting system** using IP as the join key, not the attribution fields on the clickpass record:

```
1. Start with VV's ad_served_id and ip
2. Find the bid_ip for that ad_served_id's impression
3. Search stage 1/2 campaign segments for the FIRST occurrence
   of that bid_ip within the 30-day window
4. If stage 1 → done (that's the original bid)
5. If stage 2 → find the ad_served_id for that IP's VAST event,
   get ITS bid_ip, look back another 30 days in VAST events
```

From Zach (Slack):
> "so say you wanted to go from a stage 3 vv back to the first ctv impression. it would look something like: find the ad_served_id and ip of the vv, find the bid ip of the impression for the ad_served_id, find the first occurrence of that bid ip in the stage 1 or 2 campaign for that campaign group within the 30 day window"

### How this differs from our trace

| | Our audit trace | Zach's traversal |
|---|---|---|
| **Join key** | `ad_served_id` (deterministic) | IP (fuzzy — can mutate) |
| **Starting point** | `clickpass_log.ad_served_id` | `clickpass_log.ad_served_id` + `ip` |
| **First-touch method** | Follow `first_touch_ad_served_id` pointer | Search event_log by IP within 30-day window |
| **Handles IP mutation** | No — relies on stored pointers | Yes — but "can be a bit painful because of the ip switching" |
| **Non-CTV coverage** | Only if they have VAST events | Only CTV — explicitly says "first ctv impression" |
| **Complexity** | 1-2 joins | Multi-step, potentially recursive |

### Implications for the production audit table

Our `ad_served_id` trace is correct and sufficient for the core audit (last-touch bid IP → visit IP mutation). Zach's method would be needed if we want to:
- Trace first-touch for the 40% where `first_touch_ad_served_id` is NULL
- Independently reconstruct the full impression chain without relying on stored attribution fields
- Validate that `first_touch_ad_served_id` actually points to the true first impression

We've already demonstrated the full backward traversal is technically feasible — the Q9/Q10 queries in `audit_trace_queries.sql` do exactly this for Types A through E (1 to 369 impressions). The segment system itself (from `membership_updates_proto_generate.py`) only stores `MAX(epoch)` per (ip, data_source_id, category_id) — there is no segment history to traverse. So Zach's backward trace goes through `event_log` data, not the segment store.

**Scope decision — ANSWERED (Zach, 2026-03-03): "never assume only CTV. always assume all."** The A4 production table already traces all VV types (CTV + display). CTV VVs get full 4-checkpoint tracing via event_log; non-CTV VVs get 2-checkpoint tracing via impression_ip. Zach also noted that adding clicks to Stage 3 is "one thing we could improve" — future work. The table provides one row per VV with the last-touch bid IP. The full impression chain is available via the Q9/Q10 queries if a deeper audit is ever needed.
