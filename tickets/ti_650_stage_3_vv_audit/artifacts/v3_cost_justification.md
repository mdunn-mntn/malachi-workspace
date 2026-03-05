# V3 Stage-Aware VV IP Lineage Table — Cost & Design Justification

## Why each data source is required

The table traces the full IP lineage of every verified visit (VV) across
all stages of the funnel. No existing table pre-computes these relationships.

### 1. clickpass_log (anchor — 1 day)
**This IS the verified visit.** One row per VV. Contains:
- `ad_served_id` — the UUID that links everything together
- `ip` — redirect IP (IP at the moment of redirect to advertiser's site)
- `first_touch_ad_served_id` — UUID of the very first impression this IP saw
- `is_new`, `is_cross_device` — classification flags
- `campaign_id` — which campaign's impression triggered this VV

**Why we need it:** It's the anchor. Every other join starts from here.

### 2. event_log (single 90-day scan, joined 3 times)
**VAST impression details.** Contains the bid IP and VAST playback IP that
clickpass_log does NOT have.

Joined three ways on the same materialized CTE:
- **Last-touch** (`ON ad_served_id`): the impression that got credit for this VV
- **First-touch** (`ON first_touch_ad_served_id`): the Stage 1 impression
- **Prior VV's impression** (`ON prior_vv_ad_served_id`): the impression that
  triggered the VV that put this IP into the retargeting segment

**Why we need it:** clickpass_log only has redirect_ip. To trace IP mutation
(bid → VAST → redirect → visit), we MUST join event_log to get bid_ip and
VAST playback IP. Without these, we can't identify where mutation happens
or confirm the VAST IP → targeting segment pipeline.

**Why 3 joins:** Each join resolves a different UUID. The last-touch, first-touch,
and prior VV impressions are three separate ad_served_ids pointing to three
different events in event_log.

### 3. ui_visits (±7 day window around VV date)
**Independent visit record.** Contains:
- `visit_ip` — IP when the user actually landed on the advertiser's site
- `visit_is_new` — independent NTB pixel (different from clickpass_is_new)
- `impression_ip` — bid IP carried onto the visit record (works for all inventory)

**Why we need it:** The `redirect_eq_visit` flag (redirect IP = visit IP) is how
we confirm the IP didn't change between redirect and site landing. Also gives us
the NTB disagreement rate (clickpass vs visit pixel = 41-56% disagreement).

### 4. clickpass_log (prior_vv_pool — 90-day lookback)
**Self-join to find retargeting chain.** For each VV, we search the last 90 days
of clickpass_log for any earlier VV on the same IP.

**Why we need it:** There is NO other table that records "which VV put this IP into
a retargeting segment." The only way to know is to find a prior VV whose redirect IP
matches the current VV's bid IP. This is what makes a VV "Stage 3" vs "Stage 1."

**Why it's IP-based (not UUID):** The link between stages is the IP itself — when
the VAST IP from a Stage 1 impression gets added to the Stage 2 targeting segment,
the only identifier carried forward is the IP address. There is no UUID chain
between stages.

### 5. campaigns (tiny dimension table)
**Stage classification.** `funnel_level` directly maps campaign_id → stage number.
Negligible cost. Without it, we can't classify VVs by stage.


## Why there's no simpler alternative

| Question | Answer |
|----------|--------|
| Can we skip event_log? | No — clickpass_log doesn't have bid_ip or VAST IP |
| Can we skip ui_visits? | Could, but lose visit_ip and independent NTB flag |
| Can we skip prior_vv? | No — it's the ONLY way to identify retargeting VVs |
| Can we use 1 event_log join instead of 3? | No — each resolves a different UUID |
| Can we reduce the 90-day lookback? | Risk: miss prior VVs with long retargeting gaps |
| Is there a pre-computed table? | No — this table IS the first one to compute these relationships |


## Cost analysis

BQ on-demand pricing: $6.25/TB scanned.

### Per-run cost (1 day, all advertisers)

| Component | Data scanned | Notes |
|-----------|-------------|-------|
| event_log (90-day, single scan) | ~2.5 TB | Dominant cost — VAST events are high-volume |
| clickpass_log (90-day prior_vv) | ~0.2 TB | 90 days of VVs for IP matching |
| clickpass_log (1-day anchor) | ~0.01 TB | Just the target day |
| ui_visits (±7 day) | ~0.05 TB | Small window |
| campaigns | negligible | Dimension table |
| **Total per daily run** | **~2.8 TB** | **~$17/day** |

### Backfill options (60 days)

| Strategy | Total scan | Cost | Time |
|----------|-----------|------|------|
| Naive: 60 separate daily queries | ~166 TB | ~$1,039 | 60 × 10 min = 10 hours |
| **Batch: 1 query, 60-day cp range** | **~4.6 TB** | **~$29** | ~15-20 min |
| Savings | | **97%** | **97%** |

Batch works because: the lookback CTEs (event_log, prior_vv) only need to
extend to cover the oldest day's lookback. One 150-day event_log scan covers
all 60 days of VVs, vs. re-scanning 90 days × 60 times.

### Ongoing monthly cost

| Scenario | Cost/month |
|----------|-----------|
| Daily run (30 days × $17) | ~$520 |
| With flat-rate reservation (if available) | Fixed slot cost |

Note: SQLMesh may use slot-based pricing rather than on-demand, which would
change the cost model entirely. Check with data platform team.


## Optimizations applied

### 1. Single event_log CTE (saves ~8%)
Original: 3 separate event_log scans (30-day + 60-day + 90-day = 180 days total).
Optimized: 1 scan at 90 days, joined 3 times by different ad_served_id.
BQ materializes the CTE and reuses it.

### 2. Batch backfill (saves 97%)
Instead of 60 daily queries, run 1 query with:
- cp_dedup: 60-day date range
- el_all: 150-day lookback (covers oldest VV's prior VV's impression)
- prior_vv_pool: 150-day lookback
- v_dedup: cp_start - 7 to cp_end + 7

### 3. Future: self-referencing prior_vv (after table exists)
Once the table is populated, daily incremental runs could look up prior VVs
from the materialized table itself instead of re-scanning clickpass_log.
This would reduce the daily scan from ~2.8 TB to ~0.5 TB (event_log only).

### 4. Future: partition the event_log scan by advertiser
If event_log is clustered by advertiser_id, adding an advertiser filter
could significantly reduce bytes scanned. Currently not used because the
UUID join handles filtering naturally, but worth testing.


## Summary for data platform team

**What:** Row-level VV IP lineage table. One row per verified visit across all
advertisers, all stages. Traces IP through bid → VAST → redirect → visit,
links first-touch and prior VV, classifies by funnel stage.

**Backfill:** 1 batch query, ~4.6 TB, ~$29, ~20 minutes.

**Daily incremental:** ~2.8 TB/day, ~$17/day, ~$520/month on-demand.

**Pipeline:** SQLMesh job (per Zach). 90-day rolling retention.

**Table location:** TBD — need to confirm with data platform team.
