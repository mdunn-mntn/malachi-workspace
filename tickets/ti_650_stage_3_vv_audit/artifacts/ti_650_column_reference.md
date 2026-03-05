# vv_ip_lineage — Column Reference

**One row per verified visit.** Traces the IP address through every stage of the funnel:
bid (auction) -> VAST (ad playback) -> redirect (clickpass) -> visit (site landing).

Links each VV to its first-touch impression (Stage 1) and most recent prior VV (retargeting chain).

---

## Identity

| Column | Type | Description |
|--------|------|-------------|
| `ad_served_id` | STRING | **Primary key.** UUID from clickpass_log — one per verified visit. |
| `advertiser_id` | INT64 | MNTN advertiser ID. |
| `campaign_id` | INT64 | Campaign that got credit for this VV (last-touch attribution). |
| `vv_stage` | INT64 | **Attribution stage.** From `campaigns.funnel_level`: 1=Prospecting, 2=Retargeting, 3=Re-Retargeting. This is which campaign's budget paid for the impression — NOT how deep the IP is in the funnel. |
| `max_historical_stage` | INT64 | **Journey stage.** Deepest stage this IP has reached in the 90-day window. `GREATEST(vv_stage, max prior VV stage)`. A VV can be `vv_stage=1` but `max_historical_stage=3` — meaning S1 campaign got credit, but the IP was already retargeted through S3. |
| `vv_time` | TIMESTAMP | When the verified visit happened. |

## Last-Touch IP Lineage (this VV's impression)

These columns trace the IP at each checkpoint for the impression that got credit for this VV.

| Column | Type | Description |
|--------|------|-------------|
| `lt_bid_ip` | STRING | IP at bid time (auction). From `event_log.bid_ip`. |
| `lt_vast_ip` | STRING | IP during VAST ad playback. From `event_log.ip`. |
| `redirect_ip` | STRING | IP at redirect (clickpass). From `clickpass_log.ip`. **This is the IP used for retargeting segment membership.** |
| `visit_ip` | STRING | IP when user landed on advertiser's site. From `ui_visits.ip`. |
| `impression_ip` | STRING | Bid IP carried onto the visit record. From `ui_visits.impression_ip`. |

**The mutation point** is between `lt_vast_ip` and `redirect_ip` (5.9-20.8% of VVs). All other transitions are ~99.9%+ stable.

## First-Touch Attribution (Stage 1 impression)

The very first impression this IP ever saw (from `clickpass_log.first_touch_ad_served_id`).

| Column | Type | Description |
|--------|------|-------------|
| `ft_ad_served_id` | STRING | UUID of the first-touch impression. |
| `ft_campaign_id` | INT64 | Campaign of the first-touch impression. |
| `ft_stage` | INT64 | Stage of the first-touch campaign. Should typically = 1. |
| `ft_bid_ip` | STRING | Bid IP of the first-touch impression. |
| `ft_vast_ip` | STRING | VAST IP of the first-touch impression. |
| `ft_time` | TIMESTAMP | When the first-touch impression happened. |

## Prior VV (retargeting chain)

The most recent earlier VV on the same IP. This is how we link stages — the prior VV's redirect IP put this IP into a retargeting segment.

| Column | Type | Description |
|--------|------|-------------|
| `prior_vv_ad_served_id` | STRING | UUID of the prior VV. NULL if no prior VV found. |
| `prior_vv_time` | TIMESTAMP | When the prior VV happened. |
| `pv_campaign_id` | INT64 | Campaign of the prior VV. |
| `pv_stage` | INT64 | Stage of the prior VV's campaign. |
| `pv_redirect_ip` | STRING | Prior VV's redirect IP (clickpass.ip). |
| `is_retargeting_vv` | BOOL | TRUE if a prior VV was found = this IP was retargeted. |

## Prior VV's Impression IPs

The impression that got credit for the PRIOR VV. Lets you trace the full chain back one more step.

| Column | Type | Description |
|--------|------|-------------|
| `pv_lt_bid_ip` | STRING | Bid IP of the prior VV's attributed impression. |
| `pv_lt_vast_ip` | STRING | VAST IP of the prior VV's attributed impression. |
| `pv_lt_time` | TIMESTAMP | When the prior VV's attributed impression happened. |

## IP Comparison Flags

Pre-computed boolean flags for common analysis patterns. All are CTV-only (NULL when `is_ctv = FALSE`).

| Column | Type | Description |
|--------|------|-------------|
| `bid_eq_vast` | BOOL | `lt_bid_ip = lt_vast_ip` — IP stable between bid and VAST playback? (~96.5%) |
| `vast_eq_redirect` | BOOL | `lt_vast_ip = redirect_ip` — **THE MUTATION POINT.** IP stable between VAST and redirect? (79-94%) |
| `redirect_eq_visit` | BOOL | `redirect_ip = visit_ip` — IP stable between redirect and site visit? (99.98%+) |
| `ip_mutated` | BOOL | `bid=vast AND vast!=redirect` — clean mutation at redirect boundary. |
| `any_mutation` | BOOL | `lt_bid_ip != redirect_ip` — any IP change from bid to redirect. |
| `lt_bid_eq_ft_bid` | BOOL | Same bid IP for first-touch and last-touch impressions? |

## Classification

| Column | Type | Description |
|--------|------|-------------|
| `clickpass_is_new` | BOOL | NTB (new-to-brand) per clickpass pixel. **Client-side JavaScript, not auditable via SQL.** |
| `visit_is_new` | BOOL | NTB per independent visit pixel. |
| `ntb_agree` | BOOL | Both NTB sources agree? 41-56% disagreement is expected and real. |
| `is_cross_device` | BOOL | Ad shown on one device, visit on another (e.g., CTV ad, laptop visit). |

## Trace Quality

These flags tell you how complete the trace is for each VV.

| Column | Type | Description |
|--------|------|-------------|
| `is_ctv` | BOOL | event_log join succeeded = CTV inventory. FALSE = display/non-CTV (lt_ columns will be NULL). |
| `visit_matched` | BOOL | ui_visits join succeeded. |
| `ft_matched` | BOOL | First-touch impression found in event_log. NULL if no ft_ad_served_id. |
| `pv_lt_matched` | BOOL | Prior VV's impression found in event_log. NULL if no prior VV. |

## Metadata

| Column | Type | Description |
|--------|------|-------------|
| `trace_date` | DATE | **Partition key.** Date of the verified visit. |
| `trace_run_timestamp` | TIMESTAMP | When this row was computed. |

---

## Key Insight: Attribution Stage vs Journey Stage

Because IPs stay in all retargeting segments and Stage 1 has 75-80% of budget, **20% of S1-attributed VVs are on IPs that have already reached Stage 3.** The `vv_stage` tells you which campaign got credit; `max_historical_stage` tells you how deep the IP actually is in the funnel.

## Known Limitations

- **Prior VV match uses redirect_ip = bid_ip** (~94% accurate). The targeting system actually uses VAST IP, but getting VAST IP requires pre-joining event_log.
- **Non-CTV VVs**: `lt_` columns will be NULL (display uses impression_log, not event_log).
- **is_new / visit_is_new**: Client-side JavaScript pixel determination — not auditable via SQL.
