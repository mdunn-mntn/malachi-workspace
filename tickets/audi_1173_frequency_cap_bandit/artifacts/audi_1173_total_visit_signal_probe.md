# AUDI-1173 — Attribution-independent total-visit signal probe

**Verdict:** Yes — attribution-independent **total advertiser site visits per household** is measurable in BQ today at the grain the RCT needs, and it makes the suppression-holdout arm (arm H) measurable. The single best source is **`dw-main-silver.logdata.guid_log`**, joined to the arm on **household = `(advertiser_id, ip)`** (CIDR-stripped) over `[first_bid_time, +window)`. Even simpler: **reuse the production `dw-main-silver.enriched.lift__ghost_bid_visits`** — it already embeds exactly this guid_log join and emits a binary `visited` per arm IP. Biggest caveat: cross-device / IP-mismatch coverage (~85–90% of attributable visits), and guid_log has **no campaign_id** (household grain is advertiser×ip, not campaign-exact).

Date: 2026-07-28. Every claim below is tied to a schema check or a COUNT. Total bytes billed across this probe ≈ **15.0 GB** (one 15.0 GB scan of the lift table; the rest were <0.02 GB dim reads).

---

## 1. The total-visit signal: `logdata.guid_log`

`guid_log` is MNTN's own site pixel — **one row per page view** on an advertiser site by a tracked household, fires **regardless of whether MNTN ever served an ad** (Zach Schoenberger, data_catalog L327). This is the attribution-free "total site traffic" signal; `clickpass_log`/`summarydata.ui_visits` are MNTN-**attributed** (impression-anchored) and are the wrong signal for a holdout.

- **Schema (`bq show --schema`):** `guid, epoch, time, ip, ip_raw, original_ip, is_new, advertiser_id, is_control_group, is_cookied, referer, parent_referer, is_mobile_device, browser, operating_system, …`
- **Grain:** page-view event. **IP-keyed** (`ip`, `ip_raw`, `original_ip`) **and advertiser-keyed** (`advertiser_id`). **No `campaign_id`** → the finest visit key is `(advertiser_id, ip)`.
- **Visit column:** there is no boolean — a **row existing** for `(advertiser_id, ip)` in the window = a visit. The derived binary `visited` is built downstream (see §2).
- **Cost / TTL:** VIEW → `history.guid_log_physical` (107 B rows / 366 TB, DAY-partitioned on `time`, ~90d durable). **Never full-scan in BQ** — always advertiser + `time` filtered, or read the GCS archive on Databricks. This probe did NOT raw-scan guid_log; it read the pre-computed lift table that embeds the join.

**Candidates evaluated:**
| Candidate | Total or attributed? | Holdout-defined? | Verdict |
|---|---|---|---|
| `logdata.guid_log` | **TOTAL** (all pixel page views) | **Yes** | **Recommended source** |
| `enriched.lift__ghost_bid_visits` | TOTAL (built on guid_log) | Yes | **Recommended — reuse directly** |
| `logdata.clickpass_log` / `summarydata.ui_visits` | MNTN-**attributed** (impression-anchored) | **No** (holdout has no impression → 0) | Do NOT use as RCT outcome |
| `reporting.lift__ghost_bid_*` | rollups of the above guid_log outcome | Yes (aggregate) | Analysis layer, not row source |

---

## 2. The INCR ghost-bid "visited" is a TOTAL signal (holdout-measurable) — confirmed

**Source-of-truth read** — the production model `sqlmesh/models/dw-main-silver/enriched/lift__ghost_bid_visits.sql` uses **guid_log**, explicitly NOT VV:

- L52: source `dw-main-silver.logdata.guid_log (all pixel visits)`.
- L67–72 (verbatim intent): *"WHY guid_log AND NOT summarydata.visits (Verified Visits) — DO NOT 'upgrade' this to VV: summarydata.visits is attribution OUTPUT, impression-anchored… A holdout IP has no impression, so it can NEVER carry a verified visit for the held campaign → swapping VV in here zeroes the holdout arm → manufactured lift. **guid_log is the only attribution-free, arm-symmetric visit source.**"*
- `visited` = `NOT wv.ip IS NULL` where `window_visits` = INNER JOIN of arm IPs to guid_log on `v.advertiser_id = m.advertiser_id AND v.ip = m.ip AND v.time ∈ [first_bid_time, +7d)`. Household join key = **(advertiser_id, ip)**, ip CIDR-stripped `split(g.ip,'/')[OFFSET(0)]`.

> Note the older **exploratory** `ti_1044_ghost_lift.sql` used `clickpass_log` (the TI-837 method) — that is NOT the production signal. The production `enriched.lift__ghost_bid_visits` was upgraded to guid_log precisely to make the holdout arm measurable. Don't cite the ti_1044 query as the current definition.

**Empirical confirmation — the ghost (holdout) arm has nonzero total visits** (`lift__ghost_bid_visits`, dt 2026-07-10..07-20, 15.0 GB scan):

| arm | IPs | visitors | **visit_rate** | wins | **won_rate** | conv_rate |
|---|---|---|---|---|---|---|
| **ghost** (holdout) | 62,863,354 | 556,905 | **0.886%** | 3 | **0.0%** | 0.0386% |
| submitted (treated) | 680,991,839 | 10,574,354 | **1.553%** | 329,400,303 | 48.37% | 0.0646% |

The holdout arm won **3 impressions out of 62.9M IPs (0.0% — clean suppression)** yet still shows a **0.886% total-visit rate**. An attributed-VV signal would show ≈0 here. This is the direct proof that guid_log gives a counterfactual visit rate for never-served households → **arm H is measurable.**

---

## 3. IP-keying, the join, and the cross-device caveat

- **Join to the RCT arm:** household `(advertiser_id, ip)`; ip on both sides CIDR-stripped to the plain bid-log IP. Visit qualifies if a guid_log row lands in `[first_bid_time, +window)`.
- **Which IP:** guid_log carries the **visit-device IP** (where the page view happened); the arm/bid carries the **bid IP**. For same-device display these coincide.
- **CTV cross-device caveat (the load-bearing limitation):** ad served to a CTV IP, site visit on a phone/laptop on a different IP → the `(advertiser_id, ip)` join **misses** that visit. The model's bias register B7 validated guid_log's IP-exact join at **~85–90% coverage of attributable verified visits**; structural miss is **ip_mismatch 4–10%** (NOT the 24–99% is_cross_device flag, which overstates it). The cross-device gap is **plausibly arm-symmetric** (independent of the holdout hash) → it **cancels in the absolute T−C percentage-point lift**, though it depresses both arms' absolute rates. Design the RCT to report **absolute pp lift**, not relative, so the coverage gap cancels.
- **No campaign_id in guid_log** → visits attach to advertiser×ip, not campaign×ip. For a per-campaign_group cap RCT this is fine (household is the unit); just don't expect campaign-exact visit attribution.

---

## 4. Base rate for power

The **holdout (ghost) arm's 7-day total-visit rate = 0.886%** IS the attribution-independent per-household base rate for the RCT (the counterfactual). Contrast:

- **Total site visits / household (7d, holdout):** **~0.89%** — measurable for never-served households.
- **Total site visits / household (7d, treated/bid-on):** ~1.55%.
- **MNTN-attributed VV rate:** ~1.0% per the ticket premise — but this is **structurally ≈0 for a holdout**, so it cannot anchor an arm-H power calc at all.

Implications for the RCT power re-derivation:
- Base rate p ≈ **0.9% at a 7-day window**; a **30-day window raises it** (more time to visit — the RCT should use ≥30d for a higher base rate + tighter power, consistent with the "lookback ≥ 2–3× post-period" rule).
- The relative effect to detect is **smaller** than the attributed-VV lift (attributed VV shows 2–8× because attribution itself is frequency-confounded; total visits move far less — that is the whole point of switching the outcome).
- Higher base rate + honest (smaller) effect → re-run the power calc on p≈0.9–1.5%; the existing ~636K/arm sizing should be re-derived against the true total-visit MDE, not the inflated attributed-VV lift.

---

## 5. Pixel-coverage constraint

The RCT must restrict to **site-wide-pixel** advertisers (pixel fires on all pages), because a conversion-page-only pixel makes guid_log ≈ conversion fires (no general traffic to measure).

**Dim flags exist** on `dw-main-bronze.integrationprod.advertisers` (schema check): `tracking_pixel_status_id` (site-wide tracking pixel), `conversion_pixel_status_id` (conversion pixel), `pixel_id` / `pixel_id_conversion`, `pixel_isolation`, `conv_pixel_opt_out`.

**Empirical distribution** (`deleted=FALSE AND is_test=FALSE`, n=36,172 active advertisers):
- `tracking_pixel_status_id`: **9 → 31,985** (live/dominant state), 10 → 4,172, 11 → 15.
- `tracking_pixel_status_id != conversion_pixel_status_id`: **only 1 advertiser** → the two pixels' status move together; the dim confirms the pixel is **LIVE**, not its **page-level scope**.
- `pixel_isolation = TRUE`: **0** among active advertisers (rare/legacy; not a usable filter here). Note: elsewhere `pixel_isolation=true` advertisers are left-anti-joined OUT of the guid_log DS23 feed — so where set, it IS a hard exclusion, just empty today.
- `conv_pixel_opt_out = TRUE`: 154.

**Consequence:** the dim gives "pixel live" but **not** "site-wide vs conversion-only." Site-wide coverage must be **validated empirically per advertiser from guid_log** (page-view volume ≫ conversion count; distinct `referer`/`parent_referer` spanning many URL paths, not just a `/checkout` path). Cheapest operationalization: **restrict the RCT to the advertiser universe already in `lift__ghost_bid_audiences`** — those advertisers are exactly the ones the ghost-bid pipeline validated at ~85–90% guid_log coverage (§3), i.e., they already have working site-wide pixels. Require `tracking_pixel_status_id = 9`, `pixel_isolation = FALSE`, and a guid_log-pageviews ≫ conversions sanity check per advertiser.

---

## 6. Verdict

**Measurable today: yes.** Attribution-independent total site visits per household is a live BQ signal at the exact grain the RCT needs, and it is what makes arm H (suppression holdout) measurable.

- **Best source table:** `dw-main-silver.logdata.guid_log` — or, better, **reuse `dw-main-silver.enriched.lift__ghost_bid_visits`**, which already computes the guid_log join and emits a self-consistent binary `visited` (+ `won` compliance, `converted`) per arm IP. The frequency-cap RCT can mint arms via `lift__ghost_bid_audiences`-style membership and read `visited` as the reward — no new pipeline.
- **Join key:** household `(advertiser_id, ip)`, ip CIDR-stripped, visit in `[first_bid_time, +window)`.
- **Holdout works:** ghost arm empirically 0.886% total-visit rate at 0.0% won-rate (62.9M IPs) — nonzero visits without exposure.
- **Base rate:** ~0.9% (7d) → higher at 30d; re-derive power against this, not the attributed-VV lift.
- **Biggest caveat:** cross-device / IP-mismatch coverage (~85–90%; report **absolute pp lift** so it cancels). Secondary: guid_log has no campaign_id (household = advertiser×ip), and the RCT must restrict to site-wide-pixel advertisers (validate empirically; `tracking_pixel_status_id=9` + `pixel_isolation=FALSE` + guid_log pageviews ≫ conversions; reuse the ghost-bid advertiser universe).

---

### Provenance
- Schema checks: `guid_log`, `enriched.lift__ghost_bid_visits`, `integrationprod.advertisers`, `ui_advertiser_pixel_infos` (`bq show --schema`).
- Model source: `~/Developer/work/mntn/sqlmesh/models/dw-main-silver/enriched/lift__ghost_bid_visits.sql` (L52, L67–76, `window_visits` join).
- COUNTs: arm visit/won/conv rates (15.0 GB, dt 2026-07-10..20); pixel-status + isolation distribution (2 dim reads, <0.02 GB).
- Contrast doc: `knowledge/bq/summarydata/ui_visits.md` (attributed VV = impression-anchored UNION, holdout-undefined).
