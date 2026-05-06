# MNTN Select Lift — Initial Read

**Power Line:** _placeholder until lift lands — e.g. "Select drives X visit-rate lift, but only when pooled."_

**Audience:** Kale McNaney (Director, Identity & Attribution); Hannah Burke (Sr. Director, Product MNTN Select).
**Window:** 2026-04-22 → 2026-05-05 (14 days).
**Cohort:** 38 active Select advertisers (`campaign_groups.product_id = 2`).

---

## 1. The Question

> "Did we look at MNTN Select campaigns separately? Select is geo + show targeting only — no audience scoring. Could be more incremental."

We tested this on every active Select advertiser we have augmentor coverage for.

---

## 2. The Cohort — Volume Sets the Ceiling

[chart: ti_933_chart_volume_by_advertiser.png]

**Top 5 advertisers by impression volume (last 30 days):**

| Rank | Advertiser | Impressions | Monthly equiv. spend |
|-----:|------------|------------:|---------------------:|
| 1 | Masterbuilt | 6.3M | $106k |
| 2 | Hugo Insurance | 4.7M | $81k |
| 3 | Extra Space Storage | 2.4M | $38k |
| 4 | Goldfish Swim School | 1.8M | $28k |
| 5 | NinjaOne | 1.4M | $22k |

**38 advertisers total, all prospecting (zero retargeting in Select).** Largest is $106k/mo. The TI-917 visit-rate MDE floor was **$200k/mo** — meaning **no single Select advertiser has stat power on its own**. Pooling is the only path to a defensible number.

---

## 3. Per-Advertiser Power — None Individually Powered

[chart: ti_933_chart_per_advertiser_power.png]

Each dot is one advertiser. 95% CIs swamp every individual estimate. **Zero advertisers** have a CI that excludes zero.

Implication for execs: if a Select customer asks "what's MY lift?" — we can't answer that question with this volume. We can only answer "what's the lift across the Select line of business as a whole?"

---

## 4. The Pooled Number

[chart: ti_933_chart_pooled_lift.png]

Pooled across all 38 Select advertisers, 14-day window:

- **Visit-rate lift (guid):** _LIFT_PP_ pp, 95% CI [_CI_LO_, _CI_HI_]
- **Visit-rate lift (clickpass):** _CP_LIFT_ pp (caveat: clickpass requires an MNTN impression, so holdout cannot register a click — biases toward inflated lift; guid is the honest measure)
- **Conversion-rate lift:** _CONV_LIFT_ pp, 95% CI [_CONV_LO_, _CONV_HI_]

**Comparison to TI-917 product-wide segments:**

| Segment | Visit-rate lift | Notes |
|--------|----------------:|-------|
| All campaigns | +3.12 pp | TI-917 baseline |
| Prospecting | +0.78 pp | TI-917 |
| Stage 1 | −0.06 pp | TI-917, ~zero |
| Retargeting | +21.07 pp | TI-917, dominant |
| **MNTN Select (this analysis)** | **_SELECT_LIFT_** | Pooled, 14d |

---

## 5. Methodology — How We Filtered to Select

**Source of truth:** `campaign_groups.product_id = 2` in coredb (Postgres → BigQuery via Datastream). Values: `1=PTV`, `2=Select`, `3=QuickFrame`. Confirmed 2026-05-05 — agrees across all four BQ table variants (`campaign_groups` / `..._raw` / `public_...` / `public_..._raw`).

**Cohort construction:**
1. `campaign_groups WHERE product_id = 2 AND deleted = FALSE AND is_test = FALSE` → 260 active Select groups.
2. `campaigns INNER JOIN` above → all Select campaigns.
3. Filter to advertisers with cost_impression_log activity in last 30 days → **38 advertisers**.
4. Cross-checked against Kale's "Select Live Campaigns" list (58 AIDs total) — all 38 in our cohort are in his list. The 20 in his list that we don't include either have zero recent activity or fall outside augmentor's ~14-day TTL.

**Lift method (identical to TI-917):**
- 10% biddable holdout per (advertiser_id, IP) hash: `MD5(AID || ":" || IP) mod 1000`, bucket 0–99 = holdout, 100–999 = targeted.
- "Biddable" = appeared in `augmentor_log` during the window (validated: 99.99% of Select-served IPs appear in augmentor — the filter applies cleanly).
- Holdout subsampled to per-advertiser empirical win-rate so denominators match treated arm.
- Visit-rate = `COUNT_DISTINCT(visiting IPs) / COUNT_DISTINCT(arm IPs)`, where visits come from `guid_log` (independent identity graph) within +3 days of the impression window.

---

## 6. What This Means

- **For brand-direct Select customers asking for proof:** we can show pooled Select lift with a defensible CI. We cannot promise per-advertiser numbers at current volumes.
- **For the Incrementality Overhaul (BER-2250):** Select fits the same biddable-holdout framework. No new infrastructure needed.
- **To unlock per-advertiser readouts:** ghost-bidding (TI-886) is the path — it removes the augmentor TTL ceiling and gives us multi-month windows.

---

### Appendix — Data files

- Volume per advertiser: `outputs/ti_933_select_volume_by_advertiser.csv`
- Cohort cross-check vs Kale's xlsx: `outputs/ti_933_xlsx_vs_our_cohort.csv`
- Per-advertiser lift table: `outputs/ti_933_per_advertiser_lift.csv`
- Pooled lift JSON: `outputs/ti_933_select_lift_pooled_14d.json`
- Queries: `queries/ti_933_select_volume_by_advertiser.sql`, `queries/ti_933_augmentor_select_intersection.sql`, `queries/ti_933_select_lift_pooled_14d.sql`
