# MNTN Select — Is It Incremental, and What Do We Do?

**Power Line (one-of-three, fills based on result):**
1. _"Select is incremental. Lean into awareness-only flights — projected $X revenue uplift from a 20% spend reallocation."_
2. _"Select isn't pulling weight today. Three easy-mode levers can change that. Pick one to test in Q3."_
3. _"Volume isn't there yet. Ghost-bidder (TI-886) unlocks the answer in 90 days. Here's the ROI of greenlighting it."_

**Audience:** Kale McNaney (Director, Identity & Attribution); Hannah Burke (Sr. Director, Product MNTN Select). PM context: Bryce Wagg.
**Window:** 7d (2026-04-29 → 2026-05-05) primary, 14d (2026-04-22 → 2026-05-05) for stat-power robustness.
**Cohort:** 38 active Select advertisers (`campaign_groups.product_id = 2`).

---

## 1. The Question

> Kale, post-TI-917 review: "Did we look at Select campaigns separately? Select is geo + show targeting only — no audience scoring. Could be more incremental."

**The real question for execs is not "what's the lift?" — it's "what should we do about Select?"** This deck answers both.

---

## 2. The Cohort — What's In Play

[chart: ti_933_chart_volume_by_advertiser.png]

**38 active Select advertisers** in the last 30 days. All prospecting, zero retargeting. Largest is Masterbuilt at $106k/mo — none clear TI-917's $200k/mo MDE floor for individual stat power.

Cross-checked against Hannah's 58-AID Live Campaigns list: **all 38 of our advertisers are in her list**; the 20 not in our cohort have either zero recent impressions or fall outside the augmentor 10-day TTL (e.g., LifeMD's 5M impressions are from older flights we can't query).

**Implication:** the only path to a defensible number is pooling. Per-advertiser readouts wait for ghost-bidder.

---

## 3. The Pooled Number

[chart: ti_933_chart_pooled_lift.png]

| Metric | 7d window | 14d window | TI-917 baselines for context |
|--------|----------:|-----------:|------|
| Visit-rate lift (guid) | _LIFT_7D_ pp [_CI_7D_] | _LIFT_14D_ pp [_CI_14D_] | all +3pp · prosp +0.78pp · rtg +21pp |
| Conversion-rate lift | _CONV_7D_ pp | _CONV_14D_ pp | (revenue not powered in TI-917 either) |

**Read:** _populates after queries land. Three branches:_
- **If positive and tight CI:** "Select is incremental at scale, even without intent scoring. The geo+show framing works."
- **If ≈0 with tight CI:** "Select is not adding incremental visits beyond what those IPs would have done anyway. Targeting is leaving lift on the table."
- **If wide CI spanning zero:** "Volume can't tell us. We need more days, more advertisers, or ghost-bidder."

---

## 4. Per-Advertiser Power — Why Pooling Was Necessary

[chart: ti_933_chart_per_advertiser_power.png]

Each dot = one Select advertiser. 95% CIs swamp every individual estimate. **Zero advertisers** clear stat-power on their own. The largest single Select advertiser (~$106k/mo) is half the spend needed for individual readouts. Customers asking "what's MY lift?" — we cannot answer with current volume.

---

## 5. What This Means — Action Slide (the Bryce slide)

**If Select shows incrementality (Result 1):**
- Reallocate $X of low-incremental retargeting spend to Select awareness flights → projected $Y annual revenue uplift.
- Pitch to Hannah: "Awareness-only Select is doing real work — we can prove it on the line of business."

**If Select shows ≈0 incrementality (Result 2):** _three easy-mode levers, each with bounded expected lift_
- **Add intent layer to Select.** Currently: geo + show only. Adding `prospecting_intent` scores (already in BQ) on top of show-targeting could yield +1-3pp visit lift, similar to PTV prospecting. Engineering cost: weeks.
- **Tighten geo precision.** Current Select geo is DMA-level. Moving to ZIP/ZIP+4 with audience-overlay constraints could cut wasted impressions 15-30%.
- **Show optimization based on response data.** Use clickpass/visit response per show to dynamically reweight. Currently flat across the show universe.

**If volume can't carry the answer (Result 3):**
- **Greenlight ghost-bidder (TI-886).** It removes the 10-day augmentor TTL, gives 90-day windows, and enables per-advertiser readouts. Without it, no Select customer ever gets a personalized incrementality number.
- Estimated cost: _<engineering effort from Jack/Ryan>_. Estimated unlock: per-advertiser lift readouts for the entire MNTN line of business, not just Select.

---

## 6. Methodology — How We Filtered to Select

**Source of truth:** `campaign_groups.product_id = 2` in coredb (Postgres → BigQuery via Datastream). `1=PTV`, `2=Select`, `3=QuickFrame`. All four BQ table variants agree (verified 2026-05-05).

**Lift method (identical to TI-917):**
- 10% biddable holdout per (advertiser_id, IP) hash: `MD5(AID || ":" || IP) mod 1000`, bucket 0–99 = holdout, 100–999 = targeted. Hash is product-agnostic — applies to Select unchanged.
- "Biddable" = appeared in `augmentor_log` during the window. Validated: **99.99%** of Select-served IPs appear in augmentor on 2026-05-04 — the filter applies cleanly.
- Holdout subsampled to per-advertiser empirical win-rate so denominators match the treated arm.
- Visit-rate = `COUNT_DISTINCT(visiting IPs) / COUNT_DISTINCT(arm IPs)`. Visits from `guid_log` (independent identity graph, no clickpass survivorship bias) within +3 days of impression window.

**Caveats for execs:**
- _Geo confound:_ holdout IPs are not restricted to in-DMA. Treated IPs are (Select bids only fire in target DMAs). Bias is toward zero — Select's true lift is at least as large as what we measure here.
- _Awareness conversion sparsity:_ Select is awareness-only. Conversion-rate readout will have wider CIs than visit-rate.

---

### Appendix — Files

- Volume per advertiser: `outputs/ti_933_select_volume_by_advertiser.csv`
- Cohort cross-check vs Hannah's xlsx: `outputs/ti_933_xlsx_vs_our_cohort.csv`
- Per-advertiser lift table: `outputs/ti_933_per_advertiser_lift.csv`
- Pooled lift JSON (7d): `outputs/ti_933_select_lift_pooled_7d.json`
- Pooled lift JSON (14d): `outputs/ti_933_select_lift_pooled_14d.json`
- Queries: `queries/ti_933_*.sql`
