# AUDI-1173 — Frequency-cap leakage (cross-group + cross-stage) and the fix

**Bottom line:** MNTN's frequency counters have no advertiser rollup, so a household is capped separately by each campaign_group and each funnel stage it appears in. In one 7-day window, **6.9% of households (20.3% of impressions, 17.1% of spend) are served by 2+ campaign_groups of the same advertiser, and 13.5% of households (34.8% of impressions, 29.5% of spend) are served across 2+ funnel stages**; **12.5% of households (27.4% of spend) are hit in BOTH prospecting and retargeting inside the same week**, each stage counting the IP independently. Estimated leakage-attributable over-delivery is **$0.41M–$0.66M per 7 days (5.5%–8.9% of analyzed spend)**. **The fix is a single advertiser-level rollup counter (`rtb:frequency:{ip}:advertiser=<aid>`) on the DEFAULT cap only — a control-plane correctness fix that ships WITHOUT the RCT.**

All numbers: `queries/audi_1173_leakage_cross_group_stage.sql` → `outputs/audi_1173_leakage.json`. Window 2026-07-06..07-12. Household = `(ip, advertiser_id)`. Cohort = all live campaigns (`public_campaigns` `deleted=FALSE AND is_test=FALSE`) joined to `cost_impression_log` on `campaign_id`; `campaign_group_id` + `funnel_level` come from the DIM join, never `model_params`. WGU (31357) and AID 90 (PSA) excluded. Spend = `media_spend+data_spend+platform_spend`. Bytes billed: **41.49 GB**.

---

## The defect (from scope §3)

Counters live in Redis keyed `rtb:frequency:{ip}:campaign_group_id=<cg>:campaign_id=<c>` — per campaign and per campaign_group, on IPv4. **There is no advertiser dimension and no rollup key.** Consequence: one household can be hit up to N times *per group* across an advertiser's groups, and up to N times *per stage* across its funnel — delivered frequency > configured frequency by construction, with no cross-group or cross-stage bound.

---

## 1. Cross-GROUP leakage (refines Phase-0 §4c, which was prospecting-only 4.8%/13%)

Over ALL stages/advertisers, leakage is larger than the prospecting-only slice:

| groups / household | households | hh % | imp % | spend % | avg imps/hh |
|---|---:|---:|---:|---:|---:|
| 1 | 117.4M | 93.07 | 79.71 | 82.94 | 2.20 |
| 2 | 7.10M | 5.63 | 14.18 | 11.52 | 6.48 |
| 3 | 1.17M | 0.93 | 3.65 | 3.11 | 10.09 |
| 4–5 | 0.44M | 0.35 | 2.12 | 2.04 | 15.70 |
| 6+ | 0.04M | 0.03 | 0.35 | 0.39 | 28.65 |
| **2+ (leaked)** | **8.75M** | **6.93** | **20.29** | **17.06** | — |

**6.9% of households take 20.3% of impressions and 17.1% of spend across 2+ groups.** avg imps/hh rises monotonically with group count (2.2 → 28.6) — the multi-group households are exactly the over-frequency tail.

## 2. Cross-STAGE leakage (new)

Distinct `funnel_level` per household (S1 prospecting / S2/S3/S4 engaged/retargeting):

| stages / household | households | hh % | imp % | spend % | avg imps/hh |
|---|---:|---:|---:|---:|---:|
| 1 | 109.1M | 86.50 | 65.16 | 70.52 | 1.94 |
| 2 | 16.3M | 12.94 | 30.71 | 26.42 | 6.10 |
| 3 | 0.70M | 0.56 | 4.13 | 3.06 | 19.02 |
| **2+ (leaked)** | **17.03M** | **13.50** | **34.84** | **29.48** | — |

**Cross-stage leakage (13.5% of hh) is ~2x cross-group leakage (6.9%).** Because a single campaign_group holds S1/S2/S3 campaigns and each stage is a separate campaign with its own per-campaign counter, cross-stage over-serving happens even *inside one group* — not only across groups.

**Prospecting ∩ retargeting (the sharpest cut):** **12.5% of households (15.8M; 27.4% of spend, $2.04M/7d) are served in BOTH S1 prospecting AND S2+ retargeting within the week.** These are the households the DSP both prospects and re-touches simultaneously, with the two stages' caps blind to each other.

## 3. Estimated over-delivery (leaked households only)

"Excess" = impressions an advertiser-level rollup counter would have suppressed. Two methods, stated explicitly. **This is estimated over-delivery, not savings** — see the redirect caveat.

**Method A — cap-agnostic (leakage-pure).** Roll every counter up to the household's heaviest single counter and hold that counter's delivery fixed; excess = `total_imps − heaviest_counter`. Assumes no cap value; isolates the impressions that exist *only* because counters don't roll up.
- Cross-group: 8.75M leaked hh → 25.0M excess imps → **$0.51M / 7d**
- Cross-stage: 17.0M leaked hh → 33.4M excess imps → **$0.66M / 7d**

**Method B — explicit advertiser cap of N/wk** on leaked households (2+ groups OR 2+ stages) with `total_imps > N`; excess = `total_imps − N`. This bounds total household frequency at N and includes over-delivery a loose single-group cap would also permit (an upper measure vs A on the leaked set).
- Cap 8/wk: 3.52M hh over cap → 37.4M excess imps → **$0.61M / 7d**
- Cap 12/wk: 1.95M hh over cap → 26.2M excess imps → **$0.41M / 7d**

**Headline range: $0.41M–$0.66M per 7 days = 5.5%–8.9% of the $7.44M analyzed weekly spend.** Linear monthly extrapolation (×4.35): **~$1.8M–$2.9M/mo** (conservative floor — the 7-day window understates true household frequency; a 30-day window would show more).

### Assumptions behind the $ estimate
1. **Excess spend = excess impressions × the household's realized avg cost-per-impression** (`total_spend/total_imps`), applied per household then summed. Not a flat platform CPM.
2. **Method A holds the heaviest counter's delivery constant** — it removes only the *additional* groups/stages, so it is a conservative, cap-value-free measure of the leakage.
3. **Method B assumes a single advertiser-level cap of N would bind total weekly frequency at N.** N=8 and N=12 chosen from the Phase-0 curve (freq≥8 = the high-frequency pool); they bracket a plausible policy.
4. **Fixed campaign budgets ⇒ redirectable, not saved.** Capping frequency does not return budget; pacing respends it on other households (net-new reach). The dollar figure is the magnitude of *wasteful repetition that would be reallocated toward incremental reach*, whose lift is what the RCT measures. Do not quote it as cost savings.
5. **Conservative floor:** 7d window; INNER JOIN to `public_campaigns` drops any delivery on campaigns absent from the live dim; both understate leakage.

## 4. The fix (scope for the bidder team)

Add **one advertiser-level rollup counter** in `rtb-campaign-service`, keyed **`rtb:frequency:{ip}:advertiser=<aid>`**, incremented on every won impression alongside the existing per-group/per-campaign counters. `do_fcap` evaluates it in addition to the current keys and suppresses the bid if the advertiser-level counter binds — collapsing an advertiser's fragmented per-group / per-stage counters into a single household frequency, which is what eliminates both leakage axes above. **Client-transparency constraint:** the rollup cap governs the **DEFAULT** cap only; advertisers with `has_custom_frequency_caps=TRUE` keep their explicit per-group/campaign behavior untouched. Because the counter merely enforces the advertiser-level frequency the system already intends (the missing rollup is a correctness gap, not a policy choice), **this ships independently of the RCT** — the RCT sets the *optimal* cap value and the bandit adapts it; the rollup counter fixes the leak now. Fails-open behavior (scope §3) is unchanged: on Redis error the new counter, like the existing ones, stops enforcing, so delivered frequency must still be measured, not assumed.

---

## Caveats
- **Cross-stage over-counts pure leakage where a `campaign_group`-level cap is set** (that cap already bounds a group's stages); the unambiguous, always-unbounded defect is **cross-group** ($0.51M/7d Method A). Cross-stage ($0.66M) is the broader upper bound.
- **7-day window = conservative floor.** True lifetime household frequency (and thus leakage) is higher.
- Absolute spend ($7.44M/7d) reflects only live-dim campaigns with valid IP, WGU + AID 90 excluded; leakage **percentages** are the robust output, not the absolute total.
