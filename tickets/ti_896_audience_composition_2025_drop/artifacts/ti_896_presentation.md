# TI-896 — Audience composition shift analysis

War-room investigation into the late-2025 conversion / ROAS drop. This is the audience-side lane; conversion / pixel / attribution work is owned by Ray's team and customer-mix work by Will Cavey.

Malachi Dunn · 2026-04-22
Deck: https://gist.githack.com/mdunn-mntn/c4c818b76abacbdcc029db4a54e150c4/raw/ti_896_deck_standalone.html

---

## Question

Did the mix of audience types used by 2025-active advertisers shift materially during the late-2025 conversion drop, and if so — in which direction, when, and which cohorts?

## Cohort and data

- **Cohort:** 4,109 advertisers with ≥1 impression on any day in 2025; ~4,000 currently have active archive activity.
- **Lookback:** Nov 2024 → Apr 2026, weekly.
- **Targeting source:** `dw-main-bronze.integrationprod.archives_audience_segment_archives` (per-campaign segments, `expression_type_id = 2`, `is_targeted = TRUE`) and `archives_audiences_archives` (audience templates).
- **Delivery source:** `dw-main-silver.summarydata.sum_by_campaign_by_day`.
- **Effective windows:** per-campaign `LEAD(update_time)` capped at `min(LEAD, last_active_day + 1)` so paused-but-not-deleted campaigns don't extend their last-attached audience indefinitely.

## Product context — MNTN Match 2.0 audience tiers

Mountain Matched (MM 2.0) is the umbrella audience system. Within it, an impression's "TI Name" is determined by which scoring criteria the user satisfies. Peak Performance is one tier in this system, not a separate audience product.

| State | In Bucket (DS13) | In Vertical | Has Keywords (DS19) | TI Name | Score | MM 2.0 bid? |
|---|---|---|---|---|---|---|
| 1 | — | — | — | NULL | NULL | No |
| 2 | — | — | ✓ | Max Reach | NULL | Yes (keywords only) |
| 3 | ✓ | — | — | Mid Intent (not bid) | 3333-6665 | No |
| 4 | ✓ | — | ✓ | Mid Intent | 3333-6665 | Yes |
| 5 | ✓ | ✓ | — | **Peak Performance** | **8000** | Yes (DS13 + vertical, no keywords) |
| 6 | ✓ | ✓ | ✓ | High Intent | 10000 | Yes |

Fangorn scoring (4/30 beta launch) maps these to: High Intent 8001-10000, Peak Performance 6666-8000, Mid Intent 3333-6665. Advertiser-level scores do not change with the rollout.

## Bucket detectors and what they catch

| Bucket | Detector |
|---|---|
| "PP-enabled" (this analysis) | `score_type=rtc` AND `data_source_id=13` AND `data_source_id=19` |
| Mountain Matched | `data_source_id=2` OR per-advertiser `% - First Party Audience` source IDs |
| Keywords | `data_source_id=19` |
| 3P | `data_source_id=35` (LiveRamp IP) |
| CRM | `data_source_id=4` |

**Detector caveats:**

1. **Specificity.** The "PP-enabled" detector requires both DS13 and DS19. Per the state table, true PP-tier impressions (state 5) require DS13 AND vertical match but NOT keywords. So this detector catches audience expressions that enable Mid Intent + High Intent (states 4 + 6) and by inclusion can also deliver to PP. It does NOT specifically catch a "PP-only" audience (state 5 only). Read the 12% number as "advertisers with audiences capable of delivering across the High Intent + PP + Mid Intent tiers" rather than "advertisers explicitly choosing PP".

2. **Bucket overlap.** Of 100 randomly sampled PP-detector segment expressions, 24 also contain DS2 (the MM detector flag). PP-enabled and MM detectors are partially overlapping — counting them as fully independent overstates the underlying composition shift.

### Detector verification against prior conventions

- **Detector pattern matches prior tickets.** TI-221 and TI-270 use `expression LIKE '%"data_source_id":19,%' AND expression_type_id = 2` for the same MM/Keywords detection. Same regex convention.
- **`expression_type_id = 2` filter is correct.** Per Zach (2026-03-13), type 1 is legacy text format, not read by the system. Cohort archives since Aug 2025 contain 18,589 type-1 rows, all with `is_targeted = false` — confirms type 1 is non-targeting legacy.
- **Canonical names in `data_sources.name` don't match Bryce's product labels.** DS2's canonical name is "MNTN First Party" (Bryce → "MM"); DS19's canonical name is "MNTN Matched" (Bryce → "Keywords"). Bryce's product labels are used in this analysis; the dim names are cited for cross-reference only.
- **Per-advertiser data source IDs are zero-prevalence in cohort segment expressions.** The 12,158 per-advertiser sources named `{AID} - First Party Audience`, `{AID} - Third Party Audience`, etc. don't appear in cohort archive expressions (Apr 2026 sample). The MM detector's `name LIKE '% - First Party Audience'` fallback clause never fires; effective MM detection is just DS2.
- **3P bucket is narrowly scoped per direction (DS35 only).** Broader 3P ecosystem (DS3, DS11, DS17, DS18, DS20, DS22, DS29, DS33, DS36, DS39) is excluded by Bryce's scope. ~10-25% of PP expressions reference these other 3P sources; including them would shift the 3P bucket numbers materially.

## Headline numbers (Apr 13 2026)

| Bucket | % advertisers using | % of cohort spend | Δ presence Sep 29 → Dec 29 2025 |
|---|---:|---:|---:|
| Peak Performance | 12.4% | 12.2% | +7.8 pp |
| Mountain Matched | 96.6% | 39.1% | −2.0 pp |
| Keywords | 40.4% | 39.7% | −4.9 pp |
| 3P | 32.1% | 41.5% | −3.5 pp |
| CRM | 18.7% | 34.2% | −0.5 pp |

## Peak Performance adoption ramp

- Pre-launch baseline (through Sep 22 2025): ~1% of currently-active advertisers, attributable to early-access and legacy RTC+DS13+DS19 configurations.
- Sep 29 2025: 3.7%
- Oct 6 2025 (PP launch week): 6.5%
- Nov 17 2025: 11.0%
- Dec 29 2025: 11.5%
- Apr 13 2026: 12.4%

Spend-weighted PP share tracks the presence number closely (~12%). The Nov 19 2025 Max-Reach-off event did not visibly bend the trajectory.

## Mountain Matched spend share (DS2 detector)

| Period | MM share of cohort spend |
|---|---:|
| Through Oct 20 2025 | 73–79% |
| Oct 27 2025 | 56.7% |
| Nov 3 2025 | 44.0% |
| Nov 2025 → Apr 2026 | 42–46% (sustained) |

Presence-based MM stayed near 100% (advertisers retained at least one DS2-flagged campaign), so the shift is in delivery dollars not in whether MM is attached.

### Empirical check — MM cliff vs PP rise advertiser overlap

Per-advertiser comparison of week of Oct 13 2025 (pre-cliff) vs Nov 3 2025 (post-cliff). 1,773 advertisers active in both periods:

| Slice | n |
|---|---:|
| Of 135 new PP-detector advertisers in post: also retained MM (DS2) | 78 (58%) |
| Of 341 advertisers who lost MM (DS2) in post: gained PP detector flag | 52 (15%) |
| Stable on both PP and MM in both periods | 118 |
| Stable MM-only (no PP detector) in both periods | 526 |
| Lost MM (DS2) without gaining PP detector | 248 |

Most of the MM-spend cliff is NOT explained by detectable migration to PP-enabled. Likely explanations for the unexplained MM loss: (a) advertisers migrated to MM 2.0 syntax that uses neither DS2 nor matches the PP detector, (b) genuine reductions in MM-style spend, (c) campaign restructuring under the new tier system. Audience-side data does not isolate which.

## Other buckets

Sep 29 → Dec 29 2025 deltas (presence): Keywords −5pp, 3P −3.5pp, MM −2pp, CRM ≈ flat. All bucket presences declined modestly; only Peak Performance gained.

## Default vs custom Peak Performance

Two ways an advertiser runs PP:
- **Default:** audience template carries the minimal PP pattern (DS13 intent + DS19 keywords) and nothing else.
- **Custom:** template carries DS13+DS19 plus additional DS clauses (exclusion lists, CRM overlays, extra keyword groups, geo logic).

Random-sample discovery of 1,000 PP audience templates (Oct 2025+):
- 39% pure DS13+DS19
- 48% layered (3-4 DS ids)
- 14% heavily layered (≥5 DS ids)

Weekly cohort-level breakdown of PP adopters (stable across the entire ramp):
- ~32% default-only
- ~61% custom-only
- ~3% both
- ~5% unclassified (template not yet replicated to archives — CDC lag)

Classifier limit: template-level structural test does not capture campaign-level layering. An advertiser can attach a "pure" template to a campaign that also has separate exclusion audiences. Formal product definition of "default" is open with the audience-tools team (Ryan / Jordan).

## Per-advertiser ROAS comparison (Track C)

For 1,213 advertisers delivering in both windows below with ≥1,000 view-views in each:
- **Baseline:** Aug 1 – Sep 28 2025 (8 weeks, pre-launch tail)
- **Post:** Dec 1 – 31 2025 (4 weeks, post-launch ramped)
- **New PP adopter:** PP delivery share <1% baseline AND ≥5% post (n=206)
- **Non-adopter:** PP delivery share <5% post (n=1,007)
- **Continuing:** ≥5% in both windows (n=4, too small to publish)

Bootstrap medians, 1,000 resamples, 95% percentile interval:

| Cohort | n cohort | n with valid ROAS | Median Δ ROAS | 95% CI | Median Δ conv rate | 95% CI |
|---|---:|---:|---:|---:|---:|---:|
| New PP adopter | 206 | 101 (49%) | +64% | [+25%, +121%] | +38% | [+24%, +73%] |
| Non-adopter | 1,007 | 381 (38%) | +130% | [+104%, +154%] | +65% | [+49%, +84%] |

About half of each cohort has $0 order value in at least one window (lead-gen advertisers, no e-commerce pixel) — `delta_roas_rel` is undefined for them and they drop from the median calculation. The 95% CIs of the two cohort medians overlap, so the gap between adopter and non-adopter ROAS lift is directional but not statistically robust at 95%.

AOV: adopter median −1.3% [−3.7%, +2.5%], non-adopter median −0.1% [−1.9%, +1.7%]. Flat in both. The conversion-rate / ROAS gap, where it exists, is in conversion volume per impression, not basket size.

## Selection bias in Track C

Spend-weighted ROAS per cohort per week (median is uninformative because ~50% of advertisers have zero order value):
- Aug 2025 baseline: new-adopter cohort ~28–31, non-adopter cohort ~17–25.
- Q4 2025: both cohorts lifted in absolute terms.
- Apr 2026: new-adopter cohort ~28–35, non-adopter cohort ~25–29.

The new-adopter cohort had ~1.5x higher baseline spend-weighted ROAS than the non-adopter cohort. The two cohorts are not exchangeable at baseline — adopters self-selected into PP. Without propensity matching, the lift comparison does not isolate PP's effect.

## Default vs custom × ROAS

Within new adopters (n=206), classified by which template type drove ≥80% of their post-window PP delivery:

| Class | n cohort | n valid ROAS | Median Δ ROAS | Median Δ conv rate |
|---|---:|---:|---:|---:|
| Default-dominant | 54 | 34 (63%) | +41% | +43% |
| Custom-dominant | 135 | 58 (43%) | +64% | +24% |
| Mixed | 17 | 9 (53%) | +290% | +114% |

Sample sizes are too small for confident inference; bootstrap CIs are wide and overlap. Default and custom directions diverge (custom > default in ROAS, default > custom in conv rate) — interesting but underpowered. Re-run with longer post window once Q1 2026 settles.

## Caveats

- Cohort assignment in Track C self-selects. Comparing post-period outcomes does not isolate PP's effect on its own.
- Adopter and non-adopter cohorts had ~1.5x baseline ROAS gap.
- ~50% of Track C cohort has $0 order value; medians on residual; AOV not informative for those advertisers.
- Default-vs-custom classifier is template-level; doesn't capture campaign-level layering.
- `objective_id` is unreliable post-2025 TV migration; retargeting share chart uses both `objective_id` and `funnel_level`.
- Intent score history has 35-day TTL in BQ; cannot retroactively inspect Nov 2025 PP scoring.
- MM and PP share ~24% of advertisers at the database level; the MM-spend cliff and PP rise are not fully independent observations.

## Open questions

- **What does the new MM 2.0 audience expression look like at the database level?** Does it use a different syntax not caught by the DS2 or DS13+DS19+rtc detectors? If so, the bucket counts here understate MM 2.0 adoption and overstate the MM cliff.
- **What drove the Oct 27 MM (DS2) spend drop?** 248 of 341 affected advertisers don't show up in the PP detector. Audience-side data shows the timing but not the cause. Likely needs product-side review of what shipped that week.
- **What's the formal product definition of "default Peak Performance"?** Audience-tools team (Ryan / Jordan).
- **Default vs custom PP performance with longer post window.** Current 4-week Dec window is underpowered.
- **DS16 jump (1% → 35%).** Real in volume but partially polluted by NOT-clause exclusions (12.5%); needs include-vs-exclude JSON parsing before publishable.

## Methodology and verification

Reproducible from:
- 6 SQL queries in [`queries/`](../queries/)
- 7 output CSVs in [`outputs/`](../outputs/)
- Chart generator: [`artifacts/generate_charts.py`](generate_charts.py) (10 PNGs, 200 DPI)
- Bootstrap CI script: [`artifacts/bootstrap_track_c.py`](bootstrap_track_c.py) (1,000 resamples, RNG seed 20260422)
- Standalone deck builder: [`artifacts/build_standalone_deck.py`](build_standalone_deck.py)
- 16 verification checks (V1–V16): [`artifacts/ti_896_verification.md`](ti_896_verification.md)
- PP/MM overlap empirical check: [`queries/ti_896_pp_mm_overlap_check.sql`](../queries/ti_896_pp_mm_overlap_check.sql) → [`outputs/ti_896_pp_mm_overlap_check.csv`](../outputs/ti_896_pp_mm_overlap_check.csv)
