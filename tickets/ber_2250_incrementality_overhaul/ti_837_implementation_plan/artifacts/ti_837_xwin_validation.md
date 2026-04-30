# TI-837 — Cross-Window Validation (Phase 0c)

**Status:** _IN FLIGHT — awaiting xwin job completion. Numbers will be filled in by `compare_xwin_vs_v5.py` once `outputs/ti_837_lift_30adv_7day_v5_xwin_2026_04_22_to_28.json` is populated._

## Purpose

Validate that the v5 segment ordering and approximate ATT magnitudes
reproduce on a different week of data. Standard methodology check before
broader sharing — answers Alex Knorr's "let's validate the findings" ask.

## Design

| Field | v5 (canonical) | xwin (replication) |
|---|---|---|
| Cohort | 30 advertisers (Phase 2 cohort) | Same 30 |
| Hash | MD5(advertiser_id:ip) mod 1000 < 100 | Same |
| Segments | all, prosp, stage1, rtg | Same |
| win_rates | Hardcoded STRUCT (computed from v5 run) | Same STRUCT (carried over — see caveat 1) |
| Impression window | 2026-04-20 → 2026-04-26 | **2026-04-22 → 2026-04-28** (+2-day shift, 5/7 day overlap with original) |
| Visit window | 2026-04-20 → 2026-04-29 | 2026-04-22 → 2026-04-30 (in flight at submission time — see caveat 2) |
| BQ wall | ~5.7 hr | _TBD_ |
| BQ slot-sec | ~8.2M | _TBD_ |
| Bytes billed | ~531 TB | _TBD_ |

## Caveats

1. **Same win_rates carried over.** The v5 STRUCT literal contains
   per-(advertiser, segment) win_rates computed from the original window.
   Real win_rates drift week to week; we use the v5 values as an
   approximation. Expected drift: <10% relative on most advertisers, with
   higher drift for low-volume advertisers. Resolution: future runs
   should recompute win_rates per window via `queries/ti_837_compute_winrates_per_advertiser_segment.sql`.

2. **Asymmetric visit window for late impression days.** The xwin run was
   submitted 2026-04-29 19:00 PT. Visit data through 2026-04-30 was not
   yet fully landed. So:
   - 04-22 impressions: full +3 day post-period
   - 04-28 impressions: only ~+1 day post-period

   Both arms (treated and holdout) get the same truncation, so ATT is
   unbiased. Power is reduced for the last 2 impression days.

3. **5/7 day overlap with original v5 window.** The xwin window
   (04-22 → 04-28) overlaps days 04-22, 23, 24, 25, 26 with v5
   (04-20 → 04-26). Only 2 fresh days (04-27, 04-28) are truly
   unseen. This bounds how independent the two estimates are. A fully
   non-overlapping cross-window would need 04-27 → 05-03, which requires
   waiting for 05-03 augmentor + 05-06 visit data to land — not feasible
   without 5+ days of waiting.

## Headline (high-intent guid sample-weighted)

_To be populated by `compare_xwin_vs_v5.py` once xwin data lands._

| Segment | guid ATT (v5) | guid ATT (xwin) | Δ (xwin − v5) |
|---|---|---|---|
| Retargeting only | _TBD_ | _TBD_ | _TBD_ |
| All campaigns combined | _TBD_ | _TBD_ | _TBD_ |
| Prospecting (all stages) | _TBD_ | _TBD_ | _TBD_ |
| Stage 1 only | _TBD_ | _TBD_ | _TBD_ |

## Segment ordering check

The v5 deck's claim is **rtg > all > prosp > stage1**, with stage1 ≈ 0
and rtg ≫ prosp. For cross-window validation:

- **Pass** if xwin reproduces this same ordering.
- **Fail** if any pair flips.

_Result: TBD._

## Per-cell stability (high-intent guid)

The v5 per-advertiser distribution had `Δ < ±0.05pp` from leave-one-out.
For xwin, we compute per-(advertiser × segment) ATT_xwin − ATT_v5 deltas
and flag any cell with `|Δ| > 5pp`.

_Per-cell delta count: TBD._
_Cells with `|Δ| > 5pp`: TBD._

## Conclusions

_Pending xwin completion._

Anticipated outcomes:
- If segment ordering reproduces and per-cell deltas are mostly
  within ±5pp → **strong validation**, deck claims hold.
- If ordering flips on any segment pair → **invalidation signal**, need
  to investigate whether v5 was a one-off or xwin had a data issue.
- If magnitudes shift by more than ±5pp on the headline → quote a range,
  not a point estimate, in external materials.

## Files

- Query: `queries/ti_837_lift_analysis_30adv_7day_v5_segments_xwin_2026_04_22_to_28.sql`
- Submitter: `artifacts/run_xwin_robust.py`
- Comparator: `artifacts/compare_xwin_vs_v5.py`
- BQ output: `outputs/ti_837_lift_30adv_7day_v5_xwin_2026_04_22_to_28.json`
- Comparison output: `outputs/ti_837_xwin_vs_v5_comparison.json`
- v5 baseline: `outputs/ti_837_lift_30adv_7day_v5_2026_04_20_to_26.json`
