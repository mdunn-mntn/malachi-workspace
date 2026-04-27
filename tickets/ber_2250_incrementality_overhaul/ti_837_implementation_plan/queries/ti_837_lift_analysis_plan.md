# TI-837 — Ad-hoc Ghost-Bidding Lift Analysis Plan

**Date:** 2026-04-27
**Owner:** Malachi
**Status:** In progress — analysis pipeline being built

## Goal
Compute Average Treatment on the Treated (ATT) — visit-rate lift from CTV impressions for IPs that *would have been served if not for the holdout assignment* — on a single advertiser, using only existing data (augmentor_log + cost_impression_log + clickpass_log + guid_log).

This is the **stopgap analysis** path. If it produces meaningful estimates, it becomes the April 30 TI-855 deliverable and the validation signal for the production bidder-level ghost bidding (Zach/Jordan path B).

## Methodological foundation
- **Holdout hash**: `MD5('{AID}:{IP}')` mod 1000 ∈ 0-99 = holdout (10%), 100-999 = targeted (90%).
- **Holdout IPs in augmentor_log**: present in the log but *not in `mntn_segments`* for the segment they're a holdout of (verified 2026-04-22 by Matt Brorby + Alex Knorr).
- **Implication**: target audience must be reconstructed externally — we use Alex Knorr's `dw-main-bronze.external.TI_835_prospecting_scores` table, which already has the targetable IP universe for 10 advertisers including WGU (31357).
- **Exclude AID 90** (MNTN PSA advertiser — serves to holdouts intentionally).

## Pipeline

```
Step 1: Scope the problem (cheap)
    1a. Inspect TI_835_prospecting_scores volumes per advertiser
    1b. Pick analysis window inside augmentor_log TTL (10 days)
    1c. Identify active WGU campaigns in window

Step 2: Build candidate-holdout set (the new methodology)
    2a. From prospecting_scores (WGU): compute holdout bucket on each IP
    2b. Filter to holdouts (bucket 0-99) — this is the "would-have-been-targeted" universe
    2c. Look up these IPs in augmentor_log during window — at least 1 appearance proves
        biddability. Drop IPs with zero appearances.
    2d. Output: candidate-holdout IP set with intent_group + min/max augmentor appearance time

Step 3: Build treatment set
    3a. From prospecting_scores (WGU): filter to targeted (bucket 100-999)
    3b. Join cost_impression_log on IP + advertiser_id + window
    3c. Exclude AID 90 (PSA)
    3d. Output: treated IP set with intent_group + first/last impression time

Step 4: Compute visit outcomes for both groups
    4a. clickpass_log visits within window per IP (MNTN-attributed)
    4b. guid_log visits within window per IP (total site traffic)
    4c. Aggregate: visit_rate per IP-group per intent_tier per outcome-source

Step 5: Raw ATT
    visit_rate(treated, intent_tier) - visit_rate(holdout-candidate, intent_tier)
    Two outcome variants: clickpass-based and guid-based.

Step 6: Propensity-matched ATT
    6a. Bin intent_score (already binned in intent_group: high / peak / mid / max_reach)
    6b. Within each bin, compute matched ATT
    6c. Stratification weights match the treated distribution

Step 7: Confidence intervals
    Two-proportion z-test on each (intent_tier, outcome-source) cell. Bootstrap if needed.
```

## Files
| File | Purpose |
|------|---------|
| `ti_837_01a_inspect_prospecting_scores.sql` | Volumes per advertiser, intent_group breakdown |
| `ti_837_01b_pick_window.sql` | Confirm augmentor_log TTL coverage, pick window |
| `ti_837_01c_active_wgu_campaigns.sql` | WGU campaigns active in chosen window |
| `ti_837_02_candidate_holdouts.sql` | Step 2 — candidate biddable holdouts |
| `ti_837_03_treatment_set.sql` | Step 3 — actually-served IPs |
| `ti_837_04_visit_outcomes.sql` | Step 4 — clickpass + guid joins |
| `ti_837_05_att_raw.sql` | Step 5 — raw ATT by intent_tier |
| `ti_837_06_att_propensity_matched.sql` | Step 6 — matched ATT |

## Decisions / Assumptions
- **Single advertiser to start: WGU (31357).** Largest, has Alex's pre-computed prospecting_scores.
- **Window: 7 days inside augmentor_log TTL.** Conservative — leaves buffer.
- **Visit window: same 7-day window for treatment exposure → visit, with no visit lookback before window start.** Avoids attributing pre-window visits to in-window exposures.
- **Outcome metrics: both clickpass and guid.** TI-835 showed they tell different stories — both useful.
- **Intent tier as propensity-match variable:** intent_group is the authoritative tier label in prospecting_scores.

## Open questions to resolve as we go
1. How much of WGU's prospecting_scores universe actually appears in augmentor_log during the 7-day window? (drives candidate-holdout volume)
2. What's the served-vs-not coverage for treated IPs? (sanity check vs TI-835's 14-16% figure)
3. Does the raw ATT differ from the propensity-matched ATT? (signal of intent-tier confounding)
4. Do clickpass and guid produce different ATTs? (re-validates "Two Stories" finding at IP level)
