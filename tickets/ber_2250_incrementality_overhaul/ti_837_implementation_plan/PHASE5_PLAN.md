# TI-837 Phase 5 — Multi-segment Lift Analysis

**Goal:** Measure lift across **4 campaign segmentations** so we can see which targeting strategies drive incremental lift, and defend our choice of segmentation in the deck.

## The 4 segmentations

| # | Segment | SQL filter | What it isolates |
|---|---|---|---|
| 1 | **All campaigns** | (no filter) | Total MNTN incrementality across all strategies. What v1 measured (with the loose-denominator bug). |
| 2 | **Prospecting — all stages** | `objective_id IN (1, 5, 6)` | Stage 1 prospecting + Multi-Touch (S2) + Multi-Touch Full Funnel (S3). What v4 measured. |
| 3 | **Stage 1 only** | `funnel_level = 1` | Pure top-of-funnel — IPs MNTN reaches via prospecting before they enter the multi-touch flow. The "movable middle" investigation tier per Alex Bloore. |
| 4 | **Retargeting only** | `objective_id = 4` | IPs already engaged with the advertiser. Should show low/zero incremental lift if our methodology is correct (these visits were going to happen anyway). |

Same 30-advertiser cohort, same 7-day window, same hash, same prospecting universe — only the served_treatment / clickpass_visits filter changes per segment.

## Why each one matters

- **Segment 1** lets us show what a naïve "MNTN lift" calculation produces — and contrast with the segmented views.
- **Segment 2** is our headline number for prospecting-strategy incrementality.
- **Segment 3** isolates the cleanest top-of-funnel prospecting — where Alex Bloore predicts low high-intent lift, larger mid-intent lift.
- **Segment 4** is the falsification check: if retargeting shows huge lift, our methodology is broken (because retargeted IPs visit anyway). If retargeting shows ~0 lift, the methodology correctly handles already-engaged populations.

The four together tell the full story.

## Technical approach

### Step 1 — compute per-segment win_rates per advertiser

Each segment has a different served_treatment count → different empirical win_rate.

For the existing cohort:
- All-campaigns served per advertiser: from v1 output (already have)
- Prospecting-all-stages served: from v4 upstream query (already have)
- Stage-1-only served: NEW small query (`funnel_level = 1` filter on cost_impression)
- Retargeting-only served: NEW small query (`objective_id = 4` filter)

Per-advertiser win_rate = `served_segment / biddable_targeted`, where `biddable_targeted ≈ biddable_holdouts × 9` (hash symmetry).

### Step 2 — v5 SQL: single megaquery with 4-segment output

One pass, 4 served_treatment CTEs (one per segment), 4 biddable_holdouts CTEs (subsampled at segment-specific win_rate). Output keyed by `(segment, advertiser, group, tier, outcome)`.

Augmentor scan happens once (segment-agnostic) — dominates cost. The 4 cost_impression filters add modest extra work (cost_impression is small relative to augmentor).

Estimated cost: similar to v4 (~$90, ~110-130 min wall).

### Step 3 — extend compute_att.py

Add `segment` as a grouping key. Output per-segment IVW pools, alt pooling, leave-one-out, per-advertiser distributions.

### Step 4 — new charts

- **By-segment headline:** bar chart of lift per segment × tier (4 segments × 3 tiers).
- **By-segment wedge:** clickpass/guid wedge per segment.
- **Retargeting falsification:** per-advertiser retargeting-only ATT distribution — should cluster near zero.

### Step 5 — rebuild deck with multi-segment story

New narrative arc:
1. Cold open (puzzle)
2. Methodology
3. **Multi-segment headline** — "We measured the same advertisers four ways. The story changes by which campaigns count."
4. All-campaigns view (huge inflated lift, like v1)
5. Prospecting-all-stages (the headline)
6. Stage-1-only (movable middle test)
7. Retargeting-only (falsification check — should be ~0)
8. Wedge across segments
9. Per-advertiser distribution (high-intent, prospecting)
10. Methodology + caveats
11. Pipeline + cohort
12. What's next
13. Power Line

## Cohort decision

**Don't re-sample the cohort.** Diagnostic showed:
- 25 of 30 advertisers have nonzero retargeting spend (sufficient retargeting data for segment 4)
- All 30 have prospecting spend (segments 2, 3 covered)
- Pearson correlation between prospecting-share and v4 ATT = 0.080 (essentially zero) → cohort isn't selection-biased on this dimension

The 4-segment analysis works on the existing cohort. If after running we see a particular segment is data-poor for some advertisers, we can flag those individually rather than re-cohort.

## What gets updated (documentation)

After v5 runs:
- `summary.md §11` — v5 marked CANONICAL, full multi-segment table
- `methodology_status.md` — v5 row added, run history complete
- `methodology_explainer.md` — segment definitions, why each matters
- `methodology_defense.md` — NEW. Anticipates objections, defends choices.
- Deck rebuilt as the canonical 4-segment story
- Jira comment posted with v5 numbers

## Run order

1. Compute per-segment served counts (small upstream query — both Stage-1 and Retargeting in one query)
2. Compute per-advertiser per-segment win_rates
3. Write v5 SQL
4. Dry-run, validate
5. Kick off v5 in BQ background, Monitor armed
6. While running: extend compute_att.py for multi-segment, draft methodology_defense.md
7. When v5 lands: run compute_att, regenerate charts, rebuild deck
8. Commit, post Jira, share deck link

ETA total: 2-3 hours wall (~90-100 min for v5 BQ run + 30-60 min for cascade).
