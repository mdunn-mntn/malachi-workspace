# TI-837 — Phase 2 Run Handoff Prompt

Use this prompt to start a fresh chat that picks up after the Phase 2
**cohort selection** session (2026-04-27). The 30-advertiser cohort is
locked. What's left is the actual ATT run on the new cohort.

---

You are picking up TI-837 Phase 2 after the cohort selection session
shipped. The 30-advertiser stratified cohort is finalized and committed.
Augmentor partitions for the analysis window (2026-04-20 → 2026-04-26
UTC) are still live but the 04-20 partition expires on/around 2026-04-30
under the 10-day TTL — run within 3-4 days.

## What's already done — don't redo

- **Cohort locked:** 30 advertisers, stratified across 13 high / 7 mid /
  10 low spend × 20 verticals. Phase 1 anchors (Ancient Nutrition,
  Ferguson) retained; the 4 tier-collapsed Phase 1 advertisers
  (HexClad, First Watch, Zazzle, Northern Tool) correctly excluded.
  Largest single advertiser is 8% of pooled high-tier biddable holdouts
  (vs Ancient was ~40% in Phase 1) — IVW dominance fragility bounded.
  - List: `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_phase2_cohort.md`
  - Selection rationale: `ti_837_cohort_selection_criteria.md`
  - Reproducible pipeline: `ti_837_cohort_scorer.py`, `ti_837_cohort_builder.py`
  - Stage A queries + outputs: `queries/cohort_selection/`, `outputs/cohort_selection/`

- **Methodology hardening documented:**
  - `frac_high_only ≤ 0.95` tier-collapse gate (new)
  - Empirical power floor: n ≥ 5,000 biddable_holdouts at 95% CI ≤ 0.5pp
  - Sister-company audience dedup
  - Stage A.2 (full augmentor scan) skipped — used hash-symmetry +
    cost_impression as biddability proxy ($250-500 saved)
  - Discovered: `agg__daily_sum_by_campaign` stale at 2026-03-31 — saved
    to `knowledge/data_knowledge.md`

- **Summary §11** in `summary.md` documents the full cohort selection
  methodology.

- **Jira:** comment 566598 posted on TI-837.

## Phase 2 ATT run — what this session is for

Run the same Phase 1 lift pipeline on the new 30-advertiser cohort.

### Step 1 — Adapt the lift SQL (5 min)

The Phase 1 SQL is at:
`tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_7adv_7day.sql`

The only change: replace the hardcoded 7 advertiser IDs in three places
(line 21 comment, line 48 prospecting filter, line 118 cost_impression
filter) with the 30-advertiser list:

```
30126, 30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320,
32404, 32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086,
35374, 35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525,
56187
```

(31 IDs there — that's a typo, drop one. Get the canonical 30 from
`outputs/cohort_selection/cohort_final.csv` column `advertiser_id`.)

Save as
`queries/ti_837_lift_analysis_30adv_7day.sql`. Don't modify the original
7-advertiser SQL — keep it for reference.

### Step 2 — Run the lift pipeline (2-4 hours wall)

```bash
cat queries/ti_837_lift_analysis_30adv_7day.sql | bash .claude/scripts/bq_run.sh \
  --ticket "TI-837" --label "lift_30adv_7day_2026_04_20_to_26" \
  --use_legacy_sql=false --format=prettyjson --max_rows=200 \
  --project_id=dw-main-silver \
  > outputs/ti_837_lift_30adv_7day_2026_04_20_to_26.json 2>&1
```

Expected cost: $200-400 (Phase 1 was 126 TB / $90 for 7; 30 advertisers
should be 200-400 TB if augmentor scan is amortized as in Phase 1).
Expected wall time: 2-4 hours.

**Run in background** (`run_in_background: true`) and use Monitor
with the `Logged to` keyword to wait. Don't poll.

### Step 3 — Run the IVW + sensitivity (1 min)

```bash
python3 artifacts/ti_837_compute_att.py \
  --input outputs/ti_837_lift_30adv_7day_2026_04_20_to_26.json \
  --window 2026-04-20_to_26 \
  --out-dir outputs/
```

Expected outputs (gitignored):
- `ti_837_per_cell_table_30adv.csv`
- `ti_837_meta_analysis_30adv_2026_04_20_to_26.json`

### Step 4 — Compare to Phase 1 numbers

Phase 1 headline (locked):
- High-intent IVW: clickpass +4.17pp, guid +3.36pp. Wedge ratio **1.24×**.
- Peak-intent IVW: clickpass +0.55pp, guid +0.88pp. Wedge ratio **0.62×**.
- Mid-intent: noise floor.

The Phase 2 30-advertiser run has different headline numbers. The
**direction** of the wedge inversion (high over-credits, peak
under-credits) should hold if the methodology is robust. The
**magnitude** should be more conservative (less Ancient-dominance,
more advertiser averaging).

Per-advertiser ATT: 30 spans, including 2 Phase 1 anchors for
cross-validation. Ancient Nutrition's per-advertiser high-intent
guid-ATT was +1.76pp in Phase 1 — the Phase 2 number should match
(same window, same advertiser, same pipeline).

### Step 5 — Charts + deck (optional, 1-2 hours)

If presenting to the team: regenerate the 4 Phase 1 charts with the
30-advertiser data. Adapt `artifacts/generate_charts.py` to accept the
new CSV. Build a Phase-1-vs-Phase-2 comparison chart as a 5th figure.

Self-contained RevealJS deck pattern: see
`artifacts/ti_837_presentation_deck.html` — duplicate, swap data, share
via `share_deck.sh`.

## Operational rules (from global CLAUDE.md)

- BQ queries: pipe SQL via stdin (the bq CLI flag-parser crashes on SQL
  strings starting with `--`). Use `bash .claude/scripts/bq_run.sh`.
- Augmentor TTL: today is 2026-04-27. Window's 04-20 partition expires
  ~2026-04-30. **Run the lift pipeline within 3 days.**
- Commit and push after every meaningful artifact.
- Update Jira on TI-837 at the end of the run with the wiki-markup
  template Phase 1 used.

## Critical files to orient on

1. `artifacts/ti_837_phase2_cohort.md` — the 30-advertiser list + rationale
2. `queries/ti_837_lift_analysis_7adv_7day.sql` — Phase 1 SQL to adapt
3. `artifacts/ti_837_compute_att.py` — IVW + sensitivity (no changes
   needed; works on any per-cell table)
4. `summary.md` §11 — Phase 2 cohort selection record
5. `knowledge/experimentation.md` — methodology notes

## Suggested first action

1. Read `artifacts/ti_837_phase2_cohort.md` to confirm the 30 IDs.
2. Adapt the SQL (Step 1) and dry-run for byte estimate.
3. Confirm bytes are reasonable (<500 TB) before kicking off Step 2.
4. Use `run_in_background: true` for Step 2 — Monitor for completion.
5. Steps 3-4 are fast once Step 2 finishes.
