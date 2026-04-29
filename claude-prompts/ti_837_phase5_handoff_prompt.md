# TI-837 — Phase 5 Handoff Prompt (canonical state, multi-segment lift shipped)

Use this prompt to start a fresh chat that picks up TI-837 after v5 (the
4-segment multi-segment lift analysis) shipped on 2026-04-29.

---

You are picking up TI-837 after Phase 5 shipped. The 4-segment lift
analysis is the canonical study. The deck has NOT been shared externally
yet — only with the immediate TI team. Three open decisions for this
session.

## Open decisions you may be asked to act on

1. **Send the deck to Alex Knorr for sanity check?** Recommended path
   before broader sharing. He's the methodology lead on TI; he'd spot
   any remaining issues with the +21pp retargeting result (the most
   likely-questioned finding).
2. **Set up Databricks** (TI-837 cluster provisioned by Victor S.; we
   need the user's Databricks API token + the cluster's runtime version
   to install matching `databricks-connect`). Unlocks Phase 2a
   (conversions, 30-day window) and cross-window validation.
3. **Cross-window validation** — re-run v5 on a different 7-day window
   and check whether the segment ordering and magnitudes reproduce.
   Standard methodology rule before any external sharing.

## Final results — v5 (canonical, 2026-04-28)

**Single 30-advertiser cohort, 7-day window 2026-04-20 → 04-26 UTC. Same
hash, same prospecting universe. Cost: ~6 hr wall, 4.5T slot-ms,
126.7 TB billed, 139 stages (4-segment UNION ALL inflated graph).**

**The 4-segment headline (high-intent guid IVW):**

| Segment | guid IVW | sample-wt | clickpass IVW | wedge | cells pos |
|---|---|---|---|---|---|
| **Retargeting only** | **+21.07pp** | +28.89pp | +13.97pp | 0.66× | 8/8 |
| All campaigns combined | +3.12pp | +5.44pp | +2.88pp | 0.92× | 25/27 |
| Prospecting (all stages) | +0.78pp | +0.46pp | +1.24pp | 1.58× | 20/26 |
| **Stage 1 only** | **−0.06pp** | −1.03pp | +0.47pp | −8.5× | 12/25 |

**Segment SQL filters:**
- `all` — no filter
- `prosp` — `objective_id IN (1, 5, 6)` (Stage 1 prospecting + Multi-Touch S2 + MTFF S3)
- `stage1` — `objective_id IN (1, 5, 6) AND funnel_level = 1`
- `rtg` — `objective_id = 4`

`funnel_level` is authoritative for stage (per global gotcha — UI migration
broke `objective_id` mapping; many S3 campaigns have `objective_id=1`).

**Power Line (in deck):**
> Retargeting drives the lift. Pure prospecting drives almost none. Combined views hide both.

**Three findings:**
1. Retargeting drives the bulk of measured incremental lift in the experiment's frame. The "+21pp is incremental in the experiment" — what's open is the counterfactual scope (what would happen if MNTN didn't run retargeting at all). Tighter counterfactual needs Phase 2b bidder-level ghost bidding.
2. Stage 1 prospecting alone shows zero incremental lift at high intent. High-intent shoppers were going to convert anyway. Validates the "movable middle" hypothesis (mid-intent has more room).
3. The "+3.12pp combined" view is misleading. Mixed-segment denominators conflate retargeting's +21pp with Stage 1's zero. Earlier internal incrementality reports used this conflated view.

## Deck (canonical)

🔗 **https://gist.githack.com/mdunn-mntn/79e8e3e1d56a52a61dca2754c0161b59/raw/ti_837_phase2_presentation_deck_standalone.html**

15 slides:
1. Cold open — measuring the same advertisers 4 ways
2. Methodology (ghost-bidding ATT)
2b. **Segment definitions (SQL filters explicitly shown)**
3. Headline chart (4-segment lift comparison)
4. Headline numbers table
5. Segment × tier chart
6. Why retargeting drives 21pp (counterfactual scope caveat)
7. Stage 1 zero lift finding
8. Wedge chart by segment
9. Two methodology fixes (vs prior internal numbers)
10. Pipeline diagram
11. Cohort design
12. 6 caveats (incl. cross-window validation defined)
13. What's next
14. Power Line return

Built from `artifacts/build_phase2_deck.py` with charts as base64.

## What's in the repo (where to read first)

**Read in this order:**

1. **`tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/summary.md` §11** — full execution log, v5 marked CANONICAL with all numbers. Has a "Current State" header at top.
2. **`artifacts/ti_837_methodology_status.md`** — canonical issue tracker, v0→v1→v2→v3→v4→v5 run history with status of each fix.
3. **`artifacts/ti_837_methodology_defense.md`** — 18 anticipated objections + answers (ATT vs ATE, hash holdout, biddability filter, win-rate sampling math, MAX-tier construction, cohort selection, tier boundaries, window choice, IVW vs alt pooling, prospecting filter rationale, guid vs clickpass, N-gate, power analysis, propensity scoring, caveats, retargeting reframe, cross-window validation definition).
4. **`artifacts/ti_837_methodology_explainer.md`** — plain-English reference for IVW / N-gate / wedge / pooling methods / ATT, with v5 numbers.
5. **`artifacts/ti_837_phase2_cohort.md`** — final 30-advertiser list with rationale.
6. **`PHASE5_PLAN.md`** — the plan that produced v5.
7. **Meeting actions:**
   - `meetings/ti_837_01_alex_meeting_actions.md` — Alex Knorr 1:1 (denominator fix)
   - `meetings/ti_837_02_team_meeting_actions.md` — team meeting (Alex Bloore strategic framing, Bryce selection-bias caveat)
   - `meetings/ti_837_03_victor_meeting_actions.md` — Victor Savitskiy Databricks setup walkthrough

**Knowledge base updates from TI-837 work:**
- `knowledge/data_catalog.md` — augmentor/guid GCS paths added; `agg__daily_sum_by_campaign` stale-since-2026-03-31 noted
- `knowledge/data_knowledge.md` — Databricks read patterns (job compute is 3× cheaper than interactive cluster, GCS direct reads, BQ connector code samples); explicit-partition-filter requirement
- `knowledge/mntn_business.md` — intent-tier terminology table (high / **peak performance** / mid / max reach)
- `knowledge/experimentation.md` — IVW pathology, ATT methodology lessons
- `.claude/databricks_setup.md` — TI-837 cluster URL + auth setup pattern

## Canonical SQL

`queries/ti_837_lift_analysis_30adv_7day_v5_segments.sql` — the v5
multi-segment SQL. Single megaquery emitting `(segment, advertiser,
group, tier, outcome)` cells. Per-advertiser per-segment win_rates
hardcoded as STRUCT literal (computed upstream from biddable_holdouts × 9
hash symmetry). 4 segments: all / prosp / stage1 / rtg.

If you re-run for cross-window validation, change these 3 lines:
- `WHERE day IN ('20','21','22','23','24','25','26')` → new days
- `DATE(time) >= DATE(...)` → new window start (3 places)
- `DATE(time) <  DATE(...)` → new window end (3 places, +3 days post for visit windows)

Win_rates are advertiser-specific; if cohort changes or window changes
significantly, recompute via the upstream pattern in
`PHASE5_PLAN.md` step 1.

## Cohort (30 advertisers, do NOT change without re-validating)

Stratified across 13 high / 7 mid / 10 low spend × 20 verticals. Ferguson
Home (31276) and Ancient Nutrition (31455) are Phase 1 anchors retained.
Largest single advertiser is 8% of pooled high-tier weight. Selection
gates documented in `artifacts/ti_837_cohort_selection_criteria.md`.

```
30181, 30392, 30496, 31276, 31297, 31455, 31464, 32244, 32320, 32404,
32527, 32899, 33467, 33572, 33684, 34141, 34365, 34862, 35086, 35374,
35573, 37222, 37796, 38307, 38422, 42097, 43996, 46426, 50525, 56187
```

## Operational rules (carried forward)

- **BQ queries**: pipe SQL via stdin to `bash .claude/scripts/bq_run.sh`. The bq CLI flag-parser crashes on SQL strings starting with `--`.
- **Augmentor 10-day TTL**: today is 2026-04-29. Window 04-20 partition expires ~2026-04-30. Backward replication beyond ~3 days will fail. Forward replication (next week) is straightforward; cross-window validation should target 2026-05-04 → 05-10 as the next clean window.
- **Job compute is 3× cheaper than interactive cluster** for Databricks. Use job compute for runs >20-30 min or >16-20 cores. Tags required on every cluster: `project=TI-XXX, squad=ML, env=Dev`.
- **GCS read paths** (no BQ scan billing):
  - `gs://mntn-data-archive-prod/augmentor_log/`
  - `gs://mntn-data-archive-prod/guid_log/`
  - `gs://household-scoring-prod/output/scoring/prospecting_intent/` (hive year/month/day)
- **Specify partition predicates explicitly** in Spark `.filter()` for GCS reads, not just downstream WHERE. Otherwise Spark scans every partition.
- **Commit and push after every meaningful change.** No batching.
- **Update Jira on TI-837** after major sub-stages with wiki-markup template (curl REST API v2; MCP `jira_add_comment` renders wiki markup as literal text).
- **Use MNTN terminology** in any external-facing material: "high intent", **"peak performance"** (NOT peak intent), "mid intent", "max reach". Names of specific people are stripped from performance decks (Alex Bloore, Alex Knorr, Bryce, Victor, etc.) — internal docs keep attribution.

## Methodology summary (one-paragraph version)

Ghost-bidding ATT methodology: subjects are the 30 advertisers' targetable
IPs from `prospecting_intent_v1`, max-tier-assigned over the 7-day week,
hash-bucketed into 10% holdout / 90% targeted. Treated arm = 90% bucket
∩ `cost_impression_log` filtered per segment. Holdout arm = 10% bucket ∩
`augmentor_log` (biddability proof) subsampled at per-(advertiser, segment)
empirical win_rate. Outcomes are visits in `clickpass_log` (attribution-credited)
and `guid_log` (cause-agnostic). Per-cell ATT via two-proportion difference
with Wald SE; pooled via IVW + arithmetic mean + median + sample-weighted.
Per-cell N-gate at CI half-width ≤ 0.5pp on guid-ATT. Leave-one-out
sensitivity check at the cell-pool level. 6 known caveats (cohort selection,
single window, intent-score movement, CTV multi-advertiser confounding,
random subsampling vs bidder selection, retargeting counterfactual scope).

## Suggested first action

1. Read `summary.md` "Current State" section at the top.
2. Open the deck link in a browser; spot-check that it renders.
3. Confirm with the user which open decision (1, 2, or 3 above) to act on
   this session. Don't assume; the path forward depends on which of:
   sanity check with Alex Knorr → Databricks setup → cross-window
   validation has priority for the user's deadline.

If the user opens with a question, answer it first using the methodology
docs above as ground truth. If they ask for the headline number, the
correct answer depends on which segment — never just say "+X pp" without
specifying segment.
