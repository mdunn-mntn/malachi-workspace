# TI-837 — Ghost-Bidding Lift Analysis: Execution Prompt

You are picking up TI-837 (under BER-2250) at the **execution phase**. Planning is complete. A 7-stage execution plan is locked. Your job is to run it end-to-end and produce a presentation-ready deck for the TI team by 2026-04-30.

This is an **execution chat**. You write SQL, run BQ queries, compute meta-analysis, build charts, and assemble the deck. You do NOT re-litigate locked decisions.

---

## 60-second context

TI-837 is the ATT (ghost-bidding) replacement for the failed ITT shuffling experiment. The 1-day Zazzle smoke test (2026-04-27) confirmed the methodology works — recovered a +1.30pp guid-ATT signal at high-intent that ITT in TI-835 couldn't see, plus quantified the clickpass-vs-guid wedge as ~78% attribution-capture overstatement.

Now scale to 7 advertisers × 7 days, combine via inverse-variance-weighted meta-analysis, and produce a deck.

---

## Locked decisions (do NOT re-litigate)

- **Advertisers (all 7, single batched query):** Ferguson Home (31276), Ancient Nutrition (31455), First Watch (34143), HexClad (34611), Clayton Homes (34838), Zazzle (37775), Northern Tool (40563). Angi and REVOLVE excluded — not in `household_scoring__prospecting_intent__v1`.
- **Analysis window:** 2026-04-20 → 2026-04-26 UTC (7 days, inclusive start / exclusive end).
- **Visit observation window:** 2026-04-20 → 2026-04-29 UTC (3-day post-period for cross-day attribution).
- **Aggregation:** inverse-variance-weighted meta-analysis (per-tier across advertisers; MNTN-overall across all non-empty cells).
- **Per-advertiser N gate:** 95% CI half-width < 0.5pp on guid-ATT.
- **Outcomes:** clickpass AND guid (always both — the wedge IS the attribution-capture story).
- **Tier stratification:** mandatory (high / peak / mid / max_reach where present).
- **BQ access:** read-only. No sandbox table writes. Use single-query batching as the primary cost optimization (one augmentor_log scan amortized across 7 advertisers).
- **Cost:** speed wins, cost is not the constraint. Don't waste cycles micro-optimizing.
- **Methodology:** ghost-bidding ATT with prospecting_intent + augmentor_log + cost_impression_log + clickpass_log + guid_log. Biddable-holdout filter stays loose. CUPED + tightening deferred. No external validation.
- **Negative peak guid-ATT:** if it replicates, document in deck as "selection bias on loose biddable-holdout filter, deferred to Phase 2." Do NOT chase as separate investigation.

---

## Your starting point

Read these in order before doing anything else:

1. **`tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_execution_plan.md`** — the plan you are executing. This is the source of truth for the staged ladder, expected costs, go/no-go criteria, output structure, and risks.
2. **`tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/summary.md`** — full ticket context, smoke-test results, methodology corrections.
3. **`tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis.sql`** — working single-advertiser SQL pipeline. Your starting template.
4. **`tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_compute_att.py`** — single-advertiser ATT computation. Extend it for multi-advertiser + IVW + sensitivity.
5. **`tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/outputs/ti_837_lift_zazzle_1day_2026_04_24.json`** — smoke-test output for the Stage 1 sanity check (Zazzle row should reproduce these point estimates within ±0.05pp on the new 04-23 window).
6. **`knowledge/data_knowledge.md`** sections: "Canonical prospecting_intent table", "BQ dry-run unreliable on federated tables", "clickpass_log cannot do apples-to-apples holdout comparisons", "augmentor_log mntn_segments for holdout IPs".
7. **`knowledge/experimentation.md`** section "Ghost-Bidding ATT — TI-837 Application Notes (2026-04-27)" — reusable methodology lessons from the smoke test.
8. **`documentation/docs/presentation_playbook.md`** + Tufte standards in `.claude/CLAUDE.md` — for Stage 5 deck.

---

## What you produce

Working backward from the deck:

| Stage | Output file(s) | What it is |
|---|---|---|
| 1 | `outputs/ti_837_lift_7adv_1day_2026_04_23.json` | 1-day 7-advertiser smoke validating multi-advertiser batching |
| 2 | `outputs/ti_837_lift_7adv_7day_2026_04_20_to_26.json` | Primary 7-day analytical output |
| 3 | `outputs/ti_837_meta_analysis_2026_04_20_to_26.json` + `outputs/ti_837_per_cell_table.csv` | IVW pools + per-cell N-gate flags + leave-one-out sensitivity |
| 4 | `outputs/ti_837_lift_7adv_extended_<aid>_<window>.json` (only if needed) | Window extensions for cells failing N gate |
| 5 | `artifacts/generate_charts.py` + 4 PNGs + `artifacts/ti_837_presentation.md` + `artifacts/ti_837_presentation_deck.html` | Deck deliverable |

Critical Stage 5 charts (per the plan):
1. Per-tier guid-ATT bar with 95% CI + clickpass overlay (the wedge — money chart)
2. Per-advertiser high-intent guid-ATT with 95% CI, descending
3. Wedge ratio (clickpass-ATT / guid-ATT) per tier
4. MNTN-overall headline number with CI

Run the critique at `claude-prompts/presentation_critique.md` against the deck before declaring done. Don't ship without ≥4 on Power Line, Data Persuasion, Billboard Test.

---

## First execution step (start here, no ambiguity)

1. Read the plan file. Then come back here.
2. Verify `augmentor_log` partitions for 2026-04-20 onward exist (TTL is 10 days, today is 2026-04-27 — buffer is tight):
   ```bash
   bq query --use_legacy_sql=false --format=prettyjson --project_id=dw-main-silver \
     "SELECT partition_id, total_rows FROM \`dw-main-bronze.raw.INFORMATION_SCHEMA.PARTITIONS\` WHERE table_name = 'augmentor_log' AND partition_id >= '20260419' ORDER BY partition_id"
   ```
   If 04-20 is missing, slide the window forward by N days and update the plan accordingly.
3. Copy the smoke-test SQL to a new file:
   ```bash
   cp tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis.sql \
      tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_7adv.sql
   ```
4. In the new file, replace **every** occurrence of `advertiser_id = 37775` and `CAST(advertiser_id AS INT64) = 37775` with `advertiser_id IN (31276, 31455, 34143, 34611, 34838, 37775, 40563)` (cast as needed — prospecting needs the CAST, the silver tables don't).
5. Add `advertiser_id` (and join it where needed — biddable_holdouts, served_treatment, clickpass_visits, guid_visits) to the SELECT, GROUP BY, and the subjects CTE. Each subject row should now carry (advertiser_id, ip, intent_tier).
6. Update the window dates to `2026-04-23 00:00:00 UTC` → `2026-04-24 00:00:00 UTC` (Stage 1 1-day window).
7. Update the prospecting partition filter to `day = '23'`.
8. Run via the perf wrapper (NOT plain `bq query`):
   ```bash
   bash .claude/scripts/bq_run.sh --ticket "TI-837" --label "stage1_7adv_1day_smoke" \
     --use_legacy_sql=false --format=prettyjson --max_rows=500 --project_id=dw-main-silver \
     "$(cat tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_7adv.sql)"
   ```
9. Save output to `outputs/ti_837_lift_7adv_1day_2026_04_23.json`.
10. Verify the Stage 1 go criteria (in the plan §5):
    - Query completes; no per-advertiser anomalies.
    - Zazzle row reproduces smoke-test point estimates within ±0.05pp (window date differs slightly so allow drift).
    - Per-advertiser high-intent guid-ATT lands roughly in [+0.3pp, +3pp] across advertisers.
    - Single-query batching cost ≤2× the smoke-test 18 TB (i.e., augmentor_log scan amortized correctly across 7 advertisers).
11. If go, proceed to Stage 2. If no-go, debug the failing advertiser before scaling.

---

## Process expectations

- **Commit and push after every stage** (per global CLAUDE.md). Small, frequent commits with `TI-837:` prefix.
- **Update `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/summary.md` after each stage** — findings, surprises, cost/wall-time actuals. Don't let it fall behind reality.
- **Update `knowledge/data_knowledge.md` and `knowledge/experimentation.md`** the moment a new lesson surfaces (e.g., did single-query batching work as predicted? Was the 7-day cost in the plan's range?).
- **Use `bash .claude/scripts/bq_run.sh`** for every BQ run — it logs perf metrics to `knowledge/bq_perf_log.jsonl`.
- **Post Jira progress comments** at end of stages 2, 3, and 5 (use curl REST API v2 per `.claude/CLAUDE.md` Section 9).
- **Add Todoist updates** as subtasks land.
- **Do not ask "should I proceed?" between stages** — execute the ladder. Pause only if a go criterion fails or a methodology question arises that the plan didn't anticipate.

You're not planning. You're shipping.
