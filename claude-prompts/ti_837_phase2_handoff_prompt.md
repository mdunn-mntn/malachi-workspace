# TI-837 — Phase 2 Handoff Prompt

Use this prompt to start a fresh chat that picks up after Phase 1 (visit-based ATT) shipped.

---

You are picking up TI-837 after Phase 1 shipped. The 7-advertiser × 7-day visit-based ATT analysis is complete: deck delivered, methodology validated, Jira ticket updated, knowledge files updated. The previous session ran the full execution ladder (Stages 1-5 plus a polish revision after user review of the rendered deck).

**Final shared deck (Phase 1):**
https://gist.githack.com/mdunn-mntn/ef648cae0ba1c6ac769df652f2de4615/raw/ti_837_presentation_deck.html

## What's already done — don't redo any of this

- 7-advertiser pipeline working (Ferguson, Ancient Nutrition, First Watch, HexClad, Clayton, Zazzle, Northern Tool). SQL: `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_7adv_7day.sql`.
- Per-cell ATT, IVW meta-analysis, N-gate, leave-one-out sensitivity all implemented in `artifacts/ti_837_compute_att.py`.
- Tufte-style chart generator at `artifacts/generate_charts.py`. Four PNGs + a self-contained 527 KB RevealJS deck.
- Headline numbers (final, locked):
  - High-intent IVW pool: clickpass +4.17pp, guid +3.36pp (CI ±0.02pp, n=22M IPs across 7 advertisers, 7-day window 2026-04-20 → 04-26 UTC). Wedge ratio **1.24×** (clickpass over-credits real lift by 24%).
  - Peak-intent IVW pool: clickpass +0.55pp, guid +0.88pp. Wedge ratio **0.62×** (clickpass under-credits real lift by 38%). The wedge inverts at peak.
  - Mid-intent: noise floor.
  - Per-advertiser high-intent guid-ATT spans 200×: Ferguson +10.55pp, HexClad +5.08, First Watch +4.52, Zazzle +3.63, Ancient +1.76, Clayton +1.08, Northern Tool −0.05.
- Knowledge docs updated:
  - `knowledge/experimentation.md` — Stage 2 quantified findings, MAX-score collapse trade-off, IVW pathology, "attribution vs incrementality answer different questions" reporting principle, Northern Tool case study.
  - `knowledge/data_knowledge.md` — bq CLI flag-parser RecursionError workaround (pipe SQL via stdin).
  - `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/summary.md` — full stage-by-stage execution log with cost, wall time, slot-hours.
- Jira progress comment posted on TI-837.

## Phase 2 scope — what this session is for

Phase 2 extends the validated visit-ATT methodology to **conversions**, **iROAS**, and (eventually) **bidder-level ghost bidding** to escape the augmentor_log 10-day TTL. Pick whichever path the user directs you toward — they're independent and can run in parallel.

**Phase 2a — Conversions outcome.** Same 7-advertiser pipeline, swap `ui_conversions` for `guid_log` as the outcome event. Conversions are ~10-20× rarer than visits (typical rates 0.05-0.3% vs. visit rates 1-2%), so:
- Need to extend the analysis window (3-4 weeks, not 1) to pass the 0.5pp N-gate. **OR** loosen the gate to 0.1pp absolute on conversions and accept wider per-cell CIs.
- Need to extend the visit post-period to match advertiser attribution windows (typical 7-30 days). Check `bronze.integrationprod.advertiser_configs` for per-advertiser attribution lookback.
- Augmentor_log TTL constraint becomes binding: a 30-day window plus 30-day post means we'd need 60 days of augmentor data. We don't have it. So either (a) start Phase 2a TODAY (2026-04-27 onward) and wait 30+ days for natural data accumulation, (b) materialize the biddable-holdout candidate set into a sandbox table as soon as Stage 2 runs (before TTL purge), or (c) wait for Phase 2b bidder-level work.

**Phase 2b — Bidder-level ghost bidding.** Production solution that escapes the augmentor TTL entirely. Pending Alex Bloore's decision (per Phase 1 plan §3). Zach Schoenberger and Jordan Piepkow own the implementation. **Don't build this from this chat** — it's a bidder-team project. But you should:
- Check the latest status (search Slack via `slack_bot/` extractions, or ask the user).
- If unblocked, the role for this session is to draft the data-side spec: what fields the bidder team should emit, what schema, what TTL, what join keys.

**Phase 2c — iROAS.** Per-advertiser `(incremental conversions × AOV) ÷ MNTN spend`. The number Kale and leadership actually want. Requires Phase 2a outputs + advertiser AOV from `ui_conversions.order_amt` (NOT `order_amt_usd` — that column is NULL; this is documented in MEMORY). Spend joins from `agg__daily_sum_by_campaign` aligned to the same windows. Each advertiser gets its own iROAS with CI; "MNTN-overall iROAS" is a weighted aggregate where the weights matter a lot.

## Open methodology questions worth investigating

These came up during Phase 1 but were deferred:

1. **Tighter biddable-holdout filter.** Current filter is "any augmentor_log appearance during window." Tighter options: (a) augmentor row's `mntn_segments` matched the focal advertiser's targeting envelope, (b) the row was within the campaign's intent-threshold gate, (c) the row was a real bid request for the focal advertiser. Each tightens the counterfactual but shrinks the holdout pool. Worth running once on Phase 1 data to see how much the level of the ATT shifts.

2. **Per-day subjects vs MAX-tier subjects.** Phase 1 used `MAX(household_score)` per (advertiser, IP) over the week. This collapsed peak/mid into high for 4 of 7 advertisers. Per-(advertiser, IP, day) subjects would preserve daily tier composition but introduce within-IP correlation that needs clustered SEs. For Phase 2 conversions where per-tier breakouts matter more (since conversion rates differ wildly by tier), reconsider.

3. **Northern Tool deep dive.** Phase 1 surfaced Northern Tool with guid-ATT ≈ 0 but clickpass-ATT +5.56pp. Diagnostic question: is their natural visit rate driven by brand strength / search dominance / repeat customers? If so, MNTN's room to add incremental visits is structurally limited. Cross-reference with their search SOV, brand keyword volume, or organic traffic share.

4. **Alternative MNTN-overall pooling.** The all-cells IVW pool collapses to mid-tier rates (known IVW pathology). Two valid alternatives worth computing: (a) **arithmetic mean of per-advertiser high-intent ATTs** (advertiser-equal weighting; gives ~+3.80pp), (b) **sample-size-weighted pool** across cells with a min-rate threshold (drops mid-tier from the pool). Pick one as the default headline; the IVW number remains for technical audiences.

## Files to orient on (in order)

1. `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/summary.md` — full Phase 1 execution record, stage-by-stage with timings, lessons, and known caveats. Section §9 is the execution log.
2. `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_presentation.md` — the narrative shipped in Phase 1. Read this to understand what's already been told to the team.
3. `knowledge/experimentation.md` — section "Ghost-Bidding ATT — TI-837 Application Notes" and "Stage 2 update — 7-day, 7-advertiser primary findings (2026-04-27)". Methodology lessons to reuse.
4. `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_7adv_7day.sql` — the working Phase 1 SQL. Phase 2a reuses this with `ui_conversions` swapped in.
5. `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_compute_att.py` — Phase 1 IVW + N-gate + sensitivity. Phase 2a likely needs (a) lower N-gate threshold, (b) per-advertiser AOV-weighted iROAS aggregator added.

## Operational rules (from global CLAUDE.md)

- BQ queries: use `bash .claude/scripts/bq_run.sh` and pipe SQL via stdin (the bq CLI's flag parser crashes on SQL strings starting with `--`).
- Augmentor_log 10-day TTL: confirm partition coverage via `INFORMATION_SCHEMA.PARTITIONS` before each new run. Today is 2026-04-27 — partitions back to 04-19 confirmed; by 2026-05-04 the 04-20 partition will be purged.
- Read `knowledge/strategic_north_star.md` to confirm Phase 2 still aligns with Q2 OKR priorities. If Kale's incrementality focus has shifted, flag it before running.
- Commit and push after every meaningful artifact (per `.claude/CLAUDE.md` Section 2).
- Update Jira on TI-837 at the end of each major Phase 2 sub-stage with the same wiki-markup template used in Phase 1.

## Suggested first action

Ask the user which Phase 2 path to start (2a, 2b, or 2c), then read the orientation files in the order above. Do not start writing SQL until the user confirms scope.
