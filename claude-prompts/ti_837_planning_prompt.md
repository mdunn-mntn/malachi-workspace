# TI-837 — Ghost-Bidding Lift Analysis: Planning Prompt

You are picking up TI-837 (under BER-2250 incrementality overhaul) at the planning phase. A 1-day smoke test on Zazzle has run successfully; today's job is to plan how to scale from there to a finished presentation for the TI team.

This is a **planning chat**. You are NOT writing more SQL or running expensive queries. You produce a written execution plan that a follow-on chat (or Malachi himself) executes.

---

## 60-second context

TI-837 is the ATT (ghost-bidding) replacement for the failed ITT shuffling experiment. TI-835 already ran ITT and showed ~0% lift on `guid_log` (total visits) but 2-8× lift on `clickpass_log` (MNTN-attributed visits) — the "Two Stories" finding. ITT failed because of coverage dilution: 86% of the "targeted" group never got served, so any real effect washed out.

Ghost bidding fixes this by comparing only IPs that were **actually served** (treatment) against IPs that **would have been served if not for the holdout** (control). The control approximation is "appeared in `augmentor_log` during the window," proving biddability.

The 1-day Zazzle smoke test (2026-04-27) shows the methodology works:

| Tier | clickpass lift | guid lift | Read |
|---|---|---|---|
| high | +1.49pp (70×) | +1.30pp (3.4×) | Real new traffic exists |
| peak | +0.31pp (30×) | **−0.25pp** | Selection-bias artifact (likely) |
| mid | noise | noise | Sample too thin |
| weighted overall | +0.92pp | +0.65pp | Real, but ~78% of MNTN-credited visits are attribution capture |

Run cost: 18 TB / ~$90 / 10 min wall. The federated-table dry-run estimated 610 GB — wrong by ~30×.

---

## Goals the deck must support

1. **MNTN-overall incrementality estimate** — single number with CI on a representative advertiser sample.
2. **Per-tier breakout** — high / peak / mid / max_reach where applicable, with CIs.
3. **Per-advertiser breakout** — only where N is sufficient for significance.
4. **Magnitude** — point estimates + 95% CIs, not just direction.
5. **Attribution-capture commentary** — clickpass-vs-guid wedge per tier and overall.

Audience: TI team. Technical detail welcome. Final artifact is a presentation deck.

---

## Locked decisions (do NOT re-litigate)

- **Methodology = ATT ghost bidding** with `prospecting_intent` + `augmentor_log` + `cost_impression_log` + visit logs.
- **Outcome variables = `clickpass_log` AND `guid_log`** (both, always; the wedge IS the attribution-capture story).
- **Tier stratification is mandatory** — high / peak / mid / max_reach.
- **No external validation** (LiftLab, Kochava) — internal consumption only.
- **Biddable-holdout filter stays loose** (any augmentor appearance counts) — tightening deferred.
- **Variance reduction (CUPED, stratified randomization) deferred** to Phase 2 if signal warrants.
- **Advertiser pool**: 7 of TI-835's 9 with prospecting-feed data — Ferguson Home (31276), Ancient Nutrition (31455), First Watch (34143), HexClad (34611), Clayton Homes (34838), Zazzle (37775), Northern Tool (40563). Angi (32766) and REVOLVE (53308) are excluded (not in `household_scoring__prospecting_intent__v1` — same exclusion pattern as WGU, likely keyword-only advertisers).
- **Iteration discipline**: speed first, log learnings, optimize before scale; don't iterate aimlessly.

## Anti-goals (per user direction)

- Don't tighten the biddable filter — deferred.
- Don't add CUPED — deferred.
- Don't seek external validation.
- Don't chase the peak-negative finding as a separate investigation. Let it surface naturally if it persists across advertisers.
- Don't build a parameterized framework for Ryan to productionize — this is a one-shot for the deck.

---

## Open questions you must resolve in the plan

### A. Advertiser selection
- Final pick: 5, 6, or all 7? Trade-off of breadth (better MNTN-overall) vs depth (more cost-per-result).
- Vertical diversity: today's 7 cover Home Goods, Supplements, Restaurants, Cookware, Mobile Homes, E-commerce, Tools. Any over-represented? Drop any?

### B. Window strategy
- 1 day was 18 TB / $90 / 10 min. Worst-case linear scaling: 7 days × 7 advertisers ≈ 700 TB / $3.5K unless we optimize.
- Sweet spot: 1 day = noisy & peak/mid underpowered; 7 days = well-powered but expensive; 3 days = compromise.
- Should we run SHORT first across all 7, then expand only the strongest signals?
- Visit-window vs analysis-window: add a 1-3 day post-period for cross-day attribution?

### C. Aggregation formula for MNTN-overall
- Equal-weight per advertiser? Spend-weight? Sample-weight? Inverse-variance-weighted meta-analysis of per-advertiser ATTs?
- Pooled (concat all and recompute) vs aggregated (per-advertiser ATTs combined)?
- Justify the choice.

### D. Cost optimization (in priority order)
- The 18 TB scan is dominated by `augmentor_log` + visit-log scans. Options:
  - Materialize prospecting + holdout-hash intermediate to a sandbox table (requires write access — confirm permissions).
  - Column-prune `augmentor_log` to just `ip` (already done; verify reduces measured bytes).
  - Pre-aggregate visits per `(ip, advertiser_id)` into a small table once, reuse across windows.
  - Use `bronze.tpa.tmul_daily` or other aggregates as a cheaper substitute (TTL: 14 days) — feasibility unclear.
- Plan order of operations so we don't burn iterations on the wrong optimization.

### E. Per-advertiser sample-size threshold
- "Where N is sufficient for significance" needs a number. What's the threshold? Tied to the MDE that produces a useful CI for a presentation (e.g., ATT CI half-width < 0.5pp).

### F. Execution ladder
- A staged sequence: smallest validation runs first, then scale. Each step must produce a clear go/no-go output.
- Plan 4-6 stages with concrete cost + time estimates.

---

## Constraints

- **April 30 is the TI-855 EPIC deadline** (3 working days from 2026-04-27).
- Iteration cost is not the bottleneck; per-query cost and per-query time are.
- Speed of querying is the limiting factor.
- The user prefers small validation runs first to confirm method, then scale up.

---

## Files to read first

| File | Why |
|------|-----|
| `tickets/ber_2250_incrementality_overhaul/summary.md` | Epic-level context; status update at top covers the smoke test |
| `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/summary.md` | Full methodology, 04-27 smoke test, locked decisions |
| `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis.sql` | Working SQL pipeline |
| `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_plan.md` | Original methodology doc |
| `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_compute_att.py` | Python ATT computation |
| `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/outputs/ti_837_lift_zazzle_1day_2026_04_24.json` | Smoke test raw results |
| `tickets/ber_2250_incrementality_overhaul/ti_835_control_group_design/summary.md` | Predecessor ITT analysis; same 9 advertisers |
| `knowledge/experimentation.md` (section "Ghost-Bidding ATT — TI-837 Application Notes") | Reusable methodology lessons |
| `knowledge/data_knowledge.md` (sections "Canonical prospecting_intent table", "BQ dry-run unreliable on federated tables", "clickpass_log cannot do apples-to-apples holdout comparisons") | Data gotchas |
| `knowledge/strategic_north_star.md` | Why this is highest leverage |

## Key BQ tables

| Table | Use | Notes |
|-------|-----|-------|
| `dw-main-bronze.external.household_scoring__prospecting_intent__v1` | Targetable IP universe per advertiser | Federated Parquet — dry-run estimates are unreliable. 10-day rolling. Schema: ip, advertiser_id, campaign_group_id, campaign_id, household_score, year, month, day. |
| `dw-main-bronze.raw.augmentor_log` | Biddability proof | 10-day TTL. ~528 GB/day. Partitioned by `time`. Use `DATE(time)` filter. |
| `dw-main-silver.logdata.cost_impression_log` | Treatment side | Filter by advertiser_id and date. |
| `dw-main-silver.logdata.clickpass_log` | MNTN-attributed visits | No `dt` column — use `DATE(time)`. |
| `dw-main-silver.logdata.guid_log` | All visits (any source) | Use `DATE(time)`. |

## Intent tier mapping (from household_score)

| household_score | tier | Meaning |
|---|---|---|
| 10000 | high | vertical + keyword match |
| 7000-9999 | peak | vertical only |
| 3333-6999 | mid | keyword only |
| <3333 | max_reach | neither |

## Holdout hash

```js
holdout_bucket(hex_str) = uint64(hex_str[0:16]) mod 1000
// where hex_str = TO_HEX(MD5('{advertiser_id}:{ip}'))
// bucket 0-99 = holdout (10%); 100-999 = targeted (90%)
```

Already implemented as a `CREATE TEMP FUNCTION` in `ti_837_lift_analysis.sql`.

---

## What you produce in this chat

A markdown plan saved to:

```
tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_execution_plan.md
```

The plan must contain, in order:

1. **Final advertiser pick** + rationale (vertical diversity, sample size, exclusions).
2. **Window strategy** — length, exact start/end dates, post-period if any, justification.
3. **Aggregation formula** for MNTN-overall + per-advertiser-N threshold + justification.
4. **Cost optimization tactics** ordered by expected leverage; for each tactic state expected cost reduction and effort to implement.
5. **Execution ladder** — 4-6 staged runs, each with: scope, expected cost, expected runtime, go/no-go criterion, what failure looks like, what success looks like.
6. **Risks** + mitigations (data freshness, sample-size shortfalls, peak-negative replication, federated-dry-run miscalibration).
7. **Output structure** — what tables/charts feed the deck, in what shape; how per-advertiser → per-tier → MNTN-overall results compose.
8. **Time estimate** per stage and end-to-end.

## Process expected from you

1. Read the listed files. Don't re-derive what's already documented.
2. Ask Malachi clarifying questions before producing the plan, especially on aggregation formula, per-advertiser N threshold, and execution-ladder cadence. Don't ask more than ~5 — pick the highest-leverage uncertainties.
3. Produce the plan. Concrete numbers wherever possible.
4. End by stating the **first execution step** explicitly — what query, against what advertiser, for what window — so the next chat can start there with no ambiguity.

You are not running queries. The next chat does that. Your job ends with the plan.
