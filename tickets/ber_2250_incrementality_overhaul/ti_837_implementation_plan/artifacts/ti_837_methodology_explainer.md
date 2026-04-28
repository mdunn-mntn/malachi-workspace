# TI-837 Methodology — Plain-English Reference

Every concept the deck uses, explained with the actual numbers from this study.

---

## The two outcomes — and why we can measure lift on both

There's a recurring confusion: **"How can we measure lift on `clickpass_log` if it's only attributed visits?"** The answer is that we measure visits among two arms — but we don't filter `clickpass_log` to only "MNTN-attributed" visits in the holdout arm.

### What the two visit tables actually contain

| Table | What lands in it |
|---|---|
| `clickpass_log` | A visit-event row fires whenever an MNTN-served impression is followed by a visit to the advertiser site within the attribution window. **It's a row per (impression → visit) match.** Used for billing/attribution. |
| `guid_log` | A visit-event row fires whenever a known guid (MNTN-tracked household identifier) visits an advertiser's site, **regardless of whether MNTN ever served them an ad.** It's the raw "did this person visit?" signal. |

### How the lift comparison works

We measure **the visit rate per IP** in two arms:

```
served IPs (treatment)  → look up each IP in clickpass_log  → visit_rate_treated_clickpass
                       → look up each IP in guid_log       → visit_rate_treated_guid

biddable holdout IPs   → look up each IP in clickpass_log  → visit_rate_holdout_clickpass
                       → look up each IP in guid_log       → visit_rate_holdout_guid
```

For the **holdout arm**, the IPs were never served an MNTN ad — so almost no `clickpass_log` rows match (clickpass requires an impression). The holdout clickpass visit rate is essentially zero. The holdout guid visit rate captures organic visits (search, direct, other channels).

For the **treated arm**, both clickpass and guid land hits — but clickpass only counts visits MNTN's attribution chain credits, while guid counts every visit regardless of cause.

### The two ATT measurements answer different questions

| Outcome | What it measures |
|---|---|
| **clickpass-ATT** = clickpass-treated-rate − clickpass-holdout-rate | "How much more attribution-credited lift do we record for served IPs?" — what attribution claims |
| **guid-ATT** = guid-treated-rate − guid-holdout-rate | "How many more total visits happen when we serve vs not serve?" — actual causal lift |

The **wedge** = clickpass-ATT / guid-ATT tells you how much over- or under-credit attribution carries vs. true causal lift.

If wedge = 1.0×, attribution captures all causal lift, no more, no less.
If wedge = 1.5×, attribution over-credits by 50% (we claim more than we caused).
If wedge = 0.5×, attribution under-credits by 50% (real lift is 2× what we claim).

---

## Pooling methods — different ways to combine 27 advertiser ATTs into one number

We have a per-(advertiser, tier, outcome) ATT — that's 27 high-intent guid-ATT cells in this study. To produce a single "MNTN-overall high-intent guid-ATT" headline, we have to combine those 27 numbers. The four methods give different answers:

### 1. **Arithmetic mean** (advertiser-equal weighting)
Just average the 27 ATTs. Each advertiser contributes equally regardless of size.
- High-intent guid: **+4.38pp**
- Treats Sur La Table (large advertiser) the same as Overjet (small advertiser).
- Pro: each advertiser is one vote.
- Con: small advertisers with high-variance estimates get the same weight as well-powered ones.

### 2. **Median**
Sort the 27 ATTs, take the middle one.
- High-intent guid: **+2.86pp**
- Pro: outlier-robust. TurboTenant's +16.29pp doesn't dominate.
- Con: ignores volume entirely.

### 3. **Sample-size weighted**
Weight each cell by `n_treated + n_holdout`. Big advertisers' ATTs dominate.
- High-intent guid: **+5.13pp**
- Pro: answers "across all the impressions we served, what was the average lift?"
- Con: TurboTenant + Sur La Table + Ferguson dominate; small advertisers barely register.

### 4. **IVW — Inverse-Variance Weighted**
Weight each cell by `1 / variance(ATT_cell)`. Tighter-CI cells get higher weight.
- High-intent guid: **+2.69pp**
- This is the **statistically optimal** combiner under the assumption that all cells estimate the same underlying parameter.
- Pro: most precise (smallest CI on the pooled estimate).
- Con: a cell with vanishing variance (tiny ATT, tiny rate) gets enormous weight even if it's at the noise floor — and can pull the pool toward that noise floor cell. (This is how Phase 2's peak IVW collapsed to 1.0×.)

### Why we report all four

Different methods answer different questions. If they all converge (high-intent: 0.88-1.00× wedge), we have high confidence in the pattern. If they diverge (peak: IVW 1.0× vs median 0.30×), we know something interesting is happening — usually one method is being fooled by structure in the data.

---

## IVW-pooled lift — explained from scratch

**IVW = Inverse-Variance Weighted**. It's the classical meta-analysis combiner.

### Formula

Given N cells, each with estimate `ATT_i` and variance `var_i`:

```
weight_i  = 1 / var_i
pooled_ATT = Σ (ATT_i × weight_i) / Σ weight_i
pooled_var = 1 / Σ weight_i
pooled_SE  = sqrt(pooled_var)
95% CI half-width = 1.96 × pooled_SE
```

### Why the math says "use IVW" by default

If all cells are independent estimates of the same true parameter, IVW is the **minimum-variance unbiased estimator**. Any other linear combination has a wider CI.

### Why it can lie

The math assumes all cells estimate **the same parameter**. In reality, each advertiser has its own true ATT (Ferguson +10.7pp, Outback −1.2pp). We're pooling heterogeneous quantities. IVW's assumption is violated, but the procedure still runs — it just answers "what's the variance-weighted average of these heterogeneous things" rather than "what's the one true MNTN-wide ATT."

When some cells are at the noise floor (peak intent for 8 advertisers in Phase 2), those cells have **near-zero variance** and get **near-infinite weight** — they dominate the pool even though their estimates are noise.

This is why for peak/mid we report median or sample-size-weighted instead.

---

## Per-cell N-gate — what it gates

A cell = (advertiser, intent_tier, outcome). e.g., "Ferguson Home, high-intent, guid" is one cell. We have 30 advertisers × 3 useful tiers (high/peak/mid) × 2 outcomes (clickpass/guid) = up to 180 cells.

The **N-gate** filters out cells that don't have enough data to be statistically meaningful before they enter the pooled estimates. The gate threshold: **the 95% CI half-width on guid-ATT must be ≤ 0.5pp**.

In plain English: if we can't pin down this advertiser's high-intent guid-ATT to within ±0.5pp at 95% confidence, the cell doesn't contribute to the headline. Its ATT gets reported in an appendix only.

### What "0.5pp gate" means

A "pp" (percentage point) is an absolute difference between two rates. If holdout visit rate is 1.3% and treated is 7.5%, the lift is 7.5% − 1.3% = **6.2 percentage points** (pp). NOT "6.2%" — that would be a relative lift (which would be 467% in this case).

The 0.5pp gate is the precision floor we require: the 95% CI half-width must be tighter than 0.5pp. So if a cell's ATT is reported as +3.0pp ±0.7pp, it fails the gate (CI half-width > 0.5pp). If it's +3.0pp ±0.3pp, it passes.

### Phase 2 numbers

- **27 of 29** high-intent guid cells passed.
- The 2 fails: Barbara B. Mann Performing Arts (+19.8pp ±0.73pp — too few IPs to pin down precisely) and NET-A-PORTER (zero variance — degenerate cell).
- Failed cells appear in an appendix; they don't contribute to the IVW pool or the median.

---

## Leave-one-out sensitivity (LOO swing)

A robustness check: drop each advertiser one at a time, recompute the overall pooled ATT, see how much it moves.

```
For each of the 30 advertisers:
  pool the OTHER 29 advertisers' cells
  record the new pooled ATT
  swing = | new_pool − full_pool |
```

If any single advertiser's drop moves the pool by more than the headline's CI half-width, that advertiser is "driving the result" — the headline isn't really a 30-advertiser finding, it's mostly that one advertiser.

### Phase 1 vs Phase 2

- **Phase 1** (7 advertisers): Ancient Nutrition's drop swung the all-cells pool by ±1.17pp — way bigger than the CI. Ancient was effectively the headline.
- **Phase 2** (30 advertisers): largest swing observed is **±0.04pp** — smaller than the headline CI. No advertiser drives the result.

The "0 single-advertiser dominance flags" claim in the deck means: no advertiser's removal moves the headline by more than the CI half-width.

---

## The wedge — formal definition

```
wedge = clickpass-ATT / guid-ATT
```

Both numerator and denominator are ATTs for the **same** (advertiser, tier) — same subjects, same window, same denominator. Only the visit table differs.

| Wedge value | Interpretation |
|---|---|
| 1.0× | Attribution captures real causal lift exactly |
| > 1.0 | Attribution over-credits (claims more lift than caused) |
| < 1.0 | Attribution under-credits (real lift bigger than claimed) |
| < 0 | Direction disagreement (one positive, one negative) — ignore as noise |

In Phase 2:
- High-intent wedge: **0.96×** (clickpass and guid agree)
- Peak-intent wedge: **0.30× (median)** (clickpass shows only 30% of real lift)
- Mid-intent: noise floor, ratio meaningless

---

## Quick reference — Phase 2 numbers in one place

| Metric | Value |
|---|---|
| Cohort | 30 advertisers, 7-day window 2026-04-20 → 04-26 |
| n IPs total | 45.4M (high-tier across 30 advertisers) |
| Holdout visit rate (guid, sample-weighted, high-intent) | 1.33% |
| Treated visit rate (guid, sample-weighted, high-intent) | 7.54% |
| Sample-weighted ATT (guid, high-intent) | **+6.21pp absolute / +467% relative** |
| IVW-pooled ATT (guid, high-intent) | **+2.69pp ± 0.012pp** |
| Median per-advertiser ATT (guid, high-intent) | **+2.86pp** |
| Advertisers with positive lift | 25 of 27 (93%) |
| LOO max swing | ±0.04pp (no domination) |
| High-intent wedge (clickpass/guid) | 0.96× ≈ 1.0× |
| Peak-intent wedge (median) | 0.30× (clickpass under-credits 3×) |

**Heads-up:** the numbers above are from the v1 run. **v4 is the canonical run** — applies both methodology fixes (prospecting-only filter + win-rate-corrected denominator). v4 numbers are dramatically smaller:

| Metric | v1 (no fixes) | v4 (canonical) |
|---|---|---|
| HIGH guid IVW | +2.69pp | **+0.77pp** |
| HIGH clickpass IVW | +2.59pp | **+1.22pp** |
| HIGH wedge | 0.96× | **1.59× over-credit** |
| Sample-weighted high lift | +6.21pp / +467% rel | **+0.44pp / +19% rel** |
| Per-advertiser median (high) | +2.86pp | +0.56pp |
| % positive (high) | 93% | 78% |

The methodology fixes (especially the prospecting-only filter) revealed that v1's lift was inflated by retargeting impressions. True prospecting lift is modest — high-intent +0.4-0.8pp depending on pooling method. Clickpass over-credits real lift by ~60% at high intent.

Canonical tracker: `artifacts/ti_837_methodology_status.md`. Canonical SQL: `queries/ti_837_lift_analysis_30adv_7day_v4.sql`.
