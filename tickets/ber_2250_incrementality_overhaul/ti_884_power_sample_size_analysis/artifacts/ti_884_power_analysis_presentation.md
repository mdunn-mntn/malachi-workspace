# Power & sample size analysis — what can MNTN actually measure?

**Power Line:** *Most reported lifts at MNTN's scale are statistical noise.*

**For:** Mike Dolt, Bryce Wagg, Kale (TI leadership), Al Beretta (CS / measurement).
**Date:** 2026-04-30 · TI-884 · BER-2250 epic.
**Methodology:** Lewis & Rao (2015 QJE) + MNTN-measured CUPED ρ + ghost-ad conditioning + stratified randomization.

---

## Disruption

Last quarter, MNTN ran 7 incrementality tests for advertisers. Reported lifts: **0.57% – 1.00%**.

That looks like CTV doing what we promised. Modest but real lift. Tests done. Move on.

But did those tests have the statistical power to detect those lifts? Nobody had run the math.

So I did.

## Revelation — Three things that change the conversation

### 1. The reported lifts are below detection threshold

Three of those 7 tests ran on advertisers we still have current data for.
At April 2026 scale, with MNTN's actual visit rates, holdout sizes, and variance:

| Test | Reported lift | Required MDE (raw) | Verdict |
|---|---|---|---|
| GLD | **0.67%** | 3.12% | 4.7× below MDE → **noise** |
| Ownerly | **0.72%** | 5.92% | 8.2× below MDE → **noise** |
| Boll & Branch | **1.00%** | 88% (paused, no traffic) | **noise** |

We can't reject the null that the reported lifts are zero. Whatever those numbers represent, they aren't reliably-measured incrementality.

### 2. Visit-rate measurement works above ~$200k/month — but barely

At cohort medians (IVR=2.15%, CPM=$24.84, 3.5 imps/IP, 10% holdout):

| Monthly spend | Visit-rate MDE (raw) | Visit-rate MDE (post-stack) |
|---|---|---|
| $50k | 7.9% | 4.7% |
| **$200k** | **4.0%** | **2.4%** |
| $500k | 2.5% | 1.5% |
| $2M | 1.3% | 0.7% |

The break point is around $200k/month for raw measurement, ~$100k/month with the full variance-reduction stack. Below that, visit-rate experiments don't have power.

### 3. Conversion-rate measurement is in another league

Same scale. Same advertisers. Same variance-reduction stack. Conversions ≠ visits.

| Spend percentile | Visit MDE | Conversion MDE |
|---|---|---|
| Median (top-50) | ~3% | ~22% |
| P75 | ~5% | ~35% |
| P90 | ~8% | ~50% |

**38 of 47** top-50 advertisers (with measurable conversion data) are underpowered for conversion-rate experiments. The few that aren't (Ferguson Home, AID 34835) have either very high baseline CVR or very high spend.

To detect a 5% lift in conversion rate at the cohort median, an advertiser needs **~$5M/month** in Stage 1 spend. We have one such advertiser.

## Resolution

**For Al's question — at what budget threshold can we run an incrementality test?**

The answer is two answers:
- **Visit-rate experiments:** ~$200k/month minimum. ~$500k/month if you want to detect a 2-3% lift confidently.
- **Conversion-rate experiments:** Don't, at most spend levels. Need $2M+/month, and even then expect MDE in the 5-10% range.

**For TI-885 advertiser selection** (mid-intent ghost bidding pilot):
- Recruit only from the **48-of-50 well-powered top-advertisers for visits** (post-stack tier).
- Don't promise conversion-rate readouts for any advertiser below ~$2M/month.
- The MDE calculator is in `artifacts/ti_884_mde_calculator.py` — sample sizing for any advertiser is one function call.

**For BER-2250 stakeholder communication:**
- Stop reporting raw "Lift %" without an accompanying MDE confidence band. Anything below MDE is noise — even when it looks like a clean number.
- Re-frame the conversation: incrementality measurement is a budget question, not a methodology question. Methodology is solved (ghost bidding, Lewis-Rao, CUPED). Sample size is the binding constraint.

---

## Methodology details

> **Want the full math walk-through?** See [ti_884_methodology.md](ti_884_methodology.md) — derives Lewis-Rao from the two-proportion z-test, walks through CUPED and ghost-ad math, and shows worked examples that reproduce the CSV numbers exactly.

### How "power" is calculated, in 30 seconds

We're testing whether treated and control visit rates differ. Under standard
CLT assumptions, the lift estimator `Δ̂ = p̂_t − p̂_c` is approximately normal
with SE = `σ · √(1/n_t + 1/n_c)`, where `σ = √(p(1−p))` for a binary outcome.

**Power** = probability of rejecting H₀ (no lift) when the true lift = δ.
We want power ≥ 0.80 at α = 0.05. Inverting the test gives the smallest δ
detectable at that power:

```
MDE_abs = (z_{α/2} + z_{1−β}) · σ · √(1/n_t + 1/n_c)
        = 2.80 · σ · √(1/n_t + 1/n_c)
MDE_rel = MDE_abs / p
```

That's the entire calculation. Everything else in this analysis is plugging
in MNTN-specific values for σ (depends on baseline rate p), n_t, n_c, plus
multiplying by the variance-reduction stack.

### Variance-reduction stack (post-stack SE multiplier 0.595 = 40% SE reduction)

- **CUPED** — `√(1 − ρ²)` where ρ = correlation of pre-period visit indicator
  with treatment-period visit indicator per IP. **Measured on MNTN data
  (this ticket):** mean ρ = 0.357 across 3 large advertisers (range 0.17–0.46).
  Multiplier = **0.934**. Weaker than literature (≈0.866) — driven by high
  binary-outcome variance and moderate cross-period IP retention.
- **Ghost-ad conditioning** — restrict to biddable IPs only (won an auction
  in either arm). Removes population dilution. Multiplier ≈ **0.75**
  (Johnson-Lewis-Reiley 2017). TI-837's win-rate work supports this.
- **Stratified randomization** — randomize within intent-tier strata, analyze
  with stratified estimator. Cochran's variance theorem gives ≤ overall
  variance. Multiplier ≈ **0.85** (literature, conservative).

### Setup

- **Holdout:** 10% per-advertiser hash, validated TI-837 phase 0c (2026-04-29).
  Biddable holdout = treated × (10/90).
- **Window:** April 2026 (full month, Stage 1 only, exclude AID 90).
- **Cohort:** Top 50 advertisers by April spend ($143k – $3.35M monthly). Plus
  Lauren's 7 completed-test advertisers pulled separately (3 had measurable
  current data).
- **Unit of analysis:** IP. Outcome aggregation: did this IP visit at least
  once in the window? (binary).

### Sanity check — Lewis-Rao hand calc

At p=0.05, n_t = n_c = 10,000, no variance reduction:
- σ = √(0.05·0.95) = 0.2179
- SE = 0.2179 · √(2/10,000) = 0.00308
- MDE_abs = 2.80 · 0.00308 = 0.00863 (0.86 pp)
- MDE_rel = 0.00863 / 0.05 = **17.27%**

Calculator self-test passes this exact value. See [ti_884_mde_calculator.py](ti_884_mde_calculator.py).

---

## What's next

1. TI-885 mid-intent pilot uses the calculator to gate advertiser enrollment.
2. Lauren's tracker gets a back-filled "Power Score" column from the calculator.
3. CUPED ρ measurement gets added to `data_knowledge.md` as MNTN tribal knowledge.
4. iROAS / revenue MDE extension (calculator API ready, needs per-advertiser revenue σ pulls) lands as a follow-up if Al asks.

---

*Internal — TI-884 — Malachi Dunn — 2026-04-30*
