# TI-884 Methodology — How "power" is calculated, from first principles

This is the math reference for TI-884. It walks from "what is statistical power"
all the way to the per-advertiser MDE numbers in
[ti_884_top50_mde_tiers.csv](../outputs/ti_884_top50_mde_tiers.csv), with worked
examples at each step. If you only want the punchlines, see the
[deck](ti_884_power_analysis_deck_standalone.html). If you want to know exactly
how the calculator gets from "WGU has 15.6M treated IPs and a 9.66% visit rate"
to "MDE = 0.69%," read on.

---

## 1. What "power" actually means

Every incrementality experiment is a hypothesis test. We have two competing claims:

- **H₀ (null hypothesis):** the treatment did nothing. The treated group's visit
  rate equals the holdout's. Lift = 0.
- **H₁ (alternative):** the treatment moved the visit rate by some amount Δ ≠ 0.

We collect data, compute a test statistic, and decide which hypothesis to
believe. Two ways we can be wrong:

| | Truth: H₀ (no lift) | Truth: H₁ (real lift) |
|---|---|---|
| **Decide H₁** | **Type I error** (false positive). Probability = α. | Correct. Probability = power = 1 − β. |
| **Decide H₀** | Correct. | **Type II error** (false negative). Probability = β. |

We pick α (significance level) up front — conventionally 0.05. That's our
tolerance for falsely claiming a lift when none exists.

Power (1 − β) is the probability of correctly detecting a true lift of size Δ.
Conventionally we want power ≥ 0.80 — i.e., if there really is a lift of size
Δ, we'll catch it 80% of the time. **Power depends on three things:** sample
size, the size of the effect Δ we're looking for, and the variance of the
outcome.

The **minimum detectable effect (MDE)** is the smallest Δ we have power to
detect at our chosen α and 1−β. It's the inverse of a power calculation: given
fixed α, fixed power, fixed sample size, fixed variance — what's the smallest
Δ that survives the math?

That's the number TI-884 computes for every advertiser.

---

## 2. The two-proportion z-test (the test we're powering)

Our outcome is binary per IP: did the IP visit the advertiser site within the
window? Yes (1) or no (0). For each arm, we compute the visit rate:

```
p̂_t  = (treated IPs that visited) / (treated IPs total)
p̂_c  = (control IPs that visited) / (control IPs total)
```

Our point estimate of the lift is the difference:

```
Δ̂ = p̂_t − p̂_c
```

Under standard CLT assumptions (large N, independent IPs), `Δ̂` is approximately
normally distributed:

- **Mean:** the true lift Δ.
- **Variance:** sum of the per-arm variances of a sample mean of Bernoulli draws:
  ```
  Var(Δ̂) = p_t(1−p_t)/n_t  +  p_c(1−p_c)/n_c
  ```

Where does the per-arm `p(1−p)/n` come from? Each IP is a Bernoulli(p) draw.
A Bernoulli has variance p(1−p) (proof: E[X]=p, E[X²]=p, so
Var = p − p² = p(1−p)). The sample mean of n IPs has variance σ²/n = p(1−p)/n.
The two arms are independent, so variances add.

**Standard error:**
```
SE(Δ̂) = √( p_t(1−p_t)/n_t  +  p_c(1−p_c)/n_c )
```

For MDE math we approximate `p_t ≈ p_c ≈ p` (the baseline rate), so:
```
SE(Δ̂) ≈ σ · √(1/n_t + 1/n_c),    where σ = √(p(1−p))
```

That's the σ that appears in every Lewis-Rao formula in this repo.

**Test statistic and decision rule.** Standardize:
```
z = Δ̂ / SE(Δ̂)
```
Under H₀, z is approximately standard normal. We reject H₀ if `|z| > z_{α/2}`,
i.e., if `|Δ̂| > z_{α/2} · SE`. For α = 0.05, z_{α/2} = 1.96.

---

## 3. Lewis-Rao — derive the sample size

We want to find the smallest Δ such that, **when the true effect equals Δ**, our
test rejects H₀ with probability ≥ 1 − β.

Under H₁ (true effect = δ), `Δ̂ ~ N(δ, SE²)`. We reject H₀ when `Δ̂ > z_{α/2} · SE`
(taking the positive tail; symmetric argument for the negative). The probability
this happens:

```
P(Δ̂ > z_{α/2} · SE | true effect = δ)
  = P((Δ̂ − δ)/SE > z_{α/2} − δ/SE | true effect = δ)
  = 1 − Φ(z_{α/2} − δ/SE)
```

For this to be ≥ 1 − β, we need:
```
Φ(z_{α/2} − δ/SE) ≤ β
z_{α/2} − δ/SE   ≤ z_β   (where Φ(z_β) = β, i.e., z_β is negative for β < 0.5)
```

Equivalently, using `−z_β = z_{1−β}` (the upper β-quantile, which is positive):
```
δ ≥ (z_{α/2} + z_{1−β}) · SE
```

This is the **Lewis-Rao threshold:**
```
δ_min = (z_{α/2} + z_{1−β}) · SE
      = (z_{α/2} + z_{1−β}) · σ · √(1/n_t + 1/n_c)
```

For α = 0.05, β = 0.20:
- z_{α/2} = z_{0.025} = 1.96
- z_{1−β} = z_{0.80} = 0.84
- Sum = **2.80**

**For balanced arms** (n_t = n_c = n):
```
δ_min = (z_{α/2} + z_{1−β}) · σ · √(2/n)
n     = 2 · ((z_{α/2} + z_{1−β}) · σ / δ)²    ← classic Lewis-Rao "N per arm"
```

**For unequal arms** (MNTN's case — 90% treated, 10% holdout), use the SE form:
```
MDE_abs = (z_{α/2} + z_{1−β}) · σ · √(1/n_t + 1/n_c)
```

This is the form the calculator uses.

**Relative MDE.** Stakeholders care about percentage lift, not percentage points:
```
MDE_rel = MDE_abs / p
```

That 17.27% number from the calculator self-test? Comes out of:
- p = 0.05 → σ = √(0.05 · 0.95) = 0.218
- n_t = n_c = 10,000 → √(2/10,000) = 0.01414
- MDE_abs = 2.80 · 0.218 · 0.01414 = **0.00863** (0.863 pp)
- MDE_rel = 0.00863 / 0.05 = **17.27%**

---

## 4. Inverting the formula — "how big a sample do we need?"

Al's question — "what budget do we need to detect lift X?" — is the inverse
of the MDE calculation. Solve for n given target δ:

For MNTN's holdout fraction h (default 0.10 = 10% holdout, 90% treated), let
total N = n_t + n_c, with n_t = (1−h)N, n_c = hN. Then:
```
1/n_t + 1/n_c = 1/((1−h)N) + 1/(hN) = 1 / (h(1−h)N)
```

So MDE = (z_{α/2} + z_{1−β}) · σ · √(1 / (h(1−h)N)) · vr, where vr is the
variance-reduction multiplier (see §5).

Solving for N:
```
N_total = (z · σ · vr / MDE)² / (h · (1−h))
```

The calculator's `n_required_binomial(p, target_mde_abs, ...)` returns this N.

For 10% holdout: `h(1−h) = 0.09`. So you need 1/0.09 ≈ **11× more total IPs**
than a balanced (50/50) design to hit the same MDE — the SE is dominated by
the small arm.

**From N to dollars.** `spend_required(...)` chains:
```
N_treated = (1 − h) · N_total
impressions = N_treated × imps_per_ip
spend       = impressions × CPM / 1000
```

Where `imps_per_ip` (typical impression frequency per unique IP per month) and
`CPM` come from the advertiser's actual data. For the spend-curve chart we use
cohort medians.

---

## 5. Variance reduction — how the post-stack multiplier works

Each variance-reduction technique replaces SE with `SE · vr` for some `vr ≤ 1`.
They stack multiplicatively (assuming the techniques operate on different
sources of variance, which they roughly do).

### CUPED — Controlled-experiment Using Pre-Experiment Data

**Idea:** if an IP's pre-period visit rate predicts its treatment-period visit
rate, regress out the pre-period and analyze residuals. We're testing
"did treatment add anything *beyond* what the IP already does naturally?"

**Math:** define the adjusted outcome
```
Y_adj_i = Y_i − θ · (X_i − E[X])
```
where Y is the treatment-period outcome, X is the pre-period outcome (the
"covariate"), and θ = Cov(Y, X) / Var(X) is the OLS regression coefficient.

Variance of the adjusted outcome:
```
Var(Y_adj) = Var(Y) − Cov(Y, X)² / Var(X) = Var(Y) · (1 − ρ²)
```
where ρ = Corr(Y, X). So the **CUPED SE multiplier is `√(1 − ρ²)`**.

If ρ = 0 (pre-period doesn't predict post-period at all), CUPED does nothing.
If ρ = 1 (pre = post perfectly), CUPED gives infinite reduction. In practice
ρ for repeat-visit-prone domains is 0.3–0.7.

**MNTN ρ measurement (this ticket).** I pulled per-IP visit indicators
(binary 0/1) for IPs treated by Stage 1 in BOTH February and March 2026, for
3 large advertisers, then computed Pearson correlation. (For binary–binary
data, Pearson correlation equals the φ coefficient.)

| Advertiser | n IPs in both periods | ρ | √(1 − ρ²) |
|---|---|---|---|
| WGU (31357) | 10.1M | 0.461 | 0.887 |
| Vivint (30506) | 3.2M | 0.170 | 0.985 |
| Ferguson Home (31276) | 2.2M | 0.441 | 0.897 |
| **mean** | — | **0.357** | **0.934** |

Query: [ti_884_cuped_rho_measurement.sql](../queries/ti_884_cuped_rho_measurement.sql).

So MNTN's CUPED multiplier is 0.934 — a 6.6% SE reduction. Lower than the
literature midpoint of 0.866 (which assumes ρ ≈ 0.5). Driver: high binary-outcome
variance plus moderate cross-period IP retention.

### Ghost-ad conditioning

**Idea:** an experiment's intent-to-treat (ITT) population includes IPs that
were eligible to receive an ad but never got one (either we lost the auction
or had no inventory). Those IPs dilute the signal — they look like control no
matter which arm they're in.

The **local average treatment effect (LATE)** restricts to IPs that actually
won an auction in the treated arm AND would-have-won in the control arm. The
unbiased estimator is `τ_LATE = ITT / first-stage-exposure-rate`, which has
SE divided by the same factor.

For MNTN's typical first-stage exposure rate (~75% biddable retention given
TI-837's win-rate analysis), the SE multiplier is ~0.75 — a 25% reduction.

(This is the "ghost-ad" methodology of Johnson, Lewis & Reiley 2017, *Marketing
Science*. The name comes from the fact that we treat un-served eligible IPs
as if they had received a "ghost" ad.)

### Stratified randomization

**Idea:** if outcomes vary by some pre-known covariate (e.g., intent tier),
randomize *within* each stratum and analyze with stratified estimator. Variance
becomes a weighted sum of within-stratum variances, which is ≤ overall variance
when within-stratum is tighter than overall (Cochran's theorem).

Practical reduction for intent-tier strata: 10–20%. Conservative midpoint:
SE multiplier ≈ 0.85.

### Stack

```
SE_stack = SE_raw · √(1 − ρ²) · ghost_ad_mult · stratified_mult
         = SE_raw · 0.934 · 0.75 · 0.85
         = SE_raw · 0.595
```

So MDE_stack ≈ 0.595 · MDE_raw — about a 40% SE reduction. The deck's
"post-stack" numbers all use this 0.595 multiplier.

---

## 6. Worked examples

### Example A — WGU (largest top-50 advertiser)

Inputs from [ti_884_top50_mde_tiers.csv](../outputs/ti_884_top50_mde_tiers.csv)
(advertiser_id 31357):

```
treated_ips_stage1         = 15,599,393
biddable_holdout_ips_stage1 = 1,733,266   (= treated × 10/90)
p_visit                    = 0.0966       (9.66%)
σ                          = √(0.0966 · 0.9034) = 0.2955
```

**Raw MDE:**
```
SE = σ · √(1/n_t + 1/n_c)
   = 0.2955 · √(1/15,599,393 + 1/1,733,266)
   = 0.2955 · √(6.41e-8 + 5.77e-7)
   = 0.2955 · √(6.41e-7)
   = 0.2955 · 0.000801
   = 0.0002367
MDE_abs = 2.80 · SE = 0.000663 (0.0663 pp)
MDE_rel = MDE_abs / p = 0.000663 / 0.0966 = 0.687%
```

CSV says **0.69%** — match.

**Post-stack:**
```
MDE_rel_stack = 0.687% · 0.595 = 0.41%
```

CSV says **0.41%** — match.

**Interpretation:** at WGU's April scale, with a typical experiment design on
Stage 1, we can confidently detect a relative lift as small as 0.7% (raw) or
0.4% (post-stack). WGU is in the lucky position of being measurable.

### Example B — GLD (Lauren's tracker, reported lift 0.67%)

Inputs from [ti_884_lauren_validation.csv](../outputs/ti_884_lauren_validation.csv)
(advertiser_id 40586):

```
treated_ips_stage1 = 2,387,685
biddable_holdout  = 265,298    (≈ treated × 10/90)
p_visit           = 0.0326     (3.26%)
σ                 = √(0.0326 · 0.9674) = 0.1776
```

```
SE = 0.1776 · √(1/2,387,685 + 1/265,298) = 0.1776 · 0.00204 = 0.000363
MDE_abs = 2.80 · 0.000363 = 0.001016 (0.1016 pp)
MDE_rel = 0.001016 / 0.0326 = 3.12%
MDE_rel_stack = 3.12% · 0.595 = 1.86%
```

GLD's reported lift was **0.67%**. The MDE at full April scale (most generous
case) is **3.12% raw**. The reported lift is **4.7× below the detection
threshold**. Even with the post-stack 1.86%, it's still 2.8× below.

A real-world test would have run for ~4 weeks on a subset of campaigns, so
effective N is smaller and the gap is even wider.

**Conclusion:** the GLD test result is statistically indistinguishable from
zero. Whatever the 0.67% number means, it's noise.

### Example C — the spend-threshold curve

For Al's question, hold rate constant at the cohort median (p = 0.0215, IVR =
2.15%) and sweep monthly spend. At each spend level:

```
impressions = spend / median_CPM × 1000          (CPM = $24.84 → spend × 40.26)
treated     = impressions / median_imps_per_ip   (imps/IP ≈ 3.52)
holdout     = treated × 10/90
SE          = σ · √(1/treated + 1/holdout)       (σ = √(0.0215·0.9785) = 0.145)
MDE_rel     = 2.80 · SE / 0.0215
```

At $200k/month:
```
impressions = 200,000 / 24.84 × 1000 = 8,051,529
treated     = 8,051,529 / 3.52 = 2,287,366
holdout     = 254,152
SE          = 0.145 · √(1/2,287,366 + 1/254,152) = 0.145 · 0.00203 = 0.000294
MDE_rel     = 2.80 · 0.000294 / 0.0215 = 3.83% raw  ≈ 4.0% (CSV)
MDE_rel_stack = 3.83% · 0.595 = 2.28% ≈ 2.4% (CSV)
```

This is why the deck says "$200k/month" is the visit-rate threshold.

### Example D — why CVR is so much harder

Same advertiser at top-50 cohort median. Visits: p = 0.0215. Conversions:
p_cvr = 0.000552 (0.0552%). σ_cvr = √(0.000552 · 0.999448) = 0.0235.

Note `σ_cvr / p_cvr = 42.6`, vs `σ_visit / p_visit = 6.74`. The relative MDE
scales as `σ/p`, so:
```
MDE_rel_cvr / MDE_rel_visit = (σ_cvr / p_cvr) / (σ_visit / p_visit) = 42.6 / 6.74 = 6.3×
```

CVR is ~6× harder to measure than visits at the same scale, holding everything
else equal. That's why the visits-vs-CVR chart shows the gap that it does.

---

## 7. The unit of analysis question

We unit-of-randomize on **IP** (since that's what the holdout hash is keyed
on — `MD5('{advertiser_id}:{IP}') mod 1000`, validated TI-837 phase 0c).

**Outcome aggregation:** "did this IP visit at least once?" (binary). Alternative
framings (visit *count* per IP, visit-day count, etc.) would change σ and the
math. Visit count per IP would be Poisson-ish — variance scales with mean, so
σ²/μ² is similar to binary, and σ/μ is roughly 1/√mean. For typical CTV
cadences, binary aggregation is the cleanest.

**Sample independence assumption.** Lewis-Rao assumes independent draws. IPs
are not perfectly independent — a household can have multiple IPs (CGNAT
rotation, mobile, etc.). For TI-884's purposes the bias is small because:
(a) the holdout/treatment split is at the IP level, so misclassification is
randomized, and (b) the CUPED ρ measurement absorbs some of the IP-level
clustering by using the same IP across periods.

A more rigorous design would use household-level randomization (which
MNTN's identity graph doesn't currently support cleanly). That's a future
methodology improvement, not a TI-884 deliverable.

---

## 8. Assumptions, limitations, and where MDE numbers might be off

**What's solid:**
- The Lewis-Rao math (§3) is straightforward Z-test inversion; well-validated.
- The variance-reduction stack math (§5) is standard CUPED + LATE + post-stratification.
- The 10% holdout assumption is empirically validated (TI-837 phase 0c).
- The CUPED ρ is MNTN-measured (this ticket) on real data.

**What's approximate:**
- **CUPED ρ varies by advertiser.** I measured 3 (range 0.17–0.46). For TI-885
  enrollment, measure ρ per recruit before publishing per-advertiser MDE.
- **Ghost-ad multiplier 0.75 is a literature placeholder.** TI-837's win-rate
  work supports something in this range, but I haven't computed an MNTN-specific
  number for this ticket.
- **Stratified multiplier 0.85 is conservative literature midpoint.** Could be
  measured against TI-837's intent-tier data.
- **Cohort-median spend curve assumes typical advertiser.** Real advertisers
  have spread on every dimension (IVR, CPM, imps/IP). Use the per-advertiser
  table for actual decisions, not the curve.

**What we deliberately punted:**
- iROAS (revenue) MDE. Calculator API supports it via `mde_continuous`, but
  pulling per-advertiser revenue σ is a separate ticket if Al asks.
- Per-advertiser CUPED ρ for top-50. Currently using cohort mean.
- Time-clustered SEs (visits across days within an IP are correlated). Standard
  Bernoulli treatment is a slight under-estimate of variance for highly active
  IPs; in practice the bias is small.

---

## 9. Third-party comparison — Haus benchmark

Alex Knorr shared (Slack 2026-04-30) Haus's stated thresholds for valid geo
incrementality experiments:

- **500–1000 conversions per week minimum** per advertiser.
- **$10M/year cross-channel spend minimum** ("brands that spend at least
  $10,000,000 per year across all channels is where they see incrementality
  benefits"). $10M/year ≈ **$833k/month**.

Compared against this analysis:

| Threshold | TI-884 (MNTN Stage 1 only) | Haus (cross-channel) |
|---|---|---|
| Visits-rate measurable | ~$200k/month | n/a |
| Conversion-rate measurable | ~$2M/month | ~$833k/month |

Haus's $833k sits between our two thresholds because they measure full
cross-channel incrementality — richer signal, lower σ/μ — while TI-884 isolates
MNTN Stage 1 only. The 500–1000-conversions-per-week heuristic is a useful
concrete benchmark when stakeholders push back on the spend-threshold framing,
since it translates directly to a per-week N. See
[knowledge/experimentation.md](../../../../knowledge/experimentation.md) for the
full note.

## 10. References (in-repo and external)

In-repo:
- [ti_884_mde_calculator.py](ti_884_mde_calculator.py) — the math, with self-tests against the hand calc.
- [ti_884_run_analysis.py](ti_884_run_analysis.py) — applies calculator to top-50 and Lauren's 7.
- [ti_884_cuped_rho_measurement.sql](../queries/ti_884_cuped_rho_measurement.sql) — MNTN ρ measurement.
- [iroas_measurement_playbook.md](../../artifacts/iroas_measurement_playbook.md) §2 — the broader iROAS measurement framework.
- [knowledge/data_knowledge.md](../../../../knowledge/data_knowledge.md) — MNTN-specific gotchas, CUPED ρ note.

External:
- Lewis & Rao (2015), *QJE* — "On the Near Impossibility of Measuring the Returns to Advertising." The ground-truth power-analysis paper for CTV.
- Deng, Xu, Liu, Schmidt (2013), *KDD* — original CUPED paper.
- Johnson, Lewis & Nubbemeyer (2017), *Journal of Marketing Research* — canonical ghost-ad design (predecessor to Reiley paper).
- Johnson, Lewis & Reiley (2017), *Marketing Science* — exposure conditioning adds 31% precision.
- Cochran (1977), *Sampling Techniques* — stratified estimator variance theorem.

---

*TI-884 · Malachi Dunn · 2026-04-30*
