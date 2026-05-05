# Feedback on CTV Incrementality Cheat Sheet

Edgar's doc, reviewed against MNTN priors (BER-2250 lessons, TI-748 CausalImpact, TI-884 power, TI-885 design, 10% always-on holdout, TI-835 Two Stories).

Format: comments anchored to specific cells, then a list of additions to consider. Tight enough to paste directly into the doc.

---

## Per-row comments

### Row 1 — Weekly Conversions per cell = Total / 2

The label says **weekly** but the formula divides total by 2. Two questions to disambiguate:
- Is the 500/cell floor measured *weekly* or over the *full test horizon*?
- The 50/50 split assumption should be stated. Most MNTN holdouts are 90/10 (always-on), not 50/50 — the formulas below break under 90/10.

### Row 2 — MDE = 2 / sqrt(conversions per cell)

This formula is the **2× standard error** (≈95% CI half-width on a Poisson count), which is the *detection threshold*, not the 80%-power MDE. The standard formula at α=0.05 two-tailed, 80% power is:

> MDE_rel ≈ (z_{α/2} + z_β) × sqrt(2/N) ≈ **4 / sqrt(N)**

So 600 conv/cell → ~16% MDE at 80% power, not 8%. The doc's number is optimistic by ~2×. Recommend either:
- Restate as **4 / sqrt(N)** (more honest stakeholder framing), or
- Note explicitly that the formula assumes 50% power / detection-only.

This matters: if Scout flags an advertiser as "feasible at 8% MDE" and the test runs underpowered, we're back to BER-2250 Lesson 1 ("good design ≠ good outcome"). Anchoring to 80% power up-front avoids that.

Also — this is naive A/B math. **CausalImpact / synthetic-control gets tighter MDE** because covariates absorb pre-period variance. TI-748 recovered effects of ~3-10% with BIC-selected covariates on N << this threshold. If Scout knows the test method, it can adjust.

### Row 3 — Weekly spend per geo

Aligned with **BER-2250 Lesson 3** (density > total spend). One refinement: dollars depend on CPM (CTV CPMs vary $30-50). If you can compute it, **weekly impressions per geo** or **unique IPs reached per geo** is a tighter exposure-density metric. The $5K floor will correctly flag thin-spread tests, but density is really at the impression level.

### Row 4 — Frequency

Aligned. The 2-3+ floor and 6 ceiling are reasonable. One thing missing: **reach overlap**. Frequency tells you delivery is concentrated *enough*; what we also need is that treatment and control geos look similar pre-test (matched reach, matched conversion baseline). Could add a covariate-balance check across geo cells.

### Row 5 — Prospecting weight

This is the **most load-bearing metric on the page** — directly validates BER-2250 Lesson 2 and TI-835 ("Two Stories"). Two refinements:

1. **`objective_id` is unreliable for stage classification.** Per Ray (2026-03-11), 48,934 S3 ("Multi-Touch Plus") campaigns are mis-tagged objective_id=1 from a UI bug during TV Only migration. Use **`campaigns.funnel_level = 1`** as the prospecting filter, not objective_id. Otherwise this metric is dirty.

2. **High-prospecting ≠ high-incrementality on its own.** A 100%-prospecting advertiser that's all *high-intent* prospecting will still show low lift. TI-885 finding: most MNTN advertisers target high-intent only, and tier diversity (mid + low intent share) is the better incrementality predictor. Consider replacing or augmenting with **% spend outside high-intent tiers**.

### Row 6 — Conversions per $1K

Aligned with **TI-884** (rare conversions need bigger samples). Two clarifications:

1. **Per which event?** Visits are 10-20× more frequent than conversions (TI-884). Same advertiser passes on visits, fails on conversions. Recommend running this for **each tracked event** Scout has access to, with the floor calibrated per event type.
2. **BER-2250 Lesson 4** — primary KPI may not be where lift shows. If Scout only checks the configured primary KPI, it misses advertisers where retail / repeat / downstream events would carry the signal. Compute for ≥3 event types if available.

### Row 7 — CTV share of spend

Aligned, but **scope this to test type**. CTV share matters for *aggregate / pre-post / synthetic-control* designs (you need CTV to be detectable above other-channel noise). For a true randomized geo holdout where control geos see CTV-off and treatment sees CTV-on, all-else-equal, CTV share matters less because the contrast is engineered in. Worth tagging which test type each metric assumes.

### Row 8 — Minimum test duration = window × 2

Aligned with **BER-2250 Lesson 5** and **TI-885** (short tests fail). One adjustment:

- TI-885 standard is **6 weeks active + 2 weeks post-treatment**, regardless of conversion window.
- For a 14-day window, "× 2" gives 28 days = 4 weeks — **shorter than MNTN's standard**.
- For a 30-day window, "× 2" gives 60 days ≈ 8.5 weeks — fine.

Recommend a floor: `max(window × 2, 6 weeks active) + 2 weeks post`. Also, **TI-748 found a 4-week post-launch ramp-up window** where new-campaign noise dominates — exclude that from the analysis (separate from the 2-week post-treatment window).

---

## Stop-the-test rules

All five are good. One addition: **stop if pre-period covariates are unavailable** (no ≥26 weeks of stable advertiser-level pre-data in `sum_by_campaign_by_day`). Without covariates, you fall back to naive pre/post, which we've seen produce false-positive lift (per `feedback_no_naive_pre_post.md` — spend changes alone fake lift).

Also — rule 5 says "CTV < 5% AND can't isolate geos." Worth specifying: if you *can* isolate geos (geo-holdout test), CTV share matters less. The rule should be "CTV < 5% **and** test type is aggregate / non-randomized."

---

## Recommended additions

These are feasibility checks I'd want Scout to compute *before* the table above, since they gate whether any of the formulas are even valid.

### A. Pre-period data availability — gates CausalImpact-style tests
- **Metric:** weeks of stable pre-period spend ≥ 26 (min) / 52 (ideal).
- **Source:** `sum_by_campaign_by_day` (back to 2024-01-01). **Don't use `agg__daily_sum_by_campaign`** — only goes to Sep 2025.
- **Why:** TI-748 — covariate-adjusted lift estimation needs full seasonality. Without it, no CausalImpact / synthetic control.

### B. KPI steady-state stability
- **Metric:** coefficient of variation of weekly KPI in pre-period < ~0.5; weeks with <1,000 impressions excluded.
- **Why:** TI-748 + TI-885. High-CV KPIs make tight MDE infeasible regardless of conversion volume. Low-impression weeks produce extreme rate values that destroy variance estimates.

### C. 10% always-on holdout viability (ITT) — should run BEFORE recommending external test
- **Metric:** holdout-bucket conversion count per advertiser per week.
- **Why:** every MNTN advertiser already has a 10% IP-level holdout (Zach 2026-04-30 — `MD5('{AID}:{IP}') mod 1000 < 100` = holdout, per-advertiser per-IP). This is the **cheapest possible lift test** and Scout should rank it ahead of recommending a geo holdout. If ITT MDE is sufficient on the existing 10%, no external test is needed. The geo-holdout machinery should be reserved for advertisers where ITT is too thin.

### D. Attribution-incrementality tension flag
- **Metric:** if advertiser is top-quartile attribution (high VVR, high IVR, low CPV) AND prospecting weight < 60% → flag *low expected incrementality even if power is fine*.
- **Why:** TI-835 "Two Stories" — high-attribution audiences are systematically *lower* incrementality. Stakeholder framing: "we can detect lift, but expect it to be small" is a better message than running a green-lit test that returns 1%.

### E. Multi-KPI breadth
- **Metric:** advertiser tracks ≥3 distinct event types.
- **Why:** BER-2250 Lesson 4 — CTV impact often appears outside the primary KPI. Single-event tests under-detect.

### F. Spend stability across the test window
- **Metric:** coefficient of variation of weekly spend through the planned test < ~0.3.
- **Why:** TI-748 — pause/scale events corrupt CausalImpact. Pre-flight: confirm advertiser isn't in a budget-flux period.

### G. CTV vs display isolation
- **Metric:** explicit `channel_id = 8` filter (CTV) vs `channel_id = 1` (display) — `bronze.integrationprod.channels` is authoritative.
- **Why:** the doc title says "CTV incrementality" but a campaign group can mix CTV + display. For pure CTV lift, exclude display from spend baseline. Don't trust string-match on placement names.

### H. Covariate computability — gates CausalImpact
- **Metric:** can `metric_lag1`, `spend_change_pct`, and platform-wide covariates be computed for this advertiser?
- **Why:** TI-748 used per-advertiser BIC selection. Brand-new advertisers fail this; default to simpler design (or wait).

---

## Operational gotchas Scout should encode

These are dirty-data traps that will silently corrupt the metrics above:

1. **`objective_id` unreliable** — use `funnel_level` for stage. (Already noted in Row 5.)
2. **`fpa_advertiser_verticals.advertiser_name` is stale** — 79-82% of new advertisers since 2025-12-23 have empty name. Always JOIN to `advertisers.company_name` for current name + vertical.
3. **WGU (AID 31357) is ~30% of MNTN monthly spend.** If Scout normalizes anything cross-advertiser (e.g., percentile ranks), WGU dominates. Filter or weight.
4. **Uniques in `agg__daily_sum_by_campaign` are unreliable** — VVR computed off this table is wrong. Use `sum_by_campaign_by_day` or compute from raw.
5. **`agg__daily_sum_by_campaign` starts 2025-09-01**, not 2025-01-01 (despite GCP data floor). Use `sum_by_campaign_by_day` for any pre-period > 6 months.
6. **WGU's attribution window is the only known >90-day exception.** For >99% of advertisers, 90-day lookback is sufficient.

---

## Summary recommendation

The table is a strong v1 — covers power, density, audience, duration, and isolation. The biggest single fix is **Row 2 (MDE formula)**: rewrite to 80%-power-aware (~`4/sqrt(N)`), or note explicitly that it's a detection threshold. After that, the highest-value additions are **(C) ITT viability on the existing 10% holdout** and **(A) pre-period availability** — they gate whether any of the table's metrics are even applicable for that advertiser.

If 3+ rows fail, the doc says "build scale first." That's right for a one-shot test but misses the cheapest path: **the 10% always-on holdout already exists — measure that first, externally test only when ITT is too thin.**
