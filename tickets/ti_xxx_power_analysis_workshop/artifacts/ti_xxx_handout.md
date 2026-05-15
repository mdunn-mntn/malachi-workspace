# Power Analysis — One-Page Reference

**Power is the question you have to answer *before* you run the test — not after.**

## Three questions to ask before launching any lift study

1. **What's the metric?**
   Visits, conversions, or iROAS — each has a different MDE. At MNTN scale: 48/50 top advertisers can measure visits; 8/50 can measure CVR; 2/50 can measure iROAS. Pick the metric you can actually detect.

2. **What's the expected effect size?**
   Use prior MNTN results as the prior.
   - Retargeting at high intent: ~+21 pp on visit-rate lift. Easy.
   - Combined / all-campaigns: ~+3 pp. Detectable for most.
   - Prospecting pooled: ~+0.8 pp. Barely detectable.
   - Pure Stage-1 prospecting: ~0 pp. Undetectable at any scale.

3. **Does the advertiser's monthly spend put MDE below the expected effect?**
   If yes — run it.
   If no — pool with peers, extend the window, use a larger holdout, or *don't run the test*.

## Spend-threshold rule of thumb (visits, MNTN cohort defaults)

| Monthly spend | MDE raw | MDE post-stack | Verdict |
|---|---|---|---|
| $50k | ~8% | ~5% | borderline |
| $124k | ~5% | ~3% | well-powered (raw threshold) |
| $200k | ~4% | ~2.4% | well-powered |
| $500k | ~2.5% | ~1.5% | strongly powered |
| $2M+ | ~1.3% | ~0.7% | strongly powered |

Cohort defaults: visit rate 2.15%, CPM $24.84, 3.5 imps/IP, 10% holdout, α=0.05, power=0.80.

## Variance reduction stack

| Method | SE multiplier | Approx. effect |
|---|---|---|
| CUPED (ρ=0.357 cohort mean) | 0.934 | ~7% SE reduction |
| Ghost-ad conditioning | 0.75 | 25% SE reduction |
| Stratified randomization | 0.85 | 15% SE reduction |
| **Combined post-stack** | **0.595** | **~40% SE reduction (≈ 2.7× effective sample size)** |

## Quick math (Lewis-Rao)

```
MDE_abs = 2.80 · √(p·(1−p)) · √(1/n_t + 1/n_c) · var_reduction
MDE_rel = MDE_abs / p
```

Tiers: well-powered (<5% MDE_rel) · borderline (5–10%) · underpowered (≥10%).

## Calculator & references

- **Live calculator:** https://gist.githack.com/mdunn-mntn/34c2828f4288d123f5bfaf60f08bc244/raw/ti_xxx_mde_calculator.html
- **Workshop deck:** https://gist.githack.com/mdunn-mntn/9ba1ca32c6f7d8d38f7d4e83772e6280/raw/ti_xxx_workshop_deck.html
- **TI-917** — combined Loom: lift results + power primer + screening rule.
- **TI-884** — power & sample size analysis (the math, MNTN-measured ρ, top-50 tiering).
- **TI-933** — Select pool-or-nothing story.
- **`knowledge/experimentation.md`** — Lewis-Rao formula, variance-reduction stack, MDE rules of thumb.

## Who to ask before launching

- **Math / MDE check:** Malachi or the TI team. Plug the advertiser into the calculator first.
- **Methodology fit (CTV vs display, holdout sizing):** Zach Schoenberger.
- **Cross-product (Select, PMP, retargeting):** Mike Dolt or Bryce Wagg.
- **Vendor measurement (LiftLab, Kochava, third-party):** TI team — currently driving BER-2250 incrementality overhaul.
