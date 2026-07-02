# TI-1037 Kindred (35094) — Timing-Lag Confound Test (can we refute VV-window claim?)

## Question
Prior session: CVR "crashes" week of 2025-09-07 (~+30d after 8/08 VV-window change),
attributed to long-tail-VV aging out. Is a ~30d lag really the VV-window signature, or a
DIFFERENT event on/around 9/07?

## Method
Main prospecting group 69884 (LowPop 96108 negligible: <10 vis/day, 0-9 conv/day).
Stratify by spend regime because a Sep 10-16 spend BURST (4x volume) is superimposed.
Reason in conv/visit (CVR) and conv/1k-imp (denominator-immune), NOT blended all-day.

## Findings (REFUTES the ~30d-lag VV-aging interpretation)

### 1. No config/gate event on 9/07-9/10
archives_advertiser_configuration_archives 35094: only versions at 2025-06-07 and 2025-10-29.
ZERO flag flips (block_prospecting/conversion/1p, audience_isolation, taxonomy_block, etc.)
between Jun 7 and Oct 29. Nothing lands on the crash date.

### 2. Burst-free normal-day CVR is FLAT across the +30d mark
Normal-spend days (<=60k imp), weekly CVR:
  Aug18 8.75 | Aug25 5.94 | Sep01 7.19 | Sep08 6.94 | Sep15 6.93 | Sep22 7.55 | Sep29 9.13
No discontinuity at Sep 7-8. Sits at ~7% before AND after. The "crash" is only in the
BLENDED all-day CVR (Aug26-Sep2 burst inflated it to ~12%, Sep10-16 burst deflated to ~5.7%).

### 3. Regime-stratified pre/post: 30-59d-post CVR is HIGHER, not lower
Normal-spend CVR: pre 7.71% -> post 0-29d 7.82% -> post 30-59d 8.11% -> post 60d+ 5.43%.
The supposed "+30d crash" window (30-59d) shows the HIGHEST normal-day CVR, not a crash.

### 4. The visible drop is the Sep 10-16 spend BURST (over-scaling), not VV aging
Sep 10-16: imp ~90-145k/day (4x), CPM FLAT ~$6.1-6.4 (same price/quality), conv_per_1k
collapses to 0.08-0.22 vs ~0.5-0.7 normal. Pure volume-driven diminishing returns.

### 5. Volume-matched pre/post: no post-change conversion suppression
conv_per_1k by matched volume bucket:
  mid 45-90k: PRE 0.539 vs POST 0.716 (post HIGHER)
  high >90k:  PRE 0.502 vs POST 0.406 (comparable; CVR 9.75 vs 9.28)
Efficiency is driven by VOLUME (0.78 low -> 0.72 mid -> 0.41 high), identically in both eras.
July high-volume bursts (pre-change) already showed conv_per_1k ~0.50 = same mechanism.

## Verdict
The ~30d "lag" is NOT a clean VV-aging signature. It is a spend-burst weighting artifact:
the Sep 10-16 over-scaling burst (AUDI-1070) crashed the BLENDED metric that week. Once you
remove burst days, normal-day CVR is flat ~7% straight through the +30d mark, and 30-59d-post
CVR is actually higher than pre. This confound (over-scaling burst timed to land in the +30d
window) plausibly accounts for the observed conv/CVR "crash" INSTEAD of the VV window.
The mechanism (conversion rides on a VV) is still true, but the Kindred natural experiment
does NOT show a window-attributable CVR drop once the spend confound is netted out.
