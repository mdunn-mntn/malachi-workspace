# Slack draft, Edgar von Trotha, Orangetheory ghost-bid test (2026-08-20)

Source: `outputs/incr_75_final_tiered.csv`, advertiser 39718 Orangetheory National, run 2026-08-19.

---

You don't need the calculator for this one. I re-ran everything on 8/19 and Orangetheory National is in the workbook I sent you, Al and Lauren. It came out Top tier.

Their numbers as of 8/19: trailing 30d spend $74k, typical month $129k, peak $426k, visit rate 1.40%.

At their normal spend an 8-week test detects about a 6.1% relative visit lift. To get to a 5% MDE they'd need roughly $190k total across the test, about $103k/mo, so they're close at their current pace already.

At the $500k they're floating they'd get to about a 3.1% MDE. That's the number that matters, because the measured lift on them so far is +9.6% relative with a 95% CI running 3.9% to 15.2%. At $500k even the pessimistic end of that range is detectable. At $190k it isn't, so the bigger budget is buying insurance against the downside case rather than just precision.

$500k also clears the conversion-rate read, which needs about $379k and is out of reach at their normal spend.

Don't use the 6/4 calculator. It had their visit rate at 0.60% against the 1.40% we measure now, so it would have told you the test was harder than it is. Refresh is ticketed as AUDI-1213.

---

## Numbers behind it (not for the Slack message)

| Field | Value |
|---|---|
| `spend_30d` | $74,451 |
| `avg_monthly_spend` | $128,874 |
| `max_month_spend` | $426,328 |
| `ivr` | 1.400% |
| `cvr` | 0.0791% |
| `mde_ivr_direct_56d_pct` | 6.080% |
| `mde_ivr_at_normal_pct` (8wk extrapolated) | 4.474% |
| `budget_for_mde_ivr_5pct` | $190,064 |
| `req_monthly_spend_ivr_5pct` | $103,178 |
| `budget_for_mde_cvr_15pct` | $378,847 |
| `can_hit_cvr_15pct_8w` | No (at normal spend) |
| `final_tier` | Top, `value_score` 60.8 |

Quoted the direct 56-day MDE (6.08%), not the extrapolated 4.47%. The extrapolation assumes reach grows linearly to 8 weeks; measured 56d/30d IP growth is 1.39x median, not 1.84x.

TI-1019 calculator had 39718 at `pVisit` 0.6028% and `cpm` $9.17, against 1.400% and $14.87 measured 2026-08-19. Both directions of error, and the spend basis differs (media_cost vs advertiser-facing).


## Update 2026-08-20, after the Lauren thread

Two different lift numbers exist for 39718 and they are not the same read:

| Source | Relative | Absolute | Note |
|---|---|---|---|
| `incr_75_eligible_with_current_lift.csv` `current_rel_lift` | +6.3% | 0.0394 pp | Entry cohort 2026-06-23 to 07-07, partner 8. Same row's `current_lift_confirms` says "flat so far" |
| `incr_75_gold_clean_ivw.csv` IVW, full window | **+9.59%** | 0.0613 pp | n_t 2,009,997 / n_h 207,751, `se_ivw` 1.8455e-4, z 3.32, 95% CI **3.93% to 15.24%** |

The "+6%" quoted in the Lauren thread traces to the entry-cohort row, which is labelled "flat so far", so it is the weaker of the two and reads as more confirmed than the field claims. The full-window IVW is the stronger evidence and moves the number up, not down.

**What this changes for the budget question.** MDE scales as 1/sqrt(budget): 5% at $190,064, so 3.08% at $500,000. Against a true effect whose 95% CI floor is 3.93%, a $500k test detects even the pessimistic end; a $190k test at 5% MDE does not. So "the more they spend the better" is right, but the specific reason is downside coverage, not extra precision.

`prior_lift_pp` is blank and `has_prior_lift` is FALSE for 39718 in `incr_75_final_tiered.csv` even though the gold IVW carries a significant result. The prior-lift join is dropping it. Worth checking before the workbook is quoted again.

Conversions are noise here: `conv_rel_lift` -1.7% on a holdout of 38 converting IPs.
