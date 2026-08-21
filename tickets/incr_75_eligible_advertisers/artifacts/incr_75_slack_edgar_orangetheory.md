# Slack draft, Edgar von Trotha, Orangetheory ghost-bid test (2026-08-20)

Source: `outputs/incr_75_final_tiered.csv`, advertiser 39718 Orangetheory National, run 2026-08-19.

---

You don't need the calculator for this one. I re-ran everything on 8/19 and Orangetheory National is in the workbook I sent you, Al and Lauren. It came out Top tier.

Their numbers as of 8/19: trailing 30d spend $74k, typical month $129k, peak $426k, visit rate 1.40%.

At their normal spend an 8-week test detects about a 6.1% relative visit lift. To get to a 5% MDE they'd need roughly $190k total across the test, about $103k/mo, so they're close at their current pace already.

At the $500k they're floating they'd clear that easily, and they'd also power the conversion-rate read, which needs about $379k. That one's out of reach at normal spend, so if conversions are how they'll judge it, the bigger budget is what buys it.

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
