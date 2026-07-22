# Gruns — CTV Prospecting Incrementality (CGID 126905, excludes high intent)

**Ticket:** AUDI-1148 (spike) · **Related:** INCR-75 · **Advertiser:** Gruns (42097) · **Prepared:** 2026-07-22

Stakeholder request (Kirsa, incrementality team): performance metrics + incremental visit
lift for the Gruns CTV prospecting campaign that excludes high intent. Companion to the
workbook `INCR-75 Gruns Incrementality CGID 126905.xlsx`.

---

## The campaign

Gruns campaign group **126905** ("CTV Prospecting TOFU High DMA") — a Connected-TV prospecting
group whose prospecting audience **excludes high intent**. Top-of-funnel, high-population DMAs,
live Jun 8 – Aug 1, 2026, $2.50 cost-per-visit goal. CTV-only in practice (the paired display
and "Ego" campaigns never delivered). Runs on the Beeswax bidder.

## Performance (flight to date, Jun 8 – Jul 22)

| | Impressions | Reach (HHs) | Spend | Visits | Visit rate | CPV |
|---|---|---|---|---|---|---|
| Prospecting (excl. high intent) | 440,919 | 348,769 | $9,576 | 835 | 0.19% | $11.47 |
| Campaign group total | 1,519,469 | — | $33,252 | 1,543 | 0.10% | $21.55 |

Visits = MNTN view-through visits (industry-standard lens; corroborated by the visit pixel log).
The **~0.2% visit rate and ~$11 CPV are low by design** — excluding high intent removes the users
who would visit anyway, so raw performance is weaker while incremental value is higher. Conversions
are sparse at this top-of-funnel stage, so visit rate (not conversions) is the meaningful KPI.

## Incremental visit lift (ghost-bid holdout)

Active window **Jun 24 – Jul 14** (entry-cohort method; drops the left-censored first logging day).

| Group | Prospects | Visits | Visit rate |
|---|---|---|---|
| Ad served (treatment) | 207,323 | 213 | 0.1027% |
| Holdout (no ad) | 21,309 | 19 | 0.0892% |

**Relative visit lift = +15%** (absolute +0.0001357), **95% CI [−32%, +63%], p = 0.53.**
Directionally positive but **not statistically significant** — the holdout produced only 19 visits.
This reproduces the incrementality pipeline's numbers exactly.

**Why it can't be confirmed on this campaign, and won't be soon:** the limit is holdout visit
count, not window length. The holdout is a fixed ~10% platform-wide, and 10% of a 0.1%-visit-rate
campaign yields ~1 holdout visit/day, so running to the Aug 1 flight end takes us from ~19 to ~29
holdout visits — still far short. The table only reaches back to Jun 22 (raw feed ~10-day
retention), so the Jun 8–21 launch period is unrecoverable.

## Does excluding high intent improve incrementality? (the well-powered answer)

Yes, directionally — but the evidence is **platform-wide**, not from this one campaign. Across
100M+ IPs (all advertisers, clean holdout), relative incremental visit lift by intent band:

| Intent band | Incremental lift | Read |
|---|---|---|
| High intent (blocked here) | +0.2% | Incrementally dead — visits anyway |
| Prime prospect | +1.6% | Some lift |
| Mid intent | +3.3% | Carries the lift |
| MaxReach (low intent) | +3.4% | Carries the lift |
| No score (untargeted reach) | +0.1% | Incrementally dead — reach only |

So blocking high intent **should** improve incrementality — provided the freed spend flows to
**mid-intent** prospects, because untargeted reach is incrementally dead too. This single Gruns
campaign is directionally consistent with that, just too small to prove on its own.
