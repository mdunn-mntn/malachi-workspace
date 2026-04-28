# Alex Knorr — Ghost Bidding Review (2026-04-28)

Source: [ti_837_01_alex_ghost_bidding_review_2026_04_28.txt](ti_837_01_alex_ghost_bidding_review_2026_04_28.txt)

## The big methodological flaw Alex caught

**The biddable-holdout denominator is artificially large.** We're treating "appeared in `augmentor_log`" as equivalent to "would have been served an impression." But not every augmentor row converts to an impression — only a fraction (the win rate) of biddable IPs actually win an auction.

### What's currently happening

```
Targeted arm:
  100k IPs in augmentor → win rate ~20% → 20k served impressions
  treated visit rate = visits / 20k  (small denominator → high rate)

Holdout arm (CURRENT):
  100k IPs in augmentor → no win-rate filter → 100k IPs counted
  holdout visit rate = visits / 100k  (artificially LARGE denominator → LOW rate)
```

The denominators are **not conceptually equivalent**. The treated denominator
is "actually served"; the holdout denominator is "biddable." Hence the lift
(treated − holdout rate) is over-stated.

### What the fix looks like

Apply MNTN's empirical win rate (per advertiser-day, computed as
`cost_impression_rows / augmentor_rows`) as an **inclusion probability** on
holdout augmentor rows. Subsample holdout IPs at the win rate so the
denominators on both arms are equivalent ("would have been served at MNTN's
actual win rate").

This is the [Aggregate win-rate sampling (Ryan)] approach mentioned in
[summary.md line 57](../summary.md). It was deferred in Phase 1 because Matt
argued the per-event targeting signal made it unnecessary. **Alex disagrees
on review and asks us to apply it before publishing.**

### Likely impact on numbers

- Treated visit rate: **unchanged** (already conditional on being served)
- Holdout visit rate: **rises by ~1/win_rate** (denominator shrinks proportionally)
- Lift (treated − holdout): **shrinks**

Rough estimate at 20% win rate:
- Current: holdout 1.3%, treated 7.5%, lift +6.2pp
- Corrected: holdout ~6.5%, treated 7.5%, lift ~+1.0pp

The headline number could drop from +6.2pp to ~+1pp. The wedge ratio between
clickpass and guid may also shift — possibly resurrecting Phase 1's "clickpass
over-credits" story under proper correction.

### Per Alex (paraphrased)

> "We're not winning every auction. The holdout group includes 100% of biddable
> IPs because we didn't have to actually bid on them. So we could be
> underweighting the IVR of the holdout group. The denominator in the visit
> rate for the holdout is artificially large, so it's going to show a bigger
> lift."

## Other points raised

### Selection-bias from intent-score movement (peak under-credit caveat)

> "You are taking people who were assigned their max score of peak performance
> before the campaign started. There is in your post-period window … some
> movement before they are served an impression. Meaning your post-period sees
> them as peak performance, but then maybe they visit a keyword in the next two
> days and we serve that impression because now they're high intent."

The peak-intent wedge inversion (clickpass under-credits guid by 3×) could be
partially a selection artifact — peak IPs upgrading to high-intent during
the analysis window get an impression that gets credited at "high" but
they're in our "peak" subject pool. Document more prominently in caveats.

### CTV multi-advertiser confounding

People see ads from many advertisers concurrently within a CTV viewing
session. Some of the lift we attribute to MNTN could be from competitor or
co-running ads. Hard to disentangle without cross-platform exposure data —
flag as caveat.

### Add DS19 keyword + prospecting campaign check to cohort

Alex's notebook checks each advertiser had:
1. DS19 keywords in audience expressions
2. Active prospecting campaigns in the window

Add a column to the cohort table confirming both. He'll send his notebook.

### LiftLab uses geo-split

LiftLab (vendor in attribution/incrementality space) uses geo-based
experimentation: split the US into matched treatment/control DMAs, only
serve campaigns to treatment half. Alex will share their methodology video
for comparison. Different design, no augmentor dependency — interesting
alternative for Phase 2b/c.

### Conversions deferred

Confirmed not in scope for this round. Visits is sufficient.

### Meeting context

The upcoming team meeting (afternoon of 2026-04-28) is **not** a Phase 2
review — it's experiment-setup discussion for separate tickets. We have
breathing room to iterate the methodology fix and re-share before formal
review.

### Reassurance on past attribution claims

Alex initially questioned whether MNTN had been over-attributing. After
review: agreed previous "incrementality" reporting was over-stated because
it didn't account for intent groups (random holdout vs random served, not
intent-matched). Phase 2 with the win-rate fix is the correction.

## Action items in priority order

| # | Action | Status |
|---|---|---|
| 1 | **Compute per-advertiser-day win rate** = `cost_impression_rows / augmentor_rows` | TODO |
| 2 | **Apply win-rate sampling to holdout augmentor rows** (deterministic by `MD5(ip) mod 1000` < win_rate × 1000) | TODO |
| 3 | **Re-run lift analysis** with corrected denominator | TODO |
| 4 | **Update deck** with corrected numbers; rewrite headline | TODO |
| 5 | **Add intent-score movement caveat** more prominently | TODO |
| 6 | **Add DS19 keyword + prospecting check** column to cohort table | TODO (after Alex sends notebook) |
| 7 | Add CTV multi-advertiser confounding caveat | TODO |
| 8 | Send Alex updated deck before next review | TODO |
