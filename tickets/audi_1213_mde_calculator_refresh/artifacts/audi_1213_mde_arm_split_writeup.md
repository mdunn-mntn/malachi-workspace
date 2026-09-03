# Forecasted MDE is ~5% pessimistic: the holdout is charged for impressions

**For:** Nick Scialli · **From:** Malachi Dunn · **Date:** 2026-09-03
**Code read:** `gary-ql@cbae0e94`, `premier-ui@aaf65d59`

`computeMde` treats the spend-derived IP pool as treated **plus** control. The holdout is
never served, so the spend buys the treated arm only. Every forecast the tab shows is
`1/sqrt(1-h)` too large: **5.41% at a 10% holdout**, 11.8% at 20%.

Direction: the number is pessimistic, not optimistic. Fixing it makes tests look
*easier* to power, so the same budget clears a tighter MDE band.

## Where

Both copies of `computeMde` have it, and both call sites are live:

| Call site | File |
|---|---|
| Saved-experiment forecast | `gary-ql src/gql/types/IncrementalityExperiment/resolvers.ts` → `forecasted_mde_percent` |
| Live wizard forecast | `premier-ui src/app/scenes/Testing/ExperimentBuilder/useMdeForecast.ts` → `ForecastSidebar`, `HoldoutSection` |

## The line

```ts
const totalIps = ((monthlyBudget * durationMonths) / cpm) * 1000 / impressionsPerIp;
const nTreated = totalIps * (1 - holdoutPercent);
const nControl = totalIps * holdoutPercent;
```

`totalIps` is derived from impressions, and only the treated arm receives impressions,
so `totalIps` **is** `nTreated`. Splitting it again bills the holdout for media it never got.

## The fix

```ts
const nTreated = ((monthlyBudget * durationMonths) / cpm) * 1000 / impressionsPerIp;
const nControl = nTreated * (holdoutPercent / (1 - holdoutPercent));
const totalIps = nTreated + nControl;
```

Arm ratio stays `h/(1-h)`, which is what the holdout hash actually produces. Matches
`ti_884_mde_calculator.py` `spend_required` (`impressions = n_total * (1 - h) * imps_per_ip`),
the source of truth I sent on 2026-08-24.

## Test vector

$100,000/mo, 2 months, CPM $25, 3.5 imps/IP, 10% holdout, baseline 10.7%:

| | nTreated | nControl | mdeRel |
|---|---:|---:|---:|
| Today | 2,057,143 | 228,571 | 1.7834% |
| Fixed | 2,285,714 | 253,968 | 1.6919% |

Ratio 1.054093, exactly `1/sqrt(0.9)`. The ratio is baseline-independent, so any `p` works
as an assertion, and it is the same under either z convention.

Those two percentages are computed with your `Z_ALPHA_2 + Z_BETA = 2.80`. The Python uses
`norm.ppf` and gets 2.8015852, so the same inputs there read 1.7845% and 1.6929%. Assert on the
ratio, not the absolute percentages, unless you also match the z.

## Second bug: the "Impressions" stat is a household count

`premier-ui src/app/scenes/Testing/ExperimentBuilder/ForecastSidebar/index.tsx` renders:

```tsx
{ label: 'Impressions', value: result ? formatCount(result.totalIps) : '--' }
```

`totalIps` is an IP count. Impressions are `totalIps * impressionsPerIp`. Anyone backing a CPM
out of that stat is off by the imps/IP factor. Either multiply, or relabel it to Households.

## Two knock-ons

`HoldoutSection`'s households-withheld estimate reads `totalIps * holdout`. Under the fix
the withheld count is `nTreated * h/(1-h)`, so that number moves too. Keep them on one
derivation.

`MdeStrengthBadge` thresholds sit at 5% and 10%. A 5.41% shift moves experiments across a
band boundary near those edges, so expect some badges to change on deploy.

## Not in scope here

`DEFAULT_VAR_REDUCTION = 1` is the right default to ship. My standalone also renders a
0.595 post-stack figure; that multiplier is unverified and I am re-measuring it. Do not
port it.
