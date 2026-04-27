# TI-837 — Ghost-Bidding Lift Analysis

**Audience:** TI team
**Power Line:** *Targeting is real. Attribution overstates by 24%.*
**Date:** 2026-04-30 (TI-855 epic deadline)
**Owner:** Malachi

---

## Slide 1 — Cold open

> TI-835 told us two stories.
> ITT said: *zero lift.*
> Clickpass said: *eight times the visits.*
>
> Both can't be right.
>
> **Today we know which one is.**

(*Opener: contrast. No throat-clearing — straight into the puzzle the TI team has been living inside.*)

---

## Slide 2 — Why ITT failed, why we pivoted

ITT compared the *whole* treatment group (everyone in buckets 100-999) against the *whole* holdout (buckets 0-99). Problem: only 14-16% of the treatment group was actually served an impression. The other 84% diluted the lift to ~zero.

**Ghost-bidding ATT fixes this.** Compare only IPs that were served (treatment) against IPs that *would have been served if not for the holdout flag* (biddable holdouts proven via `augmentor_log` appearance).

Same randomization, different denominator. Same data, different question:

- ITT: *"Does being assigned to the treatment group matter?"* — answered by TI-835 with a flat zero.
- ATT: *"Does actually being served matter?"* — answered today.

---

## Slide 3 — The headline

> **MNTN's high-intent targeting causes +3.4pp of incremental visits.**
> *95% CI: ±0.02pp. n = 22 million IPs.*
> *IVW-pooled across 7 advertisers, 04-20 → 04-26 UTC.*

[chart: `ti_837_chart_mntn_overall_headline.png`]

This is the first defensible single-number incrementality estimate the TI team has had.

---

## Slide 4 — The wedge: clickpass overstates at high intent by 24%

[chart: `ti_837_chart_money_per_tier_with_wedge.png`]

| Tier | Clickpass-ATT | Guid-ATT | Direction |
|------|---------------|----------|-----------|
| **High intent** | **+4.2pp** | **+3.4pp** | clickpass over-credits 1.24× |
| **Peak intent** | **+0.55pp** | **+0.88pp** | clickpass *under-credits* 0.62× |
| **Mid intent** | +0.01pp | +0.005pp | noise floor |

Clickpass measures the visits *MNTN's attribution credits.* Guid measures the visits *that actually happened.* The gap is how much over- (or under-) credit our attribution carries.

**Two surprises** — the wedge isn't uniform. At high intent, attribution is too generous. At peak, it's too stingy. Aggregate hides both errors.

---

## Slide 5 — Every advertiser, ranked

[chart: `ti_837_chart_per_advertiser_high_intent.png`]

Six of seven advertisers show statistically significant high-intent guid-visit lift. The seventh — Northern Tool — is indistinguishable from holdout.

The spread is **200×** wide:

| Advertiser | High-intent guid-ATT | Vertical |
|---|---:|---|
| Ferguson Home | **+10.55pp** | Plumbing / Home Goods |
| HexClad | +5.08pp | DTC Cookware |
| First Watch | +4.52pp | Restaurants |
| Zazzle | +3.63pp | E-commerce |
| Ancient Nutrition | +1.76pp | Supplements |
| Clayton Homes | +1.08pp | Real Estate |
| Northern Tool | −0.05pp | Industrial Tools |

Magnitude tracks vertical fit, not advertiser size. Ferguson's +10.55pp comes from a category where intent is durable (a kitchen-faucet shopper stays a faucet shopper for weeks). Northern Tool's flat result is the contrarian data point — and the most interesting.

---

## Slide 6 — The Northern Tool moment

The chart shows seven bars. Six tell a story we expected: targeting works, with magnitude varying by vertical fit.

**The seventh tells a story we didn't.**

Northern Tool's biddable-holdout IPs visited the site at **5.90%** in the 7-day window — without ever being served an MNTN ad. The targeted IPs visited at **5.85%**.

A difference of **0.05 percentage points the wrong way.**

Yet clickpass shows Northern Tool with a +5.56pp "lift" — because every served impression that's followed by a visit gets attribution credit, regardless of whether the visit would have happened anyway.

**For Northern Tool's high-intent IPs, MNTN doesn't cause incremental visits. We capture credit for visits already happening.** That's not a methodology failure. That's the methodology working — surfacing exactly the kind of advertiser where the wedge matters most.

---

## Slide 7 — The wedge tells us where attribution is honest

[chart: `ti_837_chart_wedge_ratio_per_tier.png`]

A wedge ratio of 1.0× means clickpass and guid agree — attribution captures real causal lift, no more, no less.

- **High intent: 1.24× — modest over-credit.** Clean signal for the highest-intent IPs. Visits MNTN takes credit for are mostly visits MNTN actually caused.
- **Peak intent: 0.62× — *under*-credit.** Visits caused by peak-tier targeting often don't fire clickpass events. We're leaving credit on the table — and probably under-charging advertisers.
- **Mid intent: 2.3×, but the absolute ATTs are ~0.01pp each.** Noise floor. Don't read into the ratio.

The implication is funnel-asymmetric: at the top of the intent funnel, our attribution is too generous. In the middle, it's too stingy. Both errors exist simultaneously, and they roughly cancel in aggregate — masking each other.

---

## Slide 8 — Methodology, briefly

**Setup.** Per (advertiser, IP), the 10% holdout flag is `MD5('{AID}:{IP}') mod 1000 < 100`. Same hash as production. Per-advertiser, per-IP — so an IP can be Zazzle-holdout but Ferguson-treated.

**Pipeline.**

```
prospecting_intent (federated Parquet)
   → targetable IP × tier per advertiser, max household score over 7-day window
   → split into holdouts (bucket 0-99) and targets (bucket 100-999)

augmentor_log [04-20 → 04-27]
   → 1 scan amortized across 7 advertisers (single-query batching)
   → INNER JOIN to holdouts → "biddable holdouts" (proved eligible to bid on)

cost_impression_log [04-20 → 04-27]
   → INNER JOIN to targets → "served treatment"

clickpass_log + guid_log [04-20 → 04-30]
   → LEFT JOIN to subjects on (advertiser_id, IP)
   → 3-day post-period for cross-day visit attribution
```

**Aggregation.** Two-proportion ATT per (advertiser, tier, outcome). Inverse-variance-weighted across advertisers for the per-tier pool.

---

## Slide 9 — Caveats, honestly

Three things I'd want a methodologist to push on.

1. **MAX-household-score collapses peak/mid into high for some advertisers.** Each (advertiser, IP) is assigned its strongest observed tier across the week. For HexClad, First Watch, Zazzle, Northern Tool — virtually every targetable IP scored 10000 on at least one day, so their peak/mid tiers came back empty. The per-tier peak pool only includes Ferguson, Ancient Nutrition, Clayton Homes.
2. **The biddable-holdout filter is loose.** "Appeared in augmentor_log at all" is the floor for biddability. A tighter filter (intent threshold, HHST gate) is deferred to Phase 2. The treated arm has equivalent bias under the loose filter, so the comparison is internally consistent — but the *level* of the ATT may shift under tightening.
3. **MNTN-overall as IVW-across-all-cells is dominated by mid-tier low-rate cells.** Leave-one-out swings the all-cells pool from +0.16pp to +1.33pp when Ancient Nutrition drops out (1.17pp swing). Per-tier pools are stable; the all-cells number is a known IVW pathology — we lead with per-tier numbers instead.

---

## Slide 10 — What's next

**Phase 2a — visits → conversions.** Same pipeline, swap `ui_conversions` for `guid_log` as the outcome. Conversions are ~10-20× rarer than visits, so we'll need a longer window or accept wider CIs at high intent.

**Phase 2b — escape augmentor's 10-day TTL.** Bidder-level ghost bidding (Zach + Jordan, pending Alex Bloore decision). Without it, conversions analysis is bounded by the TTL — and conversions lag impressions by 7-30 days.

**Two different questions, both worth answering.** Attribution captures the visits MNTN can credibly claim credit for; incrementality captures the visits targeting actually causes. They're not supposed to be the same number — last-touch, view-through, multi-touch all systematically diverge from true lift, by design. The wedge is the calibration term between them, and it's worth publishing alongside clickpass so we know the size of the gap when reading attribution-driven reports.

---

## Slide 11 — The Power Line, returned

> **Targeting is real.**
> *Six of seven advertisers; +3.36pp guid lift at high intent; CI ±0.02pp.*
>
> **Attribution overstates by 24%.**
> *At high intent. At peak it understates by 38%.*
>
> The next question is whether we report both, or just one.

---

## Appendix — full per-cell table

| Advertiser | Tier | Outcome | n_treated | n_holdout | ATT (pp) | 95% CI | Pass N-gate |
|---|---|---|---:|---:|---:|---|:---:|
| Ferguson Home | high | clickpass | 2,147,811 | 984,110 | +14.962 | [+14.91, +15.02] | ✓ |
| Ferguson Home | high | guid | 2,147,811 | 984,110 | +10.551 | [+10.47, +10.63] | ✓ |
| Ferguson Home | peak | clickpass | 420,409 | 1,668,582 | +4.785 | [+4.72, +4.85] | ✓ |
| Ferguson Home | peak | guid | 420,409 | 1,668,582 | +14.369 | [+14.25, +14.48] | ✓ |
| Ferguson Home | mid | clickpass | 144,587 | 947,223 | +0.068 | [+0.054, +0.081] | ✓ |
| Ferguson Home | mid | guid | 144,587 | 947,223 | +0.018 | [+0.003, +0.032] | ✓ |
| Ancient Nutrition | high | clickpass | 1,445,221 | 1,018,702 | +2.048 | [+2.02, +2.07] | ✓ |
| Ancient Nutrition | high | guid | 1,445,221 | 1,018,702 | +1.763 | [+1.73, +1.80] | ✓ |
| Ancient Nutrition | peak | clickpass | 541,646 | 1,286,809 | +0.350 | [+0.33, +0.37] | ✓ |
| Ancient Nutrition | peak | guid | 541,646 | 1,286,809 | +0.534 | [+0.51, +0.56] | ✓ |
| Ancient Nutrition | mid | clickpass | 339,724 | 1,070,454 | +0.009 | [+0.006, +0.012] | ✓ |
| Ancient Nutrition | mid | guid | 339,724 | 1,070,454 | +0.004 | [+0.001, +0.008] | ✓ |
| First Watch | high | clickpass | 543,274 | 2,266,820 | +5.468 | [+5.41, +5.53] | ✓ |
| First Watch | high | guid | 543,274 | 2,266,820 | +4.515 | [+4.45, +4.58] | ✓ |
| HexClad | high | clickpass | 1,720,262 | 5,061,109 | +3.844 | [+3.81, +3.87] | ✓ |
| HexClad | high | guid | 1,720,262 | 5,061,109 | +5.077 | [+5.04, +5.11] | ✓ |
| Clayton Homes | high | clickpass | 510,638 | 673,756 | +1.780 | [+1.74, +1.82] | ✓ |
| Clayton Homes | high | guid | 510,638 | 673,756 | +1.084 | [+1.04, +1.13] | ✓ |
| Clayton Homes | peak | clickpass | 95,249 | 490,687 | +0.283 | [+0.25, +0.32] | ✓ |
| Clayton Homes | peak | guid | 95,249 | 490,687 | +0.189 | [+0.15, +0.23] | ✓ |
| Clayton Homes | mid | clickpass | 5,311 | 21,761 | +0.019 | [−0.018, +0.056] | ✓ |
| Clayton Homes | mid | guid | 5,311 | 21,761 | +0.019 | [−0.018, +0.056] | ✓ |
| Zazzle | high | clickpass | 2,873,747 | 5,061,359 | +5.088 | [+5.06, +5.11] | ✓ |
| Zazzle | high | guid | 2,873,747 | 5,061,359 | +3.627 | [+3.60, +3.66] | ✓ |
| Northern Tool | high | clickpass | 192,308 | 1,348,117 | +5.558 | [+5.46, +5.66] | ✓ |
| Northern Tool | high | guid | 192,308 | 1,348,117 | −0.054 | [−0.166, +0.059] | ✓ |
