# TI-835: CTV Incrementality — What the Holdout Data Already Tells Us

## Audience
Kale (Director), Alex Bloore (Engineering lead on incrementality), Paulo (VP Eng — optional). Technical-business hybrid audience. They need to understand what we found and how it shapes the shuffling experiment design.

## Power Line
**CTV ads capture attribution, not new traffic — and the data already proves it.**

---

## 1. The Setup: We Already Have An Experiment Running

Every MNTN campaign has a **10% holdout group** baked in — IPs randomly excluded from receiving ads via MD5 hash bucketing.

That means we have a natural experiment on every advertiser, right now, with no new infrastructure needed.

We looked at 9 advertisers across verticals (home, food, fashion, health, tools) over 30 days.

The question: **what happens to the people we don't show ads to?**

---

## 2. The Two Stories

We measured the same thing — holdout share of unique visitors — against two different tables. The results could not be more different.

### Story 1: Total Site Traffic (guid_log)

| Advertiser | Holdout Share |
|---|---|
| Angi | 10.0% |
| Zazzle | 10.0% |
| HexClad | 10.0% |
| REVOLVE | 10.0% |
| First Watch | 10.0% |
| Ferguson Home | 10.0% |
| Northern Tool | 10.0% |
| Clayton Homes | 9.9% |
| Ancient Nutrition | 9.8% |

**Expected if ads have no effect: 10%.** Observed: 10%.

CTV ads do not increase total site traffic. The holdout visits at the same rate as the targeted group.

### Story 2: MNTN-Attributed Visits (clickpass_log)

| Advertiser | Holdout Share | Implied Lift |
|---|---|---|
| Angi | 1.3% | **7.4x** |
| Northern Tool | 1.5% | **6.3x** |
| First Watch | 2.2% | **3.9x** |
| Zazzle | 2.6% | **3.1x** |
| HexClad | 3.2% | **2.4x** |
| REVOLVE | 3.6% | **2.0x** |
| Clayton Homes | 3.8% | **1.8x** |
| Ferguson Home | 3.9% | **1.7x** |
| Ancient Nutrition | 5.1% | **1.1x** |

**Expected if ads have no effect: 10%.** Observed: 1.3-5.1%.

CTV ads cause a 2-8x increase in MNTN-attributed visits. All 9 advertisers significant at p < 0.001.

### The Chart

![Dual Story Chart](ti_835_chart_dual_story.png)

Left panel: gray bars hugging the 10% null line. Right panel: red bars pulled dramatically left. Same advertisers, same time window, different measurement.

---

## 3. What This Means

The two stories aren't contradictory. They answer different questions:

1. **"Do CTV ads drive more people to the site?"** → No. Total traffic is the same whether you're in holdout or targeted.

2. **"Do CTV ads increase MNTN-attributed visits?"** → Yes, by 2-8x. The targeted group generates far more visits through the VV redirect flow.

**The mechanism:** CTV ads don't create new visitors — they cause existing visitors to arrive through the MNTN attribution path. The ad triggers the visit-verification redirect, which is what clickpass_log captures.

This is the incrementality story MNTN reports to clients: **not net new traffic, but captured attribution.**

---

## 4. Why This Matters for the Shuffling Experiment

The shuffling experiment (TI-837) is designed to test whether intent tier targeting produces incremental lift. Our observational findings create three design constraints:

1. **Define the metric before you build.** If the experiment uses guid_log (total visits), expect near-zero signal regardless of shuffle strategy. If it uses clickpass_log (attributed visits), expect large effects driven by the VV redirect mechanism, not by targeting quality.

2. **Per-tier analysis is currently impossible.** All scored IPs get flat HHST=10000. There are no differentiated tiers in production. The shuffling experiment depends on continuous scoring rollout.

3. **The counterfactual exists.** The 10% holdout provides a clean baseline for any experiment design. No need to create a new control group — we already have one.

---

## 5. Recommendations

1. **Share this finding with the shuffling experiment design team** — it shapes how they define success.
2. **Align on metric definition** before investing in experiment infrastructure. "Incremental" means different things depending on the table.
3. **Accelerate continuous HHST scoring** — per-tier analysis (the original goal of TI-835) is blocked until this ships.
4. **Consider adding a total-traffic metric** alongside attributed visits in client reporting — the current numbers may overstate true incrementality.

---

## Statistical Rigor

- **Method:** Binomial test (H0: targeted proportion = 0.9 under no ad effect), parametric bootstrap for 95% CIs, Benjamini-Hochberg FDR correction across advertisers
- **guid_log:** 7/9 advertisers show no significant deviation from null. 2 (Ancient Nutrition, REVOLVE) are statistically significant but with <3% lift — functionally zero.
- **clickpass_log:** 9/9 advertisers show highly significant deviation (all p < 0.001 after FDR correction). Lift ranges from 1.1x to 7.4x.
- **Sample:** 9 advertisers, 30-day window (March-April 2026), total ~30M unique guid_log visitors, ~3.5M unique clickpass_log visitors.

## Caveats

1. **Observational, not experimental.** The holdout is a natural experiment, not a randomized controlled trial. Selection effects at the audience-segment level could bias results.
2. **IP-level analysis.** One IP ≠ one person (shared IPs, VPNs, CGNAT). This adds noise but shouldn't bias direction.
3. **30-day window only.** Longer-term effects (brand lift, repeat purchase) not captured.
4. **No per-tier breakdown.** HHST is flat — can't distinguish HI from MI from PP effect.

## Links

- [Summary](../summary.md)
- [Significance Testing Script](ti_835_significance_testing.py)
- [Chart Generation Script](generate_charts.py)
- [Dual Story Chart](ti_835_chart_dual_story.png)
- [Lift by Advertiser Chart](ti_835_chart_lift_by_advertiser.png)
- [Holdout Scatter Chart](ti_835_chart_holdout_scatter.png)
