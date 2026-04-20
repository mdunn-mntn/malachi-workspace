# Incrementality Glossary — Know These Cold

Every acronym and concept in the presentation. If someone asks you any of these in a meeting, you should be able to answer in one sentence without hesitating.

---

## The Acronyms

| Acronym | Full Name | One-Sentence Definition |
|---|---|---|
| **RCT** | Randomized Controlled Trial | Flip a coin to assign treatment/control — eliminates selection bias by construction. |
| **ITT** | Intent to Treat | Compare groups as *assigned*, regardless of whether they actually saw the ad — conservative, clean, always valid. |
| **TOT** | Treatment on the Treated | Adjust to estimate the effect on people who *actually saw* the ad — what leadership wants, but harder to compute cleanly. |
| **ATT** | Average Treatment on the Treated | The average causal effect for the subgroup that actually received treatment — what ghost bidding estimates. |
| **LATE** | Local Average Treatment Effect | The causal effect for "compliers" — people whose exposure was changed by the randomization (Imbens-Angrist). |
| **DiD** | Difference in Differences | Compare the *change* in treatment vs the *change* in control — cancels out baseline differences and shared trends. |
| **BSTS** | Bayesian Structural Time Series | The statistical model under CausalImpact — builds a synthetic counterfactual from control time series using Bayesian methods. |
| **MMM** | Marketing Mix Modeling | Top-down regression of sales on spend by channel — the only method that handles all channels at once. |
| **MDE** | Minimum Detectable Effect | The smallest lift your experiment can reliably detect — compute this *before* you run anything. |
| **iROAS** | Incremental Return on Ad Spend | Revenue *caused by* the ad divided by spend — the causal version of ROAS. |
| **ROAS** | Return on Ad Spend | Revenue *attributed to* the ad divided by spend — NOT causal without a counterfactual. |
| **DML** | Double Machine Learning | Uses ML for nuisance parameters (propensity, outcome) with cross-fitting — sounds great, still fails in ads (Gordon 2023). |
| **PSA** | Public Service Announcement | What you show the control group instead of the real ad in a ghost-ad experiment. |
| **CTV** | Connected TV | TV content delivered via internet (Roku, Fire TV, smart TVs) — household is the unit, not the user. |
| **DMA** | Designated Market Area | Nielsen's 210 geographic TV markets in the US — the unit of randomization in geo experiments. |
| **SUTVA** | Stable Unit Treatment Value Assumption | "My treatment doesn't affect your outcome." Violated when ad exposure in one DMA spills into another. |
| **CUPED** | Controlled-experiment Using Pre-Experiment Data | Variance reduction technique — subtract the pre-period prediction from the outcome to reduce noise by 20-50%. |
| **IV** | Instrumental Variable | A variable that affects exposure but has no direct effect on the outcome — auction randomness can serve as an IV. |
| **SE** | Standard Error | How uncertain your estimate is — smaller SE = more precise. Driven by sample size and variance. |
| **CI** | Confidence Interval | Range around your estimate — 95% CI means if you ran the experiment 100 times, ~95 intervals would contain the truth. |
| **pp** | Percentage Points | Absolute difference between percentages — "62-115 pp error" means if true lift is 10%, estimate could be 72-125% or -52 to -105%. |

## The Concepts

### Potential Outcomes
Every household has two potential futures: Y(1) if they see the ad, Y(0) if they don't. The causal effect is Y(1) - Y(0). You only observe one. The other is permanently gone. This is the fundamental problem of causal inference.

### Counterfactual
"What would have happened without the ad." This is the Y(0) you never observe. Every measurement method is a strategy for estimating it. Without a counterfactual, ROAS is fiction.

### Selection Bias
The targeting system picks people likely to convert. So the exposed group converts more — but they would have converted more *anyway*. The ad gets credit for the targeting, not for its own effect.

### Activity Bias
Heavy users see more ads AND buy more. Exposure and conversion are correlated even if the ad does nothing. A confounder that inflates every view-through metric.

### Confounding
A third variable causes both the treatment and the outcome. Holidays boost ad spend AND sales. The correlation between spend and sales partly reflects the holiday, not the ad.

### Ghost Ads / Ghost Bidding
Run the auction for control households but don't serve the ad. Log the "would have shown" moment. Both groups are selected by targeting — only one is exposed. Removes selection bias without losing auction comparability. Google runs 100M+ ghost ads/day.

### Synthetic Control
Build a weighted combination of untreated units (DMAs, states) that matches the treated unit historically. Post-treatment gap = causal effect. What CausalImpact does under the hood.

### Parallel Trends
The key assumption of DiD: without treatment, treated and control units would have moved in sync. If this fails, DiD estimates are biased. Always plot and test.

### Statistical Power
Probability of detecting a real effect. Low power = high chance of missing a true effect and calling it null. CTV is structurally low-power because conversion rates are low and true lifts are modest (2-8%).

### Triangulation
Run experiments (geo holdouts, ghost ads) → feed results as Bayesian priors into MMM → MMM extends to all channels. Experiments provide causal truth; MMM provides portfolio-wide allocation. The modern standard.

### Adstock
Ad effects don't vanish instantly — they decay over days or weeks. Adstock models this carryover. Important in MMM: without adstock, you underestimate long-running campaigns.

### Hill Saturation
Diminishing returns on ad spend. The first million dollars of CTV spend drives more incremental sales than the tenth million. Hill function models the curve shape.

## The Key Numbers

| Number | What It Is | Source |
|---|---|---|
| **4,100% → -63%** | eBay's attributed vs causal ROI on paid search | Blake-Nosko-Tadelis 2015, Econometrica |
| **3x** | How much observational methods overestimate lift (half the time) | Gordon et al. 2019, Marketing Science |
| **62-115 pp** | Median absolute error of DML on lift, even with 5,000+ features | Gordon et al. 2023, Marketing Science |
| **41%** | Share of CTV incremental effect that shows up *after* campaign ends | Haus BFCM 2025 report |
| **~210** | Number of DMAs in the US (unit of geo experiments) | Nielsen |
| **46%** | Share of leading marketers using the MMM + experiment "trifecta" | BCG 2025 |
| **100M+** | Ghost ads Google runs per day | Johnson-Lewis-Nubbemeyer 2017 |
| **$0.90** | Good incremental ROAS benchmark (per Matt Brorby) | Internal, April 2026 |
| **$1.15** | Trade Desk's incremental ROAS (considered good) | Internal, April 2026 |

## The Papers (if someone asks "where does that come from?")

| Short Name | Full Citation | What It Proves |
|---|---|---|
| **Lewis & Rao** | "On the Near Impossibility of Measuring the Returns to Advertising" (2015, QJE) | Most ad experiments are underpowered — CIs wider than 100 pp |
| **Gordon 2019** | "A Comparison of Approaches to Advertising Measurement" (2019, Marketing Science) | Observational methods off by 3x in half of 15 Facebook RCTs |
| **Gordon 2023** | "Close Enough?" (2023, Marketing Science) | DML with 5,000+ features still has 62-115 pp error on 663 RCTs |
| **Blake-Nosko-Tadelis** | eBay paid search experiments (2015, Econometrica) | Attributed ROI 4,100%, true causal ROI -63% |
| **Johnson-Lewis-Nubbemeyer** | "Ghost Ads" (2017, JMR) | The canonical ghost-ad design — won the 2022 Weitz-Winer-O'Dell Award |
| **Brodersen et al.** | "Inferring Causal Impact Using BSTS" (2015, AoAS) | The method behind Google's CausalImpact package |
| **Abadie** | "Using Synthetic Controls" (2021, JEL) | The synthetic control methodology review |
