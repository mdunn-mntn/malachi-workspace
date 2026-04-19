# Measuring iROAS at a CTV DSP: a Ranked, Feasibility-Scored Playbook

**Author:** Malachi Dunn (research compilation)
**Date:** 2026-04-19
**Context:** BER-2250 Incrementality Overhaul — reference artifact for causal method selection
**Related:** TI-835 (observational analysis, complete), TI-837 (implementation plan), knowledge/experimentation.md

---

**Bottom line up front.** For a CTV DSP with billions of monthly impressions, a hybrid household identity graph, advertiser pixels, a BigQuery/Databricks/Greenplum stack, and *no* bidder-side PSA logging today, the only scientifically defensible path to a portfolio-wide iROAS number in the next 90 days is **randomized geo holdouts analyzed with Augmented Synthetic Control (Meta GeoLift / Google TBR)**, triangulated over 12-18 months with **household-level RCT lift once bidder-side ghost-bid logging is built**, and calibrated into a **Bayesian hierarchical MMM (Google Meridian)** as the production layer that produces the iROAS number advertisers see. Every other method on the list is either (a) a supporting tool, (b) scientifically weak for headline iROAS but useful for targeting, or (c) a vendor wrapper around one of these three primitives. The dominant evidence base -- Lewis & Rao 2015 QJE, Gordon et al. 2019 and 2023 Marketing Science, Blake-Nosko-Tadelis 2015 Econometrica, Shapiro-Hitsch-Tuchman 2021 Econometrica, and Johnson/Lewis/Nubbemeyer 2017 JMR -- is unambiguous: **observational match-back iROAS has median error of 60-115 percentage points vs. RCT truth**, and **no amount of ML closes that gap**. Plan accordingly.

---

## Executive summary: the ranked list

1. **Geo randomized holdouts + Augmented Synthetic Control (GeoLift / Meridian-calibration pattern).** Feasible today, privacy-durable, walled-garden-agnostic, and the only rigorous method that runs on your stack in Q1 without bidder changes.
2. **Household-level RCT lift with ghost-bid logging (intent-to-treat + LATE via Johnson/Lewis/Nubbemeyer).** The scientific gold standard for CTV; blocked today by the confirmed absence of bidder PSA logging. Build it in Phase 2.
3. **Bayesian hierarchical MMM calibrated by lift-test priors (Google Meridian).** The production surface for per-advertiser iROAS once (1) and (2) supply causal priors; weak alone, strong when calibrated.
4. **CausalImpact / BSTS for single-market switch-ons and always-on campaigns (Brodersen et al. 2015).** Already in-house from the prior targeting-algorithm analysis; keep using, but know its SUTVA limits.
5. **Switchback experiments (Bojinov, Simchi-Levi, Zhao 2023 Management Science).** The right answer when geo spillover is severe or when an advertiser refuses a holdout; complementary to (1).
6. **Staggered-adoption DiD with Callaway-Sant'Anna / de Chaisemartin (2020-2021).** For measuring targeting-feature or pricing-policy rollouts across advertisers; avoid canonical TWFE.
7. **IV via random auction variation / throttling (Waisman, Nair, Carrion 2024; Gui, Nair, Niu 2022).** The cheapest path to *something* causal without building PSA logging -- exploit bid-shading or reserve-price jitter as an instrument.
8. **Regression discontinuity at frequency-cap and reserve-price thresholds (Calonico-Cattaneo-Titiunik 2014).** Narrow, local, but nearly free once cap logs exist.
9. **Uplift / CATE modeling (meta-learners, GRF) -- for targeting, not iROAS.** Useful as a bid-multiplier layer once experimental data exists; headline iROAS claims from uplift models are not defensible.
10. **Double ML, TMLE, PSM/IPW on observational exposure.** Use only as internal diagnostics, never as the number you report. Gordon et al. 2023 "Close Enough?" shows median absolute error of 62-115 percentage points on lift even with 5,000+ covariates and deep-learning DML.

**90-day pilot recommendation.** Stand up a **DMA-randomized geo holdout pilot on 6-10 performance advertisers with >=\$500k/month CTV spend**, analyzed in parallel with (a) classical Geo-Based Regression (Vaver-Koehler 2011) and (b) Meta GeoLift's augmented synthetic control in R on Databricks, with a pre-registered MDE of 5-10% on revenue; use the results as informative ROI priors into a Meridian proof-of-concept for three of those advertisers.

---

## Part 1. Ranked method deep-dives

Each method gets the required eight-section treatment. Feasibility scorecards are Green (ready), Yellow (work needed), Red (blocked by stack or science).

---

### Method 1 -- Geo randomized holdouts + Augmented Synthetic Control

**1. Name and definition.** Randomize DMAs (or commuting zones) into test and control, then estimate the counterfactual control outcome for test markets using an augmented synthetic control and compute iROAS = incremental KPI / incremental CTV spend.

**2. How it works.** Assign geos to arms either by stratified random sampling (LiftLab-style, Finance-auditable) or by algorithmic market selection over pre-period fit (Meta GeoLift). For each test DMA, construct a synthetic control as a weighted combination of donor DMAs whose weights minimize pre-period distance on the outcome and covariates; Abadie-Diamond-Hainmueller (2010, JASA) restricts weights to the simplex; Ben-Michael, Feller and Rothstein's (2021) **Augmented SCM** adds a ridge outcome-model bias correction and permits modest extrapolation when the treated unit sits outside the donor convex hull -- exactly the situation for NY, LA, Chicago. Inference uses conformal inference (Chernozhukov, Wuthrich, Zhu 2021) in GeoLift's default or placebo permutation tests in classical ADH. The headline estimator is the average treatment effect on the treated (ATT): **tau_hat = (1/T_post) sum_{t>T0} (Y_treated,t - sum_j w_hat_j Y_j,t)**, and **iROAS = sum tau_hat_t / delta_spend**.

**3. Scientific validation.** Synthetic control is one of the most-cited methods in modern causal inference; Abadie's 2021 *Journal of Economic Literature* review formalizes its assumptions. ASCM (Ben-Michael et al., JASA 2021, arXiv 1811.04170) is the modern default. Vaver and Koehler's 2011 Google Research paper *Measuring Ad Effectiveness Using Geo Experiments* and Kerman, Wang and Vaver's 2017 *Estimating Ad Effectiveness Using Geo Experiments in a Time-Based Regression Framework* (research.google/pubs/pub45950) pioneered the DSP/ad-measurement application. Meta's GeoLift (github.com/facebookincubator/GeoLift) productized ASCM for ad lift in 2021; Google's Meridian (github.com/google/meridian, GA Jan 2025) embeds the same logic as a prior-calibration target. The core caution comes from Lewis & Rao's 2015 QJE "On the Near Impossibility of Measuring the Returns to Advertising," which showed that even with 25 RCTs totaling millions of users, median standard error on ROI was 26.1% for retail and 115% for brokerage -- **confidence intervals wider than 100 percentage points**. Geo designs inherit this power problem, but DMA-level aggregation actually reduces the noise relative to user-level methods by pooling variance.

**4. CTV-specific considerations.** Geo is a near-perfect match for CTV because (a) the bidder can target and report at DMA/postal-code level, (b) walled-garden supply (Disney, Netflix via Microsoft, Amazon) participates whether or not the advertiser has clean-room access, and (c) no persistent user ID is required. The main CTV-specific risk is **spillover**: roughly 5-15% of CTV impressions are mis-geolocated via IP (VPN, mobile hotspot, streaming to another market), and co-viewers may straddle DMA boundaries. Haus explicitly markets **Commuting-Zone aggregation** (GeoLift offers the option) as the mitigant, collapsing adjacent DMAs with high mobile-GPS commute overlap into single units. Co-viewing does *not* bias geo estimators as long as the household is the unit of exposure *and* the KPI is aggregated at the household -- but if the numerator is individual-level conversions it inflates impressions and mechanically depresses iROAS.

**5. Industry adoption.** Haus (haus.io), Measured (measured.com), LiftLab (liftlab.com) and Recast (getrecast.com) all productize geo holdouts for CTV; Haus claims "up to 4x better precision" vs matched-market tests and has Susan Athey on its advisory brain trust. Measured's 2025 CTV Insights Report (274 experiments across 60 enterprise brands) reports median CTV iROAS of $2.88 vs Meta $2.30 and Google $2.39. Tatari cites Haus and Measured as the "undress the magician" auditors of view-through attribution. GeoLift and Meridian are open-source; so are `augsynth` (R) and `SparseSC` / `pysyncon` (Python).

**6. Data and infrastructure requirements.** A geo x date panel (210 DMAs x 90 days = 19k rows) with columns: `geo_id, date, treated, spend_ctv, spend_other, impressions, reach_households, conversions, revenue`. Pre-period of at least 25 weeks required for GeoLift; Meridian wants 2-3 years weekly. Source: bid-stream logs in BigQuery joined to advertiser-provided conversion pixels and the verified-visit layer. Tables needed: `impressions_raw` partitioned by date, `households_resolved` (graph output), `conversions_pixel` (per-advertiser), `dma_lookup`, `commuting_zone_lookup`.

**7. Feasibility scorecard for the stack.**
- Data availability: **Green**. Geo is in IP enrichment; DMA rollups are trivial in BigQuery.
- Experimentation plumbing: **Green**. You can already split at DMA/ZIP level per the brief.
- Statistical power: **Green at DMA, Yellow at ZIP**. GeoLift detects 2-10% lifts with 15-30 test DMAs and 4 weeks of pre-period on well-behaved DTC KPIs; ZIP has insufficient per-unit impression volume for most advertisers.
- Analyst skill ramp: **Yellow**. Reader has CausalImpact experience, which is 80% of the mental model; GeoLift is R-only and its power calculator requires a few days to internalize.
- Stakeholder interpretability: **Green**. "We turned CTV on in half of DMAs and compared sales" is a narrative sellers, finance, and advertisers all understand.

*What we'd need to build:* A geo-panel data mart in BigQuery refreshed daily, a GeoLift wrapper running on Databricks (R runtime or a Python port via PyMC-Marketing's lift-test likelihood), a commuting-zone lookup table, and a dashboard that surfaces ATT, iROAS, and conformal prediction intervals per advertiser. Sample-size calculator should use GeoLift's power simulator backfilled with 52 weeks of the advertiser's historical revenue.

**8. Edge cases and gotchas.** Selection bias in which DMAs the advertiser agrees to hold out (usually insists on keeping top-revenue markets live -- stratify and force random assignment). SUTVA failures from cross-DMA spillover (commuting zones mitigate, not eliminate). Measurement-window bias: ad spend shifts *toward* control markets if the advertiser reallocates other-channel spend (use intention-to-hold-out, not realized spend, as the treatment indicator). Co-viewing inflates HH impressions but does not bias geo ATT as long as KPI is consistent. Seasonality can swamp small campaigns in Q4 -- Measured explicitly warns; use longer pre-periods and ASCM with ridge augmentation. Novelty/halo effects decay over 2-4 weeks post-exposure; measurement window should be >=4 weeks. Cookie/ID loss is irrelevant because geo is identity-agnostic. Finally, geo estimators' CIs can be **anti-conservative** with heterogeneous treatment effects and cross-geo correlation -- cluster-robust SEs at DMA and conformal inference are non-negotiable.

---

### Method 2 -- Household-level RCT conversion lift with Ghost Ads

**1. Name and definition.** Randomize households into treatment and control at the bid-decision level; control households are kept out of the auction but the would-have-won moment is logged as a "ghost" impression; compute ITT and LATE.

**2. How it works.** Johnson, Lewis and Nubbemeyer (2017, *Journal of Marketing Research*, DOI 10.1509/jmr.15.0297; SSRN 2620078) formalize the estimator. For each bid request, hash(household_id, campaign_id, salt) mod N assigns an arm; treatment arm proceeds through the auction as normal; control arm is suppressed *but the auction outcome is logged* -- a "ghost impression" marking the moment the household would have been exposed. The ITT estimator is the mean KPI difference across all assigned households (exposed or not). Because ~50% of assigned households are never actually exposed (they lose the auction, the user never opens the app, frequency caps hit), filtering to the subset that was actually exposed in treatment *and* would-have-been-exposed in control recovers the **treatment-on-treated / LATE / CACE** effect -- Imbens & Angrist's (1994) Wald estimator **tau_LATE = ITT / first-stage exposure rate**. Johnson, Lewis and Reiley (2017, *Marketing Science* 36(1):43-53, "When Less Is More") show this filtering delivers a **25% SE reduction = 31% more precision**, equivalent to growing a 3.1M-user experiment to 5.3M. Google's ghost-ads pipeline runs >100M predicted-ghost-ads per day; Meta's Conversion Lift uses the same primitive inside the walled garden.

**3. Scientific validation.** This is the academic gold standard -- the Johnson-Lewis-Nubbemeyer paper won the 2022 Weitz-Winer-O'Dell Award. Gordon, Zettelmeyer, Bhargava and Chapsky (2019, *Marketing Science* 38(2):193-225) used 15 Facebook RCTs of this design, 500M user-experiment observations and 1.6B impressions, to benchmark every observational alternative; observational methods were off by a **factor of three in half the studies**. The 2023 follow-up Gordon-Moakler-Zettelmeyer "Close Enough?" (*Marketing Science* 42(4):768-793, arXiv 2201.07055) tested 663 Meta RCTs against deep-learning DML with 5,000+ user features and still found **median absolute error of 115%, 107% and 62% on upper, mid and lower funnel lifts vs true RCT values of 28%, 19% and 6%**. Blake, Nosko and Tadelis's (2015) *Econometrica* eBay paid-search experiments showed OLS attribution ROI of **>4,100% when the true causal ROI was -63%** -- the observational-causal gap is not a nuance, it's a sign-flip. Lewis & Rao (2015) QJE is the reality check on how *much* data is needed: for a campaign at $0.14/user cost with sigma/mu = 10 on weekly sales, **distinguishing +25% ROI from breakeven at 80% power requires roughly 1.42M users per arm**; distinguishing +50% from 0% needs 9x more data.

**4. CTV-specific considerations.** The randomization unit must be the **household**, not the user, because CTV delivery is at the TV device tied to an IP-approximated household. Co-viewing inflates within-household outcomes but does not bias household-level ITT -- provided the conversion numerator is also aggregated to the household via the identity graph. Walled gardens are a hard boundary: you cannot ghost-bid into Disney, Netflix, or Amazon-sold inventory because the auction happens on the publisher side; lift studies inside those walled gardens must use their clean-room facilities (AMC, ADH, Disney Clean Room). Applying Lewis-Rao's formula to a **typical CTV campaign of 10M impressions at $25-$50 CPM, frequency =5, ~2M households**, assuming household sigma/mu = 10 and $0.125 per-HH cost, the MDE at 50/50 split and 80% power is a **$0.28 lift per household per week** -- right at break-even iROAS. Stacking CUPED (20-50% variance reduction), ghost-ad conditioning (25-31%), and stratified randomization on pre-period sales (10-20%) compounds to ~40% SE reduction, turning a 10M-impression campaign into one with modest power to detect a 2% sales lift. Below 5M impressions the honest statement is "directionally consistent with positive ROI" -- not a point estimate.

**5. Industry adoption.** The Trade Desk markets "Ghost Bids" for conversion lift (thetradedesk.com/resources/best-practices-for-better-conversion-lift); Viant markets the same under "Household ID" (viantinc.com/insights/blog/what-are-ghost-bids-and-ghost-ads); Meta Conversion Lift and Google Conversion Lift are the original platform-embedded versions. Amazon Marketing Cloud supports holdout-based lift inside AMC. Outside the majors, most DSPs (including, per the brief, this one) do not have bidder-side ghost logging; the gap is a well-known competitive deficit.

**6. Data and infrastructure requirements.** Critical new logs: `(bid_request_id, household_id, arm_assignment, win_flag, suppressed_flag, would_have_won_flag, ghost_impression_flag, timestamp, campaign_id, line_item_id, creative_id, auction_context)`. Household ID must be deterministic from the identity graph before the auction decision (not post-hoc). Randomization via deterministic hash. Outcome data from advertiser pixels joined to household via the same graph. All of this lives naturally in BigQuery partitioned by date and clustered by household_id; Databricks runs the variance-reduction pipelines (CUPED, stratification, bootstrap inference).

**7. Feasibility scorecard.**
- Data availability: **Yellow** -- have bid-stream and HH IDs; lack ghost-impression logs.
- Experimentation plumbing: **Red today, Green after Phase-2 build**. Bidder-side PSA/ghost logging is confirmed missing.
- Statistical power: **Yellow**. With 10M impressions per campaign and household sigma/mu =10, detectable lift is ~2-3% after variance reduction -- borderline for many advertisers.
- Analyst skill ramp: **Yellow**. Two-proportion z is trivial; compliance-adjusted LATE via 2SLS needs coaching for a reader without formal causal training.
- Stakeholder interpretability: **Green** for ITT, **Yellow** for LATE (sales teams confuse ITT and TOT routinely).

*What we'd need to build:* (a) Bidder change to support "suppressed-with-logging" arms under a policy controlled by campaign config; (b) a ghost-impression pipeline into BigQuery; (c) a lift-test orchestration service (assignment manager, power calc, report generator); (d) variance-reduction library (CUPED, bootstrap); (e) a compliance layer (legal review per state privacy law, especially Texas TDPSA and Oregon OCPA post-2026 where cure periods sunset).

**8. Edge cases and gotchas.** Selection bias: randomization must be at bid-request, not post-hoc; post-hoc lists of "eligible users" reintroduce targeting bias. SUTVA violations from multiple household members exposed on multiple devices, or from brand-advertising spillover (brand ads likely violate; performance ads less so). Contamination across walled gardens -- a household held out by the DSP may still see the same campaign on Amazon or Meta, diluting the control. Co-viewing inflates the numerator. Frequency-cap confounding: heavy viewers get more impressions and also convert more regardless; ghost ads address this by conditioning on *opportunity*, not exposure. Divergent delivery (Eckles-Gordon-Johnson critique): the bidding algorithm optimizes differently when the control arm is excluded -- freeze the model during experiments. Short measurement windows miss long-run effects (Johnson-Lewis decay work). Cookie/MAID loss does not affect CTV households directly but does break the verified-visit chain to mobile/desktop conversions -- force IP-household-based bridging and accept ~10% unmatched.

---

### Method 3 -- Bayesian hierarchical MMM calibrated by lift-test priors (Google Meridian)

**1. Name and definition.** Fit a geo-hierarchical Bayesian regression of KPI on ad spend (adstocked and saturated) with ROI as a native parameter, using experimental lift results as informative priors.

**2. How it works.** Meridian (github.com/google/meridian) implements Sun et al. 2017 "Geo-level Bayesian Hierarchical MMM" with Jin et al. 2017 "Bayesian Methods for Media Mix Modeling with Carryover and Shape Effects" (research.google.com/pubs/archive/46001.pdf) and Zhang et al. 2024 "MMM Calibration with Bayesian Priors." Media inputs transform as **adstock (geometric/binomial decay) then Hill saturation: y_transformed = x^alpha / (x^alpha + K^alpha)**; response regresses on transformed media plus seasonality, baseline, and Google Query Volume as a demand control. The model is **reparameterized so ROI_m is a parameter with a LogNormal prior**; when the DSP feeds a geo-lift result as `LogNormal(log(point_estimate), SE)`, the posterior is the Bayesian-optimal blend of in-sample correlation and the causal prior. Fit via TensorFlow Probability NUTS MCMC on GPU. Robyn (Meta) does the frequentist analog via ridge + Nevergrad Pareto optimization with MAPE.LIFT as a calibration objective.

**3. Scientific validation.** Chan and Perry's 2017 Google Research paper "Challenges and Opportunities in Media Mix Modeling" (research.google/pubs/pub45998) is the definitive catalog of MMM's identification problems -- multicollinearity across channels, selection bias from ad-spend responding to demand, omitted-variable bias, ad-exposure measurement error. Jin et al. 2017 explicitly concede that "the model may produce biased estimates for the typical sample size of a couple of years of weekly national-level data." The Zhang 2024 calibration paper is the direct answer: **place the prior from an experiment, and the posterior ROI collapses to the causal estimate in proportion to prior tightness**. Recast's technical documentation (docs.getrecast.com/docs/experiments) describes the same pattern with its Gaussian-mixture "strict" prior. Without calibration, Meridian's 80% ROI CIs commonly span +/-30-60% -- useful for allocation ranking, not for a contractual iROAS number.

**4. CTV-specific considerations.** Meridian natively supports reach-and-frequency inputs with Hill on frequency, critical for CTV where diminishing returns hit at frequency 7-10 per Innovid 2025 benchmarks. Google's YouTube CTV meta-analysis (across Nielsen MMMs) reports YouTube CTV **3.1x more effective than linear TV** on average -- a useful prior benchmark for stakeholder conversations. The main CTV-specific risk is **limited history**: many CTV advertisers have only 12-18 months of at-scale CTV spend, below Meridian's recommended 2-3 years weekly minimum; for these, pool across advertisers in the same vertical or treat the model as geo-cross-sectional with shorter time series.

**5. Industry adoption.** Meridian, Robyn and PyMC-Marketing are the credible open-source options; of commercial vendors, Recast publishes the most detailed methodology, Analytic Partners was named Leader in the 2025 Gartner MMM Magic Quadrant, and Nielsen/Circana sell the most-audited but least-reproducible incumbent. **Incrmntal markets "causal MMM" and is not peer-reviewed; Rockerbox and Northbeam are MTA-first with MMM bolt-ons that lack published methodology and should be treated as marketing-first**. INCRMNTAL's positioning as "always-on causal inference from small change-events" is novel but unverified. The cleanest pattern in the literature today is the **triangulation architecture** endorsed by Meta (Robyn + lift calibration), Google (Meridian + calibration period), and PyMC-Marketing (`add_lift_test_measurements` against the saturation curve). A 2025 BCG study cited by LiftLab finds 46% of leading marketers use the MMM-experiment-attribution "trifecta"; 40% of top performers calibrate MMM with incrementality.

**6. Data and infrastructure requirements.** A weekly panel of `(advertiser_id, geo, week, spend_by_channel, impressions, reach, frequency, KPI, GQV, seasonality_controls, promotions, holidays)` across 100+ weeks. A separate `experiment_priors` table: `(advertiser_id, channel, roi_point_estimate, roi_se, calibration_window)`. Meridian runs in Python/TFP on Databricks GPU clusters; BigQuery is the natural data lake; the MMM output tables join back for activation into DSP reporting.

**7. Feasibility scorecard.**
- Data availability: **Yellow** -- CTV spend and impression history is rich; advertiser KPI history is thin for mid-market brands; competitor/promo controls missing.
- Experimentation plumbing: **N/A for MMM itself; Green once Method 1 is running to supply priors**.
- Statistical power: **Yellow** -- without calibration, wide posteriors; with calibration, strong.
- Analyst skill ramp: **Red->Yellow**. Bayesian hierarchical models are a significant learning curve for a reader without formal causal training; start with PyMC-Marketing tutorials rather than full Meridian NUTS tuning.
- Stakeholder interpretability: **Green** for point ROI; **Yellow** for posterior intervals (advertisers want a number, not a distribution).

*What we'd need to build:* Weekly MMM data mart in BigQuery, Meridian pipeline on Databricks GPUs, a calibration bridge that auto-ingests geo-lift results as priors, and a reporting layer translating posterior ROI into iROAS with explicit uncertainty.

**8. Edge cases and gotchas.** Multicollinearity between CTV and digital video when both run together (use hierarchical shrinkage and calibration priors). Endogeneity from ad spend reacting to demand signals the model can't see (GQV control helps, doesn't eliminate). Saturation identifiability is weak when spend doesn't vary much -- experimental priors on Hill parameters are better than priors on ROI alone (PyMC-Marketing's `add_lift_test_measurements` supports this). The **experiment estimand != MMM estimand** -- experiments measure a short-window, partial-reduction effect; MMM ROI is against zero-spend counterfactual -- Meridian's `roi_calibration_period` aligns these. Cookie/ID loss doesn't affect MMM directly because MMM is aggregated. Novelty effects distort early-campaign priors -- weight recent experiments more.

---

### Method 4 -- CausalImpact / BSTS for switch-ons and always-on campaigns

**1. Name and definition.** Bayesian structural time-series model comparing a treated series to a synthetic counterfactual built from correlated control series, with pointwise and cumulative credible intervals.

**2. How it works.** Brodersen, Gallusser, Koehler, Remy and Scott's 2015 *Annals of Applied Statistics* paper (DOI 10.1214/14-AOAS788) specifies a state-space model with local linear trend, seasonality, and a regression on contemporaneous controls with a spike-and-slab prior for variable selection. Fit on the pre-period, posterior-predicted forward; the difference between observed treated and predicted counterfactual is the causal effect. Google's R `CausalImpact` package is the canonical implementation; `tfcausalimpact` and `pycausalimpact` are Python ports.

**3. Scientific validation.** Brodersen et al. is the dominant industry standard for single-market quasi-experiments; used widely at Google, Lyft, Stripe; strong simulation validation. Failure modes documented: contaminated control series that *are* affected by the intervention, regime changes, mis-specified seasonality.

**4. CTV-specific considerations.** Ideal for launches of new CTV partners, regional CTV creatives, or single-DMA campaign shifts where geo-randomized holdouts aren't possible. Already in-house -- used for the targeting-algorithm rollout analysis per the brief.

**5. Industry adoption.** Ubiquitous; Google, Lyft, Stripe, Airbnb all use; used at Haus and Recast as single-market fallback.

**6. Data and infrastructure requirements.** Two time series (treated + one-or-more controls) with 8-26 weeks of daily or weekly pre-period data. Trivial to pull from BigQuery.

**7. Feasibility scorecard.** Green across the board except **Yellow on power** for short campaigns; the team's existing fluency with this method is a significant asset.

**8. Edge cases and gotchas.** The "no spillover into controls" assumption is the sharp risk in CTV -- if you use other DMAs or other advertisers as controls, ensure they are truly unaffected. Seasonality confounds with short pre-periods. Credible intervals narrow only with good pre-period R-squared (>0.8); thin-history advertisers get uninformative results. Not a substitute for randomized holdouts -- it is quasi-experimental.

---

### Method 5 -- Switchback experiments

**1. Name and definition.** Randomly alternate a campaign on/off in time blocks at a single market or national level; compare on-period vs off-period outcomes with a Horvitz-Thompson-style estimator.

**2. How it works.** Bojinov, Simchi-Levi and Zhao (2023, *Management Science* 69(7):3759-3777, DOI 10.1287/mnsc.2022.4583) derive the optimal design over carryover order m with martingale-CLT inference. Randomize time blocks (hours, days, weeks) into treatment and control; estimator is E[Y | on] - E[Y | off] adjusted for carryover persistence.

**3. Scientific validation.** Foundational in ride-sharing and marketplace experimentation (Lyft, DoorDash blogs); now applied in media. The 2023 Management Science paper is the formal reference.

**4. CTV-specific considerations.** The right answer when (a) geo holdouts are politically impossible ("don't go dark in my markets"), (b) cross-geo spillover is severe, or (c) an always-on campaign has existed for years with no natural holdout. Requires the campaign to have meaningful on-vs-off contrast within a short period -- works better for performance response than brand-building.

**5. Industry adoption.** LiftLab explicitly offers switchback; Recast supports it; Haus markets Time Tests as the equivalent.

**6. Data and infrastructure requirements.** Minimal -- a single market or national outcome time series with the switching schedule recorded.

**7. Feasibility scorecard.** Green on data, Green on plumbing, Yellow on power (needs 8-12 weeks of daily switches to achieve 5-15% MDE), Green on skill (conceptually simpler than synthetic control), Yellow on interpretability (advertisers don't love "we turned you off at 10am on Tuesday").

**8. Edge cases and gotchas.** Long-memory carryover violates the bounded-order assumption; underestimating m biases the estimator. Day-of-week and seasonal confounds if block lengths are misaligned. Competitor action during on-periods only. Cannot isolate per-household effects -- purely market-level. Not appropriate for brand-building campaigns where carryover is weeks to months.

---

### Method 6 -- Staggered-adoption DiD (Callaway-Sant'Anna, de Chaisemartin-D'Haultfouille, Sun-Abraham, Goodman-Bacon)

**1. Name and definition.** Difference-in-differences with rolling treatment timing across units, using modern estimators that avoid the negative-weighting problem of two-way fixed effects.

**2. How it works.** Classical 2x2 DiD is fine for single-date rollouts. When treatment starts at different times across DMAs, advertisers, or campaigns, canonical TWFE is biased -- Goodman-Bacon's 2021 *Journal of Econometrics* decomposition shows it averages 2x2 DiDs with some **negative weights**, and with treatment-effect heterogeneity can even flip sign. Callaway-Sant'Anna 2021 JoE compute group-time ATTs using never-treated or not-yet-treated comparators and aggregate with positive weights; de Chaisemartin-D'Haultfouille 2020 AER provide the DID_M estimator; Sun-Abraham 2021 JoE provide the interaction-weighted event-study; Borusyak-Jaravel-Spiess 2024 provide the imputation estimator. R packages: `did`, `fixest`, `DIDmultiplegt`; Python: `csdid`, `differences`, `pyfixest`.

**3. Scientific validation.** This literature is the single biggest methodological shift in applied econometrics of the last five years. Heavy replication; extensive simulation. Parallel-trends remains the identifying assumption and must be defended with event-study plots.

**4. CTV-specific considerations.** The natural application is not per-advertiser iROAS but **platform-level studies**: the rollout of a new CTV supply partner across markets, a bid-shading change that goes live advertiser-by-advertiser, a new identity-graph version. The team's prior targeting-algorithm analysis is a textbook use case.

**5. Industry adoption.** Universal in econometrics; adoption in adtech is more recent. Haus and LiftLab use DiD variants under the hood of their synthetic-control frameworks.

**6. Data and infrastructure requirements.** Geo x week or advertiser x week panel with treatment start-date variable. Trivial in BigQuery.

**7. Feasibility scorecard.** Green on data, Green on plumbing, Green on power (210 DMAs x 52 weeks commonly detects 3-5% ATT), Yellow on skill (the reader must learn which estimator to use when), Yellow on interpretability (event-study plots are not self-explanatory).

**8. Edge cases and gotchas.** Differential pre-trends -- always plot and test. Anticipation effects -- redefine t0 to announcement, not rollout. Do not use canonical TWFE with staggered rollouts and heterogeneous effects. Spillover across units kills parallel trends. Treatment-effect dynamics over time matter -- average ATT can hide a decaying effect.

---

### Method 7 -- IV via random auction variation (Waisman, Gui-Nair-Niu)

**1. Name and definition.** Use randomized bid-shading, reserve-price jitter, or auction throttling as an instrument for ad exposure, then estimate LATE via 2SLS.

**2. How it works.** Waisman, Nair and Carrion's 2024 revised paper "Online Causal Inference for Advertising in Real-Time Bidding Auctions" (arXiv 1908.08600) shows that in first- and second-price auctions, optimal bids relate algebraically to the treatment effect, and a modified Thompson Sampling recovers ATE for RTB. Gui, Nair and Niu's 2022 "Auction Throttling" (arXiv 2112.15155) uses random throttling at JD.com as a natural IV. The 2SLS recipe: first stage regresses `exposed` on the random instrument (bid-tier, throttle flag) and controls; second stage regresses `conversion` on the predicted exposure. Under random-Z and the exclusion restriction, this recovers LATE.

**3. Scientific validation.** The theoretical foundations are Imbens-Angrist 1994 (*Econometrica*) and Angrist-Imbens-Rubin 1996 (*JASA*); the ad-specific applications are newer and less replicated. Sahni, Narayanan and Kalyanam have additional papers in this space.

**4. CTV-specific considerations.** Highly attractive as a **Phase-2 stopgap before full ghost-bid logging** because it exploits auction variation you already create (bid-shading, frequency caps, budget pacing) -- no bidder-side product change required beyond *logging* the randomized component. Main constraint: CTV auctions are increasingly PMP/programmatic-guaranteed, not pure open auctions, which thins the IV.

**5. Industry adoption.** Limited vendor productization; JD.com is the canonical case study; internal use at some major DSPs based on conference chatter (not peer-reviewed disclosures). Not a commercial SaaS offering.

**6. Data and infrastructure requirements.** Bid-stream logs with the randomized component preserved (bid-shade seed, throttle assignment, reserve jitter). Household-to-conversion join.

**7. Feasibility scorecard.** Yellow on data (depends on what random components exist in the current bidder), Yellow on plumbing (need to *guarantee* randomness, not just observe it), Yellow on power (LATE compliance rate can be small), Red on skill (2SLS without causal training is dangerous), Yellow on interpretability.

**8. Edge cases and gotchas.** **Weak instruments** are the classic failure -- if the random component barely moves exposure, the IV estimator has explosive variance (Stock-Yogo rule: first-stage F > 10). Exclusion restriction violations -- the instrument affects only Y through D; if bid-shading also affects creative selection, exclusion fails. Defier presence violates monotonicity. Non-stationarity from dayparting breaks mean-field assumptions.

---

### Method 8 -- Regression Discontinuity at caps and thresholds

**1. Name and definition.** Exploit sharp cutoffs (frequency cap, reserve price, budget pacing exhaustion) where exposure changes discontinuously to identify the local treatment effect.

**2. How it works.** Calonico, Cattaneo and Titiunik (2014, *Econometrica* 82(6):2295-2326) provide the modern standard: MSE-optimal bandwidth selection, local-linear polynomial, bias-corrected robust confidence intervals. Effect = limit-from-above minus limit-from-below at the cutoff. Tools: `rdrobust`, `rdbwselect`, `rddensity` (McCrary manipulation test), available on R/Stata/Python via rdpackages.github.io. Narayanan and Kalyanam (2015, *Marketing Science* 34(3):388-407) is the template application in paid search: naive position-effect estimates are **massively overstated**; RD estimates are much smaller and exist only in some positions.

**3. Scientific validation.** RDD is the most credible quasi-experimental design in econometrics. CCT 2014 is the inference workhorse. Ad-specific applications are narrower -- Hartmann, Nair and Narayanan (2011) *Marketing Science* 30(6):1079-1097 is another canonical example.

**4. CTV-specific considerations.** The natural cutoffs are **frequency caps** (4th impression vs 3rd for a household-capped campaign), **reserve prices** on specific deals, and **auction-rank tie-breakers** at minimum quality scores. Budget pacing exhaustion creates a sharp discontinuity but may confound with time-of-day.

**5. Industry adoption.** Rare in commercial vendors; appears in academic DSP/search work. Not a product offering anywhere.

**6. Data and infrastructure requirements.** Bid logs with the running variable (impression count, bid price, quality score) preserved at auction time, plus household-level outcomes.

**7. Feasibility scorecard.** Green on data, Green on plumbing, Yellow on power (local estimates are narrow), Yellow on skill (bandwidth selection and manipulation tests are technical), Yellow on interpretability (local effect, not portfolio iROAS).

**8. Edge cases and gotchas.** Manipulation/sorting around the threshold (advertisers adjusting bids to land just above a floor) invalidates the design -- test with McCrary/Cattaneo-Jansson-Ma density tests. Compound treatments at the cutoff (multiple things change at the same threshold) confound. External validity is limited to the cutoff neighborhood. Sparse density near the threshold is common in ad auctions with mass points. Bandwidth sensitivity -- always use CCT robust bias-corrected CIs.

---

### Method 9 -- Uplift / CATE modeling (meta-learners and causal forests)

**1. Name and definition.** Estimate heterogeneous treatment effects tau(x) at the household or segment level for targeting optimization.

**2. How it works.** Kunzel, Sekhon, Bickel, Yu 2019 *PNAS* (DOI 10.1073/pnas.1804597116) formalize S-, T- and X-learners; Nie & Wager 2021 *Biometrika* the R-learner with Robinson residual-on-residual loss; Wager & Athey 2018 *JASA* causal forests with "honesty"; Athey, Tibshirani, Wager 2019 *Annals of Statistics* generalized random forests (`grf` R package); Kennedy 2023 DR-learner via AIPW pseudo-outcomes. Libraries: EconML, CausalML, DoWhy, `grf`.

**3. Scientific validation.** Extensively replicated at the method level; Curth and van der Schaar 2021 show learner rankings flip across datasets -- no dominant meta-learner. The sharp warning for ad measurement is that **CATE methods inherit the same unconfoundedness requirement as ATE methods**; Gordon et al. 2023 "Close Enough?" still showed 62-115% median absolute error in lift with DML on 5,000+ features.

**4. CTV-specific considerations.** **Use for targeting, not for headline iROAS.** The right deployment is: run periodic lift tests, fit CATE on experimental data, deploy tau_hat(x) as a bid multiplier. Applying CATE to observational exposure data and calling the average "incrementality" is not defensible.

**5. Industry adoption.** Nearly every major DSP and retail-media platform uses CATE internally for bidding; none publish honest benchmarks against RCT truth.

**6. Data and infrastructure requirements.** Experimental or quasi-experimental assignment signal + rich user features + outcome. EconML and `grf` run on Databricks single-node; DR-learner scales with distributed sklearn-compatible regressors.

**7. Feasibility scorecard.** Yellow across the board -- useful, but not the path to the headline iROAS number the brief is asking for.

**8. Edge cases and gotchas.** Learner rankings are unstable. Overlap is usually bad in ad data (highly targeted campaigns). Pretending the CATE average is an unbiased ATE when data is observational is the most common mistake in industry blog posts. Individual-level CATE variance is often larger than its mean -- many "significant heterogeneities" are noise.

---

### Method 10 -- Double ML, TMLE, PSM/IPW on observational exposure

**1. Name and definition.** Observational estimators of ATE using flexible ML for nuisance functions under conditional-ignorability assumptions.

**2. How it works.** Chernozhukov et al. 2018 *Econometrics Journal* Double ML uses cross-fitting and Neyman-orthogonal scores; van der Laan-Rubin 2006 TMLE uses a targeting step driven by the efficient influence function. Rosenbaum-Rubin 1983 propensity scoring and IPW remain the textbook baseline; King & Nielsen 2019 *Political Analysis* proved that PSM used for *matching* (as opposed to regression adjustment or IPW within DR estimators) has a paradox: tighter calipers increase imbalance and bias.

**3. Scientific validation.** The methods are mathematically sound; the empirical track record in ads is devastating. **Gordon-Zettelmeyer-Bhargava-Chapsky 2019** showed half of Facebook RCTs had observational lift estimates off by a factor of 3 even with rich platform features. **Gordon-Moakler-Zettelmeyer 2023** ("Close Enough?", *Marketing Science* 42(4)) extended this to 663 RCTs with 5,000+ user features and deep-learning DML, finding **DML modestly better than PSM but both still wrong** by median 62-115 percentage points on the lift scale versus median true lifts of 6-28%. Blake-Nosko-Tadelis 2015 on eBay is the single most damning case: OLS-attributed ROI of >4,100%, true causal ROI of -63%.

**4. CTV-specific considerations.** CTV ad delivery is the output of a targeting system that conditions on unobservable predicted-value features; no vendor third-party data set captures those. Unconfoundedness essentially *cannot* hold from outside the ad platform.

**5. Industry adoption.** Universal, but often dishonestly. Many "incrementality" SaaS vendors sell observational methods as causal: **Incrmntal's "causal AI" on aggregated change-events, Rockerbox's and Northbeam's MTA-plus-ML iROAS, Measured's attribution-based dashboards (distinct from their geo experiments)** -- these produce observational estimates under various assumptions that are rarely stated and almost never tested against RCT benchmarks. Treat their iROAS numbers as directional at best until they publish calibration studies.

**6. Data and infrastructure requirements.** Rich user/household features, exposure flag, outcome. EconML on Databricks; BQML for nuisance logistic/boosted-tree.

**7. Feasibility scorecard.** Green on data and plumbing, Red on scientific validity for headline iROAS, Yellow on skill, Green on interpretability (but dangerously so -- stakeholders trust numbers that look precise).

**8. Edge cases and gotchas.** All the usual -- hidden confounders, overlap violation, extreme propensities. For Double ML specifically, **precise bias** is the worst kind: tight confidence intervals around a wrong number. Use only as internal diagnostic, never as customer-facing iROAS.

---

### Triangulation (cross-method)

**Name and definition.** Use geo lift (1) and RCT lift (2) to calibrate Bayesian priors in MMM (3), with BSTS (4) and switchback (5) as situational fallbacks, to produce a single reported iROAS per advertiser.

**How it works.** Run ~4-6 geo lift tests per advertiser per year; feed point estimates and SEs as `LogNormal(log(est), SE)` priors on channel ROI into Meridian; refresh MMM posterior weekly; reported iROAS = posterior mean with 80% credible interval. This is the "unified measurement" architecture Meta, Google and PyMC-Marketing have converged on and that the BCG 2025 "trifecta" study describes as the top-performer pattern.

**Gotcha.** Experiment estimand != MMM estimand -- align windows and counterfactuals (Meridian's `roi_calibration_period`).

---

## Part 2. Cross-cutting issues

### CTV-specific statistical power

Apply Lewis & Rao's formula, **N_per_arm = 2 * ((z_{alpha/2}+z_beta) * sigma/delta_y)^2**. For a typical CTV campaign (10M impressions, $25-$50 CPM, ~2M households, frequency =5, cost per HH =$0.125, household sigma = $70 weekly sales, required $0.25 break-even lift at 50% margin), the break-even-ROI MDE at 50/50 split and 80% power is = $0.28 per household per week -- **right at break-even iROAS**. Smaller campaigns (<5M impressions) cannot distinguish break-even from 2x ROI. Variance reduction stacks -- CUPED (20-50%), ghost-ad conditioning (25-31%), stratified randomization (10-20%) -- can compound to ~40% SE reduction, equivalent to 2.7x the sample. **Rule of thumb: do not report an iROAS point estimate without a +/-50 percentage-point confidence interval for any campaign below 5M impressions.** This is not pessimism -- it is the Lewis-Rao QJE result applied to realistic CTV volumes. Advertisers will push back; pre-empt by citing.

### Household vs. individual identification

Household is the dominant unit in CTV, confirmed by the stack brief and by the IAB/MRC CTV/OTT measurement guidelines (August 2021). Co-viewing multipliers range from **1.23 to 1.90 per impression** depending on daypart, demo and genre; TVision reports average viewers-per-viewing-household of **1.46 on linear and 1.44 on CTV**, peaking at **1.52 in primetime**; iSpot reports co-viewing contributes an incremental **~41% of viewership on streaming**. These are *dynamic*, not static -- the industry's frequent use of a single "1.2x" factor masks material variation. For iROAS: **keep the numerator (conversions) and denominator (exposure) at the same unit of analysis**. If conversions are household-resolved via the identity graph, use household-level impressions and household-level spend; if conversions are individual-pixel (typical for web purchases), accept that co-viewing inflates the apparent iROAS and either apply a daypart/genre-specific co-viewing adjustment or flag the co-viewing bias in reporting. LiveRamp's household RampIDs, TransUnion's TruAudience graph (95-98% US adult match claim), and Viant's HouseholdID (TransUnion-powered) are the dominant commercial options; the hybrid deterministic/probabilistic graph the DSP already runs is the right architecture -- treat the third-party graphs as benchmarks, not replacements.

### Walled-garden measurement

CTV walled-garden spend is concentrated: per eMarketer's H2 2025 forecasts, US CTV ad spend reaches **$33.35B in 2025 and ~$38B in 2026**, with YouTube (~12% share), Amazon (>10%), Disney (>10%) and Netflix (>$1.5B, doubling in 2026) dominating. For each, measurement is gated through platform-controlled clean rooms:

- **Amazon Marketing Cloud** supports custom SQL over Amazon Ads event-level signals with a ~100-user aggregation threshold; holdout-based lift is supported. Built on AWS Clean Rooms; DP is available via the underlying AWS layer, aggregation is primary.
- **Google Ads Data Hub** supports custom SQL over Google/YouTube event-level data with a 50-user threshold (10 for clicks/conversions) and optional noise-mode which lowers thresholds to ~20. MRC-accredited for non-noise queries.
- **Disney Clean Room** uses InfoSum/Snowflake/LiveRamp backends with Audience Graph household IDs; templated queries, not open SQL; measurement via VideoAmp, Samba TV, EDO.
- **Netflix Ads** (transitioning from Microsoft/Xandr to in-house) uses Snowflake, InfoSum and LiveRamp for post-campaign measurement; programmatic via TTD, DV360, Xandr.
- **Roku OneView / Snowflake-based clean room** uses templated queries with 20+ measurement partners.
- **NBCU One Platform** (Snowflake, VideoAmp) offers restricted-template measurement.

**What a DSP can and cannot measure:** Inside walled gardens, household-level ghost-bid logging is impossible -- the auction happens on the publisher side. Your iROAS framework for walled-garden inventory must rely on (a) geo holdouts across the advertiser's total CTV spend (works because walled gardens participate whether or not you control the auction), (b) clean-room-conducted lift studies where the platform supports them (AMC, ADH), and (c) MMM with walled-garden spend as a channel input. Do not pretend you can measure incremental conversions for walled-garden impressions at the household level without clean-room integration.

### Data clean rooms

For the DSP's own product architecture, the relevant comparison is between **LiveRamp Safe Haven / Habu** (acquired Jan 2024 for ~$200M; multi-cloud, RampID-centered), **InfoSum** (non-movement architecture, built-in differential privacy, neutral third-party), **Snowflake Data Clean Rooms** (Samooha-based, templated SQL, GA differential privacy with epsilon/budget since Snowday 2023), **AWS Clean Rooms** (GA differential privacy 2024, analysis-rule-based custom SQL), and **BigQuery Data Clean Rooms** (native Analytics Hub, GA differential privacy with parameter budgeting, Tumult Labs partnership, entity resolution). Since primary analytics are in BigQuery, **BigQuery Clean Rooms is the path of least resistance for native warehouse collaboration** -- advertisers with conversion data in other warehouses can onboard via linked datasets without copying. For integrations with walled-garden-side clean rooms (Disney, Netflix), **LiveRamp Safe Haven** is the most battle-tested bridge; **Snowflake** is the necessary second integration because NBCU, Netflix and Roku run on it. InfoSum is strongest when decentralized architecture is a regulatory requirement. Prioritize in this order: BigQuery Clean Rooms (native), Snowflake Clean Rooms (publisher reach), LiveRamp Safe Haven (identity resolution), AMC (Amazon exclusive), ADH (YouTube exclusive).

---

## Part 3. Phased rollout

### Phase 1 (0-3 months): geo pilot, BSTS refresh, MMM data mart skeleton

Stand up a **DMA-randomized geo holdout pilot** on 6-10 performance advertisers spending >=$500k/month on CTV. Pre-register each pilot: treatment-DMA assignment via stratified random sampling balanced on pre-period revenue; 4-week pre-period; 4-week treatment; conformal inference at alpha=0.10 via GeoLift; parallel analysis in classical Vaver-Koehler 2011 Geo-Based Regression for convergent validity; commuting-zone aggregation where spillover risk is high (coastal markets, NY-NJ-CT tri-state, DC-VA-MD). Primary metric: verified-visit-adjusted conversion rate per household. Guardrail metrics: impression delivery in test vs control (should differ only by design), frequency distribution (should not diverge), creative mix (should be balanced). Success criteria: conformal CI excludes zero on at least 5 of 10 pilots; median iROAS within 2x of the advertiser's internally reported MTA ROAS (directional agreement, not equality). Pre-compute MDE via GeoLift's power simulator on 52 weeks of historical revenue per advertiser; refuse pilots with MDE above 15%. Tooling: R 4.4 on Databricks with augsynth and GeoLift; data pulled from BigQuery via bigrquery. In parallel, refresh the CausalImpact pipeline already in use for single-market analyses and begin building the weekly MMM data mart (advertiser x geo x week x channel x KPI).

### Phase 2 (3-9 months): bidder-side ghost-bid logging, MMM beta, clean-room integrations

Build the bidder feature to support ghost-impression logging for campaigns flagged into a lift-test arm: on suppressed bid requests from assigned-to-control households, log (bid_request_id, household_id, arm, would_have_won, ghost_timestamp) without serving. Add a randomization service that assigns household_id into arms at campaign onboarding via deterministic hashing. Build a lift-test orchestration service on Databricks: assignment manager, power calculator (Lewis-Rao formula with historical sigma calibration), CUPED variance reduction, bootstrap inference, LATE estimator via 2SLS with assignment as instrument. In parallel, stand up Meridian in beta on three pilot advertisers with 2+ years of weekly history, ingesting Phase-1 geo-lift results as LogNormal priors on CTV channel ROI. Integrate BigQuery Data Clean Rooms for advertiser first-party data collaboration and LiveRamp Safe Haven for walled-garden bridging. Begin IV-via-auction-throttling as a complement: log the randomized component of existing bid-shading as a stopgap LATE estimator for campaigns that can't yet run ghost bids.

### Phase 3 (9-18 months): unified measurement

End state: every advertiser receives a single iROAS number per campaign, computed as the Meridian posterior mean on channel ROI, with the posterior calibrated by that advertiser's rolling stock of lift experiments (geo + household RCT + switchback). Production output includes: point estimate, 80% credible interval, most-influential input (lift test vs in-sample regression), sensitivity to co-viewing adjustment, walled-garden share flag. The advertiser UI shows the number alongside the MMM-only and lift-only counterfactuals so discrepancies are visible. A quarterly calibration review re-estimates priors; advertisers below the experiment-cadence threshold receive wider intervals by construction. Internally, a PIE-style meta-model (Gordon-Moakler-Zettelmeyer 2023/2026, arXiv 2304.06828, R-squared 0.88 vs last-click's 0.19) predicts iROAS for non-lift-tested campaigns from campaign features, trained on the portfolio's lift-tested subset -- this covers the 80-90% of campaigns too small for their own RCT. Clean-room integrations (AMC, ADH, Disney) produce walled-garden-specific lift estimates that feed back into Meridian as channel-partition priors. The "unified measurement" artifact is not one number from one method; it is a Bayesian blending of geo, household RCT, switchback, clean-room and MMM, with explicit uncertainty.

---

## Part 4. Open decisions to drive

The following eleven decisions, once made, unblock the rollout. Drive them with Product, Engineering, Legal, and Finance partners.

1. **Holdout size policy.** Is 10% the floor, or do we allow advertisers to opt up to 20-50%? This directly controls Phase-1 MDE and advertiser willingness to participate.
2. **Minimum detectable effect floor.** Do we refuse to run lift tests where MDE exceeds, say, 15% -- or do we run them and caveat heavily? Recommend the former.
3. **Co-viewing treatment.** Apply a flat daypart/genre multiplier (industry 1.2x) or the Nielsen/iSpot dynamic adjustment or no adjustment with a bias disclosure? This is both a scientific and a sales decision.
4. **Feature pricing and packaging.** Is iROAS a free tier for enterprise advertisers, a paid add-on, or a standard KPI in the UI? Haus, LiftLab and Measured charge $50k-$500k/year for comparable.
5. **Build vs buy on geo experimentation.** Use open-source GeoLift and own the methodology, or partner with Haus/LiftLab/Measured for a managed service that gives faster time-to-market but weaker transparency and ongoing cost?
6. **Bidder-side ghost-bid logging.** Green-light the bidder change for Phase 2 -- which engineering team owns it, and is PSA/suppression-with-logging allowed under current supply-partner contracts?
7. **Clean-room integration priority.** Which clean rooms first -- BigQuery native, Snowflake, AMC, ADH, or LiveRamp Safe Haven? Recommend BigQuery + Snowflake + LiveRamp first; AMC and ADH on demand.
8. **MMM buy vs build.** Build on Meridian (open-source, best methodology) or license Recast / Analytic Partners for managed delivery? Build-on-Meridian is recommended.
9. **Randomization unit.** Household as default (matches stack brief) -- but explicit policy for advertisers with device-level pixels who push for user-level randomization despite CTV's household-only reality.
10. **Measurement window.** 7-day, 14-day, or 28-day post-impression for conversions? Affects both the numerator and lift magnitude; brand vs performance decision.
11. **Reporting uncertainty format.** Point estimate only, point + interval, or interval only? Stakeholder interpretability vs scientific integrity trade-off.

---

## Part 5. Reading list (in consumption order)

1. **Lewis & Rao (2015), QJE**, "On the Near Impossibility of Measuring the Returns to Advertising." The ground-truth power analysis -- read first so every other method is sized correctly.
2. **Gordon, Zettelmeyer, Bhargava & Chapsky (2019), Marketing Science**, "A Comparison of Approaches to Advertising Measurement." Empirical proof that observational methods fail at Facebook scale with platform features.
3. **Gordon, Moakler & Zettelmeyer (2023), Marketing Science**, "Close Enough?" (arXiv 2201.07055). Deep-learning DML on 663 RCTs and 5,000+ features still fails -- read before anyone proposes an ML-only iROAS.
4. **Johnson, Lewis & Nubbemeyer (2017), JMR**, "Ghost Ads" (SSRN 2620078). The canonical design for household-level RCT in ad auctions.
5. **Johnson, Lewis & Reiley (2017), Marketing Science**, "When Less Is More." Why exposure conditioning adds 31% precision -- the core variance-reduction argument.
6. **Blake, Nosko & Tadelis (2015), Econometrica**, eBay paid search. The sign-flip case study on attribution vs causal ROI.
7. **Shapiro, Hitsch & Tuchman (2021), Econometrica**, "TV Advertising Effectiveness and Profitability: Generalizable Results from 288 Brands." Median own-ad elasticity 0.01; marginal ROI negative for >80% of CPG brands -- the reality check for TV/CTV priors.
8. **Brodersen, Gallusser, Koehler, Remy & Scott (2015), AoAS**, "Inferring Causal Impact Using Bayesian Structural Time-Series." The BSTS method already in use -- read for identification assumptions.
9. **Abadie (2021), JEL**, "Using Synthetic Controls." The methodology review behind GeoLift and Meridian's geo priors.
10. **Ben-Michael, Feller & Rothstein (2021), JASA**, "The Augmented Synthetic Control Method" (arXiv 1811.04170). The ridge-augmented estimator GeoLift defaults to.
11. **Meta GeoLift methodology docs** (facebookincubator.github.io/GeoLift/docs/Methodology/). The open-source production implementation.
12. **Chan & Perry (2017), Google**, "Challenges and Opportunities in Media Mix Modeling." The definitive catalog of MMM identification problems -- read before proposing any MMM.
13. **Jin, Wang, Sun, Chan & Koehler (2017), Google**, "Bayesian Methods for MMM with Carryover and Shape Effects." The model under Meridian.
14. **Zhang et al. (2024), Google**, "MMM Calibration with Bayesian Priors." The experiment-as-prior pattern that operationalizes triangulation.
15. **Callaway & Sant'Anna (2021), JoE**, "Difference-in-Differences with Multiple Time Periods." The staggered-DiD reference for platform-level rollouts.

Additional short reads: Imbens & Angrist (1994) Econometrica for LATE; Calonico, Cattaneo & Titiunik (2014) Econometrica for robust RDD inference; Bojinov, Simchi-Levi & Zhao (2023) Management Science for switchback; Waisman, Nair & Carrion (arXiv 1908.08600) for auction-IV; King & Nielsen (2019) Political Analysis on why PSM shouldn't be used for matching.

---

## Conclusion

Incrementality measurement in CTV is not a tooling problem -- it is a scientific-culture problem. The math is settled: observational methods produce tight confidence intervals around wrong numbers, and the ad industry has spent fifteen years hiding that under dashboard gloss. The DSP's advantage is not that a better algorithm exists -- it's that the stack can support a triangulation architecture where geo holdouts, household RCTs and Bayesian MMM reinforce each other and expose their own uncertainty. Geo first, bidder-side ghost bids next, Meridian as the production surface, clean rooms as the walled-garden bridge. Every other method on this list is supporting infrastructure. The single largest risk is not methodological -- it is stakeholder pressure to report a single confident number where the science supports only a range. The single largest opportunity is that no competing DSP has built this yet for CTV; the first to ship honest iROAS with calibrated uncertainty wins the enterprise-performance segment, because Finance trusts it.

The reader's ramp is the shortest path: they already have BSTS intuition from the targeting-algorithm analysis; extending to synthetic control is one conceptual step, not a new discipline. Get GeoLift running on two advertisers this quarter and the rest of the roadmap follows.
