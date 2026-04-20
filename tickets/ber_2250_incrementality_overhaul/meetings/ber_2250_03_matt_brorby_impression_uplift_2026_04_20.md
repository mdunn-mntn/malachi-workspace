# BER-2250: Impression Uplift Model + Ghost Bidding Strategy Working Session

**Date:** 2026-04-20
**Attendees:** Malachi Dunn, Alex Knorr (present from the start), Matt Brorby (Staff DS — joined later)
**Type:** 3-way working session — ghost bidding strategy + Matt's prototype walkthrough once he joined
**Transcript:** `ber_2250_03_matt_brorby_impression_uplift_2026_04_20.txt`
**Branch:** https://github.com/SteelHouse/databricks_targeting/tree/mbrorby/workspace/impression-uplift

---

## The Decision

Matt reviewed ~4,000 articles on incrementality measurement. Only two feasible paths exist for MNTN:

1. **Ghost bidding** — the agreed primary path. Leverages the existing 10% holdout. No bidder engineering changes required (data post-processing). What Google and Meta use.
2. **Geo testing / synthetic controls** — blocked by budget-driven power requirements (need $500k+/month to detect 2-8% lift at 15% MDE). Zach/Jordan "hate geo with a passion."

RCTs are gold standard but impractical at MNTN's scale. Observational ML alone failed at Meta even with thousands of features — without a counterfactual, you predict your own targeting, not incrementality.

## Matt's Prototype (Already Built)

**Branch:** `mbrorby/workspace/impression-uplift` in `SteelHouse/databricks_targeting`

**Architecture: T-Learner with Platt Scaling**
- Two XGBoost Spark models, one per treatment arm
- Model_T: P(visit | impression), trained on 90% treatment
- Model_C: P(visit | no impression), trained on 10% holdout
- Platt Scaling calibrates both to observed visit rates
- Uplift = Platt(Model_T(x)) − Platt(Model_C(x))
- Ranking by uplift, not absolute visit probability
- Qini curve evaluation (mentioned but outputs not captured in notebook)

**Holdout hash (ported from Greenplum to Spark):**
- `MD5(advertiser_id:ip)` first 16 hex → mod 1000, buckets 0-99 = holdout
- Uses `decimal(20,0)` to avoid overflow

**Key fix (2026-04-11):** Control IPs now drawn from `cost_impression_log` (IPs served by *other* advertisers during the same window), not feature store. Feature-store IPs include households no ad system would target (~0.03% vs ~9% visit rate). Fixed support mismatch.

**Status:** Experimental. Runs on Databricks with Vault GCP workload auth. Outputs to Matt's dev bucket (`gs://mntn-data-archive-dev/matt.brorby/`). Not in airflow-ti.

## Ownership (Explicit)

> "I don't necessarily want to build the models." — Matt

- **Matt:** Domain expert, strategist, methodology advisor. Will **not** own implementation.
- **Malachi + Alex (co-drivers):** Both in the call from the start discussing strategy before Matt joined. Joint ownership of ghost bidding pipeline, model work, and experiment coordination — Alex leans implementation/model, Malachi leans methodology/stakeholder comms.
- **Ryan Kleck:** Data accessibility expert — need to sync re: augmentor_log + future-store.

## Malachi's Commitments (Action Items)

### This week
1. **Meet with Ryan** — augmentor_log access, win-rate calculation, future-store relationship
2. **Define ghost bidding methodology manually** — build output dataset schema
3. **Brush up on power / sample size** — be able to answer "what budget do we need for what MDE?"

### By April 30 (Bryce's checkpoint — prep, not execution)
5. **Coordinate mid-intent experiment** with Kirsa + Nick
6. **Decision needed:** narrow (mid-intent only) vs broad (all intent tiers including max-reach)
7. **Stakeholder communication** — explain power/MDE constraints to Mike & Bryce

### Collaborative / shared
8. **Model finalization with Alex Knorr** — who owns? TBD.

## Technical Gaps Identified

1. **Win rate calculation** — uncertain mechanics. Matt: "tricky, but for sure we can do the subsetting down."
2. **Intent score for holdout** — holdout IPs have no impressions; solution: use intent score at time of first augmentor_log appearance. Acknowledged non-zero ambiguity.
3. **Max-reach / unscored universe** — prospecting scores only go down to mid-intent now. Max-reach IPs exist in membership DB but aren't scored. Future work with Ryan's continuous scoring.
4. **Notebook evaluation outputs** — Qini curves not in shared notebook. Need Matt's version with outputs.
5. **Production path** — model serving? Multi-model per advertiser? Offline analysis only? All TBD.

## Core Strategic Insights

### The power problem is the whole ballgame
> "The problem with advertising is power... the minimum measurable lift is 15%, but we're only getting 2 to 8%."

This is why geo doesn't work at MNTN's budget scale, why observational ML fails, and why ghost bidding (which reuses the existing 10% holdout rather than carving out a new one) is structurally the only path.

### Ghost bidding is internal measurement, not a product
> "I don't think the end goal with ghost bidding is like an actual product... customers care about incrementality from third parties who are impartial. Not grading your own homework."

This means ghost bidding is for **our** model training and internal validation. External iROAS numbers for customers still come from LiftLab / Haus / Measured.

### The counterfactual is non-negotiable
> "You can't just train on features and try to predict incrementality... it just predicts intent to treat."

Even rich ML fails without a control arm. The 10% holdout is what makes our T-learner viable.

### Why target mid-intent, not max-reach
> "Max-reach could very possibly have the biggest incrementality... but we can't score them right now."

Until Ryan's continuous scoring catches up, mid-intent is the best-instrumented cohort for our first experiment.

## Methodology Reference

| Concept | What it means here |
|---|---|
| **ITT** | All assigned treatment vs all assigned control — dilutes when coverage is low (14-16%) |
| **ATT/TOT** | Effect on those who actually received impressions — what ghost bidding estimates |
| **T-Learner** | Two separate models (treatment + control), subtract predictions |
| **X-Learner** | T-Learner + second de-biasing stage using cross-imputed effects |
| **Platt Scaling** | Logistic calibration so predicted aggregate = observed aggregate |
| **Qini curve** | AUC-analog for uplift ranking — how well does your model order incrementality |
| **MDE** | Min detectable effect given sample size. Ours: ~15%. Reality: 2-8% |

## Coordination Notes

- **Incrementality Slack channel** — Bryce to create (not yet live)
- **TI-837 needs stream split** per Bryce: (1) ghost bidding methodology, (2) mid-intent experiment setup
- **LiftLab investigation** — still a sprint task (TI-856)
- **Paulo's involvement** on holdout setup — unclear if still active

## Next Syncs

- Matt + Alex Knorr — meta-learner architecture (ongoing)
- Malachi + Ryan — augmentor_log + future-store (to schedule)
- Malachi + Kirsa/Nick — mid-intent experiment setup (before April 30)
