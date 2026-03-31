# MNTN Business Knowledge
Last updated: 2026-03-30

General knowledge about MNTN as a business — products, strategy, org structure, industry context, terminology, and institutional knowledge. Sourced from shared docs, meetings, messages, and conversations. Updated as new business context is learned.

---

## Company Overview

- MNTN is a CTV (Connected TV) advertising platform
- Advertisers use MNTN to run TV campaigns on streaming platforms
- Revenue comes from advertiser spend on campaigns

---

## Products & Features

### Multi-Touch Display
- Display ad campaigns that complement CTV (television) campaigns
- **History:** In 2021-2022, Mark and executives pushed to eliminate multi-touch display. Experiments showed TV-only campaigns with aggressive frequency caps (1/14 days, 1/30 days) could match multi-touch performance.
- **Current status (2026):** Multi-touch is being encouraged again. The original TV-only advantage eroded as the platform changed since the 2021 baseline. Multi-touch is currently turned off by default but advertisers can opt in.

### Mountain Match (Targeting)
- **V1:** MNTN's proprietary targeting system, replacing interest-based audiences for prospecting campaigns
- Kirsa manually assigned every AID (advertiser ID) to a vertical/protocol — took months
- Performance vs interest audiences: ~500% improvement
- Led to current targeting approach including Fangorn (bottoms-up keywords)

### Media Plan
- Algorithm-based publisher allocation for CTV campaigns
- Currently in beta, planned release to customers within a few months (as of March 2026)
- Running validation experiments before release

### Pause Ads
- Ad format where an ad is displayed when a viewer pauses content
- Was a major initiative that experimentation team was heavily involved in

### Vertical Video
- New ad format currently being developed and tested

### Retargeting
- Targeting users who have previously visited the advertiser's site
- Audiences are inherently small, making TV-only retargeting hard to scale
- Currently uses multi-touch display to augment small CTV retargeting audiences

---

## Org Structure & Teams

### Key People
- **Mark** — Executive, drives product direction. Pushed multi-touch elimination (2022), pushes IVR as primary metric.
- **Kirsa** — Experimentation Lead / Product Manager. Owns experiment design, execution, and analysis. Previously PM for data monitoring, then product PM for targeting (Mountain Match). Has a set monthly budget for experiments.
- **Nick** — Works with Kirsa on experimentation methodology and statistical approach. Involved in power analysis and methodology improvement planning.
- **Toph** — Production ops. Validates campaign changes for pacing risk (e.g., media plan beta rollout).

### Three Main Experimentation Focus Areas (as of March 2026)
1. **Targeting** — Fangorn, bottoms-up keywords, audience optimization. Most impactful lever.
2. **New ad formats** — Vertical video, pause ads
3. **Lift and incrementality** — New-to-brand rates, lift test performance improvement, geo-based lift testing with partners (e.g., Lift Lab)

---

## Business Metrics & KPIs

### Attribution Model
- **Current default:** First touch attribution (changed from last touch)
- Previous frequency experiments were run under last touch — similar results seen under both models
- `r2_advertiser_settings.reporting_style` controls per-advertiser attribution

### Primary Experiment Metric
- **IVR (Impression-to-Visit Rate)** — primary metric per Mark's direction
- Rationale: MNTN can only drive visits; what happens on the advertiser's site is their responsibility

### Supporting/Guardrail Metrics
- **CPA (Cost Per Acquisition)** — must not worsen when IVR improves
- **ROAS (Return on Ad Spend)** — must not worsen
- **Visit Conversion Rate** — conversions / visits (quality of visits)
- **Effective Conversion Rate** — conversions / impressions

### What "Good" Looks Like for Experiments
- 10-15% improvement = a win for most experiments
- 50-100% = exceptional (seen with targeting changes like Fangorn)
- 500% = extraordinary (Mountain Match V1 vs interest audiences)

---

## Industry & Market Context

### Attribution Industry Standards
- **Last touch deterministic** is considered the industry standard by most MMPs (Mobile Measurement Partners)
- IP-based attribution has a lower confidence threshold than device-level identifiers (e.g., IDFV)
- CTV has inherent limitations — no SDK on most devices, reliance on IP-based matching
- Apple's privacy changes (ATT/App Tracking Transparency) obfuscated device identifiers for mobile

### Lift Testing Partners
- **Lift Lab** — external partner used for geo-based lift measurement. Segments by geography and measures raw metric lift during campaign period.
- Lift tests are a key focus area — finding ways to improve MNTN's performance in partner-run lift studies

---

## Customer & Advertiser Context

*Add knowledge about advertiser segments, verticals, common use cases, onboarding patterns.*

---

## Internal Terminology & Acronyms

| Term | Definition |
|------|------------|
| **Multi-touch** | Display ad campaigns that complement CTV campaigns (retargeting via display) |
| **Mountain Match** | MNTN's proprietary targeting system (replaced interest audiences) |
| **Fangorn** | IP-level scoring model (0-1 score per IP per advertiser). Currently all high-intent IPs scored at flat 10,000 |
| **BUK (Bottoms Up Keywords)** | Data-driven keyword recommendation via ALS collaborative filtering model (TI-273, Paused). Replaces LLM-only MM V2 with pixel-data-driven recommendations |
| **DAR (Dynamic Attribute Recommendations)** | Original name for the BUK initiative |
| **ALS (Alternating Least Squares)** | Collaborative filtering matrix factorization model used in BUK. Users=advertisers, items=DS19 keywords |
| **Mountain Match V2 (MNTN Matched)** | Current production keyword system. LLM-based, homepage scrape → 20 parent → ~200 child → DS19 alignment |
| **DS19 (Data Source 19)** | The targetable keyword universe (~20,000 keywords as `data_source_category_id`). Used in audience expressions |
| **Continuous Scoring** | Planned initiative to blend BUK keyword rankings + Fangorn IP scores via DCG, replacing flat 10K scoring |
| **DCG (Discounted Cumulative Gain)** | Method to convert BUK keyword ranks into per-IP scores based on which keywords the IP visited |
| **Parent keywords** | User-facing keyword labels in UI (LLM-generated from clustered child keywords) |
| **Child keywords** | DS19 keyword IDs in the audience expression (not shown to customers) |
| **Shopper Graph API** | Internal API serving keyword recommendations per advertiser (both MM V2 and BUK). URL: `shopper-graph.in.mountain.com/autopilot` |
| **Feature Store** | Airflow-based pipeline for BUK features, recently migrated to VS (Vertex/Spark) |
| **Campaign splits** | (Planned) Ability to split a live campaign's audience for experimentation |
| **IVR** | Impression-to-Visit Rate (primary performance metric) |
| **VCR** | Video Completion Rate |
| **Lift Lab** | External geo-based lift measurement partner |
| **PEX** | Team involved in identifying beta candidates for features |
| **CS** | Customer Success team |
| **Hidden campaigns** | Non-customer-facing experiment campaigns invisible to advertisers in UI/reporting |

---

## Update Log

| Date | Source | What was added |
|------|--------|----------------|
| 2026-03-30 | Initial creation | Template structure |
| 2026-03-30 | Kirsa meeting (TI-504) | Multi-touch history, Mountain Match, attribution defaults, org/people, metrics philosophy, industry context, terminology |
| 2026-03-31 | Alex Knorr meeting (BUK) | BUK/ALS/DAR/DS19/Fangorn/DCG/Continuous Scoring terminology, Shopper Graph API, Feature Store, parent/child keywords |

---

## Key People — Data Science / Targeting

| Person | Role / Context |
|--------|---------------|
| **Alex Knorr** | Lead on BUK (Bottoms Up Keywords) model development. Built ALS pipeline, experiment design, scoring methodology |
| **Brian** | Also involved in BUK development |
| **Victor** | Infrastructure/compute for BUK pipeline (Databricks budget, DAG management) |
| **Matt** | Working on Fangorn continuous scoring; proposed DCG-based IP-level scoring approach |
| **Michelle** | Presented beta BUK campaign performance results |
| **Richard** | Provided critical feedback on BUK experiment results ("numbers are bullshit" — size confounding) |
| **Mike** | Sees value in BUK but needs clearer performance signal |
| **Allison** | Sees value in BUK, involved in prioritization decisions |
