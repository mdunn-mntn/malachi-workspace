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

*Add MNTN-specific jargon, acronyms, and terminology that isn't obvious from the codebase.*

---

## Update Log

| Date | Source | What was added |
|------|--------|----------------|
| 2026-03-30 | Initial creation | Template structure |
