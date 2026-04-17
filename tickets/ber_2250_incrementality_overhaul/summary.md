# BER-2250: Incrementality Overhaul

**Jira:** https://mntn.atlassian.net/browse/BER-2250
**Status:** In Progress
**Date Started:** 2026-04-06
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Initiative to prove whether MNTN's intent tier targeting generates **incremental** lift, or whether we're buying audiences who would have converted anyway. This is the single highest-leverage initiative for Q2 2026.

MNTN's high-intent targeting concentrates spend on the same audience segments targeted by Meta and Google. If our marginal contribution to conversion is low, we're charging customers for outcomes they would have achieved without us — a retention and differentiation risk.

**Product Brief:** [Intent Score Shuffling — Confluence](https://mntn.atlassian.net/wiki/external/NTM1ZmViMzc1YzczNDQ0YjgzZDVlMjdkNTk2ZGY4NmY)

## 2. The Problem

We currently measure incrementality against a counterfactual, but we have **never tested whether the intent scoring methodology itself drives incremental lift**. Without that test, we cannot determine whether the scores we use to allocate spend are optimizing for incrementality or simply for conversion correlation.

- Advertisers increasingly demand proof of incremental lift, not just attribution
- If competitors (Meta, Google) can claim the same conversions on the same audiences, MNTN's value proposition narrows to reach and format, not performance
- The inability to demonstrate incrementality becomes a churn driver as advertisers mature in their measurement sophistication

## 3. Three Workstreams Under BER-2250 (Alex Bloore, 2026-04-08)

**Workstream 1: Intent Score Shuffling Experiment (THE PRIORITY — product brief)**
- Discovery work — "needs to happen FIRST" (Alex Bloore)
- TI squad: shuffle IPs between HI/MI tiers, log original scores
- RX squad (or TI with RX consultation): ITT measurement
- Tickets: TI-837 (design + implement), TI-839 (measure results), TI-842 (present)

**Workstream 2: Population Split / Deciles (separate from experiment)**
- Random 10-group split of all US IPs for customer A/B testing (like Trade Desk)
- Customers select even/odd groups, layer their segments on top
- Ticket: TI-831

**Workstream 3: Observational Analysis (our initiative, not in brief)**
- Use existing 10% holdout to measure baseline incrementality NOW
- Ticket: TI-835
- **COMPLETE (2026-04-08):** "The Two Stories" finding — guid_log ~0% lift, clickpass_log 2-8x lift. See TI-835 summary.

### Key Insight: Control Group Already Exists
Every campaign has a **10% holdout group** — `MD5('{AID}:{IP}') mod 1000`, bucket 0-99 = holdout. Per-advertiser per-IP. No shuffling needed for observational analysis.

### Kale's Strategic Direction (2026-04-08)
**"The most valuable thing right now is getting this incrementality thing out. Solving this would be HUGE and dramatically change growth and retention."**

- **Incremental ROAS** is the top metric — not visits, not impressions
- MNTN almost certainly looks bad on external incrementality (LiftLab/Kochava) because everything is optimized toward the visit
- **TI-835 confirms this:** internal attributed-visits metric (clickpass_log) shows 2-8x lift, but total site traffic (guid_log) shows ~0% lift. External vendors measure closer to guid_log.
- **Strategic shift:** shutter internal incrementality dashboards → move to approved third-party vendors
- OKR: **run 5 experiments with external vendors**
- Customer-driven: ask advertisers what they want (reach, performance, incrementality) → tailor experience
- Need a dedicated LiftLab liaison/DS
- CPM pricing → incrementality changes don't directly hit profit, but IVR metrics will suffer
- If we adjust targeting for incrementality, IVR performance will look worse — but it's the right thing to do

### Key Tension: Performance vs Incrementality
Optimizing for incrementality and visit rate are partially opposed (Matt Brorby, confirmed by Kale). Kale's direction: incrementality wins. If we start adjusting for incrementality, IVR performance will suffer, and TI will appear to perform worse on current metrics. But incremental ROAS is what matters.

### HHST Reality (2026-04-08)
All scored IPs get flat HHST=10000 (HI). Per-tier analysis not possible until continuous scoring rolls out. PP at 8000 is planned but not active. Aggregate analysis only for now.

## 4. Investigation & Findings

### Matt Brorby Sync (2026-04-07)
- 10% holdout exists on all campaigns — use this as control (no shuffling needed for baseline)
- ITT methodology: compare ALL IPs in 90% targeted group vs 10% holdout, regardless of actual impression delivery
- Nick has the holdout identification query
- Kristen may already be doing related work (#chapter-data-analytics)
- Phase 2 idea: train a model on lift directly, using impression receipt as a feature
- Alex Bohr is the product lead on incrementality (identity team)
- Performance vs incrementality trade-off is a real tension — need leadership direction

### Matt Brorby Sync (2026-04-08) — Incremental ROAS & Industry Context
- **"Everyone suspects intent scoring is just capturing people who would visit anyway."** — validates TI-835 guid_log ~0% lift finding
- Matt's prior role: measured incremental ROAS for mobile (deterministic, app installs)
- **Time-delta bucketing method:** bucket users by time from ad impression to conversion event. Short windows (5s) ≈ 100% incremental. Signal becomes "barely noticeable" beyond ~6.5 hours for apps. Has a published article on this.
- **Incremental ROAS benchmarks:** Good advertisers ~$0.90/dollar. Poor ~$0.50. Trade Desk ~$1.15 (considered good). Over $1.00 is rare and "awesome." Companies claiming $8 ROAS are measuring attributed, not incremental.
- **CTV-specific challenges:** Not deterministic (IP-based), long conversion windows (weeks), cellular IP noise, signal-to-noise degradation at longer intervals. Should filter out cellular IPs via identity graph. Matt hasn't analyzed CTV yet — "might be totally different."
- **LiftLab:** Paid by the advertiser → bias toward conservative measurement. MNTN is "at the mercy of these third parties."
- **Ensemble approach:** "No one model to rule them all." IVR model for performance-focused advertisers, incremental ROAS model for incrementality-focused ones. Only applies to opt-in advertisers.
- **Internal dashboards:** Matt noted he's "never seen negative or zero incrementality" on MNTN's internal incrementality dashboard — always positive. Suggests internal dashboards overstate.
- Confirmed Jira restructure: TI-835/837/839/842 should be under a new EPIC (not TI-831). Decile work is separate, owned by Sean/Ryan/Zach/Jordan.

### TI-835 Observational Results (2026-04-08) — "The Two Stories"
- **guid_log (all pixel visits):** Holdout share ~10% across all 9 advertisers = no incremental lift on total site traffic. CTV ads don't generate net new visits.
- **clickpass_log (VV-attributed visits):** Holdout share 1.3-5.1% = 2-8x incremental lift on attributed visits. All 9 advertisers significant at p < 0.001 after FDR correction.
- **Interpretation:** CTV ads capture attribution, not new traffic. Same people who'd visit anyway arrive through the MNTN VV redirect path. Internal metrics overstate true incrementality. External vendors (LiftLab) will measure something closer to guid_log.
- Full details in [TI-835 summary](ti_835_control_group_design/summary.md).

### Fangorn Experiment Results — Current Lift by Intent Tier
From the experimentation team's Mode dashboard (EX50):
- **High Intent:** 11.61% IVR lift
- **Mid Intent:** 9.78% lift
- **Mid Intent with Peak Performance:** 11.2% lift
- **Peak Performance:** 36% lift

Original OKR target was 10% lift in Visit Rate for 5 verticals.

### Incrementality Priority Sequencing (Bryce Wagg, confirmed with leadership)
1. A spike must be completed to create a concrete plan for all incrementality work
2. Incrementality learnings expected by end of April 2026
3. Those learnings will inform continuous scoring release at end of Q2
4. **Incrementality is priority over continuous scoring**

## 5. Solution

### What We Know So Far (TI-835)
CTV ads don't increase total site traffic but dramatically increase MNTN-attributed visits. The gap between internal attribution metrics and external incrementality measurement is the core problem to solve. Incremental ROAS (not visits) is the metric that matters.

### ITT Results (April 2026) — Zero Lift Detected
No statistically significant incrementality under ITT across any intent tier. Root cause: **coverage dilution** — only 14-16% of treatment group actually receives impressions; remaining 86% behaves identically to holdout. This is structural, not a statement that ads don't work.

### Pivot: Ghost Bidding Replaces Shuffling (April 2026)
Intent score shuffling has been replaced by **ghost bidding methodology** + **dedicated mid-intent experiment**:
- **Ghost bidding:** Compare exposed treatment IPs vs pseudo-exposed holdout IPs (matched via campaign win rate applied to holdout bid stream appearances)
- **Pure mid-intent treatment group** — not shuffled mix — to generate stronger signal
- **ATT (Average Treatment on the Treated)** instead of ITT to eliminate coverage dilution
- **Target deadline:** April 30th for experiment setup
- Trade Desk previously built this methodology; Alex Bloore involved in alpha testing at Goodway

### Action Items (from meeting)
- **Malachi:** Build ghost bidding methodology for experiment analysis framework
- **Malachi:** Set up dedicated mid-intent campaign experiment with Experiments team
- **Alex Knorr:** Review findings and recommendations with Malachi
- **Kyla:** Program management connecting all incrementality workstreams
- Defer to Kirsa and Nick for experiment sizing (budget, advertiser selection)
- Confirm whether bidder logging changes can ship by April 30th or use win rate approximation

### Fellowship System (Alex Knorr, April 2026)
Conceptual framework for balancing performance and incrementality long-term:
- Toolbox of independent targeting models (conversion, incrementality, new-to-brand, keyword intent)
- Combination engine with adjustable weights per campaign/advertiser goal
- Bayesian updating feedback loop adjusts weights based on outcomes
- Connects to continuous scoring roadmap

### What We Need to Determine
- Whether bidder logging changes can ship by April 30th or use win rate approximation
- Exact experiment budget and advertiser selection (Kirsa + Nick)
- Cross-squad coordination: Jason (partner vendors), Megan (experimentation), TI (methodology)

## 6. Questions Answered

- **Q:** Is our intent targeting generating incrementality, or are we buying audiences who would have converted anyway?
  **A:** Partially answered. TI-835 shows CTV ads don't increase total site traffic (guid_log ~0% lift) but do increase MNTN-attributed visits (clickpass_log 2-8x lift). The full answer requires external vendor validation and per-audience-segment analysis.

- **Q:** What does "good" incremental ROAS look like?
  **A:** Industry benchmarks (Matt Brorby): good = ~$0.90/dollar, poor = ~$0.50, Trade Desk = ~$1.15. Over $1.00 is rare. Companies claiming $8 ROAS are measuring attributed, not incremental.

- **Q:** Will adjusting for incrementality hurt performance?
  **A:** Yes. Kale confirmed this is expected and acceptable — only for advertisers who opt in. CPM pricing means profit isn't directly affected, but IVR metrics will look worse.

## 7. Data Documentation Updates

- Added Intent Score Shuffling section to `knowledge/experimentation.md` (ITT methodology, design, parameters)
- Added incrementality initiative context to `knowledge/mntn_business.md`
- Created `knowledge/strategic_north_star.md` with Q2 OKR leverage framework
- Added incremental ROAS benchmarks and CTV challenges to `knowledge/experimentation.md`
- Added guid_log vs clickpass_log methodology lesson to `knowledge/experimentation.md`
- Added customer lifecycle / pricing impact / external vendors to `knowledge/mntn_business.md`
- Added BQ holdout hash and visit source comparison to `knowledge/data_knowledge.md`

## 8. Open Items / Follow-ups

- [x] TI-835: Observational analysis — **COMPLETE** (two stories finding)
- [ ] TI-856: Research LiftLab methodology — prerequisite for external experiments
- [ ] TI-857: Plan and scope 5 external vendor experiments (Q2 OKR)
- [ ] TI-858: Identify which targeting audiences are incremental vs not
- [ ] TI-859: Expand holdout bucketing infrastructure (Zach + Jordan)
- [ ] TI-837: Implementation plan — which systems change, how scores are logged, rollback
- [ ] TI-831: Audience deciles — even/odd targeting (separate customer-facing A/B tool)
- [ ] Coordinate with RX squad on ITT reporting requirements
- [ ] Determine minimum experiment duration for statistical power
- [ ] Establish LiftLab liaison/DS relationship

## Jira Structure (updated 2026-04-08)

```
BER-2250: Incrementality Overhaul (Initiative)
├── TI-831: Audience Deciles for Advertiser Experimentation (separate workstream)
└── TI-855: Incrementality Experimentation & External Vendor Validation (EPIC) ← NEW
    ├── TI-835: Observational incrementality analysis (10% holdout) — COMPLETE
    ├── TI-837: Design and implement intent score shuffling experiment — Backlog
    ├── TI-839: Measure incrementality results — Backlog
    ├── TI-842: Present results to broader audience — Backlog
    ├── TI-856: Research LiftLab methodology — NEW
    ├── TI-857: Plan 5 external vendor experiments (Q2 OKR) — NEW
    ├── TI-858: Identify incremental vs non-incremental audiences — NEW
    └── TI-859: Expand holdout bucketing infrastructure (Zach + Jordan) — NEW
```

## Child Tickets

### TI-855: Incrementality Experimentation & External Vendor Validation (EPIC)

| Ticket | Summary | Status | SP |
|--------|---------|--------|----|
| [TI-835](https://mntn.atlassian.net/browse/TI-835) | Observational incrementality analysis (10% holdout) | **Complete** | 3 |
| [TI-837](https://mntn.atlassian.net/browse/TI-837) | Design and implement intent score shuffling experiment | Backlog | 5 |
| [TI-839](https://mntn.atlassian.net/browse/TI-839) | Measure incrementality results | Backlog | 5 |
| [TI-842](https://mntn.atlassian.net/browse/TI-842) | Present results to broader audience | Backlog | 3 |
| [TI-856](https://mntn.atlassian.net/browse/TI-856) | Research LiftLab methodology | Not Started | 3 |
| [TI-857](https://mntn.atlassian.net/browse/TI-857) | Plan 5 external vendor experiments (Q2 OKR) | Not Started | 5 |
| [TI-858](https://mntn.atlassian.net/browse/TI-858) | Identify incremental vs non-incremental audiences | Not Started | 5 |
| [TI-859](https://mntn.atlassian.net/browse/TI-859) | Expand holdout bucketing infrastructure | Not Started | 5 |

### TI-831: Audience Deciles (separate workstream)

| Ticket | Summary | Status | SP |
|--------|---------|--------|----|
| [TI-831](https://mntn.atlassian.net/browse/TI-831) | Audience Deciles for Advertiser Experimentation | Not Started | 5 |
