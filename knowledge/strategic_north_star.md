# Strategic North Star — Q2 2026
Last updated: 2026-04-17

This is the **leverage filter** for all work. Every ticket, every task, every hour should be evaluated against this document. If work doesn't connect clearly to the objectives below, question whether it's the highest-leverage use of time.

---

## The One Question

**"Is this the absolute highest-leverage thing I could be doing right now?"**

Before starting any task, answer:
1. Does this directly advance a Q2 OKR or leadership priority?
2. Is there something else that would have more impact on revenue growth, revenue retention, or cost reduction?
3. Would Kale, Paulo, or Richard consider this a good use of time if they saw it?

If the answer to #1 is no, or #2 is yes — stop and reprioritize.

---

## Leadership Priority Stack (in order)

### Tier 1: Direct Leadership Asks
Anything directly requested by Kale, Paulo, or Richard. These are paramount and override everything else. Current examples:
- **BUK keyword Loom for Paulo/Richard** (TI-829) — demonstrate BUK value to executive leadership
- **Incrementality experiment design** (BER-2250) — prove whether intent targeting generates incremental lift

### Tier 2: Q2 OKR-Aligned Work
Work that directly maps to the Q2 OKR table (from PM planning doc). These are the committed outputs.

### Tier 3: Infrastructure / Knowledge That Accelerates Tier 1-2
Work that makes Tier 1-2 faster or better — data catalog updates, pipeline improvements, reusable tooling. Only justified when it clearly accelerates higher-tier work.

### Tier 4: Everything Else
Nice-to-haves, exploratory work, cleanup. Should be deprioritized or eliminated unless explicitly approved.

---

## Q2 2026 OKRs — TI Squad Relevant Work

From PM planning doc (2026-04-06):

| Epic | Q2 Deliverable | Malachi's Role |
|------|---------------|----------------|
| **BER-2250: Incrementality Overhaul** | Audience deciles for advertiser experimentation (AUD-5221/TI-831), intent score shuffling experiment | **Primary owner** — experiment design, measurement, analysis |
| **TI-606: MNTN Matched AI (Continuous Scoring)** | TI-816: Continuous Scoring POC/MVP | Contributor — BUK keyword model feeds into continuous scoring |
| **TI-189: Audience Intent Scoring** | TI-457: Enhancements for GA (Fangorn Rollout) | Contributor — BUK integration with Fangorn |
| **TI-189: Audience Intent Scoring** | TI-718: Modularize and extend model inputs (q2) | Contributor — feature engineering |
| **TI-784: B2B Audience Curation** | TI-786: Interest Segment Quality Score Implementation | Possible contributor |
| **PRO-118: Campaign Objectives V3** | TI-639: ROAS/CPA Audience Scoring Model | Possible contributor |

### The Big Bet: Incrementality (KALE'S #1 PRIORITY — 2026-04-08)

**Kale's words: "The most valuable thing right now is getting this incrementality thing out. Everything regresses to incrementality / incremental ROAS. Solving this would be HUGE and would dramatically change growth and retention."**

The single highest-leverage initiative for Q2 is **making MNTN look good on third-party incrementality platforms** (LiftLab, Kochava). This is existential-level importance:

- MNTN almost certainly looks bad on external incrementality because **everything is optimized toward the visit** — internal metrics overstate true incrementality (TI-835 confirmed: guid_log ~0% lift vs clickpass 2-8x lift)
- **Incremental ROAS** is the top metric — not incremental visits, not impressions
- If advertisers trust LiftLab, **we have to trust LiftLab** — need a dedicated LiftLab liaison/DS
- Customers care about incrementality at lifecycle inflection points: evaluating CTV as a new channel, periodic budget planning ("should I pour more or less into CTV?")
- OKR: **Run 5 experiments with external vendors**

**Strategic shift (Kale direction):**
- **Shutter internal incrementality dashboards** — move to approved third-party vendors
- Messaging: "We changed the way we do incrementality"
- Customer-driven: ask advertisers what they want (reach, performance, incrementality) → tailor their experience
- **Change targeting methodology** so we're incremental, not just attributed
- CPM pricing means incrementality changes don't directly hit profit, but IVR performance metrics will suffer

**The uncomfortable truth (TI-835 observational finding):**
Our internal attributed-visits metric (clickpass_log) shows 2-8x lift — but total site traffic (guid_log) shows ~0% lift. External vendors like LiftLab measure something closer to guid_log. **The gap between our internal metrics and external measurement IS the problem to solve.**

This is why BER-2250, the shuffling experiment, and external vendor partnerships are the top priority.

---

## Q2 Delivery Framework

From Rogus (2026-04-06): **"Shifting to Output-Driven Delivery"**

Four focus areas for Q2:
1. **Scope** — Clear boundaries on what's in and out for each sprint and project
2. **Risks** — Proactive identification, tracking, and mitigation of blockers
3. **Timelines** — Realistic dates with clear milestones and accountability
4. **Deliverables** — Defined outputs for every sprint, tied to project goals

Updated ceremonies:
- **Recurring Project Syncs** — Weekly, cross-functional alignment
- **Shorter Standups** — Daily, focused on blockers/progress/needs
- **Dedicated Backlog Grooming** — Weekly, refine/estimate/prioritize
- **More Sprint Rigor** — Tighter commitments, clearer acceptance criteria, consistent velocity

---

## The Leverage Test

When evaluating any piece of work, score it:

| Factor | Question | Weight |
|--------|----------|--------|
| **Leadership proximity** | Is Kale/Paulo/Richard waiting on this? | Highest |
| **Revenue impact** | Does this protect or grow revenue? | High |
| **Retention impact** | Does this reduce churn risk? | High |
| **Cost impact** | Does this reduce spend or waste? | Medium |
| **Velocity multiplier** | Does this make future work significantly faster? | Medium |
| **Visibility** | Will leadership see and value this output? | Medium |
| **Learning value** | Does this build critical knowledge for future high-leverage work? | Low-Medium |

**Red flags that work is low-leverage:**
- Nobody asked for it and it doesn't map to an OKR
- It's cleanup/polish on something already shipped
- It optimizes something that isn't a bottleneck
- It's interesting but doesn't connect to revenue/retention/cost
- It could be done later with no consequence

---

## Q2 2026 — Three Strategic Themes (Mike Dolt, Q2 Roadmap, 2026-04-17)

From Mike Dolt's Q2 Targeting Roadmap presentation. **31 line items** committed for Q2, organized under three themes. All three feed into the North Star: Mountain Matched AI.

### Theme 1: B2B Expansion
Expanding ICP (Ideal Customer Profile) into B2B via integrations, data sources, and audience types.
- **6 integrations slated:** Tillium, Klaviyo, Ours Privacy, HubSpot V2, Freshman, Bombora (list may shift — Jason Puertas confirmed integration priorities are moving fast)
- **Mountain Mesh for B2B:** Existing Mountain Mesh doesn't work well for B2B (can't infer targeting from website alone — need company size, industry, target persona inputs)
- **Waypoint Targeting:** Targeting users who dropped off at various points in the B2B sales cycle
- **Interest Segment Quality Score:** Evaluating quality of ~20K interest segments so we can recommend effectively (Alex's prior-quarter work continuing)
- **B2B Firmographic Curation:** Building out firmographic data — currently P3, priority may shift
- **B2B customer identification:** Mechanism TBD — possibly a toggle during PTV onboarding (self-sign-ups go to Express, PTV requires salesperson)

### Theme 2: Incrementality
Standardize third-party lift measurement as a first-class capability. Build experimentation framework for advertisers and internal use.
- Incrementality measurement produces signal that makes targeting models smarter (every lift study generates training data for Fangorn and future models)
- **Incrementality as a separate scoring model** is being discussed — working alongside Fangorn, trained for different outcomes
- **Audience deciles** for advertiser experimentation (directly enabling the experimentation framework)
- **Unified customer experiments audience** — currently P3, priority may shift
- External vendor validation (LiftLab, Kochava) continuing

### Theme 3: Mountain Matched AI (North Star)
Next-gen targeting powered by continuous scoring, graph-based identity, and smarter ML models. **Multi-quarter bet — shipping piece by piece, not all at once.**
- **10 initiatives**, 3 already in development (Fangorn)
- **What's shipping Q2:**
  - Fangorn rollout (end of April target)
  - Continuous scoring (high probability ships with Fangorn)
  - Campaign objectives models (ROAS/CPA-specific)
  - Modularize and extend model inputs (TI side)
  - DAR/BUK integration with Fangorn
- **Identity & Data Quality:**
  - Identity graph integration (starting slow — scoring signals + CRM exclusions first)
  - Customer profile recommendations quality (Victor's work)
- **Select products:**
  - LLM text audience expression (needed for Select, aligns with MM AI plans)
  - Mountain Matched Awareness Audience (max reach/awareness model for Select)
  - Permel Universal Optimization Controller integration (future pacing)

### Technical Investments (Keeping the Lights On)
- **MembershipDB resilience**
- **Targeting-wide monitoring dashboard** (similar to Mission Control — daily check for service health, DAG status, pipeline issues)
- **Audience overlays** (needed for Fangorn rollout; hack in place until overlays ship)
- **Geo-changes:** Move geo-resolution from targeting to Bidder. Bidder has geo data from bid request. **Currently missing 20-25% of bids** because we lack geodata for those IPs — significant improvement opportunity.
- **Public API:** Move business logic from Gary to Audience Service
- **Select: Move from targeting all US to specifically who they need**
- **Feature inventory and data quality evaluations** (TI side — know what data we have, assess quality)

### Key Insight: B2B + Incrementality Are Foundations for MM AI
Both B2B and incrementality work are not separate from Mountain Matched AI — they are literally the foundation pillars that Q3/Q4 MM AI will be built on.

### MNTN Express and Targeting
Express clients **don't install Pixel** — no performance metrics available. Targeting has no meaningful role until Express adds Pixel and performance metrics. This will come later down the line.

---

## Active High-Leverage Work (Current)

### BER-2250: Incrementality Overhaul
- **Why highest leverage:** Answers whether MNTN's core targeting creates incremental value. Existential for the business model.
- **Malachi's deliverables:**
  - TI-835: Control group design and measurement methodology
  - TI-837: Implementation plan for intent score shuffling
  - TI-839: Results measurement and follow-up
  - TI-842: Presentation of results to broader audience
  - TI-831: Audience deciles for advertiser experimentation

### TI-813: BUK 500 Advertiser Scale
- **Why high leverage:** Validates BUK keyword model at scale. Feeds into continuous scoring (TI-816) and Fangorn improvements. Direct Paulo/Richard visibility via Loom (TI-829).

### TI-810: Feature Store Pipeline (Bidstream)
- **Why high leverage:** Enables DS13/DS19 audience augmentation. Part of TI-789 epic. Infrastructure that multiplies targeting capability.

---

## How AI Should Use This Document

1. **At session start:** Read this document. Understand the current priority stack.
2. **When starting a task:** Check it against the Leverage Test. If it scores low, flag it.
3. **When the user creates or picks up a ticket:** Ask: "Is this the highest-leverage thing right now? Here's how it scores against the north star."
4. **When prioritizing Todoist:** A-priority = Tier 1-2 work only. B-priority = Tier 3. C/D/E = Tier 4.
5. **When work surfaces mid-session:** Evaluate immediately against this framework before creating tasks.
6. **Proactively suggest:** If you notice the user spending time on Tier 4 work while Tier 1-2 items are pending, say so.

<!-- slack-extracted: 2026-04-08-full -->
- ### Incrementality vs. Continuous Scoring Priority (April 2026)

As of the Q2 kickoff, **incrementality is the priority over continuous scoring**. The sequencing is:

1. A spike must be completed to create a concrete plan for all incrementality work
2. Timeline: incrementality learnings are expected by end of April
3. Those learnings will inform and feed into the continuous scoring release at end of Q2

This was confirmed by Bryce Wagg after syncing with leadership.
- ### Incrementality OKR — Two Distinct Workstreams

The "Incrementality" initiative (BER-2250 umbrella) contains two separate workstreams that were initially conflated:

**1. Incrementality Experiment (BER-2250 discovery)**
Does MNTN's current intent targeting generate incremental lift? Uses score shuffling and ITT (Intent-to-Treat) methodology as described in the experiment brief. This is discovery work that must come first.

**2. Population Split / "Deciles" (AUD-5221)**
Enables customer-facing A/B testing by splitting the entire US IP universe into 10 randomly assigned groups (US Population 1–10). Advertisers can select even/odd groups to create clean A/B campaign splits with any variable (e.g., creative) tested across them. This is the same mechanism used by The Trade Desk.
- Groups must be kept current as IPs rotate in/out (cadence TBD: daily or weekly)
- Implemented via a new data source; existing MemDB hash mechanism may be reusable
- Owned by TI team (confirmed); timeline TBD pending design decision on random vs. intent-stratified deciles

These are tracked separately in Jira but both fall under the Incrementality OKR.
- ### Fangorn Experiment Results — Current Lift by Intent Tier (as of April 2026)

The Fangorn V2 experiment (EX50) is showing the following average IVR lift figures, tracked via the experimentation team's Mode dashboard:

- **High Intent:** 11.61% lift
- **Mid Intent:** 9.78% lift
- **Mid Intent with Peak Performance:** 11.2% lift
- **Peak Performance:** 36% lift

The original OKR target was 10% lift in Visit Rate for 5 verticals, benchmarked against 2025 YTD numbers. A non-technical stakeholder explainer has been published to Confluence. A dashboard to track ongoing progress is planned post-MNTN Meet (Nick is the Mode resource).

**Note:** The Fangorn audience lookalike support epic (TI-462) was confirmed complete as of this period.
- ### Vertex Scoring Pipeline — Advertiser vs. Vertical Level Decision

The full advertiser-level scoring pipeline (TI-798) for Vertex was tested at scale across ~9,800 advertisers but hit a 7-hour TTL on the cluster. As a result, the team pivoted to **vertical-level scoring** for the initial deployment:

- Vertical-level run: ~30 minutes on the same cluster (to be scaled back before deployment)
- Full advertiser-level run: ~$200/hour, unsustainable for daily cadence currently
- Eventual goal is daily advertiser-level scoring; vertical-level is the interim approach

The Dataproc cost spike observed around March 27, 2026 was attributed to testing this pipeline (not to IPDSC changes).

<!-- slack-extracted: 2026-04-16 -->
- **Fangorn Rollout — Approved Tier Allocation and Next Steps:** Leadership formally approved the Fangorn phased rollout. Tier allocation: 44% Tier 1 / 40% Tier 2 / 16% Tier 3. Sales enablement and GTM preparation are being coordinated. A tech blog post has been initiated by Bryce and Kale at Richard's request. The Mid-Intent + Peak Performance logic bug (Treatment side only) was identified and a fix is scoped for the production rollout. (via Bryce Wagg, #dev_fangorn-model_ex, 2026-04-01)

<!-- slack-extracted: 2026-04-17 -->
- **Bidstream Data (augmentor_log) as Replacement for 33Across Site Visit Signal Spend**

Analysis (TI-647) found that replacing 33Across data with internal augmentor_log/bid_event data for Site Visit Signals could save approximately $21K/month:
- Data source 28 (33Across): ~$45K/month spend, ~38.6% match rate → estimated $17K savings
- Data source 40 (33Across API): ~$27K/month spend, ~13.5% match rate → estimated $4K savings

Additional advantages of using internal bidstream data over 33Across:
1. Data is available sooner than 33Across delivery.
2. Can be used in models without incremental cost.
3. Contains net-new signals not present in third-party data.
4. Naturally filters to IPs MNTN can actually bid on, reducing noise.

Next step: TI-657 implements the augmentor_log/bid_event data integration into Site Visit Signals. (via Ryan Kleck, #tgt-infrastructure-squad, 2026-04-16)

<!-- slack-extracted: 2026-04-21 -->
- **Incrementality Dev Channel (#dev-incremental-lift) established April 2026:** A dedicated engineering channel was created to coordinate MNTN's incrementality and experimentation initiative. Al Beretta described three work themes: (1) Holdout and experimentation infrastructure — ghost bidding, audience segmentation, campaign isolation for A/B and multivariate testing; (2) Partner integrations — connecting with measurement partners (LiftLab and others) so experiment parameters, reporting data, and lift study results flow in and out of the platform; (3) New reporting — surfacing incrementality and A/B testing results natively in-platform. The bulk of Q2 work is on the TI squad or related to LiftLab. Mike Dolzer noted a project plan was forthcoming. (via Al Beretta, #dev-incremental-lift, 2026-04-20)
