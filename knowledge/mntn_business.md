# MNTN Business Knowledge
Last updated: 2026-04-08

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
- **Jack** — Engineering manager for identity team (took over role ~March 2026).
- **Alex Floor** — Covering product leadership for targeting during GPM search (alongside Mike Dole and Kale).
- **Mike Dole** — Covering product leadership for targeting during GPM search (alongside Alex Floor and Kale).

### Strategic Direction Shift (Kale, 2026-03-31)
- **Incrementality is the new north star for targeting.** Kale is sharpening the TI team focus: Fangorn and prediction models will narrow toward incrementality, not just intent/ROAS optimization.
- **Rationale:** Purely exploitative optimization (targeting highest-intent users) hurts incrementality — those users are already targeted by Google/Meta, so MNTN's incremental lift looks weak. The system needs exploration (multi-armed bandit thinking) alongside exploitation.
- **Practical implication:** Media plan budget allocation, Fangorn scoring, and feature development will all be evaluated through an incrementality lens. Plan forthcoming from Kale.
- **BUK (Bottoms Up Keywords) is NOT dead** — deprioritized due to low beta adoption, but Kale sees keywords as a valid feature in the predictive model. The interface (exposing keywords as a separate audience mechanism) is the bigger concern, not the underlying signal.

### Q2 2026: Output-Driven Delivery (Rogus, 2026-04-06)
- **Shift to output-driven delivery** — clearer scope, proactive risk identification, realistic timelines, defined deliverables per sprint
- **Updated ceremonies:** Weekly project syncs, shorter daily standups (blockers/progress only), weekly backlog grooming, tighter sprint commitments
- **Engineering Levels & Skills Rubric released** — specific per-level criteria for Speed (Delivery/Volume), Craft (Technical Quality), Adaptability (Responsiveness/Ownership). Next official review ~April 2027, manager feedback within ~1 month, mid-point ~November 2026.

### Incrementality Initiative (BER-2250, Q2 2026)
- **Phase 1 (now): Observational analysis** — use existing 10% holdout group (IP hash, last 2 digits < 10) to measure baseline incrementality by intent tier. No experiment needed.
- **Phase 2 (contingent): Intent Score Shuffling** — shuffle IPs between intent tiers to causally confirm observational findings. Only if Phase 1 shows incrementality differences.
- **Phase 3 (future): Lift-optimized model** — train a model on incremental lift directly, using impression receipt as a feature (Matt Brorby concept).
- **ITT (Intent to Treat) methodology** — measure by assigned group, not actual impression delivery, to avoid selection bias
- **Key tension (Matt Brorby, 2026-04-07):** Performance (visit rate) vs incrementality (lift) are partially opposed. High-intent = high VR, low lift. Low-intent = low VR, high lift. Need leadership direction on balance.
- **Business stakes:** If low incrementality → retention risk (charging for outcomes that would've happened). If proven → competitive moat vs Meta/Google.
- Product brief: [Confluence](https://mntn.atlassian.net/wiki/external/NTM1ZmViMzc1YzczNDQ0YjgzZDVlMjdkNTk2ZGY4NmY)

### Three Main Experimentation Focus Areas (as of March 2026)
1. **Targeting** — Fangorn, bottoms-up keywords, audience optimization. Most impactful lever.
2. **New ad formats** — Vertical video, pause ads
3. **Lift and incrementality** — New-to-brand rates, lift test performance improvement, geo-based lift testing with partners (e.g., Lift Lab). **Elevated to top priority for Q2 2026 via BER-2250.**

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

### Pricing Model & Incrementality Impact (Kale, 2026-04-08)
- MNTN charges on **CPM** (cost per thousand impressions), not CPV — so changing targeting for incrementality shouldn't directly affect profit/revenue.
- **But it WILL affect performance metrics (IVR)** — if targeting is adjusted to optimize for incrementality (e.g., reaching lower-intent users), IVR performance will suffer.
- TI squad could appear to be performing worse on current metrics while actually improving incrementality.
- Incrementality is a bucket/category of measurement — if the North Star is IVR and we adjust for incrementality, IVR suffers. This is the core tension.

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

### Lift Testing Partners & External Incrementality Vendors
- **LiftLab** — primary external partner for incrementality measurement. Keeps coming up with advertisers. Geo-based lift measurement — segments by geography and measures raw metric lift during campaign period.
- **Kochava** — another incrementality/attribution vendor option.
- Possibly more vendors in the space.
- **Strategic shift (Kale, 2026-04-08):** Plan to shutter internal incrementality dashboards and move to approved third-party vendors. Messaging: "We changed the way we do incrementality." Need a dedicated LiftLab liaison/DS on the MNTN side.
- If advertisers trust LiftLab, MNTN has to trust LiftLab — can't argue with the vendor the customer chose.
- Lift tests are a key focus area — finding ways to improve MNTN's performance in partner-run lift studies.

### Competitive Positioning on Incrementality (Kale, 2026-04-08)
- Solving incrementality would be **HUGE** — dramatically change growth and retention (Kale's words).
- MNTN almost certainly looks bad on external incrementality studies because everything is currently optimized toward the visit (high-intent users who would've converted anyway).
- This is the core strategic risk: if external vendors show low lift, it undermines MNTN's value proposition. If MNTN can prove incrementality, it becomes a competitive moat vs Meta/Google.

---

## Customer & Advertiser Context

### Incrementality in the Customer Lifecycle (Kale, 2026-04-08)
- Customers care about incrementality at specific lifecycle points:
  - **(a) Evaluating CTV as a new channel** they've never used before — "is this incremental to my existing spend?"
  - **(b) Periodic budget planning** — marketing managers ask "how incremental are my channels today vs last year, should I pour more or less into CTV?"
- Customers go in and out of incrementality measurement to make decisions. Once they figure it out, they likely shift their objective to something different (reach, performance).
- **Onboarding implication:** When advertisers come into MNTN, ask their funnel AND which feature they want: reach, performance, or incrementality — then tailor their specific experience accordingly.

### Incremental ROAS Is the Top Customer Metric (Kale, 2026-04-08)
- Not incremental visits, not incremental impressions — **incremental ROAS** is what matters.
- This is what external vendors measure and what advertisers ultimately care about.
- Internal metrics (IVR, visit rate) are operational; incremental ROAS is the customer-facing proof point.

---

## Internal Terminology & Acronyms

| Term | Definition |
|------|------------|
| **Multi-touch** | Display ad campaigns that complement CTV campaigns (retargeting via display) |
| **Mountain Match** | MNTN's proprietary targeting system (replaced interest audiences) |
| **Fangorn** | IP-level scoring model (0-1 score per IP per advertiser). Currently all high-intent IPs scored at flat 10,000. As of 2026-04-07: all-verticals support complete (Brian), final validation running. TI-457 close to ready for full rollout. TI-745 (model validation methodology) converted to spike — not blocking rollout, targeting end of Q2. |
| **BUK (Bottoms Up Keywords)** | Data-driven keyword recommendation via ALS collaborative filtering model (TI-273, Paused). Replaces LLM-only MM V2 with pixel-data-driven recommendations |
| **DAR (Dynamic Attribute Recommendations)** | Original name for the BUK initiative |
| **ALS (Alternating Least Squares)** | Collaborative filtering matrix factorization model used in BUK. Users=advertisers, items=DS19 keywords |
| **Mountain Match V2 (MNTN Matched)** | Current production keyword system. LLM-based, homepage scrape → 20 parent → ~200 child → DS19 alignment |
| **DS19 (Data Source 19)** | The targetable keyword universe (~20,000 keywords as `data_source_category_id`). Used in audience expressions |
| **Continuous Scoring** | Planned initiative to blend BUK keyword rankings + Fangorn IP scores via DCG, replacing flat 10K scoring. Architecture: scores output to Bidder team AND MembershipDB in parallel. Long-term plan is Bidder reads from MembershipDB (requires Zach/Jordan sync), but direct Bidder output continues until that path is proven. |
| **MembershipDB** | Database that stores audience/scoring data. Continuous scoring will write Fangorn scores here; Bidder team needs integration path to read from it (TBD, requires Zach/Jordan coordination). |
| **DCG (Discounted Cumulative Gain)** | Method to convert BUK keyword ranks into per-IP scores based on which keywords the IP visited |
| **Parent keywords** | User-facing keyword labels in UI (LLM-generated from clustered child keywords) |
| **Child keywords** | DS19 keyword IDs in the audience expression (not shown to customers) |
| **Shopper Graph API** | Internal API serving keyword recommendations per advertiser (both MM V2 and BUK). URL: `shopper-graph.in.mountain.com/autopilot` |
| **Feature Store** | Airflow-based pipeline for BUK features, recently migrated to VS (Vertex/Spark) |
| **Campaign splits** | (Planned) Ability to split a live campaign's audience for experimentation |
| **IVR** | Impression-to-Visit Rate (primary performance metric) |
| **VCR** | Video Completion Rate |
| **LiftLab** | Primary external incrementality measurement vendor. Geo-based lift measurement. Advertisers frequently use them. MNTN moving toward LiftLab as approved third-party vendor. |
| **Kochava** | External attribution/incrementality vendor (alternative to LiftLab) |
| **Incremental ROAS** | The top incrementality metric — what external vendors measure and advertisers care about. Not incremental visits or impressions. |
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
| 2026-03-31 | Malachi/Kale 1x1 | Strategic direction shift to incrementality, Michelle departure, Jack → identity EM, product leadership gap coverage, BUK not dead |
| 2026-04-06 | Rogus announcement | Engineering Levels & Skills Rubric released, Q2 output-driven delivery shift, updated ceremonies |
| 2026-04-06 | PM planning doc | Q2 OKR table — epic-to-deliverable mapping, incrementality initiative (BER-2250) |
| 2026-04-07 | Matt Brorby sync | 10% holdout exists on all campaigns (IP hash), observational analysis approach, lift-optimized model concept, performance vs incrementality tension |
| 2026-04-07 | TGT Infrastructure Standup | Fangorn all-verticals done (Brian, validating), continuous scoring architecture (MembershipDB + Bidder parallel), identity graph blocked on CRM rollout, Bryce/Sean/Victor/Forrest people context, Jira workflow update, MountainMeet NYC this week |
| 2026-04-08 | Kale McNaney conversation | Incrementality customer lifecycle (eval + budget planning), CPM pricing vs IVR tension, incremental ROAS as top metric, external vendors (LiftLab primary, Kochava), shutter internal dashboards strategy, competitive positioning |

---

## Key People — Data Science / Targeting

| Person | Role / Context |
|--------|---------------|
| **Alex Knorr** | Lead on BUK (Bottoms Up Keywords) model development. Built ALS pipeline, experiment design, scoring methodology |
| **Brian** | Working on Fangorn Vertex pipeline. Completed all-verticals support (merged, running final validation). Also involved in BUK development. |
| **Victor** | Infrastructure/compute for BUK pipeline (Databricks budget, DAG management). Working on TI-750 customer profile recommendations (due May 15). |
| **Sean** | Identity graph integration. Blocked on CRM rollout for all advertisers (external dependency). |
| **Matt Brorby** | Staff Data Scientist. Working on Fangorn continuous scoring; proposed DCG-based IP-level scoring approach. Wrote the lift-optimized model doc (training on impression receipt as a feature). Key advisor on incrementality methodology. |
| **Alex Bohr** | Product lead on incrementality (identity team). Wrote the Intent Score Shuffling product brief. Driving BER-2250. Believes incrementality should be the sole optimization target (no trade-off). |
| **Nicholas** | Experimentation team. Runs experiment analysis, has audience expression queries and holdout identification. Identifies experiments by parsing campaign_group name for "EX-{number}" pattern. Works with Kirsa on methodology. |
| **Kristen** | Data analytics. May be doing related incrementality intent analysis (posted in #chapter-data-analytics). Check before duplicating work. |
| **Zach Schoenberger** | Audience tools team (with Jordan). Provided the holdout hash function (`MD5('{AID}:{IP}')` mod 1000). Confirmed expression_type 1 is legacy/not read. Key contact for audience expression mechanics. |
| **Jordan** | Audience tools team (with Zach Schoenberger). Nick wants Jordan to build an "expression → IP list" tool. Key contact for audience infrastructure. |
| **Ryan Kleck** | TI team. Suggested MemDB hash reuse for deciles. Works on feature store pipeline (airflow-ti). |
| **Bryce Wagg** | TPM/Scrum Master for TGT Infrastructure squad. Runs standups, manages sprint workflow, Jira hygiene. Updated Jira workflow (2026-04-07): developer field auto-assigned on move to in-progress, must go through in-review → ready-for-deployment → done. |
| **Rogus** | Engineering leadership. Announced Engineering Levels & Skills Rubric (2026-04-06). Driving Q2 shift to output-driven delivery. |
| **Forrest** | Involved in continuous scoring POC/MVP timeline discussions. |
| **Michelle** | Former GPM for targeting. Departed ~March 2026. Presented beta BUK campaign performance results |
| **Richard** | Provided critical feedback on BUK experiment results ("numbers are bullshit" — size confounding) |
| **Mike** | Sees value in BUK but needs clearer performance signal |
| **Allison** | Sees value in BUK, involved in prioritization decisions |

<!-- slack-extracted: 2026-04-08-full -->
- ### MNTN Express — GA Launch (April 6, 2026)

MNTN Express (formerly called MNTN Go) launched to General Availability on April 6, 2026 — one month ahead of the original May target.

**What it is:** A simplified, self-serve PTV (Performance TV) application designed for small business / lower-sophistication advertisers. Built from zero to customer spend in approximately 2 months for beta.

**Routing changes at GA:**
- All self-sign-ups via Mountain.com now route to MNTN Express (PTV access only available via sales demo or manual routing)
- A new question in the sign-up flow assesses marketing expertise to route intermediate+ users back to PTV

**Key details:**
- Express domain: `express.mountain.com` (production redirects from the Go domain in place)
- Express landing page lives on the PTV corporate site (no separate website)
- Billing: no changes required
- A one-time bulk migration of churned small-business PTV accounts to `express_enabled` was performed at launch with coordinated outreach via Biz Ops, Sales, and PEX
- ### Engineering Levels & Skills Rubric (April 2026)

MNTN published an updated Engineering Levels & Skills Rubric in April 2026, available at the Engineering Confluence space. Key points:

- The rubric applies to **all IC engineering roles**: SWE, DE, DS, and DA. Language is intentionally generic across disciplines (not SWE-specific).
- Next official review cycle is planned for approximately one year from publication.
- All engineers should receive rubric-based feedback at least twice before then: once from their manager within ~1 month (translating current performance to new expectations), and once at an informal ~6-month midpoint check.
- Analysts are not capped at Senior — the rubric applies uniformly across levels for all engineering functions.
- ### Identity Graph — Scale Metrics (April 2026)

The MNTN identity graph covers approximately:
- ~150 million households
- ~1.85 billion device IFAs

**Data providers:** Experian, Deepsync, Augmentor log, GUID log
- ### Campaign Strategy (Marketing Objective) Feature — Legacy Audience Behavior

When the Campaign Strategy feature (stored as `objective_id` on `campaign_groups`, previously called Marketing Objective) was introduced (~2025), customers were prompted to select a strategy (Retargeting or Prospecting). Adoption was voluntary — customers who declined to update were not forced, and no drop-dead date was enforced.

As a result, "legacy" audiences that were never updated can still exist on live campaigns without a strategy designation or with a mismatched strategy. If a customer edits such an audience, the strategy will be locked to match the campaign's current strategy going forward.

Note: An audience can be attached to multiple campaigns. Updating it to align with one campaign's strategy may cause misalignment with another campaign it is attached to. The UI surfaces a warning in this case.
- ### TI/AUD Squad Q2 Workflow Process Changes

Effective Q2 2026, the following process changes apply to TI and AUD squads:

**Ticket hygiene:**
- Default priority is P3 (adjusted during grooming)
- `Developer` field is required on all tickets (feeds software capitalization audit)
- Avoid moving tickets directly from In Progress to Done
- `Release Type` defaults to Backend on creation — update to UI or N/A as appropriate

**New ceremonies:**
- Weekly backlog grooming (Bryce + Mike, with squad leads planned)
- Weekly project syncs per active project (for risk/timeline/scope alignment, demos, and pairing)

**Standup focus:** Blockers, PR reviews, and immediate needs only — larger discussions handled in project syncs. Tickets should be updated before standup.
