# MNTN Business Knowledge

## Security policy: no local Slack apps / no local-env API keys (2026-06-10)
MNTN security (Robin Fox) **no longer allows locally-hosted Slack apps or API keys held in local env files.** Personal/self-hosted bots with tokens on local machines (e.g. a Raspberry Pi) are disallowed and will be disabled and deleted. Secrets are being rotated into **SOPS** (encrypted secrets in the ArgoCD repo), via a Basecamp tool that PRs the re-encrypted secret (KMS Decrypt is disabled for individuals; Vault is optional but unsupported by DevOps). The TI Slack knowledge-extraction bot was decommissioned under this policy. (via Robin Fox + Harvey Yau, 2026-06-10)

## Compass — MNTN's internal AI infrastructure investigator (whitepaper, 2026-03; rollout 2026-06)
**What it is:** A multi-agent AI system embedded in the Internal Developer Platform (Backstage) that investigates infra problems like a senior engineer — Router decomposes a query, dispatches domain **Advisors** (Infrastructure/Billing/GCP/Knowledge/GitHub) in parallel, each with its own context window and scoped tools. Findings flow into a live **evidence knowledge graph** (8-stage ingestion: normalize→tag→extract facts→link entities→correlate→detect conflicts→project to slots→evaluate readiness) that **detects contradictions** between advisors and tracks **evidence provenance** (every claim cited to a tool call). Answers when info requirements (TaskFrame slots) are satisfied, streaming a provisional answer early. **Read-only** — never mutates infra; remediation via GitOps PRs. Built on MCP + Google A2A + Backstage + Grafana LGTM + ContextForge (IBM MCP gateway). Anti-hallucination: Chain-of-Verification, source attribution, impossible-ratio detection, causation prohibition, IaC-as-truth.

**Access:**
- **Atlas Code MCP** (300+ tools, full codebase/GCP/docs) → add to Claude Code: `claude mcp add mountain --transport http https://agent-gateway.management.in.mountain.com/mcp/code`. Cursor: `{"mcpServers":{"atlas-code":{"url":"https://agent-gateway.management.in.mountain.com/mcp/code"}}}`
- **Compass as a service** — 3 protocols, one core: (1) HTTP `POST /api/basecamp-chat/a2a`; (2) MCP `compass-query` tool via ContextForge; (3) Google A2A (`/.well-known/agent-card.json`, `POST /message:send` or `:stream`). Callable by CI/CD, bots, agents.
- **Extensibility:** add a tool = deploy MCP server + annotate Backstage catalog (auto-registered in ContextForge); add an advisor = register in A2A registry. No code change.
- Questions: **#dev-basecamp**, Harvey Yau's group.

**Relevance to TI knowledge work:** architecture is domain-agnostic (already searches Confluence via knowledge MCP → BigQuery vector DB). **Slack integration and knowledge-doc authoring are roadmap (§9 "What's Next"), NOT yet shipped** — so Compass does not yet replace a scheduled Slack-scrape→markdown pipeline. (via Harvey Yau, #engineering-team / whitepaper, 2026-06-10)

**Coverage gap — Quickframe (QF) platforms mostly NOT covered.** Compass was built around MNTN's core infrastructure. QF platforms were built outside it, so coverage is thin — you can get billing + some GCP info, but the data is far less rich than for central-platform services. Don't rely on Compass for deep QF investigations. (via Harvey Yau, 2026-06-10)

**Compass is also used for repo/codebase forensics** — e.g. confirming whether a repo is dead/archived. `SteelHouse/audience-loader` was confirmed a dead/archived repo (GitHub 404); its referenced secrets were not found in AWS Secrets Manager. Compass conversations are shareable via `basecamp.in.mountain.com/mcp-chat?join=<uuid>` links. (via Macie Kluting / Jason Whiting, 2026-06-10)

## Peak Performance / Mountain Matched relationship (per TI-896, 2026-04-22)

**Product level:** Mountain Matched is part of the Peak Performance product offering (per user 2026-04-22). They are not independent audience products.

**Database / expression level:** mostly separable, but with partial overlap. Empirical sample of 100 random PP segment expressions (`score_type=rtc + DS13 + DS19`) since Oct 15 2025:
- **24 of 100 also contain DS2** (the "MM" detector flag)
- 76 of 100 do not contain DS2
- 0 of 100 contain per-advertiser DS ids in the 1000-99999 range
- 0 of 100 contain the strings "first party" or "mountain matched" in the expression JSON

**Most common DS-id sets in PP expressions:**
- `13,14,19,21,34` (25%) — the bare PP signature plus auxiliary system flags
- `13,14,17,19,21,34,35` (8%) — adds 3P
- `13,14,19,21,34,35` (8%) — adds 3P
- `2,4,13,14,19,21,34` (7%) — adds MM + CRM
- The auxiliary DS14, DS21, DS34 are present in nearly all PP expressions — likely system-level (global flag, holdout, geo).

**Implication for analysis:** if you bucket "MM" by DS2 presence and "PP" by `score_type=rtc + DS13 + DS19`, the buckets are mostly disjoint (76% of PP expressions don't flag MM) but ~24% of PP advertisers will appear in BOTH buckets. The MM-spend cliff and PP rise are likely the same product-migration event observed on two sides of the categorization boundary, but they are NOT identical at the database level.

Source: `queries/ti_896_pp_mm_overlap_check.sql` and `outputs/ti_896_pp_mm_overlap_check.csv`.

## Peak Performance — corrected adoption numbers (TI-896 v2, 2026-04-22)

Peak Performance audience tier launched week of Oct 6 2025. As of Apr 2026:

- **~12% of currently-active advertisers** are running at least one delivering PP campaign (corrected from earlier 21% — the inflated number was an artifact of paused-campaign attribution, see Fix M10 in TI-896 verification).
- **~12% of cohort spend** flows through PP campaigns. Presence and spend-weighted views agree.
- **Among adopters:** ~32% use the default template (pure DS13+DS19 structural pattern), ~61% customize the template by layering additional DS clauses, ~3% run a mix, ~5% unclassified.
- **Track C ROAS cross-check:** adopters median +64% Q4 ROAS lift [bootstrap 95% CI +25% to +121%, n=101 valid] vs non-adopters +130% [+104% to +154%, n=381 valid]. **CIs overlap — directional only.** Adopters had ~1.5x higher *baseline* ROAS than non-adopters (~28-31 vs ~17-25 in Aug 2025), so cohorts are not exchangeable.

Structural "pure DS13+DS19" (template level) vs "DS13+DS19 + additional DS" is the best currently-known proxy for default-vs-custom PP usage. Formal product definition of "default" is a Ryan / Jordan (audience-tools team) question.


Last updated: 2026-04-22

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

### Mountain Matched AI (North Star — Q2 2026+)
- **Next-gen targeting suite** encompassing continuous scoring, graph-based identity, and ML-driven audience consumption
- Replaces static segments and static scores with dynamic, intelligent targeting
- Not on company OKR list for Q2, but is the **horizon all Q2 work aims toward** — B2B and incrementality are the foundation
- **Multi-quarter bet** — shipping piece by piece across Q2-Q4, not all at once
- "What's gonna make Mountain's targeting really unique" — competitive moat vs other CTV platforms (Mike Dolt, Q2 Roadmap)
- Components: Fangorn rollout, continuous scoring, campaign objective models, identity graph integration, LLM audience expressions

### Mountain Mesh for B2B
- Existing Mountain Mesh doesn't work for B2B — cannot infer targeting from website alone (unlike e-commerce where product is obvious)
- B2B needs additional inputs: company size, industry, target persona within the company
- Separate B2B-specific flow planned (Mountain Mesh would look completely different for B2B)

### Waypoint Targeting
- Planned feature for B2B: targeting users who dropped off at various points in the B2B sales cycle
- Important because B2B sales cycles are long with multiple drop-off points

### Interest Segment Quality Score
- Project to evaluate and rank the ~20K interest segments (DS19 keywords)
- Currently MNTN can't recommend segments because quality is unknown
- Alex Knorr working on this (continued from prior quarter)

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
- **Alex Bloore** — VP Engineering. Covering product leadership for targeting during GPM search (alongside Mike Dolt and Kale). Approved Mike's Q2 roadmap ("Mike, you killed it").
- **Mike Dolt** — Acting as himself + Ellison + RTPM for Q2 roadmap/product. Covering product leadership for targeting during GPM search (alongside Alex Bloore and Kale). Presented Q2 Targeting Roadmap (2026-04-17).

### Empirical Incrementality Lift (Alyson Lefkowitz to Malachi, 2026-06-01)

- **MNTN's true causal incremental lift is < 1-2% across the board.** Per Alyson: "true causal incremental lift was like... actually less than 1% across the board most of the time," with a typical range up to ~2% on average.
- **Retargeting gets ~20% lift on average** — order of magnitude higher than prospecting.
- **Implication:** because MNTN doesn't have rich identity infrastructure, the prospecting system ends up scoring people who would have converted anyway (effectively retargeting). The incremental contribution is small even when targeting is well-tuned.
- **Product direction (Alyson, 2026-06-01):** rather than forcing one optimization target, MNTN will likely offer **performance marketing vs incremental marketing as an explicit buyer choice.** Pure incremental ("don't target any IP that has had a site visit ever") would hurt performance but maximize incrementality; pure performance keeps the current scoring stack. The buyer picks.

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

## Q2 2026 B2B Expansion (Mike Dolt, Roadmap 2026-04-17)

MNTN is sharpening focus on B2B as a mid-market segment. B2B customers already exist on the platform but aren't well-served by current targeting.

### Q2 B2B Integrations (6 planned)
Tillium, Klaviyo, Ours Privacy, HubSpot V2, Freshman, Bombora. List subject to change — Jason Puertas confirmed integration priorities move fast.

### B2B Customer Identification
- Mechanism TBD — possibly a toggle during PTV onboarding
- All self-sign-ups now route to MNTN Express; PTV access requires salesperson
- Salesperson onboarding interview is one potential identification point
- Brian McAdams raised the question; Mike confirmed it's still being debated internally

### Why B2B Matters for MNTN
- If MNTN solves B2B targeting, it also solves MNTN's own marketing problem (MNTN is itself a B2B company)
- Paula touched on this: solving B2B for customers = solving it for ourselves

---

## Geo-Resolution — Bidder Migration (Q2 2026)

- Currently, targeting does geo-resolution for IPs. IPs without geodata are **not bid on** (risk of geo-violation)
- **20-25% of bids are missed** because targeting lacks geodata for those IPs
- Proposal: Have Bidder do geo-resolution instead — bid requests already carry geo data
- This would be "discovery-heavy work" to determine feasibility
- Victor asked about IPDSC impact — Mike said likely no direct impact
- Significant revenue opportunity if resolved (20-25% more bids)

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
- **LiftLab** — primary external partner for incrementality measurement. Keeps coming up with advertisers. Geo-based lift measurement — segments by geography and measures raw metric lift during campaign period. Provides a **self-serve web tool that recommends a test design** (power/holdout/geo) per customer (per Edgar, 2026-06-17); MNTN reviews + tightens those recommendations before offering studies. Reviewing vendor designs → see `knowledge/experimentation.md` § "Reviewing a third-party vendor's lift-test design" + TI-1039.
- **Kochava** — another incrementality/attribution vendor option.
- Possibly more vendors in the space.
- **Strategic shift (Kale, 2026-04-08):** Plan to shutter internal incrementality dashboards and move to approved third-party vendors. Messaging: "We changed the way we do incrementality." Need a dedicated LiftLab liaison/DS on the MNTN side.
- If advertisers trust LiftLab, MNTN has to trust LiftLab — can't argue with the vendor the customer chose.
- Lift tests are a key focus area — finding ways to improve MNTN's performance in partner-run lift studies.

### Competitive Positioning on Incrementality (Kale, 2026-04-08)
- Solving incrementality would be **HUGE** — dramatically change growth and retention (Kale's words).
- MNTN almost certainly looks bad on external incrementality studies because everything is currently optimized toward the visit (high-intent users who would've converted anyway).
- This is the core strategic risk: if external vendors show low lift, it undermines MNTN's value proposition. If MNTN can prove incrementality, it becomes a competitive moat vs Meta/Google.

### CTV vs Paid Search — Accelerator Framing (Alex Bloore VP TPM, 2026-04-20)
Alex pushed back on positioning CTV as a "richer-signal upgrade to paid search" (Malachi's original BUK Loom framing). His read — endorsed by Malachi — is materially different and more defensible. This is the framing to use in exec/buyer-facing materials:

- **Paid search's moat is timing, not signal.** Search wins because it surfaces options at the moment intent is absolute-peak — right when a user sits down to explore. CTV cannot pump budget and instantly see results. The timing advantage is structural; richer signal does not close it.
- **Google has richer behavioral signal than MNTN.** GA is on everyone's site — Google has durable, structured behavioral data MNTN cannot match. Claiming CTV has richer signal than search overstates the case.
- **CTV's real value is as an accelerator.** Take the non-incremental portion of a marketer's search budget and make it incremental by creating more high-intent users. CTV makes lower-funnel spend more efficient; it doesn't replace search.

Implication for positioning: do not frame CTV as "search replacement" or "signal upgrade." Frame it as the thing that creates the high-intent audience that search then closes. This is the contrarian-but-defensible angle. Tracking ticket: [TI-891](https://mntn.atlassian.net/browse/TI-891). Reinforces Kale's "incremental ROAS is the metric" read from 2026-04-08.

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

## The Data Model Chain — How Audiences, Campaigns, and Bidding Fit Together

The canonical hierarchy from raw data to bid decision. Source: Malachi's Slack explainer to Matt Brorby, 2026-04-22 — captured here because this mental model is useful onboarding context and recurs in every incrementality / targeting discussion.

```
data_sources (IP + category_id mapping)
    │   category_id within a data_source IS the atomic audience of that data_source
    ▼
audience expressions (boolean include/exclude of data_source × category_id tuples)
    │   resolves to an IP set, loaded into MembershipDB as an audience_segment
    ▼
campaigns (1:1 with one audience_segment via audience_segment_campaigns, expression_type=2)
    │
    ▼
advertisers
    │
    ▼
verticals (one per advertiser, fixed assignment)
```

### Atomic unit: (data_source_id, category_id, ip)
- **data_sources** are catalogued providers of IP data (internal or third-party)
- **category_ids** are each data_source's self-categorization of IPs (e.g. "sports fans," "in-market for auto")
- A category_id within a data_source is, functionally, an atomic audience — the smallest targetable group
- MNTN authors some data_sources in-house (DS19 keyword universe, DS38 UI keywords, DS42 PMP deals); most are third-party

### How audiences get built
- An **audience expression** is a boolean combination of (data_source_id, category_id) tuples — include these, exclude those
- When a campaign is created, the expression is translated into the set of IPs that satisfy it
- That IP set is loaded into **MembershipDB** as an audience segment — the thing the bidder actually reads
- One campaign → one expression → one segment in memdb (via `audience_segment_campaigns`, `expression_type = 2`)

### What augmentor_log captures
- Every bid request that came through gets evaluated against memdb segments
- augmentor_log records the evaluation: IP, time, which campaigns/audiences matched, intent score, HHST, geo, etc.
- **Pre-bid-decision** — it's the "who passes the targeting gate for whom" log, not the bid itself
- Bidding happens after augmentor_log: if an IP passes a campaign's targeting gate at auction time, we bid

### Log chain (Malachi → Matt, 2026-04-22)
| Table | What it is | Unique ID |
|-------|-----------|-----------|
| `logdata.guid_log` | All page views | none — use ip + time + advertiser_id |
| `logdata.conversion_log` | Conversions | none — use ip + time + advertiser_id |
| `logdata.augmentor_log` (bronze.raw) | Every targeting-gate evaluation | none — use ip + time + advertiser_id |
| `logdata.impression_log` | Impressions bid on (attempted) | yes (impression_id) |
| `logdata.cost_impression_log` | Impressions won — **use this most** | yes |

Only impression tables have first-class unique IDs. For visits/conversions/augmentor events the composite key is `(ip, timestamp, advertiser_id)`.

**Open nuance to reconcile:** my catalog trace paths put impression_log *after* win_logs for CTV (see data_knowledge.md "Impression trace paths"). Malachi's plain-English framing is "impression_log = bid attempts, cost_impression_log = wins." Possible reconciliation: impression_log rows fire in two contexts (bid submission AND post-win pixel), with different semantics per channel. Worth empirically checking whether impression_log rows exist without a corresponding win_logs row. Flagged 2026-04-22.

### Filtering order for analysis
Work down the chain, not up: `audiences → campaigns → advertisers → verticals`. Start by picking the audience(s) you care about, then campaigns using them, then the advertisers those campaigns belong to. This reflects how the system is built — IPs are never directly attached to anything; they're always attached via a (data_source, category_id) membership.

### Implications for ghost bidding (TI-837)
The "Would MNTN have bid on this holdout IP?" question decomposes into three boolean gates evaluated on a single augmentor_log row:
1. IP matches a campaign's audience expression at that moment
2. Intent score ≥ campaign threshold
3. HHST gate clear

All three yes → valid ghost-bid candidate for that campaign. All three fields are present in the augmentor_log row, so the filter is a pure row-level predicate — no external joins to memdb required at analysis time (the audience-expression match is already baked into which campaign's evaluation produced the row).

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
| **Mountain Matched AI** | North Star: next-gen targeting suite — continuous scoring, graph-based identity, ML-driven audience consumption. Multi-quarter bet (Q2-Q4 2026+). |
| **Mountain Mesh** | Component of Mountain Match that auto-generates targeting from advertiser's website. Doesn't work for B2B (needs company size, industry, persona inputs). |
| **Waypoint Targeting** | Planned B2B feature: target users who dropped off at various sales cycle stages |
| **Interest Segment Quality Score** | Scoring system to rank ~20K DS19 interest segments by quality (Alex Knorr) |
| **Firmographic Curation** | B2B demographic data (company size, industry, revenue) — not yet built, P3 for Q2 |
| **Audience Overlays** | System for modifying audience segment behavior without changing base expressions. Needed for Fangorn rollout. Jamie building (~2 sprints). |
| **Gary** | Internal service that currently holds some business logic being migrated to Audience Service |
| **Permel** | External partner — Universal Optimization Controller for future pacing integration |
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
| 2026-04-17 | Mike Dolt Q2 Roadmap presentation | Mountain Matched AI North Star, 3 themes (B2B/Incrementality/MMAI), 31 line items, B2B integrations (6), Mountain Mesh for B2B, Waypoint Targeting, geo-resolution migration (20-25% missed bids), MNTN Express no pixel, Alex Bloore/Mike Dolt roles, terminology (audience overlays, Gary, Permel, firmographics) |
| 2026-04-22 | Malachi → Matt Brorby Slack explainer | Consolidated data model chain (data_source → category_id → audience expression → campaign → advertiser → vertical), log chain with unique-ID rules (augmentor_log / impression_log / cost_impression_log / guid_log / conversion_log), ghost-bidding decomposition in terms of the three augmentor_log gates |

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

<!-- slack-extracted: 2026-04-16 -->
- **Scalyr Log Management Tool:** Scalyr was decommissioned in March 2026. Engineers no longer have access and the Okta tile has been removed. Any workflows that previously relied on Scalyr for log search/monitoring need alternative tooling. (via Edris Mohsin, #engineering-team, 2026-04-01)
- **Fangorn DS13 → DS46 Swap — Implementation Approach:** The decision was made to implement the DS13 → DS46 audience swap using an advertiser-configuration flag approach rather than an exclusion-script hack. Key design decisions:
- The Audience Service reads a per-advertiser `on_fangorn` flag during segment breakdown; when `true`, it swaps DS13 → DS46
- Toggling the flag triggers a segment rebuild
- The base expression is unchanged, so UI audience sizes will NOT change
- New advertisers default to `true`
- Long-term, Jamie will replace this with an audience overlay system (~2 sprints out)

**Rollout plan:** Set all advertisers to `false` → API-flip one advertiser to `true` → validate → scale via script. (via Bryce Wagg, #targeting-squad, 2026-04-01)
- **Fangorn Leadership Review Outcomes:** Leadership approved the Fangorn phased rollout with the following tier allocation: 44% Tier 1 / 40% Tier 2 / 16% Tier 3. Reported average IVR lift in peak performance: 35.9%. Richard requested a tech blog post (Bryce and Kale leading). Mark flagged a sales enablement opportunity to be discussed with GTM on a project kickoff call. (via Bryce Wagg, #dev_fangorn-model_ex, 2026-04-01)

<!-- slack-extracted: 2026-04-17 -->
- **Fangorn Rollout — Approved Configuration Details (April 2026)**

Leadership approved a phased Fangorn rollout with the following tier allocation: 44% Tier 1 / 40% Tier 2 / 16% Tier 3. Key outcomes from the approval call:
- 35.9% average IVR lift in peak performance confirmed.
- A logic error was identified in mid-intent + peak performance interaction.
- Richard requested a tech blog post (Bryce and Kale have started drafting).
- Mark flagged a sales enablement opportunity to be discussed with GTM on Friday's project kickoff call.

**Fangorn Enablement Mechanics:**
- Audience service will read a new advertiser configuration flag (`onFangorn`) and swap DS13 to DS46 during segment breakdown.
- A 3-point ticket is going to Jamie in the next AUD sprint (reusing existing card AUD-5301).
- Rollout is executed via a script that flips the `onFangorn` flag per advertiser ID (AID).
- Command Center toggle UI will be implemented.
- UI audience sizes should not change for peak performance since base expressions are unchanged — only the segment breakdown shifts.

**Continuous Scoring Pairing:**
Continuous scoring (100-point step increments replacing fixed-value buckets) will ship with Fangorn on the same advertiser-level inclusion/exclusion list. This avoids the peak-performance-to-mid-intent floodgate problem and stacks expected lift. May 15 PER deadline for continuous scoring fits within the Fangorn two-week rollout window.

**Pre-Launch Due Diligence:**
Toph (production ops) is checking pacing risk for the ~10% of campaigns currently spending in peak performance against the Tier 1 rollout list, to catch meaningful audience-size drops (especially high-budget campaigns) before the flag is flipped. Tier 3 advertisers are excluded from rollout. (via Bryce Wagg, #dev_fangorn-model_ex, 2026-04-16)
<!-- slack-extracted: 2026-04-19 -->
- **Haus x MNTN Partnership (announced Oct 2025)**

MNTN has an official business partnership with Haus (haus.io), an incrementality measurement vendor specializing in geo-based lift testing with augmented synthetic control. Key details:
- **Partnership type:** Business partnership, NOT a technical integration. Haus is featured in the MNTN Integrations Marketplace in the UI.
- **Customer-facing:** MNTN provides Haus-specific lift testing best practices in the Help Center.
- **Press release:** Published 2025-10-14.
- **Haus methodology:** Geo holdouts + augmented synthetic control (same methodology as Meta GeoLift). Susan Athey on advisory board.
- **How sales uses it:** Haus is one of several lift testing partners (alongside LiftLab, Measured, Northbeam) that customers can use. Sales has an intake process in #ask-incremental-lift-tests for lift test requests.
- **Office Hours (2026-02-20):** Solutions x Sales ran a "Lift Testing" session covering methodologies behind LiftLab, Haus, and Measured, ICPs for each, and how to speak to lift testing with prospects.
- **Other partnerships mentioned alongside Haus:** Northbeam, iSpot (potential), LiftLab, Measured.
(via Allie Dupere, #sales, 2025-10-10; Keaton, #sales, 2026-02-20; Lauren Reedy, #ask-incremental-lift-tests, 2026-03-23)

- **Claude Enterprise Licensing — Seat Expansion and Policy**

MNTN upgraded its Claude plan to Anthropic Enterprise (from a 150-seat cap). Key policy decisions made during the transition:
- Claude licenses are being restricted to Engineering department employees. Non-engineering staff (~30 seats) had their licenses reviewed and potentially revoked.
- The Enterprise plan also provides higher token quotas, which was a primary driver for the upgrade.
- Requests for Claude seats should go through IT at ithelp.mntn.com. (via Robin Fox, #engineering-team, 2026-04-16)
- **PTV Tech Course — Performance Pacing & ML Micro-Learning (Workramp)**

A new micro-learning module, *Performance Pacing & Performance Machine Learning*, has been added to the PTV Tech Course learning path in Workramp. It covers how campaigns are paced, how budgets are managed in real time, and how machine learning predicts and optimizes performance before campaign launch. The module is not mandatory but highly encouraged. Accessible via the PTV Tech Course learning path for engineers who have already completed it. (via hellsbells, #engineering-team, 2026-04-16)
- **AI Engineering Training — Claude Code Workshop (TaskFlow)**

MNTN Engineering published an internal hands-on workshop repository (github.com/SteelHouse/claude-code-workshop) called TaskFlow. It is a CLI task manager codebase with intentional bugs and architectural flaws mapped to guided exercises. The workshop teaches four disciplines of AI engineering:
1. Prompt Engineering (Exercises 1–4)
2. Context Engineering — including CLAUDE.md design (Exercises 5–6)
3. Intent Engineering — goals, guardrails, decision boundaries (Exercise 7)
4. Specification Engineering — machine-actionable specs as contracts (Exercise 8)
5. Combined exercises including multi-file debugging, race conditions, and architectural refactors (Exercises 9–15)

Optimized for Claude Code but compatible with any AI coding assistant. (via Adam Ferras, #engineering-team, 2026-04-16)

<!-- slack-extracted: 2026-04-21 -->
- **Incrementality Primer (Malachi, April 2026):** Malachi produced a 20-minute Loom video titled "What is Incremental ROAS?" aimed at all of engineering. The video covers the conceptual difference between attributed ROAS and incremental ROAS — the core analogy: attributed ROAS is pressing a crosswalk button and feeling responsible when the light changes; incremental ROAS is recognizing the light was on a timer. Alex Bloore endorsed it as "probably the single most meaningful concept" for anyone touching targeting and measurement. Slides: https://gist.githack.com/mdunn-mntn/18ebda7bd650bb054c7522797ea1db35/raw/incrementality_primer_deck_standalone.html#/29 (via malachi, #engineering-team, 2026-04-20)
- **Open-Source MMM Tools Referenced for Incrementality Work:** Malachi flagged three open-source Marketing Mix Modeling (MMM) products as relevant context for MNTN's incrementality initiative, noting they are adjacent but not identical to incrementality measurement: Google Meridian (https://developers.google.com/meridian), Meta Robyn (https://facebookexperimental.github.io/Robyn/), and Meta GeoLift (https://github.com/facebookincubator/GeoLift). Will Cavey (MNTN marketing) noted he uses pymc-marketing internally rather than Meridian because Meridian's advantages are too Google-centric, and Robyn is native R. (via malachi, #tgt-infrastructure-squad, 2026-04-20)
- **Claude Design — Org-Wide Rollout (April 2026):** Claude Design (Anthropic's AI design tool) was made available to the entire MNTN org as of 2026-04-20. Key operational notes: (1) Token usage is shared across the org — users should be mindful of quota consumption. (2) The preferred integration approach is linking a repo-maintained design system so it stays current; the `mntn-go` UI components library (https://github.com/SteelHouse/mntn-go/tree/main/packages/ui-components) was used as the seed. (3) A shared `core-components` repo (https://github.com/SteelHouse/core-components) was being set up to serve as the canonical design system source. (4) An initial `MNTN-Strata` design system was created in Claude Design from the `mntn-go` repo. (5) Claude Design is currently browser-only (https://claude.ai/design/) — not available in the desktop app. (6) Handoff to Claude Code produces an HTML/CSS bundle (downloadable or fetchable via CC CLI) — initial testing showed promising but imperfect results. Icon library: MNTN uses Remixicon (via Iconify), with custom icons documented in the Figma Core Icons library. (via Paulo, #engineering-team, 2026-04-20)
- **Data Platform (DPLAT) Responsibility Boundary — Data vs. DDL Changes:** DPLAT's role is limited to DDL changes (schema changes, table creation/drops, replication setup) and does not extend to modifying actual data values within application tables. Application teams (e.g., PRO, LAB) are considered the authoritative owners of their data and are responsible for running DML (INSERT/UPDATE/DELETE) on their own tables. DDL changes still require a DPLAT ticket until new migration tooling is published, primarily for permissions reasons. (via scotty, #data-platform, 2026-04-20)
- **Team rename (2026-06-17):** the squad is now officially **Audience Intelligence (AUDI)** — renamed from "Targeting Infrastructure" / "TI squad". The Jira **project key stays `TI`** (tickets remain TI-XXXX); only the team name changed. Use "Audience Intelligence (AUDI)" on artifact bylines.
- **TI Squad Jira Operating Standards (Bryce Wagg, April 2026):** The TI (now Audience Intelligence / AUDI) squad uses the following Jira conventions: Story points: 1=half day, 2=1 day, 3=1-2 days, 5=3-5 days, 8=1 week+ (should be broken down), 13+=must be an epic (max story size is 8). Ticket hygiene: no subtasks; use Tasks over Stories; spikes are unpointed and research-only (code goes in a separate ticket); default priority P3; every ticket must pass through every status. Epics should ideally represent a single sprint; rarely span two sprints. Required fields on creation: Release Type, User-Facing, Developer. Sprint naming format: `<Team Acronym> Sprint - MM/DD/YYYY - MM/DD/YYYY` (e.g., `TI Sprint - 04/07/2026 - 04/20/2026`). Scrum cadence: daily 15-min standup (blockers first, right-to-left board), weekly backlog refinement (exit = next 1-2 sprints ready), sprint planning at sprint start, review/demo + retro at sprint end. (via Bryce Wagg, #targeting-squad, 2026-04-20)
- **Audience and Intent Scoring Venn Diagram — Documented in Confluence:** The Venn Diagram illustrating the relationship between High Intent, Mid, Max Reach, and Peak Performance audience tiers is now documented in Confluence at https://mntn.atlassian.net/wiki/spaces/TAR/pages/3567452174/Audience+and+Intent+Scoring+Venn+Diagram. Previously this only existed in Slack. (via Ryan Kleck, #tgt-infrastructure-squad, 2026-04-20)
- **NeonPixel — No Dayparting Usage:** As of April 2026, there are no NeonPixel (NP) campaigns using dayparting. Confirmed by Tofer (production ops). (via Tofer, #production-ops, 2026-04-20)

<!-- slack-extracted: 2026-04-22 -->
- **LiftLab Integration — Two Initiatives**

MNTN has two active initiatives with LiftLab, described as 'Data In' and 'Data Out'. There is a shared external Slack channel with LiftLab; the primary business-side point of contact is named Kent. Internal documentation is maintained in a shared Google Doc (linked in the dev-incremental-lift channel file list). LiftLab feasibility approvals are required before advertisers can be onboarded for incremental lift tests. (via Al Beretta, #dev-incremental-lift, 2026-04-21)
- **Fangorn — Tier 1 Rollout Advertiser Selection Criteria**

The initial Fangorn Tier 1 rollout list was ranked using four criteria:
1. **HHST × Audience Ratio** — Do campaigns stay in high-intent and is audience size maintained or growing?
2. **Score Opportunity** — Does the existing audience's median Fangorn score show room for improvement (approximately < 0.8–0.85)?
3. **Audience Size Stability** — How much does Fangorn change audience size? Smaller changes are preferred.
4. **Scale / Budget** — Advertiser spend, log-scaled and normalized, as a proxy for impact of a successful rollout.

The initial list contains 369 advertisers. (via Matt Brorby, #dev_fangorn-model_ex, 2026-04-21)
- **Fangorn — Continuous Scoring Rollout Scope**

Continuous intent scores (replacing bucketed scores of 10k, 8k, etc.) will only be available for advertisers included in the Fangorn rollout, as determined by a CoreDB reference table being developed by the targeting team. Advertisers excluded from the rollout will continue to receive bucketed scores. The continuous scoring rollout is intentionally synchronized with the Fangorn model rollout — it is not a universal upgrade to all advertisers. (via Matt Brorby, #production-ops, 2026-04-21)
- **Fangorn — Intent Group Bucket Boundaries (Continuous Scoring)**

With the rollout of continuous Fangorn scoring, intent groups are mapped to score ranges (0–10000) as follows:
- **4 Max Reach:** 0–3332
- **3 Mid:** 3333–6665
- **2 Peak Performance:** 6666–8000
- **1 High Intent:** 8001–10000

Key boundary decisions:
- High Intent begins at **8001**, not 8000 (score 8000 is reserved for Peak Performance).
- Bucket boundaries are set at clean 100-point increments for reporting and code simplicity.
- Option 1 bucket schema was selected (boundary rows span ~100 scores each) over Option 2 (which produced single-score edge rows at 8000 and 10000).
- The distribution of actual scores does not show discontinuous spikes at boundary values (6666, 8001, 10000), validating the even-increment approach. (via Forrest Bajbek, #production-ops, 2026-04-21)
- **New Hire — Luis Chelala, Sr. Project Manager (PMO)**

Luis Chelala joined MNTN as a Senior Project Manager on the PMO team, starting 2026-05-04. He will be supporting the Attribution and Identity teams. (via Tasha, #identity_core, 2026-04-21)

<!-- slack-extracted: 2026-04-23 -->
- **Crypto/Bitcoin/Blockchain Advertiser Policy (effective ~April 2026):** MNTN now accepts Crypto, Bitcoin, and Blockchain advertisers with the following requirements:
- Must be a US-based company
- Pre-payment required before launch (advertiser must complete a designated intake form)
- Minimum budget is $50,000 (monthly, consistent spend preferred)
- Sites with gambling components are not eligible — submit domain through the account approvals process in Salesforce if unsure (via Rachel Siegel, #sales, 2026-04-22)
- **World Cup Inventory — Outreach Restrictions:** MNTN does not have rights to mention the World Cup in any public outreach or written materials. Sales sequences must not reference it directly. Acceptable alternatives include phrases like 'soccer games this summer' or 'an upcoming soccer tournament.' Discussion of World Cup inventory is permitted on calls. (via alexaguttroff, #sales, 2026-04-22)
- **MNTN Select — Salesforce Budget Date Validation Rule:** A Salesforce validation rule is now live for the MNTN Select product. When adding MNTN Select to an opportunity, Budget Start Date and Budget End Date are required fields. The rule blocks saving if: either date is blank; Budget Start Date or Budget End Date is earlier than the Opportunity Close Date; or Budget Start Date is later than Budget End Date. Budget Start Date equal to Opportunity Close Date is permitted. Budget Start Date equal to Budget End Date is also permitted. (via Michael Botler, #sales, 2026-04-22)
- **Peak Performance Audience Customization Rate (updated ~April 2026):** Among Peak Performance adopters, the audience customization breakdown is:
- 34% use the default-only template (pure DS13+DS19 structural pattern)
- 58% customize the template (layered with extra DS clauses)
- 3% use both default and custom configurations
- 5% are unclassified

This supersedes the previously documented figures of ~32% default / ~61% custom (TI-896), which may have been calculated on a slightly different population or date range. (via malachi, #targeting-squad, 2026-04-22)

### Vertical Taxonomy and Auto-Assignment (Kirsa + Mike Dolt, 2026-04-23)

MNTN's vertical taxonomy was authored by Kirsa Haenebalcke in **late-2023 / early-2024** based on the advertiser mix at the time. It has not been updated since. Kirsa considers it "not very good" — the buckets were chosen from the advertisers she had then, not from a first-principles taxonomy, and it has not been refreshed as the advertiser base has grown.

**Operational consequence:** Every advertiser must have a vertical assigned. Auto-assignment handles most of this, but a growing tail (~100+ at time of this meeting) is not getting auto-assigned and has to be hand-labeled by Mike Dolt and Nick. Root-cause mix is a combination of:
- Higher advertiser volume overall (absolute count up materially since start of year)
- Self-sign-up → Express as the default path, which brings more low-signal advertisers into the pipeline
- Scraper failures on advertisers whose sites are "two JPEGs and no text" or who arguably should not have been approved in the first place

**Why this matters for Fangorn:** Fangorn scores are produced at the **vertical level** and then joined to advertisers. A missing or wrong vertical is not a cosmetic label problem — the advertiser cannot be scored. Fangorn increases, not decreases, the dependency on accurate vertical assignment.

**Roadmap status:** There has been an idea for a vertical-replacement mechanism living in the household-scoring area, but it has slipped below the line on the roadmap. No committed owner or date as of 2026-04-23. Fully releasing Fangorn will NOT eliminate the need for verticals — Fangorn depends on them.

<!-- slack-extracted: 2026-04-24 -->
- ### Spotify Privacy Compliance Requirement — Pixel Data Isolation (2026-04)

Spotify has raised a blocker to deploying the MNTN pixel based on privacy law concerns. Specifically, Spotify requires that data collected via the MNTN pixel be used **solely for Spotify's purposes** and not fed into MNTN's identity graph for cross-device attribution at the household level or used to benefit other clients or MNTN's broader business.

Spotify's stated requirements to proceed:
1. MNTN must agree to act as a **Service Provider** under applicable privacy laws.
2. A contractual commitment that pixel data is not used beyond Spotify's purposes.
3. **Technical validation** that those contractual limitations are being enforced (i.e., pixel fires are isolated from the identity graph, Mountain Matched, etc.).

**Business stakes:** Spotify is a $200K/month account at launch, with potential to grow to $20M/year in spend. Launch date is 2026-05-15. This requirement is expected to become standard for most large brand advertisers.

**Strategic implication:** MNTN needs a mechanism to isolate specific advertiser pixel fires from the identity graph and other shared data uses. This is considered a likely dealbreaker class of requirement for enterprise/brand advertisers going forward. (via Elena, #identity_core, 2026-04-23)
- ### Geographic Targeting — Minimum Radius

MNTN supports geographic targeting down to a minimum radius of **0.5 miles**. This applies to source audience location targeting. (via Riley Skoric, #sales, 2026-04-23)

<!-- slack-extracted: 2026-04-28 -->
- ## Identity Graph Help Desk & Access Pattern

The Identity team has launched a dedicated help desk channel for all Identity Graph questions. Key changes:
- **Access pattern:** BigQuery is the official access point for batch use. Delta/Parquet references are deprecated.
- **Schema and contracted columns**, SLAs, and integration guidance are documented in the help desk channel canvas (RFD and data contracts pinned there).
- All Identity Graph questions, integration use cases, and escalations should be directed to the help desk channel rather than pinging engineers directly. (via Elena, #engineering-team, 2026-04-28)
- ## QFMP Creative Tracking — Post-April 2026 Gap

As of April 1, 2026, QFMP (Quill Full-Motion Production) videos are no longer uploaded through the QFMP platform. All QFMP video handling is done manually and off-platform. As a result:
- Creative tagging for QFMP is unreliable for any creatives after that date.
- The prior CAAS tagging system for QFMP tracking is broken.
- **QFAI creatives** can still be tracked via a Tableau dashboard and the `viva-server` Neon DB.
- For QFMP creative sourcing questions post-April 2026, manual investigation or PM escalation is required. (via Tejas Widjonarko, #data-platform, 2026-04-28)
- ## No-Share Advertiser Policy — Overview & Implementation

MNTN is implementing a "No-Share" policy allowing advertisers to opt out of contributing their pixel data to shared models and audiences. Key details:

**Scope of data affected:** `guid_log`, `conversion_log`, `verified_visits` (clickpass data), and any downstream uses including Fangorn, BUK, DS13, DS19, and feature store tables.

**Mechanism:** A new boolean column will be added to `public.advertisers`. A value of `TRUE` indicates the advertiser's data **cannot** be used. All pipelines touching the affected data sources must join against `public.advertisers` and filter on this column.

**Initial use case:** Spotify is the first advertiser requesting no-share status. Spotify has confirmed they will not use MNTN Matched, so Mountain Match model degradation concerns are mitigated for this case.

**Known limitations of the current approach:**
1. Retroactive removal is not possible — the exclusion only applies going forward.
2. Fangorn: if implemented without retraining, models will score against a feature distribution not seen during training, causing miscalibration for excluded advertiser households.
3. BUK: excluded advertisers receive no collaborative filtering recommendations; cold-start LLM path is the fallback.
4. DS13/DS19: households that exclusively visit an excluded advertiser's URLs will not receive vertical or keyword tags from those URLs.

**What "No-Share" means operationally:** No using the data to build shared models or target other advertisers' audiences. Internal analytics use is permitted.

**Long-term path:** Pseudo-anonymization or tokenization techniques should be evaluated as the scalable solution for cases where full data cutoff is too blunt. A formal policy definition (who qualifies, what branches to trim vs. full root cutoff) is required before this becomes a standard offering. (via Ryan Kleck, #tgt-infrastructure-squad, 2026-04-28)
- ## Media Plan Service — Gemini API Credit Constraint Failure Mode

New Media Plan generation failed due to a resource/credit constraint on Gemini APIs called by the PER-ML media plan service. This is a known failure mode: if the GCP service account associated with the media plan service lacks appropriate roles or exhausts Gemini API credits, Media Plan creation will silently fail for all users attempting to create new plans. Resolution requires verifying GCP service account roles and API quota. (via Tom Manuel, #mission-control, 2026-04-27)
- ## Campaign Budget Minimums — Override Mechanism and Risk

MNTN has a mechanism to disable budget minimum enforcement entirely for individual advertisers, setting the effective minimum to $0.01. This override has existed at least since February 2026 and can allow campaigns with daily budgets as low as $7 to go live. This directly contributes to chronic underspend on affected campaign groups, as the pacing system attempts to manage budget splits on non-viable budgets. The override is stored in the advertiser configuration (not the campaign group), and the standard Command Center minimum of $500 does not apply when the override is active. A review of which advertisers have this override enabled and whether it should be permitted is needed. (via Tofer, #production-ops, 2026-04-27)
- ## Q1/Q2 2026 Performance Investigation — Leading Hypotheses

An ongoing cross-functional investigation into performance decline and customer churn has identified the following leading hypotheses, in order of current investigation priority:

1. **High CPMs** — Customer sentiment data and aggregate metrics suggest CPMs have risen, degrading ROAS/CPA.
2. **Conversion pixel misconfiguration** — Many advertisers lack valid Order IDs or Order Amounts, which affects CPA (deduplication removes conversions) and ROAS measurement. Prevalence among new vs. existing customers is a key open question.
3. **Poor new customer performance** — New customers (primarily SMB) have worse spend and performance metrics, and their growing share in the customer mix is pulling down aggregate metrics.
4. **Customer mix shift toward SMB** — Revenue per advertiser has declined since Q1/Q2 2025. A concurrent increase in advertiser count has not offset the per-AID revenue decline. Hypothesis: rapid SMB growth with poor retention is replacing higher-spending customers with lower-spending ones.
5. **Customer Lifetime Value decline** — A notable increase in customers spending 10%+ less MoM has been observed; investigation pending overlay with conversion data to separate pixel issues from genuine LTV decline.

**Analytical approach:** Splitting data along customer size tier (SMB/Mid/Large), conversion pixel quality, customer age, and competitive spend until a directional smoking gun is found. (via ray, #q1-2026-performance-churn-investigation-how-am-i-alive-what-is-life-i-wanna-die, 2026-04-28)
- ## Daniel Hartnett — New Senior Engineer, Audience Team

Daniel Hartnett joined the Audience (Targeting) team as a Senior Engineer in late April 2026. (via Mike Dolt, #targeting-squad, 2026-04-28)

## Mountain-Match AI Roadmap — Intent-Probability vs. Incremental-Lift Scoring

**Strategic framing (Alex Bloore, 2026-04-28 team meeting):**
- Current default targeting prioritizes **high-intent** IPs.
- "High intent, almost by definition, is not going to be incremental" — high-intent shoppers were going to convert anyway; targeting them captures attribution but doesn't drive new visits.
- **The "movable middle" hypothesis:** mid-intent IPs may produce more incremental lift, because they're not yet committed but can be primed.
- **CTV's role per Alex Bloore:** CTV is a funnel-priming channel — set up lower-funnel conversions assisted by Meta / Google / search, NOT a last-touch closer.

**Implication for Mountain-Match scoring:**
The Q2-Q3 roadmap should consider evolving from "intent-probability" scoring → **"incremental-lift-probability"** scoring (uplift modeling). Different IP ordering for advertisers who explicitly opt into incrementality optimization. Matt has a PRD draft for this.

**Why the offline ATT methodology matters:** Without an internal incrementality measurement we trust, MNTN can't validate that a new uplift model actually drives more incremental lift than the existing intent model. The TI-837 ghost-bidding ATT pipeline becomes the validation harness for any future targeting strategy.

**Third-party measurement stance (Alex Bloore):** MNTN does not use third-party tools (LiftLab, Houzz) to drive internal scoring decisions — those are for cross-validation only. Internal measurement is the source of truth.

## TI Squad Incrementality Program — Cross-Team Dependencies (2026-04-28)

The incrementality program touches multiple teams beyond TI:
- **Jason** — integration-side work (data pipeline / publisher integration)
- **Al** — reporting-side (incrementality dashboards customer-facing)
- **Megan** — UI experiment-setup (advertiser-facing experiment configuration)
- **Bidder team (Zach + Jordan)** — bidder-level ghost bidding (escapes augmentor 10-day TTL; production solution; pending Alex Bloore decision)
- **Edgar** — third-party attribution liaison (Houzz, LiftLab)
- **Matt** — uplift modeling PRD

**Coordination owners:** Bryce + Kyla + Howard. Weekly TI-incrementality check-in being established.

<!-- slack-extracted: 2026-04-29 -->
- ## AI/ML Model Inventory by Squad (Compliance Audit, 2026-04-28)

### Targeting Squad (TGT) — Production
**Pre-trained OSS Models:**
- `BAAI/bge-large-en-v1.5` via sentence-transformers 2.6.0 — Semantic embeddings for keyword clustering (Bottom Up Keywords)
- `Alibaba-NLP/gte-large-en-v1.5` — Vector search for keyword recommendations and signals categorization

**OSS Algorithms trained on proprietary MNTN data:**
- Apache Spark ALS (pyspark 3.4.1) — Keyword recommendations (Bottom Up Keywords)
- XGBoost (xgboost 2.0.3) — IP-advertiser conversion scoring (Fangorn)
- scikit-learn K-means (scikit-learn 1.3.0) — Keyword clustering (Bottom Up Keywords)
- scikit-learn LogisticRegression — URL ecommerce binary classifier (DDP URL → vertical classification, hourly job)

**Supporting libraries:** PyTorch 2.1.0 (CPU), HuggingFace Transformers 4.41.0, LangChain 0.1.20

**Open Source Data:** Mountain Matched Keywords training uses Common Crawl (commoncrawl.org); versions updated monthly.

### PERML Squad — Production
**Proprietary LLM APIs (not OSS):**
- Gemini 2.0 Flash via Google Vertex AI — media plan generation, network scoring
- GPT-4o-mini via OpenAI API — evaluation, fallback
- Gemini Embedding 001 via Google Vertex AI — vector embeddings for network search
- `ft:gpt-4.1-mini-2025-04-14:mntn::BMhxthUA` — vertical categorization (fine-tuned, not OSS)

**OSS Algorithms trained on proprietary MNTN data:**
- AutoGluon Tabular + TimeSeries (Apache 2.0) — delivery forecasting (impressions, spend, reach, cost)
- LightGBM (MIT) — network performance prediction; also used inside AutoGluon ensemble
- XGBoost (Apache 2.0) — inside AutoGluon ensemble
- CatBoost (Apache 2.0) — inside AutoGluon ensemble
- scikit-learn RandomForestRegressor (BSD-3) — network performance modeling
- FAISS (MIT) — vector similarity search index

**Supporting libraries:** LangChain, Pydantic AI, Instructor

**Non-Prod (emerging):** CausalImpact (Apache 2.0) + PyMC (Apache 2.0) + statsmodels (BSD-3) — Bayesian causal analysis

**Open Source Data:** None — all models trained on proprietary MNTN data (advertiser-network daily performance, CTV flight-level delivery metrics via Beeswax augmentor logs, historical campaign data from internal PostgreSQL/BigQuery).

### CDS Squad — Non-Prod Only
PoC Content Moderation Pipeline (not in production):
- Gemma 4 (open weights) via Ollama — local inference only
- CLIP + Whisper (MIT), YOLOv8/Ultralytics (AGPL-3.0), PaddleOCR (Apache 2.0), NudeNet (Apache 2.0), Detoxify (Apache 2.0), Falconsai NSFW classifier, TimesFormer (Meta research license)
- All inference-only; no fine-tuning or training; fully on-prem.

### AI Squad, MSS, Reporting, UI, ATTR, QFAI — Nothing to declare
No OSS models in use across these squads as of 2026-04-28.

### Identity Core Squad — Nothing to declare
Uses open source software libraries but no OSS AI models (pretrained LLMs or open-weight models).

**Note:** All OSS algorithm licenses confirmed permissive (Apache 2.0, MIT, BSD-3) — no GPL licensing concerns flagged. (via Kale McNaney, addy, bermudez, Brian McAdams, Victor Savitskiy, Adam Ferras, Alexander Jerneck, #engineering-team, 2026-04-28)
- ## URL Ecommerce Classifier — DDP Vertical Classification

An ecommerce binary classifier exists in production that converts DDP URLs into verticals. It runs as an hourly job.

- **Model:** scikit-learn LogisticRegression (scikit-learn 1.7.2)
- **Trained by:** Tucker (with Alyson's supervision); training performed in AWS; model copied to GCS/Databricks
- **Model registry:** Databricks Models — `prod/ml/ecommerce_classifier`
- **API code:** `github.com/SteelHouse/ip-vertical-classification` — loads model from Databricks
- **Calling model:** `github.com/SteelHouse/dbt/blob/main/ml_squad/models/vertical_categorization/ddp_vertical_classification_api.py` calls the above API
- **Training repo:** `github.com/SteelHouse/url-ecommerce-predictor`
- **Gotcha:** Original training notebook was in AWS; backup status uncertain. Model artifacts are in Databricks. (via Ryan Kleck, Alex Knorr, Victor Savitskiy, #tgt-infrastructure-squad, 2026-04-28)

## Intent-tier terminology (MNTN-specific)

The household_score → intent tier mapping in production scoring uses these labels. **Always use the MNTN labels in customer-facing or exec-facing materials**, even if "peak intent" reads more naturally outside the org.

| household_score | MNTN label | Notes |
|---|---|---|
| 10000 | "high intent" | Saturation flag — top-quintile + keyword match. The bidder treats this tier differently from peak. |
| 7000–9999 | **"peak performance"** (NOT "peak intent") | Per Alex Knorr 1:1 transcript 2026-04-28 + team meeting same day. "Peak" alone is fine; if qualified, use "peak performance." |
| 3333–6999 | "mid intent" | The "movable middle" hypothesis — historically under-tested but where MNTN may have most incremental headroom. |
| <3333 | "max reach" | Lowest intent tier, broadest pool. |

Boundaries from `bronze.external.household_scoring__prospecting_intent__v1`. These match the bidder's eligibility logic, not statistical conveniences (e.g., score deciles).

<!-- slack-extracted: 2026-04-30 -->
- ## CoreDW Deprecation — Hard Cutover Date: 5/19/2026

Legacy CoreDW is being decommissioned on **May 19, 2026**. All teams with tables or services reading from CoreDW must have cutover work planned to their BigQuery equivalents prior to this date. Teams needing BigQuery access should raise a DPLAT access ticket immediately. The Data Platform team is assisting with onboarding and the #data-platform channel contains onboarding resources. (via Mike Dolzer, #engineering-team, 2026-04-29)
- ## Select Reached Audiences — New Product Feature

**Select Reached Audiences** is a retargeting capability that allows advertisers to re-engage households that previously saw a MNTN Select ad across 150+ premium networks, directly within Performance TV campaigns. It is designed to be combined with Select Content Targeting and intent-based targeting to create a layered awareness-to-conversion funnel on CTV. This feature was being previewed to revenue-facing teams in late April/early May 2026. (via Gabs Fuchs, #sales, 2026-04-29)
- ## Sales Tool Transition: Outreach → Nooks

MNTN is transitioning all Brand and Agency Account Executives from Outreach to **Nooks** as the primary sales engagement/dialing platform. The cutover date for all Brand and Agency AEs was Monday, May 4, 2026. Nooks accounts are provisioned via Okta SSO. (via Abe Lofy, #sales, 2026-04-29)

<!-- slack-extracted: 2026-05-01 -->
- ## Zip Code / Location Reporting Beta

MNTN's Reporting team launched a Zip Code Reporting and Location Report Beta. The redesigned report makes top state performers more discoverable alongside zip-code-level data. Early results from Hugo Insurance (a beta participant) show the feature drove significant advertiser behavior changes: budget redistribution by state, cross-channel reallocation, and underwriter pricing adjustments based on MNTN CPA data. Hugo Insurance increased overall budget by 77% (some state budgets more than doubled) as a direct result of insights surfaced by this report. The initiative was a cross-functional effort spanning BER, RX, Design, and the Mid Market Initiative. (via Al Beretta, #engineering-team, 2026-04-30)
- ## MNTN-Strata Design System & Claude Design Integration

MNTN has an internal design system called **MNTN-Strata**, built on shadcn components. The MNTN Express (LAB) team created the components and exposed a shadcn registry. Andrew Corp (Integrations team) piloted using the registry and collaborated with Preston to create a Claude Design design system from it.

- **Hosted Storybook (internal, VPN required):** `https://mntn-shadcn-registry.qa.in.mountain.com/`
- **Registry JSON:** `https://mntn-shadcn-registry.qa.in.mountain.com/registry.json`
- **Source repos:** Figma (Strata) + `mntn-go` GitHub repo
- **Access:** Requires Tailscale VPN (internal `.in.mountain.com` domain)
- **Status (as of 2026-04-30):** Not 100% pixel-perfect replication of Figma/components, but useful for ideating design solutions. Not yet broadly publicized. (via Preston, #engineering-team, 2026-04-30)
- ## api.mountain.com Ownership Transfer — May 1, 2026

Starting May 1, 2026, `api.mountain.com` is owned by MAPI and no longer returns Reporting/r2ds/request-data-service data. Internal users are not expected to be impacted. The internal r2ds URL (`r2ds.prod.in.mountain.com`) remains available through end of Q2 2026 (final decom date TBD). Teams still using r2ds should migrate to CHAPI. Contact Lauren Gregg for migration assistance. (via Lauren Gregg, #engineering-team, 2026-04-30)
- ## Sales Tooling: Outreach → Nooks Migration

MNTN's AE sales team migrated from Outreach to Nooks for sequencing and outbound sales activity. As of May 1, 2026, Outreach access was removed. Prospect sequences carried over to Nooks at the same stage they were in Outreach. Monday AM training was provided for the new setup. AEs log in via Okta dashboard. (via Abe Lofy, #sales, 2026-04-30)
- ## Fangorn Model — Alpha Launch (2026-04-30)

Fangorn launched to a 3-advertiser Alpha cohort on April 30, 2026 at 5:00 PM PT (midnight UTC). Matt Brorby served as Mission Control Director for the launch. The scoring pipeline writes scores for the bidder to consume. Advertiser onboarding is managed via a PUT command to Audience Service (documented in the Targeting runbook: https://mntn.atlassian.net/wiki/spaces/TAR/pages/3584196632). The launch was preceded by a preflight checklist at 4:30 PM PT. (via Bryce Wagg, #tgt-infrastructure-squad, 2026-04-30)
- ## SMB Performance Decline — Q1/Q2 2026

As of late April 2026, the percentage of SMB campaign groups (CGs) hitting performance goals has been declining since mid-March, while Mid-Market and Large Corporate segments remain steady or improving over the same period. SMB is defined as advertisers spending less than $25K/month. (via Johnny, #q1-2026-performance-churn-investigation-how-am-i-alive-what-is-life-i-wanna-die, 2026-04-30)

<!-- slack-extracted: 2026-05-02 -->
- **Tasha promoted to VP of Program Management.** Tasha has been promoted to VP of Program Management after 7-8 years at MNTN. She is also leading the merger of the Business PMO and Engineering PMO into a single unified organization. She has historically been the connective tissue between all product lines and the Engineering org, supporting PTV, Express, QF, Select, and other initiatives. (via richard, #engineering-team, 2026-05-01)
- **Clerk → Auth0 migration completed.** MNTN completed a full migration from Clerk to Auth0 for authentication, including all user data. The migration was led by the PRO/LAB team. Going forward, monitor Auth0 status at https://status.auth0.com/ — Clerk status (https://status.clerk.com/) is no longer relevant. The migration was more complex than originally anticipated. (via Benny, #engineering-team, 2026-05-01)
- **`api.mountain.com` r2ds endpoints deprecated — use internal domain.** The public endpoint `api.mountain.com` no longer returns data for Reporting/r2ds/request-data-service endpoints. All services depending on r2ds-owned endpoints must point to the internal domain `r2ds.prod.in.mountain.com` instead. (via Lauren Gregg, #engineering-team, 2026-05-01)
- **Sales tooling: Outreach → Nooks migration.** MNTN sales is migrating from Outreach to Nooks for sequencing and prospect management. Access to Outreach was removed as part of the cutover; all existing sequence prospects were migrated to Nooks at the same stage they were in Outreach. (via Andrew Celaya, #sales, 2026-05-01)
- **CoreDW → BigQuery migration timeline.** CoreDW is being decommissioned. All services using CoreDW must be converted to use BigQuery by 2026-05-05 (migration date). CoreDW will be fully killed on 2026-05-19. (via Mike Dolt, #tgt-infrastructure-squad, 2026-05-01)
- **Fangorn ML scoring model launched to production.** The Fangorn predictive scoring model successfully launched to its first three live advertiser accounts: Big Blue Bubble Inc. (AID 38659), Biz2Credit (AID 32320), and University of Northwestern Ohio (AID 32233). These advertisers were moved from DS13 to DS46 and are now receiving Fangorn-generated intent scores sent to the bidder. Launch confirmed week of 2026-04-30. (via Ryan Kleck, #fangorn_launch_day, 2026-04-30)

<!-- slack-extracted: 2026-05-05 -->
- **Hannah** was promoted to **Senior Product Director**. Her scope includes platform performance, managing partnerships, and leading the media business — specifically MNTN Select, described as the "first ever" media marketplace for media buyers to shop for their own programs and deals. She has been instrumental in revenue growth by defining market opportunities and coordinating across the org and with network partners. (via Marwan Soghaier, #engineering-team, 2025-05-04)
- **MNTN Select** is MNTN's media marketplace product — described as the "first ever" media marketplace allowing media buyers to shop for their own programs and deals. It represents MNTN's entry into the broader media business beyond performance advertising. (via Marwan Soghaier, #engineering-team, 2025-05-04)
- **Mike Dolzer** was promoted to **Senior Project Manager**. His contributions include driving major migrations and projects to completion, upleveling operational teams (BAE and DPLAT) to handle increased request volume, and adapting focus as priorities evolve across the teams under his ownership. (via Kaila, #engineering-team, 2025-05-04)
- **Luis Chelala** has joined the Identity Core team as a new PM (Product Manager). This was announced by Elena in #identity_core. (via Elena, #identity_core, 2025-05-04)

<!-- slack-extracted: 2026-05-06 -->
- **Atlas Code MCP (Internal AI Tooling)**

MNTN has developed a next-generation MCP system called **Atlas Code MCP**, distinct from the existing MNTN Prod MCP currently used by Compass. Key differences:

- **MNTN Prod MCP**: Used by Compass for infrastructure context. Focused on infrastructure, developed during the BQ migration.
- **Atlas Code MCP**: ~300+ additional tools (and growing). Covers the entire MNTN codebase, GCP, best practices documentation, and team documentation. Designed to provide enough context to write complete fixes or features in a single planning session (Claude Code or Cursor).
- The Atlas system is architecturally reusable and extensible — new agent personalities can be created via a single pull request without touching core code.
- As of May 2026, Atlas Code MCP is in active development (not GA). An announcement will be made when it reaches GA.
- The core architecture was refactored from the April release; apps, MCPs, and integrations can be hung off the core. (via Harvey Yau, #engineering-team, 2026-05-05)
- **Tejas Widjonarko promoted to Senior Technical Program Manager (QFAI team)**

Tejas Widjonarko was promoted to Senior Technical Program Manager on the QFAI team, effective May 2026. He owns PRD creation and program execution for QFAI. Notable recent deliveries include a new login flow and a tiered subscription model. (via Emily (Jaffe) Sgroi, #engineering-team, 2026-05-05)

<!-- slack-extracted: 2026-05-07 -->
- ## Incident Response Bots — HAL vs C-3PO

- **`hal`** is the correct bot to use when creating a triage/incident Slack channel for production issues.
- **`c3po`** is a different bot and does NOT create private triage channels. Using c3po for incidents will post to unintended channels (e.g., #pex) rather than spinning up a dedicated incident channel.
- When declaring an incident, use `hal` to spin up the triage channel, then add devops and relevant stakeholders. (via bermudez, #mission-control, 2026-05-06)
- ## BOS (Budget Optimization Service) — Freeze Behavior During BQ Incidents

- **BOS** recalculates and re-evaluates campaign budgets on an hourly basis using BigQuery data.
- When BQ is unavailable, BOS must be **frozen** (stopped), meaning daily budgets are not updated in Beeswax (BW).
- **Potential impacts during a BOS freeze:**
  - Overspending campaigns will not be stopped sooner than scheduled.
  - Underspending campaigns will not have budgets increased to offset prior-day underspend.
  - Flights ending during the freeze window may under- or overspend.
- BOS freeze does **not** stop bidding/spending — the platform continues to bid and serve ads; only the budget recalculation loop is halted.
- This issue affects BQ only, not CoreDW. (via trixy, #mission-control, 2026-05-06)
- ## Integration Partner Test AIDs — Process & Sandbox Concept

- Scripts exist (owned by members of the engineering/QA team) to spin up test Advertiser IDs (AIDs) on production quickly — the process takes approximately one minute.
- Test AIDs should be clearly labeled (e.g., "Tealium TEST" or "Tealium Partner Acct") so internal teams (PEX, engineering, etc.) can easily identify them.
- **Q2 2026 integrations requiring test AIDs:** Tealium, Liftlab, Upwave, Quorum.
- Desired sandbox functionality: ability to disable specific feature sets on test accounts (e.g., prevent billing additions or campaigns going live).
- This process should be documented in the integrations playbook going forward.
- Long-term vision: evolve test AIDs into formal sandbox accounts. (via Jason Huertas, #engineering-team, 2026-05-06)
- ## Targeting Squad — Owned Services, DAGs, and Jobs

The following repositories and pipelines are owned or actively worked on by the Targeting Squad:

**Services (GitHub repos under SteelHouse org):**
- `ad-buying-ui`
- `audience-consumer`
- `audience-service`
- `clickpass-consumer`
- `crm-integration-consumer`
- `ip-vertical-classification`
- `membership-db` (contains server, gateway, and ETL — all in one repo)
- `membership-updates-aggregator`
- `oracle-audience-service`
- `search-service`
- `segmentation-journal`
- `shopper_graph`

**DAGs (Astronomer/Airflow):**
- `audience_intent`
- `category_taxonomy`
- `tpa_ipdsc_export`

**membership-db architecture:**
- **Server** = the membership DB itself
- **Gateway** = read layer (reads data from membership DB)
- **ETL** = write layer (writes data to membership DB)
- All three components live in the single `membership-db` repo.

**Criticality guidance:** UI-facing services are P0/critical. Streaming services have a longer grace period before downtime causes client-facing issues. (via Macie Kluting, #targeting-squad, 2026-05-06)
- ## Fangorn Rollout — ~50 Advertiser Milestone

As of the Fangorn launch, approximately 50 advertisers were successfully added to the Fangorn rollout. The scoring job ran successfully overnight, updated their targeting expressions, and output the expected counts. This represents a meaningful rollout milestone for the Fangorn system. (via Ryan Kleck, #fangorn_launch_day, 2026-05-06)

<!-- slack-extracted: 2026-05-09 -->
- **Upwave Brand Lift Integration (Beta Launch ~May 2026)**

MNTN is entering beta with an Upwave integration that enables PTV and Select customers to run brand lift studies via surveys. This is the first brand lift measurement integration supported for PTV and Select campaigns.

- **Vendor:** Upwave (https://www.upwave.com/capabilities-brand-lift)
- **Methodology:** Survey-based brand perception measurement
- **Eligibility:** PTV and Select customers
- **Context:** MNTN has been providing Upwave as a managed service to customers for approximately one year prior to this self-serve integration
- **Strategic value:** Intended to prove top-of-funnel performance for customers and complement incrementality measurement in customer readouts
- **Owner:** Jason Huertas (Product)
- **Channel context:** Leadership noted that brand lift and incremental lift results should eventually be combined in customer readouts (via Jason Huertas, #incremental-lift-stakeholders, 2026-05-08)
- **Org Changes — Engineering Promotions (May 2026)**

- **Michael Pascuzzi** promoted to **Staff Engineer**. Key technical leader driving architecture decisions and cross-platform systems thinking.
- **Jared** (last name unclear from context) promoted to **Senior Software Engineer**. Known for quality focus, design partnership, and attention to detail.
- **Hamza** (last name unclear from context) promoted to **Senior Software Engineer**. Led significant platform work including a recently launched AI chat feature. (via Emily (Jaffe) Sgroi, #engineering-team, 2026-05-09)

<!-- slack-extracted: 2026-05-15 -->
- **PAPI (PTV API) — Production Launch**

The PTV API (PAPI) launched to production as MNTN's new public API for Performance TV campaign management. PAPI is the strategic replacement path for GaryQL (the internal GraphQL service). Base URL: `https://api.mountain.com/ptv` (requires API key). Public docs: `https://api.mountain.com/ptv/docs`.

**Capabilities at launch:**
- Enterprise-grade REST APIs for programmatic campaign operations
- Standardized AuthN/AuthZ, request validation, and rate limiting
- OpenAPI specs with auto-generated docs
- Endpoints covering: campaigns, flights, advertisers, audiences, budget, geo-lists, and reporting (via CHAPI)

**Strategic significance:** LiftLab is the first external technology partner integrating via PAPI for incrementality experiments. PAPI is also a critical path for next-gen UI development. (via Benny, #engineering-team, 2026-05-14)
- **BigQuery Migration — Final Cutover Date**

The CoreDW-to-BigQuery migration final cutover date has been set to **Thursday, May 21, 2026**. After this date, all data will live natively in BigQuery. CoreDW servers are scheduled to be shut down on **Tuesday, May 26, 2026**. The cutover date was shifted from an earlier target to accommodate the large scale of work remaining for downstream teams. (via Mike Dolzer, #engineering-team, 2026-05-14)

<!-- slack-extracted: 2026-05-19 -->
- **Brand to Select Bundle Matcher v2 (in testing):** A new version of the Bundle Matcher GPT tool is being tested for pre-sale use. Key improvement over v1: v2 pulls from MNTN's API directly, so bundle recommendations made pre-sale match exactly what the brand will see when they log into the Select platform. This eliminates the inconsistency between pre-sale recommendations and in-platform options. v2 URL: https://chatgpt.com/g/g-69fe4c6bef488191a07d41b7ab622a91-bundle-to-brand-matcher-api. The original Bundle Matcher GPT remains functional — sales team is advised to try both while v2 is in testing. (via Mick Mathis, #sales, 2026-05-18)

<!-- slack-extracted: 2026-05-20 -->
- ## Product Releases — May/June 2026 (Sales Update)

### HubSpot Integration V2
- **Status:** Closed Beta, GA early June
- **Function:** Sends MNTN impression data into HubSpot for attribution
- **Relevance:** Opens B2B opportunities for accounts using HubSpot for lead attribution tracking
- **Action:** Flag accounts for the beta to the Solutions team

### Tealium Integration
- **Status:** Closed Beta, GA early June
- **Function:** Sends audience data from Tealium to MNTN for targeting/exclusions
- **Relevance:** Unlocks CDP-based conversations for prospects using Tealium

### Upwave Integration
- **Status:** Beta, end of May
- **Function:** Automated onboarding in the MNTN platform for Upwave brand studies
- **Action:** Flag interested customers in the Upwave channel to be added to beta

### Creative Scheduling
- **Status:** Closed Beta, June 1 (active customers only)
- **Function:** Lets advertisers schedule a creative to run during a specific time window
- **Use case:** Marketers with promotional calendars

### Creative Sequencing
- **Status:** Rolling out to GA over the week of this update

### Coming Soon
- **Unified Reporting Dashboard** with period-over-period comparison — closed beta May 28
- **Retargeting Deliverability enhancements** — closed beta June 2 (via Mick Mathis, #sales, 2026-05-19)
- ## Targeting Squad — Audit & Monitor Reference for DS/Model Changes

A Confluence reference page has been established at `mntn.atlassian.net/wiki/spaces/TAR/pages/3609919524` titled **"Audits & Monitors for Targeting Model / DS Changes."** It is a Targeting-squad-focused snapshot derived from the Mission Control audit registry spreadsheet.

**Contents:**
- Targeting squad P0 audits (audit IDs: 17, 40, 109, 110, 117, 118, and others) with ownership
- Adjacent Bidder and Campaign Management audits that targeting model swaps can inadvertently trigger (audit IDs: 34, 72, 41, 42, 56)
- A pre-flight checklist to run before any targeting model or DS change ships

**Source of truth:** The Mission Control audit registry Google Sheet. The Confluence page is a curated snapshot for the Targeting squad.

**When to use:** Review before any DS swap or model change ships, especially releases involving Fangorn or Continuous Scoring. (via Bryce Wagg, #targeting-squad, 2026-05-19)
- ## Jira Ticket Workflow — Fix Versions and Flags (Targeting Squad)

### Fix Version Automation
- Fix versions are only auto-generated when a ticket moves to **Ready for Deployment (RFD)** status. Tickets that skip RFD and go directly to Done will not have a fix version attached, and the release Slack notification will not fire.
- A PR-approval automation moves tickets to RFD automatically. Tickets without associated code (observability, infra, docs) do not trigger this automation and must be moved to RFD manually.

### Jira Flags
- Jira's flag feature places a yellow banner on a ticket without changing its status. Intended for situations that need attention but are not fully blocked — e.g., stalled on a question, awaiting a decision, or carrying a known risk.
- **Convention:** Use flags as a status overlay, not a replacement for the Blocked status. Use Blocked for hard stops only.

**Reference:** `mntn.atlassian.net/wiki/spaces/TAR/pages/2050097175/Jira+Ticket+Workflow` (via Bryce Wagg, #targeting-squad, 2026-05-19)

<!-- slack-extracted: 2026-05-27 -->
- ## BigQuery Cutover — Reporting UI (Completed ~2026-05-27)

**What happened:** The Reporting UI and all dependent services were fully cut over to BigQuery as the source of truth. Customers now see data sourced from BigQuery, not CoreDW.

**Cutover mechanics:**
- New plan submissions were paused starting 8:30 AM PST on cutover day. The BER squad performed rebasing and restatements (~4 hours).
- During the pause window, SQLMesh showed an error requiring 6 approvers for new prod plans. Team migrations were allowed; new prod plan work was not.
- Dev plans were unimpacted throughout.
- CoreDW completed its final run successfully the day before, so both systems reflected updated data on cutover day.

**Post-cutover state:**
- CoreDW continues to run but is no longer the source for UI Reporting.
- The majority of CoreDW monitors come down the day after Mission Control following cutover.
- Legacy CoreDW servers are being decommissioned on **June 2, 2026**.
- A backup of CoreDW is being taken at shutdown in case data recovery is needed, but recovery will require significant effort.
- The `summarydata.visits` export from CoreDW was failing and has been removed from the pipeline; no backfill required as of cutover, but a manual export can be requested if needed.
- Smoke tests for CoreDW deprecation will be shared in the data platform channel. (via Mike Dolzer, #engineering-team, 2026-05-27)
- ## Audience Expression Logic — Segment Boolean Behavior (AND vs. OR)

The expected behavior for the audience expression builder is that all non-geo segments (interest segments, Peak Performance / DS13, Mountain Match keywords, CRM) should be **OR'd together by default**. AND logic should only apply when explicitly triggered by the user via the audience narrowing toggle.

**Geo segments** are auto-AND'd and the narrowing toggle is disabled for geo — this is by design.

**Known bug (PRO-905 / AUD blocker, ~2026-05-27):** A bug was identified where the boolean relationship between Peak Performance and other segments depended on the **order in which segments were added** to the audience expression, not on any explicit user action. Specifically:
- Adding an interest segment, then enabling PP → PP and interest segment are OR'd (correct)
- Adding an interest segment, enabling PP, then adding an MM keyword → PP gets grouped with MM and the group is AND'd with the interest segment (incorrect)
- Adding MM keywords first, then adding an interest segment → all are OR'd (correct)

This order-dependency is a bug. The fix should ensure that regardless of addition order, all non-geo segments are OR'd unless the user explicitly narrows the audience. (via Mike Dolt, #targeting_helpdesk_ask_anything, 2026-05-27)

<!-- slack-extracted: 2026-05-30 -->
- **Underspend Incident — 2026-05-26 to 2026-05-28 (P0)**

A significant platform-wide underspend issue began on 2026-05-26. The leading theory among engineering and data teams was that it was caused by the BigQuery migration that went live on 2026-05-26, as the timing aligned precisely. Prospecting score distributions (2026-05-23 to 2026-05-28) showed no anomalies — score counts, campaigns, and advertiser coverage all increased normally during that window, ruling out scoring pipeline issues. The audience-service team confirmed no global overlays were added to production, only overlay infrastructure changes on 2026-05-27. A membership-consumer change (PR #145) on 2026-05-27 was identified but confirmed to be related to ghost bidding already under investigation, not the underspend root cause. A memdb-tf change was also identified but attributed to a separate outage. Spend ratio reached 75% on 2026-05-28, with the deepest underspend on that date. A P0 War Room was convened on 2026-05-29; bidding was restored and spend began catching up to budget by 2026-05-29 hr4 UTC. ProdOps/PER teams monitored over the weekend. Incident channel: #incident-20260528-pac. (via Mike Dolt, #targeting-squad, 2026-05-28)
- **Mountain Match (MM) Campaign Definition — Measurement Complexity**

When measuring the impact of Mountain Match (MM), campaign inclusion criteria must be very carefully scoped. The majority of MM campaigns have additional data sources, geo restrictions, or CRM list overlays layered on top of the core MM data sources, which significantly affects performance outcomes and makes clean attribution to MM itself impossible. For rigorous measurement, a "pure MM" campaign should be defined as one that uses ONLY the MM data sources (e.g., DS13 or DS46) with no additional modifications. Performance varies vastly across MM campaigns based on how advertisers have configured them. (via malachi, #tgt-infrastructure-squad, 2026-05-28)
- **BUK / DAR — Relationship to Mountain Match V2 and Cold-Start Handling**

BUK (leveraging DS19 as an input source) replaces some of the LLM-generated keyword pipeline but not the product categorization itself. BUK and Mountain Matched V2 (current production) together effectively define the next generation of Mountain Match — BUK does not replace Mountain Match V2 but combines with it. Mountain Matched V2 remains in place specifically to handle cold-start cases where BUK lacks sufficient data (new advertisers or keywords without enough history to generate recommendations). A new formal definition of "Mountain Matched" may be required that distinguishes pure-V2 campaigns from BUK-enhanced campaigns. (via Alex Knorr, #tgt-infrastructure-squad, 2026-05-28)

<!-- slack-extracted: 2026-06-02 -->
- **CoreDW Tableau Disconnection (2026-06-01):** CoreDW has been disconnected from Tableau. Approximately 100 dashboards were migrated and QA'd in under three weeks. Dashboards not marked as 'critical' or with high consistent usage will have stale data. Teams needing updates to non-migrated dashboards should submit a BAE ticket. Dashboard archival is continuing after additional sanity checks. (via Mike Dolzer, #engineering-team, 2026-06-01)
- **Campaign Stage Definitions:** MNTN campaigns target audiences across three retargeting stages: (1) **Stage 1 (Non-Engaged):** The audience the client is targeting in the UI — prospecting. (2) **Stage 2 (Engaged):** Any user who has seen a CTV ad from the campaign. (3) **Stage 3 (Engaged+VV):** Any user who has seen a CTV ad from the campaign AND registered a Verified Visit (VV). Stage 3 is also referred to as 'Multi-Touch Plus' in the database. Campaigns with very low budgets (e.g., $0.01–$0.02/day at the stage level) will not sync to Beeswax, as there is a minimum sync threshold of approximately $0.10. (via Tofer, #production-ops, 2026-06-01)
- **Budget Minimum Enforcement — Known Bug (2026-06):** There are three separate sets of business rules governing budget minimums that conflict with each other, creating a known bug that allows advertisers to launch campaigns with budgets as low as $0.01. The old control system includes legacy toggles in Command Center that, when enabled, override dynamic budget minimums and permit sub-cent budgets. A planned fix involves removing these legacy toggles entirely and relying solely on the dynamic budget minimum system. Campaigns with such low budgets cause downstream issues: Stage 2 and Stage 3 CIDs are stuck at $0.01 and do not sync to Beeswax, leading to underspend. A ticket and alignment meeting are in progress to define the path forward. (via Jordyn Betzer, #production-ops, 2026-06-01)
- **Mountain Match (MM) + Third-Party Segment Targeting Behavior:** When a third-party (3P) inclusion segment is added to a Mountain Match campaign, the combination uses an OR logic (not AND). This means the 3P segment does not restrict or refine the MM audience — it effectively adds an additional audience pool. As a result, adding a 3P inclusion segment to a MM campaign has no meaningful impact on targeting or performance. The MM + 3P (inclusion) combination is tied for the highest spend share on the platform at ~18.7%. This combination will not be removed from the UI because of the spend volume associated with it. When measuring MM performance, it is safe to include MM-only and MM + 3P (inclusion) campaigns together; other additions to MM campaigns materially affect performance and should be treated separately. (via malachi, #tgt-infrastructure-squad, 2026-06-01)
- **Fangorn — New Advertiser Vertical Auto-Enrollment (Wave 2, 2026-06):** Fangorn scores are being automatically assigned to new advertisers in a defined subset of verticals as part of a phased rollout. Express advertisers are explicitly included in this auto-enrollment. The logic for automatic enrollment by vertical is managed by Ryan. The release was delayed from the original date to avoid confounding with a concurrent system health evaluation. The list of included verticals is tracked in an internal Google Sheet. (via Alex Knorr, #tgt-infrastructure-squad, 2026-06-01)
- **Fangorn — Vertex-Based Pipeline vs. Experiment Pipeline:** The Fangorn production infrastructure currently uses a Vertex-based scoring pipeline. However, the experiment validating Fangorn was run against a different model object. Before switching the production workflow to point to the new model, the team must (1) validate the appropriate score threshold for the new model and (2) assess the impact on audience sizing. (via Alex Knorr, #tgt-infrastructure-squad, 2026-06-01)

<!-- slack-extracted: 2026-06-03 -->
- CoreDW (the legacy Postgres data warehouse) has been officially shut down. All users were removed from the warehouse as of 2026-06-02. The shutdown was confirmed by Mike Dolzer via the engineering-team channel. (via Mike Dolzer, #engineering-team, 2026-06-02)

<!-- slack-extracted: 2026-06-06 -->
- **SOPS Secrets Management — Policy Change (Security Hardening)**

As of mid-2026, SOPS decryption access has been revoked from all developers. Only DevOps personnel can decrypt secrets when needed. The new secrets management strategy is being rolled out in phases:

1. **Preferred approach:** Minimize secrets by using environment-native identity (e.g., service accounts for workload authentication) instead of raw credentials.
2. **Fallback (external vendor API keys, etc.):** HashiCorp Vault with DevOps gating.
3. **SOPS:** Retained only for edge cases beyond the above two approaches.

**Interim guidance:** Use 1Password to share secrets while new processes are finalized. Do not share `.env` files over Slack.

**Longer-term direction (per Rogus/Harvey):** Each team will run its own ArgoCD repo with isolated RBAC, and Kargo will be used for scripted auto-promotions. CODEOWNERS path-based rules will also be added to `mntn-argocd`.

**Branch protection update:** Direct pushes to `main` in `mntn-argocd` are now blocked; a PR is required. Approvals policy and incident-time escalation paths are still being defined. (via Rogus, Harvey Yau, Tasha, #engineering-team, 2026-06-05)
- **Pause Ads — Manual Operations End-of-Life (Q2 2026)**

Pause Ads campaigns have been managed manually (including app bundle list configuration in Beeswax) since last quarter, primarily by the AdOps/campaign management team. Key operational notes:

- The current sync configuration **disables certain Beeswax settings** for Pause Ad campaigns to prevent the MNTN sync from overriding manual Beeswax configuration.
- **Automation was deprioritized by the Select team** and has not been implemented.
- Pause Ads will **no longer be sold as a manual product starting next quarter** (after end of current month). Support is only required through end of the current billing period.
- A ticket was requested to clarify requirements if deal assignment automation is still needed for any remaining Pause Ads campaigns. (via Meghan Besse, Zafar, Kirsa, #production-ops, 2026-06-05)

<!-- slack-extracted: 2026-06-09 -->
- **Creative Upload — known incident: MT creatives stuck in 'Analyzing' state (2026-06-08)**

On 2026-06-08, newly uploaded creatives were not progressing past the 'Analyzing' state, potentially affecting all advertisers uploading creatives. 27 advertiser IDs were identified as potentially impacted. A separate but related incident on the same day caused HTTP 500 errors when attempting to create or save Multi-Touch (MT) eligible creatives. Both issues were resolved the same day (confirmed by Jen Wang). JIRA references: PS-8255 (analyzing state), PS-8264 (MT upload 500 error). (via David Rose, #mission-control, 2026-06-08)
- **Campaign Creative Group IP Bleed monitor — false positive due to creative sequence feature**

An increase in the 'Campaign Creative Group IP Bleed' system signal (alert [629]) observed since 2026-06-02 is a false positive caused by the 'creative sequence' feature released on 2026-06-01. The existing IP bleed monitor does not account for delivery mode sequencing. A ticket is being opened with the DM (Delivery Management) squad to update the monitor to exclude delivery mode sequencing from IP bleed tracking. The bleed increase is not operationally concerning. (via bermudez, #mission-control, 2026-06-08)
- **Hard-deleting campaign/audience entities — policy**

MNTN does not hard-delete campaigns or audiences from the database. Due to reporting lineage and activity tracking requirements, entities should not be removed even when unused. For campaigns stuck in broken states (e.g., `ACTION_REQ` from a legacy Express campaign post-PTV migration), the preferred remediation is to update a status field to a hidden/inactive state rather than deleting the record. Audiences tied to such campaigns cannot be deleted from the UI until the campaign relationship is resolved. (via ray, #reporting_helpdesk_ask_anything, 2026-06-08)
- **Express-to-PTV migration — known gap: legacy Express campaigns inaccessible in PTV UI**

When an advertiser is migrated from Express to PTV, their pre-migration Express campaigns remain in the system but cannot be viewed or modified through the PTV UI. Audiences attached to those legacy Express campaigns continue to appear in the PTV audience selector, but cannot be deleted because the underlying campaign relationship still exists and the campaign is in a broken/inaccessible state. MNTN admin cannot fix or stop these campaigns through normal UI flows if the campaign has a broken budget/schedule state. Workaround: manually update the campaign or audience status in the DB to a hidden state. (via Tyler Rauch, #reporting_helpdesk_ask_anything, 2026-06-08)
- **Stage 2 / Stage 3 campaign shutdown — no bulk tool currently available**

As of June 2026, there is no built-in bulk tool to shut down Stage 2 and Stage 3 campaigns across many campaign groups for a given advertiser. The current approach requires going through Campaign Controls (CC) one campaign group at a time, which fires the necessary side effects to turn off stages in Beeswax and reallocate budget. Applying advertiser-level defaults only affects future campaign groups, not existing ones. A historical script for bulk stage shutdown has existed but is not standardized. Budget reallocation should be spot-checked after manual shutdowns. (via Jordyn Betzer, #production-ops, 2026-06-08)
- **SQLMesh prod access guard — Cloud Identity API dependency**

Running `sqlmesh plan` locally requires the Cloud Identity API to be enabled in the GCP project being targeted. If the API is disabled (e.g., in `dw-main-gold`), the prod access guard script (`sqlmesh_prod_access_guard.py`) will throw an `HttpError 403` when checking Google Group membership. Resolution: enable the Cloud Identity API in the relevant GCP project via the Google Cloud Console. Changes may take a few minutes to propagate. (via scotty, #data-platform, 2026-06-08)
- **Sarah (new EA for Richard) — org update**

Sarah has joined as an executive admin for Richard (executive leadership). She will be running key executive initiatives in addition to admin responsibilities. For scheduling with Richard, both Kiana and Sarah should be included while Sarah gets onboarded. (via richard, #engineering-team, 2026-06-08)
- **SEL (Select) campaign groups on MNTN Bidder — budget configuration issue**

As of 2026-06-08, SEL campaign groups on the MNTN Bidder have budget set at the campaign group (CG) level but not at the campaign ID (CID) level, causing a 'no spend' state. This was identified in the 'no spend check' report and is being addressed via PER-6472 (targeted for completion EOW 2026-06-08). The Select team actively uses the QA environment for development. (via Johnny, #mission-control, 2026-06-08)

<!-- slack-extracted: 2026-06-10 -->
- ## GCP Just-in-Time Access (PAM) Migration

MNTN has migrated GCP privileged access from standing grants to just-in-time (JIT) elevation via Google's Privileged Access Manager (PAM). Standing privileged access has been reduced to near-zero.

**What changed:**
- Broad standing grants for `gcp-dev-eng@` were removed: project editor, cluster exec (`container.developer`/`clusterAdmin`), node SSH, IAM-admin, and SA impersonation.
- All engineers now have org-wide browser + PAM viewer baseline (read/metadata access only).
- Elevated access (deploy, `kubectl exec`, read a secret) must be requested per-project and auto-expires.

**How to request access:**
- Console: IAM & Admin → Privileged Access Manager → Grant access (pick project + entitlement)
- CLI: `gcloud pam grants create --entitlement=<ENTITLEMENT_ID> --project=<PROJECT_ID> --location=global --requested-duration=3600s --justification="<ticket / why>"`
- Approvals go to the DevOps team (per-project approvers to be added over time). All grants are logged and auto-revoked at expiry.

**Gotcha:** Actions that previously "just worked" (e.g., `kubectl exec`, editing resources, reading secret values) will now return permission errors until access is explicitly requested. (via Tim OBrien, #engineering-team, 2026-06-09)
- ## Secrets Cleanup — Compliance Fire Drill (June 2026)

MNTN underwent a company-wide, all-hands secrets cleanup initiative driven by compliance requirements. All plaintext secrets, API keys, and tokens stored in codebases were required to be rotated and removed.

**Scope:** All secrets found in codebases are tracked in an internal spreadsheet. Engineers were directed to double-check their own repos for additional secrets not caught by the initial scan.

**Clarifications:**
- This includes DB passwords and service passwords stored in code — those also require rotation.
- The initiative targets *insecurely stored* plaintext secrets, not the elimination of secrets/keys/tokens altogether (Paulo clarified this distinction).
- GitHub Actions secrets should be migrated to Vault per the new secrets management SOP: `SteelHouse/mntn-devops/docs/standard_operating_procedures/052-secrets-management-strategy.md`.
- For secrets exposed in git history, the standard approach is to rotate the exposed secret and ensure Vault is used going forward (not necessarily rewriting git history, which requires DevOps involvement).

**Priority:** All engineering work was paused until 100% of secrets were rotated and removed from source control. (via richard, #engineering-team, 2026-06-09)
- ## Campaign Sync Issue — Gary/SQS Credential Rotation (June 2026)

A wave of campaign groups (CGIDs) failed to sync to Beeswax following a DevOps credential rotation for the Gary/SQS AWS integration. The issue caused no-spend conditions for affected campaigns.

**Root cause (suspected):** DevOps modified the credentials/keys/permissions for Gary's SNS/SQS AWS integration. This appears to have caused silent failures on some campaign group sync messages, though no explicit errors were observed in Gary-side logs. Only a subset of CGIDs were affected (not all).

**Workaround:** Manual re-sync via Gary causes updates to propagate downstream into Beeswax successfully. Affected CGIDs were identified via a Mode report and re-synced by the PER team.

**Team responsibilities for sync issues:**
- ProUI team: investigate no-spend root cause per line item.
- PER (PAC) team: execute re-syncs for CGIDs with confirmed sync failures.
- Jordyn Betzer (ProUI): owns root cause investigation and Gary logging improvements.

**Related ticket:** CAM-796 (longer-term fix).

**Note:** CGID 127404 was a separate issue (not part of the sync fix) and was resolved independently. (via Jordyn Betzer, #mission-control, 2026-06-09)
