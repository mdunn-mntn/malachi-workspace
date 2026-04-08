# Strategic North Star — Q2 2026
Last updated: 2026-04-08

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
