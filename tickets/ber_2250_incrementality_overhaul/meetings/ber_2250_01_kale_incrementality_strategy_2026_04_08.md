# BER-2250: Kale McNaney — Incrementality Strategy Direction

**Date:** 2026-04-08
**Attendees:** Kale McNaney (Director), Malachi Dunn
**Type:** 1:1 strategic direction

---

## Key Takeaways

### Incrementality is THE #1 Priority
Kale's words: "The most valuable thing right now is getting this incrementality thing out." Everything regresses to incrementality / incremental ROAS at the end of the day. Solving incrementality is "super super important" — the biggest OKR thing.

### The Core Problem
MNTN likely looks bad on third-party incrementality platforms (LiftLab, Kochava) because **everything is optimized toward the visit**. We almost certainly look poor on incremental metrics when measured externally.

### Why This Matters for Growth and Retention
- Solving this would be **HUGE** — dramatically change growth and retention
- Customers care about incrementality at specific lifecycle points:
  - **Evaluation:** When adding CTV as a new channel to their marketing mix
  - **Periodic budget planning:** "How incremental are my channels today vs last year? Should I pour more or less into CTV?"
  - Once they figure it out, they shift objectives to something else (reach, performance)
- If advertisers trust LiftLab, **we have to trust LiftLab** — need a dedicated LiftLab liaison / DS

### The Strategic Shift
- **Shutter internal incrementality dashboards** — move to third-party platforms
- Messaging: "We changed the way we do incrementality" — work with approved vendors
- Customer-driven incrementality — let the customer choose their vendor from our approved list
- OKR: **Run 5 experiments with external vendors**

### What If 60% of Targeting Audience Is Not Incremental?
- Actionable: we show up poorly on 3rd-party incrementality platforms
- Not only need to measure what we know isn't incremental — need to **change targeting methodology**
- When advertisers come to MNTN, ask their funnel AND which feature they want: reach, performance, or incrementality
- **Tailor their specific experience** around what their desire is

### Revenue / Profit Impact
- We charge on **CPM not CPV** — so incrementality changes shouldn't directly affect profit
- But may affect **performance metrics** (IVR) — if we start adjusting for incrementality, IVR performance will suffer
- TI could appear to be performing worse on visit-based metrics
- Incrementality is like a bucket/category of measurement — if the North Star is IVR and we adjust incrementality, IVR performance suffers

### Incremental ROAS Is the Top Metric
- Not incremental visits, not incremental impressions — **incremental ROAS**
- This is what external vendors measure
- This is what advertisers care about

### External Vendors
- **LiftLab** — keeps coming up, primary vendor
- **Kochava** — another option
- Possibly more vendors
- Need to ignore/shutter internal dashboards in favor of these

---

## Implications for TI-835 / Our Observational Analysis

Our "two stories" finding maps directly to Kale's concern:

1. **guid_log (total traffic) shows ~0% lift** — this is closer to what external vendors like LiftLab would measure (total incremental effect on business outcomes). This confirms Kale's fear: MNTN looks bad on true incrementality.

2. **clickpass_log (attributed visits) shows 2-8x lift** — this is what our internal dashboards measure. It looks great because it's measuring the attribution path, not true incrementality.

**The gap between these two stories IS the problem Kale wants to solve.**

When LiftLab runs an incrementality test, they're measuring something much closer to guid_log (did total conversions increase?) than clickpass_log (did MNTN-attributed conversions increase?). Our internal metrics overstate true incrementality because they measure attribution capture, not net new business outcomes.

## Action Items

- [ ] Reframe TI-835 presentation around Kale's strategic direction — the two stories finding isn't just academic, it's the core of why we look bad on LiftLab
- [ ] Research LiftLab methodology — how do they measure? What tables/signals does their approach correspond to?
- [ ] Understand: can we identify which targeting audiences ARE incremental vs not?
- [ ] Think about: how would we change targeting methodology to optimize for incrementality instead of visits?
- [ ] OKR: 5 experiments with external vendors — what does this look like operationally?
