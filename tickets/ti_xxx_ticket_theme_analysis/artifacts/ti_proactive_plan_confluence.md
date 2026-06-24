# Audience Intelligence: Recurring Analyses and How to Automate Them

We looked at a full quarter of analysis work — **57 tickets, ~Mar–Jun 2026.** The work clusters into a few recurring **problem types**. For each: how many tickets it took, its share of the work, **what we did** to solve it, and **how we can solve it automatically** so we stop re-running the same analysis by hand.

*Author: Malachi Dunn · Audience Intelligence (AUDI). For incoming TPM review.*

---

| Problem type | Tickets | Share | What we did | Solve it automatically |
|---|:--:|:--:|---|---|
| **Incrementality** — "did your targeting actually *cause* this, or would it have happened anyway?" | 16 | 28% | Built a counterfactual by hand for each ask — holdout / ghost-bid lift, CausalImpact + difference-in-differences, power/MDE calcs — in a one-off notebook per advertiser or rollout (ElevenLabs, Root, Fangorn tiers, Max Reach, Media Plan, Jaguar). | **Standing incrementality pipeline:** always-on holdouts + a pre-flight power/eligibility gate + automatic lift tests. *Pre-flight power in progress (Chris Franz); always-on tests are the main gap — buildable on the new bidder.* |
| **Data trust** — "can I *trust* the numbers you report (households reached, visits, CRM match, NTB)?" | 13 | 23% | Forensic one-off anti-joins and lineage traces per incident — households-reached reconciliation, CRM match rate, verified-visit lineage, IPDSC/identity coverage, CDC parity — often on data that had already expired. | **Standing coverage / freshness / reconciliation monitors with alerting,** generalized from the one we already productized (TI-253 daily anti-join). *Longer-term — next sprint and beyond.* |
| **Audience / vendor quality** — "is the audience / segment / vendor I'm paying for *worth* it?" | 8 | 14% | Decomposed each advertiser's audience and scored segment / keyword / vendor quality by hand (Orange Theory, 5×5 vendor eval, BUK keyword value, interest-segment sizing). | **Scheduled quality scoring** (done for segments) surfaced as quality scores + automatic answers in the buyer UI at selection time, plus a per-vendor renewal scorecard. *In progress, tied to the new campaign changes.* |
| **Performance drop** — "why did my performance suddenly *drop* after your rollout?" | 6 | 11% | Ran a manual multi-query diagnostic per advertiser — audience expression, score gating, deliverability, peer benchmarking — to find the real cause (e.g., AutoCamp's drop was a year-over-year scaling outlier, not the model). | **Automatic performance-drop checks** on campaigns and their audience expression, plus a self-serve advertiser diagnostic and flip-readiness alerts. *Building now.* |
| Foundational — the targeting signal + durable knowledge under all of the above | 14 | 25% | Built feature-store features, scoring pipelines, and the experiment archive that make the answers above possible. | Largely already productized (feature store, scheduled scoring, experiment archive). *Ongoing.* |

**75% of the quarter (43 of 57 tickets) is reactively answering the four customer problems above — each rebuilt by hand.**

---

## Where this leaves us

Three of the automations are already in motion — **audience quality scores, performance-drop checks, and pre-flight power.** The two gaps that still need an owner:

1. **Automatic, always-on incrementality tests** — we don't have these; they can be built on the **new bidder system.**
2. **Data-quality monitors** — longer-term; generalize the TI-253 pattern.

*Detailed theme breakdown and the full ticket→problem map for all 57 tickets live in the team's analysis workspace.*
