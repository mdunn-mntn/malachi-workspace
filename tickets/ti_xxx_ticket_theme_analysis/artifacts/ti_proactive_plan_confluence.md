# Audience Intelligence — From Reactive Analysis to Proactive Capabilities

**Purpose:** Represent the *types* of analysis the Audience Intelligence (AUDI) team does, surface the recurring **customer questions** and **tooling gaps** behind them, and lay out a **plan** to shift from answering each question by hand to standing, proactive capabilities.
**Audience:** Incoming TPM (for review), PMO (Bryce), leadership (Alyson).
**The outstanding question this doc is meant to drive:** *what do we do with this to resolve it* — how to prioritize, sequence, resource, and own the proactive capabilities below.
**Author:** Malachi Dunn · Audience Intelligence (AUDI)

---

## TL;DR

- We analyzed a full quarter of analysis work — **57 tickets, ~Mar–Jun 2026.**
- **75% of it (43 of 57 tickets) is reactively answering four customer questions**, each rebuilt by hand, one advertiser at a time.
- **Each question is a product we haven't built yet.** The proactive fix is to turn each into a standing capability that answers it before — or the moment — it's asked.
- **Three of those capabilities are already in motion** (performance-drop checks, pre-flight power, audience quality scores); **the two biggest gaps are unowned** (automatic always-on incrementality tests, and data-quality monitors).

---

## 1 · The pattern — four questions customers keep asking

| The customer question | Tickets | Share |
|---|:--:|:--:|
| **Q1 · Incrementality** — "did your targeting actually *cause* this, or would it have happened anyway?" | **16** | 28% |
| **Q4 · Trust** — "can I *trust* the numbers you report (households reached, visits, CRM match, NTB)?" | **13** | 23% |
| **Q2 · Audience/Vendor** — "is the audience / 3P segment / vendor I'm paying for *worth* it?" | **8** | 14% |
| **Q3 · Performance** — "why did my performance suddenly *drop* after your rollout?" | **6** | 11% |
| Foundational — targeting signal + durable knowledge (the enablers) | 14 | 25% |
| **Total** | **57** | 100% |

Named-advertiser escalations make these concrete: ElevenLabs and Root (Q1, ran their own lift tests and found ~0), Orange Theory and 5×5 (Q2, "this delivers 8–10× worse"), AutoCamp (Q3, "ROAS fell 8×→2× after your flip"), mid-market households-reached reconciling off by up to 66% (Q4).

---

## 2 · The cross-cutting tooling gap

The same systematic analysis is rebuilt by hand for one advertiser / experiment / vendor after another. Maturity ladder:

`L0 one-off notebook → L1 rerunnable script → L2 scheduled pipeline → L3 self-serve / in-product / alerting`

Almost everything sits at **L0–L1**. The handful that reached **L2** prove the team can productize: TI-253 (daily missing-domain monitor), TI-849→921 (wave-aware lift pipeline), TI-956 (scheduled segment-quality scoring), TI-1037 (parameterized advertiser diagnostic). The proactive target is **L3**.

---

## 3 · The proactive target — three moves

> **Stop answering these one advertiser at a time. Turn each question into a standing capability.**

1. **Get ahead of the #1 churn question (incrementality)** — always-on holdouts + a pre-flight power gate, so we surface lift *before* the advertiser's data-science team finds ~0 and churns.
2. **Put quality where the decision is made (audience/vendor)** — quality scores in the buyer UI at selection time, not in an analyst's notebook after a complaint.
3. **Watch the pipes (trust + performance)** — standing monitors + alerts so we catch drops, drift, and reconciliation gaps before the advertiser does.

The self-serve **Advertiser Diagnostic** is the connective tissue — the on-demand report (audience-expression decomposition, size funnel, score gating, peer benchmarking) that answers per-account questions across all three.

---

## 4 · The plan — current state & sequencing

What we're actually doing about it today, and what's still open. **This is the part that needs a TPM to drive.**

### NOW — in progress
| Capability | Answers | Status | Owner / notes |
|---|---|---|---|
| **Automatic performance-drop checks** — monitor campaigns + their audience expression to flag regressions proactively | Q3 | Building now | AUDI team |
| **Pre-flight power / eligibility calc** — is an advertiser already powered to detect lift before we commit budget? | Q1 (gate) | In progress | **Chris Franz** |
| **Audience quality scores + automatic answers** — surface segment/audience quality, tied to the new campaign changes | Q2 | In progress | AUDI; linked to new campaign-objective changes |

### NEXT — the biggest gap (needs the new bidder)
| Capability | Answers | Status | Enabler |
|---|---|---|---|
| **Automatic / always-on incrementality tests** — we do *not* have these today | Q1 (test) | Not built — gap | Can be set up on the **new bidder system** |

### LATER — longer-term
| Capability | Answers | Status | Notes |
|---|---|---|---|
| **Data-quality monitors** — standing coverage / freshness / reconciliation checks with alerting | Q4 | Future ticket (~next sprint+) | Generalize the TI-253 pattern |

---

## 5 · How this groups into epics

Mapped against the live TI epic backlog (86 epics): feature-store and Fangorn-rollout work is well-covered; incrementality and segment-quality epics exist but are mostly *Released/Closed* point-deliverables, not standing homes. **Three gaps have no epic home and are candidates for new epics:**

- **New Epic A — Advertiser Decision Support & Diagnostics.** Reactive per-advertiser analysis (validate a lift claim, advise a setup, diagnose a drop, evaluate an audience). Home for the ad-hoc tickets (e.g. TI-1044 ElevenLabs, TI-1045 client incrementality direction) plus the diagnostic tool (TI-1037).
- **New Epic B — Rollout & Incrementality Evaluation Tooling.** The reusable measurement harness (DiD + CausalImpact + power gating) and the always-on incrementality tests — distinct from the feature-rollout epics that ship the features.
- **New Epic C — Data-Quality & Identity Monitoring.** Standing monitors generalized from TI-253.

*Full theme-by-theme breakdown and the ticket→theme map for all 57 tickets live in the team's analysis workspace.*

---

## 6 · Open questions for the TPM

1. **Sequencing & resourcing** — the three NOW items are moving; who drives them to L3 (self-serve / alerting) vs. stalling at L1?
2. **Automatic incrementality tests (the big gap)** — this depends on the new bidder system, so it needs coordination with the bidder team. Who owns that dependency and timeline?
3. **Data-quality monitors** — longer-term and currently unowned. Fold into next-sprint planning, or stand up a dedicated workstream?
4. **Epics** — do we formalize the three new epics (A/B/C) so future ad-hoc work has a home, instead of landing un-grouped?

---

## Appendix · ticket counts by theme (each ticket counted once)

| Theme | Bucket | Tickets |
|---|---|:--:|
| Incrementality Measurement & Power Gating | Q1 | 6 |
| Rollout / Feature-Lift Evaluation Pipeline | Q1 | 10 |
| Audience, 3P-Segment & Vendor Quality | Q2 | 8 |
| Advertiser Decision Support & Diagnostics | Q3 | 4 |
| RTC & Rollout Performance Monitoring | Q3 | 2 |
| Identity, Coverage & Metric-Integrity Monitoring | Q4 | 13 |
| Feature-from-Analysis / Feature Store | Foundational | 7 |
| Durable Knowledge, Reference & Infra Hygiene | Foundational | 7 |

**Method:** automated read of every ticket's working summary → structured extraction → three independent synthesis lenses (customer pain / tooling gaps / analysis taxonomy) → reconciled into themes → mapped against the live TI epic backlog.
