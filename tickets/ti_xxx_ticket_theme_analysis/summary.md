# Ticket Theme Analysis — summary

## Introduction
Bryce Wagg (PMO) asked, in the #analysis Slack thread (cc Alyson Lefkowitz), to "run Claude against all the tickets over the last quarter and suggest customer pain points or themes that we can use to group future tickets into epics." Goal: better represent the *types* of analysis the AUDI team does, and tell a story about tooling gaps / questions customers are asking, so future tickets can be grouped into epics in retrospect.

No Jira ticket number assigned (ad-hoc PMO request via Slack). Folder uses the `ti_xxx_` placeholder convention.

## Problem
- Every ticket was recently filed under an epic, but ad-hoc analyses (e.g. TI-1044 ElevenLabs, TI-1045 client incrementality direction) have no natural epic home.
- The team's analysis work isn't represented as recurring *themes* — so tooling gaps and repeated customer questions aren't visible at the planning layer.

## Method
1. Inventoried all ticket folders: 57 with content (56 with `summary.md`; `ti_argocd_secrets_audit` is an empty stub).
2. Ran a background multi-agent workflow (`ticket-theme-analysis`):
   - **Read phase** — one agent per folder read `summary.md` (+ presentation if present) → structured record (trigger, analysis_type, customer/stakeholder, business question, pain point, **tooling gap**, KPIs, reusable artifact, theme guess). 57/57 read.
   - **Synthesize phase** — three independent lenses over all 57 records: (a) customer pain points / questions, (b) tooling gaps / productization, (c) analysis-type taxonomy → candidate epics.
   - **Merge phase** — reconciled the three lenses into 8 themes + recurring customer questions + cross-cutting tooling gaps.
3. Pulled the live TI epic backlog (86 epics, `/rest/api/3/search/jql`) and mapped each theme to existing epics vs. gaps.
- Raw output: `outputs/theme_analysis_raw.json`. Epic list: `outputs/ti_epics_2026_06_24.tsv`.
- Workflow stats: 61 agents, ~3.1M tokens, ~6 min wall.

## Solution / Findings
**Headline:** Customers keep asking "is this real?" and almost every answer is a hand-built one-off. The portfolio reads as four recurring customer questions (incrementality, audience/vendor worth, performance-drop, trust-the-numbers) each paired with a tooling-gap twin (the same analysis re-run by hand).

**8 candidate epics** (full detail + ticket→theme map + epic-home status in `artifacts/ti_ticket_theme_epic_map.md`):
1. Incrementality Measurement & Power Gating — released-only epics, no standing home
2. Rollout / Feature-Lift Evaluation Pipeline — **gap** (evaluation harness ≠ feature-rollout epics)
3. Audience, 3P-Segment & Vendor Quality — released-only epics
4. **Advertiser Decision Support & Diagnostics — no epic** (← TI-1044/1045 home)
5. Feature-from-Analysis / Feature Store — covered (TI-789/718/566)
6. Identity, Coverage & Metric-Integrity Monitoring — partial (TI-822/495)
7. RTC & Rollout Performance Monitoring — partial; fold into #6
8. Durable Knowledge, Reference & Infra Hygiene — partial (TI-732/602)

**Ticket counts by bucket** (each ticket once; 43/57 = 75% directly answer a customer question):
- Q1 Incrementality "did you cause it?" — 16 · Q4 Trust "can I trust your numbers?" — 13 · Q2 Audience/vendor worth — 8 · Q3 Performance drop — 6 · Foundational enablers — 14.

**Proactive story:** *Each question is a product we haven't built yet.* Today every answer is reactive (L0–L1 on the maturity ladder); the proactive target is L3 (self-serve / in-product / alerting). Three moves: (1) get ahead of the #1 churn question — always-on incrementality + pre-flight power gate; (2) put audience/vendor quality in the buyer UI at selection time; (3) watch the pipes — standing reconciliation/freshness monitors with alerting. The self-serve Advertiser Diagnostic (Epic A) is the connective tissue. Detail + maturity-ladder table in the artifact.

**Three recommended NEW epics** for homeless-but-recurring work:
- A. Advertiser Decision Support & Diagnostics (reactive per-advertiser analysis; TI-1044, TI-1045, TI-1026, TI-1027, TI-1017, TI-644, TI-501, TI-896, TI-1037)
- B. Rollout & Incrementality Evaluation Tooling (the reusable DiD+CausalImpact+power-gating harness)
- C. Data-Quality & Identity Monitoring (generalize TI-253 into standing monitors)

## Questions answered
- **What's the theme of TI-1044/TI-1045?** Advertiser Decision Support — applying incrementality/measurement expertise to one client's decision. They don't fit the *build* epics; they need a new "apply" epic (Epic A above).
- **Which themes already have epic homes vs. gaps?** See table in the artifact; biggest gap = Advertiser Decision Support.

## Published
- **Confluence (TAR / Targeting space):** "Audience Intelligence: Recurring Analyses and How to Automate Them" — page id 3668312118 (v2, simplified per Bryce: problem type → tickets/share → what we did → how to automate) — https://mntn.atlassian.net/wiki/spaces/TAR/pages/3668312118 — for incoming TPM review. Source: `artifacts/ti_proactive_plan_confluence.md`; storage XHTML: `artifacts/confluence_body.html`.

## Open items
- Decide whether to create the 3 new epics (Bryce/PMO call).
- TI-1045 has no GitHub workspace folder → create a stub if it should be tracked in this corpus.
- Optional: a one-page / RevealJS version for an Alyson/leadership share-out.

## Knowledge-base updates
- None to data_catalog/data_knowledge (this is meta/PMO work, not new schema). Method captured here for reuse next quarter.
