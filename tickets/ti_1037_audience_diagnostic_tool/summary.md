# TI-1037: Automate client-performance diagnostics (audience-expression-driven)

**Jira:** https://mntn.atlassian.net/browse/TI-1037
**Status:** In Progress (sprint 06/15–06/29, id 6926) — **UNBLOCKED** 2026-06-18 (Chris Addy deliverability deep-dive done)
**Parent:** TI-602 · **Relates-To:** TI-1026 (prototype), TI-956 (segment-quality engine) · **Assignee:** Malachi

---

## Current state (read first if you're a new session)
> **New chat?** Paste the block in [`HANDOFF_PROMPT.md`](HANDOFF_PROMPT.md). It primes a fresh session with the spec,
> the prototyped modules, the load-bearing facts, and next actions.

- **Spec is written:** `knowledge/audience_diagnostic_playbook.md` — the diagnostic as steps 0–9 (each = a tool module:
  question → query → interpretation → gotcha). **Step 9 (deliverability) is now scoped** (see §4 below).
- **Prototype is done:** every step 0–8 is prototyped in `tickets/ti_1026_orange_theory_audience_eval/` (queries/ +
  artifacts/ + a full worked example in its summary.md).
- **Nothing built here yet** — this folder is scaffolding + handoff. Next is the parameterized build (plan being drafted).

## 4. Step 9 deliverability — scoped (Chris Addy deep-dive, 2026-06-18)
**There is NO predictive targetable-IP model.** The platform does not compute "what % of an audience's IPs will be
biddable in period X." So the tool must never promise a targetable-% number — answer deliverability empirically.
**Deliverability target = peer pacing to 96% of budget:** what did *comparable* campaigns require (last ~60–90 days)
to hit **96% of budget** (the "fully delivered" bar), then judge this campaign's spend vs that peer envelope, scaled by
flight length. → **Step 9 = a peer-pacing benchmark, a sibling of Step 7's peer-VR benchmark** — queryable in-tool from
`spend_log` (nanosecond epoch) + the budget/cap field (TBD: campaign config / dso) + delivery logs. No external Olympus
dependency. Comparable-cohort selection is the design-sensitive part. Full detail: playbook Step 9 + `data_knowledge.md`
("How deliverability is actually set"). Distinct from the Media-Plan `deliverability_classification` guardrail risk bucket.

## 1. Introduction
TI stakeholders repeatedly ask "why is this client performing this way / why is the audience small / which 3P segments
should they use?" The first step is always the audience expression, and the analysis is highly systematic (proven by
TI-1026, Orange Theory). This ticket productizes that into a parameterized tool / query-series.

## 2. The Problem
Today every such question is a bespoke investigation. We want an on-demand diagnostic that takes an
`advertiser_id` (+ `audience_id`/`campaign_id`) and emits a standard report covering: expression decomposition,
3P-segment quality, keyword evaluation, the size funnel (geo + exclusions), scoring/HHST, availability, targeting-vs-
creative, UI-size-vs-deliverable, and (once scoped) deliverability.

## 3. Plan of Action
1. ✅ **[UNBLOCKER]** Deliverability deep-dive with **Chris Addy** (Olympus/media-plan) → step 9 scoped (§4, 2026-06-18).
2. ⏳ Define the parameterized diagnostic spec (inputs + the 10 modules steps 0–9; what each emits).
3. ⏳ Build the query series / tool (productize the TI-1026 queries; parameterize advertiser/audience/campaign).
4. ⏳ Standardize the output report + validate on a second advertiser.

## 4. Key references
- Spec: `knowledge/audience_diagnostic_playbook.md`
- Prototype + worked example: `tickets/ti_1026_orange_theory_audience_eval/` (`summary.md`, `queries/`, `artifacts/`)
- Backing knowledge: `knowledge/data_knowledge.md`, `knowledge/data_catalog.md` (segment-expression/DS14, HHST gate,
  ipdsc hygiene, ui.audience_keyword_state, UI-size source, 3P demo-data quality, MaxMind geo-fence)
- Full priming detail: `HANDOFF_PROMPT.md`

## 5–8. Solution / Q&A / Doc updates / Open items
_(pending build)_ Open items = the 4 plan steps; step 1 (Chris Addy) is the blocker.
