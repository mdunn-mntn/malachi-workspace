---
name: Bidder-level ghost bidding approved as BER-2250 go-forward (2026-05-04)
description: After the Monday 2026-05-04 shareout review of TI-837 v5 + TI-884 power analysis, leadership approved bidder-level ghost bidding as the path forward for incrementality measurement at MNTN. TI-837, TI-884, TI-885 closed; bidder-process implementation is our workstream. TI-886 T-learner model is NOT ours (Alex confirmed 2026-05-08, reassigned).
type: project
originSessionId: 1ddedb6a-ff08-4281-8285-aab919ee6906
doc_type: memory
keywords: [ghost bidding, bidder-level, ber-2250, ti-837, ti-884, incrementality, ghost bid logging, t-learner ti-886, ascent team, iroas lift]
domain: [project, incrementality, bidding]
lifecycle: active
last_verified: 2026-06-01
---
**Decision:** 2026-05-04 review approved bidder-level ghost bidding as the canonical incrementality measurement and modeling approach for BER-2250.

**SHIPPED 2026-05-27 (Ryan Kleck DM 2026-06-01):** ghost-bid logging is now live in both bidders. **No backfill — data from 2026-05-27 forward only.** Field identifiers and BQ silver column locations are documented in `knowledge/data_knowledge.md` § "Ghost Bids — Bidder Feature". Ghost WINS are not logged — only ghost bids; downstream analysis must simulate wins via per-campaign / per-advertiser win-rate. Open design debate on whether a Scylla push + ghost-win simulation service is needed: see [Ghost Win Simulation Discussion](https://mntn.atlassian.net/wiki/spaces/DATA/pages/3608150103/Ghost+Win+Simulation+Discussion). With ghost bids live, the downstream BER-2250 workstreams that were "blocked on bidder-process implementation" are now eligible to start — 30-day window run, net-new cohort, segment-level lift refresh.

**Why:** the post-hoc analytical approach (TI-837 v5 ghost-bidding ATT against augmentor logs) demonstrated the methodology works and produced defensible per-segment lift numbers, but is bounded by:
- Augmentor 10-day TTL — can't extend beyond ~30 days even with Databricks GCS reads (Phase 2a is the limit)
- Random hash subsampling matches denominator size but doesn't replicate bidder selection logic — leaves CIA assumption fragile for retargeting
- Re-runs require expensive BQ scans / Databricks compute per analysis

Bidder-level ghost bidding solves all three: the bidder evaluates each auction "as if it would have served" without actually serving, producing a continuously-updated ground-truth treated/holdout split with no TTL constraint and no random-subsampling proxy.

**How to apply:**
- Treat TI-837 v5 results as the methodological foundation, not the ongoing measurement system.
- The +21pp / +30pp retargeting lift, ≈0 Stage 1 prospecting, and segment-specific framing are validated and should inform any near-term targeting / iROAS conversation.
- For NEW incrementality work, default to "wait for the bidder-level system" rather than re-running the post-hoc analysis. Re-runs only when there's a specific question the bidder system can't yet answer.
- **TI-886 (T-learner uplift model) is NOT our task** — Alex confirmed 2026-05-08 it's been reassigned to another owner. Do not pick that workstream up. Our side is the bidder-process implementation, not the model.

**Closed tickets (2026-05-04):**
- TI-837 — ghost bidding experiment design, mid-intent focused. Done.
- TI-884 — power & sample size analysis, iROAS measurement capacity. Done.
- TI-885 — mid-intent treatment experiment setup. Done.

**Active workstreams (post-decision):**
- TI-886 — uplift T-learner model. **Reassigned away from us 2026-05-08 (Alex).** Owned by another person — do not work on this.
- **Bidder-process implementation — our workstream and the upstream blocker for nearly all BER-2250 follow-up analysis.** Wires ghost-bid evaluation into production bidder. Most other analyses (30-day window, net-new cohort, etc.) cannot proceed until this is live.
- TI-917 — combined Loom (v5 findings + power analysis primer). Closed/done.
- TI-919 — Alex Knorr's mid-intent / peak-performance experiment design spike.

**Blocked on bidder-process implementation (do not start in parallel):**
- 30-day window incrementality run — confirmed 2026-05-08, the post-hoc Databricks-on-augmentor-logs path is no longer the plan. Run once ghost-bidding is live.
- Phase 1 (30 net-new advertiser cohort) — cross-cohort generalization check, also waits on ghost-bidding.

**Stakeholders:**
- Engineering: Malachi (analysis foundation), Alex Knorr (likely model lead), bidder team (process implementation)
- Audience platform: Zach Schoenberger + Jordan Piepkow (integration with audience expression eval)
- Bidder team: Chris Rogus (Director of Engineering, primarily over the bidder team; existing technical proposal for ghost-bidder process from ~a month before approval; not yet on bidder roadmap, capacity expected)
- Org structure: First **Ascent team** being spawned around incrementality (Elon mentioned). Mike Dolt + Alex Knorr scoped the ghost-bidder work as part of it. TI-side work folds into the Ascent team. Bidder-team resourcing being negotiated with Paulo.
- Targeting/strategy: Mike Dolt, Alex Bloore (CTV framing), Cara (asked the audience-type breakdown question)
- Leadership: Kale (Director TI), Paulo (VP Eng), Richard (CTO)

**Source review:** 2026-05-04 shareout meeting transcript + actions doc at `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/meetings/ti_837_07_shareout_incrementality_results_2026_05_04.txt` and `ti_837_07_shareout_meeting_actions.md`.

**Open follow-ups from the meeting:**
- Cara asked for audience-type breakdown (Mountain Matched vs interest-based / third-party prospecting). Open whether enough advertisers run interest-only to support the comparison.
- Cara has a LiftLab-produced list of ~200 advertisers stack-ranked by incrementality-test viability (WGU at top by spend); will share for cross-referencing against TI-837 cohort.
- Operating principle stated in the meeting: *"Obviously we cannot present people incrementality that will show that we are not incremental"* — work product is to *get to* incremental, not just measure non-incrementality.
