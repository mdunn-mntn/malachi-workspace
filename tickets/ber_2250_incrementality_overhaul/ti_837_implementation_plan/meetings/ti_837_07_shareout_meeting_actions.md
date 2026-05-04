# 2026-05-04 — Incrementality results shareout: actions

**Audio + transcript:** [ti_837_07_shareout_incrementality_results_2026_05_04.txt](ti_837_07_shareout_incrementality_results_2026_05_04.txt) (~27 min, 5,372 words)
**Purpose:** present TI-837 v5 + TI-884 power analysis to broader incrementality stakeholders; align on path forward.

## Decisions

1. **Bidder-level ghost bidding approved as the canonical path forward.** Replaces post-hoc augmentor-based methodology. Two-track scope:
   - **Bidder process:** new table (similar to `cost_impression_log`) storing a ghost-bid row per auction. No augmentor TTL constraint. Initial technical proposal exists from Rokas + Zach (written ~a month ago, not on bidder roadmap yet).
   - **Modeling layer:** train a model on incremental visits. Either a toggle (perf vs incrementality) or behind-the-scenes blend. Matt's sketch is the starting point.

2. **First Ascent team being spawned around incrementality.** Mike Dolt + Alex Knorr scoped the ghost-bidder work as part of it. Resourcing negotiating with Paulo. TI side is also rolling into the Ascent team. Open: do we get a bidder-team engineer assigned, or do code reviews suffice (per Alex Knorr).

3. **Power thresholds confirmed publicly.** Visits measurement: ~$200K/month per advertiser (low end of Haus's $10M/year cross-channel rec). Conversion measurement: ~5× that, ~$1M/month. Only ~10 MNTN advertisers spend at that volume.

4. **Combined deliverable:** Malachi will consolidate the v5 deck + power analysis deck into one deck + post in chat with a written summary of decided next steps. (= TI-917 scope.)

## Action items

| Item | Owner | Ticket | Status |
|---|---|---|---|
| Combine v5 + power decks; post summary in chat | Malachi | TI-917 | In progress |
| Loop in Rokas / bidder team on prioritizing the ghost-bidder work | Mike Dolt + Alex Knorr | (Ascent) | Live |
| Confirm bidder-team resourcing | Paulo | — | Negotiating |
| Cross-reference LiftLab's 200-advertiser viability list with TI-837 cohort | Malachi (LiftLab list owner: Cara to share) | (TBD) | Waiting on LiftLab list |
| Investigate interest-based audience vs Mountain Matched comparison feasibility | Open | (TBD) | Question raised by Cara; depends on whether enough advertisers run interest-only |
| Build the bidder-level ghost-bidding model (T-learner) | TBD (Alex Knorr likely) | TI-886 | Pending |

## Open questions surfaced

- **Audience-type breakdown.** Cara asked whether we can split prospecting by audience type (Mountain Matched vs interest-based / third-party). Malachi: unsure if enough advertisers run interest-only to support the comparison. Mike was not on the call to confirm.
- **30-day window value.** Malachi: numbers wouldn't change "much" but would still be slightly different. Phase 2a Databricks work would deliver this.

## Quote highlights

> "Obviously we cannot present people incrementality that will show that we are not incremental." — confirms the work product is to *get to* incremental, not just measure it.

> "Most tests don't really reach power. So a lot of times people are looking at results and have no [framework] — they think things look good and it's fine for kind of understanding which direction you're going to go. But to say this *causes* this amount of lift, very few of our advertisers actually even have that power on a month-to-month basis."

> "We should keep [Rokas + bidder team] in the loop on that though." — bidder-team coordination is the unblock for the ghost-bidder process work.
