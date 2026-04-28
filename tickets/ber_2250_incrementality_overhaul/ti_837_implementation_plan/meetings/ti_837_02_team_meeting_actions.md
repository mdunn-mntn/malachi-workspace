# Team meeting — Incrementality program review (2026-04-28)

Source: [ti_837_02_team_meeting_2026_04_28.txt](ti_837_02_team_meeting_2026_04_28.txt)
(31 min audio, 452 lines, 5,767 words; OpenAI/local merged)

**Attendees (inferred):** Bryce (PM, facilitator), Alex Bloore (VP TPMs,
strategic driver), Alex Knorr (TI methodology), Malachi, Edgar (3rd-party
attribution), Mike, Megan, Matt, possibly others.

## Headline pivot

The original "end-of-month incrementality experiment" plan **no longer
requires a formal media experiment.** Alex Bloore confirmed the
ghost-bidding ATT methodology + 10% holdout we already have is enough to
answer the immediate strategic question internally. The 4/30 deadline
becomes a *deeper-dive review* of the offline analysis, not the launch
of a new media test.

## Alex Bloore's strategic question — the actual ask

> "Our current default targeting methodology prioritizes high intent. High
> intent, almost by definition, is not going to be incremental. … The
> easiest way to test that hypothesis would just be: let's move down our
> intent tiers and see if that affects things — how does that play out if
> we target more of mid-intent as opposed to high intent? Can we use what
> we know CTV is good at, which is sort of priming customers for
> lower-funnel conversions down the road that might be assisted by a Meta
> or a Google or something else?"

The hypothesis: **mid-intent IPs may produce more incremental lift than
high-intent IPs**, because high-intent shoppers were going to convert
anyway. CTV's role is funnel-priming, not closing.

The deliverable: enough data to inform whether MNTN should adjust scoring
to prioritize incrementality (when an advertiser asks for it) — not just
intent probability.

**This goes into the Mountain-Match AI roadmap for Q2-Q3.** It's not just
about TI-837 — it's a broader scoring-strategy question.

## What our current results say (preview, before final review)

- **High intent IS incremental** in our 30-advertiser cohort. Contradicts
  the "high intent isn't incremental" prior. Lift is real and broad-based
  (93% of advertisers positive).
- **Mid intent** has very little signal — both at noise-floor in current
  data. Hard to say from this cohort because most advertisers don't
  target mid-intent, so volumes are tiny and we couldn't power the test.
- **Cohort composition matters.** Most MNTN advertisers target high-intent
  only. Getting good mid-intent samples requires picking advertisers that
  span tiers — which is itself a non-random sample.

## Bryce's selection-bias question — important caveat

> "Some selection bias potentially. If there's a selection bias there
> because maybe the reason they have a low threshold is because their
> high intent is really small, and smaller high-intent audiences tend to
> be more or less incremental."

Our 30-advertiser cohort was filtered for tier-diversity (advertisers
whose IPs span multiple intent tiers). These may be systematically
different from "typical" MNTN advertisers. Add this caveat prominently
to the deck. The lift number is about *this cohort* — extrapolation to
"all MNTN advertisers" requires care.

## Methodology status

- **ITT (intent-to-treat)** showed flat lift because 80% of "treated"
  IPs weren't actually served. Confirmed dead.
- **Ghost-bidding ATT** — current methodology. Working post-hoc by
  scanning augmentor + cost_impression.
- **Win-rate correction** (Alex Knorr flagged 2026-04-28 morning) — fix
  is implemented; v2 lift run currently in BQ (~75 min in as of writing).
- **Augmentor 10-day TTL** is the binding constraint for replication
  beyond a few days back. Production version of ghost-bidding belongs at
  the bidder layer, not as a post-hoc query.

## Action items (in priority order)

| Priority | Action | Owner | Deadline |
|---|---|---|---|
| P0 | Finish v2 win-rate-corrected lift run + write up corrected numbers | Malachi | 2026-04-30 |
| P0 | Schedule deeper-dive review meeting | Bryce | this week / early next |
| P0 | Add selection-bias caveat to deck | Malachi | before review |
| P1 | Bidder-level ghost-bidding implementation handoff | Alex Knorr / Zach + Jordan (bidder team) | Q2 — already in motion |
| P1 | Watch LiftLab methodology video (geo-split, US halved into matched DMAs) | Alex Bloore + Malachi | this week |
| P1 | Watch Houzz methodology recording (Edgar to send) | Alex Bloore | this week |
| P1 | Set up weekly TI-incrementality check-in cadence | Bryce | start next week |
| P2 | Uplift modeling — Matt has PRD draft | Matt + future | Q2/Q3 roadmap |
| P2 | Per-advertiser custom incrementality models for clients who ask | future | Q3 |
| P3 | Re-do analysis with Fangorn scores (when available) | future | when scores swap in |

## Things mentioned but not committed to

- **Houzz** ↔ MNTN integration: most-used 3rd-party but not the chosen
  partner.
- **Ownerly experiment in progress:** high-intent vs not-high-intent ×
  mountain-matched vs interest-based, run via Houzz. Provides external
  validation data point.
- **Alex Bloore wants to "poke at the results"** before turning them into
  a learning agenda for the next 6 weeks. The 4/30 deliverable is one
  data point in a longer thread.
- **Broader incrementality program ownership** — Bryce + Kyla + Howard
  to figure out cross-team coordination (Jason's integration work, Al's
  reporting work, Megan's UI experiment-setup work, etc.).

## Updates needed in MNTN context

Save to `knowledge/mntn_business.md`:
- Strategic framing: high-intent → likely-to-convert-anyway → low
  incrementality; mid-intent is the "movable middle" hypothesis
- CTV's role per Alex Bloore: priming for lower-funnel conversions
  assisted by other channels (Meta/Google), not last-touch closer
- Mountain-Match AI Q2/Q3 roadmap: scoring may evolve from
  intent-probability → incremental-lift-probability for advertisers who
  opt in
- Alex Bloore's stance on third-party measurement: not used internally
  to drive decisions; we measure ourselves
