# 2026-04-30 — Alex Knorr deck-review meeting: actions

**Audio:** [ti_837_05_team_meeting_2026_04_30.txt](ti_837_05_team_meeting_2026_04_30.txt) (~48 min, full transcript)
**Participants:** Malachi Dunn, Alex Knorr
**Purpose:** 1-on-1 deck review before broader team meeting (Mon 2026-05-04)

## Summary

Alex Knorr walked the v5 4-segment deck end-to-end. Direction agreed:
1. Headline holds: high-intent prospecting is **approximately zero** incremental.
2. Build bidder-level ghost-bidding holdout-impression infra (separate workstream).
3. **Design a focused experiment for peak performance + mid intent tiers** — created [TI-919|https://mntn.atlassian.net/browse/TI-919] spike for Alex K.

## Deck fixes (priority order)

### 1. Slide 13 (selection bias) — REWORD second statement

The v5 deck currently says the bidder "preferentially bids higher on visit-prone IPs." Alex flagged this as not-quite-correct.

**What's actually true (Alex K, 2026-04-30):**
- Bidder does NOT differentiate by retargeting-vs-prospecting at IP level. If an IP is eligible for retargeting on one campaign and prospecting on another, both campaigns get evaluated equivalently.
- Bidder DOES preferentially bid on the **smaller-audience campaign** when an IP qualifies for both. Retargeting audiences are smaller than prospecting → retargeting wins more bids on shared IPs.

**Fix:** rewrite slide 13 second-bullet to "*The bidder gives preference to campaigns with smaller targeting audiences. Retargeting audiences are smaller than prospecting, so retargeting wins disproportionately when an IP is eligible for both.*"

### 2. Stage 1 / "high intent shoppers were going to convert anyway" slide — ADD context

Add Alex Bloore's framing from the prior team meeting: *"the call to action from a CTV ad versus when you're on your phone already and can click right away"* — reinforces why CTV-driven prospecting lift looks low at 7-day window. Long conversion cycle, smaller call-to-action effect on TV.

**Fix:** add one supporting sentence with this framing.

### 3. Wedge slide — REFRAME takeaway

Currently informational. Alex's reframe: this slide is about **modeling decisions**, not just attribution observation.

**Fix:** change takeaway to "*guid_log is the right label to use for incrementality modeling, because clickpass over-credits attributed visits in prospecting. The wedge tells us why we trust guid_log over clickpass for the targeting model's source of truth.*"

### 4. Lift profile chart (high vs peak vs mid) — DROP or ADD sample-size overlay

Currently shows ATT by tier. Alex: "*this raises more questions than answers*" — mid intent is noise floor, but the chart doesn't communicate that.

**Fix options (pick one):**
- A. **Drop the slide entirely.** Mention briefly in narration; defer detail to spike [TI-919|https://mntn.atlassian.net/browse/TI-919].
- B. **Add a sample-size overlay** (secondary axis or dot size) so audience can see why mid-intent values are statistically meaningless. Per Alex: "*overlay both pieces — the ATT and the size of the groups you're evaluating to get that number.*"

Recommended: B if there's time (clear and self-explanatory); A if not.

### 5. Methodology fixes vs prior internal numbers slide — REMOVE

Alex: "*you can take the slide out — I don't think we need to throw the methodology in their mixed segment treatment denominator.*" Methodology cleanup belongs in the verbal preamble, not as a slide.

**Fix:** remove the slide. Mention briefly upfront that earlier internal numbers used a mixed-segment denominator, fixed in v5.

### 6. Cohort design slide — ADD caveat

The "tier diversity ≥5%" gate is on **IP-level prospecting score**, but the actual campaign budget might serve into a different tier than the IP-level score would suggest. Alex flagged this as a confounder we should call out.

**Fix:** add caveat: "*Tier diversity gate is on prospecting-score distribution, not on what the campaign actually served. Some advertisers in the cohort may have IPs that score peak/mid but never received an impression at that tier because campaign budget concentrated at high intent.*"

### 7. Stage 1 zero — EMPHASIZE "approximately zero, sample size limited"

Alex: "*none of these really met power anyways — these ratios are kind of invalid. The point of this is to show they're not that incremental — it's almost nothing.*"

**Fix:** soften the negative-pp callout (e.g., -0.06pp) to "approximately zero" framing. Don't litigate the specific magnitude when CI half-widths exceed the values.

## Action items

| Item | Owner | Ticket | Status |
|---|---|---|---|
| Send Slack to targeting squad asking about expression_type=1 (older audiences) holdout behavior | Malachi | — | Not started |
| Apply deck fixes #1-7 above | Malachi | [TI-842](https://mntn.atlassian.net/browse/TI-842) | Not started |
| Design peak/mid experiment for Kirsa meeting | Alex Knorr | [TI-919](https://mntn.atlassian.net/browse/TI-919) | Not started — spike just created |
| Rerun analysis on 30-day window via Databricks | Malachi | — (next session) | Spark port works; need to scale cluster + run |
| Re-meet with Alex tomorrow morning to walk through math/SQL | Both | — | Scheduled informally |

## Open methodological notes

- **Holdout on retargeting confirmed.** Alex initially thought there was no holdout on CRM retargeting; Malachi confirmed there IS a 10% holdout on retargeting (verified via `audience_segments.expression`). Alex agreed. Slack to Zach Schoenberger pending: clarify implication of older expression_type=1 audiences which appeared not to have holdout JSON.
- **iROAS predictions are out of scope** for this analysis. Alex agreed — iROAS modelling needs a per-advertiser model trained on counterfactual + uplift, separate workstream.
- **Showing numbers that don't meet power** to advertisers — Alex was OK with it as long as we caveat clearly that "this isn't us — sample size constraints in the data we have."

## Quote highlights

> "the difference between a good statistician and a bad statistician is a good statistician knows all the assumptions" — Alex K, 2026-04-30

> "BigQuery is really weird. It doesn't scale compute very well. I hate BigQuery." — Alex K, on why we want Databricks

> "Job compute clusters in Databricks cost a quarter of the cost of your interactive cluster. So even if it needs to run for like 13 hours, you can get answers for cheap." — Malachi, sharing what Victor taught about job compute
