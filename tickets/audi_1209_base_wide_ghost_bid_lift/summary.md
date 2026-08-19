---
doc_type: ticket
title: "[SPIKE] Ghost-bid incrementality across the advertiser base + refreshed lift-test candidates"
status: done
date: 2026-08-19
summary: "Base-wide ghost-bid lift and a refreshed tiered lift-test candidate list for Imani Clark"
result: "Visit lift +4.7% (CI +4.4/+5.0), conversions +3.3%; Top 81 / Mid 207 / Low 927 of 1,215 eligible. Two method findings limit what the instrument can say."
question: "What is ghost-bid incrementality across the whole advertiser base, and which advertisers can run a credible lift test?"
framing_state: "skip: retroactive — the ask arrived in Slack and was answered in the same session"
---

# [SPIKE] Ghost-bid incrementality across the advertiser base + refreshed lift-test candidates

**Jira:** [AUDI-1209](https://mntn.atlassian.net/browse/AUDI-1209)
**Status:** done
**Date:** 2026-08-19
**Assignee:** Malachi
**Requester:** Imani Clark

---

## 1. The request

Imani Clark, Slack DM, 2026-08-19 14:15:

> desperately searching for an analysis you did a while back of applying our ghost bidder incrementality logic to all of our customer base and what the expected results would have been
>
> am i making that up? or did you spin up an analysis on that?

She was looking for INCR-75. Al Beretta had already sent her the June workbook in another thread. Her follow-ups set the real scope:

> i think im most interested in seeing the prior run
>
> and getting a sense of where we're driving lift in aggregate across our advertiser base
>
> may need to tap you for an updated pull on whatever handful of advertisers we choose for this beta

So: (a) find the prior analysis, (b) aggregate lift across the base, (c) a candidate list for a beta, (d) a per-advertiser pull once the beta cohort is picked — still outstanding.

## 2. What was delivered

Rerun of INCR-75 on current data, rebuilt on the shared `lib/mntn_xlsx` format.

**Workbook:** `My Drive/Tickets/INCR/INCR-75/INCR-75 Incrementality Lift Test Candidates.xlsx` — <https://docs.google.com/spreadsheets/d/1KoatqZq24_5QCCelg3DH15BpPtWv12lb/edit>

| | Answer |
|---|---|
| Pooled visit lift | **+4.66%**, 95% CI [+4.35%, +4.96%], across 1,054 advertisers |
| Pooled conversion lift | **+3.33%**, 95% CI [+1.77%, +4.91%], across 624 advertisers |
| Eligible for a lift test | 1,215 of 1,859 delivering advertisers |
| Tier Top / Mid / Low | 81 / 207 / 927 |
| Where the lift is | Unscored +6.1% · Peak Performance +3.8% · High Intent +2.9% · Mid Intent and Max Reach ~0 (n.s.) |

## 3. Two findings that limit the instrument

Both are written up in full, with the evidence and the discriminating test, in [INCR-75 summary §9](../incr_75_eligible_advertisers/summary.md). Short form:

1. **The measured window cannot be extended past 2026-07-07 (~15 days).** The entry-cohort anchor exhausts the holdout arm — a held-out IP never wins, so it never leaves the bidding pool and anchors immediately, while bid-on IPs churn. Observed holdout share decays 0.105 → 0.084 against a fixed 10% platform holdout and lift inflates to a false +18.6% over the full 58-day table. More data did not buy a longer window.
2. **The recorded audience-band gradient reverses under a correct relative-effect estimator.** "Mid-intent carries the lift, unscored is dead" came from dividing a precision-weighted absolute effect by a precision-weighted base rate; that denominator collapses and inflates exactly the lowest-baseline bands. Re-estimated on the log risk ratio, on the same data, unscored leads and mid-intent is ~0. Both readings are kept in the record.

Also found: `partner_id` 79 (the MNTN Rust bidder leg) entered the lift table the week of 2026-07-05 reading +128% to +290% at a 0.066-0.083 holdout share, and is excluded as unreliable; and `silver.aggregates.agg__daily_sum_by_campaign` no longer exists, so advertiser spend now comes from `summarydata.sum_by_advertiser_by_day`.

## 4. Where the work lives

All analysis, queries, outputs and the workbook builder are in the INCR-75 ticket, not here — this spike records the ask and the answer.

- Analytical record: [`tickets/incr_75_eligible_advertisers/summary.md`](../incr_75_eligible_advertisers/summary.md) §9
- Queries: `incr_75_advertiser_metrics.sql`, `incr_75_entry_cohort_clean.sql`, `incr_75_band_lift_clean.sql`, `incr_75_entry_cohort_byday_window.sql`
- Builder: `artifacts/incr_75_build_workbook.py`; pooling helper `artifacts/incr_75_lift_stats.py`
- Knowledge: `knowledge/experimentation.md` (two new sections), memory `reference_ghost_bid_lift_register` (contradiction appended)

## 5. Open items

1. **Imani:** per-advertiser pull once the beta cohort is chosen. `incr_75_entry_cohort_clean.sql` filtered to their advertiser IDs.
2. **Matt Brorby:** is partner 79's holdout write path wired? And can the entry-cohort anchor be redefined so a window longer than 15 days is measurable?
3. **AUDI-789 WS1:** the "a visit/spend-optimized scorer de-optimizes incrementality" warning rests on the reversed band ordering. Revisit before acting on it.
