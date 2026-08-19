---
doc_type: ticket
title: "[SPIKE] Advertisers spending with no measurable site visits"
status: in_progress
date: 2026-08-19
summary: "Share of voice by advertiser: how much of their own site traffic MNTN touched, and who falls short of similar accounts"
result: "25 advertisers spending $10k+ reach less of their site audience than three quarters of size-matched peers. Only 39 report no visits at all."
question: "Which advertisers are we failing to reach, once site traffic and site size are accounted for?"
framing_state: "skip: diagnostic list, the deliverable is the list itself"
---

# [SPIKE] Advertisers spending with no measurable site visits

**Jira:** [AUDI-1210](https://mntn.atlassian.net/browse/AUDI-1210)
**Status:** in_progress
**Date:** 2026-08-19
**Assignee:** Malachi

---

## 1. Where this came from

Surfaced during the AUDI-1209 rerun. The lift-test screen drops any advertiser without a measurable visit rate, and that filter removed **479 of 1,859** delivering advertisers — a quarter of the live base. Malachi flagged the size of it: several were recognisable brands spending real money. Worth its own look rather than a footnote in a screening funnel.

## 2. What was found

**The list was recut twice, and both recuts came from Johnny Chen. He was right both times.**

**First cut (wrong).** Ranked on visit rate alone: 542 advertisers under 0.5%, led by Real Techniques, Food Lion and Valvoline, framed as likely pixel defects.

**Second cut.** Johnny: attributed visits are a subset of the advertiser's own reported visits, so a zero match on a quiet site means nothing. Adding `raw_visits` from `summarydata.sum_by_advertiser_by_day` reclassified **171** advertisers as quiet sites, including all three named above. Their own pixels report 55, 20 and 70 visits in 30 days.

**Third cut (current).** Johnny again: compare MNTN visits to the advertiser's total site visits, a share of voice, because a low match rate reflects campaign audience against site size rather than measurement. He showed it with a model account: **Maurices (66784) matches 3.15% of served IPs but reaches only 0.26% of its site traffic, while Re-Bath Cherry Hill (39510) matches 0.13% and reaches 0.29%.** The account with the far worse match rate reaches a larger share of its audience.

### Share of voice shrinks with site size, so peers are matched on it

Correlation of log site visits to log share of voice = **-0.24**. Medians by site-size quintile:

| Site size group | Median site visits | Median share of voice |
|---|---|---|
| Smallest fifth | 9,269 | 1.09% |
| Second fifth | 59,683 | 0.91% |
| Middle fifth | 218,520 | 0.77% |
| Fourth fifth | 648,346 | 0.78% |
| Largest fifth | 2,565,052 | 0.39% |

Ranking on raw share of voice selects large sites and nothing else: an unadjusted bottom-quartile cut flagged ElevenLabs, Buckle, Apollo.io, EcoATM and Owala purely for having huge sites. Within-quintile ranking drops them.

### The current answer

Of 1,859 live advertisers that served in the trailing 30 days: **1,649 scorable · 171 sites too quiet to score (under 1,000 visits) · 39 reporting no visits at all.**

**25 advertisers spent $10,000 or more and sit in the bottom quartile of share of voice against size-matched peers.** Largest: ElevenLabs ($939k, 0.025%), Policygenius ($76k, 0.336%), Benlysta ($49k, 0.370%), Metal Supermarkets ($35k, 0.348%), MegaFood ($32k, 0.236%). Lowest against peers: Front (8th percentile), Nili Lotan (12th), MegaFood (17th).

The 39 reporting nothing at all remain the clearest setup question, though they are small: $82,479 of spend between them.

## 3. Reading

A visit exists only when the advertiser's own site pixel fires and writes a `clickpass_log` row keyed to their advertiser id. That makes matched visits a strict subset of reported visits, which is why the raw number has to sit beside the matched one.

A low share of voice against size-matched peers can come from campaign configuration, audience quality, flight length or budget, exactly as Johnny said. It says the account is worth opening, not that anything is broken.

**Why it matters beyond reporting:** an advertiser with no measurable visit rate cannot be screened for an incrementality lift test and cannot be shown a result. This was the largest single cut in the AUDI-1209 screening funnel, at 479 of 1,859.

**The definition, settled empirically (2026-08-19).** Johnny read Re-Bath Cherry Hill at 1.25%, this file read 0.29%. Rather than ask, both candidate numerators were computed on that one advertiser against the same denominator (221,337 reported site visits):

| Numerator | 39510 | 66784 |
|---|---|---|
| Distinct matched IPs | 0.291% | 0.260% |
| Matched clickpass rows | 0.336% | 0.315% |
| All clickpass rows | 0.675% | 0.417% |
| **Verified visits** (clicks + views + competing_views) | **1.269%** | **0.404%** |

Verified visits reproduces Johnny's 1.25% and 0.5%. It is the client-facing Reporting figure, so the workbook now uses it. Distinct matched IPs undercounts on two grounds: a household visiting several times counts once, and the IP-level join misses cross-device.

**This changes the reading of his example.** On the correct definition Re-Bath Cherry Hill sits at the **50th percentile** of its size peers, and Maurices at the 42nd. Both are ordinary. That was his point.

## 4. Deliverable

`My Drive/Tickets/AUDI-1210/AUDI-1210 Advertisers With No Measurable Visits.xlsx` — <https://docs.google.com/spreadsheets/d/1KpOHoI2yB0cbF6_pxIL5QyNCojsTh2sp/edit>

Sheets: the 26 flagged accounts, the Maurices vs Re-Bath comparison that reframed the list, share of voice by site size, the 39 reporting nothing, the full 1,859, a Read me, method notes, and the standalone SQL.

- Query: `queries/audi_1210_share_of_voice.sql` (runs standalone; returns the whole base with both ratios and both percentiles)
- Builder: `artifacts/audi_1210_build_xlsx.py`

## 5. Open items

1. **Pixel ops to confirm the cause.** Ashley Pineda Varela owns `conversion_log` / pixel-firing routing per Zach Schoenberger (2026-05-06); Johnny is the immediate check. Start with WGU 67978 vs Western Governors University 31357.
2. If it is a pixel defect at this scale, the fix returns a quarter of the live base to incrementality measurement.
3. Decide whether a standing monitor is worth it, so a newly-dark pixel is caught in days rather than at the next screen.
