---
doc_type: ticket
title: "[SPIKE] Advertisers spending with no measurable site visits"
status: done
date: 2026-08-19
summary: "Share of site visits by advertiser: how much of their own site traffic MNTN touched, and who falls short of similar accounts"
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

**Third cut (current).** Johnny again: compare MNTN visits to the advertiser's total site visits, a share of site visits, because a low match rate reflects campaign audience against site size rather than measurement. He showed it with a model account: **Maurices (66784) matches 3.15% of served IPs but reaches only 0.26% of its site traffic, while Re-Bath Cherry Hill (39510) matches 0.13% and reaches 0.29%.** The account with the far worse match rate reaches a larger share of its audience.

### Share of site visits shrinks with site size, so peers are matched on it

Correlation of log site visits to log share of site visits = **-0.24**. Medians by site-size quintile:

| Site size group | Median site visits | Median share of site visits |
|---|---|---|
| Smallest fifth | 9,269 | 1.09% |
| Second fifth | 59,683 | 0.91% |
| Middle fifth | 218,520 | 0.77% |
| Fourth fifth | 648,346 | 0.78% |
| Largest fifth | 2,565,052 | 0.39% |

Ranking on raw share of site visits selects large sites and nothing else: an unadjusted bottom-quartile cut flagged ElevenLabs, Buckle, Apollo.io, EcoATM and Owala purely for having huge sites. Within-quintile ranking drops them.

### The current answer

Of 1,859 live advertisers that served in the trailing 30 days: **1,649 scorable · 171 sites too quiet to score (under 1,000 visits) · 39 reporting no visits at all.**

**25 advertisers spent $10,000 or more and sit in the bottom quartile of share of site visits against size-matched peers.** Largest: ElevenLabs ($939k, 0.025%), Policygenius ($76k, 0.336%), Benlysta ($49k, 0.370%), Metal Supermarkets ($35k, 0.348%), MegaFood ($32k, 0.236%). Lowest against peers: Front (8th percentile), Nili Lotan (12th), MegaFood (17th).

The 39 reporting nothing at all remain the clearest setup question, though they are small: $82,479 of spend between them.

## 3. Reading

A visit exists only when the advertiser's own site pixel fires and writes a `clickpass_log` row keyed to their advertiser id. That makes matched visits a strict subset of reported visits, which is why the raw number has to sit beside the matched one.

A low share of site visits against size-matched peers can come from campaign configuration, audience quality, flight length or budget, exactly as Johnny said. It says the account is worth opening, not that anything is broken.

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

Sheets: the flagged accounts, the advertisers reporting nothing, the full audit trail, a Read me, method notes, and the one standalone query. Two argument tabs (the Maurices comparison, the size-group table) were cut on 2026-08-19 as unnecessary; both points live in Method & caveats as a sentence each.

**Column naming (2026-08-19):** `Rank vs peers` → **Similar sites we beat** (the percentile read forward: 23% means we reach a bigger slice than 23 of every 100 comparable advertisers), `Site size group` → **Compared against**, labelled with the actual traffic band (Under 25K · 25K to 120K · 120K to 350K · 350K to 1.4M · Over 1.4M) rather than "fourth fifth", so the peer group explains itself, `Reading` → **Tracking history**. Renamed after review: the originals did not say what they were for without opening the Read me.

**Metric naming:** the metric was called "share of voice" through the afternoon, borrowed from Johnny. Renamed to **share of site visits** because share of voice normally means impression share against competitors, and this is a credit ratio. The file name is unchanged on purpose, so the link already circulating with Johnny and Imani still resolves.

- Query: `queries/audi_1210_share_of_site_visits.sql` (runs standalone; returns the whole base with both ratios and both percentiles)
- Builder: `artifacts/audi_1210_build_xlsx.py`

## 4b. Imani's opt-out hypothesis — tested, does not hold

Imani Clark (Slack, 2026-08-19): the dark advertisers are "very likely advertisers that opted out of a tracking pixel," and there is a field in the UI to select it. Checkable directly — `integrationprod.advertisers` carries `conv_pixel_opt_out`, `tracking_pixel_status_id` and `conversion_pixel_status_id`.

| Group | n | `conv_pixel_opt_out` TRUE | `tracking_pixel_status_id` |
|---|---|---|---|
| Pixel reported nothing | 39 | 1 (2.6%) | 38 are 10, 1 is 11 |
| Site too quiet to score | 171 | 7 (4.1%) | 170 are 10, 1 is 11 |
| Flagged vs peers, $10k+ | 25 | 1 (4.0%) | all 25 are 10 |
| All live advertisers | 1,859 | 64 (3.4%) | 1,848 are 10, 8 are 9, 3 are 11 |

**Opt-out is no more common among the dark advertisers than in the base** (2.6% against 3.4%), so it does not explain them. And `tracking_pixel_status_id` = 10 for 38 of the 39, the same status essentially every live advertiser carries. Across the whole table status 9 is the bulk (33,340 rows against 4,354 at status 10), while live-serving advertisers are almost all 10, so 10 reads as the active state. These advertisers are marked as tracking normally and reporting nothing.

**Stronger test: did they ever track? (2026-08-19)** An opt-out produces an advertiser that NEVER reported a visit. A defect produces one that reported visits and stopped. Over the trailing 12 months (`sum_by_advertiser_by_day`), of the 39: **6 never tracked at all · 33 tracked and stopped.** So opt-out cannot be the general explanation.

But the volumes gut the alarm. Only **Dura Guard Roofing** is substantive: 7,338 visits over 12 months, last visit 2026-04-28. The other 32 recorded between 1 and 151 visits across the whole year, so their "stop date" is indistinguishable from a quiet site that happened to log nothing recently. Most last-visit dates cluster Apr-Jul 2026.

**Net:** one clear breakage worth chasing (Dura Guard), six plausible opt-outs or never-installed, and the rest too small to call either way. The 25 peer-flagged advertisers on the main sheet remain the higher-value list.

**Imani's follow-up: is the field even valid? (2026-08-19)** She suspected `conv_pixel_opt_out` is a net-new column that was never backfilled, so an advertiser that launched earlier and opted out would still read FALSE. **She is right about the field.** Share TRUE by advertiser creation year: 0.00% for every year 2010-2021 · 0.07% 2022 · 0.09% 2023 · 0.24% 2024 · **0.80% 2025** · 0.39% 2026. The column carries no signal before roughly 2024.

**But it does not change the answer for this group,** because these advertisers are all recent: of the 38 reporting nothing, **29 were created in 2026, 6 in 2025, 3 in 2024** — entirely inside the populated era. Within their own cohort the flag is set on 1 of 38 (2.6%) against roughly 0.5% for advertisers created 2025-2026, so it is somewhat enriched but still accounts for a single advertiser.

**And the load-bearing test never used that field.** 33 of the 38 reported visits and then stopped. An opt-out at launch cannot produce a stop date. That result stands independent of the column's history.

**Caveat before this is quoted back to Imani:** `conv_pixel_opt_out` is the CONVERSION pixel. The UI control she describes may be a separate visit-tracking setting that this table does not carry, and `pixel_id` is NULL for every row here so it cannot be used as a proxy. Johnny's team has the logs to settle it.

## 5. Open items

1. **Pixel ops to confirm the cause.** Ashley Pineda Varela owns `conversion_log` / pixel-firing routing per Zach Schoenberger (2026-05-06); Johnny is the immediate check. Start with WGU 67978 vs Western Governors University 31357.
2. If it is a pixel defect at this scale, the fix returns a quarter of the live base to incrementality measurement.
3. Decide whether a standing monitor is worth it, so a newly-dark pixel is caught in days rather than at the next screen.
