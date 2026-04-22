# TI-896: Audience composition shift analysis — 2025 performance drop war room

**Jira:** https://mntn.atlassian.net/browse/TI-896
**Status:** In Progress
**Date Started:** 2026-04-22
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

CTO-led revenue war room (Slack channel `C0ATVHK2EDV`, kicked off by Richard 2026-04-21). Conversions and order-value metrics dropped materially in late 2025; top-of-funnel metrics (CPV, CPA, visits, CPMs) stayed healthy. Pixel opt-out has been ruled out as the driver (0.4% of active accounts). Customer-mix shift toward SMB/lead-gen has been identified as one contributor (Dave, 2026-04-21 09:51), but doesn't explain the full delta.

Alex Bloore ([BER-2250](https://mntn.atlassian.net/browse/BER-2250) lead) asked for an analysis of **share of audience types (MM / 3P / CRM / Interest / RTC Keywords) over time** to detect distribution shifts during the measurement period. Jason Huertas offered; Malachi picked up the audience-composition workstream. Scope is strictly the audience angle — Ray's team is handling reporting / pixel / attribution; Will Cavey is handling customer-profile cuts.

## 2. The Problem

**Symptom:** Conversions and order values dropped materially starting roughly **Nov 2025**. ROAS dropped as a consequence. CPV / CPA / visits / CPMs unaffected. Site visits stayed the same.

**Reported by:** Richard (CTO) kicked off the war room 2026-04-21 07:44 PT.

**Impact:** Revenue trajectory. Customer retention risk (advertisers churning as ROAS erodes; CPM cut in March 2026 as a mitigation lever didn't produce expected recovery).

**Temporally correlated events (candidate root causes):**
- **Peak Performance tier launched** early October 2025 (Mike Dolt, 2026-04-22 12:52 Slack) — new audience tier that could have shifted mix toward lower-quality inventory. Scoring bug existed at launch, fixed end of October.
- **Max Reach scoring turned OFF** on **2025-11-19** (Ryan Kleck, confirmed in 2026-04-22 meeting transcript). Previously scored with random numbers, then went unscored. Ryan confirmed the team only looked at pacing/visits impact at the time — **nobody checked the conversion-rate impact of turning max-reach scoring off**. This is a prime candidate for the audience-mix analysis to surface.
- Google / GCP migration September 2025 (predates drop).

**Question we answer:** Did the mix of audience types used by 2025-active advertisers shift materially in the measurement window, and if so — in which direction, how fast, and which customer cohorts?

## 3. Plan of Action

### Scope (from 2026-04-22 meeting + Slack)

**Primary cut — audience-type mix over time:**
Weekly share of each `data_source_id` across active advertisers:
| DS | Type label |
|---|---|
| 2 | MM (MNTN first-party / OPM) |
| 3 | 3P (LiveRamp) |
| 4 | CRM |
| 13 | Interest / Peak Performance (Jaguar) |
| 19 | RTC Keywords |

**Secondary cut — default vs custom audiences:**
% of advertisers using MNTN-provided default audiences vs advertiser-built audiences. (Malachi flagged in meeting; Alex Knorr confirmed in Slack — "good idea, yeah.") Detection logic TBD — likely: default audiences either (a) have a well-known template `audience_id` / name pattern, or (b) have `user_id` set to a system/service account rather than an advertiser user.

**Tertiary cut — retargeting campaign share over time** (Alex Knorr 2026-04-22 Slack):
> "if advertisers are setting up fewer Retargeting type campaigns that could explain conversion drops possibly"

Use `campaigns.objective_id = 4` (retargeting) vs `campaigns.objective_id IN (1, 5, 6)` (prospecting). Known gotcha: `objective_id` is unreliable as a stage indicator post-2025 TV migration — `funnel_level` is more authoritative. Report both, note the caveat.

### Steps

1. **Resolve archive-table location (BLOCKER).** Does `archives.audiences_archives` exist in BQ, or must we query Greenplum before 2026-04-30 deprecation? Check `bronze.integrationprod` for mirrors first.
2. **Cohort:** every advertiser with any campaign spend in 2025 (Will Cavey's shared sheet is the anchor; fall back to BQ pull if sheet not accessible).
3. **Classifier:** parse expression JSON → tag by `data_source_id` set + default-vs-custom flag.
4. **Time reconstruction:** per `(advertiser_id, audience_id)`, sequence archive rows by `update_time` → effective-window timeline.
5. **Weekly rollup** — Nov 2024 → Apr 2026 (18-month lookback per Richard's standing directive).
6. **Cuts:** overall cohort, spend-tier quartiles, vertical (only if time allows).
7. **Annotate charts** with Nov 19 2025 max-reach-scoring-off and early-Oct 2025 Peak Performance launch events.
8. **Deliverable:** `presentation.md` (three-act, Power Line, Tufte charts) + RevealJS deck + githack share link. End-of-day 2026-04-22 initial status post to war room; full deck by EOD 2026-04-23.

### Out of scope

- Conversion / ROAS performance correlation (Ray's team)
- Pixel / attribution (Ray's team)
- Customer profile (Will Cavey)
- Causal attribution of *why* mix shifted (requires product / UX change log review)

## 4. Investigation & Findings

### Archive tables (resolved)
- `dw-main-bronze.integrationprod.archives_audience_segment_archives` confirmed in BQ. 4.1M rows, 279K distinct campaigns, data range 2023-04-20 → 2026-04-22. 3.66M rows have `expression_type_id = 2`.
- `dw-main-bronze.integrationprod.archives_audiences_archives` also in BQ (template level), 146K rows back to 2011. Has `user_id` and `name` — useful for default-vs-custom work.
- **No Greenplum dependency** — analysis can live entirely in BQ.

### Canonical DS mapping (critical correction)
Memory / `data_knowledge.md` had DS3 = LiveRamp 3P — **this is wrong.** The canonical `dw-main-bronze.integrationprod.data_sources` dim says:
- DS2 = MNTN First Party
- **DS3 = MNTN Third Party** (not LiveRamp)
- DS4 = CRM
- **DS11 = LiveRamp** (not DS3)
- DS13 = MNTN Vertical Categorization (**= Peak Performance in war-room language, per Bryce**)
- DS19 = MNTN Matched (**= Keywords in war-room language, per Bryce**)
- **DS35 = LiveRamp IP** (this is what Bryce listed as "3P" in his scope)

Per-advertiser audiences also get their own DS ids (1000+ range) with names like "{AID} - First Party Audience" / "Third Party Audience" / "Control Group Audience" / "Extension Audience". Meaningful for any MM/3P breakdown that needs to include custom advertiser-built audiences.

→ **Logged in [../../knowledge/data_knowledge.md](../../knowledge/data_knowledge.md) gotchas list on commit.**

### Bryce's canonical 5 buckets (war-room scope)
Per Bryce's 2026-04-22 13:25 post:
| Bucket | DS id |
|---|---|
| Keywords | DS19 |
| Peak Performance | DS13 |
| 3P | DS35 |
| CRM | DS4 |
| Mountain Matched (MM) | (interpret as) DS2 + `% - First Party Audience` |

### Primary finding — Peak Performance adoption tripled
Cohort-level share of 2025-active advertisers using at least one Peak Performance (DS13) audience:

| Week | PP share |
|---|---|
| 2025-09-22 | 9.6% (flat line for ~9 months prior) |
| 2025-09-29 | 12.7% |
| **2025-10-06** | **15.7%** ← Peak Performance launch week |
| 2025-10-20 | 19.3% |
| 2025-11-03 | 21.2% |
| **2025-11-17** | **23.1%** ← Max Reach scoring turned off Nov 19 |
| 2025-12-01 | 24.1% |
| 2025-12-29 | 24.9% |
| 2026-04-20 | 30% |

**Other buckets in the Sep–Dec 2025 drop window:** MM 100% → 98% (nearly flat); Keywords 70% → 71% (flat); 3P 56% → 57% (flat); CRM 25% → 25% (flat). Retargeting share stable at ~25%.

Peak Performance is the **only** cohort-level composition shift above noise.

### Max Reach off (Nov 19) shows no composition signal
The Peak Performance ramp continued smoothly through Nov 19. Max Reach turn-off may have affected conversion rates (Ryan confirmed the team only looked at pacing impact at the time) but did not shift *who advertisers targeted* at the cohort level.

### Peak Performance scoring bug ruled out
Scoring bug existed at PP launch (early Oct 2025), fixed end of Oct. Adoption ramp continued well past the fix — the composition signal is post-fix, not an artifact of random scoring.

### Artifacts
- Query: [queries/ti_896_composition_by_week.sql](queries/ti_896_composition_by_week.sql)
- CSV: [outputs/ti_896_composition_by_week.csv](outputs/ti_896_composition_by_week.csv) (77 weeks)
- Charts: [artifacts/ti_896_chart_*.png](artifacts/) (4 PNGs, 200 DPI, Tufte-aligned)
- Chart generator: [artifacts/generate_charts.py](artifacts/generate_charts.py)
- Deck: [artifacts/ti_896_deck_standalone.html](artifacts/ti_896_deck_standalone.html)
- Deck URL: https://gist.githack.com/mdunn-mntn/f47a6f106ed5ff502cedcb7de50231d8/raw/ti_896_deck_standalone.html
- War-room context: [meetings/ti_896_war_room_shared_charts_2026_04_22.md](meetings/ti_896_war_room_shared_charts_2026_04_22.md)

## 5. Solution

*Pending.*

## 6. Questions Answered

- **Q:** Who is the cohort? **A:** Every advertiser with any active campaign from 2025-01-01 onward (even if first campaign launched in March). Confirmed in meeting transcript.
- **Q:** Which table preserves historical audience expressions? **A:** `archives.audiences_archives` (CoreDB). Ryan confirmed in meeting. BQ mirror existence TBD.
- **Q:** Scope limit? **A:** Audience composition only. Ray's team owns reporting/pixel/attribution; Will Cavey owns customer profile.

## 7. Data Documentation Updates

*To be filled at ticket close.*

## 8. Open Items / Follow-ups

- [ ] Confirm BQ mirror for `archives.audiences_archives`
- [ ] Confirm DS id for "third party" — meeting transcript may have said DS35 (possible mistranscription of DS3). Verify in schema.
- [ ] Cross-check Peak Performance tier vs `data_source_id=13` — whether it's a new DS id or a sub-tier of DS13 (Jaguar intent)
- [ ] After primary analysis: does the timeline show a regime change at Nov 19 2025 when max-reach scoring was turned off?
- [ ] Hand-off of remaining scores-based work (35-day TTL means most historical scores are gone)

## Links

- **Slack war room:** `C0ATVHK2EDV` — raw scrape in [knowledge/slack_raw/2026-04-22_c0atvhk2edv_war_room_60d.json](../../knowledge/slack_raw/2026-04-22_c0atvhk2edv_war_room_60d.json), distilled context in [meetings/ti_896_slack_war_room_context_60d_2026_04_22.md](meetings/ti_896_slack_war_room_context_60d_2026_04_22.md)
- **Meeting transcript (2026-04-22):** [meetings/ti_896_01_war_room_audience_analysis_2026_04_22.txt](meetings/ti_896_01_war_room_audience_analysis_2026_04_22.txt)
- **Action items doc (war room):** https://docs.google.com/document/d/1zxLvBjd1EldNyKE1DHTGpbLHQIxFv9DPK11UjQ_eWvE/edit
- **Will Cavey's AID list:** https://docs.google.com/spreadsheets/d/1ghdvbDla2uvG5iAaGBPjVI3Y3_N7FUiY_Io5bSAaIVs/edit
- **Yesterday's war-room recording:** https://mountain.zoom.us/rec/share/7_tft7DC_6k7iMZeIIdTzyIher7AjpRhXK6VxjbzZMxLzbKyhque2NvSlGRwQEMf.bw1qjxAt8kBbr2cQ?startTime=1776787917000 (Passcode: 6TUj2A+E)
- **Canvas:** F0AUCJM4NBC (mntn.enterprise.slack.com)
- **Related:** [BER-2250](https://mntn.atlassian.net/browse/BER-2250)
