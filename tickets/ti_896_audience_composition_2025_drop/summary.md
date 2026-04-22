# TI-896: Audience composition shift analysis — 2025 performance drop war room

**Jira:** https://mntn.atlassian.net/browse/TI-896
**Status:** Complete (Phase 1 + follow-up Tracks A/B/C shipped)
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
- Charts: [artifacts/ti_896_chart_*.png](artifacts/) (5 PNGs, 200 DPI, Tufte-aligned)
- Chart generator: [artifacts/generate_charts.py](artifacts/generate_charts.py)
- Deck: [artifacts/ti_896_deck_standalone.html](artifacts/ti_896_deck_standalone.html)
- Deck URL (current): https://gist.githack.com/mdunn-mntn/2e8ac10861643c9f979a8340efaefb1b/raw/ti_896_deck_standalone.html
- War-room context: [meetings/ti_896_war_room_shared_charts_2026_04_22.md](meetings/ti_896_war_room_shared_charts_2026_04_22.md)
- Verification bundle: [artifacts/ti_896_verification.md](artifacts/ti_896_verification.md)

### Track A — Spend-weighted composition view (2026-04-22)

To address the "counting attached-but-not-delivered" critique, joined archive effective windows to `sum_by_campaign_by_day` and weighted by `media_cost`.

**Result:** PP spend-weighted share reached **~12–13% of cohort spend**, vs the 21% advertiser-presence number. ~8pp gap = PP adopters skew smaller than the cohort average. MM spend share dropped 75% → 38% over 18 months (noteworthy for follow-up — bigger shift in spend than advertiser-presence indicated).

Coverage reconciliation: archive-joined total spend Oct-Dec 2025 matches cohort total ($48M over 14 weeks) — no material missing spend.

Query: [queries/ti_896_composition_spend_weighted.sql](queries/ti_896_composition_spend_weighted.sql)
CSV: [outputs/ti_896_composition_spend_weighted.csv](outputs/ti_896_composition_spend_weighted.csv)
Chart: [artifacts/ti_896_chart_05_pp_spend_share.png](artifacts/ti_896_chart_05_pp_spend_share.png)

### Track B — Default vs custom Peak Performance audiences (2026-04-22)

Three heuristics tested (discovery on 1,000 PP-detecting audience templates from `archives_audiences_archives`):

| Heuristic | Clarity |
|---|---|
| `user_id` histogram | One account (122462) holds 28% of templates — possible service account, but no clean ≥80% boundary |
| Name pattern (contains "Peak Performance") | **Fails** — 7 of 1000 audiences. Names are advertiser-driven. |
| Expression structural — pure (DS13+DS19 only) vs layered (+more DS) | **25% pure / 52% layered / 23% heavily-layered** — clean split |

**Best-effort classifier adopted (flagged as heuristic in the deck):** expression structural test.

- Template classified as `default_pp` if its expression uses only DS13 + DS19.
- Template classified as `custom_pp` if DS13 + DS19 plus any additional DS clause.
- Segments propagate the template classification via `audience_id`.

**Result (among PP adopters, stable since launch):**
- **34% default-only** (advertiser accepts the template as-is)
- **58% custom-only** (advertiser layers on exclusions / overlays / extra keywords)
- **3% both**
- **5% unclassified** (template not yet in archives — CDC lag or new audience)

Pattern holds steady across the entire ramp — no drift toward either default or custom over time.

Queries:
- Discovery: [queries/ti_896_pp_default_custom_discovery.sql](queries/ti_896_pp_default_custom_discovery.sql)
- Weekly rollup: [queries/ti_896_pp_default_custom_weekly.sql](queries/ti_896_pp_default_custom_weekly.sql)
CSVs: [outputs/ti_896_pp_default_custom_discovery.csv](outputs/ti_896_pp_default_custom_discovery.csv), [outputs/ti_896_pp_default_custom_weekly.csv](outputs/ti_896_pp_default_custom_weekly.csv)
Chart: [artifacts/ti_896_chart_06_pp_default_vs_custom.png](artifacts/ti_896_chart_06_pp_default_vs_custom.png)

**Open question:** formal product definition of "default Peak Performance" is a follow-up for the audience-tools team (Ryan / Jordan). This analysis uses a structural-heuristic proxy.

### Track C — Per-advertiser ROAS delta vs PP adoption (2026-04-22)

Audience-side cross-check against the war-room conversion metric. Source: `summarydata.sum_by_campaign_by_day` (no TTL issue; covers the window). Uses `view_viewed` for VVs, `click_conversions + view_conversions` for conversions, `click_order_value + view_order_value / media_cost` for ROAS, `order_value / conversions` for AOV.

Cohort: 1,217 advertisers with ≥1,000 VVs in both baseline (Aug 1–Sep 28 2025) and post (Dec 1–31 2025).

Labels:
- **new_adopter** — PP delivery share <1% in baseline AND ≥5% in post
- **continuing** — ≥5% in both windows (tiny sample: n=3)
- **non_adopter** — <5% in post

**Median deltas (relative):**
| Cohort | n | Δ conv rate | Δ ROAS | Δ AOV |
|---|---|---|---|---|
| new_adopter | 161 | +38% | +46% | −1% |
| continuing | 3 | — | — | — (noisy) |
| non_adopter | 657 | +82% | +124% | 0% |

**Key finding:** both cohorts saw Q4 ROAS lift, but **PP adopters captured ~half the lift non-adopters did (+46% vs +124%)**. AOV is flat in both, so the gap is in conversion rate, not basket size. This is consistent with PP correlating with weaker per-advertiser ROAS improvement in the drop window.

Framing guardrails:
- Audience-side **cross-check** — not the canonical conversion analysis (Ray owns that).
- Baseline (Aug–Sep 2025) is the tail of pre-drop period; no cleaner baseline available since PP didn't exist earlier.
- Survivorship bias: advertisers that cut spend entirely are excluded by the ≥1,000 VV threshold.

Query: [queries/ti_896_pp_vs_conv_scatter.sql](queries/ti_896_pp_vs_conv_scatter.sql)
CSV: [outputs/ti_896_pp_vs_conv_scatter.csv](outputs/ti_896_pp_vs_conv_scatter.csv)
Chart: [artifacts/ti_896_chart_07_pp_vs_conv_scatter.png](artifacts/ti_896_chart_07_pp_vs_conv_scatter.png)

## 5. Solution

Four-phase analytical bundle delivered as a single RevealJS deck (14+ slides), with query + CSV + chart artifacts for each phase:

1. **Headline cohort analysis** — Peak Performance (strict detector: `score_type=rtc + DS13 + DS19`) adoption went near-zero → 21% of 2025-active advertisers since the Oct 6 launch. Every other audience bucket flat ±1pp.
2. **Track A — Spend-weighted view** — PP ~12–13% of cohort spend vs 21% advertiser-presence. Adopters skew smaller-spend.
3. **Track B — Default vs custom** — 34% adopters use the pure DS13+DS19 template; 58% customize by layering additional DS clauses; 3% both; 5% unclassified.
4. **Track C — ROAS cross-check** — PP adopters captured ~half the Q4 ROAS lift (median +46% vs +124%). AOV flat in both cohorts.

Final deck: https://gist.githack.com/mdunn-mntn/f836ba48d987ead2894535e772c8f451/raw/ti_896_deck_standalone.html

Verification bundle: [artifacts/ti_896_verification.md](artifacts/ti_896_verification.md) — ten independent checks (V1–V10).

## 6. Questions Answered

- **Q:** Who is the cohort? **A:** Every advertiser with any active campaign from 2025-01-01 onward (even if first campaign launched in March). Confirmed in meeting transcript.
- **Q:** Which table preserves historical audience expressions? **A:** `archives.audiences_archives` (CoreDB). Ryan confirmed in meeting. BQ mirror existence TBD.
- **Q:** Scope limit? **A:** Audience composition only. Ray's team owns reporting/pixel/attribution; Will Cavey owns customer profile.

## 7. Data Documentation Updates

- `knowledge/data_knowledge.md` — corrected DS-id mappings (DS3 ≠ LiveRamp; DS35 = LiveRamp IP; DS11 = LiveRamp segments; DS13 = Vertical Categorization = Peak Performance). Added the canonical PP detector spec (score_type=rtc + DS13 + DS19) and noted the template-vs-segment schema difference.
- `knowledge/mntn_business.md` — added "Peak Performance adoption mix" section with the 21% / 12–13% / 34-58-3-5 / +46-vs-+124 numbers and the default-vs-custom heuristic.

## 8. Open Items / Follow-ups

### Resolved during this ticket
- ✅ BQ mirror for `archives_audiences_archives` confirmed
- ✅ DS35 = LiveRamp IP (canonical `data_sources` dim; not DS3 — memory was wrong)
- ✅ DS13 confirmed as Peak Performance per both product language (Bryce) and structural detector
- ✅ Nov 19 max-reach-off does not visibly bend any cohort-level composition curve

### Open for follow-up tickets
- [ ] **Causal test of Peak Performance on ROAS** — Track C shows PP adopters captured ~half the Q4 ROAS lift non-adopters did. Correlation only. A controlled hold-out / staggered-rollout analysis is the canonical next step. Default-following 34% cohort is a natural "as-designed" baseline.
- [ ] **Default vs custom performance split** — do custom-PP adopters underperform default-PP adopters? Segment Track C's ROAS deltas by Track B's classifier.
- [ ] **Formal default-PP definition** — for Ryan / Jordan (audience-tools). Our structural proxy (pure DS13+DS19 template) is best-effort.
- [ ] **MM spend-share decline** — Track A surfaced a sidebar finding: MM spend share dropped 75% → 38% over 18 months, materially bigger than advertiser-presence indicated. Worth its own ticket.
- [ ] **DS16 jump** — separate 1% → 35% DS16 (MNTN Taxonomy Data) ramp observed during verification, partially polluted by NOT-clause exclusions (12.5%). Deserves a dedicated structural look.
- [ ] **Scores-based work** — 35-day TTL means most historical PP score distributions are gone; can only analyze forward from ~now.

## Links

- **Slack war room:** `C0ATVHK2EDV` — raw scrape in [knowledge/slack_raw/2026-04-22_c0atvhk2edv_war_room_60d.json](../../knowledge/slack_raw/2026-04-22_c0atvhk2edv_war_room_60d.json), distilled context in [meetings/ti_896_slack_war_room_context_60d_2026_04_22.md](meetings/ti_896_slack_war_room_context_60d_2026_04_22.md)
- **Meeting transcript (2026-04-22):** [meetings/ti_896_01_war_room_audience_analysis_2026_04_22.txt](meetings/ti_896_01_war_room_audience_analysis_2026_04_22.txt)
- **Action items doc (war room):** https://docs.google.com/document/d/1zxLvBjd1EldNyKE1DHTGpbLHQIxFv9DPK11UjQ_eWvE/edit
- **Will Cavey's AID list:** https://docs.google.com/spreadsheets/d/1ghdvbDla2uvG5iAaGBPjVI3Y3_N7FUiY_Io5bSAaIVs/edit
- **Yesterday's war-room recording:** https://mountain.zoom.us/rec/share/7_tft7DC_6k7iMZeIIdTzyIher7AjpRhXK6VxjbzZMxLzbKyhque2NvSlGRwQEMf.bw1qjxAt8kBbr2cQ?startTime=1776787917000 (Passcode: 6TUj2A+E)
- **Canvas:** F0AUCJM4NBC (mntn.enterprise.slack.com)
- **Related:** [BER-2250](https://mntn.atlassian.net/browse/BER-2250)
