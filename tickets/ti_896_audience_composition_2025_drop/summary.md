# TI-896: Audience composition shift analysis — 2025 performance drop war room

**Jira:** https://mntn.atlassian.net/browse/TI-896
**Status:** Complete v2 — post-critique fixes (M1–M10 + new charts 08/09 + bootstrap CIs) shipped 2026-04-22
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

### Primary finding — TWO coincident audience shifts (post-critique correction)

After the LEAD-cap fix (V11), the headline numbers changed materially. The "21% of advertisers have adopted PP" claim was inflated by paused-but-not-deleted campaigns whose archive expressions extended forever past their last delivery. Corrected:

**A. Peak Performance: near-zero → ~12% of currently-active advertisers (and ~12% of cohort spend).**

| Week | PP share (corrected) |
|---|---|
| 2025-09-22 | 0.7% (pre-launch baseline) |
| 2025-09-29 | 3.7% |
| **2025-10-06** | **6.5%** ← Peak Performance launch week |
| 2025-10-20 | 10.3% |
| 2025-11-03 | 10.7% |
| **2025-11-17** | **11.0%** ← Max Reach scoring turned off Nov 19 |
| 2025-12-29 | 11.5% |
| 2026-04-13 | **12.4%** |

**B. Mountain Matched spend share collapsed at the week of Oct 27 2025.** Held 73-79% of cohort spend through Oct 20; dropped to 56.7% on Oct 27; 44.0% on Nov 3; sustained 42-46% through April 2026. **A 30pp cliff in 1 week**, materially larger than the PP rise in spend dollars. Coincident with PP rollout.

**Other buckets in Sep–Dec 2025:** Keywords 49% → 44% (-5pp), 3P 41% → 38% (-3.5pp), MM presence 100% → 98% (-2pp), CRM flat. The original "everything else flat" claim was the same paused-campaign attribution issue; it weakens after correction.

**Presence and spend-weighted PP views now AGREE at ~12%** (the prior 21%/12% spread was the same Fix M10 issue). PP adopters do NOT skew smaller than cohort average.

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

### Track A — Spend-weighted composition view (2026-04-22; revised post-Fix-M10)

Joined archive effective windows to `sum_by_campaign_by_day` and weighted by `media_cost`. After Fix M10 corrected the presence overcount, the two views now agree.

**Result:**
- PP spend-weighted share = **~12% of cohort spend** (matches the corrected 12% presence number; no longer a "skew smaller" finding).
- **MM spend cliff (V16):** 73-79% pre-Oct 20 → 56.7% Oct 27 → 44% Nov 3 → 42-46% sustained through April 2026. This is the larger absolute-dollar shift in the drop window; promoted from sidebar to co-equal headline.

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

### Track C — Per-advertiser ROAS delta vs PP adoption (2026-04-22; revised with bootstrap CIs)

Audience-side cross-check against the war-room conversion metric. After Fix M10 (LEAD cap), the cohort sizes shifted (more advertisers correctly classified as new_adopter once stale paused-PP attribution was removed).

**Cohort sizes (post-Fix-M10):** 1,213 advertisers with ≥1,000 VVs in both Aug–Sep and Dec windows.

**Critical caveat (Fix M1):** about half of each cohort has $0 order value in at least one window (lead-gen / no-pixel). `delta_roas_rel` is NULL for them. Medians are computed on the valid-ROAS subset.

**Bootstrap-honest median deltas (Fix M3, 1,000 resamples, 95% CI):**

| Cohort | n cohort | n with valid ROAS | Median Δ ROAS | 95% CI |
|---|---:|---:|---:|---:|
| new_adopter | 206 | 101 (49%) | +64% | [+25%, +121%] |
| non_adopter | 1,007 | 381 (38%) | +130% | [+104%, +154%] |
| continuing | 4 | 3 | — | (too small) |

**Key revised finding:** point-estimate ROAS gap is real (~half the lift) but **the 95% CIs OVERLAP**. The gap is directional, not statistically robust. Framing softened from "captured ~half the lift" to "directional cross-check, consistent with concern but not definitive."

**Selection bias (M2 disclosure):** new adopters had ~1.5x higher *baseline* spend-weighted ROAS than non-adopters (~28-31 vs ~17-25 in Aug 2025). The two cohorts are not exchangeable — adopters self-selected. Attributing the lift gap to PP requires propensity matching; this analysis does not do that.

Framing guardrails:
- Audience-side **cross-check** — not the canonical conversion analysis (Ray owns that).
- Baseline (Aug–Sep 2025) is the tail of pre-drop period; no cleaner baseline available since PP didn't exist earlier.
- Survivorship bias: advertisers that cut spend entirely are excluded by the ≥1,000 VV threshold.

### Track D (NEW per Fix Section-4 #2) — Default-PP vs custom-PP × ROAS

Intersected Track B template classification with Track C window methodology. New adopters labeled by dominant template type:

| Class | n cohort | n valid ROAS | Median Δ ROAS |
|---|---:|---:|---:|
| Default-dominant | 54 | 34 | +41% |
| Custom-dominant | 135 | 58 | +64% |
| Mixed | 17 | 9 | +290% (noisy) |

Sample sizes are too small for confident inference. The apparent reversal (custom > default in ROAS lift, default > custom in conv-rate lift) is interesting but the CIs are too wide to claim it. Logged for follow-up with longer post window.

### Track E (NEW per Fix Section-4 #3) — Weekly cohort spend-weighted ROAS time series

Median ROAS is uninformative (most advertisers have $0 order value → median is 0). Switched to spend-weighted ROAS per week per cohort. Reveals the M2 selection-bias concretely: adopter cohort baseline ROAS was ~28-31 in Aug 2025, non-adopter was ~17-25 — the cohorts were not comparable at baseline.

Query: [queries/ti_896_pp_vs_conv_scatter.sql](queries/ti_896_pp_vs_conv_scatter.sql)
CSV: [outputs/ti_896_pp_vs_conv_scatter.csv](outputs/ti_896_pp_vs_conv_scatter.csv)
Chart: [artifacts/ti_896_chart_07_pp_vs_conv_scatter.png](artifacts/ti_896_chart_07_pp_vs_conv_scatter.png)

## 5. Solution (v2 — post-critique)

Five-phase analytical bundle delivered as a single RevealJS deck (~20 slides) + verification doc with V1–V16 checks:

1. **Headline cohort analysis** — Peak Performance (strict detector: `score_type=rtc + DS13 + DS19`) adoption went near-zero → ~12% of currently-active advertisers since Oct 6 2025. Other buckets: Keywords -5pp, 3P -3.5pp, MM -2pp presence; CRM flat.
2. **Track A — Spend-weighted view + MM cliff** — PP ~12% of cohort spend (matches presence). Mountain Matched spend dropped 73-79% → 42-46% in 1 week starting Oct 27 — co-equal headline.
3. **Track B — Default vs custom** — ~32% default-only, ~61% custom-only, ~3% both, ~5% unclassified. Random-sample discovery confirmed structural split.
4. **Track C — ROAS cross-check (with bootstrap CIs)** — adopters median +64% [+25%, +121%], non-adopters +130% [+104%, +154%]. CIs OVERLAP — directional only.
5. **Track D — Default vs custom × ROAS** + **Track E — Weekly cohort ROAS time series**. Selection bias surfaced: adopters had ~1.5x higher baseline ROAS.

Final deck (v2): https://gist.githack.com/mdunn-mntn/6139627102ffaff497ab2153d3bd9460/raw/ti_896_deck_standalone.html

Verification bundle: [artifacts/ti_896_verification.md](artifacts/ti_896_verification.md) — sixteen independent checks (V1–V16).
Bootstrap script: [artifacts/bootstrap_track_c.py](artifacts/bootstrap_track_c.py)
Standalone deck builder: [artifacts/build_standalone_deck.py](artifacts/build_standalone_deck.py)

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
