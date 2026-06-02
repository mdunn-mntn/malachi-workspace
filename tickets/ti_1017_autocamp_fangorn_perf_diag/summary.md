# TI-1017: Autocamp Fangorn HHST collapse + performance diagnostic

**Jira:** https://mntn.atlassian.net/browse/TI-1017
**Status:** Complete
**Date Started:** 2026-06-02
**Date Completed:** 2026-06-02
**Assignee:** Malachi

---

## 1. Introduction

Autocamp campaign **570106** (Beeswax Television Prospecting, Stage 1 PTV/CTV, campaign group 114881 "West Coast 2026 AutoCamp Summer Soundtrack", advertiser 37569) was flipped onto Fangorn on **2026-05-18**. Tofer flagged a performance concern in `#targeting-infrastructure` on 2026-06-02 after the advertiser raised it; the bidder UI showed HHST collapsing from 10000 → 0 over 5/19–5/20 and a tier-mix shift from High Intent into Peak Performance / Mid Intent / Max Reach.

## 2. The Problem

### Tofer's original Slack post (2026-06-02 3:14 PM, `#targeting-infrastructure`)

> @Alex Knorr @Swapnil Patil @trixy @Matt Brorby
>
> We had a large client recently raise a performance concern, and upon checking what happened I found that they are a Fangorn advertiser. I thought it would be helpful to bring the details here so we can discuss as a team the possible sensitivities on the HHST side and see if there is anything we want to try to snuff out in terms of improvements.
>
> **Advertiser:** Autocamp
> **Campaign Group ID:** 114881
> **Campaign ID:** 570106
>
> **Background:**
> - This client was added to Fangorn on 5/18.
> - 5/1 – 5/19 we were bidding and spending primarily in High Intent.
> - Starting 5/19, there were no more High Intent IPs in the audience. They are re-distributed into PP and Mid Intent scoring.
> - This leads to the HHST reacting accordingly and opening up, beginning to target PP/MI from that point.
>
> **HHST movement:** 10000 → 6101
>
> Continuing into 5/20, we are not able to withstand our same spending and have to drop HHST again. This opens targeting up to Max Reach / Mid Intent / PP at that point on.
>
> **HHST movement:** 6101 → 501 → 0
>
> We were then in a place where pacing was leveled out and we started the process of tightening HHST back up so that we could drive better users to the site. We see this trend continue until the pacing outage at the end of May, which drops our HHST again. Since then, we have been pacing well every day and have seen HHST continue to climb which is a promising sign.
>
> **Question →** This client's audience has a lot of MM segments. Why would they have no high intent IPs within the Fangorn scoring model?

### Tofer's follow-up (3:22 PM, same thread)

> sorry false alarm based on APEX, we have high intent IPs here

So the original "no HI in audience" hypothesis was retracted, but the underlying performance concern (HHST → 0, bid-tier mix shift, advertiser complained) was still open. The ticket reframed into: "What actually drove the HHST collapse and the perceived performance shift?"

### Impact
- Advertiser raised performance concern.
- Bidder UI showed visually-alarming HHST trajectory (10000 → 0).
- 5/19 spend missed pacing by ~70% ($546 vs ~$1,850 daily target).

## 3. Plan of Action

1. Confirm campaign + audience metadata in BigQuery.
2. Pull daily HHST band distribution from `cost_impression_log` (5/01 – present).
3. Pull daily performance KPIs (impressions, spend, completes, visits, conversions, IVR, CVR, CPA, CPM) from `all_facts` + `event_log` + `clickpass_log` + `ui_conversions`.
4. Parse audience expression from `audience.audience_segments` to verify what's in DS46 / DS19 / etc.
5. Pull RTC firing rate to check whether RTC was carrying load and dropped off.
6. Synthesize → write summary + post Jira/Slack updates.

## 4. Investigation & Findings

### Q1 — Daily HHST band mix (`queries/q1_daily_hhst_spend_kpis.sql`, output `outputs/q1_daily_hhst.csv`)

HHST = the score threshold *the bidder applied at bid time*. `advertiser_household_score = 10000` means "only IPs scoring 10000 qualified to win this impression." `-1` means "no threshold set — bidder ignored scores entirely."

| Date | HHST=10000 % | Unscored (-1) % | MI band % | Total imps |
|------|-------------:|----------------:|----------:|-----------:|
| 5/01 | 99.7 | 0.2 | 0.1 | 80,444 |
| 5/14 | 99.5 | 0.4 | 0.1 | 80,210 |
| 5/15 | 71.1 | 16.8 | 12.1 | 83,949 *(one-day blip, recovered next day)* |
| 5/16 | 98.3 | 0.3 | 1.4 | 88,169 |
| 5/17 | 99.4 | 0.4 | 0.2 | 85,997 |
| **5/18 — Fangorn ON** | **99.4** | 0.4 | 0.2 | 71,765 |
| **5/19 — Crash** | **45.7** | **18.2** | **36.2** | **15,246** *(78% volume drop)* |
| 5/20 | 6.6 | **87.3** | 6.1 | 72,832 |
| 5/21 | 10.2 | 82.6 | 7.2 | 74,647 |
| 5/22 | 15.5 | 76.7 | 7.9 | 84,464 |
| 5/23 | 36.6 | 58.0 | 5.4 | 96,061 |
| 5/24 | 37.4 | 57.1 | 5.5 | 95,044 |
| 5/27 | 45.3 | 49.3 | 5.4 | 89,134 |
| 5/29 | 39.3 | 55.0 | 5.7 | 130,494 |
| 5/31 | 36.1 | 58.2 | 5.7 | 100,043 |
| 6/01 | 38.3 | 56.0 | 5.7 | 30,177 *(partial day)* |
| 6/02 | 40.5 | 54.3 | 5.2 | 26,889 *(partial day)* |

**Story:** Pre-Fangorn, the campaign ran ~100% at HHST=10000 (pure HI bidding). On 5/19 (first full day post-Fangorn), the bidder couldn't sustain pacing at HHST=10000 and volume collapsed by 78%. By 5/20 the bidder had dropped HHST entirely (unscored mode) to recover pacing. Steady state since 5/23 is ~40% HI + ~55% unscored — the campaign is no longer score-gated for the majority of its impressions.

### Q2 — Daily performance KPIs (`queries/q2_daily_kpis.sql`, output `outputs/q2_daily_kpis.csv`)

| Date | Imps | Spend | Completes | Visits | Convs | CPM | Completion % | **IVR** | CVR | CPA |
|------|----:|------:|---------:|------:|-----:|----:|-------------:|--------:|----:|---:|
| 5/01 | 79,288 | $1,701 | 79,970 | 376 | 6 | $21.45 | 100.9% | **0.474%** | 1.6% | $284 |
| 5/14 | 78,944 | $1,725 | 79,433 | 418 | 1 | $21.85 | 100.6% | **0.529%** | 0.2% | $1,725 |
| 5/17 | 85,518 | $1,890 | 85,464 | 447 | 4 | $22.11 | 99.9% | **0.523%** | 0.9% | $473 |
| 5/18 *(Fangorn on)* | 61,048 | $1,328 | 71,416 | 410 | 3 | $21.75 | 117% | **0.672%** | 0.7% | $443 |
| **5/19** | **23,064** | **$546** | 15,082 | 409 | 5 | $23.66 | 65.4% | **1.773%** | 1.2% | $109 |
| 5/20 | 73,380 | $1,666 | 71,286 | 494 | 3 | $22.70 | 97.1% | **0.673%** | 0.6% | $555 |
| 5/22 | 87,940 | $1,959 | 83,723 | 1,243 | 4 | $22.28 | 95.2% | **1.413%** | 0.3% | $490 |
| 5/24 | 95,244 | $2,175 | 94,018 | 1,247 | 3 | $22.84 | 98.7% | **1.309%** | 0.2% | $725 |
| 5/27 | 89,215 | $2,002 | 89,881 | 817 | 4 | $22.44 | 100.7% | **0.916%** | 0.5% | $500 |
| 5/29 | 156,166 | $3,572 | 129,732 | 790 | 4 | $22.88 | 83.1% | **0.506%** | 0.5% | $893 |
| 5/30 | 94,241 | $2,187 | 101,658 | 897 | 3 | $23.20 | 107.9% | **0.952%** | 0.3% | $729 |
| 6/01 | 24,529 | $559 | 29,867 | 804 | 7 | $22.78 | 121.8% | **3.278%** | 0.9% | $80 |
| 6/02 | 19,760 | $438 | 30,152 | 654 | 3 | $22.18 | 152.6% | **3.310%** | 0.5% | $146 |

**Counter-intuitive headline finding:** IVR roughly *doubled* post-Fangorn.
- Pre-Fangorn baseline (5/01–5/17): IVR averaged **~0.48%**.
- Post-Fangorn steady state (5/23+): IVR averaged **~1.0%**, with some days touching 1.4%+.

CPM held flat at $22. Completion rate held ~99%. CVR is too noisy at 1–7 conversions/day to read decisively but appears flat-to-slightly-down. CPA is dominated by conversion sparsity noise.

### Q3 — Audience expression (`queries/q3_audience_expression.sql`, output `outputs/q3_audience.json`)

Single targeted audience segment (`audience_segment_id=687454`, `audience_id=37418`, `segment_id=621065`, last updated 2026-05-26).

```
AND:
  OR:
    any(DS46, 1 cat: [135001])           ← MM/Fangorn vertical
    any(DS19, 161 cats)                  ← keywords (UI shows 18 "AI Recommended Attributes" — they expand to 161 category_ids)
  any(DS14, 1 cat: [1])                  ← people filter
  NOT:
    OR:
      any(DS34, advertiser=37569, lookback 30d)   ← CRM exclusion
      any(DS21, advertiser=37569, lookback 30d)   ← retargeting exclusion
score block: rtc, id=135001 (vertical 135001)
geos: location_ids [4003, 4069, 1749] (West Coast)
```

**Critical observation: OR semantics between DS46 (MM) and DS19 (keywords).** Per the Fangorn scoring rules ([knowledge/data_knowledge.md §1135 "Fangorn raw-score → HHST score-band mapping"](knowledge/data_knowledge.md)), an IP needs DS46 *and* DS19 (and raw>0.8) to be classified High Intent. IPs that are in DS19-only (keyword pool but not MM) aren't scored — with HHST>0 they fail the threshold and aren't bid on. So even though the audience *expression* contains both pools, the *bid-eligible HI subset* is the MM ∩ keyword intersection at raw>0.8, which is smaller than either pool alone.

### Q4 — RTC firing rate (`queries/q4_rtc_firing.sql`, output `outputs/q4_rtc.csv`)

| Date | RTC fired % |
|------|------------:|
| 5/16–5/18 (pre/at Fangorn) | 8.2–8.7% |
| 5/19 (crash) | 3.1% |
| 5/20–5/22 (HHST=0) | 0.4–0.7% |
| 5/23+ (steady state) | 3.0–4.5% |

RTC is gated by HHST per Ryan Kleck (memory: [reference_rtc_hhst_gating](memory)). When the bidder ran in unscored mode (HHST=-1), RTC effectively stopped firing. Post-recovery, RTC settled at ~half its pre-Fangorn rate (~4% vs ~8%), reflecting the lower share of HHST-gated impressions.

### Synthesis — why HHST collapsed

The mechanical chain:
1. 5/18 Fangorn flip → MM scoring substrate becomes Fangorn's raw-score model.
2. At HHST=10000, only IPs in MM ∩ DS19-keyword at raw>0.8 qualify.
3. That intersection is too small to fill Autocamp's ~$1,850/day pacing target on the West Coast geo restriction (location_ids 4003/4069/1749).
4. 5/19 — bidder tries to sustain HHST=10000; impressions collapse to 15K (78% volume drop, spend $546 vs $1,850 target).
5. 5/20–5/22 — bidder drops HHST entirely (HHST=-1, unscored mode) to recover pacing. Volume returns to ~75–85K/day. RTC effectively off.
6. 5/23+ — partial HHST recovery as the bidder retunes. Settles at ~40% HHST=10000 + ~55% unscored.

### Why the advertiser perceived a "performance concern" — but actual KPIs are flat-to-improved

The advertiser likely saw:
- **5/19 pacing miss** — only 30% of expected spend delivered. This is the clearest visible incident.
- **The HHST trajectory in their dashboard** — 10000 → 0 looks alarming.
- **Possibly a brief CVR dip during 5/20–5/22** while the bidder retuned.

But the steady-state KPIs are good:
- IVR **doubled** (0.48% → 1.0%).
- CPM unchanged.
- Completion rate unchanged.
- CVR flat (within noise band).

This is a Fangorn-transition shock — *not* a performance regression. The campaign now delivers more visits per impression on roughly the same spend.

## 5. Solution

This was a diagnostic spike. No code/config changes recommended for this specific campaign — the bidder's adaptive HHST response is working as designed. Recommendations / discussion points for the team:

1. **Communicate to Tofer / Trixy:** The performance concern is the 5/19 pacing dip during the Fangorn transition. Steady-state IVR has doubled. Mitigation for *future* Fangorn flips: warn advertisers that pacing may dip for 1–3 days while HHST retunes.
2. **OR-semantics + HHST=10000 fragility:** Audiences with `(MM OR keywords)` + HHST=10000 are structurally fragile post-Fangorn because the bidder can only fill from MM ∩ keyword ∩ raw>0.8. If the team wants to keep HHST high after Fangorn flips, the audience builder may need to surface a warning when OR-keyword expressions are paired with high HHST settings.
3. **Cross-reference TI-999 / TI-956:** The pattern observed here (MM ∩ keyword intersection sizing) is exactly the segment-quality problem TI-956 is scoring for. Worth adding Autocamp's audience to the TI-956 backtest cohort once that pipeline is live.

## 6. Questions Answered

- **Q:** Why did the bidder UI show "no High Intent IPs available" on 5/19 even though APEX confirms HI IPs exist in the audience?
  **A:** The bidder chart shows the tier classification of IPs *being bid on at decision time*, not the underlying audience composition. When HHST collapsed to fill pacing, the bid-time classification shifted from HI to PP/MI/MR. Audience composition unchanged.

- **Q:** Why did HHST collapse from 10000 → 0?
  **A:** The bidder dropped HHST progressively over 5/19–5/22 to recover the spend pacing it lost on 5/19. The Fangorn HI pool (MM ∩ DS19-keyword ∩ raw>0.8) was too small to sustain Autocamp's $1,850/day target on West Coast geo at HHST=10000.

- **Q:** Did Fangorn hurt performance?
  **A:** No. IVR doubled (0.48% → 1.0%) post-Fangorn. CPM unchanged. Completion rate unchanged. CVR flat within noise. The advertiser's perceived concern is the 5/19 pacing miss + the HHST UI optics, not steady-state KPI degradation.

- **Q:** Was RTC carrying load and did it drop off?
  **A:** RTC firing held at ~8% pre-Fangorn, fell to ~0.5% during HHST=0 mode, settled at ~3–4% in steady state. RTC is gated by HHST, so this tracks the HHST trajectory exactly.

## 7. Data Documentation Updates

No new schema or data knowledge — the pre-existing knowledge in `data_knowledge.md` (Fangorn raw-score mapping, HHST gating semantics, OR-semantics in MM+keyword expressions) was sufficient to interpret the findings. The pattern itself ("OR-keyword audience + HHST=10000 → pacing fragility after Fangorn flip") is worth adding to `data_knowledge.md` under the Fangorn section as a *future* failure-mode reference. Will add in a follow-up commit if Kale/Tofer confirm this is a recurring concern.

## 8. Open Items / Follow-ups

- [ ] Slack reply to Tofer with the headline finding (volume + IVR table).
- [ ] Optional follow-up: matched non-Fangorn West-Coast prospecting peer comparison to firm up the "Fangorn caused the HHST collapse" attribution (vs e.g. seasonal pacing dynamics). Not started — `outputs/q1`+`q2` already show the 5/19 step-change at the Fangorn flip date, which is causally tight enough for the immediate response.
- [ ] If team wants this pattern flagged proactively for future flips: build a Fangorn-readiness check that quantifies MM ∩ keyword intersection size relative to advertiser's daily impression target.
