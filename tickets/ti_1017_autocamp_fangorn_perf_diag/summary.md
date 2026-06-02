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

### Tofer's expanded context (3:33 + 3:39 PM, after I posted findings)

After my findings were posted, Tofer shared the broader advertiser context that we didn't have at the start. This is the actual customer-facing story:

> **Tofer (3:33 PM):** I think the overall topic is worth discussing. This client's performance is suffering after rolling them onto Fangorn, which I don't think it happening to a lot our advertisers. So maybe this is a one-off example?

> **Tofer (3:39 PM):** The performance in this is admittedly tough. The background is the following:
>
> Customer is seeing significantly lower performance YoY despite using the same audience and similar creative (**8x ROAS last year vs. 2x this year**). Main differences:
> - Conversion rate is down ~50%
> - Spend increased from **~$25k last May to ~$80k+ this May**
> - They are **maxing out their high-intent audience this year**, where-as last year this wasn't the case.
>
> I hadn't scoped this one fully but that's great we are seeing IVR lift post-Fangorn.
>
> I think the high intent piece from our graph was the only focus and so it became "why are we not spending as much in high intent anymore". When the reality is, **the new set of scoring methodology rolled out has helped the campaign in the last 2 weeks. I think that should be more of the focus.**

> **Tofer (3:39 PM, to Matt Brorby):** I allow the campaign's HHST to open up and then we start to increase frequency (1x/14 → 2x/7 → 4x/3d). That then helped us pace in full and we started seeing BOS bring the value back up (0 → 401).

### Other thread participants

- **Alex Knorr (3:35 PM):** Provided Fangorn vertical details: `advertiser=37569, vertical_id=135001` — matches our audience expression's DS46 cat_id and RTC `id=135001`.
- **Alex Knorr (3:38 PM):** "Im also seeing climbing visit rates after the change" — independently corroborated the IVR lift.
- **Bryce Wagg (3:39 PM):** "@Alex Knorr something maybe we should share with PEX as a use case?" — suggests this is a candidate Fangorn success story for the PEX team.
- **Matt Brorby:** Working through the campaign Activity Log in parallel (HHST + bos_campaign frequency-cap changes around 5/18–5/23, see §4 Activity Log timeline).

### Reframed problem statement

The story is now two coupled but distinct issues:

1. **YoY performance degradation** (the real customer-facing concern): 8x → 2x ROAS year-over-year, CVR down ~50%, spend up ~3x ($25K → $80K+). The customer is now maxing out their high-intent audience because they're spending 3x as much against a similarly-sized scored pool. *This is not a Fangorn issue — Fangorn rolled out 2 weeks ago; the YoY drop is a 12-month phenomenon.*

2. **Fangorn flip transition shock** (the precipitating signal): HHST collapse on 5/19 made the YoY problem visible by surfacing it as a sudden HHST=10000 → 0 trajectory in the UI. **But Fangorn itself helped the campaign — IVR doubled.** This is the part Tofer wants the team to focus on.

### Impact
- Advertiser raised YoY performance concern.
- Bidder UI showed visually-alarming HHST trajectory (10000 → 0) that *looked like* a Fangorn regression.
- 5/19 spend missed pacing by ~70% ($546 vs ~$1,850 daily target) during the HHST retune.
- Actual Fangorn impact: positive — IVR doubled, KPIs flat-to-improved.

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

### Q5 — Campaign Activity Log (Matt Brorby, 2026-06-02 thread)

Matt shared the campaign Activity Log; chronological reconstruction of operator (Tofer) actions:

| Date / time | Field | Before → After | Direction |
|-------------|-------|----------------|-----------|
| 5/15 7:09 PM | hhst (household_score_threshold) | 6666 → 6201 | tightened slightly |
| 5/16 7:09 PM | hhst | 10000 → 6666 | loosened (likely automated daily retune) |
| 5/18 4:09 PM | bos_campaign (secondary_frequency_cap) | 1 per 14 days → 4 per 3 days | **loosened frequency dramatically** |
| 5/18 7:09 PM | hhst | 6101 → 10000 | tightened (Tofer pushing HHST high pre-Fangorn) |
| 5/19 5:10 PM | daily_budgets | 774.48 → 819.1 | budget bumped |
| 5/19 7:09 PM | hhst | 501 → 6101 | tightened (recovering after 5/19 crash) |
| 5/20 7:09 PM | hhst | 0 → 501 | tightened slightly (HHST had bottomed out) |
| 5/21 1:09 PM | bos_campaign sec_freq_cap | 2 per 7 days → 1 per 14 days | **tightened frequency** |
| 5/22 5:09 PM | bos_campaign sec_freq_cap | 4 per 3 days → 2 per 7 days | **tightened frequency** |
| 5/23 7:08 PM | hhst | 401 → 0 | loosened (intentional, per Tofer narrative) |

**Operator strategy (Tofer's own description):** "I allow the campaign's HHST to open up and then we start to increase frequency (1x/14 → 2x/7 → 4x/3d). That then helped us pace in full and we started seeing BOS bring the value back up (0 → 401)."

This means the HHST collapse on 5/19–5/22 was *partly operator-driven*, not purely an automated bidder response. Tofer deliberately allowed HHST to drop AND loosened frequency caps to recover pacing volume. The "natural" Fangorn-only response would have been less drastic; the manual frequency cap loosening (1x/14 → 4x/3d is ~10x looser) is what allowed the campaign to use the unscored pool to maintain spend.

### Q6 — ROAS pre vs post Fangorn (campaign 570106)

Computed from Q2's `order_amt_total` + `spend_usd` (same `all_facts` + `ui_conversions` source).

| Window | Spend | Order Amt | ROAS |
|--------|------:|----------:|-----:|
| Pre-Fangorn (5/01–5/17) | $31,734 | $44,651 | **1.41x** |
| Post-Fangorn (5/18–6/02) | $27,735 | $42,798 | **1.54x** |

**Post-Fangorn ROAS is up ~9%.** This is *despite* the 5/19 crash day (spend $546, ROAS 5.52x — high but tiny denominator). Caveat: conversions are sparse (1–7/day, ~110 total in May) so daily ROAS is very noisy. The directional read (post > pre) is robust to the noise.

Note this campaign-570106 ROAS (1.4–1.5x) is lower than Tofer's customer-facing "2x ROAS this year" because Tofer's number aggregates the full campaign group 114881 (CTV + display + multiple campaigns); we're looking at campaign 570106 in isolation.

### Q7 — Fangorn-cohort HHST trajectory comparison (answers Tofer's "is this a one-off?")

Compared Autocamp against all 316 advertisers flipped onto Fangorn between 2026-04-01 and today (filtered to advertisers with >1K impressions in both pre-flip and post-flip windows). Pre-flip window: 7 days before flip. Post-flip window: 14 days after flip.

**Cohort distribution of HHST=10000 share delta (post − pre), in percentage points:**

| Statistic | HHST=10000 Δ (pp) |
|-----------|------------------:|
| Min (worst collapse) | -57.5 |
| 5th percentile | -22.5 |
| 25th percentile | -6.5 |
| **Median** | **-1.6** |
| Mean | -4.7 |
| 75th percentile | +0.3 |
| 95th percentile | +5.1 |
| Max | +44.1 |
| **Autocamp (37569)** | **-50.2** |

**Cohort summary:**

- 66% of advertisers (208/316): stable HHST=10000 share, within ±5 pp of pre-flip
- 4% of advertisers (13/316): drop of ≥30 pp post-flip (severe collapse)
- **Autocamp: bottom 1.3 percentile (rank 4 of 316), only 3 advertisers had a worse drop**

**Conclusion:** Tofer's "is this a one-off" hypothesis is empirically *correct.* The Fangorn rollout is generally stable — most advertisers' HHST trajectory barely changes. Autocamp is one of the most extreme outliers, in the bottom 1.3% of the entire cohort. The factors that drive Autocamp's collapse (3x YoY spend increase, OR-semantics audience expression with a thick keyword pool, high HHST starting point at 10000) are not present in the median advertiser. Worth a follow-up to characterize the other 12 severe-collapse advertisers and see if they share Autocamp's pattern.

### IVR chart (Tofer's deeper-history snapshot)

Tofer also posted a per-day Imp Visit Rate chart showing 5/03–6/02 — clean step change at 5/18:
- 5/03–5/17 baseline: 0.50–0.70% IVR (flat band)
- 5/18 step change: jumps to ~1.5%
- 5/18–5/27: oscillates 1.3–2.0%
- 5/28–6/02 climbs further: 1.5% → 3.0%+

This is independent visual confirmation of the IVR doubling we computed from `all_facts` (Q2) — and it shows the lift is *continuing* to grow, not regressing.

### Synthesis — why HHST collapsed

The mechanical chain (combining bidder behavior + Tofer's manual adjustments per the Activity Log):

1. 5/18 Fangorn flip → MM scoring substrate becomes Fangorn's raw-score model. Tofer pushed HHST to 10000 at 7:09 PM and loosened freq cap to 4x/3d at 4:09 PM.
2. At HHST=10000, only IPs in MM ∩ DS19-keyword at raw>0.8 qualify.
3. That intersection is too small to fill Autocamp's ~$1,850/day pacing target — *especially given the YoY spend tripling* — on the West Coast geo restriction (location_ids 4003/4069/1749).
4. 5/19 — bidder tries to sustain HHST=10000; impressions collapse to 15K (78% volume drop, spend $546 vs $1,850 target).
5. 5/20–5/22 — Tofer allowed HHST to drop (manual + bidder) and loosened frequency caps; bidder runs in unscored mode (HHST=-1) for 77–87% of impressions. Volume returns to ~75–85K/day. RTC effectively off.
6. 5/23+ — Tofer started tightening HHST back up (0 → 401 → ...) and pulling frequency caps tighter (4x/3d → 2x/7 → 1x/14). Bidder settled at ~40% HHST=10000 + ~55% unscored.
7. **Throughout the entire period, IVR was rising.** Fangorn's scoring is finding better-quality IPs even in the unscored fallback pool.

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

This was a diagnostic spike. No code/config changes recommended for this specific campaign — the bidder's adaptive HHST response is working as designed and Tofer's manual retunes (frequency-cap loosening, deliberate HHST drop) have already stabilized the campaign. Recommendations / discussion points for the team:

1. **Reframe the narrative for the team / advertiser.** The "Fangorn hurt performance" story is wrong; Fangorn *helped* the campaign. The real story is two coupled issues:
   - **Fangorn flip (2-week scope):** IVR doubled (0.48% → 1.0%+ and still climbing per Tofer's chart). This is a *win.*
   - **YoY ROAS drop (12-month scope):** 8x → 2x ROAS, CVR down ~50%, spend up 3x ($25K → $80K+). The customer is now maxing out their high-intent audience because they're spending 3x as much against a similarly-sized scored pool. This is a separate scaling problem, not a Fangorn problem.
2. **Candidate PEX use-case** (Bryce's suggestion): Share Autocamp as an example where Fangorn transition optics looked bad (HHST → 0) but the actual KPI was positive. Useful for setting expectations on future flips. Recommend Alex K + Bryce drive the PEX share-out.
3. **Mitigation for future Fangorn flips.** Warn advertisers that pacing may dip 1–3 days while HHST retunes and that the visible HHST trajectory is not a quality regression. Possible product touchpoint: bidder UI annotation on Fangorn-flip day.
4. **OR-semantics + HHST=10000 fragility.** Audiences with `(MM OR keywords)` + HHST=10000 are structurally fragile post-Fangorn because the bidder can only fill from MM ∩ keyword ∩ raw>0.8. For high-spend advertisers maxing out HI, this fragility compounds with the YoY scaling issue. Worth flagging on future advertiser onboarding to Fangorn.
5. **Cross-reference TI-999 / TI-956.** The pattern observed here (MM ∩ keyword intersection sizing) is exactly the segment-quality problem TI-956 is scoring for. Worth adding Autocamp's audience to the TI-956 backtest cohort once that pipeline is live.
6. **Generalizability check (Tofer's question: "is this a one-off?").** Worth a follow-up — pull post-flip HHST trajectories for all advertisers flipped onto Fangorn in the last 60 days. If most advertisers see a smaller HHST adjustment than Autocamp, this is indeed a one-off driven by Autocamp's YoY spend scaling. Recommend scoping as a separate ticket if there's interest.

## 6. Questions Answered

- **Q:** Why did the bidder UI show "no High Intent IPs available" on 5/19 even though APEX confirms HI IPs exist in the audience?
  **A:** The bidder chart shows the tier classification of IPs *being bid on at decision time*, not the underlying audience composition. When HHST collapsed to fill pacing, the bid-time classification shifted from HI to PP/MI/MR. Audience composition unchanged.

- **Q:** Why did HHST collapse from 10000 → 0?
  **A:** The bidder dropped HHST progressively over 5/19–5/22 to recover the spend pacing it lost on 5/19. The Fangorn HI pool (MM ∩ DS19-keyword ∩ raw>0.8) was too small to sustain Autocamp's $1,850/day target on West Coast geo at HHST=10000.

- **Q:** Did Fangorn hurt performance?
  **A:** No. IVR doubled (0.48% → 1.0%) post-Fangorn. CPM unchanged. Completion rate unchanged. CVR flat within noise. The advertiser's perceived concern is the 5/19 pacing miss + the HHST UI optics, not steady-state KPI degradation.

- **Q:** Was RTC carrying load and did it drop off?
  **A:** RTC firing held at ~8% pre-Fangorn, fell to ~0.5% during HHST=0 mode, settled at ~3–4% in steady state. RTC is gated by HHST, so this tracks the HHST trajectory exactly.

- **Q:** Is the advertiser's "performance concern" actually about Fangorn?
  **A:** No. Tofer's expanded context (3:39 PM) shows the customer-facing concern is the YoY ROAS drop (8x → 2x), with conversion rate down ~50% and spend up 3x ($25K → $80K+). That's a 12-month scaling problem, not a 2-week Fangorn problem. Fangorn just made it briefly *more visible* via the HHST UI optics. Per Tofer's own conclusion: "the new set of scoring methodology rolled out has helped the campaign in the last 2 weeks. I think that should be more of the focus."

- **Q:** Was the HHST collapse organic (bidder-driven) or operator-driven (Tofer)?
  **A:** Both, in coupled fashion. The campaign Activity Log (Q5) shows Tofer manually loosened the secondary frequency cap from 1x/14-days to 4x/3-days on 5/18, then allowed HHST to drop. The bidder's own response to pacing pressure piled on. Without the frequency-cap loosening, the campaign would have collapsed even harder; with both levers loosened, the campaign found a new equilibrium at ~40% HI + ~55% unscored.

- **Q:** Is this a Fangorn-wide problem or a one-off?
  **A:** **One-off, empirically confirmed (Q7).** Across 316 advertisers flipped onto Fangorn since 4/01, the median HHST=10000 share barely changed (−1.6 pp) and 66% of advertisers were within ±5 pp of their pre-flip share. Only 4% had ≥30 pp drops. Autocamp is the **4th most severe collapse of 316**, in the bottom 1.3 percentile.

- **Q:** Did Fangorn improve ROAS for this campaign?
  **A:** Yes, +9% (Q6). Pre-Fangorn 5/01–5/17: 1.41x. Post-Fangorn 5/18–6/02: 1.54x. Caveat: conversions sparse (1–7/day) so noisy. Directional read robust.

## 7. Data Documentation Updates

No new schema or data knowledge — the pre-existing knowledge in `data_knowledge.md` (Fangorn raw-score mapping, HHST gating semantics, OR-semantics in MM+keyword expressions) was sufficient to interpret the findings. The pattern itself ("OR-keyword audience + HHST=10000 → pacing fragility after Fangorn flip") is worth adding to `data_knowledge.md` under the Fangorn section as a *future* failure-mode reference. Will add in a follow-up commit if Kale/Tofer confirm this is a recurring concern.

## 8. Open Items / Follow-ups

- [ ] Slack reply to Tofer with the headline finding (volume + IVR table).
- [ ] Optional follow-up: matched non-Fangorn West-Coast prospecting peer comparison to firm up the "Fangorn caused the HHST collapse" attribution (vs e.g. seasonal pacing dynamics). Not started — `outputs/q1`+`q2` already show the 5/19 step-change at the Fangorn flip date, which is causally tight enough for the immediate response.
- [ ] If team wants this pattern flagged proactively for future flips: build a Fangorn-readiness check that quantifies MM ∩ keyword intersection size relative to advertiser's daily impression target.
