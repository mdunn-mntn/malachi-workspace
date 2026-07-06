# The Bouqs Co. — prospecting diagnosis (from the 24-module report)

**Scope.** The Bouqs runs as two advertiser units: **32147 eCommerce** (active — the subject here) and
**31906 Subscriptions** (dark since Dec 2025 — §7). Window: continuous Jan 2025 → May 2026; YoY = Jan–May
2025 (P1) vs Jan–May 2026 (P2). All figures from `run_report.py --params params/bouqs_32147.env`.

---

## 1. Headline

Bouqs eCommerce **prospecting** declined YoY — **ROAS 3.18× → 1.37× (−57%), revenue −65%** — on *lower*
spend (−18%). AOV rose (+12%), so it's a **conversion-count / quality** problem, not smaller baskets. The
notable part: performance got **worse while spend fell** — the opposite of an over-scaling story.

**What actually CHANGED this period (candidate YoY drivers):**
1. A **new campaign, 595017, now dominates reach** — 5.0M households = **58% of all unique prospecting
   reach** — at **only 4% High-Intent / 0.54× ROAS**. A large, broad, low-intent campaign that didn't exist
   in P1 (launched Apr '26).
2. The **VV measurement window shortened 30 → 14d** — lowers absolute visits/conversions & visit-rate; CVR
   effect ambiguous (§5).
3. A **DS16 "net-new" gate was added to the active fleet (early Apr '26)** — excludes any IP already
   *impressed*, so campaigns only reach never-touched households (more aggressive than standard prospecting; §4b).
4. **DS21/DS34 exclusions added to 85384 (Oct '25)** — suppress own converters/site-visitors (standard hygiene).

**What is *chronically* suboptimal but did NOT worsen YoY** (a standing condition, not a driver of the
decline): a **persistently low HHST gate** (~4,800 time-weighted avg — well below HI-only 10,000, and it
actually *rose* 4,267→4,832 YoY) and **~30 short flights both years** (flat) that zero the gate to 0.

**Caveat:** `household_score` logging began 2025-06, so there's **no 2025 audience-quality baseline** — the
low-HI finding is a current-period cross-section, not proof quality *worsened* YoY. It is **not** caused by
geo, interest-narrowing, or pool exhaustion. The revenue engine — **retargeting** — is unaffected.

## 2. Where the money is (module 00 stage map)

| Stage | Spend | Conversions | Revenue | ROAS |
|---|--:|--:|--:|--:|
| Prospecting | $546K (58%) | 8,695 | $726K (17%) | **1.3×** |
| **Retargeting** | $316K (34%) | **40,319** | **$3.4M (81%)** | **10.9×** |
| Multi-Touch S2 | $66K | 19 | $1.4K | 0.0× |
| Multi-Touch S3 | $9K | 689 | $59K | 6.4× |

Retargeting drives **81% of revenue at ~11× on 34% of spend**. Prospecting is 58% of spend for 17% of
revenue — it is a top-funnel *reach* activity, so its YoY decline is a reach/quality story, not where the
account makes money. (Each "campaign group" is a full funnel; classify by `objective_id`, not group.)

## 3. The prospecting decline, quantified (module 04)

P1 → P2, prospecting (obj=1): spend −18%, impressions −23%, **visits −55%, visit-rate −42%, conversions
−69%, revenue −65%, ROAS 3.18→1.37×**. AOV **+12%** (baskets slightly larger). So the lost revenue is lost
*conversions*, and the lost conversions trace to lost *visits* (visit-rate halved), not to spend.

## 4. Root cause — audience quality (modules 00, 00b, 12, 12c)

Prospecting is **national** (8/9 campaigns target location_id 237 = all-of-US) with **additive** interest
(MM keywords **OR** 3P segments — never AND-narrowed). What differs is **who it reaches**:

- **Median prospecting HI-share is ~75%, and 5 of the 11 campaigns with score data are majority low-intent
  (<70% HI).**
- The single biggest campaign, **595017 "eComm" — reaches 5.0M households at 4% HI / 43% unscored, ROAS
  0.54× (a loss).** It is scaling national impressions into Mid/MaxReach + unscored supply.
- The **v2 frequency campaigns** (595010/595011/…): 6–20% HI, ROAS ~1.2–1.3×.
- The **high-quality audiences barely get volume**: the original frequency campaigns (117983/985/987) run
  93–96% HI but only 4–7K reach; **Subscriptions-prospecting group 116732 is 83% HI at ~8.6× ROAS**
  (10.0× for its lead campaign 580914) — the one strong prospecting audience, and it is small.

So Bouqs's prospecting problem is the mirror image of a narrowing problem: **broad national campaigns
buying cheap low-intent inventory**, while the audiences that convert are starved of budget.

## 4b. The DS16 "net-new" gate — added this period (modules 07/07b/12c)

Added to the **active fleet in early April 2026** (Apr 3 & 14; DS16 has a churny add/remove history —
removed mid-2025, re-added). Mechanically it **excludes any IP the advertiser has already *impressed***
(DS16 "Impressions" cat 535 + "Wins" 835567), OR keeps only the campaign's own already-served households
= a **net-new-reach gate**. This is **more aggressive than standard prospecting hygiene**, which only
excludes the advertiser's own **converters (DS21)** and **site-visitors (DS34)** — DS16 additionally
removes anyone merely *impressed*. Effect: campaigns can't re-touch impressed users, so there's **no
multi-touch/frequency within prospecting**, which tends to lower conversion efficiency (multi-touch
converts better). **NB: the "net-new gate" (DS16) ≠ the HHST *score* gate — two different mechanisms that
both get called "gate."**

The prospecting **verified-visit lookback window shortened 30d → 14d** between P1 and P2 (conversion window
constant 30d). A shorter VV window mechanically connects fewer *absolute* visits **and** conversions to
impressions (and lowers **visit rate**, since impressions are unchanged) — so part of the **−55% visits /
−42% visit-rate / lower conversion-count** in §3 is a measurement change, not real performance. **The CVR
(conversions ÷ visit) effect is ambiguous, though** (verified w/ Lizz): shortening only lowers CVR if the
dropped day-15–30 visits converted *better* than day-0–14 visits; if they converted *worse*, shortening
actually *raises* CVR. So the conversion-**rate** decline is NOT automatically explained by the window and
may be real. The absolute visit/conversion decline can't be cleanly separated without normalizing the window.

## 6. HHST gate — a STANDING low gate, not a YoY-worse one (modules 03, 03b, 06b)

Bouqs runs a **persistently low HHST score gate** (time-weighted avg **~4,800** — between Mid and
Peak Performance, well below HI-only 10,000), so it lets non-HI supply in *by design*. **Crucially, this did
NOT get worse this period** — the avg gate actually **rose** (P1 4,267 → P2 4,832) and thrash events
**fell** (104 → 76). What zeros it out are the **short flights** (≤3d auto-ungate to HHST=0), which are
**~flat YoY (30 → 31)** — also not a new problem. No-gate days cluster **Dec–Feb** (holiday volume), and the
**unscored share peaks at 46% in May 2026** (a gate-off month). So the low-HI reach is driven by the *level*
of the gate + short-flight zeroing — a **chronic condition**, not a YoY tightening/loosening. (85384, the
long-runner, has thrashed 144× over its whole life; that's lifetime, not this period.)

## 7. What is NOT the cause (ruled out)

- **Geo** (12b): **almost entirely national** (loc 237 = all-of-US). The one geo-sliced campaign is
  **108055 MM VDay 2026** (84 DMAs) — a **seasonal Valentine's** campaign (26% of spend), DMA-sliced by
  design, not a lever to change. (Several *dead* 0%-spend legacy VDay/MDAY '24–25 campaigns are also
  DMA-sliced.) So geo is not a driver — but it's not "no DMA slicing" either.
- **Interest narrowing** (12, 12c): MM and 3P are OR'd everywhere (additive); no MM-AND-3P throttle. The
  DS16 net-new funnel gates on 6–7 campaigns narrow by *who* (net-new households), not by 3P.
- **Pool exhaustion** (09, 10): cumulative HI reach is 2.5M and **still climbing** ≈ **18% of the ~14.3M
  deliverable pool** — the addressable pool is not exhausted (and contracted only ~15% from peak).
  Frequency is low (~3.3 imps/IP). Recirculation is rising (brand-new HI share ~100% in mid-'25 → ~30% by
  the June '26 partial month) but there is ample unreached HI supply — the issue is *reaching* it, not
  running out of it. *(These coverage figures span the full continuous window, Jan '25 → May '26.)*

## 8. Seasonality & data caveats

- **Seasonal spikes** (05b/05c): Feb 2026 (Valentine's) drove conv +344% / rev +334%; May (Mother's Day)
  elevated ~+80%. These are expected gifting peaks, not anomalies — read YoY, not MoM, for the trend.
- **Score YoY not available** (06/06c): `household_score` logging began 2025-06, so **P1 (2025) has no
  score data** — the HI figures are P2-only; a true score-YoY needs an advertiser scored in both windows.
- **Audience instability** (07): one prospecting campaign (398872) changed its audience 8× under a fixed
  `campaign_id` — the `audience_id` stayed constant (38049) but the **data-source composition drifted** —
  treat "the campaign" as a moving target.

## 9. The Subscriptions unit (31906) — context (dark 2026)

Separate advertiser, **dark since Dec 2025**. Prospecting Sep–Dec YoY (2024→2025, module 04): **spend −57%,
impressions −58%, ROAS 0.32× → 0.53×** — a deliberate wind-down. (Account-wide Sep–Dec gifting-season
delivery fell further — ~13.9M → 3.7M impressions from the monthly delivery pull — but that spans all
objectives; the verified prospecting figure is −58%.) Efficiency actually *improved* (prospecting
visit-rate +96%) and P2 prospecting HI-share was **high (~97% aggregate; 67–100% by month)** — so it was
**not shut off for audience quality; it was shut off for being structurally unprofitable** (ROAS < 1 even
after improving). CIL retains its Sep–Nov 2025 score data, so its modules populated; only the seasonal
0-impression months needed a divide-by-zero guard.

---

## Recommended follow-ups
1. **Cap/curtail 595017** (5.0M reach, 4% HI, 0.54× ROAS) and **rebalance toward the high-HI campaigns**
   (116732 Subscriptions-prospecting ~8.6×; the original frequency campaigns at 93–96% HI).
2. **Raise HHST toward 10000 (HI-only) and stop the short flights** (≤3d auto-ungate to 0). Note: the
   audience *already* uses MM (187 keywords) — the lever is the **gate level + short-flight zeroing**, not
   "adding MM."
3. **Normalize the P1-vs-P2 comparison for the 30→14d VV-window change** before attributing the full
   visit/absolute decline to performance (the CVR effect is ambiguous — §5).
4. **They can keep the DS21/34 exclusions and the shortened VV window** — both are defensible from their
   side — but understand these make *measured* performance look worse than before it was on. **Reconsider
   the DS16 net-new gate** (§4b): it blocks multi-touch/frequency within prospecting, which tends to lower
   conversion efficiency.
