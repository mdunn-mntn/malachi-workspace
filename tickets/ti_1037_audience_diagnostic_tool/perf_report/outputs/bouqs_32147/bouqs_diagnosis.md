# The Bouqs Co. — prospecting diagnosis (from the 24-module report)

**Scope.** The Bouqs runs as two advertiser units: **32147 eCommerce** (active — the subject here) and
**31906 Subscriptions** (dark since Dec 2025 — §7). Window: continuous Jan 2025 → May 2026; YoY = Jan–May
2025 (P1) vs Jan–May 2026 (P2). All figures from `run_report.py --params params/bouqs_32147.env`.

---

## 1. Headline

Bouqs eCommerce **prospecting** declined YoY — **ROAS 3.18× → 1.37× (−57%), revenue −65%** — on roughly flat
spend (−18%). The decline is a **conversion-quality / conversion-count problem, not smaller baskets** (AOV
+12%). The primary driver is that prospecting **scaled into low-intent national inventory** (mostly Mid /
unscored households), compounded by a **verified-visit measurement-window shortening (30→14d)** that
mechanically shrinks measured visits and conversions. It is **not** caused by geo, interest-narrowing, or
audience-pool exhaustion. The account's revenue engine — **retargeting** — is unaffected.

## 2. Where the money is (module 00 stage map)

| Stage | Spend | Conversions | Revenue | ROAS |
|---|--:|--:|--:|--:|
| Prospecting | $546K (58%) | 8,695 | $726K (17%) | **1.3×** |
| **Retargeting** | $316K (34%) | **40,319** | **$3.4M (81%)** | **10.9×** |
| Multi-Touch S2 | $66K | 2 | $1.4K | 0.0× |
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

- **Median prospecting HI-share is ~75%, and 5 of 13 campaigns are majority low-intent (<70% HI).**
- The single biggest campaign, **595017 "eComm" — reaches 5.0M households at 4% HI / 43% unscored, ROAS
  0.54× (a loss).** It is scaling national impressions into Mid/MaxReach + unscored supply.
- The **v2 frequency campaigns** (595010/595011/…): 6–20% HI, ROAS ~1.2–1.3×.
- The **high-quality audiences barely get volume**: the original frequency campaigns (117983/985/987) run
  93–95% HI but only 4–7K reach; **Subscriptions-prospecting (116732) is 83% HI at 9.36× ROAS** — the one
  strong prospecting campaign, and it is small.

So Bouqs's prospecting problem is the mirror image of a narrowing problem: **broad national campaigns
buying cheap low-intent inventory**, while the audiences that convert are starved of budget.

## 5. Contributing measurement confound (module 11)

The prospecting **verified-visit lookback window shortened 30d → 14d** between P1 and P2 (conversion window
constant 30d). A shorter VV window mechanically connects fewer visits *and* fewer conversions to
impressions, on a ~window-length lag — so **part of the measured visit/conversion/CVR gap in §3 is a
measurement change, not a real performance change.** The true decline is smaller than the raw −55% visits
implies; it cannot be cleanly separated without normalizing the window.

## 6. Gate behavior (modules 03, 03b, 06b)

The household-score gate is **auto-paced** (graduated thresholds, not on/off); the flagship 85384 thrashed
144× over the window. **No-gate days cluster in the Dec–Feb holiday window** (gate opened for volume), and
the **unscored share peaks at 46% in May 2026 — the score-level signature of a gate-off month** (cf. the
ribbon). Gate-off periods are when the low-intent/unscored supply enters, consistent with §4.

## 7. What is NOT the cause (ruled out)

- **Geo** (12b): national account, no DMA slicing — geo mix is not a lever here.
- **Interest narrowing** (12, 12c): MM and 3P are OR'd everywhere (additive); no MM-AND-3P throttle. The
  DS16 net-new funnel gates on 6–7 campaigns narrow by *who* (net-new households), not by 3P.
- **Pool exhaustion** (09, 10): cumulative HI reach is 1.3M and **still climbing** (~3K new/mo) ≈ **9% of
  the ~14.3M deliverable pool** — the addressable pool is not exhausted (and contracted only ~15% from
  peak). Frequency is low (~2.7 imps/IP). Recirculation is rising (brand-new HI share 100%→34%) but there
  is ample unreached HI supply — the issue is *reaching* it, not running out of it.

## 8. Seasonality & data caveats

- **Seasonal spikes** (05b/05c): Feb 2026 (Valentine's) drove conv +344% / rev +334%; May (Mother's Day)
  elevated ~+80%. These are expected gifting peaks, not anomalies — read YoY, not MoM, for the trend.
- **Score YoY not available** (06/06c): `household_score` logging began 2025-06, so **P1 (2025) has no
  score data** — the HI figures are P2-only; a true score-YoY needs an advertiser scored in both windows.
- **Audience instability** (07): one prospecting campaign (398872) changed its audience 8× under a fixed
  `campaign_id` (audience_id + data-source composition drift) — treat "the campaign" as a moving target.

## 9. The Subscriptions unit (31906) — context (dark 2026)

Separate advertiser, **dark since Dec 2025**. Sep–Dec gifting-season YoY (2024→2025): **impressions 13.9M
→ 3.7M (−73%), spend −57%** — a deliberate wind-down. Efficiency actually *improved* (visit-rate +96%,
ROAS 0.32× → 0.53×) and prospecting HI-share was **high (88–97%)** — so it was **not shut off for audience
quality; it was shut off for being structurally unprofitable** (ROAS < 1 even after improving). CIL retains
its Sep–Nov 2025 score data, so its modules populated; only the seasonal 0-impression months needed a
divide-by-zero guard.

---

## Recommended follow-ups
1. **Rebalance prospecting budget toward the high-HI campaigns** (116732 Subscriptions-prospecting 9.36×;
   the original frequency campaigns) and **cap/curtail 595017** (5.0M reach, 4% HI, 0.54× ROAS).
2. **Normalize the P1-vs-P2 comparison for the 30→14d VV-window change** before attributing the full
   visit/conversion decline to performance.
3. **Investigate the gate-off months** (Dec–Feb, May) — that is when low-intent/unscored supply enters;
   decide whether the holiday-volume tradeoff is worth the quality cost.
