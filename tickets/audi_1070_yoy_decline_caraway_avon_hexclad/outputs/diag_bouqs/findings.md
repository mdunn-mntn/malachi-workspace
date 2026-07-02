# The Bouqs (AID 32147) — YoY Prospecting Decline: FINAL Diagnosis

**Verdict:** REAL DECLINE · **Failure mode:** GATE-REMOVAL/THRASH (with a portfolio/mix contributor) · **Confidence:** HIGH

**Power line:** The Bouqs didn't run out of high-intent — it stopped asking for it.

This is the HexClad family (gate-removal/thrash), NOT the Caraway family (over-scaling). Over-scaling is
ruled out because within-HI visit rate RISES (0.30% -> 2.40%) rather than collapsing. Verdict verified.

---

## What the verifier corrected (applied here)

The verdict holds, but five specific claims were tightened after empirical re-runs:

1. "Overall VR tracks the gate" is FALSE. At monthly grain: corr(pct_HI, overall_VR) = -0.453
   (NEGATIVE), corr(within_HI_VR, overall_VR) = +0.05, corr(pct_HI, within_HI_VR) = -0.67.
   The gate controls delivery COMPOSITION (HI-share) — verified — but overall VR does not follow the gate
   up/down. Decisive evidence: within-HI VR is healthy/rising while HI-share collapsed -> delivery LEFT a good pool.

2. March is composition-recovery, NOT overnight performance-recovery (cold-start confounded).
   Daily scan (campaign 529549): Mar 2-3 = 5-8 imps (fresh ramp), Mar 4 HI-share jumped to 62% and to 95% by
   Mar 17 — HI-share returned within days. But within-HI VR stayed ~0.05-0.09% all March, ramping to
   ~0.12-0.16% only by early April (TI-780 ~4-week ramp). March proves the gate works FOR COMPOSITION.

3. May's "71% unscored" is ~half portfolio/mix, not pure gate-removal. Per-campaign May 2026:
   595017 (obj=1 stage-1): 7.63M imps, 2.4% HI / 48.9% unscored = TRUE gate-removal.
   595018 (obj=5 Multi-Touch stage-2): 5.86M imps, 100% unscored BY DESIGN.
   MT2/MT3 (unscored-by-design) = 47% of May prospecting imps. Only the obj=1 ungated share is a gate to fix.

4. Flights are a campaign-GROUP attribute, not per-campaign. All 5 campaigns in group 119362 show identical
   34 flights / 24 short (<=72h) / 3.1-day avg. 04_flight_length.csv undercounts. Frame short-flight
   auto-ungating as a GROUP-level config.

5. The 55%->4% HI / 44%->71% unscored swing is Jun'25->May'26, NOT Jan-May YoY. Jan-Apr 2025 have ZERO score
   data (scores begin mid-May 2025). Directional evidence of the 2026 delivery shift, not the explanation of
   the Jan-May YoY visit drop.

---

## Paulo's 9 points (final)

1. Periods. YoY = Jan-May 2025 vs Jan-May 2026. Monthly-continuous (extra_a) flags: full-account Dec-2025
   pause ($0 spend/0 imps AID-wide; 622 tail-attributed visits = artifact); prospecting VR grind 0.48% ->
   0.23%; ROAS 2-3x -> ~1x; Mar-2026 CPM spike $19.62 during the clean re-gate (real, not artifact).

2. Campaigns (01_campaign_census). 2025 flagship = 398872 (grp "CTV eComm Prospecting 2026 -old", $433k,
   dark Dec-Feb). 2026 flagship = 595017 (grp 119362, $198k, launched 2026-04-15). New freq-variant v2 fleet
   + VDay 529549. Retargeting stable. MT2/MT3 companions unscored by design — not gate targets.

3. Expression (extra_e). No CRM-include narrowing; no anomalous op:not. Standard MM DS mix (DS1/35 LiveRamp,
   DS19 keyword, DS14 vertical). Fangorn DS46 only on newest campaigns after the window. Nothing explains the decline.

4. YoY % (extra_b, both lenses). Prospecting: spend -20%, imps -32%, visits -55%, conv -67%, order_value -64%,
   CPM +17%, VR -34% (0.368->0.244), ROAS -54% (2.77->1.27). AID-wide: spend -6%, visits -14%, ROAS -12%.
   Concentrated in prospecting; retargeting holds. Holds under BOTH lenses (LT -54.8%, LT+competing -54.6%).

5. MoM swings. Extreme. HI-share 55->19->6->77->4. Overall VR bounces 0.18-1.03% but does NOT co-move with
   HI-share (corr -0.45). Sharpest: Mar HI-share 77% (re-gate) then 4% in May (ungated stage-1 + MT2 mix).

6. Score dist (extra_d). No H1-2025 baseline. Score BINARY (10000=15.98M, 8000=0.56M, Fangorn=0) -> avg score
   useless; within-HI VR is the lens. Jun'25->May'26 (RTC-excl): %HI 54.9->4.4; %unscored 43.7->71.4; within-HI
   VR RISES 0.303->2.403 -> served-HI pool fine/improving; delivery left it. Opposite of Caraway.

7. Gate (05, 03, extra_c). 51 change events on 398872 (walking-ramp thrash); full removal Nov 11-24 2025
   (0 then -1) = holiday blowout; clean re-gate 10000 Mar 2026 (77% HI). Gate reliably controls COMPOSITION.
   It does NOT move overall VR at monthly grain (corr -0.45). Decisive gate-removal evidence is compositional:
   delivery left a healthy HI pool (within-HI VR rising) as HI-share collapsed.

8. Flights (extra_c, group-grain). Group 119362 (v2 flagship fleet incl 595017) = 34 flights / 24 short (<=72h),
   avg 3.1d — group-level config auto-ungating on short flights. VDay 529549 group = 11 flights / 7 short.
   Genuine config issue; note it is a GROUP attribute, not per-campaign.

9. Other. (a) RTC LIVE here (2.02M rows @ 10000 vs 120M @ -1) -> excluding is a REAL filter (12.3% Jun'25 ->
   0.12% May'26). (b) Portfolio/mix: ~47% of May-2026 prospecting imps are MT2/MT3 unscored-BY-DESIGN (595018
   = 5.86M imps 100% unscored) — inflates the "71% unscored" headline. (c) Dec-2025 full-account pause+relaunch
   = cold-restart. (d) Subscriptions unit 31906 dark in 2026 ($0) — removes ~$77k/6.5M-imp of 2025 if blended.
   (e) Tracking outage RULED OUT (no craters).

---

## Key numbers (final)

| Metric | Jan-May 2025 | Jan-May 2026 | YoY |
|---|---|---|---|
| Prospecting spend | $777k | $622k | -20% |
| Prospecting visits | 225,042 | 101,868 | -55% |
| Prospecting visit rate | 0.368% | 0.244% | -34% |
| Prospecting ROAS | 2.77 | 1.27 | -54% |
| AID-wide visits | 406,902 | 349,578 | -14% |
| Delivery %HI (Jun'25->May'26) | 54.9% | 4.4% | share -92% |
| Delivery %unscored (Jun'25->May'26) | 43.7% | 71.4% | share +63% |
| Within-HI visit rate (Jun'25->May'26) | 0.303% | 2.403% | +693% (RISING) |
| corr(pct_HI, overall_VR), monthly | — | — | -0.45 |
| May'26 stage-1 595017 unscored | — | 48.9% | gate-removal |
| May'26 MT2 595018 unscored (by design) | — | 100.0% | portfolio/mix |

Note: HI/unscored shares are Jun'25->May'26 (no 2025 score baseline). Visit/ROAS declines are Jan-May YoY.

---

## Recommended fix

Set and HOLD HHST >= 8001 on the obj=1 stage-1 2026 v2 prospecting fleet (esp. group 119362 / 595017,
currently ~83% ungated spend) and stop the walking-ramp thrash. Consolidate the High/Low/Auto-Frequency
variant campaigns to eliminate group-level short-flight (<=72h) auto-ungating. Do NOT gate the MT2/MT3
campaigns — unscored by funnel design. March shows HI-share returns immediately on re-gate, but pair with
pacing and allow ~4 weeks for performance to ramp (TI-780). Clean the YoY: exclude the Dec-2025 pause and
decide whether the dark Subscriptions unit belongs in the blend.
