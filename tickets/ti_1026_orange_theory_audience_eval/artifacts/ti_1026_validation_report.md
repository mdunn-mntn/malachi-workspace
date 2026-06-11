# TI-1026 Independent Validation Report (8 adversarial validators)

The arithmetic confirms the key cross-check findings. Now I have everything I need to write the report.

The deliverable's stated 7-day numbers are internally arithmetic-consistent, and C3's per-segment sum (3.068M) reconciles almost exactly with C2's stated 3P reach (3.04M). But that internal consistency is the trap — both share the same single-week snapshot that two validators independently show is window-luck-dependent.

---

# TI-1026 Orange Theory Audience Eval — Independent Validation Report

## 1. Overall Verdict

**The recommendation HOLDS — but two of its supporting numbers do not, and one customer-facing claim is outright wrong.** The four load-bearing *directional* conclusions survive adversarial validation: drop the 11 3P segments (validated: ~86–87% of 3P IPs match no OTF keyword — stable across windows), prune the ~94 off-target/broad keywords (validated: 285/51/43 split reproduces exactly, every DROP is genuinely off-target), and geo is not the bottleneck (validated: 49.4%/24.9% reproduce exactly, and coverage only *rises* under tighter/US-only denominators). However, the **absolute 3P reach (3.04M), the "~2.65M incremental / +12% on MM," and the "MM is ~14x / 3P far smaller" framing are window-luck artifacts** — on an adjacent week 3P reach is 19.3M and MM/3P collapses to 1.13x. And the **"income/age (DS35) exclusions are inert / remove nobody" claim is REFUTED** — those 7 LiveRamp categories match tens of millions of IPs, including inside the deliverable's own window. The "six 3P segments deliver zero / Epsilon deprecated" claim is also a timing artifact, not segment death. The recommendation to drop the 3P segments stands on the robust overlap fraction, so the action does not change — but several specific numbers and statements must be corrected before this goes to the customer.

## 2. Claim-by-Claim Verdict

| Claim | Verdict | Key number observed | Discrepancy |
|---|---|---|---|
| Audience 34668 expression decomposition (DS19=379, DS35=11 incl; 7 excl branches; 946 studios @7mi) | **CONFIRMED** | DS19 cats=379 (set-equal to file), DS35 incl=11; radii_include=946 all @7mi | None structural. "T-Mobile" label for DS43 cat 1001 unconfirmed (no name lookup); immaterial |
| MM/3P reach & overlap | **PARTIAL** | MM 21.7M ✓; overlap 12–14% of 3P ✓; ~87% non-keyword ✓ — but 3P reach 19.3M vs stated 3.04M (6.3x); MM/3P 1.13x vs stated 7.2x | 3P absolute reach + "incremental on MM" + "14x larger MM" are window-sensitive and do NOT reproduce |
| 3P per-segment delivery (Stirista+Adsquare ~99%, 6 zero) | **PARTIAL** | In-window: Stirista 2.15M + Adsquare 0.89M ≈ 99% ✓; but 30-day: all 6 "zero" ids deliver 1.3M–6.7M | "Six deliver zero / Epsilon deprecated" is a timing artifact (last refresh 06-03, one day before window) |
| DS35 3P daily volatility (~2.1M on 06-08, 0 on 06-06) | **CONFIRMED** | 1006088981 = 2,108,102 on 06-08, no row on 06-06; 8–11 of 11 deliver nothing on any given day | None — if anything understated. DS35 was fully populated (102–107M rows/day) on zero-days, so not a partition artifact |
| Geo fence coverage / "not the bottleneck" | **CONFIRMED** | 49.4% blocks / 24.9% IP-capacity fenced — exact match | None — conclusion is conservative; CONUS (50.7%/25.4%) and US-only (52.9%/26.0%) raise coverage |
| Demographic exclusions inert | **PARTIAL** | DS1 (13 ids): zero ipdsc presence → truly inert ✓; DS35 (7 ids): match ~10M–15.4M IPs each on 06-04 (in-window) | "Income/age exclusions remove nobody" is FALSE for DS35 — removes tens of millions of IPs |
| Keyword classification (285 KEEP / 51 DROP / 43 REVIEW) | **CONFIRMED** | 285/51/43, 379 total, perfect 1:1 with canonical IDs, all sampled IDs live | None on counts/semantics. Soft: ~4 borderline KEEPs + Pillows/Mattress-vs-Blankets inconsistency (judgment, not error) |
| Campaign performance (319132–137, audience 34668) | **CONFIRMED** | 319137: 7.34M imps, 18,493 visitors, 0.252% VR, $109,380 (90d); all video, 0 display | Headline is a 90-day rollup not the 7d window; a 3rd campaign (319136) trickles 206 imps; audience-34668 link unverified (table not found) |

## 3. Cross-Check: Internal Consistency

I recomputed the arithmetic across validators:

- **C3 per-segment sum reconciles with C2's stated 3P reach.** C3's nonzero segments (Stirista 2.146M + Adsquare 0.886M + three small = **3.068M**) match C2's stated 7-day 3P reach (**3.04M**) to within 1%. Internally consistent. **But this is a false comfort:** both numbers are drawn from the *same single low-volatility week* (06-04..06-10). C2, C3, and C4 independently show that week excluded the 06-03 mega-batch (6.5M+ IPs), so the agreement reflects a shared window artifact, not a stable estimate.
- **"MM is ~14x the 3P layer" does NOT follow from C2.** C2's own stated figures give **MM/3P = 21.82M / 3.04M = 7.2x, not 14x.** If the deliverable's text says "~14x," it contradicts its own numerator/denominator — flag this directly. And on C2's independent adjacent week, MM/3P is only **1.13x** (19.3M vs 21.7M). So "14x" is wrong twice over: arithmetically inconsistent with the deliverable's own data, and not robust to window choice.
- **No contradictions between validators — they corroborate.** C2 (window-sensitivity), C3 (per-segment 30-day delivery), and C4 (daily volatility, zeros are not partition artifacts) all describe the *same* mechanism: bursty ipdsc 3P delivery (refreshes land on 2–4 days/month). C4 decisively refutes the "empty partition" alternative (DS35 carried 102–107M rows every day including zero-days). C6 extends the identical mechanism to the DS35 *exclusion* categories. The four agree; the only disagreement is each-validator-vs-deliverable, not validator-vs-validator.

## 4. REFUTED / PARTIAL / UNCERTAIN — Required Fixes

1. **DS35 exclusions are NOT inert (PARTIAL → the DS35 half is REFUTED).** *Fix:* Split the claim. Keep "DS1 (13 Oracle ids) inert" (verified zero ipdsc presence). **Delete the customer-facing statement that income/age exclusions "remove nobody"** — the 7 LiveRamp ids match tens of millions of IPs (e.g., cat 1005350999 ~15.4M on 06-04, in-window). The exclusions are *active and material*. Root cause: a single-day snapshot landed on a day those categories weren't in the rotating partition load.

2. **3P absolute reach / incremental / "14x" framing (PARTIAL).** *Fix:* Do not present 3P reach or "+2.65M / +12% incremental on MM" as point estimates — recompute across ≥3 non-overlapping 7-day windows and report a range (3P swings ~3M → ~19M) or a multi-week median with min/max. **Drop the "MM is 14x / 3P far smaller" framing** (it is arithmetically 7.2x in the deliverable's own numbers and only 1.13x on an adjacent week). Keep the robust headline: **~86–87% of 3P IPs match no OTF keyword** (overlap fraction stable at 12–14%) — this alone supports dropping the 3P segments.

3. **"Six 3P segments deliver zero / 3 Epsilon deprecated" (PARTIAL).** *Fix:* Reword to "these segments did not refresh during the 06-04..06-10 snapshot week; over the trailing 30 days all six delivered 1.3M–6.7M IPs (last refresh 06-03)." **Verify the "Epsilon deprecated" label with the segment owner before telling the customer those segments are dead — the data contradicts it** (the 3 Epsilon ids are the *largest* deliverers at 3.2M–6.7M).

4. **UNCERTAIN — audience 34668 ↔ campaign linkage.** C8 could not confirm campaigns 319132–137 attach to audience 34668 (campaign_audiences table not found, outside scope). *Fix:* If this linkage is load-bearing, cite the source table. Also clarify headline campaign metrics are 90-day lifetime totals, not the 7d eval window, and footnote campaign 319136 as a negligible trickle (recast "only 2 active" as "2 with material delivery").

## 5. Confidence for Handing to Kelly Thurlow

**MEDIUM.** The *recommendation* (drop 3P, prune keywords, geo not the bottleneck) is sound and survives every adversarial attack on its directional logic. But the deliverable currently ships at least one flatly false customer-facing statement and two window-luck numbers that an agency could easily falsify.

**Single most important caveat:** *ipdsc 3P delivery is bursty (refreshes land on only 2–4 days/month), so every reach/exclusion number computed from one week is window-luck-dependent.* This single fact invalidates three separate claims — "3P reach ~3.04M," "six segments deliver zero," and "income/age exclusions are inert." **Recompute all ipdsc category-reach figures over a ≥30-day window (and correct the DS35-inert claim) before this goes to Kelly Thurlow or the agency.**