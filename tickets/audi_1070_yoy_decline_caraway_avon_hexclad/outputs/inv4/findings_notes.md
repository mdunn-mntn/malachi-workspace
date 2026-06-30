# Investigation 4 (adversarial) — running notes

## (a) Attribution lens — reporting_style oscillated, was LT in 2025 / FT in 2026
- r2 archive: HexClad & Avon reporting_style flipped industry_standard<->last_touch DOZENS of times 2024-2025 (not a clean migration).
- As-of effective value: Avon & HexClad = last_touch during FebMay2025, industry_standard(FT) during FebMay2026. Caraway = FT 2026 (created Nov-2025).
- Client UI therefore compared 2025-LT vs 2026-FT for Avon/HexClad.
- FT-resolvable visit YoY (consistent FT both yrs) ~ same as LT YoY (Avon -11% vs -10%; HexClad -28% vs -24%; Caraway -22% vs -21%). So a CONSISTENT lens shows modest decline.
- MIXED (what client saw) LT25->FT26: Avon -62%, HexClad -78%, Caraway -22%. The lens switch manufactures ~50pp of Avon's and ~54pp of HexClad's apparent crash.
- FT null rate: Avon ~62-69%, HexClad ~76-78% (stable yoy), Caraway ~0%.

## (b)/(c) Conversion/visit-tracking BREAKS (steep, not gradual — Paulo obj 3,5)
- HexClad: VISIT-tracking gap Mar 3-17 2026. Visits 3000/day -> 20-70/day at CONSTANT impressions(250-345k) & spend; conv kept tracking. VR 0.6%->0.006%. Snap-back Mar 18. = VV pipeline break, not saturation. Sits inside FebMay window, craters March monthly (ROAS 3.53).
- Caraway: CONVERSION-tracking break Jan 2026. conv 30-90/day -> 2-15/day at constant impressions & visits; rev $20k/day->$1-3k/day. ROAS Dec 5.0 -> Jan 0.6. conv/visit 15-20% -> 1-2%. = conversion pixel break, not saturation.
- Avon LT: AOV rock-stable ~$50-57 all 26 months; conv/visit stable 4-6%; NO revenue break in LT. Avon's LT health is real; client crash = lens switch only.

## (d) MM degrade WITHIN high intent? — YES, evidence FOR Paulo's fear (HexClad, May)
- Score->VR (May2026, prospecting obj=1, non-RTC): scored_hi(AHS>=9000) 0.146% vs unscored 0.046% -> scoring beats unscored ~3x WITHIN prospecting (rescues "scored is better"). Raw unscored 1.2% was RETARGETING confound (obj=4 VR 1.2-1.9%).
- BUT YoY within prospecting: 2025 May prospecting was ALL unscored @ 0.419% VR; 2026 May "scored_hi" prospecting @ 0.145% (and residual unscored 0.273%) at ~same volume (8.2M imps both yrs).
  => 2026 high-intent-SCORED prospecting (0.145%) is WORSE than 2025 UNSCORED prospecting (0.419%). ~3x within-prospecting VR drop at constant volume. The AHS scoring layer did NOT protect quality; within-HI quality genuinely fell. Supports Paulo obj (2)/(d).
- Caraway gated 439156 (HHST=10000 must-be-scored) VR noisy 0.12-0.61%, ~0.14-0.23% in 2026 (no monotone decay but absolute VR very low for 'high intent').

## RTC note
- HexClad scored_hi RTC=true VR 0.44% (>non-RTC 0.146%) but only 274k imps. RTC (realtime_conquest) carries better VR.
