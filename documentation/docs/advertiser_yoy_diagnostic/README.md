# Advertiser YoY / MoM Performance-Decline Diagnostic

Reusable playbook + query pack to diagnose why an advertiser's prospecting performance (visits/ROAS) changed
between any two periods. Built from AUDI-1070 (HexClad/Avon/Caraway). **The dominant cause is almost always the
HHST intent gate, not the audience/MM model.**

## The decision tree
![Diagnostic decision tree](flowchart/diagnostic_flowchart.png)

Walk it top-down (**Q0→Q5**), cheapest-artifact-first: fix the measurement lens & scope, rule out a tracking
outage, then the delivery spine. The **Q2 gate fork** splits the two failure modes — delivery **left** High-Intent
(gate removed → HexClad) vs delivery **stayed** in HI but saturated a finite pool (over-scaled → Caraway).
"Genuine MM degradation" is the verdict only if every kill-condition fails.

- **Interactive / shareable page** (zoomable SVG + a table of every node's exact question and the table/column to
  query): [`flowchart/diagnostic_flowchart.html`](flowchart/diagnostic_flowchart.html) — rebuild with
  `python3 flowchart/build_flowchart_html.py`.
- **Vector + machine-readable:** `flowchart/diagnostic_flowchart.svg`, `.dot`, and
  `flowchart/diagnostic_tree.json` (full node detail — precise questions, kill-conditions, and confirm tables
  live here, not in the box labels).

## Run it
```
bash queries/run_diagnostic.sh <AID> <WIN_START> <WIN_END> <P1_START> <P1_END> <P2_START> <P2_END> [OUTDIR]
# e.g. Caraway, Jun2025–Jun2026 window, Jan–May 2025 vs Jan–May 2026:
bash queries/run_diagnostic.sh 40341 2025-06-01 2026-07-01 2025-01-01 2025-06-01 2026-01-01 2026-06-01 diag_caraway
```
WIN_* = full analysis window for the CIL score queries (scores only exist from **2025-05-06**). P1/P2 = the two
comparison periods for rate metrics (summarydata, back to 2024).

## The 7 steps (each query → what it answers → decision)
| # | Query | Answers | Decision |
|---|---|---|---|
| 1 | `01_campaign_census` | Client GROUPS (=campaign the client sees) + internal funnel-stage campaign_ids; names, lifespan, spend | Which group is the flagship? New/paused campaigns? Group names encode intent (Scale-Up / General-Interest) |
| 2 | `02_monthly_composition` | Monthly HI/PP/MI/unscored % of prospecting delivery | Did HI-share drop? When (the MoM step)? |
| 3 | `03_gate_timeline_daily` | **CRUX** — daily per-campaign delivery composition vs the HHST gate in effect | Does HI-share invert the day AFTER a gate flip? (corr≈1 ⇒ gate-driven) |
| 4 | `05_gate_change_events` | Collapsed HHST changes (0/-1=no gate; 6666=HI+PP; 10000=HI-only) | When was the gate removed/changed? Never reverted? |
| 5 | `04_flight_length` | Runs of consecutive active days per campaign | Short flights (≤3d)? **<72h flights auto-set HHST=0** (client campaign-mgmt cause; PEX to educate) |
| 6 | `06_fangorn_rtc_detector` | Continuous (Fangorn) score % + RTC share, monthly | Rule out Fangorn (continuous 8001-9999) & RTC (bypasses gate; conquest population, not fast-HI) |
| 7 | `07_rate_metrics_yoy` | Visit rate / ROAS / conv / AOV / OV for the two periods | Confirm the decline; flat AOV ⇒ conversion-count (audience-quality) problem |

## Decision logic (text form — see the tree above / `flowchart/diagnostic_flowchart.html`)
1. **Rate metrics** confirm a real decline? (flat AOV → conversion-count problem, not basket).
2. **Score distribution / monthly composition** → did HI-share fall? If not, look at attribution lens (FT vs LT, lookback) or a tracking outage.
3. If HI-share fell → **gate timeline + gate events**: was the gate removed/changed? Delivery inverts overnight with the gate ⇒ **gate is the cause**.
4. **Flight length**: short (<72h) flights auto-set HHST=0 → the client's campaign-management behavior is the root cause (PEX fix, not MNTN model).
5. **Rule-outs**: Fangorn (continuous scores; rolling per-advertiser migration), audience DS change (HI substrate = vertical DS13 ∩ keyword DS19 — check it stayed), RTC bypass, attribution lens.
6. **If gate stable + HI-share fell anyway** → pacing: live 30-day HI pool (~half the cumulative), replacement rate, HI frequency (rising = tightening); supply constraint only if frequency↑ AND reach/$↓ at matched gate.

## Reference (full mechanisms)
`knowledge/data_knowledge.md` (HHST pacing lever, short-flight auto-0, RTC, Fangorn, HI-pool-is-a-flow),
`knowledge/experimentation.md` (gate event-study, pacing methodology), `knowledge/data_catalog.md` (tables).
