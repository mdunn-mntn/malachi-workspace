# DDP Quality Score — Repeatable Runbook

**What this is:** the step-by-step, re-runnable pipeline that turns raw vendor data into a per-vendor
**quality score + verdict**, built from the AUDI-1089 / TI-1027 methodology. One query per step, one visual
per step, each step contributing one component of the score. Run quarterly (uniqueness shifts as vendors and
our own bidstream coverage change) and at every contract renewal.

**Scope:** MM site-visit DDPs (`tpa.direct_data_partners` where `used_in_mntn_match=true`) vs the free
internal baseline (DS23 guid_log, DS30 augmentor). Roster is read from the registry each run — never hardcoded.

**Standing constraints:**
- Cost lenses = `media_spend` / `data_spend` only. Never platform_spend / billed / take-rate math in anything shareable.
- Performance > cost as the value framing (sole-IP VR is the dependency bound; domain→vertical coverage is the value).
- All uniqueness at the **30-day targeting window** (snapshot windows overstate/understate redundancy).
- Temporal ordering: the signal window strictly precedes the valuation week.

**Parameters (set once per run):**
```
SIGNAL_START = <first day>          # e.g. 2026-06-02   (30 complete days)
SIGNAL_END   = SIGNAL_START + 29d   # e.g. 2026-07-01
VALUE_START  = SIGNAL_END + 1d      # e.g. 2026-07-02   (7-day valuation week)
VALUE_END    = VALUE_START + 6d     # e.g. 2026-07-08
UNION_END    = VALUE_END            # soleness judged on SIGNAL_START..VALUE_END (37d)
```
svs access: temp external tables over `gs://mntn-data-archive-prod/signals/site_visit_signal/dt=*/` (run
pattern in the SQL headers). Jobs run 15 min–2.6 h — launch in parallel, never preempt. If a client dies,
results are recoverable for 24 h from BQ anonymous destination tables (`bq show -j <id>`, mind `--location`).

---

## The score (composite 0–100, then verdict = score × cost position)

| Component | Weight | From step | Definition |
|---|---:|---|---|
| **V — unique MM value** | 40% | 4 | sole classified domains, log-normalized across roster |
| **R — non-redundancy** | 15% | 3 | % of (ip,domain) pairs sole-or-freshest in window |
| **Q — signal quality** | 15% | 4+5 | % of its domains that classify × (1 − sole-IP unscored share) |
| **D — dependency** | 10% | 6 | T1 gated sole impressions, log-normalized |
| **P — performance parity** | 20% | 7 | sole-IP VR ÷ no-svs baseline VR, capped at 2, ÷2; low-n (<5K imps) → neutral 0.5 |
| **Liveness gate** | ×0/1 | 1 | delivered ≥95% of window days, else REVIEW regardless of score |

**Verdict logic (step 8-9):** dollarize V (sole classified × $3–13/yr) + D (T1/T2 × $0.50 CPM) into the
**defensible fee band**; compare the **actual bill** (metered from `usage_reporting_data`, or flat fee from
the renewal schedule): bill ≤ band → KEEP · ≤3× band → NEGOTIATE (print the target) · >3× → DROP.
High score + over-band bill = renegotiate, don't drop (the data is good, the price isn't).

---

## Steps (build order — one session each)

### Step 0 — Roster & actual cost — **BUILT 2026-07-10** (`runbook/queries/q0_roster_cost.sql`)
- **Claim:** "These are the vendors, what they bill, and what we ACTUALLY paid each month."
- **Query** `q0_roster_cost.sql`: registry dedup (`QUALIFY ROW_NUMBER() OVER (PARTITION BY data_source_id ORDER BY valid_from DESC)=1`
  — CDC dupes; DS26 broken SCD) + monthly `SUM(impressions), SUM(usage)` from
  `dw-main-bronze.coredw.usage_reporting_data` by `reporting_month` (last 6 months).
  Verify `usage = imps × (registry fixed_cpm / 1000)` — the meter spans ALL CPM DDPs (MM @ $0.50,
  ShareThis @ $0.95, LiveRamp IP variable), so the check uses each source's own rate (`meter_check_ok`).
  Scope = MM roster (incl. disabled, e.g. DS27 LaunchLabs) + any other metered source as context rows.
- **Output:** `q0_roster_cost.csv` — one row per source × reporting_month; flat-fee/unmetered keep one
  NULL-month row. **Visual:** `runbook/charts/generate_canonical_charts.py --step 0` → wide cost table,
  one month per column + 6-mo total; table only, no trend panels (built).
- **Score input:** the verdict denominator (cost position). Flat fees stay `pending` until the renewal schedule.
- **Run-pattern gotchas (apply to all canonical steps):** pass SQL with full-line comments stripped
  (`"$(grep -v '^[[:space:]]*--' <file>)"` — bq parses a leading `--` as a flag) and keep each file
  single-statement with `-- PARAM` inline literals (a DECLARE makes bq echo the script text into CSV stdout).

### Step 1 — Scale & liveness (+IPv6) — **BUILT 2026-07-10** (`runbook/queries/q1_scale_by_day.sql`)
- **Claim:** "Every source delivered every day; here's each feed's true size and IPv6 exposure."
- **Query** `q1_scale_by_day.sql`: per `dt × data_source_id`: rows, IPv6 rows, distinct IPs, domains, % URLs with path.
  The date window lives entirely in the external-table URIS list (no date predicate in the SQL) —
  parameterizing a run = regenerating URIS for SIGNAL_START..SIGNAL_END (loop in the header).
- **Output:** `q1_scale_by_day.csv`. **Visual:** `runbook/charts/generate_canonical_charts.py --step 1` →
  liveness table: days delivered, partial days (<50% of vendor median, amber), median rows/day, weakest
  day, IPv6 share, gate (table only, internal baselines grayed).
- **Score input:** liveness gate (≥95% days) + IPv6-undercount flag (Justuno 19.6% → footprint ×~1.24).
- **First-run findings (Jun 2–Jul 1 2026):** all 10 sources 30/30 days — every gate PASS. Partial-day
  incidents: 33Across Jun 20 at ~8% of median; Klickly Jun 24–26 3-day sag (weakest 24%).

### Step 1b — Column richness of the drops — **BUILT 2026-07-10** (`runbook/queries/q1b_column_richness.sql`)
- **Claim:** "All drops share one 10-column schema; richness = which fields each vendor populates + what values look like."
- **Query** `q1b_column_richness.sql`: one HOUR slice (SAMPLE_DT × SAMPLE_HH in the external-table URI), per
  `data_source_id × field`: % populated (non-null, non-empty) + modal example (APPROX_TOP_COUNT, 80 chars).
- **Output:** `q1b_column_richness.csv`. **Visuals:** `--step 1b` → TWO PNGs: `q1b_schema_fields.png`
  (field-population matrix: field, example, % per source) + `q1b_url_richness.png` (30d median % with
  path from q1 + modal URL, ranked).
- **Score input:** context for Q (a vendor whose url is ad-infra or domain-only classifies fewer domains).
- **First-run findings:** `query_parameters` dead everywhere; `advertiser_id` internal-only (guid_log);
  `user_agent` only 33Across/Sovrn/33A API + internal; URL path share: Klickly 100% (Shopify) vs
  33A API 26% (RTB endpoints) vs 5x5 4% (domain-only); Sovrn modal URL malformed (doubled protocol).

### Step 1c — Content quality (junk markers) — **BUILT 2026-07-10** (`runbook/queries/q1c_content_quality.sql`)
- **Claim:** "Population % says a field is filled, not that it's worth anything — measure junk directly."
- **Query** `q1c_content_quality.sql` (same hour slice as q1b): per source — Googlebot-IP % (66.249.x),
  bot-UA %, top-IP share, private-IP %, uid dup %, timestamp-stamping share, URL parse-fail %
  (NET.REG_DOMAIN NULL), URL malformed % (doubled protocol), top/top-5 domain concentration, distinct domains.
- **Output:** `q1c_content_quality.csv`. **Visual:** `--step 1c` → junk-marker table, amber ≥3% / red ≥25%
  (concentration: amber ≥40 / red ≥70).
- **Score input:** feeds Q qualitatively; parse-fail directly discounts V (unparseable rows never classify).
- **First-run findings:** **Sovrn 77% of URLs malformed+unparseable** (doubled protocol — only ~23% of the
  feed is usable); **Klickly 94% myshopify.com** (117 distinct domains/hr); **5x5 53% outbrain.com**;
  **33Across 6.4% Googlebot IPs + 5.7% bot UAs**; 33A API top-5 domains = 58% (RTB infra). Clean everywhere:
  uid ~unique, timestamps real (no batch stamping), private IPs ~0.

### Step 2 — Window reach
- **Claim:** "Over the actual targeting window, vendor V reaches N IPs / M domains / P pairs."
- **Query** `q2_window_reach.sql`: per ds, window-cumulative distinct IP / domain / (ip|domain).
- **Canonicalize from:** `audi_1089_q1_scale_30d.sql` query B.
- **Output:** `q2_window_reach.csv`. **Visual (new):** grouped reach bars, IPs vs domains side-by-side —
  makes the "big IPs, tiny domains" shape (Klickly, 33Across API) visible at a glance.
- **Score input:** context only (raw supply ≠ value) — displayed, not weighted.

### Step 3 — Uniqueness & recency (pairs)
- **Claim:** "X% of V's pairs are irreplaceable in-window; Y% are same-day-redundant (insurance)."
- **Query** `q3_pair_recency.sql`: per (ip,domain) per ds MAX(dt) → per ds: sole / freshest / tied / stale +
  netnew-vs-free (not in DS23/30). The 2.6 h scan — launch first.
- **Canonicalize from:** `audi_1089_q2_pair_master_30d.sql` query A. Cross-check anchor: DS25 ≈ 69–70% sole.
- **Output:** `q3_pair_recency.csv`. **Visual:** 100% stacked recency mix (exists: `chart_recency_mix`).
- **Score input:** **R** = pct_sole + pct_freshest. Tied share doubles as the coverage-if-down (insurance) metric.

### Step 4 — Classified-domain contribution (the value axis)
- **Claim:** "V uniquely supplies K domains that Matched can actually classify — the signal MM consumes."
- **Query** `q4_domain_value.sql`: per ds: domains, sole domains, sole+classified (wcv join), % classified.
- **Canonicalize from:** `audi_1089_q2_pair_master_30d.sql` query B.
- **Output:** `q4_domain_value.csv`. **Visual:** ranked sole-classified bars (exists: `chart_sole_classified_domains`).
- **Score input:** **V** (40%) + half of **Q** (% domains classified). Dollarized: sole classified × $3–13/yr.

### Step 5 — Quality of unique reach (adverse selection)
- **Claim:** "V's unique IPs do/don't survive contact with delivery — uniqueness ≠ usefulness."
- **Query** `q5_sole_quality.sql`: IP membership (union window) × CIL valuation week: per ds × {touched, sole}:
  delivered %, score-tier mix (tiers per TI-1027: 10000 / 8000 / 6666-9999 / mid / maxreach / unscored).
- **Canonicalize from:** `audi_1089_q3_score_tiers.sql`. Cross-check: DS25 touched HI ≈ 35–40%.
- **Output:** `q5_sole_quality.csv`. **Visual:** sole-quality two-panel (exists: `chart_sole_quality`).
- **Score input:** other half of **Q** = (1 − sole unscored share).

### Step 6 — Dependency tiers (what actually needed the vendor)
- **Claim:** "$D of weekly media went to impressions that could not have served without V."
- **Query** `q6_value_tiers.sql`: per ds: T3 touched / T2 sole / T1 sole+scored(≥6666)+non-RTC
  (`NOT REGEXP_CONTAINS(model_params, r'realtime_conquest_score=10000')`; HS not AHS) — imps + media + data.
  Includes **check A** (share of scored delivered IPs with zero svs signal — svs-necessity; anchor: r≈0.05%).
- **Canonicalize from:** `audi_1089_q4_value_tiers.sql`.
- **Output:** `q6_value_tiers.csv`. **Visual:** dependency bars T2 with T1 overlay (exists: `chart_dependency_by_vendor`).
- **Score input:** **D** (10%), dollarized into the band: T2 × $0.50 CPM + T1 media.

### Step 7 — Performance parity (the headline lens)
- **Claim:** "Impressions to V-sole IPs convert at/below/above the no-signal baseline."
- **Query** `q7_sole_vr.sql`: per ds: sole imps, visits, VR overall + by band; plus the four-way membership
  baseline (vendor-sole / vendor-shared / other-svs / no-svs) for at least the renewal-focal vendor.
- **Canonicalize from:** `audi_1089_q5_vr_membership.sql` (visit join per AUDI-1070's validated pattern).
- **Output:** `q7_sole_vr.csv`. **Visual (new):** per-vendor VR dots vs the no-svs baseline line + guid_log
  reference; n<5K imps cells greyed "insufficient volume".
- **Score input:** **P** (20%) = min(VR_sole / VR_no-svs-baseline, 2) / 2; low-n → 0.5 neutral.

### Step 8 — Cost vs value bands (THE money chart)
- **Claim:** "V's bill is inside / N× above what the data is worth."
- **Query:** none — arithmetic over steps 0+4+6 (python in the chart script).
- **Visual (new):** per-vendor horizontal band (green = defensible fee band, amber = ≤3× band, red beyond)
  with the **actual-bill marker** placed on it. Flat-fee vendors: band + "fee pending" marker.
- **Score input:** the verdict multiplier (KEEP / NEGOTIATE+target / DROP).

### Step 9 — Composite scorecard
- **Claim:** one row per vendor: score components → quality score → bill → verdict.
- **Query:** none — `build_quality_score.py` reads q0–q7 CSVs, computes V/R/Q/D/P × liveness gate,
  writes `quality_scorecard.csv`.
- **Visual (new):** scorecard table + **score-vs-annualized-bill quadrant scatter** (top-left = underpriced
  keepers; bottom-right = overpaid drops) — the one-slide summary for leadership.
- **Also:** lineage blast radius is per-vendor MANUAL diligence (GitHub sweep) — required before any DROP
  is executed, not part of the score (a vendor can have a hard non-MM dependency, e.g. Predactiv HEM→CRM).

---

## Repeatability mechanics
- Everything canonical lives under `tickets/audi_1089_ddp_vendor_evaluations/runbook/`: SQL in
  `runbook/queries/` (q0–q7, each with the parameter block at top), chart script + PNGs in
  `runbook/charts/`; output CSVs land in `outputs/<run_date>/` (gitignored — raw data stays local).
- `run_quality_score.sh <SIGNAL_START>` launches q1–q7 in parallel (background), waits, recovers from anon
  tables if needed; then `audi_1089_generate_charts.py` + `build_quality_score.py` + `audi_1089_build_report.py`
  regenerate the full evidence report; share via share_deck.sh.
- Validation anchors on every run: DS25 sole ≈ 69–70%, check-A r ≈ 0.05%, usage = imps × 0.0005.
- First-run baseline (2026-07): scores/verdicts in AUDI-1089 root summary §4 + the evidence report.

## Build order for our 1-by-1 sessions
1. **Step 0** (roster+cost — smallest, immediately useful monthly even standalone)
2. **Step 1–2** (scale/liveness/reach — cheap scans, parameterization pattern established)
3. **Step 3–4** (the big pair scan + domain value — the heart of V and R)
4. **Step 5–6** (CIL joins — Q and D)
5. **Step 7** (performance — P)
6. **Step 8–9** (no new queries — bands, scorecard, quadrant chart, runner script)
