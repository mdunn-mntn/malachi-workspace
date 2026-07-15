# AUDI-1089: [SPIKE] DDP Vendor Data Evaluations — Renewal Pass/Play per Vendor

**Jira:** https://mntn.atlassian.net/browse/AUDI-1089
**Status:** In Progress — Klickly (DS39) first, due **2026-07-13** (renewal live)
**Date Started:** 2026-07-09
**Assignee:** Malachi
**Supersedes:** AUDI-1051 (closed with pointer). 5x5 (DS25) already evaluated in TI-1027 → KEEP.

---

## 1. Introduction

Contract renewal season. Paulo (Slack 2026-07-09) asked which MM site-visit data vendors to keep; Kale:
"do what we did for 5x5 [TI-1027] for Klickly"; Paulo: **Klickly pass/play by Monday 2026-07-13** — their
renewal is live. Renewal schedule for the rest incoming from Paulo. Per Bryce (PMO): ONE spike ticket,
per-vendor outcomes marked off in the ticket description as completed. Workspace: one subfolder per vendor.

## 2. The Problem

Per vendor: is the data worth its cost? Deliver a pass/play verdict + implied max defensible fee band,
convincing from data alone, using the TI-1027 playbook generalized to the IP grain at the 30-day window.

## 3. Plan (per vendor — the playbook)

1. **Liveness + cost structure** — registry (CDC-dedup), GCS delivery, lineage blast radius (non-MM consumers).
2. **Scale + freshness** — per-day rows/IPs/domains/pairs (30d); recency sole-or-freshest per (ip,domain).
3. **Uniqueness (30d)** — IP/domain/pair sole + net-new-vs-free (DS23/30) + classified (wcv).
4. **Quality** — delivered score-tier mix; served-rate sole-vs-shared (junk check); IPv6 share.
5. **Value anchor (media/data-cost lens ONLY — take rates are sensitive/private, per ray in #data)** —
   impressions + media_spend + data_spend to vendor-touched / vendor-sole IPs, tiered:
   T1 = HHST-gated + scored (HS≥6666, not AHS, non-RTC) to sole IPs (floor: "could not have served without");
   T2 = all imps to sole IPs; T3 = all touched (ceiling, transparency only).
   Check A: share r of delivered scored IPs with zero svs signal → report T1×(1−r).
6. **Verdict** — fee band: floor = T1 × data-CPM lens, ceiling = T2 × media lens, peer anchor $0.50 CPM;
   PASS (drop) / PLAY (keep) / renegotiate once actual fee arrives.

**Canonical windows:** signal svs `dt 2026-06-02 → 2026-07-01` (30d targeting lookback) · valuation week CIL
`2026-07-02 → 2026-07-08` (strictly after — temporal ordering) · soleness on the 37d union.
**Union scans compute all 10 DS at once** → cross-vendor outputs land in ticket-root `outputs/`; each vendor
folder holds its interpretation, vendor-specific queries, and verdict.

## 4. Vendor checklist (mirrors Jira description — mark off as completed)

| Vendor | DS | Billing | Prior (TI-1027) | Status | Verdict |
|---|---|---|---|---|---|
| Klickly | 39 | flat_fee | 132 unique classified domains (7d), score 36, REVIEW | **DONE 2026-07-09** | **PASS (drop) unless ~free — max defensible fee ~$0.1-1.5K/yr; 126 sole classified domains; 26 gated sole imps/week; 1 visit/week on sole IPs** |
| Justuno | 24 | $0.50 CPM | 4,823 unique classified, 84% unique, KEEP-efficient | **DONE 2026-07-10** | **KEEP-trim** — 4,605 sole classified; bill $6.4K/mo (Jun) ≈ $77K/yr vs $14-60K/yr band → just over; trim meter |
| Predactiv | 26 | flat_fee | #1 unique (164,627), KEEP; rich metadata dropped; broken registry SCD | **DONE 2026-07-10** | **KEEP (renew)** — 226,826 sole classified (2.2× all other externals combined), value $0.7-3M/yr; HARD non-MM dependency (HEM→CRM/identity); metadata lever; lock price now |
| 33Across | 28 | $0.50 CPM | 9,277 unique (30%), REVIEW; ~38.6% redundant vs augmentor (AUDI-647) | **DONE 2026-07-10** | **NEGOTIATE** — bill $35.2K/mo (Jun) ≈ $422K/yr vs $30-100K/yr band = **4-7× over**; cap ≤$100K/yr or drop |
| Sovrn | 33 | $0.50 CPM | 293 unique (1.6%), DROP-CANDIDATE | **DONE 2026-07-10** | **DROP** — bill $9.7K/mo ≈ $116K/yr vs $0.5-2.4K/yr band = **~50-200× over**; 80% same-day tied; 0 visits/wk on sole IPs |
| Cybba | 36 | $0.50 CPM | 309 unique (5.7%), REVIEW | **DONE 2026-07-10** | **DROP** — bill $1.8K/mo ≈ $21.5K/yr vs $1.1-4.7K/yr band = ~5-20× over; needs Sean DAG change (ENABLED_DSIDS) |
| 33Across API | 40 | $0.50 CPM | 2,802 unique (3.2%), DROP-CANDIDATE; ~13.5% match (AUDI-647) | **DONE 2026-07-10** | **DROP/renegotiate** — bill $14.7K/mo ≈ $176K/yr vs $10-40K/yr band = ~4-18× over; 2% domain-unique; ~81% of pixel-topic infra load |

## 4b. Repeatable quality-score pipeline (2026-07-10 →)

The methodology is being canonicalized into **`documentation/docs/ddp_quality_score_runbook.md`** — 10 steps,
one parameterized query + one visual each, composite score (V 40% / R 15% / Q 15% / D 10% / P 20% × liveness
gate) → defensible fee band vs actual metered bill → verdict. All canonical materials live under `runbook/`:
SQL in `runbook/queries/` (q0–q7), chart script + PNGs in `runbook/charts/`, built one step per session. Build order: q0 roster+cost → q1-q2 scale/reach → q3-q4 pair recency +
domain value → q5-q6 CIL joins → q7 performance → bands/scorecard/runner.

**Progress:**
- **q0 roster+cost — BUILT 2026-07-10.** `runbook/queries/q0_roster_cost.sql` →
  `outputs/run_2026_07_10/q0_roster_cost.csv` + `runbook/charts/q0_roster_cost.png`
  (wide table, one month per column, via `runbook/charts/generate_canonical_charts.py`).
  Per-step deliverables are the SQL + the PNG only — no ad-hoc exports (user preference 2026-07-10).
- **q1b column richness — BUILT 2026-07-10.** `runbook/queries/q1b_column_richness.sql` (33 GB hour-slice scan) →
  `outputs/run_2026_07_10/q1b_column_richness.csv` + `runbook/charts/q1b_schema_fields.png` (field-population
  matrix) + `runbook/charts/q1b_url_richness.png` (path-share ranking + modal URL). Findings: shared 10-col schema; `query_parameters` dead everywhere;
  `advertiser_id` = guid_log only; `user_agent` = 33Across/Sovrn/33A API + internal only; URL path share
  Klickly 100% (Shopify) → 33A API 26% (RTB endpoints) → 5x5 4% (domain-only); Sovrn modal URL malformed
  (doubled protocol). Full details in data_knowledge.md § Site Visit Signal.
- **q1c content quality — BUILT 2026-07-10.** `runbook/queries/q1c_content_quality.sql` (33 GB hour slice) →
  `outputs/run_2026_07_10/q1c_content_quality.csv` + `runbook/charts/q1c_content_quality.png`. Beyond counts:
  **Sovrn 77% of URLs malformed (doubled protocol) AND unparseable — only ~23% of the $116K/yr feed is
  classifier-usable** (strengthens the DROP case; also a vendor bug report). **Klickly = 94% myshopify.com**
  (98% top-5; 117 reg-domains/hr but 629 distinct store hosts — niche by design, subdomains collapse).
  Sovrn malformation pattern: `https://<bare-domain>` + full URL concatenated; second half valid —
  recoverable by splitting on the 2nd protocol (bug report / negotiation leverage). **OPEN: augmentor
  (internal) shows the same pattern at 1.2% — check Sovrn's RAW drop before filing the vendor bug**
  (could be shared svs-processing). Cybba: 6.2% truncated hosts. Exhibits: `q1c_unparsed_examples.png`;
  hosts-vs-domains column added to `q1c_content_quality.png`.
  **5x5 = 53% outbrain.com** (widget network). **33Across: 6.4% Googlebot IPs, 5.7% bot UAs** (webmail+bots).
  33A API top-5 domains 58% (openwebmp RTB). Clean checks: uids ~unique, timestamps real, private IPs ~0
  everywhere — no batch-stamping artifacts.
- **q1 scale+liveness — BUILT 2026-07-10.** `runbook/queries/q1_scale_by_day.sql` →
  `outputs/run_2026_07_10/q1_scale_by_day.csv` (reused Jul 9 pull — identical query + window) +
  `runbook/charts/q1_scale_by_day.png` (liveness table). Findings: **no source missed a day** —
  all 10 feeds 30/30 days Jun 2–Jul 1, every liveness gate PASS. Two partial-volume incidents:
  **33Across Jun 20 at ~8% of median** (1.08B → 86M rows); **Klickly Jun 24–26 3-day sag**
  (24–46% of its 4.3M median). IPv6: Justuno 19.7%, 33Across 8.3%, Predactiv 7.9%, rest ~0.
  Findings: the meter spans ALL CPM DDPs — **DS35 LiveRamp IP (interests) bills $243–446K/mo ≈ $3.4M/yr,
  ~4× the entire site-visit CPM roster**; DS17 ShareThis @ $0.95 (rate cut from $1.20 "starting May usage"
  per registry notes, but meter implies $0.95 for all Jan–Jun — history likely restated); DS29 deepsync
  (CRM) @ $0.50. DS27 LaunchLabs = 9th MM-roster vendor, disabled, $0 metered. 5x5 registry note: "we
  provide report but only impression counts - unknown if shared with customer". meter_check_ok passes
  vs each source's own registry rate, all fixed-CPM sources, Jan–Jun 2026.

## 4c. Ryan Kleck thread (2026-07-10) — consumption filters, billing semantics, next step

Slack with Ryan (Sean out). Verified in airflow-ti where possible:
- **Billing follows USE:** vendors only credited when their data lands on MM-targeted serves — junk that
  never scores is never billed. **Reframe: junk % discounts VALUE, not COST** (Sovrn's $9.7K/mo bill is
  already for its usable ~23%; the DROP case = near-zero sole value vs that bill).
- **DS13 vertical path excludes junk domains:** `BLOCKED_DOMAIN_NAMES = (yahoo.com, aol.com, easybrain.com)`
  + ecommerce blocklist (verified in `aug_log_ip_vertical_id_hourly.py`). 33Across's 25% mail.yahoo.com
  never reaches DS13. **OPEN: does DS19 (keywords) use yahoo.com?** Ryan unsure ("we might use it???").
- **svs feature model** drops steelhouse/googlesyndication/gtm URLs; urlsplit does NOT drop Sovrn's doubled
  URLs (garbage hosts survive to classification, then die vs wcv).
- **MemDB membership log** = `gs://mntn-data-tpa-prod/tpa_membership_update_log/v2/` ("what actually goes in").
- **33Across (per Ryan, unverified):** likely resells the Magnite auction data we already receive.
- **q1d billed usage — BUILT 2026-07-10 (CORRECTS the reframe above).** `runbook/queries/q1d_billed_usage.sql`
  → `q1d_billed_usage.csv` + `q1d_used_vs_delivered.png` (funnel) + `q1d_billed_domains.png` (exhibit).
  June consumption funnel: **we bill on 0.23% (33Across) to 6.8% (Cybba) of delivered rows; 0.6–7.9% of
  delivered domains.** BUT junk survives to billing: **Sovrn's billed domains ARE the garbage hosts**
  (msn.comhttps 3.3%, yahoo.comhttps 2.2%); **33A API's top billed domains = cookie-sync endpoints**
  (cookies.nextmillmedia.com 9.2%, sync.programmaticx.ai 8.2%); **www.yahoo.com billed for 33Across 1.9%**
  despite the DS13 block (→ DS19 path?). So "junk never billed" is FALSE in practice — parse-garbage that
  survives urlsplit gets scored and paid. Flat-fee vendors (5x5/Predactiv/Klickly) pay regardless of use.
  q2 window reach also canonicalized (`q2_window_reach.sql`, reused Jul 9 pull).
- **q1e column consumption vs latent value — BUILT 2026-07-10.** `runbook/charts/q1e_column_value.png`
  (synthesis; no query). MM today = ip + url-domain + time + uid (+plumbing). Unused: **url path+query**
  (BUK/DS38 keyword extraction — Klickly 100%/Justuno 91% path share), **user_agent** (bot filtering
  BEFORE billing credit — 33Across's ~6% bot rows get PAID today; device/OS features), **query_parameters**
  (dead — vendor ask; Klickly checkout params). Ingestion mystery RESOLVED: DS24/33/39/40 arrive via Kafka
  pixel streams (`fpa_dsid{NN}_kafka_log` BQ landing tables), rest via batch drops.
- **q2 visual + q2b daily drops — BUILT 2026-07-10.** `q2_window_reach.png` (ranked raw counts: 5x5 has
  the most external IPs, 157M/30d; Klickly smallest domains, 257) + `runbook/queries/q2b_daily_drops.sql`
  (full-day scan Jul 1) → `q2b_daily_drops.png`. Drops/day: **Sovrn 71.4% of rows and 5.5M of its 7.9M
  IPs never survive the hard filters**; 33Across 0.03% hard BUT **316M rows/day (29%) DS13-blocked
  (yahoo/aol) + 52.7M bot-UA rows/day**; 33A API 66M/day (18%) blocked; Predactiv 8.7M/day (14%);
  guid_log 19% empty urls (internal); Klickly zero drops. IPs dropped/day: Sovrn 5.5M, 33A API 351K,
  5x5 91K, Cybba 19K, 33Across 18K.
- **q2c survival funnel — BUILT 2026-07-10 (ANSWERS the DS13/DS19 question).** `runbook/queries/q2c_funnel.sql`
  (day scan × wcv × product_categorization) → `q2c_funnel.png` pivot (sources across, stages down, % per
  cell, billed at bottom). **USED (DS13∪DS19-eligible) % of raw rows: Klickly 100.0, Justuno 99.4, Cybba
  97.4, 5x5 96.5, Sovrn 92.8, Predactiv 90.4, 33Across 77.6, 33A API 63.9** — eligibility is HIGH; the
  collapse to 0.2–7% billed is first-reporter credit competition + demand, not junk filters. **DS19 path
  is permissive (no blocklist, no parse gate): 33Across's yahoo = DS19-eligible (RESOLVED how yahoo bills);
  Sovrn's malformed hosts = 90.9% DS19-categorized (product_categorization contains the garbage keys)** —
  a pc data-quality issue worth raising alongside the Sovrn bug. **Credit semantics = OR across consumers:**
  the most permissive path (DS19 today) defines each vendor's billable pool; DS13 hygiene never protects
  the wallet. Fix belongs at pc/DS19 intake (parse gate + blocklist parity). DS13-classified alone: Klickly 99.8 →
  Justuno 94.7 → 5x5 90.7 → 33Across 52.2 → Predactiv 48.2 → 33A API 36.3 → Sovrn 8.9.
- **q2d usable-pool composition — BUILT 2026-07-10.** `q2d_usable_share.png` (chart-only over q2c).
  Of the daily USABLE pool: rows — 33Across 35.7%, **internal free 43.7%** (augmentor 32.8 + guid 10.9),
  33A API 10.0%, 5x5 5.0%, rest ≤2.4%. Used IPs — 33Across 32.2%, 5x5 10.6%. Classified domains —
  **Predactiv 34.3% (the #1 supplier on just 2.4% of rows)**, 33Across 19.8%, 33A API 13.9%, 5x5 12.0%,
  Klickly 0.04%. Shares of summed per-source counts (overlap not deduped; rows exact).
- **q5 score tiers + q9 scorecard v1 — BUILT 2026-07-10.** q5/q6/q7 CSVs+SQLs canonicalized from Jul 9
  pulls. `q5_score_tiers.png`: touched HIGH (HI+PP+grad): Cybba 52.9 > Klickly 52.3 > Justuno 46.1 >
  Sovrn 39.0 > rest 34-37; **sole HIGH 1.7-4.5% across all vendors — unique IPs are overwhelmingly
  unscored (adverse selection)**. `q9_vendor_scorecard.png`: per-vendor usable %, sole IPs, media $/wk
  touched/sole, HIGH %, bill/run-rate, worth $/mo band, verdict, key ask. Pending v2: q3 refresh
  (running), flat fees, composite score.
- **q9b composite ranking + runbook/README.md — BUILT 2026-07-10.** Composite (V40/R15/Q15/D10/P20):
  **5x5 70.4 > Predactiv 61.5 > 33Across 57.7 > Justuno 56.9 > Klickly 51.1 > 33A API 49.8 > Cybba 49.7 >
  Sovrn 35.5.** WTP bands added to `q9b_quality_ranking.png` (pay-up-to = band top; bill colored vs 3x
  rule) and THE INDEX table saved in `runbook/README.md` section 4. `runbook/README.md` = the logic document: value ladder, why sole media is tiny (adverse
  selection), fee-band/3x verdict rule, per-vendor negotiation targets + leverage + improvement asks,
  score formula, chart index, open items (q7b per-bucket performance matrix queued; HI-pool union;
  flat fees; Athena). q3/q4 CSVs+SQL canonicalized (raw pairs; usable refresh running).
- **q3 usable uniqueness — LANDED 2026-07-10 (~1h scan, anchor held: 5x5 69.3% sole).** R unchanged vs
  raw for all sources → composite/curved scores stand. NEW: **Sovrn sole IPs = 15,660 (0.08%)** vs 2.7M
  raw — its uniqueness was garbage; 80% of pairs same-day tied with other PAID vendors (99.7% net-new vs
  free = it duplicates vendors, not our internal logs). Density (pairs/IP): 33Across 15.6 ... Klickly 1.07.
  Scorecard "Sole usable IPs" column refreshed from q3.
- **q9c dependency-ceiling valuation — BUILT 2026-07-10** (`runbook/dependency_valuation.md` + 2 charts).
  User-designed bottom-up model: sole stock → won-bid flow ×52 → performance → $ at observed eCPM ×
  margin ladder, net of data costs; T1/T2 attribution range; envelope not CI (N=1 week). **Klickly:
  ceiling ~$4.0K/yr, realistic 30-50% margin WTP $418-859/yr, T1 floor $13/yr, break-even margin 11%**
  — confirms drop-unless-~free on a second independent lens. 33Across dependency base $270K/yr
  (largest); 5x5/Predactiv invert between lenses (small dependency, huge domain value) — lenses stay
  side-by-side, never summed (double-count rule).
- **q9d leave-one-out + q3b launched — 2026-07-10 (user's "sole data providers" insight formalized).**
  Dropping a metered vendor reassigns credits to the next reporter — bill ≠ savings. Bounds (floor =
  bill×sole, ceiling = bill×(sole+free-co-held)): 33Across saves $130–361K/yr (dup with FREE logs —
  biggest real pool), 33A API $80–137K, Justuno $71–75K, Cybba $15–21K, **Sovrn only $14.3–14.7K of
  $116K (87% paid-overlap — money relabels into 33Across)**. **SEQUENCING: renegotiate 33Across rate
  BEFORE dropping Sovrn** (else its credits inflate 33Across's bill + measured dependency pre-negotiation).
  Survivor sole-rate increase = leverage COST not benefit. q3b scan (holder-signature histogram → exact
  256-subset frontier; first-reporter classes; flat-fee wildcard) running in background. Full logic +
  toy-example corrections in `runbook/dependency_valuation.md`.
- **DS28 + DS40 = ONE VENDOR (Ryan, 2026-07-10):** 33Across batch vs real-time-ish feeds. Combined bill
  ~$598K/yr — the single negotiation covers both; feeds separable operationally (drop-the-API-feed is a
  viable outcome). Exact combined/union soleness + savings land with q3b masks (pairs held only by
  {28,40} jointly are invisible to per-DS soleness).
- **q6b attribution — LANDED 2026-07-10 (user's challenge VINDICATED).** 97–99% of sole-IP impressions
  ran through prospecting-family (MM-gated, vendor-dependent incl. max-reach) campaigns; retargeting
  1–3%. **True dependency ≈ T2 — the ceilings are the real numbers.** Klickly $2.16K/yr dependent
  (97.6% of T2); 33Across $266.6K/yr. Caveat queued: MM-vs-3P audience split within prospecting.
- **q3b LANDED — exact reassignment + roster frontier (2026-07-10, validation anchor perfect).**
  **CORRECTION to v1: Sovrn drop saves $109K (94%), not $14.5K** — its overlap is 81% with flat-fee
  vendors (5x5/Predactiv), not metered. Exact drop savings: 33Across $385.7K (91%), 33A API $142.9K
  (81%; 18.7% metered = its sibling), Justuno $77.1K (100%), Cybba $21.2K (98%). Metered-to-metered
  reassignment is negligible roster-wide. **Frontier (all 256 subsets exact): 3 vendors = 98.1%
  coverage (33A-comb+5x5+Predactiv); 4 = 99.5% (+Justuno); free logs alone 60.4%; combined-33Across
  union sole = 1.43B pairs incl 166M jointly-only.** Charts: q9d (exact), q9e_roster_frontier.
  Sequencing revised: Sovrn drop is safe now; lock flat-fee prices before drops (they absorb coverage).
- **q10 master waterfall — BUILT 2026-07-12.** Consolidated one-row-per-source table (user request):
  feed -> usable -> sole -> served/won -> HI -> performance, all grains labeled. Caveat: serving data =
  WON impressions (CIL); lost bids not tracked here (bid_logs extension possible, 90d TTL).
- **q9e2 exhaustive frontier — BUILT 2026-07-12.** All 256 subsets, best per size k: free-only 60.4% ->
  +33Across 78.1 -> +33A API 87.3 -> +5x5 95.9 -> +Predactiv 98.1 -> +Justuno 99.5 -> then <=0.22pp each
  for Klickly/Sovrn/Cybba. **Optimal sets NESTED** (greedy = exact optimum at every k) -> add-order is
  THE marginal-coverage ranking. Knee at k=4-5. `q9e_frontier_by_k.png`.
- **q9e2 frontier + money — 2026-07-12.** Each frontier row now carries cumulative exact metered
  recovery, dependent revenue at risk, NET @30-50% margin (+flat-fee count; HEM flag when Predactiv
  dropped). Knee economics: k=5 (drop Cybba+Sovrn+Klickly) nets ~$124-128K/yr for 0.53pp coverage;
  k=4 (+Justuno) nets ~$200-203K/yr for 1.86pp; k<=3 flags HEM prod risk.
- **Status 2026-07-12:** mega-pivot row list FINALIZED (user's Google-Sheet list + audit additions;
  full performance set Spend/Imps/Visits/Conv/Revenue/CPM/IVR/CVR/AOV/ROAS per cohort; 3-tier pricing
  lenses). q7b (avg HH score + touched VR) running; **q7c (conversions/ROAS join) explicitly deferred
  ("not yet")**; q11 mega-pivot builds when q7b lands. Klickly Slack answer drafted (observed $11.54 eCPM
  version). Flat-fee-vs-metered contract explainer given (renewal date = only lever on flat).
- **Status 2026-07-12 (later): TEMPLATE FILL SHIPPED.** User delivered the final question template as
  `outputs/audi_1089_quality_template.xlsx` (106 rows x 14 vendor columns, incl. LaunchLabs DS27
  disabled + out-of-MM LiveRamp IP DS35 / ShareThis DS17 / deepsync DS29 context columns) and un-deferred
  q7c. Built `runbook/charts/fill_template.py` -> `outputs/audi_1089_quality_template_filled.xlsx`
  (every cell answered; "% of column total" rows note source overlap). New canonical queries:
  **q7c_conversions.sql** (ui_conversions joined by ad_served_id to valuation-week CIL, deduped to one
  row per conversion event preferring last-touch — type 0 treated as 1; assists + disputed excluded;
  order_amt) and **q7d_platform_week.sql** (platform week anchors: 398.3M won imps / 28.03M served IPs /
  $3.53M media). Margin ladder updated to **15/20/30%** (user: blended margin ~15-30%). Row->source
  manifest = runbook/README.md §7. Verification workflow ran per-section recomputes vs source CSVs.
- **q7b + q7c findings (both landed 2026-07-12 evening; anchors exact — q7c imps = q7b imps per
  (ds,cohort); q7b sole imps = q6 exactly):**
  - **Touched-cohort performance is a platform mirror, NOT a vendor discriminator** — every vendor's
    touched pool covers 12-97% of platform served IPs, so touched CVR ~3.0%, AOV ~$360, IVR 1.6-2.4%
    are nearly identical across all 10 sources. Vendor differentiation lives in the SOLE rows.
  - **Sole-IP serves produced ~zero conversions in the valuation week:** 33Across 4 conv ($26.50),
    33A API 1 ($224), all six other paid vendors 0. Only guid_log's sole pool converts (97/wk,
    $2.7K). Sole ROAS: 0.01x (33Across) / 0.20x (33A API) / 0.00x elsewhere vs touched ~19-36x.
    Strengthens every DROP verdict: the sole tail these vendors uniquely contribute doesn't convert.
  - **Sole avg household score much weaker than touched** (touched ~8,200-9,000 vs sole 2,842-6,420;
    Klickly worst at 2,842, Cybba 4,161, Sovrn 4,994) with only 1-6% of sole imps scored at all
    (touched: 28-31%) — the sole tail is mostly max-reach/unscored inventory.
  - Sole-cohort conversion counts are Poisson-tiny; read the 0s as "<~1/wk", not exactly zero.
- **q7e/q7f (2026-07-12 late): sole visit rates ADJUDICATED — real darkness, not attribution breakage.**
  User challenged 116 visits/wk on 446.6K sole 33Across imps as impossibly low. q7e (platform VR by
  bucket, same join): retargeting 2.89% / unscored prospecting 1.11% / scored prospecting 0.72% — so
  sole cohorts at 0.01-0.03% are ~40x below even the coldest campaign bucket, demanding adjudication.
  q7f (unconditional clickpass activity on sole IPs, NO impression join): of 99,041 served sole
  33Across IPs, only 25 (0.025%) had ANY clickpass event for ANY advertiser that week (29 events);
  guid_log served-sole IPs: 1.43% active — 57x more, measured identically. Verdict: the vendors'
  unique IPs are genuinely dark households (rotating/low-activity tail), and since the ad_served_id
  join credits cross-device visits, 116/wk is generous if anything. Sole VR rows and all DROP
  verdicts stand. Canonical queries: q7e_vr_baseline.sql, q7f_sole_ip_activity.sql.

## 4d2. AUDI-1092 RESOLVED same-day (2026-07-13): billing regime change found — cost analysis VALID

**Decisive evidence (residue analysis, `usage_reporting_data` MM vendors 24/28/33/36/40):** the
table is row-per-(vendor, domain/segment, month); Jan-Apr 2026 rows are ~100% FRACTIONAL impressions
(clean 1/N fractions: .5, .33, .25, .67, .83 = halves/thirds/quarters/sixths — equal credit SPLIT
across contributing vendors), then **May-Jun 2026 rows are 100% INTEGER — single-vendor credit.**
The meter changed regimes at reporting month 2026-05 (coincides with the augmentor-into-svs release
2026-05-07/12). Both accounts were right at different times: teammate's "everybody gets a piece" =
the Jan-Apr era; AP-3779 first-reporter = the current era.

**Implications:**
- **June-bill-based cost analysis (q3b LOO) is VALID** — it modeled the current single-credit regime.
  Winner-rule nuance: savings are EXACT under first-reporter; under cheapest/free-priority (the only
  alternative) free logs win every overlapping re-race, so savings only go UP — q3b figures are
  floors either way: Sovrn ≥$109K, 33Across ≥$386K, 33A API ≥$143K, Justuno ≥$77K, Cybba ≥$21K/yr.
- **Value ranges (T1/T2/WTP) never depended on the credit model — unchanged.**
- **Never use Jan-Apr 2026 bills for LOO** — fractional regime, different arithmetic.
- Total billed MM imps fell 36% Apr→Jun (212.9M → 135.4M): regime switch + augmentor displacement.
- Pair-grain model fit is confounded by serve-volume weighting (both models TV ~20pp) — side-finding:
  **Sovrn collects 14.3% of billed imps on a 1.9% credited-pair share** — its credits concentrate on
  high-serve-volume signals (active junk-domain IPs), explaining its stubborn bill.
- Remaining (minor, still open in AUDI-1092): confirm the winner rule (first-reporter vs
  free-priority) via targeted_signal (Athena) or the dbt models `targeted_signal_ds_13/19`
  (Databricks dbt repo — DAG `keyword_ddp_reporting` located in airflow-ti; models not cloned locally).

## 4d15. WASTE tab + measured ingestion footprint (boss ask via user, 2026-07-15)

**Asks:** (1) how much delivered vendor data we throw away + what to request vendors stop
sending; (2) roughly what we pay to INGEST the data (infra, not data fees).

**New `waste` tab** (position 3; 20 rows x 10 sources; chart `q14_ingest_waste.png`;
measurement script `q14_gcs_ingest_bytes.sh`, MANIFEST row 30 — svs is GCS-partitioned by
data_source_id so bytes are directly measurable):
- **Volumes (measured):** paid vendors ship ~156 GB/day (~57 TB/yr); 33Across alone 106 GB/day
  (68%). Free logs: augmentor 79, guid 48.
- **Thrown away** (never reaches DS13 or DS19): 33Across 22.4% (8.7 TB/yr), 33A API 36.1%
  (3.8 TB/yr), Predactiv 9.6%, Sovrn 7.2%, others <5%.
- **USED-BUT-SHOULDN'T-BE (user-caught framing):** webmail (~29% of 33Across) and Googlebot
  (6.4%) are NOT in thrown-away — DS19 has no blocklist, so they pass the gate and BILL.
  The 33Across stop-sending ask (~35% volume cut) mostly targets junk we currently USE and
  PAY FOR, not the waste. Sovrn inverse caveat: only 7.2% thrown away BECAUSE the permissive
  DS19 gate accepts its malformed rows (90.9% DS19-categorized) — low waste != clean.
- **Ingestion cost, measurable floor:** svs has NO TTL (first partition 2025-08-31);
  accumulated paid-vendor footprint 39.3 TB (33Across 22.8 TB) -> storage floor ~$9.4K/yr
  at GCS list ($0.02/GB-mo). EXCLUDED (needs Data Eng): Kafka cluster share (RT vendors
  24/33/39/40), batch ingest DAG compute, DS13/DS19 classifier compute per TB. Draft ask
  sent to user for Sean Yang's team.
- **Stop-sending asks** (per vendor, on the tab): 33Across webmail+Googlebot; 33A API
  cookie-sync URLs; Sovrn URL-doubling fix; 5x5 outbrain widget URLs; Predactiv adult filter;
  Justuno none (cleanest; add user_agent); Klickly/Cybba immaterial volume.

## 4d13. Post-preemption economics — bills if free logs stopped paying for co-held data (user question 2026-07-15)

**Question:** we supposedly pay only for used data, but the meter doesn't exclude signals
guid_log/augmentor already capture (AUDI-1093). If we stopped paying for anything in those logs,
how much do bills drop — and do any vendors become worth paying for again?

**Answer (no new scans — q3c cohold shares × bills, cross-validated by q8a same-day-dup splits;
anchor: total == $273,671 == the published AUDI-1093 figure):**

| Vendor | Bill/yr | Cut | Bill AFTER | Pay portfolio (sole-T2) | Pay ceiling (solo meas.) | Ceiling worth/bill AFTER |
|---|---|---|---|---|---|---|
| 33Across | $422.0K | −$221.7K (52.5%) | $200.4K | $27–81K | $72–217K | **1.08x** |
| 33A API | $175.9K | −$41.9K (23.8%) | $134.0K | $6–18K | $45–134K | **1.0001x — exactly AT fair** |
| Sovrn | $115.9K | −$0.3K (0.2%) | $115.6K | $0–1K | $11–34K | 0.29x |
| Justuno | $77.1K | −$3.8K (4.9%) | $73.3K | $1–2K | $4–11K | 0.15x |
| Cybba | $21.5K | −$6.1K (28.2%) | $15.4K | $0–1K | $1–3K | 0.19x |
| **Roster** | **$812.4K** | **−$273.7K (33.7%)** | **$538.7K** | | | |

- **Bills drop $274K/yr (−33.7%) and we KEEP the data** — pay ranges are unchanged by
  construction (sole/solo value never included free-coheld signal). Visit grain = the fair
  version (vendor still credited for fresher dates); strict pair-grain is barely larger (~$284K
  proxy). Flats unaffected (no meter).
- **"Worth paying again?" — nobody flips on the portfolio lens** (0.007–0.40x pay-top vs bill).
  On the most generous lens (measured-solo ceiling, top of the 10–30% band): **33A API lands
  exactly AT fair ($134.0K == $134K)**, 33Across inside its range near the top (1.08x), the
  combined pair $334.4K vs ≤$117–351K (1.05x, sum overstates — cohorts overlap). **Preemption
  moves the 33Across deal from ~4x rich to ceiling-defensible; preemption + renegotiation STACK.**
- **Sovrn (−0.2%) and Justuno (−4.9%) are nearly untouched** — their bills aren't overlap-driven
  (junk/unique credit survives preemption). Preemption does NOT rescue them; verdicts unchanged.
- Landed in the workbook: numbers/solo row "Post-preemption bill $/yr (AUDI-1093 applied)"
  (144 rows now), decisions block 5 (per-meter + combined + roster, 3 pay-range lenses, flip
  verdicts), notes convention, chart `q12_post_preemption.png`. Anchor tripwire in fill_template
  (total must stay ~$273.7K; q8a-vs-q3c share convergence warns past 0.5% — currently silent).

## 4d12. SOLO sheet — each vendor as the ONLY paid source (user-requested 2026-07-14)

**Ask:** the numbers grid measures every vendor against all other sources, so paid-vendor
overlap depresses everyone's individual numbers; recompute the whole grid per vendor as if it
were the sole paid vendor, sharing IPs only with augmentor_log + guid_log — "a cleaner example
of their individual impact."

**Three counterfactuals now live in the workbook — don't conflate:**
1. **sole** (numbers sheet) = touched by NO other source among all 10 (the strictest cohort);
2. **solo** (new sheet) = touched by the vendor and by NEITHER free log — other paid vendors
   ignored (free-log columns: vs the OTHER free log; one formula `other_free(d)`);
3. **ladder standalone** (decisions sheet) = solo at pair grain with density-estimated $ — the
   solo sheet reproduces it as its DENSITY ESTIMATE row and adds the measured version.

**Implementation:** `solo` sheet inserted between numbers and notes — the same 143 rows/14
columns, built from SPEC via a `SOLO_OVERRIDE` annotation dict (no duplicate row list; length
and key-membership guards). Treatment mix: **75 copy** (feed scale/quality/funnel/touched-cohort
rows don't change under the counterfactual), **5 rebased shares** (share-of-column-total →
share of the {vendor+free} world), **12 mask-exact** (solo pairs/visit-days/HI/PP counts +
{free+vendor} coverage from the q3b/q3c/q3d 10-bit holder masks — LOO/portfolio rows replaced
by these), **6 bounded estimates** (solo bill LOW/HIGH, density-est revenue, worth/bill est),
**45 scan-fed** (measured solo-cohort serving/perf from two new queries; cells print
"pending scan (q8)" until the CSVs land — 450 pending cells in phase 1).

**New queries (MANIFEST rows 26-27):** `q8a_solo_stock.sql` (svs 30d + wcv/pc: solo usable
IPs — the one stock masks can't give at IP grain — solo domains/classified, freshness-vs-free
at pair grain [fresher/tied/stale on co-held pairs] and visit-day grain [new-pair / refresh /
same-day-dup vs free only]); `q8b_solo_perf.sql` (svs 37d raw membership + CIL week + clickpass
+ ui_conversions with q7c's dedup verbatim: solo-cohort served IPs/imps/media/T1 inputs/visits/
conversions + per-IP HI/PP tier counts for the mask cross-check). Both dry-run validated,
launched in parallel ~16:34.

**Solo bill = bounded estimate, not a quote:** LOW = today's run-rate (hard floor — removing
competitors only ADDS credit to the survivor); HIGH = max(LOW, total metered bills × vendor's
share of paid-held visit-days). First cut of HIGH came out BELOW today's bill for
Sovrn/Justuno/Cybba — their billed credit sits on junk domains outside the usable universe,
uncontested by other vendors, so the proportional model under-runs them and the bound clamps
to LOW. Only the 33Across feeds have real solo-bill upside: DS28 $422K→$456K (+8%),
DS40 $176K→$308K (+75%, it inherits credit currently lost to DS28 batch).

**Anchors:** mask solo pairs == q3 netnew_vs_free_pairs EXACT for all 8 paid vendors
(33Across 1,057,407,760 both sides); free-partition identity exact; phase-2 asserts (in
fill_template): q8a solo pairs == masks (<0.1% wcv/pc snapshot drift tolerated), q8b ≥ q6
sole everywhere (superset), q8b HI/PP == q3d mask solo counts (<0.5%; raw-vs-usable membership
gate — q3d↔q5 precedent ratio 1.000).

**Phase-1 spot values (density-est basis):** 33Across solo = 45.3% of its usable pairs,
solo HI 2,727 IPs (vs 92 sole-HI — the paid-overlap distortion the sheet strips), standalone
$397K est → pay range $40-119K vs $422-456K bill (0.94x revenue basis, unchanged conclusion);
Sovrn 99.7% pairs solo (junk-driven uniqueness) but $21K est vs $116K bill (0.18x). New chart
`q11_solo_pnl.png` (bill bracket vs measured/est T2_solo + pay band; measured markers fill in
when q8b lands).

**q8b MEASURED results (landed 2026-07-14 ~17:05; anchor 4 monotonicity passed all 10 sources):**

| Vendor | T2_solo measured | density est | bill LOW-HIGH | pay range 10-30% | worth/bill (rev basis) |
|---|---|---|---|---|---|
| 33Across | **$724K** | $397K | $422-456K | $72-217K | 1.59-1.72x |
| 33A API | **$447K** | $87K | $176-308K | $45-134K | 1.45-2.54x |
| Sovrn | $112K | $21K | $116K | $11-34K | 0.97x |
| Justuno | $37K | $8K | $77K | $4-11K | 0.48x |
| Cybba | $10K | $3K | $22K | $1-3K | 0.45x |
| 5x5 | $281K | $67K | flat pending | $28-84K | — |
| Predactiv | $202K | $37K | flat pending | $20-61K | — |
| Klickly | $13K | $2K | flat pending | $1-4K | — |

Three takeaways: (1) **measured T2_solo runs 3-5x ABOVE the density estimate everywhere** —
the estimate inherits sole-cohort adverse selection (dark households), while the solo cohort
includes livelier multi-paid-vendor IPs. The ladder's standalone $ are therefore FLOORS under
the solo lens. (2) **The decision conclusion survives the rehabilitation: no metered vendor's
10-30% PAY RANGE reaches its bill even measured** (33Across tops out $217K vs $422K+). The
33Across feeds now clear their bills on a REVENUE basis (1.6-2.5x) — expect the vendor to argue
that lens; the answer stays "we keep ~20% of the CPM." (3) **NON-ADDITIVITY: never sum solo
columns across vendors** — solo cohorts overlap heavily (the same multi-vendor IP is solo for
every vendor vs free logs); the ladder's marginal column is the only additive lens. Caveat:
T2_solo is a generous ceiling — the solo cohort's prospecting-attribution share is unmeasured
(sole cohort's was 97-99%; livelier solo IPs likely include more retargeting serves).

**Anchor-5 became a finding, not a check:** q8b tier counts (raw 37d membership, q5/q6
convention) vs q3d mask counts (usable-gated) differ DIAGNOSTICALLY — clean vendors read 3-10%
low in q8b (free-log webmail sightings only count raw), **Sovrn reads +55-68% HIGH** (its
malformed-URL rows carry IPs that never reach a usable domain — junk-carried "solo HI").
Converted from abort-threshold to always-printed comparison; both lenses stay on the sheet
(count row = usable/q3d, % rows = raw/q8b, each internally consistent).

**q8a landed (~18:20) — SOLO SHEET COMPLETE (pending 0 / empty 0, both grids).** Anchor 3:
solo pairs == q3b masks exact for 5 sources, worst drift +0.0058% (wcv/pc snapshot);
anchor 7 (fresh-day vs q3c masks) passed. Stock: 33Across 43.5M solo IPs (vs 30.8M sole,
+41% once paid overlap is ignored); Predactiv 294K solo classified domains (vs 227K sole —
still the breadth king). Freshness vs free on co-held pairs: 33Across fresher only 8.6% /
tied 76.9% / stale 14.5%; Klickly the outlier at 73.4% fresher (checkout timing, tiny scale).
**Convergence check passed:** visit-day same-day-dup-with-free per vendor (33Across 52.5%,
Cybba 28.2%) equals the AUDI-1093 per-vendor preemption shares exactly — two independent
routes to the same free-cohold quantity. Visit-day mix vs free: 33Across only 47.5% solo
(34.2% new-pair + 13.3% refresh); Sovrn/Justuno/Klickly 95%+ new-pair (uniqueness real at
visit grain — value low for other reasons: junk, scale, darkness).

## 4d11. q3d landed + reconciliation fixes + shareable query package (2026-07-13 eve - 07-14)

- **q3d results (charts q3d_score_coverage.png / q3d_vertical_impact.png; scenario table gained
  HI/PP columns; anchor = q5 exact):** scored audiences are VENDOR-INDEPENDENT — k=4 keeps
  **99.9991% of HI (exactly 53 of 5,959,159 HI IPs lost** = dropped vendors' sole-HI singletons
  36+8+3+1 plus 5 combination-only IPs); free-only keeps 99.94% HI / 99.25% PP. Vertical
  audience-size loss under free-only concentrates in ~10 ad-invisible verticals (health/personal
  care 0-26%, cosmetics/pharmacy ~25%, airlines 23% retained); k=4 ≥94% everywhere. User caught
  the 2-decimal display reading "100.00%" at k=4 → HI/PP columns now 3-decimal.
- **User-caught reconciliation bug in `saved()`:** drop-ALL-metered recovery showed $805,161 vs
  $812,397 bills — the metered-to-metered deduction was applied even when the destination meters
  were dropped too. Fixed (deduction only while a metered destination survives); boundary identity
  now holds exactly: drop-every-meter recovery == total metered bills.
- **SCORE QUALITY gained sole-cohort rows** (HI/PP counts + % on vendor-unique served IPs; rows
  92-95) — the per-vendor decomposition of scenario HI losses. Workbook now 143 rows.
- **Shareable validation package: `runbook/queries/MANIFEST.md`** — all 25 queries in run order
  mapped to workbook sections/charts, standalone bq instructions (no workspace tooling needed),
  computed-row formulas, and independent validation anchors. Gaps closed: q0b output saved;
  **v01_visits_source_validation.sql** (ui_visits dedup == clickpass +0.5%) and
  **v02_conversion_model_fanout.sql** (per-model row fan-out justifying q7c dedup) saved + executed.
- Housekeeping: bq_perf_log.jsonl slimmed 52.6→7.5MB (per-second timeline + plan steps stripped);
  bq_run.sh now logs compact records and auto-rotates at 40MB to knowledge/archive/.

## 4d10. Fangorn migration reframes the endgame (Matt Brorby, Slack 2026-07-13 3:49 PM)

Matt confirms: (1) vendor removal has NO impact on Fangorn; (2) **most active advertisers are
ALREADY migrated to DS46**; (3) no hesitation forcing the remaining DS13 advertisers to DS46 —
Alex is working solutions for the tail. Since DS13 XOR DS46 per audience and DS46 = guid-only
(then aug+guid feature store), **the vendor-dependent DS13 vertical-audience path is actively
being sunset platform-wide.** User's take: "not seeing any reason to keep any vendors for MM."
**Structural caveat for the memo: DS19 (MM Core) still consumes vendor svs** — DS13→DS46 kills
the vertical-audience dependency, not the MM-Core one; vendor coverage loss would still shrink
DS19 audience sizes (39.6% of usable pairs are vendor-only). But the measured serve-revenue
dependency (T2, which already includes DS19-driven serves) is tiny, so the remaining question is
pure reach/audience-size (q3d in flight). **Tuesday framing upgrade: we are paying $812K/yr of
meters largely for a targeting system MNTN is actively migrating off of. Ask Alex for the DS13
tail-migration timeline → schedule the vendor sunset to that date; interim = AUDI-1093 preemption
+ 33Across repricing.**

## 4d9. Roster P&L sent to leadership (2026-07-13)

User sent boss the ladder's Bill | Standalone revenue | Pay range (10-30%) block with the two
punchlines: (1) no vendor generates more kept-margin value than it costs; (2) no metered vendor
generates more than its bill EVEN at full-CPM revenue (33Across $397K < $422K, best case).
Pushback-prep documented: density-extrapolation caveat (measured-only fallback: 33Across $270K
observed < $422K still wins), two-axis breadth caveat for flats (Predactiv/5x5 = classifier
coverage + HEM, don't kill on this lens), reach/option value excluded (needs 6-17x to close gap),
annualization bases (week x52, June x12, single-credit regime verified).

## 4d8. Slack thread outcomes (#? — "Removing aug_log/guid_log from IPs we pay for", 2026-07-13)

Sean Yang + Alyson + Matt Brorby engaged on the preemption finding: (1) Sean CONFIRMS svs feeds
DS13/DS19 only; both free logs are in targeted_signal → the AUDI-1093 exclusion is implementable
there; **Sherwin = crediting/billing contact**; Sean endorses adding the exclusion logic.
(2) **Matt Brorby: DS46 Fangorn uses guid_log ONLY today; post-retrain it moves to feature-store
data from aug_log + guid_log** — augmentor's role expands (relevant to AUDI-1091 value case).
(3) Alyson proposes a self-test validation (visit a customer site, trace guid_log vs vendor feeds).
(4) NUMBER HYGIENE for the thread: "cut our bill by 60%" conflates pair COVERAGE (60.4%) with
credit share — the exact preemption recovery is **$273.7K/yr = ~34% of the $812K metered bills**
(per-vendor: 33Across 52.5% of its bill, 33A API 23.8%, Cybba 28.2%, Justuno 4.9%, Sovrn 0.2%) —
because vendor credit concentrates precisely on the signals free logs DON'T co-hold.

## 4d7. Roster P&L at kept-margin + the two-axis flat-vendor tension (2026-07-13, late)

Ladder now shows Bill | STANDALONE | MARGINAL side by side with **PAY RANGE columns at the 10-30%
blended margin** (user's range; avg ~20%). The revenue columns are FULL CPM flowing through us;
PAY RANGE = what we keep = the negotiating number. **Roster P&L: metered bills ~$812K/yr buy
~$470K of dependent revenue = $47-141K of kept margin → net -$670 to -$765K/yr at current prices.
One-liner for the memo: "we pay data costs as if we kept 100% of the CPM, but we keep ~20%."**
No metered vendor covers its bill from kept margin — even 33Across standalone keeps $40-119K vs
$422K paid. **User independently replicated the standalone test and reached the same conclusion**
(all vendors under water) — methods convergence, cite in the room.
**Two-axis tension for FLAT vendors (do NOT let dependent-revenue alone kill them):**
dependent-revenue break-even flat fees (5x5 $6.7-20K, Predactiv $3.7-11K, Klickly $0.2-0.7K) vs
domain-axis WTP bands ($150-600K / $0.7-3M / $0.1-1.5K) disagree 30-300x for the breadth vendors —
because their unique contribution is CLASSIFIER DOMAIN COVERAGE (Predactiv 226.8K sole classified;
69% of its IPs same-day duplicated), infrastructure value that never appears as sole-serve revenue.
TI-1027 two-axes lesson at full strength. Tuesday framing = three tiers: (1) metered priced
5-150x above margin-adjusted value → drops + AUDI-1093 preemption + cap 33Across-combined at
~$60-140K; (2) flats = two-axis call, easy renew if fees near revenue break-even, else price as
classifier coverage explicitly; (3) system fix: the $0.50 meter rate assumes we keep the full CPM.

## 4d6. Scale-normalized (same-scale hypothetical) per-unit ranking (2026-07-13, user q)

Standalone $ per 1M delivered usable pairs (= netnew-vs-free rate x T2 density): **Sovrn $255 /
Cybba $247 / 33Across $170 / Klickly $167 / 5x5 $91 / Justuno $89 / 33A API $73 / Predactiv $72.**
Reads: (1) **Sovrn's #1 is an ARTIFACT** — 99.7% "netnew" is malformation-driven uniqueness (181
sole classified domains only) + Poisson-noisy density ($51/wk media). (2) **Cybba's #2 is REAL** —
70% netnew, highest genuine density, clean feed, 362 sole classified on 8x less volume than Sovrn:
the one DROP that scaling could flip → ask added: keep if they 50-100x at flat/capped price.
(3) 33Across has top-tier per-unit AND scale — why it wins the ladder. (4) Predactiv ranks LAST at
pair grain but its axis is domain breadth (226.8K sole classified, 33x anyone) — two value axes
rank differently (TI-1027 lesson holds). **Scaling viability asymmetry: flat vendors = free upside
(ask 5x5/Klickly/Predactiv for more data, costs $0); metered vendors = scaling multiplies the LOSS
at $0.50 (max justified used-imp CPM ~$0.10) — only accretive with repricing.** Densities revert as
vendors scale (marginal pairs overlap more) — the hypothetical is an upper bound. Workbook: new
scale-normalized row in ECONOMICS-WORTH.

## 4d5. Net-of-free value ladder (2026-07-13, user-requested) — decisions sheet table 3

Universe first drops EVERY pair guid_log/augmentor touch → **2.37B pairs remain (39.6% of usable)**;
all 2^8 paid-vendor subsets evaluated exactly on that universe (optima nested → clean add-order
ladder). $ = pairs x each vendor's measured T2-per-sole-pair density (assumption disclosed: marginal
pairs perform like its current sole pairs). Findings:
- **Standalone (as the ONLY paid vendor): 33Across covers 44.7% of the net-of-free universe, worth
  ~$397K/yr vs its $422K bill → 0.94x — near break-even as sole provider.** Everyone else standalone
  is far under bill: 33A API $87K/0.50x, Sovrn $21K/0.18x, Justuno $7.8K/0.10x, Cybba $2.9K/0.13x.
- **Marginal ladder (value AT each roster position):** #1 33Across $397K → #2 33A API $59K (0.34x
  bill) → #3 5x5 $62K (flat) → #4 Predactiv $18K (flat) → #5 Justuno $7.5K (0.10x) → #6 Klickly
  $2.2K → #7 Sovrn $2.7K (0.02x) → #8 Cybba $2.8K (0.13x).
- **Recency robustness check (user q, 2026-07-13): the ladder now carries BOTH valuations —
  pair-density and visit-day-density (the refresh-crediting lens). They agree within 6% at every
  step (33Across $397K pair vs $375K visit-day; all others within ~1%), because vendors rarely beat
  augmentor to a fresh date (free visit-grain coverage 59.4% AFTER crediting vendors all their
  fresher days). Conclusions are grain-robust.**
- **Reading: no metered vendor is worth its bill at any position after #1.** The ladder is the
  negotiation sheet: pay each vendor at most its marginal value at its position; the only near-fair
  contract on the roster is 33Across-as-primary — and only if the rest are dropped or repriced.

## 4d4. q3c visit-grain uniqueness LANDED (2026-07-13) — the (ip x domain x DATE) decomposition

13.29B unique visit-days over 30d (usable). Validation anchor exact (vendor rows = mask holders).
**Free coverage at VISIT grain: guid 10.7% / augmentor 48.8% / both 59.4%** (vs 60.4% pair grain —
refreshes barely move the floor). **Augmentor DS30 is the dominant free source** and is included in
every free-log number (user q; meeting's "guid only" line was a misstatement). Per-vendor visit-day
split (new-pair / refresh / same-day-dup): 33Across 20.0/14.2/**65.9** — two-thirds of its feed is
same-day duplicated; Sovrn 11.2/4.9/**83.9** (worst); Predactiv 18.8/11.6/69.5; 33A API
**58.7**/1.6/39.7 (RT feed = genuinely new pairs); Klickly **94.2**/5.2/0.5 and Justuno
**90.7**/1.3/8.0 (cleanest uniqueness, tiny scale). 33Across's recency-refresh value is real but
modest (14.2%) — doesn't overturn NEGOTIATE. **Exact AUDI-1093 preemption recoverable at visit
grain: $273.7K/yr** (33Across $221.7K, 33A API $41.9K, Cybba $6.1K, Justuno $3.8K, Sovrn $0.3K) —
pair proxy $284K held up. Workbook now 137 rows (visit-day rows + exact preemption rows) + 9
scenarios (added "augmentor only" 46.4% and "guid only" 14.2% pair-coverage rows).

## 4d3. Free-log credit preemption gap (Sean Yang, 2026-07-13) — AUDI-1093

Sean Yang confirms DS30 augmentor_log IS implemented in svs — but the meter still awards paid
vendors credit for (ip x domain x date) signals our own free logs (DS23 guid, DS30 augmentor) also
capture: **we pay for data we already have.** Mechanism: first-reporter timing only displaces a
vendor when the free log reports FIRST, and day-grain credit hands vendors fresh credit for new
dates on pairs we already track. **Rough recoverable if free sources preempt paid credit
(pair-grain proxy from q3b free-cohold shares x June bills): ≈$284K/yr** — 33Across $224K (53%
free-cohold), 33A API $50K (28.5%), Cybba $6.2K, Justuno $3.9K, Sovrn ~$0.3K. Exact triple-grain
number comes from q3c. Filed **AUDI-1093** (confirm dbt behavior, quantify exactly, spec
free-preemption rule for the owning team). Note interplay with drops: preemption and dropping are
substitutes on the overlap slice — preemption recovers the free-cohold share while KEEPING the
vendor's unique contribution.

## 4e. Team review meeting 2026-07-13 (meetings/audi_1089_01_discuss_vendor_value_quality_2026_07_13.txt)

Walked the team (Alex, Allison +) through the valuation model and the workbook. Outcomes:
- **Klickly answer APPROVED for Paulo today: drop unless ~free** ("if they're only contributing <1%
  of the IPs... that's fine"). Requested sanity check: targeted_signal — how often does Klickly get
  SOLE credit vs fractional (if always fractional alongside others, overlap conclusion validated).
- **BILLING MODEL CHALLENGE — possibly FRACTIONAL, not first-reporter-wins.** Teammate: credit
  "moved from whoever is cheapest... to everybody gets a piece of the pie" — split across ALL
  vendors contributing the signal behind an impression (e.g. 3-way split). **Corroborating evidence
  found post-meeting:** (a) usage_reporting_data per-vendor impressions are DECIMAL (Justuno June
  19,528,654.53 — impossible under integer first-reporter counting); (b) coredw.usage_reporting_audits
  carries ONE platform-wide impressions total per month (1.41B for 2025-05) identical across vendor
  rows, with only usage $ differing — consistent with per-vendor fractional shares x $0.50.
  **Impact if fractional:** q3b LOO drop-savings change — under a fixed split pie, dropping a vendor
  saves only its SOLE-credit fraction (shared fractions redistribute to surviving contributors);
  whether free/flat sources count in the divisor determines how much redistributes to meters.
  Sovrn's savings could revert toward the v1 ~$14.5K figure instead of $109K. **Klickly verdict
  unaffected (flat fee, tiny value under any model). 33Across/Sovrn drop-savings numbers are
  model-dependent — VERIFY BEFORE TUESDAY.** Verification path: targeted_signal row grain (Athena
  only) or the matched-reporting pipeline code; mntn_matched_reporting is NOT in BQ (checked —
  coredw has only usage_reporting_data + usage_reporting_audits). Question drafted for Victor/Ryan. **Filed as AUDI-1092** (verify credit model in targeted_signal,
  recompute sole-credit shares, re-derive drop savings, reconcile vs June bills).
- **Augmentor display rows = site visits with URLs** (Alex) — a bigger free source than the current
  svs augmentor subset; integration ≈ a couple sprints (Data Eng). **Filed as AUDI-1091** (spike:
  quantify full augmentor vs DS30, vendor displacement estimate, ingestion scope).
- **Klickly Shopify nuance (Alex):** myshopify URLs are DISTINCT storefronts, each categorized
  separately (shoe store vs dress store) — softens the "94% one domain" concentration critique;
  matches our 117 reg-domains / 629 distinct hosts finding. Doesn't change the verdict (scale+value).
- **IP x URL grain point (Alex):** IP-level overlap understates net-add (same IP, different URLs =
  new scoring signal) — CONFIRMED already handled: q3/q3b coverage is (ip,domain)-pair grain.
- **Margin/CPM color:** upcharge 10-30%, large clients ~10%, average ~20%; blended media CPM $10-30
  leaning ~$10 — supports the 15/20/30% ladder with fair = T2 x 20%.
- **LiveRamp site-visit question:** LiveRamp = aggregator/marketplace of partner segments; likely no
  direct raw site-visit product (their PARTNERS hold the visit data). ShareThis-the-3P-vendor is the
  same company as sharethis_predactiv's ShareThis half. deepsync = CRM.
- **Domain-type value follow-up:** differentiate junk browsing (yahoo articles) vs commercial pages
  (shopify checkout) when valuing unique domains lost per vendor.
- **"Drop everything" boundary case:** all-vendors-dropped saves ~$800K/yr contracts vs ~$416K/yr
  dependent value on paper, BUT costs ~40% of MM's targetable IPs — reach/liveness value isn't in
  the dependent-revenue number; not recommended, framing only.
- **Ryan's recommended trace = runbook steps 4-6** (svs → classified (wcv) → scored/delivered): measure
  per-vendor survival through the DS13/DS19 consumers, not raw-feed junk. Next runbook session (q3/q4)
  should add the consumption funnel: raw rows → parseable → survives consumer filters → classified → scored.

## 4d. Billing hard logic (AUDI-647 + AP-3779 deep-read, 2026-07-10)

- **Credit rule (Victor via Ryan):** first DDP to report an (ip, url) per day gets the credit, paid only
  if used for targeting. Row-level used table: `data_archive_prod.targeted_signal` (**Athena only** —
  uid/ip/dsc_id/time/data_source_id/**source_data_source_id**/dt). Chain: svs → targeted_signal →
  mntn_matched_reporting → usage_reporting_data → Maya Triman pays monthly.
- **Augmentor displacement confirmed:** DS30 entered svs 2026-05-07/12 (airflow-ti git). June = first
  fully-displaced month → 33Across bill −$19K vs May, 33A API −$9.7K (Ryan's AUDI-647 estimates: $17K/$4K).
  33Across's declining bill is partly OUR doing, not their volume.
- **AUDI-647 method** (for re-use): match svs to augmentor_log by ip + canonical page/referrer, query-string
  stripped; grain ip × composite_key × day.
- **Meter cannot split DS13 vs DS19 (verified):** data_source_category_id NULL on all 33Across June rows —
  the consumer-level decomposition of billed usage is Athena-only. Draft ask to Victor ready (see chat
  2026-07-10): one-time targeted_signal aggregate by source_data_source_id × data_source_id for June.
- **Open questions:** (1) targeted_signal needs Athena access (or Data Eng MCP / Victor) — would give exact
  per-vendor used-row counts + definitively answer the yahoo/DS19 question via source_data_source_id;
  (2) flat fees (5x5/Predactiv/Klickly) still unknown — Maya Triman has the payout schedule;
  (3) how 1/N impression decimals interact with first-reporter-wins; (4) DS33/39/40 svs ingestion path
  (not in ENABLED_DSIDS). Sensitive: "Monthly Summary by DDP.xlsx" stays local/gitignored, never on Jira.

## 5. Constraints & context (from the Slack thread, 2026-07-09)

- **Take rates sensitive/private (ray):** shareable artifacts use base cost only — media_spend
  (advertiser-agnostic, inventory/deal-based) + data_spend. No platform_spend / billed / take-rate math
  in anything shared.
- **Performance over cost as the value metric (ray, 2nd reply — adopted):** "we get paid no matter what…
  the value is whether the end-to-end generated value to the customer, so that they keep paying us —
  lean towards the performance metrics, not the raw/net costs." → the per-vendor case LEADS with
  performance (VR of impressions to vendor-sole IPs vs same-score-band multi-source IPs); the media/data
  cost lenses remain as the willingness-to-pay anchor ("how much would we pay for these IPs"), not the
  headline. Quality ≈ does the vendor's unique signal find IPs that perform.
- **Paulo:** believes payment is "waterfall style on usage basis" — registry says Klickly `flat_fee`;
  reconcile against the renewal schedule when it lands. He'll act on our recs, including keep-alls.
  Redundancy may have validity value (his point) → priced via the recency tied-share (coverage-if-down).
- **Ryan Kleck (via Matt Brorby):** DDP-vs-augmentor_log redundancy — 33Across ~38.6% IP+URL match,
  33Across API ~13.5% (corroboration for those two verdicts; AUDI-647).
- **ID-164** (Identity: toxic-hub IP scoring, PR open, Jack Barbey/elena-tpm): overlaps the per-IP value idea —
  follow-up connection after the renewal wave, not in scope here.

## 6. Investigation & Findings

*(per-vendor findings live in each vendor subfolder's summary.md; cross-vendor results summarized here)*

### Cross-vendor 30d outputs (2026-07-09) — reusable by all remaining evals
All in `outputs/` (all 10 DS): `audi_1089_scale_by_day_30d.csv`, `window_reach_30d`, `recency_pairs_30d`,
`uniqueness_domains_30d`, `score_tiers_sole_vs_touched`, `value_tiers`, `check_scored_no_svs`,
`vr_by_membership` (Klickly-focal), `vr_sole_by_ds`. Windows: svs 06-02→07-01 (30d), CIL week 07-02→07-08,
soleness on the 37d union. **Method validated: DS25 recency 69.3% sole vs TI-1027's 69.8% ✓.**

Cross-vendor early reads (for the remaining six):
- **Sole-IP delivered value is small for EVERY vendor** (even 5x5: 19.6K sole delivered IPs, 94.8% unscored)
  — re-confirms TI-1027: vendor value = domain→vertical coverage, not IP reach.
- Sole classified domains (30d): Predactiv 226.8K > 5x5 86.1K > augmentor 47.5K > Justuno 4.6K > 33Across 6.8K
  > 33Across API 2.8K > Cybba 362 > Sovrn 181 > **Klickly 126**.
- Sovrn is 80.1% tied (redundant-but-fresh) — its "insurance" framing case; 33Across carries the largest pair
  base (2.3B) at 30.8% sole — bigger than the 7d picture suggested (worth care in its eval).
- **Justuno is 19.6% IPv6** — the IPv4-only method materially undercounts it; flag for its eval.
- Check A: svs signal is necessary for scoring (r=0.05%) — strengthens every vendor's T1 logic.

### THE MONEY FINDING (2026-07-10): actual metered bills exist and are queryable
`dw-main-bronze.coredw.usage_reporting_data` holds the DDP waterfall meter — **usage = impressions × $0.0005
exactly** ($0.50 CPM confirmed), credited on MM-targeted serves via targeted_signal DS13/19 with a 30d lookback,
**1/N split across co-matching vendors** (we pay per-use on SHARED IPs — Paulo's "waterfall on usage basis"
confirmed). Monthly per-domain reports emailed to each vendor from partnerbilling@ (`bae-sql-utility`
ddpmonthlyusageemail-*.py). Actuals (`outputs/audi_1089_metered_usage_by_month.csv`):

| Vendor | Apr '26 | May '26 | Jun '26 | Jun run-rate | Defensible band | Position |
|---|---:|---:|---:|---:|---|---|
| Justuno 24 | $8,571 | $7,833 | $6,426 | ~$77K/yr | $14-60K/yr | ~1.3× over top — trim |
| 33Across 28 | $55,163 | $54,241 | $35,169 | ~$422K/yr | $30-100K/yr | **4-7× over** |
| Sovrn 33 | $13,272 | $10,324 | $9,657 | ~$116K/yr | $0.5-2.4K/yr | **~50-200× over** |
| Cybba 36 | $1,872 | $1,814 | $1,792 | ~$21.5K/yr | $1.1-4.7K/yr | ~5-20× over |
| 33Across API 40 | $27,586 | $24,415 | $14,657 | ~$176K/yr | $10-40K/yr | ~4-18× over |
| **CPM total** | **$106.5K/mo** | **$98.6K/mo** | **$67.7K/mo** | **~$812K/yr** | | |

**Savings case: dropping Sovrn + Cybba + 33Across API ≈ $313K/yr run-rate against ≤$47K/yr of defensible
value; renegotiating 33Across to its band saves another ~$320K+/yr.** (Bills declining Apr→Jun — meter tracks
MM-targeted delivery; annualizations use Jun run-rate, the conservative end.) Flat-fee vendors (5x5, Predactiv,
Klickly) are NOT in this table — their amounts still must come from Paulo's renewal schedule.

### Cross-vendor corrections captured (from the six lineage sweeps)
- **Predactiv HEM correction to TI-1027:** DS26 hashed emails are NOT dropped — `hashed_email_ds_26_signals`
  (hourly, severity-1) feeds hashed_email_signal → CRM/identity resolution. Predactiv is the only site-visit DDP
  with a hard non-MM production dependency.
- **Justuno ingest:** now a dedicated hourly S3 file-drop (s3://mntn-data-partner-justuno → EMR → fpa_vendor_log),
  not the pixel topic (legacy). Off-switch = pause justuno_dsid24_ingestion DAG.
- **Sovrn (FMX) is separately a PMP inventory vendor** (gary-ql core.partners id 68) — dropping the DS33 data
  feed does not touch inventory deals; don't conflate in the renewal conversation.
- **33Across device-ID side-feed (DS28)** exists only in the legacy AWS stack (unscheduled) — no GCP consumer.

### Scan-recovery note (methodology)
The 9 scan jobs (15-158 min) outlived their client shells; results were recovered from BQ's 24h anonymous
destination tables via `bq show -j <id>` → `SELECT * FROM` the anon table (5 jobs were in location
us-central1 — `bq show -j` needs `--location` for those). No re-scan needed.

## 7. Data Documentation Updates

*(running list)*

## 8. Open Items

- Renewal schedule + per-vendor fees ← Paulo (verdicts are bands until then).
- Klickly "waterfall usage" vs registry `flat_fee` — reconcile.
- Does scoring's event ingest exclude DS23 like the classification consumer? ← Sean/Ryan (run soleness both ways if unanswered).
- 5x5 contract: data still flowing past end-of-June contract end with no recorded renewal — confirm signed (flagged in TI-1027).
