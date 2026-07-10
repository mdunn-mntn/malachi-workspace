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
  (98% top-5; 117 distinct domains/hr — niche by design, reg-domain collapses store subdomains).
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
