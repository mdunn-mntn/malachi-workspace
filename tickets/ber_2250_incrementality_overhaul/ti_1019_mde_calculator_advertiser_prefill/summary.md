# TI-1019: MDE Calculator — Per-Advertiser Auto-Prefill

**Jira:** [TI-1019](https://mntn.atlassian.net/browse/TI-1019)
**Status:** Done (2026-06-04)
**Date Started:** 2026-06-04
**Date Completed:** 2026-06-04
**Story Points:** 2
**Assignee:** Malachi

**Live URL (secret gist, share via link only):**
https://gist.githack.com/mdunn-mntn/2d362849df017fa243eef03bb61cdfbb/raw/ti_xxx_mde_calculator_prefill.html

> Filename inside artifacts/ stays `ti_xxx_mde_calculator_prefill.html` (not renamed to ti_1019) so the gist URL above stays valid for everyone the link was already shared with. Folder renamed to `ti_1019_...` per workspace convention.

---

## 1. Introduction
Extension of the TI-884 / workshop MDE calculator (`tickets/ti_xxx_power_analysis_workshop/`). Today the calculator uses cohort-median defaults (CPM $24.84, imps/IP 3.5, baseline IVR 2.15%). This ticket adds a per-advertiser picker that auto-populates baseline rate, CPM, imps/IP, and the monthly budget from each advertiser's actual trailing-30d performance plus a "typical active-month spend" derived from the last 12 months of history. Result: zero data entry to screen any live MNTN advertiser.

Parallel work: Chris Franz's gary-ql PR #4445 adds the same prefill server-side (`Advertiser.mdeInputs` resolver + `IncrementalityExperiment.forecasted_mde_percent`) for the premier-ui wizard. This ticket delivers the standalone internal-team version that doesn't depend on the wizard shipping.

## 2. The Problem
Cohort defaults give a generic answer. The actual question the team asks before any experiment is "is **this advertiser** big enough to power a test at our target MDE?" — which needs their actual CPM, household reach, and baseline rate. Today that's a multi-query manual pull every time.

Additionally, trailing-30d-only spend misrepresents on-off advertisers (seasonal, campaign-bursted). Need a "typical active-month spend" alongside the rolling 30d to pick the right budget baseline.

## 3. Plan of Action
1. Pull per-advertiser metrics for all currently-delivering advertisers (had spend in last 7d): trailing-30d spend / impressions / distinct-IPs / visits / conversions, plus median spend across active months (>$1k) over last 12 months
2. Export as JSON keyed by advertiser_id
3. Clone the workshop calculator, add an advertiser picker (search by ID or name) that prefills budget + baseline + CPM + imps/IP + IVR/CVR toggle
4. Test in browser against known advertisers (WGU, Ferguson, Ownerly, etc.) — sanity-check against TI-884 top-50 CSV
5. Commit + share

## 4. Investigation & Findings
- **`agg__daily_sum_by_campaign` is stale by >1 month.** Max `day` = 2026-04-30 as of run (2026-06-04). All "recent" metrics had to source from raw event logs instead. Aggregate is still fine for trailing-12mo monthly-spend patterns where the pattern matters more than the most-recent month.
- **879 currently-delivering advertisers** (trailing-30d spend >$1k). Top: WGU $1.85M, Zazzle $399k, Ancient Nutrition $386k, Ferguson $231k, Gainbridge $217k.
- Cohort medians match TI-884: CPM ~$5-9, imps/IP ~5-25 (varies widely by advertiser).
- WGU sanity check (Apr 2026 vs trailing-30d as of 2026-06-04): visit-rate 9.66% → 10.32%, imps/IP 22.4 → 24.7, CPM both ~$4-9. Drift consistent with month-over-month variation.
- Query cost: 95.5 GB scan, ~$0.48.

## 5. Solution
**Files:**
- `queries/ti_xxx_advertiser_prefill_metrics.sql` — pulls per-advertiser trailing-30d signal params + 12mo monthly-spend pattern. 95.5 GB scan.
- `outputs/ti_xxx_advertiser_prefill_data.json` — raw bq output (474 KB, 879 rows).
- `outputs/ti_xxx_advertiser_prefill_compact.json` — compact embeddable JSON (167 KB).
- `artifacts/ti_xxx_mde_calculator_prefill.html` — calculator with embedded JSON + advertiser picker.

**Calculator features added on top of Matt's UI port (which itself ports TI-884):**
- **Advertiser search box** (top of controls panel): type name OR ID, click to select. Enter selects top match.
- **Auto-prefills four signal-parameter fields**: baseline rate (follows IVR/CVR toggle), CPM, imps/IP. Plus monthly budget.
- **Budget basis toggle**: TYPICAL ACTIVE MONTH (default — median of months >$1k over last 12mo) / TRAILING 30D / PEAK MONTH. Right choice depends on whether the advertiser is steady, ramping, or seasonal.
- **Loaded-advertiser pane**: shows the 6 prefilled stats so you can sanity-check what got applied.
- **CLEAR button**: restores cohort defaults ($24.84 CPM, 3.5 imps/IP, 2.15% IVR / 0.054% CVR).
- IVR/CVR toggle now re-pulls the advertiser's actual baseline for the selected outcome when one is loaded; falls back to cohort defaults when none.

## 6. Questions Answered
- **Q:** Can we source the aggregate for trailing-30d?
  **A:** No — `agg__daily_sum_by_campaign` is >1 month stale. Use `cost_impression_log` / `clickpass_log` / `ui_conversions` directly. Aggregate still fine for 12mo monthly patterns.
- **Q:** Why 879 advertisers and not the ~2-3K we expected?
  **A:** The >$1k trailing-30d threshold filters out the long tail of inactive/very-small advertisers. Without the filter, count would be much higher but most have insufficient data to give a meaningful baseline.

## 7. Data Documentation Updates
- `knowledge/data_catalog.md` — adding `agg__daily_sum_by_campaign` staleness note (>1mo behind).

## 7b. Baseline-definition verification (graph.visits vs distinct visiting IPs) — 2026-06-24

Chris Franz's proposed UI baseline `IVR = graph.visits / usersReached` is **wrong for the binomial power calc** because `graph.visits` is an *event count*, not a per-IP probability.

- `graph.visits` = event count of Verified Visits (data_catalog.md). `graph.SiteVisitors` is the distinct-IP metric. The MDE engine's unit of analysis is the advertised IP (one Bernoulli trial). The numerator must be **distinct visiting-and-served IPs**, not events.
- WGU (AID 31357), trailing 30d: distinct served IPs = 15,732,160. Distinct visiting&served IPs = 1,683,382 → **correct IVR = 10.70%**. graph.visits = 5,656,104 → naive = 35.95%. **Inflation = 3.36x** (= 2.92 visit events per visiting IP × 1.15 from visiting-but-not-served IPs in the event count). Confirmed.
- p=0.3595 is also numerically invalid as a per-IP probability anchored to that denominator — it would only be a valid proportion if every visiting IP visited once.

**MDE direction:** MDE_rel ∝ sqrt((1−p)/p), monotonically decreasing in p. Inflating the baseline (0.1070 → 0.3595) **shrinks** the reported MDE.
- sqrt((1−0.1070)/0.1070) = 2.8889; sqrt((1−0.3595)/0.3595) = 1.3348.
- MDE_rel(0.1070)/MDE_rel(0.3595) = **2.1643**. Equivalently the naive baseline reports an MDE **2.16x smaller** than reality (WGU at 90/10 split: 0.314% vs 0.680%).
- Net: the naive `graph.visits` baseline is **over-optimistic** — it understates the true MDE and overstates statistical power. Claim confirmed on both counts (3.36x inflation, optimistic MDE).

**Recommendation:** UI prefill must use distinct visiting-and-served IPs / distinct served IPs (the ti_1019 `p_visit = visiting_ips_30d / distinct_ips_30d` definition is correct). If Chris's resolver pulls `graph.visits`, switch it to `graph.SiteVisitors`-style distinct-IP-on-served logic before shipping.

### 7c. Answer sent to Chris's two questions (2026-06-24)

**Q1 — switch UI baseline to per-advertised-IP IVR? → Yes** (binomial unit = advertised IP; IVR is the headline KPI, conversions usually underpowered).

**Q2 — denominator nuance? → Denominator already matches; fix the numerator.**
- `usersReached` ≈ our distinct served IPs (his imps/IP 24.7 ≈ our 22.5 → same raw-IP grain). Confirm: distinct IPs with ≥1 impression, deduped, trailing 30d.
- Numerator = distinct visiting ∩ served IPs (see 7b). `graph.SiteVisitors` without the served intersection → ~12.3% for WGU (1.94M/15.73M) vs our 10.70% — still ~15% high; intersect for exact parity.

Additional caveats flagged (beyond 7b):
- **Grain parity** — keep numerator + denominator at the same grain. If `usersReached` is ever household/graph-deduped, the rate shifts (cf. TI-1044: 2.83% same-IP overlap → IP ≠ household).
- **var_reduction parity** — resolver uses 1.0/raw; standalone shows raw + 0.595 post-stack. Show raw-only in the UI, labeled, so buyers don't compare raw-UI to post-stack-team numbers.
- **Incrementality semantics (deeper, both tools)** — served-arm clickpass IVR is the *observed* exposed rate, not the holdout/unexposed baseline an incrementality test measures lift against (clickpass ≈ 0 for never-served holdouts). For an honest incrementality `p`, use holdout total-traffic rate (TI-835: ~0% total-traffic lift → holdout ≈ served). Methodology alignment, not a matching blocker.

Verification: 3-agent adversarial workflow (MDE math / recommendation red-team / citation audit) — all 6 source citations confirmed, MDE direction confirmed.

### 7d. R2 column trace — denominator does NOT match (supersedes 7c's "denominator already matches", 2026-06-24) — ⚠️ IP-FIELD CLAIM ("device_ip") SUPERSEDED BY 7g

Chris confirmed what R2 can actually pull (Graph table, same trailing-30d call as CPM/imps-per-IP): `graph.sitevisitors` + `graph.usersreached`, proposed baseline = `sitevisitors/usersreached`. He couldn't see the upstream definition of "Households Reached." Traced it:

- These = `summarydata.all_facts.site_visitors`/`uniques` (HLL). `uniques` is keyed on **raw `device_ip`** (lineage: PR #1033 — null `device_ip` broke `uniques_arr`; IPv6 in `device_ipv6` dropped). **NOT identity-graph households** despite the label.
- **WGU trailing-30d (BQ):** `uniques`=32.1M, `site_visitors`=1.90M → platform IVR **5.92%**. Our `cost_impression_log`: distinct `ip`=15.74M (= `partner_ip` exactly) over 355.8M impressions → IVR **10.70%**.
- 2× denominator: graph `uniques` = 32.1M vs `cost_impression_log.ip` = 15.74M. **Correction (7f):** this is NOT a raw→resolved IP collapse (initial guess) — `ip` = `ip_raw` in both clickpass and impression_log. It's a cross-table difference (different impression universes/keys): CIL 356M rows→15.7M `ip`, impression_log 935M rows→21.2M `ip`, impression_facts→32.1M `device_ip`. See 7f.

**Implications:**
- `sitevisitors/usersreached` (5.92%) ≈ **½** our calculator (10.70%) → would ~double the reported MDE. NOT parity. The 7c "denominator already matches" (inferred from imps/IP ≈ 24) was wrong — the real R2 column is a different IP field.
- Likely also internally grain-mismatched: numerators ~equal (1.90M ≈ our 1.94M) but denominators differ 2× → `site_visitors` looks resolved-IP-grained, `uniques` raw-device_ip. Confirm how `site_visitors` is keyed.
- **Twist:** "Households Reached" is the *rawer* count; our `ip` is the more-collapsed one — labels are backwards.
- **Open decision:** which IP field does the holdout (`MD5(advertiser_id:ip)`) + VV attribution use? Define numerator + denominator + holdout on that one field — may require changing OUR calculator's denominator to `device_ip`, not just the UI. Escalate to `impression_facts`/augmentor IP-resolution owner (data-platform) + BER-2250 holdout owners.
- API-vs-rederive (Chris's Q): moot until the IP field is agreed — an API can't reconcile a definitional grain difference.

Queries: `bq_perf_log` 2026-06-24 (all_facts grain check 12.2 GB; ip-vs-partner_ip reconciliation 16.1 GB).

### 7e. RESOLVED — experiment unit is the resolved `ip`; our calculator is correct (2026-06-24)

Settled the open decision ourselves instead of waiting on data-eng. Applied the production holdout hash (`MD5(advertiser_id:ip)` → bucket via the `ti_837_augmentor_holdout_bucket_verification.sql` BQ port; 0–99 = holdout) to WGU's **served** IPs in `cost_impression_log`, hashing the resolved `ip`:

- **0 of 2,356,886 served IPs (one day) fell in holdout buckets (0.0%).**

Holdout IPs are suppressed from serving, so served IPs avoid holdout buckets **only if** the holdout is computed on the field being hashed. `MD5(aid:resolved_ip)` ⟂ `MD5(aid:device_ip)`, so 0% (vs ~10% uniform expected) proves **holdout + serving run on the resolved `ip`, not `device_ip`**. `clickpass_log` (VV attribution) keys on the same resolved `ip` (+ has `is_control_group`).

**Conclusions:**
- Randomization + serving + attribution all on resolved `cost_impression_log.ip` → **our calculator (10.70%) is the correct, internally-consistent baseline.**
- `graph.uniques` (32M) is the **wrong denominator** (see 7g — it counts display by cookie/`guid`, CTV by IP; NOT `device_ip`); `sitevisitors/uniques` (5.92%) understates ~2×.
- R2's graph table can supply the right numerator (`site_visitors`, resolved-IP) but NOT the denominator (`uniques` is device_ip). So the fix is **(a)** source the baseline from the `cost_impression_log` grain (our per-advertiser number) or **(b)** data-eng adds a resolved-IP served-unique to the reporting table. Re-deriving from graph columns alone cannot be made correct.

Query: `bq_perf_log` 2026-06-24 holdout-field test (0.30 GB).

**Zach Schoenberger confirmed the mechanism (2026-06-24, authority on holdout/targeting):** holdout and VV are *two separate sides of the system*, not one field — (1) **holdout = targeting**, done on the IPs in the targeting system (= the IP that lands in the served event log, `cost_impression_log.ip`); (2) **VV = attribution**, which "doesn't know or care about md5 — it just matches on ip from event log with ip from guid log." Both sides operate on the resolved event-log `ip`; **neither uses the raw `device_ip`** that `graph.uniques` counts. This confirms the holdout-bucket test and settles the denominator: the MDE baseline must use the served event-log `ip` count, not `graph.uniques` (device_ip).

### 7f. Why the 2× denominator — corrected: cross-table, NOT IP cleaning (2026-06-24) — ⚠️ "CROSS-TABLE / DIFFERENT UNIVERSE" FRAMING SUPERSEDED BY 7g

Initial guess (7d) was that `cost_impression_log.ip` is a closed-loop-resolved collapse of raw `device_ip` (~2:1). **Disproven empirically:** `ip` = `ip_raw` exactly in both `clickpass_log` (1.94M, ratio 1.0) and `impression_log` (21.2M) — nothing is merged. (This section then guessed "cross-table / different universe" — ALSO wrong; see 7g. `impression_facts` reads the SAME served `cost_impression_log`. The table below is still useful for the won-vs-all-bids universe sizes, but `graph.uniques` is NOT from impression_log and is NOT `device_ip`.)

Per Malachi (system semantics): `cost_impression_log` = **won** bids (served impressions); `impression_log` = **all** bids (won or not). That explains the universe ordering — we bid on more IPs than we win:

| Table | rows (WGU, 30d) | distinct IPs | key | universe |
|---|---:|---:|---|---|
| `cost_impression_log` (our calc) | 356M | **15.7M** | `ip` (= `partner_ip`; unlinked 0.6%) | **won** bids (served) |
| `impression_log` | 935M | 21.2M | `ip` (= `ip_raw` = `original_ip`; `bid_ip` 21.6M) | **all** bids (won or not) |
| `impression_facts` → `graph.uniques` (Chris's source) | (1.66B cells) | **32.1M** (base-table confirmed) | `device_ip` | even broader than all-bids — source TBD |

**This sharpens the conclusion:** the baseline denominator must be the **won/served** count (you can't drive a visit from an IP you never served), which is exactly `cost_impression_log` (15.7M → 10.70%). `graph.uniques` (32.1M) over-counts — it's *larger* than even the all-bids `impression_log` (21.2M), so it includes IPs that were never served and never could have visited. Exact source of the 32.1M `device_ip` (more than all-bids) still TBD — needs the `impression_facts` model source. Immaterial to the decision: holdout + attribution run on `cost_impression_log.ip`. Queries: `bq_perf_log` 2026-06-24 (impression_facts base 61.9 GB; clickpass ip/ip_raw 1.1 GB; impression_log ip/bid_ip 320 GB; CIL linkage 11 GB).

### 7g. DEFINITIVE — what `graph.usersreached` is, from the SQLMesh model source (2026-06-24)

Read the actual model (`SteelHouse/sqlmesh` → `models/dw-main-silver/summarydata/impression_facts.sql`, owner `ber`) + reconstructed the HLL in BQ. This supersedes the `device_ip` (7d) and cross-table (7f) explanations.

**The mechanism (one line of the model):**
```sql
uniques = HLL_COUNT.INIT(CASE WHEN channel_id = 8 OR objective_id IN (5,6) THEN ip ELSE guid END)
-- FROM logdata.cost_impression_log  WHERE unlinked = FALSE AND ad_served_id IS NOT NULL
```
- **CTV/video** (`channel_id 8`, `objective_id 5,6`) → distinct **`ip`**
- **Display** → distinct **`guid`** (browser cookie)
- It reads the **SAME served `cost_impression_log`** our calculator uses — NOT `device_ip`, NOT all-bids, NOT augmentor.

**Why 32.1M (WGU 30d), reconstructed exactly off CIL deduped to 1 row/impression:**
- CTV/video leg (by `ip`) = 14.06M ≈ served CTV `ip` (12.7M) — well-behaved
- Display leg (by `guid`) = 18.40M — the balloon (cookies fan out ~2.4× per IP)
- Sum ≈ 32.46M ≈ the 32.1M HLL. Whole-table distinct `ip` = 15.53M (= our 15.7M); distinct `guid` = 37.1M.
- Confirmed NOT an event-type double-count: reconstructed from a deduped (1 row/`impression_id`) served log, so no event counted twice; checked all 35 `impression_facts` columns (`unlinked` already FALSE; `new_users_reached`/`existing_users_reached` are a separate is_new split, not summed).

**What the "graph" table is:** R2 metric layer → CHAPI (ClickHouse API) → ClickHouse `summarydata.all_facts_local_daily` (hourly copy of BQ `all_facts`). Owner: the **Backend Reporting squad (`ber`)** owns BOTH the SQLMesh model (`SteelHouse/sqlmesh`) AND the CHAPI/ClickHouse load (`SteelHouse/airflow-reporting`, `dags/chapi/`) — verified via `owners.py` + commit authors (Lizz Joslen, Mike Rivera; Aylwin Souza on squad). The CTV "reach_meter" widget is a *separate* table (`info.reach_meters` ← BQ `summarydata.reach_meters`; audience-segment-grained), not the same as per-advertiser `graph.usersreached`.

**Conclusion (unchanged):** `graph.usersreached` blends IP-counts (CTV) + cookie-counts (display) → over-counts ~2× and isn't a per-IP/per-household number. The per-IP MDE baseline (matching the per-IP holdout, §7e) is `count(distinct ip) from cost_impression_log` (15.7M → 10.70%). Queries: `bq_perf_log` 2026-06-24 (impression_facts base 61.9 GB; clickpass ip/ip_raw 1.1 GB; impression_log 320 GB; CIL linkage 11 GB; channel-split reconstructions via workflow agents).

### 7h. The fix — owner, differentiation, and the request (2026-06-24, verified via 2-agent workflow)

**Owner = Backend Reporting squad (`ber`).** GitHub team `backend-reporting`; owns the SQLMesh model AND the airflow-reporting CHAPI load (one team end-to-end). Verified via `owners.py` + model commit authors. Maintainers: **Lizz Joslen, Mike Rivera** (Aylwin Souza on squad). Route: BER Jira ticket, tag Backend Reporting.

**Can we differentiate without a new column? Only for CTV-pure advertisers.**
- **Channel split works for CTV-only:** `channel_id=8` (and `objective_id IN (5,6)`) uniques is already IP-keyed. WGU CTV uniques 12.94M ≈ CIL served CTV IP 12.78M (~1.2% HLL error). So a CTV-only advertiser gets clean IP reach via a channel filter, no new column.
- **Mixed CTV+display can't be done by channel:** display is guid-keyed, and you can't sum per-channel IP reaches — WGU CTV-IP 12.79M + display-IP 7.93M = 20.71M summed vs 15.61M distinct → **5.10M (33%) cross-channel overlap** (1 in 4 served IPs see both). Cross-channel-deduped served IP needs a single always-IP HLL (`users_reached_ip`) or a CIL query.
- **Exact parity with our 10.70% is NOT reachable from graph:** the in-window restriction needs `impression_hour`/`day_number`, which live only in `ber_stg.visit_facts__base` and are GROUPed away before `visit_facts`/`all_facts` (graph layer has neither). The day-bucket visitor arrays that survive encode elapsed-days-from-impression, a different axis — can't reconstruct "impression served in-window."

**WGU IVR reconciliation (denom 15.61M, verified):**
| numerator | count | IVR | = |
|---|---:|---:|---|
| A. all verified visitors (graph `site_visitors`) | 1.922M | **12.31%** | what `sitevisitors/users_reached_ip` gives |
| B. impression-in-window (`visit_facts__base`) | 1.690M | 10.83% | needs ber_stg, not graph |
| C. visiting-AND-served-in-window (CIL intersect) | 1.672M | **10.71%** | = our standalone 10.70% ✓ |

**Recommended path:**
- **Denominator:** add `users_reached_ip = HLL_COUNT.INIT(l.ip)` (always-IP, all channels) to `impression_facts`, additive (leave `uniques`). Only way to get cross-channel-deduped served-IP for mixed advertisers from graph.
- **Baseline / exact parity:** don't chase it from graph (not reachable). Either **(a) accept the residual** (graph 12.3%, ~1.6pp optimistic — all-VV-visitors incl. pre-window impressions) or **(b) source the calculator baseline from CIL** (exact 10.71%, one scheduled query). Primary rec: (b) for the defensible baseline + the `users_reached_ip` column so the UI headline reach is also IP-correct.

**Request sent to BER:** add `users_reached_ip = HLL_COUNT.INIT(l.ip)` across all channels → expose in graph → backfill ~30d. Problem: `graph.usersreached` counts CTV by IP / display by cookie → ~2× the true served IPs (WGU 32M vs 15.7M); MDE/power calc needs per-IP reach (holdout is per-IP); channel-split can't substitute (mixed-advertiser 33% overlap).

### 7i. How the graph 30-day reach is actually computed (ClickHouse) + corrected column spec (2026-06-25, agent-verified from chapi/airflow-reporting code)

Ryan Kleck asked: impression_facts/all_facts are hourly grain — for a 30-day MDE reach do we `SUM()`? **No — it's an HLL merge, and the BQ HLL sketch isn't even used.** Verified chain (code-proven):
- SQLMesh emits `uniques` (BQ HLL++ sketch, BYTES) **and** `uniques_arr` (raw-ID `ARRAY_AGG`). **CHAPI loads only `uniques_arr` — the `uniques` sketch is a dead column** (BQ HLL++ isn't mergeable by ClickHouse).
- ClickHouse: `all_facts_local_daily.uniques_arr Array(Nullable(String))` (hourly, raw IDs) → MV `all_facts_by_day_mv` does `uniqArrayState(uniques_arr)` → `all_facts_local_by_day(_aggregated).uniques_arr AggregateFunction(uniqArrayState, Array(Nullable(String)))`.
- **Query time:** `graph.usersreached` (`r2-metadata` `type="HLL" definition="uniques_arr"`) emits `toInt64(uniqArrayMerge(uniques_arr))` over the 30-day window — a true distinct **merge across days, NOT a sum**. (That's why it returns ~32M, not billions. Proven by chapi `HouseholdsReachedQuerySqlTest.kt`.)

**So if we go the graph-column route, the spec changes:** emit **`users_reached_ip_arr = ARRAY_AGG(l.ip IGNORE NULLS)`** (unconditional `l.ip`, vs `uniques_arr`'s CTV/display CASE) — an **array, not an HLL sketch** (the `HLL_COUNT.INIT(l.ip)` sketch would be dead on arrival like `uniques`). It is a **coordinated 3-repo change + backfill**, not one column:
- `sqlmesh` — `users_reached_ip_arr` in `impression_facts` + `all_facts` passthrough.
- `chapi` — ClickHouse DDL on 3 tables (`all_facts_local_daily` + two `_by_day` aggregates) + MV (`uniqArrayState(...)`) + `r2-metadata.xml`/`-legacy.xml` metric with `type="HLL"`.
- `airflow-reporting` — `dags/chapi/conf/reporting_config.json` load select/insert_columns + the BQ export view `v_all_facts`.
- Backfill ~30d (MVs aren't retroactive).

**Decision impact:** this confirms the graph-column path is substantial (multi-repo, DDL migrations, MV rebuild, backfill) — exactly what the **CIL route avoids** (one small daily query→table we own, exact 10.71%, zero ClickHouse work). Strengthens CIL unless BER wants IP-reach in graph for other uses.

**Exact ClickHouse query CHAPI generates** (Ryan Kleck Q, source-verified from `SteelHouse/chapi` `SummaryQueryBuilder.kt` + `r2-metadata.xml`, 2026-06-25) for Chris's params (`data=graph.spend,graph.impressions,graph.usersreached`, `begin=thirty`, `sum=advertiserinfo.id`, `includetoday=false`, `aid=31357`):
- **Table = `summarydata.all_facts_by_day_ramp_combined`** (daily grain, ClickHouse `Distributed`; no `FINAL`). Time column `day`; predicate is **half-open literal GMT timestamps** `day >= timestamp '<30d-ago>' AND day < timestamp '<today 00:00>'` (30d ending yesterday; not `today()-30`).
- `graph.spend` = `sum(media_spend)+sum(data_spend)+sum(platform_spend)+sum(legacy_spend)` (SUM); `graph.impressions` = `sum(display_impressions)+sum(ctv_impressions)` (SUM); `graph.usersreached` = inner `uniqArrayMergeState(uniques_arr)` → outer `toInt64(uniqArrayMerge(uniques_arr))` (HLL **MERGE**, gold-tested `HouseholdsReachedQuerySqlTest.kt`).
- `aid` → `WHERE advertiser_id IN (31357)`; `sum=advertiserinfo.id` → `GROUP BY advertiser_id` (joins `info.v_advertisers`); `fullname=true` only renames JSON output keys, no SQL effect; no PREWHERE / data-tier filter in SQL.
- **Settles both assumptions:** spend/impressions ARE summed; usersreached is merged (cross-day distinct → ~32M) — in one query.

**How to get the REAL runtime SQL for any graph/R2 metric (verified from chapi source, 2026-06-25):** there is **no** curl/debug param — CHAPI's `/apidata` has no `debug`/`explain`/`sql`/`dryrun` param and `format` only accepts Human/Json/Csv/Excel (the built SQL `QueryResult.sql` is never returned in the response). Two ways to capture the literal executed SQL: (1) **easiest — service logs:** every request logs it unconditionally at INFO — `DataService.kt:143` `log.info("Built SQL Command: {} | Params: {}", cmd, ...)` → grep request-data-service / Datadog `service:chapi "Built SQL Command"`. (2) **ClickHouse `system.query_log`** (CHAPI injects no query_id/comment tag, so pin on table + aid): `WHERE type='QueryFinish' AND query LIKE '%all_facts_by_day_ramp_combined%' AND query LIKE '%advertiser_id IN (<aid>)%'`. The query above is a source reconstruction (chapi `SummaryQueryBuilder.kt`); use the INFO log for the verbatim runtime SQL.

## 8. Open Items / Follow-ups
- Decide refresh cadence — currently a manual rerun. Could schedule a weekly cron via `schedule` skill if useful.
- Consider hosting the JSON separately so the HTML can fetch fresh (vs baked-in which means the calculator drifts after a few weeks).
- Publish via gist + githack for team access.
- Worth pushing back on Chris/Matt that the gary-ql resolver uses `varReduction=1` (no CUPED / ghost-ad / stratified). This calculator shows raw + post-stack side by side; the customer-facing wizard will only show raw. See `tickets/ti_xxx_power_analysis_workshop/summary.md` for the methodology backing the 0.595 stack multiplier.
