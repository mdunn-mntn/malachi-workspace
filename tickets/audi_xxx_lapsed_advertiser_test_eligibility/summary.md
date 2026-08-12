---
doc_type: ticket
title: "[SPIKE] Lapsed-advertiser incrementality-test eligibility"
status: backlog
date: 2026-08-11
summary: "Can a churned advertiser be screened for a ghost-bid lift test from their last-active window? Required-spend heuristic from IVR/CVR."
result: "tooling built and regression-tested; blocked on the advertiser id from Al"
question: "For an advertiser who has stopped spending, can we still compute what an 8-week ghost-bid lift test would cost, from their last-active visit and conversion rates?"
framing_state: draft
---

# [SPIKE] Lapsed-advertiser incrementality-test eligibility

**Jira:** not yet filed — draft in §5
**Status:** backlog
**Date Started:** 2026-08-11
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
- **Question (the unknown):** For an advertiser who has stopped spending, can we still compute what an 8-week ghost-bid lift test would cost — from their last-active visit and conversion rates — well enough to decide whether to pitch them?
- **Goal (why / the decision):** Al Beretta has a churned advertiser who left over MNTN's *legacy* incrementality story and is a plausible win-back on the new ghost-bid methodology. The decision: is this advertiser worth re-approaching with a test offer, and at what budget. Ties to the Q2 north star — incrementality is Kale's stated #1 priority, and this is the retention/win-back edge of it.
- **Objective (done-when):** A required 8-week test budget for the named advertiser at 5% and 10% relative IVR MDE, with their historical typical monthly spend beside it so the gap reads as an ask; plus a stated answer on whether spend can be predicted from VR/CR at all. Binary: the workbook exists with those numbers, or it doesn't.
- **Approach (how):** Fork the INCR-75 metrics pull, re-anchored from `CURRENT_DATE()` onto the advertiser's last-active window; feed `p_visit` / `p_cvr` / `cpm` / `imps_per_ip` into TI-884's `spend_required()`. Assumptions resolved empirically first: (a) does `cost_impression_log` actually retain the historical window, (b) does a spend-history source exist that isn't frozen, (c) do VR and CR carry any real signal about spend.
- **What would change the answer:** If the advertiser's last-active window has under 100 visiting IPs, the IVR is too unstable to quote and the whole exercise is not answerable for them (INCR-75 `MIN_VISITING_IPS`). If their last-active window predates 2023-10-01, the CIL floor blocks it outright.

## 1. Introduction
INCR-75 produced the eligible-advertiser list for ghost-bid lift tests (2,009 delivering → 1,287 eligible; Top 28 / Mid 152 / Low 1,090 after the measured-lift fold). That screen covers **currently delivering** advertisers only. Al Beretta asked what it would take to screen a churned one, phrasing it as "a quick heuristic on spend based on Visit and Conversion rate."

## 2. The Problem
The churned advertiser is absent from the list, and the reason is widely misread as a spend threshold. It is not — INCR-75 deliberately made spend **scored, not cut**. The exclusion is structural: the universe CTE reads `cost_impression_log` over the trailing 30 days with `HAVING SUM(impressions_ip) > 0`, so an advertiser with no recent delivery never enters the funnel at all.

Separately, Al's framing has a direction problem worth settling rather than quietly working around: visit rate and conversion rate are **ratios**, so they carry almost no information about how much an advertiser spends.

## 3. Plan of Action
1. Verify the three data assumptions empirically before building anything.
2. Fork `incr_75_advertiser_metrics.sql`, re-anchored on the advertiser's last-active day.
3. Regression-test the fork against a live advertiser already in the INCR-75 output.
4. Wrap TI-884's `spend_required()` for the required-budget numbers.
5. Test whether VR/CR predict spend, on the 2,009-advertiser INCR-75 cohort.
6. Branded `.xlsx` + a Slack reply to Al.

## 4. Investigation & Findings

### The assumed data blocker does not exist
`incr_75_advertiser_metrics.sql` carried the header comment *"cost_impression_log has 90-day TTL."* **It is wrong.** `INFORMATION_SCHEMA.PARTITIONS` on `sqlmesh__logdata.logdata__cost_impression_log__2498930125`: **1,047 partitions, min 2023-10-01, 77,588,957,435 rows, no TTL.** Probed `clickpass_log` and `ui_conversions` directly on 2024-06-12, 2025-01-15, 2025-06-11, 2026-01-14, 2026-06-10 — all five days return data in both. Every input to the screen is recomputable at any window back to the CIL floor. The stale comment is the single thing that made this ask look expensive; corrected in both that file and TI-1019's.

### The real trap is the spend-history source
INCR-75's 12-month monthly-spend CTE reads `aggregates.agg__daily_sum_by_campaign`, which is **frozen: 242 partitions, 2025-09-01 → 2026-04-30**. Any trailing or lapsed window returns zero rows. Replacement is `summarydata.sum_by_advertiser_by_day` — advertiser × day grain, 2024-01-01 → current, fresh, `require_partition_filter=TRUE`, and ~613x cheaper on narrow columns than `SELECT *`. Note its floor (2024-01-01) is later than CIL's (2023-10-01), so an advertiser who lapsed before 2024 has delivery data but no spend-pattern history.

### PSA exclusion bug in INCR-75 (no impact on the result)
The filter `advertiser_id != 90` is a **no-op**: advertiser 90 has no row in `integrationprod.advertisers` and zero CIL rows. The real PSA account is **9090** ("Public Service Announcement", `active=TRUE, deleted=FALSE, is_test=FALSE`), which entered the universe and passed F1 and F2. It was removed at F3 only because `p_visit = 0.0`. The 2,009 universe and 554 F3 removals each include PSA by one; **the 1,287 eligible set is unaffected.** Fixed to `!= 9090`; logged in INCR-75's summary.

### The fork reproduces the grain
Ran the fork for BoggBag (46426) pinned to INCR-75's own window, 2026-05-26..2026-06-25:

| metric | INCR-75 | fork | delta |
|---|---:|---:|---:|
| cpm | 12.3467 | 12.3273 | −0.16% |
| imps_per_ip | 3.6513 | 3.6467 | −0.12% |
| p_visit | 0.10936 | 0.10914 | −0.21% |
| p_cvr | 0.005165 | 0.005219 | +1.04% |
| spend_30d | 44,688 | 45,394 | +1.58% |
| impressions_30d | 3,619,444 | 3,682,439 | +1.74% |
| converting_ips_30d | 5,120 | 5,270 | +2.93% |

The **rate** columns — the ones that feed the power calc — reproduce within 0.21%. Volume columns run +1.6–2.9% because INCR-75 caught 2026-06-25 mid-day and that partition is now complete; conversions drift most, consistent with the documented attribution backfill. Grain unchanged.

### VR and CR cannot predict spend
On the 1,566 INCR-75 advertisers with `spend_30d > $1,000` and `IVR > 0`, OLS on `log(spend_30d)`:

| model | R² |
|---|---:|
| ~ log(IVR) | 0.045 |
| ~ log(CVR) | 0.098 |
| ~ log(IVR) + log(CVR) | **0.100** |
| ~ IVR + CVR (levels) | 0.013 |

Pearson r: log(IVR) +0.212, log(CVR) +0.314. Within any single IVR decile, spend spans **15–66x** from p10 to p90, while the median moves only ~3x across the entire IVR range. Chart: `artifacts/audi_xxx_chart_vr_cr_spend.png`.

### The rule of thumb is delivery-shape-conditional
At $30 CPM and 15 imps-per-IP, 8-week budget ≈ **$14,100 ÷ IVR** for a 5% relative MDE. That shortcut is **only** valid at those defaults. BoggBag runs $12.33 CPM and 3.65 imps/IP, where the bare shortcut is **10x too high**. General form: `$14,100 / IVR × (CPM/30) × (impsPerIP/15)`. The script prints the scaled version so the bare one can't be quoted by accident.

### A lapsed advertiser cannot reach Top tier
INCR-75's final tier is POWER × CONFIRMED-LIFT. `confirmed +` needs ≥20 holdout visits at p<.05 from a live ghost-bid holdout. A non-delivering advertiser generates no bids, so no measured lift exists and none can. **Ceiling is Mid**, on the a-priori power gate alone.

### Advertiser 39568 = Mockingbird — RESULT (2026-08-12)

**Verdict: powered for a 5% relative visit-lift test at $16.1k/month, well under the $40k they were running. Tier = Mid (the lapsed ceiling).**

Window resolved automatically to their last-active 30 days, **2026-04-07..2026-05-06**. Lapsed **98 days** (3.2 months). First active 2025-02-11, 148 delivering days, lifetime spend $164,825 across 6 active months.

| Metric | Value |
|---|---|
| Vertical | Kids & Family (not B2B) |
| IVR (`p_visit`) | **12.93%** (36,965 visiting of 285,909 served IPs) |
| CVR (`p_cvr`) | 0.082% (234 converting IPs) |
| CPM | **$96.28** |
| imps/IP | **1.46** |
| 56-day distinct-IP reach | 433,042 |
| Final month spend | **$40,143** (max month $40,668) |
| Typical active month (6-mo median) | $26,916 |

**Required 8-week test budget**

| Target | Total | Per month | vs their $40k exit rate |
|---|---:|---:|---|
| IVR 5% relative MDE | $29,687 | **$16,116** | clears, 60% headroom |
| IVR 10% relative MDE | $7,422 | $4,029 | clears, 90% headroom |
| CVR 15% relative MDE | $597,947 | $324,600 | not feasible — informational only |

**Direct 56-day cross-check passes independently.** At their own observed 433,042-IP reach, with no imps/IP extrapolation, the relative IVR MDE is **3.68%** — already below the 5% credible threshold. This is the defensible number: it does not rely on the optimistic-floor assumption baked into `spend_required`.

**Al's "$40k a month" reconciles to their exit run-rate, not their average.** Their final 30 days ran $40,143 and their peak month $40,668, but the 6-month median was $26,916 — they ramped up before pausing. The workbook compares against the $26,916 median (the INCR-75 convention); against the $40k Al remembers, the headroom is larger still. Either way it clears.

#### Caveats that came out of the pull
- **The all-funnel-vs-prospecting risk was checked and does not apply.** Their campaign dimension spans objectives 1,4,5,6,7, and a ghost-bid holdout is prospecting-only by construction, so an all-funnel IVR would have overstated the testable baseline. In this window delivery was **99.9% prospecting**: 285,905 of 285,910 served IPs on objectives 1/5/6, with obj_7 contributing 5 IPs and the retargeting campaigns delivering nothing. **12.93% is the prospecting IVR.** Query logged.
- **IVR 12.93% sits just inside INCR-75's saturation penalty band** (`IVR_SATURATED = 0.12`). The scoring rule penalizes very high visit rates on the reasoning that there is less headroom left to move. It makes them highly *detectable* but is a mild negative on expected lift size. Not a blocker; state it rather than selling a high visit rate as unambiguously good.
- **$96.28 CPM is 3.5x the $27.54 median** for the $25–60k/30d band, paired with an unusually low 1.46 imps/IP. Confirmed **not** a MNTN Select blended-CPM artifact — they are `product_id = 1` (PTV). They buy expensive, low-frequency reach. Since budget scales linearly with CPM, this is what makes their required spend higher than their high IVR alone would suggest.
- **My pre-auth conditional call was too pessimistic, in the right direction.** Using the cohort-median shape I put the 5% MDE threshold at 3.73% IVR and called it "a coin flip." Their real shape differs sharply (CPM 3.5x higher, imps/IP 0.44x, net 1.59x), which lifts the threshold at their shape to ~5.9% — but their 12.93% IVR clears it comfortably regardless. The conservative "don't quote a tier yet" hold was correct.

### Advertiser 39568 — conditional pre-answer, superseded by the result above (2026-08-12)
Al's advertiser is **AID 39568**, last running **~$40k/month** before they paused. Confirmed **not present** in `incr_75_advertiser_metrics.csv`, so their rates have never been measured and must come from the metrics pull. **BigQuery is blocked on an expired gcloud refresh token** (`gcloud auth login` needed; non-interactive session cannot complete it).

What is answerable now, by inverting the power calc (`artifacts/audi_xxx_budget_feasibility.py`). $40k/mo = **$73,684** over an 8-week test. Using the median delivery shape of the **176 INCR-75 advertisers in the $25–60k/30d band** (CPM **$27.54**, **3.30** imps/IP) as the prior:

| Target | Minimum visit rate for $40k/mo to power it |
|---|---|
| 10% relative MDE | **0.96%** |
| 5% relative MDE | **3.73%** |

Comparator visit rates in that spend band: p25 1.59% / **median 3.77%** / p75 8.68%.

**Verdict: the 10% MDE almost certainly clears at their old budget** — 0.96% sits below the 25th percentile. **The 5% MDE is a coin flip**, needing 3.73% against a comparator median of 3.77%. Since `can_hit_ivr_5pct_8w` is the gate that separates Mid from Low, their tier hinges on a number we do not yet have. Do not quote a tier to Al until the pull runs.

Sensitivity at that shape: 5% MDE costs **$96k/mo** at p25 IVR, **$40k/mo** at median, **$16k/mo** at p75. The spread is the reason to measure rather than estimate.

## 5. Solution
Built and regression-tested, blocked only on the advertiser id:

| Artifact | What it does |
|---|---|
| `queries/audi_xxx_last_active.sql` | Resolves last-active day + delivering-day count + lifetime spend |
| `queries/audi_xxx_lapsed_advertiser_metrics.sql` | The forked metrics pull, windowed on literals |
| `artifacts/audi_xxx_run_metrics.py` | Two-step driver; dry-runs and enforces a scan ceiling |
| `artifacts/audi_xxx_required_spend.py` | Wraps TI-884; IVR 5%/10%, CVR 15% informational, direct 56d cross-check, tier ceiling |
| `artifacts/audi_xxx_vr_cr_spend_check.py` | The R²=0.10 evidence + decile chart |
| `artifacts/audi_xxx_build_xlsx.py` | Branded workbook; builds with or without an advertiser id |

**Two-step by design:** BigQuery cannot prune partitions on a date derived from a subquery. Resolving the window and the metrics in one statement scanned 39.5 GB; splitting them and substituting literals brings it to 5.5 GB for a single advertiser.

## 6. Questions Answered
- **Q:** Why is the churned advertiser not on the eligible list?
  **A:** Not a spend threshold. The universe CTE requires delivery in the trailing 30 days, so they never enter the funnel.
- **Q:** Can we recover their visit and conversion rates after they stopped?
  **A:** Yes, back to 2023-10-01. CIL has no TTL; clickpass_log and ui_conversions both carry years.
- **Q:** Can spend be estimated from visit and conversion rate?
  **A:** No. R² = 0.10; spend spans 15–66x within a single visit-rate decile.
- **Q:** What can be computed instead?
  **A:** The spend a test would *require* — `spend_required()`, which already existed in TI-884 and produced INCR-75's `budget_for_mde_ivr_*` columns.

## 7. Data Documentation Updates
Pending `/capture`:
- `cost_impression_log` has **no** 90-day TTL (floor 2023-10-01, 1,047 partitions).
- `agg__daily_sum_by_campaign` is frozen at 2026-04-30; use `sum_by_advertiser_by_day` for advertiser × day spend from 2024-01-01.
- PSA is advertiser **9090**, not 90.

## 8. Open Items / Follow-ups
- **Jira `[SPIKE]` not filed.** Draft ready (393 chars, lint-clean); needs a key before the workbook goes to Drive via `--drive`.
- **Slack reply to Al not sent.** Draft ready; now carries the actual numbers.
- Auth blocker on 2026-08-12 resolved (expired gcloud refresh token). Note for future sessions: test it with a bare `gcloud auth print-access-token`, NOT wrapped in `timeout` — macOS has no `timeout`, so the wrapper fails with exit 127 and reads as an auth failure.
- If their last-active window predates 2024-01-01, the spend-pattern CTE returns nothing and "vs typical month" falls back to the window's own spend. Al's "$40k/month" is the fallback anchor.
- Jira `[SPIKE]` not yet filed — draft ready, awaiting confirm.
- If the advertiser lapsed before 2024-01-01, the spend-pattern CTE returns nothing and the "vs typical month" comparison falls back to the window's own spend.
- Whole-cohort version (every advertiser delivering since 2024-01-01 but not in the last 30d) is a natural follow-on; scoped out deliberately.
