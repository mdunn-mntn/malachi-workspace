# SCOPE: Frequency-Capping Bandit at MNTN (frequency-first, with the HHST alternative)

*Handoff brief. Self-contained. Everything is verified against BigQuery unless flagged "directional." MNTN = performance Connected-TV (CTV) DSP; KPI = verified visit (household visits advertiser site after a video ad; multi-day attribution; no clicks).*

---

## 0. TL;DR / recommendation

Build a **lift-aware frequency-capping bandit** as the first bandit, not the HHST-threshold bandit. Both optimize a real diminishing-returns curve, but they differ on the one thing that matters for a first project: **can you prove the value?**

- **HHST (intent-gate) bandit:** requires measuring lift by graduated intent score, which is **blocked in BigQuery** (no holdout stream carries a graduated 0-10000 score). Its causal proof needs a GCS + Databricks build (the reassigned BER-2250 path).
- **Frequency bandit:** its causal proof is a **household-randomized cap RCT**, which MNTN's existing MD5 holdout bucketing supports and which is **runnable now**.

The observational sizing below shows a real, cleanly-shaped diminishing-returns curve over a bounded pool of spend, but it is **confounded and cannot by itself justify a cap**. The deliverable of Phase 0 is therefore not "cap frequency, save $X" — it is "the curve is real and bounded, the causal question is open, and the experiment that closes it is cheap." Run the RCT (§6), then let a bandit set the cap adaptively (§7).

---

## 1. The two candidate bandits and why frequency wins as the first

MNTN has a bidder-side lever for each:

| | HHST intent-gate bandit | Frequency-cap bandit |
|---|---|---|
| Knob | per-campaign `household_score` threshold (which IPs to bid on) | per-campaign / campaign-group impression cap per rolling window (how many times to hit a household) |
| Currently set by | `ddm.hhst_generate_recommendation` (hourly Redshift bot, pure pacing objective) | campaign-template default -> preset -> campaign-group override, + a universal 1/30min floor |
| Addressable spend | non-RTC categorical prospecting slice | all delivery, all advertisers, all stages |
| Reward (incremental) measurable in BQ? | **NO** — needs graduated-score holdout (GCS+Databricks) | **NO observationally, but YES via a cheap household-randomized RCT** |
| Political surface | audience/intent core (your team's mandate) | bidder/delivery (partner team) |
| Narrative | novel, abstract | universally understood; adaptive freq-capping is a known capability gap |

The decisive factor is measurability. Frequency is the better first bandit because you can demonstrate value fast, and the lift-measurement muscle it builds (randomized household holdouts -> incremental visits) is exactly what the HHST bandit needs later. Sequence: **frequency first, HHST second.**

---

## 2. What the HHST work established (context; full detail on request)

- The HHST gate is set by `ddm.hhst_generate_recommendation` with a **pure budget-fill objective**: pick the highest `household_score` threshold whose remaining winnable IP population still spends the daily budget at recent cost-per-win; lower it only to keep pacing. No visit/conversion/lift term.
- Cohort `mm_campaign_classifier` (has_mm AND objective_id=1) = 6,288 campaigns / 2,275 advertisers / **$19.06M/30d**.
- Score-band response curve (RTC-excluded, 7d): HI (≥6666) ~5x cheaper per observed visit than sub-HI; ~40% of non-RTC prospecting spend is sub-HI.
- **Verified blocker:** graduated per-band LIFT is not obtainable in BQ. Correlation ≠ incrementality (served-vs-holdout ITT lift ≈ 0%). That is why HHST is not the first project.

---

## 3. The frequency-capping system (from codebase investigation)

- **Where set / stored:** `bidder.frequency_caps` / `dso.frequency_caps` as `(frequency_cap, frequency_cap_duration_seconds)` pairs; `object_type` ∈ {campaign, campaign_group}. Synced to the bidder cache; enforced in `do_fcap` before the bid is emitted. Counters live in Redis: `rtb:frequency:{ip}:campaign_group_id=<cg>:campaign_id=<c>`.
- **Scope:** enforced per **campaign** AND per **campaign_group**, keyed on **IPv4** (the household id; MNTN-ID only when `uses_mntn_id=true`; IPv6 fallback only). There is **no advertiser dimension in the counter and no rollup key** — so no cross-advertiser cap and no advertiser-level rollup.
- **Window:** true **rolling/sliding** window (`now - impression_ts <= duration`, re-evaluated per request). No calendar reset.
- **Universal floor:** a `1 impression / 30 min` cap is appended to all campaigns as a uniform default policy (per-campaign counter, not a shared counter). Observed cap durations range 30 min to 24 hr; configurable 5 min to 300 days.
- **Two failure modes that matter for measurement:** (1) **fails open** on Redis error (caps silently stop enforcing), (2) **no advertiser rollup** means one household can be hit N times per group across an advertiser's groups. So **delivered frequency > configured frequency**; always measure delivered.
- **Client-transparency constraint:** advertisers can set custom caps (`has_custom_frequency_caps`). A bandit must operate on the **default** cap only and never override an explicit advertiser choice.

---

## 4. Frequency Phase-0 findings (BigQuery, 7d 2026-07-06..07-12, household = ip x advertiser)

Method: `cost_impression_log` (spend, ip, advertiser_id, campaign_id, impression_id) grouped to households; visits via `ui_visits` on `impression_id` (verified join key — NOT `ad_served_id`); +14d attribution tail. Prospecting cohort = has_mm prospecting; retargeting cohort = `public_campaigns.funnel_level >= 2`. WGU (31357) excluded. Spend reconciles to `sum_by_campaign_by_day` within 0.02%.

### 4a. Prospecting reach/frequency curve

| Freq/wk | HH | spend share | visits/1k imps | per-HH visit rate | CPV |
|---|---|---|---|---|---|
| 1 | 40.7M | 29.7% | 8.18 | 0.63% | $3.51 |
| 2-3 | 22.5M | 35.3% | 5.00 | 0.84% | $5.53 |
| 4-7 | 7.4M | 22.9% | 3.47 | 1.13% | $7.55 |
| 8-12 | 1.17M | 6.9% | 1.96 | 1.16% | $12.69 |
| 13-20 | 324K | 2.9% | 1.18 | 1.10% | $19.66 |
| 21-40 | 101K | 1.3% | 1.07 | 1.92% | $17.62 |
| 41+ | 30K | 0.9% | 0.78 | 3.34% | $23.24 |

- Attributed visits/impression fall ~10x (8.2 -> 0.8) and CPV rises ~7x — **but a large part of this is a mechanical last-touch artifact, not diminishing returns.** Last-touch credits exactly one impression per visit, so a household's attributed visits are roughly bounded by its intrinsic visit count regardless of ad count `n`, forcing visits/1k toward ~1/n by construction. Under a constant-per-impression-value null the curve would be flat; the observed decline sits between flat and 1/n, so some is real saturation and some is artifact, and the two are **not separable observationally.** Do NOT read this as clean over-serving.
- **Per-household visit rate plateaus at ~1.1% across freq 4-20**, then jumps at 21-40 (1.9%) and 41+ (3.3%). The **plateau-then-jump** is the tell that the tail is a **different population** (heavier-viewing / multi-person households the bidder can reach 40+ times), not a dose-response on one population.
- **Bounded exposure (the defensible descriptive result):** 65% of spend at freq 1-3 (healthy); high-frequency pool **~12% of spend at freq ≥8 (~$2M/30d), ~5% at freq ≥13.** This is a **conservative floor** — the 7d window and prospecting-only scope both understate true lifetime/household frequency.

### 4b. Retargeting/engaged reach/frequency curve (funnel≥2)

| Freq/wk | HH | spend share | visits/1k imps | per-HH visit rate | CPV |
|---|---|---|---|---|---|
| 1 | 20.0M | 28.4% | 36.2 | 3.56% | $0.55 |
| 2-3 | 8.8M | 22.6% | 34.0 | 6.57% | $0.45 |
| 4-7 | 3.67M | 17.8% | 32.5 | 11.53% | $0.42 |
| 8-12 | 1.10M | 10.1% | 28.0 | 16.52% | $0.48 |
| 13-20 | 518K | 7.7% | 24.1 | 20.31% | $0.54 |
| 21-40 | 313K | 8.1% | 21.7 | 26.51% | $0.60 |
| 41+ | 109K | 5.3% | 14.5 | 31.89% | $0.74 |

- Retargeting runs at **much higher frequency**: **31% of spend at freq ≥8, 21% at freq ≥13** (vs 12% / 5% in prospecting). 7d spend $1.39M.
- Visit rates are far higher (warm users) and **keep rising with frequency (no plateau)**, so observationally high-frequency retargeting "looks" productive.
- **The trap:** retargeted users already visited the site; their visits are the **least incremental** (they were returning anyway). So retargeting is both the biggest high-frequency pool AND where the observational metric most overstates value. The RCT matters most here.

### 4c. Cross-group leakage (the "no advertiser rollup" gap)

95.2% of prospecting households are served by a single campaign_group; only **4.8% by 2+ groups (13% of impressions)**. Real but secondary — most over-frequency is a single group's cap set too loose, not cross-group leakage.

### 4d. Load-bearing caveats (why the curve can't justify a cap alone)

1. **Last-touch attribution artifact (the big one).** visits/1k and CPV decline partly *by construction* (one impression credited per visit; attributed visits ~independent of ad count). This is NOT clean evidence of diminishing returns and cannot be read causally.
2. **Selection / different populations.** Frequency is an outcome (bidder wins + IP availability), so buckets compare different populations, not one population at different doses. The plateau-then-jump confirms the tail is a distinct population.
3. **Shared IPs.** Modest at the population level (~4% of visit-IPs carry 2+ guids, and guid undercounts persons), but concentrated in the freq-21+ tail (not tested there). That tail is tiny (1.3% of impressions, 0.87% of spend), so even if fully contaminated it barely moves the sizing.
4. **Correlation ≠ incrementality**, worst in retargeting.
5. **Left-truncation + prospecting-only** both make the sizing a **conservative floor**, not an overstatement.
6. **+14d attribution tail** truncates ~15-22% of visits (last-TV-touch p90=28d) -> a real RCT needs a ≥30-45d visit tail (mostly a level effect on CPV/visits-1k).

---

## 5. The honest conclusion of Phase 0

The reach/frequency curve is real and bounded (~12% of prospecting spend, ~31% of retargeting spend at freq ≥8), but it is **observationally confounded and cannot justify a cap by itself** (the visits/1k decline is partly a last-touch artifact). Two Phase-0 results stand **without** the RCT: (a) the **sizing** clears the bar for running an experiment (that IS the Phase-0 gate); (b) the **cross-group leakage** (4.8% of households / 13% of impressions on 2+ campaign_groups with no cap rollup) is a pure **control-plane defect** — actionable immediately, no causal claim required, and the strongest standalone result in the pack. The causal cap question needs the RCT, and the entire reason frequency beats HHST first is that this RCT is cheap and immediately runnable.

---

## 6. The RCT spec (household-randomized frequency-cap experiment)

**Objective:** estimate the causal marginal value of frequency — the incremental visits per dollar at each cap level — to (a) validate/deny the observational curve and (b) supply the reward the bandit will optimize.

**Randomization unit:** household = `(advertiser_id, ip)`. Assign via the existing holdout hash: `MOD(ABS(FARM_FINGERPRINT(CONCAT(advertiser_id,':',ip))), 1000)` (mirror MNTN's `MD5(AID:IP) mod 1000` ITT convention) -> deterministic, sticky, disjoint arms.

**Arms (start narrow):**
- **A — Control:** business-as-usual caps.
- **B — Cap 8/wk**, **C — Cap 3/wk** (default-cap campaigns only; never override `has_custom_frequency_caps`).
- **H — Suppression holdout:** would-have-served households suppressed (ghost-bid / PSA) -> the absolute-lift anchor (served visit rate − holdout visit rate).

**Primary metric:** **total verified visits per household and cost-per-household** by arm — NOT attributed visits-per-impression, which is mechanically confounded by last-touch (see §4d). Incremental = arm minus suppression-holdout (H). Report incremental visits per dollar, incremental reach, and cost-per-household. **Secondary:** delivered-frequency distribution (measure delivered, not configured — fails-open/leakage).

**Population:** the affected set is households that *would* exceed the cap (freq ≥8 ≈ 1.6M prospecting households/wk). Randomize the full eligible population so the cap binds only where relevant.

**Power (computed from observed rates):** two-proportion, base per-household visit rate ~1.0% (prospecting). To detect a 5% relative change (1.00% vs 1.05%) at 80% power / alpha 0.05 needs ~**636K households/arm**. The freq≥8 affected pool is ~1.6M/wk, so 2 arms are powered in ~1 week and comfortably over a **4-week** run (4 weeks is also required for the long visit tail: impressions weeks 1-4, visits through ~week 6-8). Retargeting base rates are much higher (16-32%), so it is over-powered; the binding constraint there is incrementality size, not N.

**Inference:** per MNTN's standard protocol — two-proportion point estimate + **advertiser-clustered bootstrap** (households nested in advertisers; N=1000 resamples) reporting point / 95% CI / two-sided p. Optionally CausalImpact on the daily arm-level visit series as a convergence check.

**Guardrails:** (1) ring-fence experiment households out of concurrent A/B / Fangorn holdouts; (2) exclude AID 90 (PSA); (3) respect custom caps; (4) start on advertisers without strict delivery SLAs; (5) monitor `fcap_impressions_fetch{outcome=redis_err}` (fail-open) so delivered frequency is trustworthy; (6) run **prospecting and retargeting as separate strata** — the incrementality story differs sharply between them.

**Read-out -> bandit:** the arm-level incremental-visits-per-dollar curve IS the bandit reward. Once the RCT confirms the shape, the bandit continuously sets the per-campaign-group cap to maximize it, adapting to drift.

---

## 7. The bandit (once the RCT validates the curve)

- **Lever:** `(frequency_cap, frequency_cap_duration)` on default-cap campaign_groups; existing sync path to the bidder.
- **Arms:** discrete caps {3,5,8,12,∞}; **Context:** campaign_group / vertical / stage (prospecting vs retargeting) / device.
- **Reward:** incremental visits per dollar (from the randomized holdout continuously running as the bandit's measurement plane).
- **Algorithm:** discounted Thompson sampling (non-stationarity), hierarchical priors pooled by vertical (long-tail), monotone-response exploitation.
- **Constraint:** bandit-with-knapsacks (respect delivery/pacing). **Actuation:** write the cap; no new control surface.
- **Rollout:** shadow (recommend, log) -> ring-fenced live on high-volume default-cap campaigns -> expand by vertical/stage.

---

## 8. Open threads for the next session

1. **Combined platform sizing** (prospecting + retargeting + all stages, not two cohorts) with delivered frequency, HS x AHS purification of the "unscored"/warm mix, and a ≥30-45d visit tail.
2. **Purge shared-IP contamination:** flag NAT/CGNAT/high-device-count IPs (freq 21+ buckets) before quoting the high-frequency waste; consider MNTN-ID households where available.
3. **Within-household / within-advertiser** design to strip the selection confound in the observational curve (complements the RCT).
4. **RCT build:** the suppression-holdout (ghost-bid) mechanism, arm assignment sync to the bidder, and the durable results table (GCS/BQ, Mode-compatible) on a schedule.
5. **Ownership:** the frequency knob lives in the bidder (`rtb-campaign-service`); identify the owning team as RCT co-owner + actuation partner.
6. **HHST as Phase 2:** once the randomized-holdout lift infrastructure exists, reuse it to unblock the HHST intent-gate bandit.

---

## 9. Data appendix (exact identifiers, for reproduction)

- Spend + score + IP at impression grain: `dw-main-silver.logdata.cost_impression_log` — typed `ip`, `advertiser_id`, `campaign_id`, `impression_id`, `time` (DAY partition), `household_score`, `advertiser_household_score`, `media_spend`+`data_spend`+`platform_spend`; `campaign_group_id` and `realtime_conquest_score` only in `model_params` (+17-20GB/day). 76B rows / 62TB — filter tight.
- Visits: `dw-main-silver.summarydata.ui_visits`; prospecting branch `source_type='last_tv_touch_visits'`, retargeting branch `source_type='visits'`. **Join to CIL on `impression_id` (the `.steelhouse` composite), NOT `ad_served_id`.** Dedup `(advertiser_id, guid, epoch, impression_id)`.
- Cohort: `dw-main-silver.audience.mm_campaign_classifier` (has_mm, objective_id, campaign_group_id; funnel_level=1 scoped). Retargeting: `dw-main-bronze.integrationprod.public_campaigns` funnel_level>=2.
- Spend denominator: `dw-main-silver.summarydata.sum_by_campaign_by_day`.
- Queries: scratchpad `qf1_hh_freq.sql` (prospecting curve), `qf3_retargeting_freq.sql` (retargeting), `qf2_leakage.sql` (leakage); perf log ticket `freq_cap_sizing`.
- Key 7d numbers: prospecting cohort spend $3.93M, freq≥8 = 12% of spend; retargeting spend $1.39M, freq≥8 = 31%; leakage 4.8% of households / 13% of impressions on 2+ groups.
