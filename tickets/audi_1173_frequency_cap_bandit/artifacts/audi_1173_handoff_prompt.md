# HANDOFF PROMPT — MNTN Frequency-Capping Bandit (AUDI-1173)

*You are picking up a data-science project at MNTN. Read the CONTEXT, then execute the PLAN: dispatch ONE agent per numbered step, respecting the dependencies. All figures are verified against BigQuery unless marked "directional/correlational."*

## CONTEXT

**Company.** MNTN is a performance Connected-TV (CTV) advertising DSP ("Performance TV"). The KPI is a **verified visit** (a household visits the advertiser's site after seeing a video ad; multi-day, long-tailed attribution; no clicks in CTV). The bidder runs real-time bidding on CTV inventory.

**The decision.** Build a **lift-aware frequency-capping bandit** as MNTN's first practical Multi-Arm Bandit, **but gate the build on a household-randomized RCT** — the observational data cannot prove capping recovers value. Chosen over an HHST intent-gate bandit because frequency is causally measurable now (a cheap RCT) while HHST lift is blocked in BigQuery (needs a GCS+Databricks build). Status: scoped, not validated. The RCT is the go/no-go.

**The lever (frequency capping, from codebase investigation).** Caps live in `bidder.frequency_caps` / `dso.frequency_caps` as `(cap, duration_secs)` pairs, `object_type` in {campaign, campaign_group}; enforced in `do_fcap` (repo `SteelHouse/rtb-campaign-service`) before the bid; Redis counters `rtb:frequency:{ip}:campaign_group_id=<cg>:campaign_id=<c>`. Keyed on **IPv4** (household; MNTN-ID only when `uses_mntn_id`). **No advertiser dimension / no rollup key** → no cross-advertiser cap, and frequency leaks across an advertiser's campaign_groups. **True rolling window.** A universal 1-imp/30min default policy is appended to all campaigns. **Fails OPEN on Redis error** (delivered frequency > configured). Advertiser custom caps carry `has_custom_frequency_caps` — a bandit touches ONLY the default cap.

**Phase-0 sizing (BigQuery, 7d, household = ip x advertiser; 2 adversarial passes; CORRELATIONAL).**
- Prospecting (MM Stage-1) $3.93M/7d: **~12% of spend at freq >=8** (~$2M/30d); per-household visit rate plateaus ~1.1% across freq 4-20.
- Retargeting (funnel>=2) $1.39M/7d: **~31% of spend at freq >=8** — biggest pool, but warm-user visits are the least incremental.
- Cross-group leakage: **4.8% of households / 13% of impressions served by 2+ campaign_groups** (no-rollup control-plane defect) — actionable WITHOUT an RCT.
- LOAD-BEARING CAVEAT: attributed visits-per-impression decline is partly a **mechanical last-touch attribution artifact** (~1/n by construction), NOT clean diminishing returns. The observational curve CANNOT justify a cap. The RCT metric must be **total visits per household + cost-per-household**, never attributed visits/impression.

**Data appendix (BigQuery, project `dw-main-silver`; run via the bq_run.sh wrapper, location us-central1).**
- Impression grain (spend + score + IP): `logdata.cost_impression_log` (CIL). Typed cols: `ip`, `advertiser_id`, `campaign_id`, `impression_id`, `time` (DAY partition, filter tight), `household_score`, `advertiser_household_score`, `media_spend`+`data_spend`+`platform_spend`. `campaign_group_id` + `realtime_conquest_score` are only in `model_params` (+17-20GB/day). 76B rows / 62TB.
- Visits: `summarydata.ui_visits` — prospecting `source_type='last_tv_touch_visits'`, retargeting `source_type='visits'`. **Join CIL on `impression_id` (the `.steelhouse` composite), NOT `ad_served_id`.** Dedup `(advertiser_id, guid, epoch, impression_id)`. Attribution long-tailed (p90=28d) → use a >=30-45d visit window.
- Cohorts: `audience.mm_campaign_classifier` (has_mm, objective_id, campaign_group_id; funnel_level=1). Retargeting: `bronze.integrationprod.public_campaigns` funnel_level>=2. Spend denominator: `summarydata.sum_by_campaign_by_day`. Exclude WGU (advertiser_id 31357).

**HHST alternative (Phase 2).** The HHST intent-gate bandit is the deeper play but its lift measurement is blocked in BQ. Build it AFTER the frequency RCT stands up the randomized-holdout lift infrastructure it can reuse.

**Reference:** AUDI-1173; full scope doc `tickets/audi_1173_frequency_cap_bandit/artifacts/audi_1173_scope.md`; queries in that ticket's `queries/`.

---

## PLAN — dispatch ONE agent per step

**Step 1 — Ownership + holdout feasibility  (agent: bidder-investigator)  [UNBLOCKER; no deps]**
- Goal: determine (a) which team owns the fcap knob (`rtb-campaign-service`), (b) whether the approved ghost-bidding work can produce the RCT's **suppression-holdout arm** (would-have-served, suppressed), (c) the write path to sync per-household cap-arm assignment into the bidder cache.
- Method: read `SteelHouse/rtb-campaign-service` (fcap crate, `do_fcap`, `campaign_thresholds` sync), the ghost-bid register, `campaign-metadata-service`; identify code owners.
- Deliverable: 1-page feasibility + ownership memo. Done-when: holdout mechanism confirmed feasible-or-not, with a named owner.

**Step 2 — Finalize the RCT design  (agent: experiment-designer)  [dep: Step 1]**
- Goal: a full experiment design (prospecting-first) from scope doc section 6.
- Specify: arms {control, cap 8/wk, cap 3/wk, suppression-holdout}; household randomization `MOD(ABS(FARM_FINGERPRINT(CONCAT(advertiser_id,':',ip))),1000)`; **primary metric = total visits per household + cost-per-household**; power (~636K households/arm for a 5% relative visit-rate change at 80%); 4-week run; advertiser-clustered bootstrap inference (point / 95% CI / p); guardrails (ring-fence from concurrent experiments, respect `has_custom_frequency_caps`, exclude AID 90, monitor `fcap_impressions_fetch{outcome=redis_err}`); pre-registered go/no-go bar.
- Deliverable: RCT design doc + pre-registration. Done-when: design reviewed, go/no-go threshold agreed.

**Step 3 — Refined observational sizing  (agent: data-analyst)  [parallel; no deps]**
- Goal: tighten the magnitude for the decision doc. Re-run the reach-frequency curve with DELIVERED frequency (not configured), a shared-IP purge (flag NAT/CGNAT/high-device IPs), a >=30-45d visit tail, combined prospecting+retargeting+all stages, and an HS x AHS crosstab (separate cold prospecting from warm revisitors). Report household-grain total visits + cost-per-household by freq bucket (NOT attributed visits/impression).
- Deliverable: refined sizing tables + charts. Done-when: numbers stable across a full week; shared-IP tail quantified/excluded.

**Step 4 — Leakage quick-win  (agent: control-plane-analyst)  [parallel; no deps]**
- Goal: the no-rollup defect is actionable without an RCT. Quantify cross-group AND cross-stage frequency leakage per (ip, advertiser), estimate the over-delivery it causes, and propose the fix (advertiser-level cap rollup / consolidated counter).
- Deliverable: leakage brief + concrete control-plane recommendation to the bidder team. Done-when: leakage $ impact quantified and fix scoped.

**Step 5 — Decision doc / RFD  (agent: rfd-author)  [dep: Steps 2, 3, 4]**
- Goal: synthesize a Request-for-Decision (Confluence) for buy-in + headcount: thesis, sized opportunity (Step 3), RCT plan + go/no-go (Step 2), immediate leakage fix (Step 4), sequencing (frequency now, HHST Phase 2), and the ask (run the RCT). Lead with the decision, not a savings number.
- Deliverable: RFD draft. Done-when: reviewed, ready to circulate.

**Step 6 — Bandit + offline-replay design  (agent: bandit-designer)  [dep: Step 2 reward definition]**
- Goal: spec the post-RCT bandit — discounted Thompson sampling on the default cap; context = campaign_group / vertical / stage; reward = incremental visits per dollar from a continuously-running randomized holdout; bandit-with-knapsacks pacing constraint; actuation via the existing sync path. Include an offline-replay evaluation on logged cap history to estimate regret before going live.
- Deliverable: bandit design + offline-replay eval plan. Done-when: ready to implement pending RCT go.

**Execution order.** Steps 1, 3, 4 start in parallel. Step 2 waits on Step 1. Step 5 waits on 2+3+4. Step 6 waits on 2. **Critical path to a decision: 1 → 2 → 5.**
