# AUDI-1173 — Frequency-Cap RCT: Finalized Design

*Step 2 deliverable. Corrects and supersedes scope §6. MNTN = performance CTV DSP; KPI = advertiser site visit; household = `(advertiser_id, ip)`. Companion: the LOCKED pre-registration `audi_1173_rct_prereg.md` (freeze that before any downstream consumption). Actuation reality from `audi_1173_ownership_feasibility_memo.md` (Step 1).*

**Status: DRAFT for author (Malachi) review — vetoable.** **Owner decisions locked in this revision:** (1) **PRIMARY = mean TOTAL site visits per household (a COUNT)** — non-inferiority via household bootstrap; binary incidence `P(≥1 visit)` demoted to secondary/diagnostic. (2) **Margin δ = 5% RELATIVE** of the control mean (fixed value). Total-visit **source approach is decided** (`PENDING-A`: a **custom `guid_log` join** on `(advertiser_id, ip)` over a post-anchored `[first_impression, +30-60d]` window carrying RCT arm membership — the join is to be BUILT; `enriched.lift__ghost_bid_visits` is demoted to a platform sanity/reference only, since it is 7d-windowed on ghost/submitted arms, not the ≥30d RCT-arm estimand). Still provisional and marked inline: (a) the freq≥9-stratum **count mean + variance** that anchor N (`PENDING-B/C` — provisional μ0≈3% incidence-proxy/N≈198K now; Step 3's total-visit-by-frequency curve confirms at Checkpoint β) — **N is off the critical path** (fill ≪ exposure + maturation), so the design is decision-ready without a final N, whose exact anchor is a pre-lock measurement; (b) Step 3's refined household visit rate / shared-IP flag. **Statistics correction inside:** reporting scale is **RELATIVE** (coverage-robust), correcting the probe's absolute-pp recommendation (§5.4). Everything else is final.

---

## 1. Objective and the decision it serves

**Question:** Does tightening the *default* impression frequency cap PRESERVE household site visits while REDUCING cost per household — i.e., is the high-frequency tail of MNTN delivery incremental, or is it waste we can cut without losing visits?

**Decision it serves:** whether to ship an adaptive frequency-cap bandit (AUDI-1173 Phase 2) that lowers the default cap on the segments where the tail is non-incremental. The bandit's reward is the per-cap incremental-value curve this RCT measures. North-star tie: total-traffic incrementality mandate (honest total-visit signal over last-touch attributed VV) + cost reduction on non-incremental spend.

**Framing as non-inferiority + superiority (not point-estimate lift).** The observational curve (§4 scope) shows per-household visit rate PLATEAUS across freq 4-20, which — if causal — means the tail buys impressions that add no visits. The right question is therefore not "how big is the lift" but "can we cut the tail without losing visits." So:

- **Primary (visits): non-inferiority on the COUNT.** The sized statistic is **mean total site visits per household** (a count) — directly the "preserve total visits" objective. A tighter cap must not reduce it by more than a pre-registered margin (δ = 5% relative). Binary incidence `P(≥1 visit)` is a **secondary/diagnostic** only: sizing on incidence is biased toward GO (it can hold flat while the count — the thing the objective protects — drops), so the sized statistic must be the count, not the proportion.
- **Primary (cost): superiority.** A tighter cap must reduce cost/household.
- **GO = non-inferior on the total-visit count AND cheaper on cost.** That is the bandit's green light.

---

## 2. Deviations from scope §6 (each vetoable — one-line rationale)

| # | Scope §6 said | This design does | Why |
|---|---|---|---|
| **D1** | Analyze the **affected set = realized freq≥8**. | Define eligibility **ex ante** from a 2-4wk pre-period (predicted-to-exceed-cap), randomize the full population, analyze **ITT on the ex-ante eligible stratum**, per arm. | Realized frequency is CAUSED by the cap (a cap-3 household can never appear in freq≥8) → conditioning on it is collider selection that breaks randomization. Ex-ante prediction is a pre-treatment stratifier, so it doesn't. |
| **D2** | Hypothesis = estimate lift / incremental visits-per-dollar. | **Non-inferiority on visits/household + superiority on cost/household**, with a pre-registered numeric margin as the go/no-go bar. | The plateau means the interesting result is "no visit loss at lower cost," which a lift point-estimate frames poorly. NI states the actual decision bar. |
| **D3** | Analyze on the full cohort (implied by §6's population line). | Analyze on the **ex-ante eligible stratum only**, sized there. | freq≥8 is ~2.2% of prospecting households; a real 5%-relative effect on the bound set dilutes ~45× to ~0.1% on the full cohort — far below the MDE 636K buys. Effect must be measured where the cap binds. |
| **D4** | Inference = **advertiser-clustered bootstrap** (imported from the tiered-rollout protocol) + optional CausalImpact. | Randomization unit = analysis unit = **household**. Primary (binary) → **two-proportion z-test**; ratios → **bootstrap resampling HOUSEHOLDS**. Advertiser = stratification/CUPED covariate, **not** the resample cluster. | Household is the randomization unit; clustering to a few hundred advertisers collapses N from ~636K to hundreds and contradicts the power calc. Cluster-bootstrap is for wave-flip rollouts (unit = advertiser), not a household RCT. |
| **D5** | Ship **4 arms** incl. **H — suppression holdout** (would-have-served suppressed). | Ship **3 arms {A control, B cap 8/wk, C cap 3/wk}**. Arm H = a NEW cap-aware **partial** suppression feature, **Phase-2/optional**. | The existing ghost machinery is a fixed ~10-16% binary suppress-ALL, frequency-blind selection — not a tunable cap-aware partial holdout. The 3 served arms all have impressions, so the visit metric is well-defined and the ghost frequency bias doesn't apply. (Arm H's metric-side blocker is now removed — see §6 — but it still needs a new bidder feature.) |
| **D6** | Randomize via `FARM_FINGERPRINT(advertiser:ip) mod 1000`. | Randomize via the **TI-837-validated `MD5` bucket** — `MOD(ABS(CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip))), 1, 16)) AS INT64)), 1000)` (16 hex chars; `CAST(advertiser_id AS STRING)` in the preimage) on BOTH bidder and BQ sides; arms carved from buckets **100-999**, disjoint from the platform holdout **0-99**. | The bidder computes MD5 already; bit-matching BigQuery FarmHash-Fingerprint64 in Rust is error-prone. This exact preimage + 16-hex reduction is the production-equivalent that bit-matches the platform 0-99 holdout, so carve-from-100-999 guarantees no RCT household is also a platform-holdout household. (Step 1 memo, hash-consistency; source of truth `data_knowledge.md`.) |
| **D7** | Arms B/C are config/sync changes on `bidder.frequency_caps`. | Arms B/C require a **new `@SteelHouse/rtb` bidder feature** (per-household bucket→arm→cap in `do_fcap`) — a **hard prerequisite**; the RCT cannot start until it ships. | Confirmed in code: the cached `CampaignModel` has no per-household cap field, so the DB→cache path cannot carry a per-bucket cap. Config-only is foreclosed. (Step 1 memo.) |
| **D8** *(owner change, post-scope)* | Primary metric = MNTN-attributed verified visits (`ui_visits`), reported as **absolute pp**. | Primary = **mean attribution-independent TOTAL site visits per household (a COUNT)**, NI via **household bootstrap** (source = a **custom `guid_log` join** on `(advertiser_id, ip)` over `[first_impression, +30-60d]` carrying RCT arm membership; `enriched.lift__ghost_bid_visits` = platform sanity/reference only). Binary incidence `P(≥1 visit)` and attributed VV → **secondary/diagnostic**. **Reporting scale = RELATIVE** (coverage-robust under multiplicative cross-device miss — corrects the probe's "absolute pp cancels," which is backwards); absolute pp = companion. **Margin δ = 5% relative — OWNER-DECIDED value (fixed).** | The COUNT is what "preserve total visits" protects; incidence can hold flat while the count drops, so sizing on incidence is biased toward GO. Frequency DRIVES last-touch attribution → the higher-frequency control arm wins the tiebreak more often, inflating its attributed VV and biasing the contrast AGAINST capping; total visits removes it. Under `observed = c·true` coverage, the relative contrast is coverage-invariant while absolute is scaled by `c` — so relative is primary. (Full rationale §5.1-5.4.) |

**None of D1-D8 is silent.** If the author rejects any row, revert that row and re-open the affected section.

---

## 3. Eligible-stratum definition (ex ante, per arm) — the D1/D3 fix

**Principle:** eligibility is a **pre-treatment stratifier**, computed from data that predates (and is untouched by) the cap. It must never be realized in-experiment frequency.

**Build:** from a **2-4 week pre-period** immediately preceding randomization, compute each `(advertiser_id, ip)` household's baseline **delivered** weekly impression frequency from that advertiser (`cost_impression_log`, delivered not configured — fails-open/leakage means delivered ≠ configured; measure delivered). Eligibility rule (both arms): **predicted ≥ cap + 1**, i.e. the cap actually suppresses at least one impression. Classify each household by predicted frequency absent any cap change:

- **Arm B eligible stratum** = households predicted **≥ 9 imp/wk** (cap-8 binds — it suppresses the 9th+). Fill proxy ≈ **the Phase-0 freq≥8 stock, ~1.62M/wk** (1.17M + 0.32M + 0.10M + 0.03M); this stock **overstates** the true ≥9 eligible (it includes the freq-8 households the cap does NOT bind) and is a **7-day STOCK, not a weekly inflow**, so fill built from it is a **lower bound** on time (real inflow ≤ stock → fill no faster). Measure the actual ≥9 inflow in the pre-period build.
- **Arm C eligible stratum** = households predicted **≥ 4 imp/wk** (cap-3 binds — it suppresses the 4th+). Observationally ≈ **12.5% of prospecting households**; fill proxy from the freq≥4 stock (adds freq 4-7 = 7.44M to the ≥8 stock), same 7d-stock-not-inflow caveat.

The SAME cap+1 classifier is applied to control-arm households, so all three arms carry comparable eligible strata. Analysis contrasts:
- **B vs A** on the **freq≥9** eligible stratum,
- **C vs A** on the **freq≥4** eligible stratum.

**Operationalization (pre-flight):** the simplest classifier is mean weekly delivered frequency ≥ (cap+1) over the pre-period; a predicted-frequency model is optional refinement. Measure the actual predicted-eligible inflow in the pre-period build — the observational freq≥8 stock (~1.62M/wk) is the fill proxy (a 7d stock, over-counts the ≥9 eligible → fill is a lower bound); confirm it before locking the calendar.

**Why not the collider:** a cap-3 household physically cannot reach realized freq≥9, so "analyze realized freq≥9" compares arm A's tail to an EMPTY arm-C tail — pure selection on a post-treatment variable. Ex-ante eligibility (predicted ≥ cap+1) is fixed before treatment, so the strata are exchangeable across arms.

---

## 4. Arms and bucket allocation (D5 + D6)

**Hash (both sides) — the TI-837-validated production-equivalent:**

```sql
MOD(ABS(CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip))), 1, 16)) AS INT64)), 1000)
```

**16 hex chars** (not 15) and **`CAST(advertiser_id AS STRING)`** in the preimage — this exact form is what bit-matches the platform 0-99 holdout and the bidder, making arms 100-399 / 400-699 / 700-999 **genuinely disjoint** from the platform holdout. **The bidder must compute the IDENTICAL preimage** (`CAST(advertiser_id AS STRING)`, colon-joined to `ip`, no trailing space) **and the identical 16-hex reduction** before the mod-1000. Pin the exact byte encoding of `advertiser_id:ip` and the 16-hex/mod-1000 reduction in the joint @SteelHouse/rtb + analysis spec before implementation. Deterministic, sticky, disjoint.

**Bucket map (mod-1000):**

| Bucket range | Assignment | Share of population |
|---|---|---|
| **0-99** | Platform 10% holdout — **excluded from the RCT entirely** | 10% |
| **100-399** | **Arm A — Control (BAU default cap)** | 30% |
| **400-699** | **Arm B — Cap 8 / rolling wk** (default-cap campaigns only) | 30% |
| **700-999** | **Arm C — Cap 3 / rolling wk** (default-cap campaigns only) | 30% |

- Arms are equal thirds of the **non-holdout** population so the rare freq≥8 stratum fills fast (§7 fill clock). Fill time scales inversely with a smaller allocation, so this is the fastest clean split; shrink the arms only if 60% of default-cap traffic under a modified cap is deemed too large a footprint (note: only ~2.2% of arm-B and ~12.5% of arm-C households ever experience a *binding* cap — most see BAU behavior).
- **Cap is on the rolling window** the bidder already enforces (`now − impression_ts ≤ duration`); express 8/wk and 3/wk as the rolling-7d equivalent. Default cap only — never touch `has_custom_frequency_caps` campaigns.
- **Disjoint-from-holdout is by construction:** arms live in 100-999, holdout in 0-99, same hash → no RCT household is also platform-suppressed.

---

## 5. Metrics and estimands

### 5.1 Primary outcome — mean TOTAL site visits per household, a COUNT (attribution-independent) [D8]

**Definition:** total advertiser site visits by the household (IP) over the exposure + maturation window, from the site pixel / total-traffic signal — **independent of MNTN last-touch attribution**.

- **Primary sized statistic:** **mean total site visits per household (a COUNT)** on the eligible stratum. **Non-inferiority via HOUSEHOLD BOOTSTRAP** — resample households with replacement (≥ 5,000 resamples), report **point / one-sided lower 95% bound / p-value** on the RELATIVE contrast `(μ_T − μ_C)/μ_C` (§8). This is the statistic that MATCHES the objective ("preserve total visits" is a statement about the count, not a proportion). Anchor = **mean total visits/hh `μ0_total`** on the freq≥9 stratum (all three arms serve; see §5.3 + §6).
- **Demoted to secondary/diagnostic:** binary per-household **total-visit incidence** `P(≥ 1 total site visit)` — reported beside the primary count as a robustness read, NOT sized on and NOT the go/no-go statistic. Incidence can hold flat while the mean count falls (the tail buys extra visits from already-visiting households), so it is biased toward GO and cannot be the primary.

**Why total visits, not attributed VV (put in the deck):**
1. **Snipe.** MNTN is top-funnel CTV; the visit lands days later, so competing advertisers snipe MNTN's last-touch credit → attributed VV *undercounts* true visits.
2. **Decisive — frequency drives attribution.** The higher-frequency control arm wins the last-TV-touch tiebreak more often, inflating its attributed VV even for households that would have visited anyway. That is the §4d last-touch artifact operating INSIDE the experiment, and it biases the contrast AGAINST capping. Total visits removes it.
3. **Mandate + arm-H unblock.** Total traffic is the org incrementality signal (honest vs clickpass). It also makes a never-served holdout measurable: a never-served household still has total site-visit records via the site pixel, whereas attributed VV is zero by construction. (Arm H's metric-side blocker is thus removed; it still needs the bidder feature — §11.)

**Source approach decided (owner probe — `audi_1173_total_visit_signal_probe.md`):** the total-visit signal is validated; the RCT-grade SOURCE is a custom join to be built (below). The freq≥9-stratum count mean + variance are provisional (§5.3) pending Step 3's total-visit-by-frequency curve, dropped in at Checkpoint β.

> **`PENDING-A` — total-visit source (RCT-grade join, to be BUILT):** primary = a **custom `dw-main-silver.logdata.guid_log` join** on **`(advertiser_id, ip)`** (ip CIDR-stripped), counting each household's total site visits over a **post-anchored `[first_impression, +30-60d]` window that carries RCT arm membership** (arm from the §4 bucket hash). This is the ONLY source that yields the ≥30d, arm-A/B/C estimand the design requires. **`enriched.lift__ghost_bid_visits` is DEMOTED to a platform sanity/reference read only** — it is hard-coded to a **7-day** window and its arms are **ghost/submitted**, not the RCT's A/B/C, so it cannot deliver the ≥30d RCT-arm estimand (use it only to cross-check that the custom join reproduces the platform's 7d served/holdout rates). Attribution-independent: **yes** (site-pixel page views, fire with no impression; ghost/holdout reference empirically 0.886% 7d visit rate at 0.0% won-rate).
> **`PENDING-B` — count anchor `μ0_total` + variance (provisional):** the sizing anchor is the **mean total visits/hh on the freq≥9 stratum** over the ≥30-60d window (plus its household variance) — a COUNT, not a rate. Reference incidence points from the 7d probe: platform holdout (never-served) **~0.886%**, served (bid-on) **~1.55%**; the freq≥9 (heavily-served) stratum sits ABOVE the served average, and the RCT window is ≥30-60d (vs 7d) → the mean count is materially higher than these 7d incidence figures suggest. Provisional incidence-proxy anchor **~3%** (see §5.3); retargeting **~15-35%** (attributed-rate proxy — total ≥ attributed, so true is higher). Exact freq≥9-stratum count **mean + variance** ← Step 3's total-visit-by-frequency curve at Checkpoint β (this is the pre-lock measurement that fixes N).

### 5.3 Anchor — the count mean that sizes power (the served-arm point)

The RCT has **no never-served arm** (arm H is Phase-2). Arms A/B/C all serve impressions — they differ only in the *cap*. So the anchor that sizes the primary count contrast is the **SERVED** mean total visits/hh, **not** the 0.886% platform-holdout (counterfactual) rate. From the 7d probe, served incidence ≈ **1.55%** (platform-average across served frequencies); the count mean over the RCT's ≥30-60d window is materially higher.

The sizing stratum is **freq≥9**, the heavy-served tail. Its mean total visits/hh is **strictly above the served average** for two reasons: (a) more exposure, and (b) the targeting selects these households as higher-intent, so they visit more organically. The exact freq≥9 count mean **and its household variance** are **not yet measured** — they are the endpoint of **Step 3's total-visit-by-frequency curve** (produced in parallel), and the two inputs the count-sizing formula (§6) needs. Until they land, use a **provisional incidence-proxy anchor ≈ 3%** (≈2× the served average; a deliberately conservative-low guess so N is not under-sized) and read the reference N off the §6 grid. Two further reasons the true anchor is likely **above** 3%: the RCT window is **≥30-60d** (vs the probe's 7d → more visits per household), and freq≥9 sits on the steep part of the exposure curve.

**Retargeting anchor is a PROXY.** The retargeting **~15-35%** figure is anchored on **ATTRIBUTED** visit rates (the only retargeting number on hand); total visits ≥ attributed by construction, so the true total-visit anchor is **higher** → retargeting is even more over-powered than the grid shows. **Re-confirm all anchors at Checkpoint β and re-solve §6/§7 before freezing enrollment** — but note N is off the critical path (§6/§7), so this does not gate design readiness.

### 5.4 Reporting scale — RELATIVE is the coverage-robust primary (corrects the probe) [flag for Checkpoint β]

> **Note — the sibling probe memo's "report absolute pp" advice is OVERRIDDEN here.** This §5.4 supersedes it: under multiplicative cross-device coverage the RELATIVE scale cancels `c` and the absolute scale does not. Report relative as primary.

The probe recommended reporting **absolute** lift on the argument that cross-device coverage (~85-90%, arm-symmetric) "cancels." **From first principles this is backwards under the coverage model that actually applies.** Cross-device miss is a **multiplicative thinning**: each true visit is observed only if it lands on the same IP the arm was keyed to, with per-arm capture probability `c ≈ 0.85-0.90`. So `observed_mean = c · true_mean` (the same identity holds for the count mean μ and for the incidence rate), same `c` both arms (arm-symmetric). Then, with μ the mean total visits/hh:

- **Absolute observed difference** = `c·μ_T − c·μ_C = c·(μ_T − μ_C)` → the true absolute gap **SCALED by `c`** (shrunk ~10-15% toward zero). It does **NOT** cancel; it is biased.
- **Relative observed difference** = `(c·μ_T − c·μ_C) / (c·μ_C) = (μ_T − μ_C)/μ_C` → the `c` **cancels exactly**. Coverage-**INVARIANT**.

Under multiplicative coverage the **relative** contrast is the coverage-robust one. (The absolute pp scale would only be robust under an **additive-floor** model — `observed = true + f` — which would fit a constant background of spurious visits added equally to both arms. Cross-device miss is a downsampling of *true* visits, not an added floor, so multiplicative governs. The shared-IP purge (guardrail 1) removes the main additive-inflation source, leaving multiplicative miss as the dominant residual.)

**Consequence for reporting + margin:**
- **Primary reporting scale = RELATIVE** `(μ_T − μ_C)/μ_C`. Absolute (visits/hh) is a **companion**, labeled as **coverage-attenuated by ≈`c`** (report both, but headline the relative).
- **Margin must be RELATIVE** (5% relative of the control **mean** — owner-decided value, §9). A relative margin makes the whole NI test coverage-invariant: `δ = 0.05·μ_C` scales with `c` exactly as the observed effect does, so the `c` cancels on both sides of the test inequality. A **FIXED absolute margin** would **not** scale with `c` → the coverage-shrunk observed effect would look smaller than a fixed absolute margin → **anti-conservative** (too-easily declares non-inferiority). Never use a fixed absolute margin here.
- **Power** is computed at the **observed** (coverage-attenuated) count mean + variance — so no coverage correction to N is needed; the observed anchor is what the bootstrap sees.

**Owner decision (locked):** the reporting scale (relative primary) is a statistics correction fixed by the coverage model, and the **margin VALUE is now DECIDED: δ = 5% relative** (no longer an open 3%/10% choice). Both are set; N/anchor confirmation remains for Checkpoint β.

### 5.2 Secondary / diagnostic

| Metric | Role | Estimand | Inference |
|---|---|---|---|
| **cost/household** | **Primary VALUE driver (superiority)** | mean spend per eligible household, tighter-cap arm < control | household bootstrap, one-sided |
| **binary total-visit incidence** `P(≥1 visit)` | **secondary/diagnostic** — robustness read beside the primary count; NOT sized on, NOT the go/no-go bar (biased toward GO — §5.1) | `P(household made ≥1 total visit)`, relative contrast | **two-proportion z-test, NI form** (the ONLY place this test is used; the primary is the count via bootstrap) |
| **visits/dollar** | efficiency headline | total visits ÷ spend on the eligible stratum, tighter-cap arm > control | household bootstrap |
| **MNTN-attributed VV/household** (`ui_visits`) | **diagnostic** — quantifies the attribution bias direction (expect control's attributed VV inflated vs its total visits) | mean attributed VV per household | bootstrap / two-proportion |
| **delivered-frequency distribution** | manipulation check | per-arm delivered freq (measure delivered, not configured — fails-open/leakage) | descriptive |

**Estimand for all:** ITT (bucket-assigned), on the ex-ante eligible stratum, prospecting and retargeting as **separate strata**.

---

## 6. Power — sized on the COUNT mean + variance (N off the critical path) [D8 re-derivation]

**Primary method — size on the COUNT, not a proportion.** The primary statistic is the mean total visits/hh, tested by household bootstrap (§8), so N is sized on a **mean-difference**, not a two-proportion binomial. Per-arm N to detect a **5% relative** change in the mean at 80% power, α=0.05 two-sided:

`N ≈ (z_{0.975}+z_{0.80})² · (σ_C² + σ_T²) / δ²`, with `δ = 0.05 · μ_C` (5% relative of the control mean) and `σ` the per-household count SD on the freq≥9 stratum.

**The two inputs are `μ_C` and `σ_C` — the freq≥9-stratum count mean and its household variance (§5.3), a COUNT, not a rate.** Both are **provisional** and are the **pre-lock measurement**: Step 3's total-visit-by-frequency curve delivers the mean-visits/hh-by-frequency (→ `μ_C`) and its dispersion (→ `σ_C`) over the ≥30-60d window. **N is deliberately OFF the critical path:** fill is ≤ ~1 wk at any plausible N (§7), far below exposure (4 wk) + maturation (6-8 wk), so the calendar does not move with N and **the design is decision-ready WITHOUT a final N**. The exact N is a mechanical drop-in once `μ_C`/`σ_C` are measured at Checkpoint β; it is not on the path to locking the design.

**Reference (SECONDARY-incidence) sizing — the binomial grids below size the DEMOTED binary incidence metric (§5.2), not the primary count.** They also bound the count N loosely (a count mean with Poisson-like dispersion `σ² ≈ μ` sizes near its incidence proxy; overdispersion raises it). Use them for order-of-magnitude only; the primary N comes from the mean-difference formula above once `σ_C` lands.

**RELATIVE grid — N/arm (80% power, α=0.05 two-sided), by incidence base rate × relative MDE** *(secondary-incidence reference; NI direction `p1=(1−MDE)·p0`)*:

| `p0_total` | MDE 5% rel | MDE 3% rel | MDE 2% rel |
|---|---|---|---|
| 1.0% *(old attributed anchor — reference only)* | ~606K | ~1.70M | ~3.85M |
| 1.5% *(served platform-avg, 7d)* | ~402K | ~1.13M | ~2.55M |
| **3.0%** *(**provisional freq≥9 incidence-proxy anchor**, §5.3)* | **~198K** | ~556K | ~1.26M |
| 5.0% | ~116K | ~327K | ~739K |
| 10% | ~55K | ~155K | ~350K |
| 20% *(retargeting low end)* | ~25K | ~69K | ~156K |
| 30-35% *(retargeting)* | ~12-14K | ~32-40K | ~73-91K |

**ABSOLUTE-pp grid — N/arm (companion; fixed-pp margins are NOT coverage-robust, §5.4 — use for context only):**

| `p0_total` | MDE 0.25 pp | MDE 0.5 pp | MDE 1.0 pp |
|---|---|---|---|
| 1.5% | ~34K | ~7.7K | ~1.6K |
| **3.0%** | **~70K** | ~17K | ~3.8K |
| 5.0% | ~116K | ~28K | ~6.7K |
| 10% | ~224K | ~55K | ~13K |
| 20% | ~400K | ~100K | ~25K |
| 30% | ~526K | ~131K | ~33K |

*(N ≈ `7.849·[p0(1−p0)+p1(1−p1)]/(p1−p0)²`. At 5% rel, `p1=(1−0.05)·p0`. The 1.0%/5%-rel cell = 606K matches the scope's ~637K to within the NI-vs-superiority variance-direction difference (~5%).)*

> **`PENDING-C` — PROVISIONAL sizing (count primary via bootstrap; incidence-grid reference only until `μ_C`/`σ_C` land at Checkpoint β from Step 3's freq≥9 curve):**
> **Recommended reference cell — prospecting:** incidence-proxy anchor ≈ 3%, `MDE_rel = 5%`, **reference N/arm ≈ 198K** (5% anchor → ~116K; 1.5% served-avg floor → ~402K — all fill inside the calendar, below). **The primary count N supersedes this** once `μ_C`/`σ_C` are measured; N is off the critical path either way.
> **retargeting:** attributed-proxy anchor ≈ 20-35% (returning visitors; total ≥ attributed → true higher), `MDE_rel = 5%`, **reference N/arm ≈ 12-25K** (over-powered; binding constraint = incrementality SIZE, not N — and it is the LEAST incremental stratum, §4b scope).

**Sizing is on the BINDING stratum (freq≥9, arm B):** that stratum is the rarest, so its fill governs the calendar (§7). Arm C's freq≥4 stratum is much larger → never binding on fill.

**Re-verify at Checkpoint β:** when Step 3's freq≥9 count mean + variance land, plug `μ_C`/`σ_C` into the mean-difference formula, re-confirm N, then lock — but do not treat N as gating the design (it is off the critical path).

---

## 7. Calendar — three clocks (~10-12 weeks)

| Clock | Length | Governed by |
|---|---|---|
| **1. Arm-fill** | **≤ ~1 wk** (prospecting freq≥9) | The freq≥8 **7d STOCK** ~1.62M (a stock, not a weekly inflow — over-counts the ≥9 eligible, so weekly inflow is LOWER and this fill is a **lower bound on time**) splits 3 arms; at any reference N/arm in the §6 grid (~116K–402K) fill lands **≤ ~1 wk**. Retargeting fills faster (larger + over-powered). **Fill is ≤ ~1 wk at every plausible N → the exposure + maturation clocks dominate; N is off the critical path (§6).** *(Re-confirm once the ≥9 inflow is measured at β.)* |
| **2. Exposure** | **4 wk** | Active capping window per household — long enough for the rolling cap to bind repeatedly and for delivered-frequency contrast to separate. |
| **3. Visit maturation** | **6-8 wk** (≈45-60d past last impression) | Visits have a long tail (last-TV-touch p90 = 28d; §4d wants ≥30-45d). **Govern via `ui_visits.time` / the total-visit event timestamp — extend ~45-60d past last impression. NOT `visit_day` (capped at 14).** |

**End-to-end:** pre-period build (2-4wk, parallel) → fill (≤ ~1wk) → exposure (4wk, impressions through ~wk 5) → maturation (through ~wk 11-13). The **~10-12wk total is set by exposure (4wk) + visit maturation (6-8wk), not fill** — fill is off the critical path at every plausible N. **Go/no-go is read at tail maturity (~wk 11-13). Interim reads are DIRECTIONAL-ONLY** — do not gate on them (visit tail immature → biased toward null-of-no-effect early).

---

## 8. Inference (D4)

**Randomization unit = analysis unit = household.** No advertiser clustering.

- **Primary (mean total visits/hh — a COUNT): non-inferiority via HOUSEHOLD BOOTSTRAP.** Resample households with replacement (≥ 5,000 resamples); report **point / one-sided lower 95% bound / p-value** on the **RELATIVE** contrast `(μ_T − μ_C)/μ_C` (§5.4 — coverage-robust; absolute visits/hh as a coverage-attenuated companion). Margin **δ = 5% relative** of the control mean (`δ = 0.05·μ_C` — owner-decided, §9). Because the margin is a relative fraction of the control mean, the test is **coverage-invariant** under multiplicative cross-device miss (observed effect and `δ` both scale by `c`, which cancels); a **fixed** absolute margin would NOT be, so never use one. GO on visits requires the one-sided lower 95% bound of `(μ_T − μ_C)/μ_C` to sit above `−0.05`. N per arm = §6 (count mean-difference sizing). **Resample households — not advertisers, not days.** The two-proportion z-test is NOT used here (it is the count that the objective protects); it is retained only for the demoted incidence secondary.
- **Secondary — binary total-visit incidence: two-proportion z-test, NI form**, same relative scale — a robustness read beside the primary count (§5.2), never the go/no-go bar.
- **Other secondary ratios (cost/household, visits/dollar): nonparametric bootstrap resampling HOUSEHOLDS**, 5,000+ resamples. Report point / 95% CI / two-sided p. Cost superiority = one-sided lower bound of the cost *reduction* above 0.
- **Advertiser is a stratification/blocking + CUPED covariate for variance reduction, NOT the resample cluster.** Block randomization within advertiser (and within prospecting/retargeting) so arms balance on advertiser mix. **CUPED on the COUNT** — regression-adjust each household's post-period total-visit COUNT on its **pre-period total-visit count** (a continuous covariate), carried through the household bootstrap; NOT a CUPED on a binary indicator. Pre-period exists → randomization holds → CUPED's zero-mean guarantee holds (20-50% SE reduction per `experimentation.md`). The covariate must be genuinely external to the response (pre-period, not the post outcome).
- **Optional convergence check:** arm-level daily total-visit series under the standard-protocol CausalImpact — as a secondary consistency read only, not the primary (population is a clean RCT, so the direct household bootstrap on the count is primary per the TI-504 lesson).

**Two-sided N is deliberately conservative:** a one-sided NI test at the same 5%-relative margin needs fewer (~×0.79 of the two-sided N via `(1.645+0.8416)²` vs `(1.96+0.8416)²`), so the sized N also powers a two-sided *superiority* read if a real harm exists.

---

## 9. Go / no-go bar (frozen — see prereg)

**GO (ship the cap / green-light the bandit on this stratum) requires BOTH:**
1. **Visits (the COUNT) non-inferior:** one-sided lower 95% bound (household bootstrap) of the **relative** contrast of the **mean total visits/hh** `(μ_T,total − μ_C,total)/μ_C,total` > `−δ`, with **δ = 5% relative** — the OWNER-DECIDED value on the coverage-robust scale (§5.4). Absolute visits/hh reported alongside as a coverage-attenuated companion. Binary incidence is a diagnostic beside it, not the bar.
2. **Cost superior:** cost/household reduction 95% lower bound > 0 (tighter cap is cheaper).

**NO-GO:** visits fail NI (cap destroys > δ of total visits) OR cost not reduced. **Diagnostic read regardless:** compare attributed-VV contrast vs total-visit contrast — the gap is the in-experiment attribution bias, and is itself a reportable finding (validates D8).

Decision is per stratum (prospecting vs retargeting) and per arm (cap-8 vs cap-3) — four cells, each its own GO/NO-GO.

---

## 10. Guardrails

1. **Shared-IP purge BEFORE randomization** (not after). Exclude NAT/CGNAT/high-device-count IPs from the eligible set up front — an IP with an implausible distinct-advertiser or distinct-household(guid) count (rule: > P99 of the distinct-guid/day distribution, or a fixed floor pending Step 3's shared-IP flag). Prefer clean single-household IPs. **Quantify the excluded volume as an external-validity bound.** *(Reuse Step 3's shared-IP flag when it lands.)*
2. **Site-wide-pixel eligibility [D8].** Restrict the RCT to advertisers with **site-wide pixel coverage** — total visits need an all-page pixel, not conversion-page-only. Advertisers without it are excluded; **note the narrowed advertiser universe as an external-validity bound.**
3. **Ring-fence** experiment households OUT of concurrent A/B and Fangorn holdouts (and out of the platform 0-99 holdout, by bucket construction).
4. **Default cap only** — never touch `has_custom_frequency_caps` campaigns.
5. **Exclude AID 90 (PSA)** and **WGU (31357)**.
6. **Monitor `fcap_impressions_fetch{outcome=redis_err}`** (fail-open). On Redis error the cap silently stops enforcing → always measure **DELIVERED** frequency, not configured; drop windows with elevated redis_err from the manipulation check.
7. **Prospecting and retargeting are separate strata.** Incrementality differs sharply — retargeting is the biggest high-freq pool AND the least incremental (its visits were returning anyway). Analyze and decide independently.
8. **Start on advertisers without strict delivery SLAs** (cap-3 will suppress delivery on the eligible stratum; avoid pacing-critical accounts in wave 1).

---

## 11. Actuation prerequisite (HARD GATE — D7) and hash choice

**The RCT cannot start until a new bidder feature ships.** Arms B/C are NOT config-only (confirmed in code, Step 1 memo):

- **Insertion point:** `crates/bins/rtb-bidder-service/src/campaign/fcap.rs::do_fcap`, before building each `Campaign`. Compute the household bucket from `advertiser_id` (present on `CampaignModel`) + IP, map bucket→arm→cap, and pass the arm's cap into `check_freq_cap_threshold` in place of the configured cap. `check.rs` (lib) needn't change.
- **Why not config:** cached `CampaignModel` has no per-household cap field, so `bidder.frequency_caps` / cache-sync cannot assign per-bucket caps.
- **Owner:** `@SteelHouse/rtb`. fcap crate = `snowsignal` (Jane Lewis) / `rogusdev` (Chris Rogus); ghost/holdout machinery (for Phase-2 arm H) = Ryan Kleck (`rkleck-mntn`).
- **RCT co-owner ask:** a 30-min with `@SteelHouse/rtb` to (a) confirm the smallest `do_fcap` insertion point, and (b) **lock the exact hash on BOTH sides — the TI-837 production-equivalent** `MOD(ABS(CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip))), 1, 16)) AS INT64)), 1000)` (bidder already computes MD5; reimplementing BQ FarmHash in Rust is error-prone). **The bidder must compute the IDENTICAL preimage** — `CAST(advertiser_id AS STRING)` colon-joined to `ip`, no trailing space — **and the IDENTICAL 16-hex (not 15) reduction** before the mod-1000; this is what makes the RCT arms bit-match and stay disjoint from the platform 0-99 holdout. The analysis SQL uses this MD5 form, not `FARM_FINGERPRINT`. Bit-exactness of the `advertiser_id:ip` encoding + the 16-hex reduction is the one thing that must be verified jointly before enrollment.
- **fcap key is always IP** (even when `uses_mntn_id=true`) — favorable: an IP-based arm hash matches the always-IP fcap key. (Corrects scope §3's "MNTN-ID when uses_mntn_id".)

---

## 12. Arm H — Phase-2 (cap-aware partial suppression)

Deferred, optional. Arm H = serve up to the counterfactual cap, suppress+log only the would-be-over-cap impressions (cap-aware PARTIAL), to anchor absolute lift. It is a **new feature**, not the existing ghost holdout:
- The existing ghost holdout is binary **suppress-ALL-held-out-campaigns**, frequency-blind selection, fixed ~10-16% `MD5(advertiser:ip) mod 1000 < 100` bucket set upstream (Aerospike) — not a tunable cap-aware partial holdout.
- A cap-aware arm H reuses `GhostBid` / `apply_response_cap_split_ghosts` / `process_ghost_bids` + the `do_fcap` counters, all `@SteelHouse/rtb`-owned, and must coordinate with the ghost-bid lift pipeline (Matt Brorby) so a cap-aware ghost stream doesn't cross-contaminate the existing binary ghost-lift results.
- **Metric-side is now unblocked** (D8): total visits are defined for never-served households via the site pixel, so arm H's outcome is measurable — the only remaining blocker is the new bidder feature. Sequence it after B/C validate.

---

## 13. Pre-flight checklist (before enrollment)

- [ ] Bidder feature (per-bucket cap in `do_fcap`) shipped + hash bit-match verified with `@SteelHouse/rtb`.
- [x] `PENDING-A` resolved: total-visit source (`enriched.lift__ghost_bid_visits` / `logdata.guid_log`) + join key `(advertiser_id, ip)`. `PENDING-B/C` provisional (p0≈3% freq≥8, N/arm≈198K) — **confirm freq≥8 base rate + N at Checkpoint β from Step 3's total-visit-by-frequency curve.**
- [ ] Eligible-stratum build validated: predicted-eligible inflow measured (confirm ~1.62M/wk freq≥8 proxy) + shared-IP purge applied + excluded volume quantified.
- [ ] Site-wide-pixel advertiser universe enumerated; excluded volume quantified.
- [ ] Ring-fence confirmed: RCT households ∉ platform holdout (0-99), ∉ concurrent A/B / Fangorn.
- [ ] `has_custom_frequency_caps` campaigns and AID 90 / WGU excluded.
- [ ] Pre-registration `audi_1173_rct_prereg.md` FROZEN (this doc's §5/§6/§9 mirrored, base rate + N locked).

---

## 14. What would change the answer

- **Total-visit base rate materially higher than assumed** → smaller N, faster fill (re-solve §6/§7 at Checkpoint β).
- **Shared-IP contamination heavier than ~4%** in the eligible strata → tighter purge, external-validity narrows (guardrail 1).
- **Delivered ≠ configured frequency** widely (fails-open, no advertiser rollup) → the manipulation check fails; delivered-frequency measurement is the ONLY trustworthy exposure (guardrail 6).
- **Attributed-VV contrast and total-visit contrast agree** → D8's bias concern is empirically small (still report total as primary; note the convergence).
- **Cap-8 non-inferior but cap-3 fails NI** → the incremental floor sits between 3 and 8/wk; the bandit's action space narrows accordingly.
