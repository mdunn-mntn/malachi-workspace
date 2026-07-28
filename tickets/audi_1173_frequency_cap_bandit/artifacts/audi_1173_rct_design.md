# AUDI-1173 — Frequency-Cap RCT: Finalized Design

*Step 2 deliverable. Corrects and supersedes scope §6. MNTN = performance CTV DSP; KPI = advertiser site visit; household = `(advertiser_id, ip)`. Companion: the LOCKED pre-registration `audi_1173_rct_prereg.md` (freeze that before any downstream consumption). Actuation reality from `audi_1173_ownership_feasibility_memo.md` (Step 1).*

**Status: DRAFT for author (Malachi) review — vetoable.** Two inputs are PENDING and marked inline: (a) the total-visit source table + base rate (owner's parallel BQ probe); (b) Step 3's refined household visit rate / shared-IP flag. Everything else is final.

---

## 1. Objective and the decision it serves

**Question:** Does tightening the *default* impression frequency cap PRESERVE household site visits while REDUCING cost per household — i.e., is the high-frequency tail of MNTN delivery incremental, or is it waste we can cut without losing visits?

**Decision it serves:** whether to ship an adaptive frequency-cap bandit (AUDI-1173 Phase 2) that lowers the default cap on the segments where the tail is non-incremental. The bandit's reward is the per-cap incremental-value curve this RCT measures. North-star tie: total-traffic incrementality mandate (honest total-visit signal over last-touch attributed VV) + cost reduction on non-incremental spend.

**Framing as non-inferiority + superiority (not point-estimate lift).** The observational curve (§4 scope) shows per-household visit rate PLATEAUS across freq 4-20, which — if causal — means the tail buys impressions that add no visits. The right question is therefore not "how big is the lift" but "can we cut the tail without losing visits." So:

- **Primary (visits): non-inferiority.** A tighter cap must not reduce total visits/household by more than a pre-registered margin.
- **Primary (cost): superiority.** A tighter cap must reduce cost/household.
- **GO = non-inferior on visits AND cheaper on cost.** That is the bandit's green light.

---

## 2. Deviations from scope §6 (each vetoable — one-line rationale)

| # | Scope §6 said | This design does | Why |
|---|---|---|---|
| **D1** | Analyze the **affected set = realized freq≥8**. | Define eligibility **ex ante** from a 2-4wk pre-period (predicted-to-exceed-cap), randomize the full population, analyze **ITT on the ex-ante eligible stratum**, per arm. | Realized frequency is CAUSED by the cap (a cap-3 household can never appear in freq≥8) → conditioning on it is collider selection that breaks randomization. Ex-ante prediction is a pre-treatment stratifier, so it doesn't. |
| **D2** | Hypothesis = estimate lift / incremental visits-per-dollar. | **Non-inferiority on visits/household + superiority on cost/household**, with a pre-registered numeric margin as the go/no-go bar. | The plateau means the interesting result is "no visit loss at lower cost," which a lift point-estimate frames poorly. NI states the actual decision bar. |
| **D3** | Analyze on the full cohort (implied by §6's population line). | Analyze on the **ex-ante eligible stratum only**, sized there. | freq≥8 is ~2.2% of prospecting households; a real 5%-relative effect on the bound set dilutes ~45× to ~0.1% on the full cohort — far below the MDE 636K buys. Effect must be measured where the cap binds. |
| **D4** | Inference = **advertiser-clustered bootstrap** (imported from the tiered-rollout protocol) + optional CausalImpact. | Randomization unit = analysis unit = **household**. Primary (binary) → **two-proportion z-test**; ratios → **bootstrap resampling HOUSEHOLDS**. Advertiser = stratification/CUPED covariate, **not** the resample cluster. | Household is the randomization unit; clustering to a few hundred advertisers collapses N from ~636K to hundreds and contradicts the power calc. Cluster-bootstrap is for wave-flip rollouts (unit = advertiser), not a household RCT. |
| **D5** | Ship **4 arms** incl. **H — suppression holdout** (would-have-served suppressed). | Ship **3 arms {A control, B cap 8/wk, C cap 3/wk}**. Arm H = a NEW cap-aware **partial** suppression feature, **Phase-2/optional**. | The existing ghost machinery is a fixed ~10-16% binary suppress-ALL, frequency-blind selection — not a tunable cap-aware partial holdout. The 3 served arms all have impressions, so the visit metric is well-defined and the ghost frequency bias doesn't apply. (Arm H's metric-side blocker is now removed — see §6 — but it still needs a new bidder feature.) |
| **D6** | Randomize via `FARM_FINGERPRINT(advertiser:ip) mod 1000`. | Randomize via **`MD5(advertiser:ip) mod 1000`** on BOTH bidder and BQ sides; arms carved from buckets **100-999**, disjoint from the platform holdout **0-99**. | The bidder computes MD5 already; bit-matching BigQuery FarmHash-Fingerprint64 in Rust is error-prone. Same hash + carve-from-100-999 guarantees no RCT household is also a platform-holdout household. (Step 1 memo, hash-consistency.) |
| **D7** | Arms B/C are config/sync changes on `bidder.frequency_caps`. | Arms B/C require a **new `@SteelHouse/rtb` bidder feature** (per-household bucket→arm→cap in `do_fcap`) — a **hard prerequisite**; the RCT cannot start until it ships. | Confirmed in code: the cached `CampaignModel` has no per-household cap field, so the DB→cache path cannot carry a per-bucket cap. Config-only is foreclosed. (Step 1 memo.) |
| **D8** *(owner change, post-scope)* | Primary metric = MNTN-attributed verified visits (`ui_visits`). | Primary = **attribution-independent TOTAL site visits/household**; attributed VV → secondary/diagnostic. | Frequency DRIVES last-touch attribution → the higher-frequency control arm wins the tiebreak more often, inflating its attributed VV and biasing the contrast AGAINST capping. Total visits removes it. (Full rationale §5.) |

**None of D1-D8 is silent.** If the author rejects any row, revert that row and re-open the affected section.

---

## 3. Eligible-stratum definition (ex ante, per arm) — the D1/D3 fix

**Principle:** eligibility is a **pre-treatment stratifier**, computed from data that predates (and is untouched by) the cap. It must never be realized in-experiment frequency.

**Build:** from a **2-4 week pre-period** immediately preceding randomization, compute each `(advertiser_id, ip)` household's baseline **delivered** weekly impression frequency from that advertiser (`cost_impression_log`, delivered not configured — fails-open/leakage means delivered ≠ configured; measure delivered). Classify each household by predicted frequency absent any cap change:

- **Arm B eligible stratum** = households predicted **≥ 8 imp/wk** (the cap-8 binds here). Observationally ≈ **2.2% of prospecting households, ~1.62M/wk** (Phase-0 freq≥8 stock: 1.17M + 0.32M + 0.10M + 0.03M).
- **Arm C eligible stratum** = households predicted **≥ 4 imp/wk** (the cap-3 binds here). Observationally ≈ **12.5% of prospecting households, ~9.06M/wk** (adds freq 4-7 = 7.44M).

The SAME classifier is applied to control-arm households, so all three arms carry comparable eligible strata. Analysis contrasts:
- **B vs A** on the **freq≥8** eligible stratum,
- **C vs A** on the **freq≥4** eligible stratum.

**Operationalization (pre-flight):** the simplest classifier is mean weekly delivered frequency ≥ threshold over the pre-period; a predicted-frequency model is optional refinement. Measure the actual predicted-eligible inflow in the pre-period build — the observational freq≥8 stock (~1.62M/wk) is the fill proxy; confirm it before locking the calendar.

**Why not the collider:** a cap-3 household physically cannot reach realized freq≥8, so "analyze realized freq≥8" compares arm A's tail to an EMPTY arm-C tail — pure selection on a post-treatment variable. Ex-ante eligibility is fixed before treatment, so the strata are exchangeable across arms.

---

## 4. Arms and bucket allocation (D5 + D6)

**Hash (both sides):** `MOD(ABS(CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(advertiser_id,':',ip))),1,15)) AS INT64)),1000)` on the BQ side; the bidder's existing `MD5(advertiser:ip) mod 1000`. **Both must bit-match** — pin the exact byte encoding of `advertiser_id:ip` (string, colon-joined, no trailing space) and the mod-1000 reduction in the joint @SteelHouse/rtb + analysis spec before implementation. Deterministic, sticky, disjoint.

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

### 5.1 Primary outcome — TOTAL site visits/household (attribution-independent) [D8]

**Definition:** total advertiser site visits by the household (IP) over the exposure + maturation window, from the site pixel / total-traffic signal — **independent of MNTN last-touch attribution**.

- **Primary sized statistic (frozen):** binary per-household **total-visit incidence** — household made **≥ 1 total site visit** (yes/no) in the window. Two-proportion z-test, non-inferiority form (§8). Base rate `p0_total` = **PENDING probe** (higher than the ~1.0% attributed rate).
- **Companion effect-size:** mean **total visits/household** (count) — household bootstrap, NI form. Reported beside the binary primary; not the sized statistic (a count/ratio needs bootstrap, not a proportion test).

**Why total visits, not attributed VV (put in the deck):**
1. **Snipe.** MNTN is top-funnel CTV; the visit lands days later, so competing advertisers snipe MNTN's last-touch credit → attributed VV *undercounts* true visits.
2. **Decisive — frequency drives attribution.** The higher-frequency control arm wins the last-TV-touch tiebreak more often, inflating its attributed VV even for households that would have visited anyway. That is the §4d last-touch artifact operating INSIDE the experiment, and it biases the contrast AGAINST capping. Total visits removes it.
3. **Mandate + arm-H unblock.** Total traffic is the org incrementality signal (honest vs clickpass). It also makes a never-served holdout measurable: a never-served household still has total site-visit records via the site pixel, whereas attributed VV is zero by construction. (Arm H's metric-side blocker is thus removed; it still needs the bidder feature — §11.)

**PENDING (owner probe — drop-in):** confirmed total-visit source table + join key + eligible-stratum base rate. Candidates: `clickpass`/`guid_log` total-traffic, or the INCR enriched `lift__ghost_bid_visits` pipeline. Do not lock §7 power until this lands.

> **`PENDING-A` — total-visit source:** `________________`  join key: `________`  attribution-independent: yes
> **`PENDING-B` — eligible-stratum total-visit base rate `p0_total`:** prospecting `____%` · retargeting `____%`

### 5.2 Secondary / diagnostic

| Metric | Role | Estimand | Inference |
|---|---|---|---|
| **cost/household** | **Primary VALUE driver (superiority)** | mean spend per eligible household, tighter-cap arm < control | household bootstrap, one-sided |
| **visits/dollar** | efficiency headline | total visits ÷ spend on the eligible stratum, tighter-cap arm > control | household bootstrap |
| **MNTN-attributed VV/household** (`ui_visits`) | **diagnostic** — quantifies the attribution bias direction (expect control's attributed VV inflated vs its total visits) | mean attributed VV per household | two-proportion / bootstrap |
| **delivered-frequency distribution** | manipulation check | per-arm delivered freq (measure delivered, not configured — fails-open/leakage) | descriptive |

**Estimand for all:** ITT (bucket-assigned), on the ex-ante eligible stratum, prospecting and retargeting as **separate strata**.

---

## 6. Power — sized on the eligible stratum, base-rate parameterized [D8 re-derivation]

**Method:** two-proportion, `ti_884` `mde_binomial`. Per-arm N to detect a relative change at 80% power, α=0.05 two-sided:

`N ≈ (z_{0.975}+z_{0.80})² · [p0(1−p0)+p1(1−p1)] / (p1−p0)²`, with `p1 = (1±MDE_rel)·p0`.

**The base rate MUST be `p0_total` (total-visit reach), NOT the ~1.0% attributed VV rate.** Total visits have a **higher base rate** (→ smaller N at a fixed relative MDE) but a **smaller relative treatment effect** (more households visit organically → the ad-driven increment is a smaller share → you may need a smaller relative MDE, which raises N). These pull in opposite directions; the confirmed `p0_total` + the chosen MDE decide N. The grid below lets the author drop in the confirmed row.

**Parameterized N/arm (80% power, α=0.05 two-sided), by base rate × relative MDE:**

| `p0_total` | MDE 5% rel | MDE 3% rel | MDE 2% rel |
|---|---|---|---|
| 1.0% *(old attributed anchor — for reference)* | ~637K | ~1.77M | ~3.98M |
| 2% | ~311K | ~864K | ~1.94M |
| 5% | ~119K | ~331K | ~745K |
| 10% | ~57K | ~157K | ~354K |
| 20% | ~25K | ~70K | ~157K |
| 30% | ~15K | ~41K | ~92K |

*(N scales ≈ `6279·(1−p0)/p0` at 5% rel; ×2.78 at 3% rel; ×6.25 at 2% rel. The 1.0% / 5%-rel cell reproduces the scope's 637K, verified: `7.849·[0.0099+0.010390]/(0.0005)² = 637,015`.)*

> **`PENDING-C` — CONFIRMED sizing (fill after PENDING-B):**
> prospecting: `p0_total = ___%`, `MDE_rel = ___`, **N/arm = ______**
> retargeting: `p0_total = ___%` (much higher — likely 15-35%), `MDE_rel = ___`, **N/arm = ______** (expected small; retargeting is over-powered, its binding constraint is incrementality SIZE, not N — and it is the LEAST incremental stratum, §4b scope).

**Sizing is on the BINDING stratum (freq≥8, arm B):** that stratum is the rarest, so its fill governs the calendar (§7). Arm C's freq≥4 stratum is ~5.6× larger → never binding on fill.

**Re-verify at Checkpoint β:** when Step 3's refined household visit rate lands (longer visit tail → likely higher `p0_total`), re-run this grid and re-confirm N before freezing enrollment.

---

## 7. Calendar — three clocks (~10-12 weeks)

| Clock | Length | Governed by |
|---|---|---|
| **1. Arm-fill** | **~1.2-1.5 wk** (prospecting freq≥8) | 3 arms × N/arm eligible in the binding freq≥8 stratum ÷ ~1.62M/wk inflow. At N=637K: 1.91M ÷ 1.62M ≈ 1.2wk. Retargeting fills faster (larger + over-powered). *(Re-solve once PENDING-C sets N.)* |
| **2. Exposure** | **4 wk** | Active capping window per household — long enough for the rolling cap to bind repeatedly and for delivered-frequency contrast to separate. |
| **3. Visit maturation** | **6-8 wk** (≈45-60d past last impression) | Visits have a long tail (last-TV-touch p90 = 28d; §4d wants ≥30-45d). **Govern via `ui_visits.time` / the total-visit event timestamp — extend ~45-60d past last impression. NOT `visit_day` (capped at 14).** |

**End-to-end:** pre-period build (2-4wk, parallel) → fill (~1.5wk) → exposure (4wk, impressions through ~wk 5) → maturation (through ~wk 11-13). **Go/no-go is read at tail maturity (~wk 11-13). Interim reads are DIRECTIONAL-ONLY** — do not gate on them (visit tail immature → biased toward null-of-no-effect early).

---

## 8. Inference (D4)

**Randomization unit = analysis unit = household.** No advertiser clustering.

- **Primary (binary total-visit incidence): two-proportion z-test, non-inferiority form.** Test `p_T − p_C > −δ_abs` where `δ_abs = MDE_rel · p_C` (default `MDE_rel = 5%` relative — the frozen bar, §9). GO on visits requires the one-sided lower 95% bound of `(p_T − p_C)` to sit above `−δ_abs`. N per arm = §6. Each eligible household = one Bernoulli trial (visited yes/no) → clean two-proportion; no within-household impression correlation to model because the household is the unit.
- **Secondary ratios (cost/household, visits/dollar, total-visits/household count): nonparametric bootstrap resampling HOUSEHOLDS**, 5,000+ resamples. Report point / 95% CI / two-sided p. Cost superiority = one-sided lower bound of the cost *reduction* above 0. **Resample households — not advertisers, not days.**
- **Advertiser is a stratification/blocking + CUPED covariate for variance reduction, NOT the resample cluster.** Block randomization within advertiser (and within prospecting/retargeting) so arms balance on advertiser mix. **CUPED** on each household's **pre-period visit rate** (pre-period exists → randomization holds → CUPED's zero-mean guarantee holds; 20-50% SE reduction per `experimentation.md`). CUPED covariate must be genuinely external to the response (pre-period, not the post outcome).
- **Optional convergence check:** arm-level daily total-visit series under the standard-protocol CausalImpact — as a secondary consistency read only, not the primary (population is a clean RCT, so the direct two-proportion is primary per the TI-504 lesson).

**Two-sided N is deliberately conservative:** a one-sided NI test at the same 5%-relative margin needs fewer (~×0.79 of the two-sided N via `(1.645+0.8416)²` vs `(1.96+0.8416)²`), so the sized N also powers a two-sided *superiority* read if a real harm exists.

---

## 9. Go / no-go bar (frozen — see prereg)

**GO (ship the cap / green-light the bandit on this stratum) requires BOTH:**
1. **Visits non-inferior:** one-sided lower 95% bound of `(p_T,total − p_C,total)` > `−δ`, with **δ = 5% relative** of the control total-visit incidence. *(Author may set δ stricter (3%) or looser (10%) — business call; flagged.)*
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
- **RCT co-owner ask:** a 30-min with `@SteelHouse/rtb` to (a) confirm the smallest `do_fcap` insertion point, and (b) **lock the hash: `MD5(advertiser:ip) mod 1000` on BOTH sides** (bidder already computes MD5; reimplementing BQ FarmHash in Rust is error-prone). The analysis SQL then uses MD5, not `FARM_FINGERPRINT`. Bit-exactness of the `advertiser_id:ip` encoding is the one thing that must be verified jointly before enrollment.
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
- [ ] `PENDING-A/B/C` resolved: total-visit source + join key + `p0_total` + confirmed N/arm (owner probe).
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
