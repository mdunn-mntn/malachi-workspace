# AUDI-1173 — Frequency-Cap Bandit: DESIGN + OFFLINE-REPLAY EVAL PLAN

*Post-RCT adaptive controller. Ready to implement **pending RCT GO** (`audi_1173_rct_prereg.md` §2 bar met on ≥1 stratum). The bandit is the RCT's continuation: it never stops randomizing, so it stays a live RCT and the causal reward holds forever, not just at launch. Sits on scope §7; reward = the RCT primary (`audi_1173_rct_prereg.md` §11); actuation = the new bidder feature the RCT requires (`audi_1173_ownership_feasibility_memo.md`). Internal design doc — full technical depth intended.*

---

## 0. TL;DR — the three one-liners

- **Algorithm:** discounted Thompson sampling over discrete cap arms **{3, 5, 8, 12, ∞}** per (stage × campaign_group × vertical × device) context — Gamma-Poisson reward posteriors with **hierarchical priors pooled by vertical** (long tail), **exponential forgetting** for non-stationarity, **isotonic/unimodal shape constraints** for monotone-response exploitation, all wrapped in a **primal-dual bandit-with-knapsacks** layer (a Lagrangian spend/pacing shadow price) and actuated through the **new `@SteelHouse/rtb` per-household bucket→cap feature**.
- **Reward:** **incremental total-visits-per-dollar** = (served total site-visits/hh at cap `a` − continuously-running **ghost/suppression-holdout** total site-visits/hh) ÷ spend/hh — where "total visits" is the **attribution-independent `guid_log` COUNT** on `(advertiser_id, ip)` over `[first_impression, +30–60d]`, the **identical signal to the RCT primary**, now measured perpetually via a permanent randomized holdout bucket living in the same bidder feature.
- **Offline-replay eval:** before go-live, estimate **cumulative regret** by **semi-synthetic replay** — real logged cap-history pacing/volume/seasonality dynamics drive a bandit simulator whose **rewards come from the RCT's causally-identified incremental-visits-per-dollar response surface** (bootstrapped for uncertainty) — reporting regret vs a per-context **oracle** and vs the **current logging policy**, plus BwK pacing-violation rate; **gate live rollout** on positive expected regret-reduction with acceptable pacing under RCT-uncertainty draws.

**Why one surface does both:** the reward needs a *live concurrent counterfactual* (you cannot compute "incremental" without a same-period holdout), and the counterfactual can only be a per-household bucket (the cached `CampaignModel` has no per-household cap field — ownership memo, confirmed in code). So the bandit's exploration slice, its permanent measurement holdout, and its exploit cap are all writes to the **same** per-bucket map the RCT ships. No new control surface beyond that one.

---

## 1. Where this sits

| | |
|---|---|
| **Trigger to build** | RCT (`audi_1173_rct_prereg.md`) returns **GO** on ≥1 stratum × arm cell (visits non-inferior at δ=5% rel **and** cost strictly reduced). NO-GO everywhere → no bandit; the flat/negative curve is the finding. |
| **What the bandit adds over the RCT** | The RCT proves a *fixed* tighter cap beats BAU on the affected stratum. The bandit sets the cap **adaptively per context and tracks drift** — it finds the per-(vertical×stage×device) peak of the visits-per-dollar curve and re-finds it as creative fatigue, seasonality, and bidder behavior move it. |
| **Scope discipline (unchanged from RCT)** | **default-cap campaign_groups only** — never touch `has_custom_frequency_caps` (client-transparency constraint, scope §3). Exclude **AID 90 (PSA)**, **WGU (31357)**. Prospecting and retargeting are **separate bandit instances** (rewards differ sharply — scope §4a/§4b). |

---

## 2. Decision variable, arms, context

- **Decision variable (the knob):** the **default** frequency cap on a default-cap `campaign_group`, expressed as `(frequency_cap, frequency_cap_duration)`. Duration is fixed to the RCT window (rolling 1 wk) for v1; the cap *count* is the arm. The universal `1 imp / 30 min` floor is always retained beneath every arm.
- **Arms (discrete, ordered):** `A = {3, 5, 8, 12, ∞}` imp/rolling-week. `∞` = no bandit-imposed default cap (BAU floor only). Ordered — this ordering is load-bearing for the monotone-response exploitation in §4.4.
- **Context `c` (the state the arm is chosen for):**
  - **stage** ∈ {prospecting, retargeting} — a **hard split** (separate bandit instances, separate priors). Not a pooling dimension.
  - **vertical** — the **pooling** dimension for hierarchical priors (§4.3). fpa type-1 sub-vertical.
  - **campaign_group** — the finest actuation grain (the cap is written here).
  - **device** ∈ {CTV, display, …} — context split; frequency economics differ by device.
- **Context grain for the posterior:** `(stage) → vertical → campaign_group × device`. The posterior lives at `campaign_group × device`; it borrows strength from its `vertical` parent (§4.3). Stage is a separate model entirely.

---

## 3. Reward — incremental total-visits-per-dollar

**Definition (identical estimand to the RCT primary, `audi_1173_rct_prereg.md` §11):** for context `c` and cap arm `a`, over the maturation-complete window,

```
reward r(c,a) = [ λ_visit(c,a) − λ_visit(c, holdout) ] / s(c,a)
```

- `λ_visit(c,a)` = **mean total advertiser site visits per household** (a COUNT) for households served at cap `a` — the **attribution-independent** signal from the custom **`dw-main-silver.logdata.guid_log` join on `(advertiser_id, ip)`** over `[first_impression, +30–60d]` (ip CIDR-stripped). NOT last-touch attributed VV (`ui_visits`) — that's the confounded metric the RCT retired (scope §4d).
- `λ_visit(c, holdout)` = the **continuously-running ghost/suppression-holdout** baseline in the same context and same window — the counterfactual "would-have-served but suppressed" total-visit rate. This is the RCT's arm-H idea made **permanent**: a small fixed randomized bucket per context is always held out (ghost-bid: logged, fcap-accrued, not served), giving a live `λ_visit(c, holdout)` every epoch.
- `s(c,a)` = mean spend per household at cap `a` (`media + data + platform`, `cost_impression_log`).
- **Numerator = incremental** (served minus concurrent holdout) → causal by construction as long as the holdout keeps running. This is why the bandit is a perpetual RCT, not a one-shot.
- **Denominator = spend** → the objective is efficiency (incremental visits bought per dollar), which is exactly the diminishing-returns curve the whole ticket is about.

**Why the guid_log plane, not attributed VV, is the reward:** attributed visits/impression fall ~1/n *by construction* under last-touch (scope §4a/§4d) — a bandit rewarded on that metric would drive every cap to 3 to game the artifact, not because it's incremental. Total-visits-per-household from the ghost plane is attribution-independent (ghost/holdout reference: 0.886% 7d visit rate at 0.0% won-rate — pre-reg §11) and carries genuine incrementality.

**Delayed reward (6–8 wk maturation) — handled in §5.**

---

## 4. Algorithm — discounted Thompson sampling

Per context `c`, per arm `a`, maintain a **discounted Gamma-Poisson** posterior on the visit-count rate and a spend posterior; sample; pick the arm that maximizes the **knapsack-adjusted** sampled reward, restricted to the **monotone/unimodal** candidate set.

### 4.1 Reward posterior (conjugate, count-native)

Total visits per household are counts → **Poisson likelihood, Gamma conjugate prior** (Negative-Binomial marginal absorbs overdispersion; upgrade to Gamma-Gamma-Poisson if the RCT shows heavy overdispersion).

For each `(c,a)` maintain effective sufficient statistics `(α_ca, β_ca)`:
- `α_ca` = discounted sum of matured visit counts on served households + prior shape.
- `β_ca` = discounted count of matured served households + prior rate.
- Posterior on the served visit rate: `λ_visit(c,a) ~ Gamma(α_ca, β_ca)`.

The holdout has its own `(α_c0, β_c0)` from the permanent ghost bucket. Spend/hh `s(c,a)` gets a conjugate positive-continuous posterior (Gamma likelihood on per-household spend, or lognormal-Normal on `log s`; Gamma preferred for conjugacy).

**A Thompson draw for `(c,a)`:**
```
λ̃_a  ~ Gamma(α_ca, β_ca)            # served visit rate
λ̃_0  ~ Gamma(α_c0, β_c0)            # concurrent holdout baseline
s̃_a  ~ Gamma(k_ca, θ_ca)            # spend/hh
r̃(a) = (λ̃_a − λ̃_0) / s̃_a           # sampled incremental visits per dollar
```
Sampling the *ratio of posterior draws* propagates uncertainty in numerator and denominator jointly — the correct posterior over the efficiency objective. A shared `λ̃_0` draw is used across all arms within a draw (common counterfactual) so arm comparisons are on the same baseline.

### 4.2 Discounting (non-stationarity)

Exponential forgetting on the sufficient statistics, clocked on **maturation-completion date** (not exposure date — the reward is 6–8 wk late, §5). Each weekly update epoch:
```
α_ca ← γ · α_ca + Δvisits_matured ;  β_ca ← γ · β_ca + Δhouseholds_matured
```
- `γ ∈ (0,1)` per week; default **γ ≈ 0.90** → effective memory half-life ≈ 7 weeks ≈ one maturation cycle. Tuned in offline replay (§8) against seasonality in the cap history.
- Effect: the posterior tracks drift (creative fatigue, bidder/pacing changes, seasonality) instead of averaging over stale regimes. Discounting also keeps posteriors from collapsing so exploration never fully stops — required because the world moves.

### 4.3 Hierarchical priors pooled by vertical (long tail)

Most `campaign_group × device` cells are thin. Partial-pool by **vertical** within each stage:
- **Vertical-level hyperprior:** `λ_visit` for arm `a` across all campaign_groups in vertical `g` ~ Gamma with hyperparameters `(A_ga, B_ga)` estimated across the vertical (empirical Bayes v1; upgrade to full hierarchical Gibbs/variational if the EB point estimate is unstable).
- **Cell prior = vertical parent:** a sparse `(c,a)` cell starts at its vertical's posterior mean and shrinks toward it in proportion to its own data thinness (`β_ca` small → mostly parent; `β_ca` large → mostly own data).
- **Payoff:** a brand-new small campaign_group inherits its vertical's cap→reward shape immediately instead of paying full cold-start exploration cost. This is the mechanism that makes the bandit viable across the long tail of small advertisers.
- Stage is **not** pooled (separate models); device can be a third shallow pooling level or a hard split — default: split (cheap, and CTV/display economics differ enough that pooling risks bias).

### 4.4 Monotone-response exploitation (the shape prior that pays for the arms)

Two shape facts let the bandit explore far fewer arm-pulls than a 5-arm cell-independent TS:
1. **Numerator is isotonic:** incremental visits `λ_visit(c,a) − λ_visit(c,0)` is **monotone non-decreasing in the cap `a`** (more impressions never *reduce* incremental reach; it plateaus). Enforce with **pool-adjacent-violators (PAVA) isotonic regression on the posterior means** across the ordered arms each epoch → adjacent arms share information, posteriors tighten, dominated caps are recognized faster.
2. **Efficiency is unimodal:** because spend `s(c,a)` rises with the cap while incremental visits plateau, the objective `r(c,a)` is **single-peaked (quasi-concave) in `a`**. Exploit:
   - **Restrict the candidate set** each round to the current posterior-mode arm ± its two neighbors (a *unimodal-bandit* / line-search move) — regret scales with the number of local moves, not the full arm count.
   - **Prune stochastically-dominated arms:** if arm `a` has `P(r(c,a) ≥ r(c,a')) < ε` for a cheaper neighbor `a'` with equal-or-higher sampled incremental visits, stop pulling `a`.
   - The isotonic constraint + unimodality are **soft** (priors/pruning), not hard filters — the RCT confirmed only the *sign*, not the exact shape, so the data can still override if a vertical genuinely violates monotonicity.

The RCT read-out seeds these shapes: the arm-level incremental-visits-per-dollar curve (pre-reg §7 Checkpoint-β) initializes each vertical's `(A_ga, B_ga)` hyperprior and the expected peak location.

---

## 5. Delayed reward (6–8 wk maturation)

Total-visit reward matures ~45–60d past last impression (pre-reg §8). A naive bandit is blind for two months. Handle as a **delayed-feedback bandit with a surrogate index**:
- **Interim surrogate:** attributed VV (`ui_visits`) arrives in days. The RCT's **D8 diagnostic** (attributed-vs-total gap, pre-reg §6/§12) gives a learned bias correction `f: attributed → total`. Use `f(attributed)` as a **censored interim reward** to make provisional arm choices, then **reconcile** when the guid_log total matures. Only *matured* reward updates the durable posterior; the surrogate only widens/nudges within an epoch.
- **Discount clock on maturation date** (§4.2) so a slow arm isn't unfairly forgotten before its reward lands.
- **Exploration-fraction sizing:** the permanent randomized slice per context must be large enough that each arm accrues ≥ (power-adequate) matured households per discount half-life. This is the same power arithmetic as the RCT (pre-reg §7), applied per-epoch — sized off the confirmed `μ_C/σ_C`.
- **No interim gating on the surrogate for the GO/rollout decision** — same discipline as the RCT (interim reads directional-only).

---

## 6. Bandit-with-knapsacks — the pacing constraint

The bandit must not maximize efficiency into a pacing failure. Tighter caps under-deliver → budget left unspent / flight under-paces (a hard failure for the campaign, and MNTN's existing `hhst_generate_recommendation` objective is *pure pacing* — scope §2). Frame as **BwK**:

- **Objective:** maximize total **incremental visits** subject to **(i) spend ≤ flight budget** and **(ii) delivery ≥ pacing floor** over the flight.
- **Primal-dual solution:** maintain a **shadow price `ζ`** on the budget/pacing resource. The per-arm score the bandit actually maximizes is the **Lagrangian-adjusted** value:
  ```
  score(c,a) = (λ̃_a − λ̃_0)  −  ζ · s̃_a          # incremental visits net of shadow-priced spend
  ```
  (equivalently: pick the arm maximizing sampled incremental visits per shadow-priced dollar). `ζ` is updated online by primal-dual gradient on the pacing gap:
  - **behind pace / budget slack →** `ζ` **falls** → the bandit tolerates spend → **looser caps** (spend the budget).
  - **ahead of pace / budget tight →** `ζ` **rises** → spend penalized → **tighter caps** (efficiency without over-delivering).
- **Effect:** the knapsack stops the degenerate "cap-3 everywhere" solution (max efficiency, starved delivery) and couples the bandit to real pacing pressure. At `ζ→0` it maximizes raw incremental visits; at high `ζ` it maximizes efficiency. `ζ` is per-campaign_group (or per-flight), reflecting that group's budget/time-remaining.
- **Guardrail floor:** a hard minimum-delivery clamp beneath the soft `ζ` control — the bandit can never choose a cap that projects delivery below the campaign's contractual pacing floor, regardless of `ζ`.

---

## 7. Actuation — the new per-household bucket→cap feature

**Confirmed in code (ownership memo):** the cached `CampaignModel` has **no per-household cap field**; `check_freq_cap_threshold` applies a per-campaign cap identically to every household. So neither `bidder.frequency_caps` config nor the campaign-cache sync can carry a per-bucket cap. The RCT already requires the fix: a small localized change in **`do_fcap`** (`crates/bins/rtb-bidder-service/src/campaign/fcap.rs`, `@SteelHouse/rtb`-owned) that computes the household bucket and maps **bucket → arm → cap** before building each `Campaign`, passing the arm's cap into `check_freq_cap_threshold` (the `check.rs` lib is unchanged). `advertiser_id` is on `CampaignModel`, so the bucket hash is computable in-path.

The bandit **reuses that exact feature** — no new control surface (scope §7). The per-context bucket→cap map is what the bandit writes each epoch:

| Bucket slice (per context) | Cap written | Role |
|---|---|---|
| **Exploit majority** (~80–90%) | current best arm `â(c)` | serves the learned optimum |
| **Exploration slice** (~a few %) | spread across `{3,5,8,12,∞}` per TS | keeps every arm fresh under drift (§4.2) |
| **Permanent ghost holdout** (small fixed %) | suppress-and-log (ghost-bid, cap→0, fcap-accrued) | live counterfactual `λ_visit(c,0)` for the incremental reward (§3) |

- **Hash consistency (load-bearing, flagged unresolved in the memo):** the bidder-side bucket computation must **bit-match** the BQ analysis-side hash. Use the **TI-837 production-equivalent** form the pre-reg locked — `MOD(ABS(CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip))), 1, 16)) AS INT64)), 1000)` — computed identically on both sides (MD5, 16 hex chars, `CAST(advertiser_id AS STRING)` preimage). This keeps the bandit's buckets disjoint from the platform 0–99 holdout and reproducible in analysis. **Resolve the hash choice with `@SteelHouse/rtb` before implementation** (pre-reg prerequisite #1).
- **fcap key is always the IP** (confirmed in code) — consistent with an IP-based bucket hash; no MNTN-id dependency.
- **Fail-open awareness:** on Redis error the cap silently stops enforcing → delivered ≠ configured. The bandit must read **delivered** frequency and monitor `fcap_impressions_fetch{outcome=redis_err}`; treat fail-open epochs as missing-at-random for that context, not as an `∞`-arm observation.

---

## 8. Offline-replay regret evaluation (the pre-live gate)

**Goal:** estimate the bandit's **cumulative regret and pacing behavior on logged history BEFORE going live**, so the go-live decision isn't a leap of faith.

**The obstacle:** logged cap history is set by the current policy (template default → preset → group override) and is **confounded** — frequency is an outcome, and the observational visits/1k decline is partly a last-touch artifact (scope §4d). Naive replay (Li-et-al rejection sampling / IPS) on that reward is **biased**, because the logged reward is not the causal reward. So we do **not** replay on the observational reward.

**Method — semi-synthetic replay (two real ingredients, one causal ingredient):**
1. **Real logged *dynamics* from cap history:** pull the logged stream of `(context, cap, spend, delivered-frequency, inventory/win-rate, calendar)` per campaign_group over a long span (captures seasonality, pacing curves, inventory availability, cross-group leakage). These drive the simulator's **spend/delivery/pacing** side — the part the cap history *can* tell us truthfully (it's just accounting, not causal).
2. **Causal reward surface from the RCT:** fit `r̂(a | context)` — the incremental-visits-per-dollar response — **from the randomized RCT arm results** (the only unconfounded reward we have), with a **cluster bootstrap** (advertiser-clustered, N≥1000, per pre-reg §6) giving a *distribution* over the surface. Extrapolate across arms via the isotonic/unimodal shape (§4.4); interpolate across verticals via the hierarchical prior (§4.3).
3. **Simulate the bandit** on each bootstrap draw of the reward surface, feeding the real logged dynamics: at each epoch the bandit sees the surface-implied (noised) reward for the cap it *chooses*, updates its discounted posterior, and pays the logged spend/pacing dynamics. Roll forward over the full logged horizon.

**Regret + diagnostics reported (per stage, per vertical):**
- **Cumulative regret vs per-context oracle** (best fixed cap in hindsight) — how much the bandit leaves on the table vs perfect knowledge.
- **Lift vs the current logging policy** (the actual claim): Δ incremental visits and Δ visits-per-dollar the bandit would have delivered over BAU caps. **This is the number that justifies go-live.**
- **BwK pacing-violation rate:** fraction of flights the bandit would have under/over-delivered vs the logging policy — must be ≤ logging policy's own rate (the bandit may not degrade pacing).
- **Time-to-converge per context:** epochs until `â(c)` stabilizes — must be < a few maturation cycles or the delayed reward makes the bandit too slow to be worth it.
- **Discount-factor sweep:** replay across `γ` grid; pick the `γ` that best tracks the seasonality visible in cap history without over-forgetting.
- **Uncertainty bands:** every metric is a distribution over the RCT bootstrap draws → the go/no-go reads the *lower band*, not the point.

**Gate (go-live criterion):** proceed to shadow only if, across RCT-uncertainty draws, **expected regret-reduction vs the logging policy is positive (lower band > 0)** AND **pacing-violation rate ≤ logging policy** AND **time-to-converge < 2 maturation cycles**. Otherwise: re-tune (`γ`, exploration fraction, pooling), or hold — the RCT's fixed-cap win still ships without the adaptive layer.

---

## 9. Rollout + go/no-go

1. **Shadow** — bandit recommends and logs the cap it *would* set per context; no writes. Compare its choices to BAU and to the offline-replay prediction. Duration ≥ 1 maturation cycle.
2. **Ring-fenced live** — enable per-bucket actuation on a small set of **high-volume default-cap** campaign_groups in the GO strata only. Permanent holdout + exploration slice running. Watch pacing violations and delivered-frequency in real time.
3. **Expand by vertical/stage** — promote verticals whose live incremental-visits-per-dollar confirms the replay prediction; keep prospecting/retargeting separate; never onboard `has_custom_frequency_caps` or the excluded AIDs.
- **Kill criteria:** live incremental reward lower-band < 0 for a context, or pacing violations exceed the logging baseline, or the permanent holdout shows the served−holdout gap collapsing to zero (cap no longer buying incremental visits) → revert that context to BAU cap.

---

## 10. Dependencies + open threads

1. **RCT GO** (pre-reg §2) on ≥1 stratum — the build trigger. NO-GO → no bandit.
2. **New `@SteelHouse/rtb` `do_fcap` feature** (bucket→arm→cap) — shared with the RCT; the bandit adds a third bucket role (permanent holdout) and epoch-writable caps. Owners: `snowsignal` (Jane Lewis), `rogusdev` (Chris Rogus); ghost/holdout reuse: Ryan Kleck (`rkleck-mntn`).
3. **Hash bit-match** (TI-837 16-hex MD5 form) resolved and verified jointly bidder↔BQ before any live bucketing.
4. **Custom guid_log total-visit join** (`(advertiser_id, ip)`, `[first_impression,+30–60d]`) built and running continuously as the reward plane (pre-reg PENDING-A).
5. **Ghost-bid lift pipeline coordination** (Matt Brorby) — the permanent cap-aware holdout must not cross-contaminate the existing binary ghost-bid lift results.
6. **`μ_C/σ_C` from Checkpoint-β** — seeds the vertical hyperpriors, the exploration-fraction power sizing, and the expected peak-cap per vertical.
7. **BwK dual-price coupling to real pacing** — confirm the `ζ` update reads the same pacing signal the flight actually paces on (avoid a second, conflicting pacing controller alongside `hhst_generate_recommendation`).

---

## 11. One-liners (for return)

- **Algorithm** — Discounted Thompson sampling over cap arms {3,5,8,12,∞} per (stage×campaign_group×vertical×device) context: Gamma-Poisson reward posteriors, hierarchical priors pooled by vertical, exponential forgetting for non-stationarity, isotonic+unimodal shape constraints for monotone-response exploitation, inside a primal-dual bandit-with-knapsacks spend/pacing shadow price, actuated via the new `@SteelHouse/rtb` per-household bucket→cap feature.
- **Reward** — Incremental total-visits-per-dollar: (served total site-visits/hh at cap `a` − continuously-running ghost/suppression-holdout total site-visits/hh) ÷ spend/hh, where total visits = the attribution-independent `guid_log` COUNT on `(advertiser_id, ip)` over `[first_impression,+30–60d]` — the identical signal to the RCT primary, measured perpetually through a permanent randomized holdout bucket in the same bidder feature.
- **Offline-replay eval** — Semi-synthetic regret simulation: real logged cap-history pacing/volume/seasonality dynamics drive a bandit simulator whose rewards come from the RCT's cluster-bootstrapped causal incremental-visits-per-dollar response surface; report cumulative regret vs per-context oracle and vs the logging policy plus BwK pacing-violation rate; gate go-live on positive lower-band regret-reduction with pacing ≤ baseline.
