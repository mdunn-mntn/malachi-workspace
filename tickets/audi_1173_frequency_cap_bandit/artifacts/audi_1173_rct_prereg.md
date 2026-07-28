# AUDI-1173 — Frequency-Cap RCT: PRE-REGISTRATION (LOCKED)

*This is the falsifiable, frozen commitment. Freeze BEFORE any downstream consumption of the total-visit data or any peek at outcomes. Full design + rationale: `audi_1173_rct_design.md`. Any change after freeze goes in the Amendments log (§13) with a timestamp and reason — never a silent edit.*

- **Freeze status:** DRAFT-PENDING-LOCK. **Owner-decided (recorded, not the open items):** PRIMARY = mean total site visits per household (a COUNT); margin **δ = 5% relative** of the control mean. `PENDING-A` (total-visit source): APPROACH decided — a **custom `guid_log` join** on `(advertiser_id, ip)` over `[first_impression, +30-60d]` carrying RCT arm membership, **to be BUILT** (`enriched.lift__ghost_bid_visits` = platform 7d sanity/reference only). `PENDING-B/C` (freq≥9 count **mean + variance** + N) filled **PROVISIONALLY** (incidence-proxy ≈3%, reference N/arm≈198K). Locks once the freq≥9 count mean/variance are **confirmed at Checkpoint β** (Step 3's total-visit-by-frequency curve) and the author signs off. **N is off the critical path** (fill ≪ exposure + maturation), so the design is decision-ready without a final N.
- **Frozen-before clause:** enrollment must not begin, and no outcome data may be inspected, until this file is committed with §5/§6 numeric (count mean/variance anchor) and the author's sign-off.
- **Author sign-off:** `____________`  **Date locked:** `____________`

---

## 1. One primary estimand

**ITT contrast, on the ex-ante eligible stratum, of a tighter default frequency cap vs BAU:** the difference in **mean total (attribution-independent) site visits per household — a COUNT** — between a tighter-cap arm and control, over the exposure + maturation window. (Binary incidence `P(≥1 total site visit)` is a secondary/diagnostic, not the primary — sizing on incidence is biased toward GO; the objective "preserve total visits" is a statement about the count.)

- Unit of randomization = unit of analysis = **household `(advertiser_id, ip)`**.
- Eligibility is **ex ante** (pre-period predicted-to-exceed-cap), never realized in-experiment frequency.
- Prospecting and retargeting are **separate strata**, decided independently.

## 2. Primary hypothesis + numeric go/no-go bar

**Reframed as non-inferiority (visits) + superiority (cost). GO requires BOTH:**

1. **Visits (the COUNT) non-inferior:** one-sided **lower 95% bound** (household bootstrap) of the **RELATIVE** contrast of the **mean total visits/hh** `(μ_T,total − μ_C,total)/μ_C,total` **> −δ**, with **δ = 5% relative** of the control mean. Relative is the coverage-robust scale (design §5.4); absolute visits/hh reported alongside as a coverage-attenuated companion. A **fixed** absolute margin is prohibited (not coverage-invariant).
2. **Cost superior:** cost/household reduction, one-sided **lower 95% bound > 0** (tighter cap strictly cheaper).

**GO** = ship the cap / green-light the bandit on that stratum. **NO-GO** = visits fail NI OR cost not reduced. Decided per stratum × per arm (4 cells: {prospecting, retargeting} × {cap-8, cap-3}).

*(**δ = 5% relative — RECORDED (owner decision).** The relative SCALE is fixed by the coverage model; the VALUE is now decided at 5% relative, no longer an open 3%/10% choice. This margin is locked; formal sign-off still awaits the Checkpoint-β count mean/variance anchor.)*

## 3. Arms (3)

Buckets from the **TI-837-validated** hash `MOD(ABS(CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip))), 1, 16)) AS INT64)), 1000)`:

| Arm | Cap | Bucket range |
|---|---|---|
| **A — Control** | BAU default cap | **100-399** |
| **B — Cap 8/wk** | 8 imp / rolling wk, default-cap campaigns only | **400-699** |
| **C — Cap 3/wk** | 3 imp / rolling wk, default-cap campaigns only | **700-999** |

Platform holdout **0-99** is **excluded** from the RCT (disjoint by same-hash construction). No arm H (Phase-2).

## 4. Randomization

- **Hash (TI-837 production-equivalent) = `MOD(ABS(CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip))), 1, 16)) AS INT64)), 1000)`** — **16 hex chars** (not 15), `CAST(advertiser_id AS STRING)` in the preimage. Computed **bit-identically** on the bidder side and the BQ analysis side: the bidder must compute the IDENTICAL preimage (`CAST(advertiser_id AS STRING)` colon-joined to `ip`, no trailing space) and the IDENTICAL 16-hex reduction. This exact form is what makes arms 100-399/400-699/700-999 **genuinely disjoint** from the platform 0-99 holdout and bit-match the bidder. Deterministic, sticky, disjoint.
- **Assignment = ITT** by bucket, regardless of whether the cap binds.

## 5. Eligible stratum (ex ante — pre-treatment stratifier)

From a **2-4 wk pre-period**, per household's baseline **delivered** weekly frequency from the advertiser (`cost_impression_log`). Eligibility rule (both arms): **predicted ≥ cap + 1** (the cap actually suppresses at least one impression):

- **Arm B eligible stratum:** predicted **≥ 9 imp/wk** (cap-8 binds — suppresses the 9th+). Fill proxy = the freq≥8 **7-day STOCK** ~1.62M (a stock, NOT a weekly inflow; over-counts the ≥9 eligible → fill built from it is a lower bound). B-vs-A analyzed here.
- **Arm C eligible stratum:** predicted **≥ 4 imp/wk** (cap-3 binds — suppresses the 4th+; ≈12.5% of prospecting hh, freq≥4 stock; same 7d-stock caveat). C-vs-A analyzed here.

Same cap+1 classifier applied to control households. **Realized in-experiment frequency is never a filter** (collider).

## 6. Inference method

- **Primary (mean total visits/hh — a COUNT): non-inferiority via HOUSEHOLD BOOTSTRAP** — resample households with replacement (≥5,000), report **point / one-sided lower 95% bound / p** on the RELATIVE contrast `(μ_T − μ_C)/μ_C` (§2 bar; coverage-robust, §7 + design §5.4). N per arm per §7 (count mean-difference sizing). The two-proportion z-test is NOT the primary here.
- **Secondary — binary total-visit incidence: two-proportion z-test, NI form** (the only place that test is used), same relative scale — robustness read beside the primary count, never the go/no-go bar.
- **Other secondary (cost/household, visits/dollar): nonparametric bootstrap resampling HOUSEHOLDS**, ≥5,000 resamples; point / 95% CI / two-sided p.
- **Advertiser = stratification/blocking + CUPED covariate**, **NOT** the resample cluster. **CUPED on the COUNT** — regression-adjust the post-period total-visit count on the pre-period total-visit **count** (continuous covariate), carried through the household bootstrap; NOT CUPED on a binary indicator. 20-50% SE reduction.
- **Diagnostic:** MNTN-attributed VV/household contrast, reported beside the total-visit contrast; the gap = the in-experiment last-touch attribution bias.
- Optional CausalImpact on arm-level daily total-visit series = convergence check only.

## 7. Power / sample size

- **Sized on the BINDING freq≥9 eligible stratum, on the COUNT** — anchor = the **mean total visits/hh `μ_C`** and its **household variance `σ_C²`** over the ≥30-60d window (a count, not a rate; all 3 arms serve → the served level, raised on the freq≥9 tail — NOT the ~1.0% attributed, NOT the 0.886% never-served holdout).
- **Mean-difference, 80% power, α=0.05 two-sided, δ = 5% relative of `μ_C`.** `N ≈ (1.96+0.8416)²·(σ_C²+σ_T²)/δ²`, `δ = 0.05·μ_C`. (The two-proportion binomial grid in design §6 sizes the DEMOTED incidence secondary and bounds the count N loosely — reference only.)
- **Reporting scale = RELATIVE** (coverage-robust under multiplicative cross-device miss); the NI margin is relative, so the test is coverage-invariant. Absolute = companion only (see design §5.4).

> **PROVISIONAL VALUES (confirm at Checkpoint β with Step 3's total-visit-by-frequency curve — which yields `μ_C` and `σ_C`; then lock at sign-off):**
> Prospecting: incidence-proxy anchor ≈ 3% *(provisional freq≥9; ≥1.5% served floor, higher at ≥30d window)*, `δ = 5% relative`, **reference N/arm ≈ 198K** *(116K if 5%; 402K if 1.5%)* — superseded by the count `μ_C`/`σ_C` N once measured.
> Retargeting: anchor ≈ 20-35% *(returning visitors — anchored on ATTRIBUTED rates, a PROXY; total ≥ attributed → true higher → even more over-powered)*, `δ = 5% relative`, **reference N/arm ≈ 12-25K** *(over-powered; binding constraint = incrementality size, not N)*
> Total-visit source: a **custom `dw-main-silver.logdata.guid_log` join** on `(advertiser_id, ip)` over `[first_impression, +30-60d]` carrying RCT arm membership (the ≥30d RCT-arm estimand; to be BUILT). `enriched.lift__ghost_bid_visits` = platform 7d sanity/reference only (7d window, ghost/submitted arms — cannot deliver the RCT-arm estimand). Join key: **`(advertiser_id, ip)`** (ip CIDR-stripped).

*(Reference anchors: at the old attributed 1.0% base, 5%-rel → ~606-637K/arm. Higher count level → N lower at fixed relative δ, but the true relative effect is smaller → N may rise; the confirmed `μ_C`/`σ_C` decide N — see design §6. **Fill (≤ ~1 wk) is off the critical path**; the ~10-12wk calendar is set by exposure + maturation, so a final N is not required to lock the design.)*

## 8. Calendar and when the decision is read

- **Arm-fill ≤ ~1 wk** (binding freq≥9; the ~1.62M is a 7d STOCK not weekly inflow → fill is a lower bound, off the critical path) → **exposure 4 wk** → **visit maturation 6-8 wk** (≈45-60d past last impression, governed via the total-visit event timestamp, NOT `visit_day`). **~10-12 wk total.**
- **Go/no-go read at tail maturity (~wk 11-13). Interim reads are directional-only** and do not gate the decision.

## 9. Guardrails (committed)

Shared-IP purge BEFORE randomization (external-validity bound quantified) · **site-wide-pixel advertisers only** (external-validity bound quantified) · ring-fence out of concurrent A/B + Fangorn holdouts + platform 0-99 · **default cap only** (never `has_custom_frequency_caps`) · exclude **AID 90 (PSA)** + **WGU (31357)** · monitor `fcap_impressions_fetch{outcome=redis_err}` → always measure **delivered** frequency · prospecting/retargeting separate strata · wave-1 avoids strict-delivery-SLA advertisers.

## 10. Prerequisites (must be true before enrollment)

1. **New `@SteelHouse/rtb` bidder feature** shipped: per-household bucket→arm→cap in `do_fcap` (arms B/C are not config-only — confirmed in code). Hash bit-match verified jointly.
2. `PENDING-A` resolved (source + join key). `PENDING-B/C` (freq≥8 `p0_total` + N) **confirmed at Checkpoint β** from Step 3's total-visit-by-frequency curve — provisional until then.
3. Eligible-stratum build + shared-IP purge + site-wide-pixel universe validated.

## 11. Primary outcome — exact definition

**Total advertiser site visits by the household (IP)** over the window, from the site pixel / total-traffic signal, **independent of MNTN last-touch attribution**. Binary primary = `≥1 total site visit` (yes/no). **Source (resolved):** primary **`dw-main-silver.enriched.lift__ghost_bid_visits`** (binary `visited` per arm × ip, guid_log-based); observational fallback = direct **`dw-main-silver.logdata.guid_log`** join. **Join key = `(advertiser_id, ip)`** (ip CIDR-stripped, visit in `[first_bid_time, +window)`). Attribution-independent (holdout arm empirically 0.886% visit rate at 0.0% won-rate). Attributed VV (`ui_visits`) is **secondary/diagnostic only**.

## 12. What would falsify / flip the conclusion

- Lower 95% bound of the visit contrast dips below `−δ` → cap fails NI → NO-GO on that cell.
- Cost/household not significantly reduced → NO-GO even if visits non-inferior.
- Cap-8 passes but cap-3 fails → incremental floor between 3 and 8/wk.
- Attributed-VV and total-visit contrasts converge → the D8 attribution-bias concern is empirically small (report both).

## 13. Amendments log (append-only; empty at freeze)

*(No pre-freeze edits belong here. Any post-freeze change: date · what · why.)*

| Date | Change | Reason |
|---|---|---|
| — | — | — |
