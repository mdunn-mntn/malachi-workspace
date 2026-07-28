# AUDI-1173 — Frequency-Cap RCT: PRE-REGISTRATION (LOCKED)

*This is the falsifiable, frozen commitment. Freeze BEFORE any downstream consumption of the total-visit data or any peek at outcomes. Full design + rationale: `audi_1173_rct_design.md`. Any change after freeze goes in the Amendments log (§13) with a timestamp and reason — never a silent edit.*

- **Freeze status:** DRAFT-PENDING-LOCK — locks once `PENDING-A/B/C` (total-visit source + base rate + N) are filled and the author signs off.
- **Frozen-before clause:** enrollment must not begin, and no outcome data may be inspected, until this file is committed with §5/§6 numeric and the author's sign-off.
- **Author sign-off:** `____________`  **Date locked:** `____________`

---

## 1. One primary estimand

**ITT contrast, on the ex-ante eligible stratum, of a tighter default frequency cap vs BAU:** the difference in **total (attribution-independent) site-visit incidence per household** — `P(household made ≥1 total site visit)` — between a tighter-cap arm and control, over the exposure + maturation window.

- Unit of randomization = unit of analysis = **household `(advertiser_id, ip)`**.
- Eligibility is **ex ante** (pre-period predicted-to-exceed-cap), never realized in-experiment frequency.
- Prospecting and retargeting are **separate strata**, decided independently.

## 2. Primary hypothesis + numeric go/no-go bar

**Reframed as non-inferiority (visits) + superiority (cost). GO requires BOTH:**

1. **Visits non-inferior:** one-sided **lower 95% bound** of `(p_T,total − p_C,total)` **> −δ**, with **δ = 5% relative** of the control total-visit incidence `p_C,total`.
2. **Cost superior:** cost/household reduction, one-sided **lower 95% bound > 0** (tighter cap strictly cheaper).

**GO** = ship the cap / green-light the bandit on that stratum. **NO-GO** = visits fail NI OR cost not reduced. Decided per stratum × per arm (4 cells: {prospecting, retargeting} × {cap-8, cap-3}).

*(δ = 5% relative is the frozen default; author may lock 3% (stricter) or 10% (looser) at sign-off — record the chosen value here: **δ = ___% relative**.)*

## 3. Arms (3)

| Arm | Cap | Buckets `MD5(advertiser:ip) mod 1000` |
|---|---|---|
| **A — Control** | BAU default cap | **100-399** |
| **B — Cap 8/wk** | 8 imp / rolling wk, default-cap campaigns only | **400-699** |
| **C — Cap 3/wk** | 3 imp / rolling wk, default-cap campaigns only | **700-999** |

Platform holdout **0-99** is **excluded** from the RCT (disjoint by same-hash construction). No arm H (Phase-2).

## 4. Randomization

- **Hash = `MD5(advertiser_id:ip) mod 1000`**, computed **bit-identically** on the bidder side and the BQ analysis side (the joint spec pins the `advertiser_id:ip` string encoding + mod-1000 reduction). Deterministic, sticky, disjoint.
- **Assignment = ITT** by bucket, regardless of whether the cap binds.

## 5. Eligible stratum (ex ante — pre-treatment stratifier)

From a **2-4 wk pre-period**, per household's baseline **delivered** weekly frequency from the advertiser (`cost_impression_log`):

- **Arm B eligible stratum:** predicted **≥ 8 imp/wk** (≈2.2% of prospecting hh, ~1.62M/wk). B-vs-A analyzed here.
- **Arm C eligible stratum:** predicted **≥ 4 imp/wk** (≈12.5% of prospecting hh, ~9.06M/wk). C-vs-A analyzed here.

Same classifier applied to control households. **Realized in-experiment frequency is never a filter** (collider).

## 6. Inference method

- **Primary (binary total-visit incidence): two-proportion z-test, non-inferiority form** (§2 bar). N per arm per §7.
- **Secondary (cost/household, visits/dollar, total-visits/household count): nonparametric bootstrap resampling HOUSEHOLDS**, ≥5,000 resamples; point / 95% CI / two-sided p.
- **Advertiser = stratification/blocking + CUPED covariate** (pre-period visit rate; 20-50% SE reduction), **NOT** the resample cluster.
- **Diagnostic:** MNTN-attributed VV/household contrast, reported beside the total-visit contrast; the gap = the in-experiment last-touch attribution bias.
- Optional CausalImpact on arm-level daily total-visit series = convergence check only.

## 7. Power / sample size

- **Sized on the BINDING freq≥8 eligible stratum**, base rate = **total-visit incidence `p0_total` (NOT the ~1.0% attributed rate)**.
- **Two-proportion, 80% power, α=0.05 two-sided, MDE = 5% relative (default).** `N ≈ (1.96+0.8416)²·[p0(1−p0)+p1(1−p1)]/(p1−p0)²`.

> **LOCKED VALUES (fill at freeze):**
> Prospecting: `p0_total = ___%`, `MDE_rel = ___`, **N/arm = ______**
> Retargeting: `p0_total = ___%`, `MDE_rel = ___`, **N/arm = ______**
> Total-visit source table: `________________` · join key: `________`

*(Reference anchors: at the old attributed 1.0% base, 5%-rel → 637K/arm. Total-visit base rate is higher → N lower at fixed MDE, but the true relative effect is smaller → a smaller MDE may be chosen, raising N. The confirmed `p0_total` + chosen MDE decide N — see design §6 grid.)*

## 8. Calendar and when the decision is read

- **Arm-fill ~1.2-1.5 wk** (binding freq≥8) → **exposure 4 wk** → **visit maturation 6-8 wk** (≈45-60d past last impression, governed via the total-visit event timestamp, NOT `visit_day`). **~10-12 wk total.**
- **Go/no-go read at tail maturity (~wk 11-13). Interim reads are directional-only** and do not gate the decision.

## 9. Guardrails (committed)

Shared-IP purge BEFORE randomization (external-validity bound quantified) · **site-wide-pixel advertisers only** (external-validity bound quantified) · ring-fence out of concurrent A/B + Fangorn holdouts + platform 0-99 · **default cap only** (never `has_custom_frequency_caps`) · exclude **AID 90 (PSA)** + **WGU (31357)** · monitor `fcap_impressions_fetch{outcome=redis_err}` → always measure **delivered** frequency · prospecting/retargeting separate strata · wave-1 avoids strict-delivery-SLA advertisers.

## 10. Prerequisites (must be true before enrollment)

1. **New `@SteelHouse/rtb` bidder feature** shipped: per-household bucket→arm→cap in `do_fcap` (arms B/C are not config-only — confirmed in code). Hash bit-match verified jointly.
2. `PENDING-A/B/C` resolved (total-visit source + `p0_total` + N).
3. Eligible-stratum build + shared-IP purge + site-wide-pixel universe validated.

## 11. Primary outcome — exact definition

**Total advertiser site visits by the household (IP)** over the window, from the site pixel / total-traffic signal, **independent of MNTN last-touch attribution**. Binary primary = `≥1 total site visit` (yes/no). Source table + join key: **`PENDING-A`** (candidates: `clickpass`/`guid_log` total-traffic, or INCR `enriched.lift__ghost_bid_visits`). Attributed VV (`ui_visits`) is **secondary/diagnostic only**.

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
