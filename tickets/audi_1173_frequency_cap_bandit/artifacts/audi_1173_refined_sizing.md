# AUDI-1173 — Refined frequency sizing (30-day delivered curve)

*Consolidated sizing deliverable. Source: `outputs/audi_1173_delivered_freq_curve.json` (56 rows: household = (ip, advertiser); delivered-frequency curve over a 30-day window; visits are ATTRIBUTED; dimensions = purge {raw, shared-IP-purged} × scope {combined, prospecting, retargeting, trace "other"} × 7 frequency buckets; overcap_spend_cap3/8/12 = spend on impressions beyond that many impressions-per-household over 30d). Companion to `audi_1173_scope.md` §4/§4d.*

**What this refines vs Phase-0:** Phase-0 was a 7-day prospecting-only curve. This is the 30-day, all-stage, household-grain delivered curve with a shared-IP-purge dimension. The frequency buckets here count impressions per household **over 30 days**, so the high-frequency shares are larger than the 7-day figures purely because of the window (a household accumulates ~4× the impressions in 30d vs 7d). Compare like-for-like: these are **30d delivered** shares, not per-week.

**Honest framing (why the headline metric changed).** The originally-planned deliverable was a total-visit observational dose-response curve. It was **dropped for two reasons, not one**: (1) it was a **968 GB cost-trap** (`guid_log` at household grain), and (2) even if run, it is **selection-confounded and cannot establish diminishing returns** — when total (unattributed, `guid_log`) visits were spot-checked, **total visits per household RISE with frequency**, because heavily-served households are simply more visit-prone (see §3). So this refined deliverable sizes the **addressable pool** honestly and hands the causal question to the RCT. It never claims a saving.

---

## 1. Addressable-pool sizing from the delivered curve

### 1a. High-frequency spend share (30d delivered)

Delivered frequency ≥ 8 impressions/household over 30d:

| Scope | Raw freq≥8 spend share | Purged freq≥8 spend share |
|---|---|---|
| **Combined** (all stages) | **42.3%** | 32.2% |
| **Prospecting** (has_mm) | **34.6%** | 30.4% |
| Retargeting (funnel≥2) | 78.9% | 57.3% |

The ~35% prospecting / ~42% combined raw figures are higher than Phase-0's 7-day ~12% **because of the 30-day window**, not a new phenomenon. The purged column is the honest one (§2). Retargeting runs at extreme delivered frequency (79% of raw spend at freq≥8) but is also the most shared-IP-contaminated and the least incremental (warm users), so its pool is the least trustworthy.

### 1b. Household-grain curve — combined, purged (the honest view)

| Freq (30d imps/hh) | Households | Imp share | Spend | Spend share | Attr visits | visits/hh | hh visit rate | attr CPV | cost/hh |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 177.0M | 19.1% | $5.84M | 24.6% | 2.10M | 0.012 | 1.14% | $2.79 | $0.033 |
| 2–3 | 87.7M | 21.8% | $5.58M | 23.5% | 2.05M | 0.023 | 1.97% | $2.73 | $0.064 |
| 4–7 | 37.4M | 20.2% | $4.66M | 19.7% | 1.71M | 0.046 | 3.27% | $2.72 | $0.125 |
| **8–12** | 11.2M | 11.5% | $2.49M | 10.5% | 0.96M | 0.086 | 5.21% | $2.59 | $0.222 |
| **13–20** | 5.35M | 9.0% | $1.83M | 7.7% | 0.70M | 0.132 | 7.01% | $2.60 | $0.342 |
| **21–40** | 3.02M | 9.0% | $1.68M | 7.1% | 0.62M | 0.205 | 9.43% | $2.71 | $0.555 |
| **41+** | 1.24M | 9.4% | $1.64M | 6.9% | 0.44M | 0.356 | 12.56% | $3.73 | $1.328 |

**Read carefully — the two per-household metrics move in OPPOSITE directions, and neither proves waste:**
- **cost-per-household rises monotonically** (\$0.03 → \$1.33) — mechanically, more impressions cost more.
- **attributed visits-per-household also RISE** (0.012 → 0.356) — heavily-served households visit *more*, not less.
- **attributed CPV is roughly FLAT** across freq 1–40 (~\$2.6–2.7) in the purged combined view, rising only at 41+ (\$3.73). Once shared IPs are purged, the "7× CPV rise" from the 7-day raw prospecting curve **largely evaporates.**
- Only **attributed visits-per-1,000-impressions declines** (11.8 → 5.0, ~2.3×) — and that decline is the last-touch artifact, not proof of saturation (§3).

### 1c. Marginal over-cap spend — GROSS ADDRESSABLE, BEFORE INCREMENTALITY

`overcap_spend_capN` = 30d spend on impressions beyond the Nth impression to a household. **This is the gross pool a cap would stop buying — it is NOT a saving.** Any real recovery is this number × an incrementality factor that is unknown until the RCT runs, and could be near zero (§3).

| Cap (30d imps/hh) | Combined RAW | Combined PURGED | Prospecting PURGED |
|---|---|---|---|
| Over 3 | $16.21M | $8.10M | $7.23M |
| **Over 8** | **$9.19M** | **$3.93M** | **$3.40M** |
| Over 12 | $6.88M | $2.73M | $2.34M |

Headline honest number: **~$3.9M / 30d combined (purged) is gross-addressable above an 8-per-30d cap** (≈$3.4M of it prospecting). Before incrementality. Never call this a saving.

---

## 2. Shared-IP purge — quantified (report both; purged is honest)

Purging shared IPs (NAT/CGNAT, offices, high-device-count households — one IP, many people) removes whole households from the curve. The magnitude is **wildly non-proportional across metrics**:

| Scope | Households dropped | Impressions dropped | Spend dropped | **Attributed visits dropped** |
|---|---|---|---|---|
| **Combined** | **−19.8%** (403M → 323M) | **−37.4%** (1.484B → 928M) | **−37.1%** ($37.7M → $23.7M) | **−73.1%** (31.9M → 8.6M) |
| Prospecting | −18.2% | — | −29.4% ($31.4M → $22.2M) | −70.5% (13.9M → 4.1M) |
| Retargeting | −43.7% | — | −75.8% ($6.31M → $1.53M) | −75.1% (18.0M → 4.5M) |

**The tell:** shared IPs are ~20% of households but ~37% of impressions/spend and ~73% of attributed visits. Shared IPs are high-frequency **and** vastly inflate attributed visits (multiple people's visits collapse onto one "household"). **Any observational curve built on raw IPs over-credits the high-frequency tail roughly 3-to-1 on visits.** Retargeting is the worst: purge removes 76% of its spend and 44% of its households — its raw high-frequency pool is mostly a shared-IP artifact. **Quote purged numbers only.**

---

## 3. THE LOAD-BEARING CAVEAT — the observational curve cannot establish diminishing returns

This is the headline. The 30-day delivered curve sizes a pool; it **cannot** tell you whether capping recovers any value. Three independent reasons, none removable observationally:

1. **The declining metric is a mechanical last-touch artifact.** Attributed visits/1k-impressions declines with frequency (§1b) partly *by construction*: last-touch credits exactly one impression per visit, so attributed visits per household are roughly bounded by a household's intrinsic visit count regardless of impression count `n`, forcing visits/1k toward ~1/n. Under a constant-per-impression-value null the metric would still fall. The observed decline (~2.3× combined, far less than the ~70× a pure 1/n would predict) sits between flat and 1/n — **some saturation, some artifact, not separable.** Do not read it causally.

2. **Total (unattributed) visits per household RISE with frequency — a selection confound.** When total `guid_log` visits were spot-checked (the curve that was dropped as a 968 GB cost-trap), **total visits/hh increased with frequency**, and the attributed visits/hh in this delivered curve do the same (0.012 → 0.356, §1b). Heavily-served households visit more because the bidder wins more impressions on households that are online more / more visit-prone — frequency is an **outcome**, so buckets compare *different populations at self-selected doses*, not one population dosed up. The **plateau-then-jump** in per-household visit rate is the fingerprint of a distinct tail population.

3. **So neither observational metric proves the point.** Attributed-per-impression *falls* (but that's the artifact). Total-per-household *rises* (but that's selection). **Neither shows that a cap would recover value.** A cap could recover most of the over-cap pool, or almost none of it — the observational data is consistent with both. Only a **household-randomized cap RCT** (served vs suppression-holdout, incremental visits per household) can measure the causal marginal value. **This is the argument FOR the experiment, not a result from it.**

---

## 4. Conclusion

**The sizing clears the "worth an experiment" bar; the causal question is open and RCT-only.**

- There is a **real, bounded high-frequency pool**: ~42% of combined / ~35% of prospecting 30d-delivered spend at freq≥8, and **~$3.9M/30d combined (purged) gross-addressable above an 8-per-30d cap** (~$3.4M prospecting). Bounded, not runaway — most spend still sits at low frequency.
- The pool **survives the honest (shared-IP-purged) view** — it shrinks (freq≥8 combined 42%→32%) but does not disappear, so it is not merely a NAT/CGNAT artifact.
- **No part of this is a saving.** The observational curve cannot establish diminishing returns: the declining metric is a last-touch artifact, and total visits/household rise with frequency by selection. What fraction of the ~$3.9M is truly non-incremental is **unknown until the RCT runs**, and could be small — especially in retargeting, where visits are least incremental.
- **Go/no-go: GO to the RCT.** The pool is large enough to matter and bounded enough to be experimentally tractable; the household-randomized cap RCT (scope doc §6) is the only instrument that answers the causal question, and it is cheap and runnable now on MNTN's existing MD5/FARM_FINGERPRINT holdout bucketing.

*RCT randomization expression validated (0-byte dry-run): `MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(advertiser_id AS STRING), ':', ip))), 1000)`.*
