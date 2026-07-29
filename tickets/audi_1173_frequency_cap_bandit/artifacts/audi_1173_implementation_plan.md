# AUDI-1173 — Frequency-Cap Bandit: Implementation-Ready Plan

*Sprint-ready spec — pull from backlog and execute. This is the single source of truth for the work; supporting detail in `audi_1173_rct_design.md` (full design), `audi_1173_rct_prereg.md` (frozen commitment), `audi_1173_refined_sizing.md` (the pool), `audi_1173_leakage_brief.md`, `audi_1173_ownership_feasibility_memo.md` (bidder code paths), `audi_1173_bandit_design.md`. RFD (`audi_1173_rfd_draft.md`) = the circulate-for-buy-in cut of this same content.*

*Status: scoped, designed, and adversarially gated. Not yet started. One hard prerequisite (a small bidder feature) gates the RCT.*

---

## BLUF

Build MNTN's first production bandit — a lift-aware frequency cap — validated by a cheap household-randomized RCT. **~$3.9M/30d of delivered spend sits above an 8-impression/week cap; the RCT measures what fraction of it is non-incremental (redirectable to fresh reach) and produces the reward curve a bandit then optimizes.** We can't answer "is that spend wasted?" observationally — it needs the experiment. The one blocker to starting: a small `@SteelHouse/rtb` bidder feature, because the cap arms are not config-only (confirmed in code).

---

## Problem

- **No advertiser-level frequency control exists.** Caps are per-campaign / per-campaign_group only, IP-keyed, rolling-window, and **fail open** on Redis error. `advertiser_frequency_caps` is empty (0 rows). So a household's frequency leaks across an advertiser's campaign_groups and funnel stages, and *delivered* frequency routinely exceeds *configured*.
- **A large, bounded high-frequency pool.** ~32% of combined / ~30% of prospecting 30-day delivered spend (shared-IP-purged) goes to households already at freq ≥8 — **~$3.9M/30d gross-addressable above an 8/wk cap** (~$3.4M of it prospecting).
- **We cannot tell observationally whether that tail spend is wasted.** Attributed visits-per-impression decline with frequency, but partly as a mechanical last-touch artifact (~1/n by construction), not diminishing returns. And attribution-independent *total* visits actually *rise* with frequency — because heavily-served households are selected as more visit-prone, not because the ads caused it. Neither curve proves a cap recovers value.
- **Nothing optimizes frequency for incremental value.** Frequency is the highest-reach, most-measurable candidate for MNTN's first Multi-Arm Bandit (vs the HHST intent-gate bandit, whose graduated lift is blocked in BQ).

---

## Solution

A **3-arm household-randomized RCT** measures the causal marginal value of frequency → an **incremental-value-per-cap curve** → a **discounted-Thompson bandit** that sets the default cap per campaign_group to maximize incremental visits per dollar. A control-plane fix (advertiser-level rollup counter) ships in parallel, independent of the RCT.

- **Arms:** A = control / BAU cap · B = cap 8/wk · C = cap 3/wk. Default-cap campaigns only (never `has_custom_frequency_caps`).
- **Randomization:** household `(advertiser_id, ip)` → `MD5(advertiser:ip) mod 1000`, arms in buckets 100-399 / 400-699 / 700-999, disjoint by construction from the platform 0-99 holdout.
- **Primary metric:** mean **total site visit-days per household** (attribution-independent), **non-inferiority at 5% relative** via household bootstrap. **Secondary:** cost/household (superiority). **GO = visits non-inferior AND cost/household reduced**, decided per stratum (prospecting / retargeting) × arm (cap-8 / cap-3).
- **Why total visits, not attributed:** frequency drives last-touch attribution — the higher-frequency control arm wins the tiebreak more often and inflates its attributed visits, biasing the test against capping. Total visits removes that.

---

## How to implement (ordered — this is the work-list)

1. **Bidder feature — the hard prerequisite** (`@SteelHouse/rtb`; owners `snowsignal`/`rogusdev`). Add per-household cap selection in `crates/bins/rtb-bidder-service/src/campaign/fcap.rs::do_fcap`: compute the household's `MD5(advertiser:ip)` 16-hex bucket → map bucket→arm→cap → pass the arm's cap into `check_freq_cap_threshold` (the cached `CampaignModel` has no per-household cap field, so config/sync cannot do this). Small, localized. **Kick-off: a 30-min with `@SteelHouse/rtb`** to confirm the smallest insertion point and lock the hash preimage bit-identically on the bidder and BQ sides. *Eng effort: to confirm with rtb — believed small.* **The RCT cannot start until this ships.**
2. **Build the visit-source join.** Custom `logdata.guid_log` join on `(advertiser_id, ip)` → **visit-days** (`guid_log` is a page-view log → dedup page-views to distinct `(advertiser_id, ip, date)`), over a post-anchored `[first_impression, +30-60d]` window carrying arm membership. `first_impression` from the impression log. **`guid_log` physical is 366 TB — partition-prune + cohort-restrict, or run on Databricks; never full-scan.** Validate it reproduces the platform 7d served/holdout rates.
3. **Build the eligible stratum + guardrails.** Pre-period (2-4 wk) classifier: predicted delivered frequency ≥ cap+1 (Arm B ≥9/wk, Arm C ≥4/wk) — an ex-ante stratifier, never realized in-experiment frequency (a collider). Apply a real shared-IP purge (NAT/CGNAT/high-distinct-advertiser IPs). Restrict to **site-wide-pixel advertisers** (total visits need an all-page pixel). Ring-fence out of concurrent A/B + Fangorn holdouts + the 0-99 platform holdout; exclude AID 90 (PSA) + WGU (31357).
4. **Lock the pre-registration.** Measure the freq≥9 stratum total-visit count **mean + variance** → fill final N; author sign-off (`audi_1173_rct_prereg.md` is DRAFT-PENDING-LOCK). N is off the critical path (fills in ~1 wk), so this does not gate the calendar.
5. **Run.** 4-week exposure; ~10-12 weeks to a matured readout (visit maturation 6-8 wk past last impression, governed by the visit timestamp, not `visit_day`). Monitor `fcap_impressions_fetch{outcome=redis_err}` (fail-open → always measure *delivered* frequency).
6. **Analyze.** Household bootstrap non-inferiority on total-visit-days + cost/household superiority, per stratum × arm; report point / one-sided lower 95% bound / p on the **relative** contrast (coverage-robust under ~85-90% cross-device coverage). Attributed VV reported as a diagnostic companion.
7. **If GO → build the bandit.** Discounted-Thompson sampling on the default cap; context = campaign_group / vertical / stage / device; reward = incremental total-visits-per-dollar from a continuously-running randomized holdout (same guid_log plane); bandit-with-knapsacks pacing; **offline-replay regret eval on logged cap history first**; roll out shadow → ring-fenced live → expand by vertical/stage.
8. **Parallel, no RCT needed — the control-plane fix.** Add an advertiser-level rollup counter `rtb:frequency:{ip}:advertiser=<aid>` (default cap only). Closes the no-rollup capability gap. Whether to collapse per-group counters (they may be deliberate — distinct creatives each earning their own frequency) is a policy question the RCT informs.

---

## Impact — what it affects

- **Advertiser efficiency (the real prize).** Under *fixed campaign budgets*, capping the over-served tail **redirects** those impressions to fresh reach — same spend, more incremental visits per dollar. That is incremental ROAS, which drives **retention** (leadership's #1).
- **MNTN revenue: ~neutral short-term.** CPM pricing + fixed budgets → total spend is unchanged; impressions are redistributed, not removed. A cap does **not** cut MNTN revenue. Flag explicitly: **attributed IVR / performance metrics will shift** (fewer easy last-touch credits on the capped tail) — this is a reporting artifact of the honest metric change, not a real regression.
- **Strategic infrastructure.** Stands up the reusable **randomized-holdout lift plane** that the HHST intent-gate bandit (Phase 2) and the Q2 incrementality OKR both need. First production MAB → a durable org capability, not a one-off.

---

## Expected improvement — how much better (honestly bounded)

**The RCT sizes the prize; we do not claim it up front — that discipline is the whole point.**

- **Gross-addressable:** ~$3.9M/30d combined (shared-IP-purged) above an 8/wk cap; ~$3.4M of it prospecting. This is spend a cap would stop buying on already-saturated households.
- **Recoverable = the non-incremental fraction of that pool — unknown until measured.** Likely small in retargeting (visits were returning anyway) and larger in the cold prospecting tail.
- **Illustrative only (not a claim):** if 25-50% of the ~$3.4M/30d prospecting over-cap spend is non-incremental, that's **~$0.85-1.7M/30d redirectable to incremental reach** across the covered advertiser set. The RCT converts this range into a measured number.
- **Expected qualitative outcome:** total visits **non-inferior** (stable) while **cost/household drops** = a clean efficiency gain on the capped tail, then a bandit that continuously tunes the cap. If cap-3 fails non-inferiority but cap-8 passes, the incremental floor sits between 3 and 8/wk — still directly actionable.
- **Honest value prop:** for the price of one cheap experiment + a small bidder feature, convert an unknown-but-bounded ~$3.9M/30d efficiency pool into a measured, bandit-optimizable curve — and build the incrementality infrastructure the roadmap needs anyway.

---

## Ready-to-sprint checklist

- [ ] `@SteelHouse/rtb` bidder-feature scoped + owner named + eng effort confirmed (30-min with rtb)
- [ ] Visit-source `guid_log` visit-days join built + validated (reproduces platform 7d rates)
- [ ] Eligible stratum + shared-IP purge + site-wide-pixel universe built
- [ ] Pre-registration locked (freq≥9 count mean/variance measured → N filled → author sign-off)
- [ ] Ring-fenced from concurrent A/B + Fangorn + 0-99 holdout
- [ ] GO/NO-GO decision rule agreed with stakeholders (non-inferiority 5% + cost superiority)
