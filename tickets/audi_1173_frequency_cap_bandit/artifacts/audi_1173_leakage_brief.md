# AUDI-1173 — Frequency-cap leakage: a capability gap, not a sized bug

**Bottom line:** MNTN has **no advertiser-level frequency-cap capability** — `advertiser_frequency_caps` is empty (0 rows); caps exist only per-campaign and per-campaign_group, each keeping its own IP counter with no rollup, so an advertiser's household frequency mechanically leaks across its campaign_groups and funnel stages. That capability gap is **confirmed**. What we **cannot** yet state is how much it's worth: the prior $0.41M–$0.66M/7d over-delivery headline is **retracted** (it used a rejected excess-counting method on shared-IP-confounded, un-purged data). The fix is a **control-plane capability to build** — an advertiser-level rollup counter on the default cap only — whose value and the right cap the RCT must measure. **Do not quote a dollar savings figure.**

> **Supersedes** the prior version of this brief (v1, "$0.41M–$0.66M / 7d … ships without the RCT"). That headline failed adversarial review and is withdrawn in full. See §2.

---

## 1. The capability gap (confirmed — this is the finding)

**`advertiser_frequency_caps` is empty (0 rows).** MNTN has no advertiser-level frequency-cap mechanism at all. Frequency is enforced only at two grains, both of which are populated and live:

- **Per campaign** and **per campaign_group** caps (`bidder.frequency_caps` / `dso.frequency_caps`, `object_type ∈ {campaign, campaign_group}`), synced to the bidder cache and enforced in `do_fcap` before a bid is emitted.
- Counters live in Redis keyed **`rtb:frequency:{ip}:campaign_group_id=<cg>:campaign_id=<c>`** — per campaign_group and per campaign, on IPv4. **There is no advertiser dimension and no rollup key.**

**Consequence (mechanical, not measured):** each campaign_group and each stage holds its own independent IP counter. Nothing sums them. So a single household can be served up to N times *per group* across an advertiser's groups and up to N times *per stage* across its funnel — **delivered frequency can exceed configured frequency by construction**, with no cross-group or cross-stage bound. Fails-open behavior compounds this: on a Redis error the counters silently stop enforcing, so delivered frequency must always be measured, never assumed.

This is a structural fact about the control plane. It does not depend on any spend estimate.

## 2. The magnitude is withdrawn (retraction)

The prior headline — **$0.41M–$0.66M per 7 days**, and its ~$1.8M–$2.9M/mo extrapolation — is **retracted and should not be cited.** Three independent defects, any one of which invalidates it:

1. **Rejected excess method.** The lower/upper bounds leaned on "Method A" (roll every counter up to the household's heaviest single counter; excess = `total_imps − heaviest`). This counts excess even on households whose total delivery is already below any plausible cap — impressions no cap would ever suppress. It measures counter fragmentation, not recoverable over-delivery.

2. **Shared-IP confound in the household key.** The household is `(ip, advertiser_id)`. A NAT/CGNAT gateway collapses many real households behind one IPv4 into a single `(ip, advertiser)` unit, manufacturing apparent multi-group / multi-stage exposure. Apparent leakage rate **rises monotonically with the number of distinct advertisers sharing an IP** — the signature of contamination, not of a real cross-group defect.

3. **The purge did not fix it.** The re-run applied the same shared-IP purge as the freq-curve (`ndev≥51 OR nadv≥121 OR nimp≥501` per IP). Those thresholds were calibrated for a 30-day window; over the leakage brief's **7-day** window they trip for only **~0.34% of households** — a near-no-op. The confound is a continuous gradient across the whole IP-sharing distribution, so a fixed-threshold purge of the extreme tail cannot remove it regardless. The purged numbers are therefore **not** the honest number either.

**Net:** the observational leakage percentages (raw multi-group / multi-stage / prospecting∩retargeting shares) and every dollar figure derived from them are **unreliable as a magnitude** and are withdrawn. The confirmed content is the *existence* of the leak path (§1), not its size.

## 3. Framing: a capability to build, whose value the RCT measures

This is **not** an obvious bug with a sized savings. It is a **control-plane capability MNTN does not have**:

- **Build:** one advertiser-level rollup counter in the bidder (`rtb-campaign-service`), keyed **`rtb:frequency:{ip}:advertiser=<aid>`**, incremented on every won impression alongside the existing per-group / per-campaign counters, and evaluated in `do_fcap` so an advertiser's fragmented counters collapse to a single household frequency.
- **Scope:** the **DEFAULT cap only** — never touch advertisers with `has_custom_frequency_caps=TRUE`. The rollup governs the default; explicit advertiser choices stay untouched (client-transparency constraint).
- **Open policy question:** per-group caps may be **deliberate** — different creatives per campaign_group each earning their own frequency budget is a legitimate design, not necessarily an error. Collapsing to one advertiser counter is a **policy change**, and whether it *helps* (recovers value via reallocated reach) or *hurts* (starves distinct-creative delivery) is exactly what the RCT settles.
- **Value + cap value = RCT outputs.** Whether the capability is worth building, and what the right advertiser-level cap is, come from the household-randomized cap RCT (§6 of the scope doc) — not from an observational sizing. Even if a magnitude were clean, fixed campaign budgets make any capped spend **redirectable, not saved**: the value is incremental reach, whose lift only the RCT can price.

## 4. What is defensible vs withdrawn

| Claim | Status |
|---|---|
| No advertiser-level frequency-cap capability (`advertiser_frequency_caps` empty) | **Confirmed** |
| Counters are per-campaign / per-campaign_group, IP-keyed, no rollup | **Confirmed** (structural) |
| Frequency can leak across an advertiser's groups and stages | **Confirmed** (mechanical consequence) |
| Fails open on Redis error → measure delivered, not configured | **Confirmed** (structural) |
| Any $ over-delivery / savings figure ($0.41M–$0.66M/7d, ~$1.8M–$2.9M/mo) | **Withdrawn** |
| Observational leaked-household % (cross-group, cross-stage, prosp∩retgt) as a magnitude | **Withdrawn** (shared-IP confounded; purge ~0.34%, no-op) |
| Whether collapsing to an advertiser counter helps or hurts, and the right cap | **Open — RCT decides** |

---

*Provenance: capability gap and counter mechanics from scope §3 (`artifacts/audi_1173_scope.md`) and `queries/audi_1173_leakage_cross_group_stage.sql` (the `advertiser_frequency_caps` empty check + cap-type split). The withdrawn magnitude came from the raw, un-purged cut in `outputs/audi_1173_leakage.json`; the 7-day purge weakness is visible against the 30-day purge in `outputs/audi_1173_delivered_freq_curve.json`. No new BigQuery was run for this rewrite.*
