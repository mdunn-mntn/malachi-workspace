---
name: reference-rtc-hhst-gating
description: "RTC scoring only affects bidding when the campaign has HHST set. score_type=rtc in expression JSON doesn't mean RTC is operative."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [rtc, hhst, household score threshold, score_type=rtc, realtime_conquest_score, bidder scoring waterfall, fangorn, bid_events, ryan kleck, gating]
domain: [bidding, audience-scoring]
lifecycle: active
last_verified: 2026-06-01
---
**HHST = Household Score Threshold.** A campaign-level (or advertiser-level) threshold setting that controls whether the bidder uses MM / RTC / Fangorn scores to gate bidding. **HHST is the most important campaign-level scoring switch** — far more important than expression-level features (DS clauses, `score_type=rtc`).

- **HHST set:** bidder filters bid eligibility by score ≥ HHST. Only IPs scoring above the threshold get bid on.
- **HHST not set:** bidder ignores ALL scores (RTC, Fangorn, MM batch, advertiser-level) — they're just metadata.

**Per Ryan Kleck (TI team, 2026-06-01):** the audience-expression `score_type=rtc` flag appears in 99.9% of prospecting expressions, **but RTC is only effective when the campaign has HHST set**. Same applies to `household_score` (per-IP campaign), `advertiser_household_score` (per-IP advertiser), and all other score fields. Per Ryan: "the bidder doesn't use those unless HHST is set... so whether the bidder uses scores is really more about HHST being set or not."

**Why:** Ryan exactly: *"the score doesn't matter if the HHST is not set."* The RTC score (10K to IPs that match a specific `vertical_id` in real-time) only gates bidding when the campaign-level HHST tells the bidder to use it.

**RTC is the FIRST check in the bidder scoring waterfall (Matt Brorby, 2026-06-01).** If RTC fires for an IP, the bid happens via RTC regardless of Fangorn/MM/3P scoring on the same IP — RTC takes precedence. To attribute an impression to a non-RTC signal (e.g., 3P performance): filter to `realtime_conquest_score != 10000` so RTC didn't fire on that impression.

**For Fangorn heterogeneous-effect analysis** (Matt Brorby): filter to where Fangorn-score > HHST at time of bid. Causal impact analysis at the advertiser level (Fangorn-on vs Fangorn-off advertisers) doesn't need this filter — that's advertiser-level treatment, not per-IP heterogeneity.

**How to check if RTC is effective for a campaign:**
- Lookup bids for the campaign_id in `bid_events`.
- Check whether the threshold (HHST) is set.
- If HHST is not set → RTC is no-op for that campaign, even with `score_type=rtc` in the expression.

**Implications:**
- "RTC-touching = 99.9% of prospecting campaigns" is an over-count. The expression flag is universal; the effective scoring is not.
- For analyses where RTC presence matters (e.g., TI-999 3P baseline, MM/RTC pipeline analysis), segment campaigns by HHST-set vs HHST-not-set.
- Pass 21 "Geo-only (no buyer audience layer)" cohort and 3P-only baselines (404 camps / $1.23M) may both be confounded by RTC for the HHST-enabled subset.

**Open / unknowns:**
- Where HHST is stored — Ryan didn't specify (likely a campaign config table or a bidder runtime setting). AUD team for definitive answer.
- Does Fangorn (DS46) also use RTC? User question 2026-06-01.
- How the bidder mechanism actually combines RTC score + HHST threshold + other scores into a bid-or-skip decision. Ryan acknowledged he doesn't fully understand it.

**See also:** [[feedback_crm_polarity_matters_with_mm]] (CRM polarity rule for MM scoring), [[reference_audience_platform_authority]] (Zach Schoenberger is the authoritative source for audience-platform questions).
