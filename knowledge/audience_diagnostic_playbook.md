# Audience / Client-Performance Diagnostic Playbook

**The systematic sequence TI runs when a stakeholder asks "why is this client performing this way / why is the
audience small / why are these segments bad?"** Codified from TI-1026 (Orange Theory). This is the **spec for
TI-1037** (productize into a parameterized tool). Always start from the audience expression.

Inputs: `advertiser_id` (+ `audience_id` / `campaign_id`). Prototype queries: `tickets/ti_1026_orange_theory_audience_eval/queries/`.

> **The one rule that breaks naive analysis:** the **bidder uses the SEGMENT expression** (`audience.audience_segments`),
> NOT the user's `audience.audiences`. The segment expression AND-layers automated clauses (DS14 activity, holdout,
> RTC, retargeting) the user never sees. Pull both. (data_knowledge.md "bidder uses the SEGMENT expression".)

---

## The steps (question → source → how to read → gotcha)

**0. Pull both expressions.**
`audience.audiences` (user selections) and `audience.audience_segments` (what the bidder evaluates). Parse data
sources, include/exclude, geo, and the automated clauses. Prototype: `parse_expression.py`.

**1. Decompose the segment expression.** *What is this audience actually targeting?*
- Root `categories.where.op = "and"` of: `or(MM keywords DS19, 3P DS35…)`, `not(excludes)`, `any(DS14)`, retargeting.
- `geo.radii_include` (lat/long × radius) = studio/location fences. `select.score` = RTC directive. `select.count.holdout` = md5 bucket.
- HHST gate: `dso.household_score_thresholds` (per campaign; 0 = no gate, ~64% of campaigns).

**2. Interest-segment (3P) quality.** *Are the bought segments any good?*
- Reach + **overlap with the keyword (MM) layer** + deprecation + modality fit. Query: `per_segment_reach_7d.sql`, `reach_overlap_7d.sql`.
- Read: high keyword-overlap = redundant; low-overlap = low-intent. Off-modality = mistargeted.
- **Gotcha:** 3P (DS35) ipdsc delivery is **bursty (~2–4 days/month)** — never judge a segment from one day/week; use a **≥30-day window**. (data_catalog.md ipdsc note.)
- **⚠ Gotcha — overlap-with-MM is only an intent signal when MM is a TARGETED (small) universe; ALWAYS base-rate it.**
  The overlap % means "redundant/low-intent" only relative to the base rate `MM_distinct / population`. If the advertiser's
  MM keyword layer is near-universal, *every* segment — even unrelated ones — overlaps it at the base rate, so overlap
  carries zero intent signal. **Run a control** (overlap of a deliberately-unrelated segment with this advertiser's MM)
  or compute the base rate before reading overlap. (iMemories 2026-06-23: 211 keywords → **174.5M-IP** MM universe; own 3P
  overlapped 67–73%, unrelated control segments 67.1–67.5% → overlap uninformative. Contrast OTF: MM ~4.6M → 12% overlap
  was a real low-intent signal.) A near-universal MM universe is itself a finding: the keyword layer is barely targeting,
  and the bigger lever is curating MM, not picking 3P.

**3. Keyword (DS19) evaluation.** *Are the keywords on-target?*
- The targeting keywords are the **selected CHILD keywords** in `ui.audience_keyword_state` (= the DS19 expression);
  the UI/customer only sees the **~20 PARENT seed keywords**. MNTN-Matched flow: 20 LLM parents → ~200 products →
  N DS19 children via embedding match (drift happens at the embedding step). Resolve names from `tpa.categories`.
- **MUST filter `is_magic = false`** — `is_magic` keywords are **untargetable** UI artifacts (exist so the size
  estimate moves on UI edits); don't count them as off-target targeting. `classify_keywords.py` does NOT know about
  `is_magic` — join `ui.audience_keyword_state` to exclude them.
- **Authoritative BUK/DAR comparison:** the 20 parents + BUK recs are at the shopper-graph autopilot endpoint
  `https://shopper-graph.in.mountain.com/autopilot?advertiser_id=<id>` (VPN-only, per Alex). That's the proper DAR comparison.
- Read: flag the real (non-magic) off-target/over-broad children (curation gap from the embedding step).

**4. The size funnel + exclusion quality.** *How much does each filter remove, and are the exclusions any good?*
- Funnel: MM keyword universe (ipdsc DS19) → **geo** (MaxMind `ST_DWITHIN`, `geo_funnel.sql`) → **exclusions**
  (`exclusion_bite_on_mm.sql`) → score gate. Geo is usually the **biggest** filter (~halved the audience).
- **Demographic-exclusion quality** (`income_provider_agreement.sql`, `income_distribution.csv`): when multiple
  providers offer the same attribute (HHI bands from Equifax/Experian/TransUnion/Oracle), check (a) **cross-provider
  agreement** — they barely agree (0.36% three-way on "low-income") → IP-level demo is unreliable; (b) **distribution
  realism** — Equifax/IXI skews affluent (3.6% <$30K, asset-based, under-labels low-income), Experian HHI is realistic.
  Guidance: **never stack** (clauses OR'd → union of every provider's errors); pick the **realistic** provider (not the
  most conservative); treat demo as a coarse last resort; lean on intent scoring. Inert providers (Oracle = 0 ipdsc)
  exclude no one. Deliverable pattern: `artifacts/ti_1026_exclusions_talk_track.md`.
- **Gotcha:** filter ipdsc `dt` with a **literal** (partition prune); use `[.]`/regex for IPv4 parse (NET funcs error on bad/multi IPs, no SAFE). (data_catalog.md.)

**5. Scoring / HHST.** *Does the score gate explain delivery + why 3P fails?*
- `dso.household_score_thresholds` (the gate) + delivered score distribution from `cost_impression_log` (`delivered_score_dist.sql`).
- Read: gate ON → unscored 3P-only IPs filtered (≈inert); gate OFF → bids unscored 3P = the bad traffic. (data_knowledge.md "HHST GATE vs value".)

**6. Availability (stock vs flow).** *Is the audience deliverable, or just big on paper?*
- Realized reach + frequency + daily fresh-IP supply from `cost_impression_log` (`availability.sql`, `availability_daily.sql`).
- Read: low frequency + fresh IPs arriving daily = NOT pool-exhausted (room to scale spend). Frequency spiking + fresh-IPs→0 = availability-limited. The **DS14 7-day augmentor filter is the platform's formal availability gate.**

**7. Targeting vs creative.** *Is poor performance our targeting or their ads?*
- Score→visit-rate gradient (`visitrate_by_score.sql`, join `cost_impression_log`×`clickpass_log`) + peer benchmark (`ctv_vr_benchmark.sql`).
- Read: score discriminates VR → targeting works; if blended VR is still low-percentile vs peers and even the top tier only hits peer median → ceiling is **creative/offer (their side)**. True incremental proof needs a **holdout**.

**8. UI size vs deliverable.** *Why does the displayed audience size mislead?*
- UI size = `perml.flight_cid_day_audience_sizes` (stage-1 campaign; `funnel=total`) / `external_ddm.segment_sizes` (GCS) / `eval_batch` API.
- Read: the UI number reflects the raw user expression (≈ MM ∪ 3P national) and **does NOT apply geo, DS14, or holdout** — it can be ~5× the deliverable. Anchor on realized reach, not UI size. (data_knowledge.md "Where the UI audience-size number lives".)

**9. Deliverability.** *Can the campaign actually spend its budget?* (Scoped via Chris Addy deep-dive, 2026-06-18.)
- **There is NO predictive targetable-IP model.** The platform does not compute "what % of this audience's IPs will be
  biddable in period X." Do not promise a targetable-% number — none exists. Answer deliverability via realized
  reach/frequency (step 6) + peer pacing (this step).
- **Deliverability = peer-calibrated budget pacing.** The operative target is empirical-by-analogy: what did
  **comparable campaigns** require (over the last **~60–90 days**) to reach **96% of budget** (the platform's
  "fully-delivered" bar), then judge this campaign's spend against that, scaled by **flight length**.
- **Build = a peer-pacing benchmark — a sibling of step 7's peer-VR benchmark.** Cohort comparable campaigns
  (CTV, vertical, geo footprint, budget tier, audience shape, HHST on/off) over 60–90d; compute the spend/pacing that
  got peers to ≥96% of budget; place the target campaign in that distribution. Queryable in-tool from spend + budget +
  delivery logs (no Olympus black box) — reuses step-6 delivery data + the step-7 peer-cohort machinery.
- Read: peers at this budget hit 96% but ours doesn't → NOT a budget problem; it's audience/targeting (cross-check
  step 6 for a delivery pause vs exhaustion). Even peers can't sustain 96% at this budget → budget too high for the
  audience shape (lower budget, lengthen flight, or grow the pool via geo/keywords).
- **Gotcha:** 96%-of-budget is the "delivered" definition; measure pacing over a window ≥ the flight (or normalize
  per-day). Spend = `spend_log` (nanosecond epoch); budget/cap field still to be located (campaign config / dso). The
  comparable-campaign selection is the design-sensitive part — a bad cohort yields a meaningless benchmark.

---

## Backing knowledge (source of truth for each step)
- `data_catalog.md`: ipdsc query hygiene (literal dt; 3P burstiness → ≥30d); `geo.*` MaxMind geo-fence `ST_DWITHIN`
  pattern; `ui.audience_keyword_state` (PARENT-seed vs CHILD-DS19, `is_magic` untargetable, 20→200→N flow, shopper-graph BUK).
- `data_knowledge.md`: HHST gate vs score value + OR-include 3P mechanism; bidder-uses-segment-expression (DS14/holdout/RTC);
  where the UI audience size lives (overstates); 3P demographic data quality (provider 0.36% agreement, Equifax/IXI asset-skew, don't-stack).
- Full worked example + all queries/charts/talk-track: `tickets/ti_1026_orange_theory_audience_eval/`.

## For TI-1037 (automation)
Each step above = one module: parameterize by `advertiser_id`/`audience_id`/`campaign_id`, run the query, apply the
interpretation rule, emit a standard report section. Steps 0–8 are fully prototyped in TI-1026; **step 9
(deliverability) is now scoped** (peer-pacing-to-96%-of-budget, 2026-06-18) and buildable from spend/budget/delivery
logs — no external Olympus dependency.
