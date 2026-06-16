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

**3. Keyword (DS19) evaluation.** *Are the keywords on-target?*
- Resolve names from `tpa.categories`; compare to what BUK/DAR would recommend for the advertiser's domain. Prototype: `classify_keywords.py`.
- Read: flag off-target + over-broad terms (curation gap).

**4. The size funnel.** *How much does each filter remove?*
- MM keyword universe (ipdsc DS19) → **geo** (MaxMind `ST_DWITHIN`, `geo_funnel.sql`) → **exclusions** (`exclusion_bite_on_mm.sql`) → score gate.
- Read (TI-1026): geo is usually the **biggest** filter (~halved the audience); income/age exclusions can be material (LiveRamp active, Oracle inert).
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

**9. Deliverability.** *(TBD — pending Chris Addy deep-dive, TI-1037.)* Olympus/media-plan + supply-side constraints
that bound what's actually servable beyond the audience definition. To be slotted in once scoped.

---

## Backing knowledge (source of truth for each step)
- `data_catalog.md`: ipdsc query hygiene (literal dt; 3P burstiness → ≥30d); `geo.*` MaxMind geo-fence `ST_DWITHIN` pattern.
- `data_knowledge.md`: HHST gate vs score value + OR-include 3P mechanism; bidder-uses-segment-expression (DS14/holdout/RTC); where the UI audience size lives.
- Full worked example + all queries/charts: `tickets/ti_1026_orange_theory_audience_eval/`.

## For TI-1037 (automation)
Each step above = one module: parameterize by `advertiser_id`/`audience_id`/`campaign_id`, run the query, apply the
interpretation rule, emit a standard report section. Step 9 (deliverability) is the open design input. Steps 0–8 are
fully prototyped in TI-1026.
