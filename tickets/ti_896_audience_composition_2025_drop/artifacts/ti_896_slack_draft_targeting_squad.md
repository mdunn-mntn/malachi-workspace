# Draft Slack post — #targeting-squad (per Bryce's 13:25 post)

Target: #targeting-squad first for TI eyes check, then war-room summary.
Deck URL: https://gist.githack.com/mdunn-mntn/f47a6f106ed5ff502cedcb7de50231d8/raw/ti_896_deck_standalone.html

---

**[TI-896] Initial audience-composition findings — 2025 performance drop**

cc @Alex Knorr, @Bryce Wagg, @Ryan Kleck, @Jordan Piepkow

**Headline:** Within the audience-composition lane, one shift dominates — **Peak Performance (DS13) adoption went from 10% on Sep 22 to 30% today**, inflecting the week of Peak Performance launch (Oct 6 2025). Every other bucket (MM / Keywords / 3P / CRM) flat within ±1pp in the drop window. Retargeting share stable Sep–Dec 2025.

**Deck:** https://gist.githack.com/mdunn-mntn/f47a6f106ed5ff502cedcb7de50231d8/raw/ti_896_deck_standalone.html (~14 slides, standalone, self-contained)

**Methodology sanity-check for the squad:**
- Cohort: every advertiser with ≥1 impression on any day in 2025 (`summarydata.sum_by_campaign_by_day`). 4,111 advertisers, 93K active campaigns as of today.
- Source: `dw-main-bronze.integrationprod.archives_audience_segment_archives`, `expression_type_id = 2`, `is_targeted = TRUE`, lookback 2024-11-01 → now (weekly).
- Classifier: regex-extracts `data_source_id` from expression JSON → joined to canonical `data_sources` dim → bucketed to Bryce's 5 categories. For MM I included DS2 + per-advertiser "{AID} - First Party Audience" sources (the 1000+ range in `data_sources`). Flag if anyone sees a better mapping.
- Presence-based, not spend-weighted — a Peak Performance clause attached with zero delivery still counts. Tomorrow I'm adding the delivery-weighted view.

**Two things worth double-checking:**
1. **Max Reach off (Nov 19)** didn't bend any cohort-level composition curve — Peak Performance ramp continued through it. Possible that Max Reach's performance impact shows up in conversion numbers without shifting who advertisers *target*; that's Ray's domain.
2. **Peak Performance scoring bug (Oct 2025)** was fixed end of Oct. Our adoption chart continued ramping through and beyond the fix, so this isn't a scoring-bug artefact — the signal is post-fix.

**Next (for tomorrow):**
- Per-advertiser scatter: Δ(PP share) vs. Δ(conversion rate) Sep–Dec 2025, joined with Ray's conv data
- Default-vs-custom split on Peak Performance audiences (are advertisers accepting the recommended template or building custom on top?)
- Spend-weighted view
- Reconcile my numbers with Alex Knorr's before war-room summary

@Alex Knorr — let me know when yours is ready; happy to compare before we publish anything to war-room. If numbers diverge we'll track down the mapping differences.
