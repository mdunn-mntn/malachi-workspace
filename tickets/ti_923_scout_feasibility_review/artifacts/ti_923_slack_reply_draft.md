# Slack reply draft

Short version for the thread. Full per-row comments in the doc.

---

Reviewed against BER-2250 lessons + TI-748 / 884 / 885 / 835. Strong v1 — flagging the high-impact items.

**Big one:** Row 2 MDE formula. `2/sqrt(N)` is the detection threshold (≈95% CI, ~50% power), not 80% power. The standard 80%-power version is `≈ 4/sqrt(N)`, so 600 conv/cell → ~16% MDE, not 8%. Worth restating to avoid setting expectations that we then miss.

**Audience metric (Row 5):** filter on `funnel_level`, not `objective_id` — Ray flagged 48,934 S3 campaigns mis-tagged from a UI migration bug. Also, "high prospecting" alone underpredicts — TI-885 found *intent-tier diversity* is the better lift signal, since most advertisers are high-intent-only.

**Duration (Row 8):** `window × 2` undershoots for 14-day windows (gives 4 weeks). MNTN standard is 6 weeks active + 2 weeks post; floor at that.

**Adds I'd recommend before the table:**
1. **ITT on the existing 10% holdout** — this should be the *first* feasibility check before recommending a geo holdout. Every advertiser already has it, no setup cost. External tests only when ITT is too thin.
2. **Pre-period availability** (≥26 weeks of stable spend in `sum_by_campaign_by_day` — *not* `agg__daily_sum_by_campaign`, only goes back to Sep 2025) — gates CausalImpact-style designs.
3. **Attribution-incrementality tension flag** — TI-835 "Two Stories." High-attribution advertisers systematically show lower lift. Useful for calibrating expectations even when power is fine.

Full per-row comments in the doc.
