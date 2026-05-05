# Slack reply draft (corrected)

Short version for the thread. Full per-row comments in the Jira ticket (TI-923).

---

Reviewed against BER-2250 lessons + TI-748 / 884 / 885 / 835. Strong v1 — flagging the high-impact items.

**Big one — Row 2 MDE formula.** `2/sqrt(N)` is the detection threshold (≈95% CI, ~50% power), not 80% power. The standard 80%-power version is `≈ 4/sqrt(N)`, so 600 conv/cell → ~16% MDE, not 8%. Worth restating to avoid setting expectations we then miss.

**Row 5 prospecting filter.** Specify the source field — use `campaigns.objective_id IN (1, 5, 6)` for the numerator (or equivalently `NOT IN (2, 4, 7)`). Don't filter on `funnel_level` — that's MNTN product stage, and every stage contains both prospecting and retargeting (verified: 21,639 retargeting campaigns sit inside `funnel_level=1`). Also: "high prospecting %" alone underpredicts — TI-885 found *intent-tier diversity* (% spend outside high-intent IP-buckets) is a stronger lift signal, since most advertisers are high-intent-only.

**Row 8 duration.** Formula is right in spirit but produces durations that are too short for short windows. `window × 2` gives only 4 weeks for a 14-day window and 2 weeks for a 7-day window — both well below the TI-885 floor. The fix is a `max()`:

> `max(conversion_window × 2, 6 weeks)` active + 2 weeks post.

The 6-week floor exists because ad delivery and CTV viewer behavior both need ~6 weeks to settle — independent of the attribution window. For 30d / 45d conversion windows, the `× 2` rule still binds; for 7d / 14d, the 6-week floor binds.

**Adds I'd recommend before the table:**
1. **ITT on the existing 10% holdout** — should be the *first* feasibility check before recommending a geo holdout. Every advertiser already has it, no setup cost. External tests only when ITT is too thin.
2. **Pre-period availability** (≥26 weeks of stable spend in `sum_by_campaign_by_day` — *not* `agg__daily_sum_by_campaign`, only goes back to Sep 2025) — gates CausalImpact-style designs.
3. **Attribution-incrementality tension flag** — TI-835 "Two Stories." High-attribution advertisers systematically show lower lift. Useful for calibrating expectations even when power is fine.

Full per-row comments in TI-923.
