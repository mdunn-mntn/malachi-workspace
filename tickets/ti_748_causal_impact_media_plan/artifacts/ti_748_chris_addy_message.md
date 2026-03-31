# Slack Exchange Log — Chris Addy (Media Plan Algorithm)

**Channel:** DM with Chris Addy
**Context:** Kirsa suggested reaching out. Working on causal impact analysis for media plan experimentation (TI-748).

---

## Round 1 (2026-03-27) — Initial Questions & Answers

### Malachi → Chris

Hey Chris! Kirsa pointed me your way — I'm working on a causal impact analysis measuring whether the recommended media plan improves prospecting IVR (TI-748). I've gone through the release brief and requirements doc, but had a few questions about how the algorithm works that would help me interpret the results.

**Quick context on what we found:** The overall IVR effect is near zero across 8 advertisers, BUT we found a strong pattern — advertisers whose plans concentrated budget on fewer publishers (16 networks) saw +10-17% IVR improvement, while those spread across more publishers (26 networks) saw -26 to -31% decline. The degree of concentration appears to predict who benefits.

### Chris → Malachi (2026-03-27)

**1. Publisher count determination:**
Pipeline: semantic search (top 300 candidates) → spend capacity filter (≥$0.50/hr) → scoring & softmax allocation → drop networks below 0.5% → enforce min/max bounds. Config: `min_networks=10`, `max_networks=15`, `min_allocation=0.5%`. 26-publisher plans are above default max — likely old config version or overrides.

**2. Per-publisher score data:**
Full score breakdown exists in API response (Budget model): `score_semantic/_normalized`, `score_performance_advertiser/_vertical/_network` (all with normalized variants), `spendability_score/_normalized`, `score_cpm_efficiency`, `score_scale`, `score_performance_ml_predicted_normalized`, `score` (final combined). Weights: 25% performance composite, 25% quality, 20% semantic, 10% ML prediction, 8% spendability, 6% CPM efficiency, 4% scale, 2% accessibility. Performance composite: advertiser (50%), vertical (30%), network (20%).

**3. Deliverability classification:**
Categorical prediction: "high" (full spend expected), "medium" (moderate underspend risk), "low" (high underspend risk). Guardrail model evaluates per-network daily spend thresholds, audience size, blocked networks, budget constraints. Classification = worst individual guardrail. In-flight override: if >3 days and >90% target pace → upgraded to "high". HHI tracking exists in metrics but NOT a classification factor.

**4. Concentration tuning:**
`alpha=5.0` (softmax temperature) — the big lever. Higher = more concentrated. Also `max_allocation=12%` (cap per network), `min_allocation=0.5%` (drop threshold). Config change, not code change.

**5. Flex Targeting:**
10% budget reserved as flex pool. Un-recommended publisher impressions come entirely from flex pool.

---

## Round 2 (2026-03-31) — Follow-Up Questions & Answers

### Malachi → Chris

Follow-up questions about (1) Lighting NY 16-publisher anomaly, (2) score persistence in BQ, (3) refreshing B&B/Tempo plans, (4) alpha per-advertiser test, (5) ML feature skew, (6) HHI as guardrail.

### Chris → Malachi (2026-03-31)

**1. Lighting New York (16 publishers under old config):**
No per-advertiser override for max_networks — only advertiser-level control is blacklisted networks. Old config had `max_networks=18` initially (later bumped to 25), and old `min_allocation=1%` (vs current 0.5%). Natural pruning: their budget was likely lower or their vertical had fewer networks clearing the $0.50/hr spend capacity threshold. The higher 1% min_allocation floor dropped borderline networks. Worth checking budget relative to peers.

**2. Per-publisher scores NOT in BQ:**
Scores computed transiently in memory during API call. Stored as JSON artifacts in GCS: `media-plan-artifacts` bucket, path `media-plan/{version}/{advertiser_id}/{plan_id}/response.json`. BQ `core.media_plan_publishers` has only final allocations (name + percentage). To query at scale: (a) add BQ sink or (b) parse GCS JSON artifacts. Chris offered to scope if useful.

**3. Refreshing Boll & Branch and Tempo:**
Yes, can regenerate. Would produce plans under current config: `max_networks=15`, `min_networks=10`, `alpha=5.0`, `max_allocation=12%`, spend capacity filtering. Clean before/after comparison on same advertisers. Chris can trigger.

**4. Alpha tuning (7 vs 5):**
Alpha = softmax temperature in budget allocation step. Currently `alpha=5.0` in `MediaPlanConfig`. Options:
- Per-advertiser: Not natively supported, but straightforward to add as request-level override (pass alpha in API request). Cleanest test design.
- Time-based rollout: Just change config and deploy. Simpler but confounds with time effects.

**5. Runtime config (Chris's initiative):**
Several of these questions hit a single theme Chris already wanted to do: **make MediaPlanConfig updateable at runtime** (currently it's a deploy-time constant). This would enable alpha A/B testing, per-advertiser config, and using config in select (publisher selection UI). Chris indicated this is a relatively easy lift.
