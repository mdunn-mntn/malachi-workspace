---
name: project_ap_score_feed_migration
description: "In-flight plan: AP re-serves AUDI's advertiser/campaign scores as waterfall segment scores and the bidder drops AUDI's feed — a consumer migration of AUDI's score pipeline, unconfirmed with Alyson"
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [ap score migration, advertiser_scores, campaign_scores, segment_scores, waterfall score, bidder score feed, intent score dumps, membership consumer, zach schoenberger, mike dolt, jaime mutale, daniel hartnett, audi-1016]
domain: [bidding, audience-scoring, project]
lifecycle: active
last_verified: 2026-08-25
---
Per Zach Schoenberger (Slack, 2026-08-25, in the AUDI-1016 empty-segments thread): the bidder today loads `advertiser_scores` + `campaign_scores` from **AUDI** (intent-score GCS dumps) and `segment_scores` from **AP** (mostly RTC, >1 year). **AP is now consuming those same AUDI advertiser/campaign scores and should be testing sending down segment scores carrying the same waterfall score; the bidder then stops reading the scores AUDI sends.**

**Why:** this is a consumer migration OF AUDI's pipeline, discovered incidentally — nobody told AUDI. If it lands, the intent-score dump AUDI produces loses its serving consumer (the bidder), which changes that pipeline's criticality (it currently pages Malachi).

**How to apply:** confirm with Alyson that AUDI knows; verify the waterfall-through-AP path preserves AUDI's score semantics before the bidder cutover; revisit the intent-dump paging/ownership once the bidder stops reading it. Related: [[project_backlog_gate_pings]] (AUDI-1016), [[reference_bidder_serving_stores]].

**Appended 2026-08-26 (Eric, secondhand + hedged — do not overwrite Zach's version):** Eric's TLDR says "advertiser scores will be moved to a kafka topic and that entire flow will go away," done by Zach (per a Zach↔Ryan conversation; Eric: "not an area I fully follow"). May compose with the AP-waterfall description (AP as source, Kafka as transport) or conflict with it; settle with Zach before acting. Same TLDR: segment updates move to **MNTN-ID delta updates** (kills the empties), with an IP-based residual Eric says "will remain effectively forever."
