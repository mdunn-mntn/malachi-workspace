---
name: project_audi_431_list_refresh
description: AUDI-431 blocklist/whitelist refresh delivered 2026-08-10 — pending Ryan's deploy + Malachi's manual band; pipeline fully scripted for quarterly re-runs.
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [AUDI-431, ecommerce_blocklist, ecommerce_whitelist, missing_domains, ddp_url_verticals, website_crawl_verticals, wcv, vertical corrections, Ryan Kleck, list refresh]
domain: [project, audience-scoring]
lifecycle: active
last_verified: 2026-08-10
---

AUDI-431 (blocklist/whitelist re-assessment) delivered 2026-08-10: 1,641 blocklist + 10 whitelist auto-adds (54.2% of 28d uncategorized visit volume), 76 agreed-wrong wcv verticals in the top 500 by traffic, workbook in Drive `Tickets/AUDI-431/`.

**Why:** the lists went 11 months stale after TI-200; wcv head pollution (yahoo.com→Dating at 2.33B urls/7d) skews ip_vertical_associations.

**How to apply:** two open handoffs — (1) Malachi hand-fills the head of the Manual review tab (1,373 blank rows, volume-sorted) before shipping; (2) Ryan confirms deploy mechanism (bucket drop vs PR) + corrections mechanism (`vertical_manual_overrides/` vs `is_manual_override`); Slack draft at `tickets/audi_431_blocklist_whitelist/artifacts/audi_431_slack_handoff.md`. Re-run quarterly with the scripts in that ticket's `artifacts/` ([[reference_jira_conventions]]; IMP-036/037).
