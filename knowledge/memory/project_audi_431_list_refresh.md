---
name: project_audi_431_list_refresh
description: AUDI-431 DONE and deployed to prod 2026-08-11; the repeatable process lives in the ecommerce list refresh runbook, follow-ups are AUDI-1202 (quarterly) and AUDI-1203 (full 3.3M audit).
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [AUDI-431, AUDI-1202, AUDI-1203, ecommerce list runbook, ecommerce_blocklist, ecommerce_whitelist, missing_domains, ddp_url_verticals, website_crawl_verticals, wcv, vertical corrections, Ryan Kleck, list refresh]
domain: [project, audience-scoring]
lifecycle: active
last_verified: 2026-08-11
---

AUDI-431 closed 2026-08-11, **deployed to prod by us** (not handed to Ryan — Malachi: "it's not Ryan's side, WE own it"). Live: blocklist 1,464 -> 4,395, whitelist 3,310,123 -> 3,310,225, plus 26 vertical corrections in `vertical_manual_overrides` (412 -> 438). 94.4% of uncategorized visit volume resolved; **76 real stores rescued** from the blocklist (3.06% error rate on knowledge-only judgment).

**Why:** the lists had gone 11 months stale and nobody had ever verified them against live sites.

**How to apply:** do NOT re-derive the process — it is written down. Quarterly re-run: `documentation/runbooks/ecommerce_list_refresh_runbook.md`, tracked as **AUDI-1202 (due 2026-11-11)**. Full 3.3M-domain audit of both lists: `documentation/runbooks/ecommerce_list_full_audit_plan.md`, tracked as **AUDI-1203**. Both Jira descriptions link the runbooks so a cold start works. Scripts: `tickets/audi_431_blocklist_whitelist/artifacts/` — `audi_431_validate_deploy.py` is the pre-deploy gate and must exit 0. Deploy discipline: back up to `.../backup_pre_<ticket>_<date>/`, stay strictly additive, verify from the LIVE object. Known-and-accepted: 365 domains are in both lists (blocklist wins, so inert). [[feedback_one_resolver_for_shared_state]] [[reference_pihole_dns_contaminates_fetch]] [[feedback_constrain_llm_to_real_taxonomy]]
