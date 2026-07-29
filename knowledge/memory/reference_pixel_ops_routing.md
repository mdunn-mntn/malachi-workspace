---
name: pixel ops bug routing
description: For conversion_log / pixel-firing / order_amt issues, route to Ashley Pineda Varela in pixel ops (per Zach Schoenberger 2026-05-06).
type: reference
originSessionId: 271d838c-9e48-4dcd-93ef-32e17a33ed1a
doc_type: memory
keywords: [pixel_ops_routing, pixel, ops, routing, conversion_log, firing, order_amt, issues]
domain: [reference]
lifecycle: active
last_verified: 2026-07-08
---
For bugs in `conversion_log` related to pixel setup — corrupt `order_amt`, malformed conversion events, advertiser-pixel misfires, etc. — start with **Ashley Pineda Varela** in pixel ops. Confirmed by Zach Schoenberger 2026-05-06 when reporting the trillion-dollar-order bug for advertisers 34957 / 33903 / 32023 / 63746 found via TI-832.

Use when: future session surfaces any conversion_log data quality issue or pixel-integration bug. Send to Ashley first, not to data-platform or chapter-data-engineering.

**Org update (2026-06-23 reorg):** a new **Pixel Signals** team now owns all back-end services/integrations that consume or produce pixel/related data (pixel service, GA, Rockerbox, etc.) — Mgr Dako, under Director Kale, TPM Karli Taylor, PM Paul Reitzen. This likely becomes the home for conversion_log / pixel-firing work; confirm whether Ashley Pineda Varela now sits in Pixel Signals before re-routing. See [[mntn_leadership_chain]] and `knowledge/mntn_business.md` § "Reporting/Data Reorg under Kale".

**Client-side pixel QA chain (2026-07-08, WGU thread):** **Jessica DeLeon** (CS) runs customer-facing "Conversion Pixel QA" docs (doc convention: below-green-highlight = customer-facing; ticket-summary tabs/internal notes are not); **Kevin Cipriani** owns pixel ticket summaries + internal QA notes; **Imani Clark** = Senior Director, Platform (owns client pixel-strategy conversations). Kevin: no MNTN table tracks client tag-manager changes — true for rules, but `core_advertiser_conversion_types` + conversion_log payloads reconstruct what/when (playbook: data_knowledge § "Detecting an advertiser pixel/tag change"). See [[reference-wgu-pixel-case]].

Related: `reference_audience_platform_authority.md` (Zach is source of truth for audience-platform / holdout / expression questions — that's adjacent but different domain).
