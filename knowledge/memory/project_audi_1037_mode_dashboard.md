---
name: project_audi_1037_mode_dashboard
description: "AUDI-1037 client-perf diagnostic tool to be delivered as a Mode dashboard (Allison's directive); Nick = Mode SME"
metadata: 
  node_type: memory
  type: project
  originSessionId: 84b2cb82-cfd8-4180-9827-1f0a7ea16899
doc_type: memory
keywords: [audi_1037_mode_dashboard, audi, 1037, mode, dashboard, client, perf, diagnostic]
domain: [project]
lifecycle: active
last_verified: 2026-07-16
---
AUDI-1037's `perf_report` tool (parameterized YoY client-performance diagnostic — ~21 modules + an
overview flag scorecard; run for Bouqs 32147 / Kindred 35094 / Bouqs Subs 31906) is to be delivered as a
**Mode dashboard**. Directive from **Allison**. **Nick** — experimentation team, ex-Criteo (built a
campaign-troubleshooter there), owns the causal-impact Mode dashboard — gave the porting walkthrough
2026-07-07 (`tickets/ti_1037_audience_diagnostic_tool/meetings/ti_1037_01_nick_mode_dashboard_2026_07_07.txt`;
also copied to audi_1070 meetings/).

**Why:** stakeholders (via PECs / leadership, often Neon Pixel escalations) keep asking "why is my campaign
worse YoY"; the answers reduce to ~5 standard reasons (see AUDI-1037 summary), so a self-serve Mode dashboard
lets them run + flag it themselves. The tool's first job is to **verify the client's numbers** before diagnosing.

**How to apply:** mechanics in [[reference_mode_dashboard_porting]]. Load-bearing: **Mode can't render
matplotlib PNGs — rebuild the charts as HTML/JS.** Keep the (advertiser + period) parameterization. Point
Claude at Nick's causal-impact `index.html` in the `modeassets` repo (AUDI = "Audience Intelligence" space)
for styling. Open feature ideas from Nick: advertiser-vs-campaign-group scope toggle, campaign-change events
overlaid on the timeline, decision-tree guided framing. Related: [[reference_hhst_pacing_lever]],
[[reference_within_hi_vr_discriminator]], [[project_intent_tier_pacing]].

**Status 2026-07-08 (evening):** batch 1 functionally complete via paste-deploy (staging =
`perf_report/mode/`; deploy = paste into Mode UI + Run). **6 tabs** (Overview / Audience & Scores / Gate &
Flights / Delivery & Measurement / IP Recirculation / HI Recirculation), 14 queries (params + 13 modules
incl. `13 Pixel Health`), **15-signal flags scorecard** impact-banded (outcomes -> drivers HI-first ->
measurement confounds quarantined) with tracking-change + pixel-change detectors. Params: advertiser =
searchable query-backed dropdown; periods = free date pickers with SQL sentinels (start 1900-01-01 -> Jan 1
of current year; end EXCLUSIVE, clamped to last full month, default 2027-01-01). Semantics: one % basis
(whole-group window spend, RT excluded); HI = 10000 at bid; re-touch = 10000 both times; RTC counts as a
touch; frequency = medians. Next (batch 2): campaign-scope dropdown + module 00 live audience audit.

**Status 2026-07-16:** deploys are now **fully programmatic** — `perf_report/mode/deploy_mode.sh` via Mode
REST API (see [[reference_mode_dashboard_porting]]); paste relay dead. Nick's filters shipped live: campaign
filter at **campaign_group_id** grain (relabeled "Campaign groups" — MT rows are the platform's separate
sibling MT groups, verified on Bouqs), Funnel stage multiselect (modules unified obj IN (1,5,6) +
funnel_level predicate; Stages='1' reproduces old anchors to the cent), Min_Spend_Pct free text box (share
of full-window spend). User-caught: ALL+specific checked together didn't filter → specific-overrides-ALL
predicates (21 across 11 queries, BQ-validated) + exclusive-ALL JS in index.html (unconfirmed vs live
widget). Team wants the API access too (Brian Gereke; #dev-mode-support channel).

**Status 2026-07-08 (night):** module 13 (pixel monitor) audited via 22-agent adversarial workflow —
SQL contract sound (WGU replay reproduces ground truth month-for-month; pruning verified ~831 GB/refresh),
but **fixes pending Malachi's go-ahead**: sentinel regex (6 pseudo-types, not 2 — ~46 false new-type flags),
month scaffold (total pixel stop fails OPEN; 7.8% prevalence), month-truncated bounds, and JS-side fixes
(rank hits before slice(0,4) — Oct'25 STOP hidden behind "+3 more"; sum-spike + $0-placeholder checks on
already-shipped columns; placeholder "thru" mislabels era start; add px to bsrc). Full findings in ticket
summary § "Module 13 pixel-monitor AUDIT".
