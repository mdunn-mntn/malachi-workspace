---
name: audi-1111-vendor-quality
description: "AUDI-1111 epic (Vendor Data Quality & Valuation) — 1115/1116/1117 measured+verified 07-16/17; billing=WON-impression single charge (BAE ddp_mm_winners_imp), ~0.2% of ingested bills; per-imp media CPM ~$10.7 vendor-independent → rate fine, preemption is the lever; two grains of vendor-unique (5.6M membership vs 27.5M impression); RTC vendor-independent 0.01%; DS14 pool 97M free-stale vs 95.7M vendor-only; routing Alyson→Mike+Kale→Paulo"
metadata: 
  node_type: memory
  type: project
  originSessionId: cd88fb3d-15ea-4c2f-a714-1f519abde06b
doc_type: memory
keywords: [audi_1111_vendor_quality, audi, 1111, vendor, quality, epic, data, valuation]
domain: [project]
lifecycle: active
last_verified: 2026-07-22
---
**AUDI-1111** epic (research-for-a-proposal; implementation separate) from the 2026-07-16
AUDI-1089 stakeholder readout. Children: AUDI-1093 (preemption spec, re-parented) + 1113
(preemption impl, unassigned — Sean's team?) + 1114 (5-vendor outreach checklist, unassigned —
Alyson?) + 1115/1116/1117 (Malachi analyses). Folder `tickets/audi_1111_vendor_quality/`
(epic summary §4b = findings snapshot; child folders inside).

**AUDI-1093 CLOSED (Done) 2026-07-22** — the investigate+spec ticket is complete: behavior
confirmed (1/N split, free logs in divisor at $0 CPM, meter does not preempt), overpayment
quantified at two grains (domain floor ~$275K / vertical-targeting $412.4K, roster $812.4K→$400.0K),
fix spec validated by the BAE billing team and handed off. Execution lives in AUDI-1113 (impl) +
AUDI-1144 (contract-owner discussion) + AUDI-1145 (pipeline ownership). ⚠ DUP: AUDI-1113 (epic
child) and standalone AUDI-1143 are the SAME impl ticket ("no paid-vendor credit when a free log
covers the imp") — both Backlog; one should be closed as a duplicate / re-parented. Flagged to user.

**All three analyses MEASURED + 5-agent adversarially VERIFIED (2026-07-16/17):**
- **1115 WTP, 4 lenses (L0 meter / L1 ingested / L2 flow-filtered unique / L3 bid-won):**
  verdict lens-invariant — no metered vendor breaks even at $0.50 (L0 break-even: 33Across
  $0.086–0.257, 33A API $0.127–0.381 closest, Sovrn/Justuno/Cybba ≤$0.15). Flow filter
  (meeting rule: no same-day free-log credit, 30d lookback, BOTH logs) drops free-union
  coverage 59.4%→44.1%. L2 anchors reproduce deck_d1 EXACTLY. Value bands = q8b solo
  (excludes free logs only — bands overlap across vendors, never sum).
- **1116 RTC:** vendor-only 0.01% of RTC-fired imps (99.59% guid-realtime). svs `uid` is a
  ULID → ingest-latency instrument (data_knowledge §svs): free logs 0min, vendors 2.4–8.6h
  (Predactiv ~12h) = the DAG's CONFIGURED per-DS lag hours. Freshness SLA = renegotiation lever.
- **1117 DS14:** gate soft on CTV (87.8% upper bound; display = same-day-aug echo, not
  evidence; funnel_level 4 exists but negligible). 36.1% of 301.5M svs IPs in-gate; expansion
  97.0M free-stale (widen free windows, free) vs 95.7M vendor-only; 33Across 50% non-biddable
  at arrival. Rec: widen free windows before paying vendors for reach.

**L2 engineering note:** single-query flow scan hit BQ's 6h job limit → day-bitmask (INT64
per pair, no window sort) + 4 IP-hash shards + exact histogram merge (`audi_1115_l2_flow_shard.sql`
+ `l2_merge.py`) — landed <1h, anchors exact. Pattern reusable for any pair-history scan.

**BILLING STRUCTURE + CPM LAYER (2026-07-17, Alyson pointed to the BAE gold table; 3-agent
verified):** billing = single charge at the WON impression (`dw-main-gold.reporting.ddp_mm_winners_imp`,
keyed on ad_served_id), gated by DS13/DS19 usage, NOT ingestion — only ~0.2% of ingested rows
bill. Queries: `l0b` (BAE recon — free-only rows tv_cpm=$0, mixed rows still $0.50 = 291M/mo
preemption gap) + `l0f` (fractional-credit CPM = BAE winners ⋈ CIL media). KEY RESULT: per-
credited-impression media CPM ~$10.7 = MNTN's CTV media rate, **vendor-independent** → break-even
$1–3 for every vendor → $0.50 is BELOW break-even on the residual. So the rate isn't the lever;
**preemption (removing ~90% free-log overlap volume) is.** l0f is a PRICING lens, NOT keep/drop
(over-credits non-marginal imps). The two grains of "vendor-unique" (IP-membership ~5.6M/mo =
marginal/keep-drop vs impression-winner ~27.5M/mo = credited/pricing, ~5× apart) → the exact
grain the eng team's fractional-credit system uses is THE 07-20 sync question (sets residual
volume/total). Also from BAE: credit splits across matched DATA PATHS (3P segments DS17 @ $0.95
CPM in the denominator). Fractional-credit CPM sheet now in BOTH the quality + deck workbooks
(shared `cpm_fractional_sheet.py`). See [[reference-ddp-billing-logic]], data_knowledge § billing,
valuation framework § 2026-07-17.

**Why:** proposal spine = "the price, not the data, is the problem; the real-time layers don't
need vendors." Routing: consolidated proposal → pre-read Alyson → Mike + Kale → Paulo.
**Open:** flat-fee bills (Maya) = 24 xlsx cells; 1113/1114 owners at grooming; Monday
2026-07-20 billing credit-rule meeting (fractional vs Victor's first-reporter); CTV soft-gate
mechanism (MemDB or Zach/Sean); candidate tickets NOT yet created (selection-bias/IPDSC test;
keyword campaign-usage report). Related: [[audi-1089-ddp-evals]] [[reference-ddp-billing-logic]]
[[reference_ddp_valuation_framework]].
