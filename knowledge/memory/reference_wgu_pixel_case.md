---
name: reference-wgu-pixel-case
description: "WGU (31357) pixel/revenue case — revenue was never real ($1/lead placeholder), Sep'25 client retag broke amount+semantics, lead event fires under DEAD AID 10942"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 7c5afb02-06fb-4890-830b-8e042ffcc60f
doc_type: memory
keywords: [wgu_pixel_case, wgu, pixel, case, 31357, revenue, real, lead]
domain: [reference]
lifecycle: active
last_verified: 2026-07-08
---
WGU (31357) pixel forensics, closed 2026-07-08 ("WGU-REV"). Load-bearing facts for any WGU metric question:

- **Dashboard "Revenue" was NEVER dollars**: old pixel hardcoded `shoid=lead&shoamt=1` on one inquiry-form step → revenue == conversion count exactly, all queryable history (2024-01 → 2025-10-02).
- **2025-09-30 22:57:04 client-side retag** (zero MNTN config changes): new `app_submitted` type fires on enrollment-portal PAGEVIEWS (69% = application-status re-checks) with the literal unfilled macro `shoamt=ORDER AMOUNT` → order_amt NULL → revenue $0 from 10-03. Conversions 3.2×'d overnight → **CPA/conversion trends across Oct 2025 are apples-to-oranges** (54+ CPA goal groups affected; WGU has NO ROAS goals).
- **Lead event now DARK**: `whatAreYourGoals` pixel fires under **dead AID 10942** (no advertisers row; legacy SteelHouse tag, `px.steelhousemedia.com`, passes `shopid=<Salesforce record id>`) — ~18K fires/mo, ~226K since Jan 2025, no attribution/reporting. Fix = re-point to 31357, don't delete.
- **Feb 2026 $833,883.40 = pentest garbage** (Burp scan, one IP, 2026-02-07; reconciles bronze $71.7T → silver $222.9M → attributed to the dollar).
- **Untyped LP tag cycles on/off** in WGU's Adobe Launch: on 2026-04-30→05-16 (~5K/day), off, **on again 06-24→present** (~2K/day) — matches CS pixel-QA notes.
- **WGU stance (Imani Clark)**: lead-focused, revenue not a priority. Open: event taxonomy, 10942 re-point, CPA-goal resets.

**Full detail:** `knowledge/data_knowledge.md` § "WGU (31357) revenue — it was NEVER real", § "Conversion pixel payload anatomy", § "Detecting an advertiser pixel/tag change" (the reusable playbook), § "WGU YoY comparisons are confounded" (adds the separate **Jul 2025 visit-tracking step**, IVR re-based 1.2%→2.2%). People: [[pixel ops bug routing]].
