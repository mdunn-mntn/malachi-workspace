---
name: reference_advertiser_vertical_correction
description: "How to fix a mis-tagged advertiser vertical — Shopper Graph API, SoT CoreDB, owner Alyson"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 758d4fa9-d9c5-4441-891f-0424674f4b12
doc_type: memory
keywords: [advertiser_vertical_correction, advertiser, vertical, correction, tagged, shopper, graph, coredb]
domain: [reference]
lifecycle: active
last_verified: 2026-07-27
---
Correcting an advertiser's assigned vertical: operational source of truth is **CoreDB** (Postgres); the vertical is set/changed by **an API call to the Shopper Graph service**, not by editing BQ. BQ `bronze.integrationprod.fpa_advertiser_verticals` → `silver.fpa.advertiser_verticals` is a read-only Datastream CDC mirror — cannot patch it directly.

Owner/contact who makes the change: **Alyson Lefkowitz** (verified 2026-07-27, AUDI). Kirsa flagged AID 69864 "Lake Erie Heritage Foundation" mis-tagged B2B; Alyson corrected via Shopper Graph.

Mechanics: both rows move together — type=0 parent + type=1 sub. Fangorn rollout scorer keys on the `type=1` sub `vertical_id` (= RTC vertical), so a wrong sub is functional not cosmetic. Propagation to BQ lags the source change (CDC batch) — re-query to confirm.

Worked example: 69864 B2B (parent 104 "B2B Software & Services" / sub 104012 "B2B - Sales & Marketing") → Travel (parent 135 "Travel" / sub 135006 "Travel Destination Promotion"). See `knowledge/data_catalog.md` §silver.fpa.advertiser_verticals.
