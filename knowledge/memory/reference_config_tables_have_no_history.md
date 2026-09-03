---
name: reference_config_tables_have_no_history
description: "MNTN config dimensions in dso.* and public.* are current-state with no history and churn daily; joining one to a past measurement window silently mislabels rows - look for the archives.*_archives twin first"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [config table, current state, no history, archives, frequency_cap_archives, dso.frequency_caps, slowly changing dimension, as-of join, update_time, mislabel, historical window, TI-1313, campaign config]
domain: [bigquery, data-catalog]
lifecycle: active
last_verified: 2026-09-03
---
**A config dimension read today does not describe a campaign that ran two months ago.** MNTN's `dso.*` and
`public.*` config tables hold one row per entity with no history, and they churn hard: `dso.frequency_caps`
carried a fresh `update_time` on **12,197 of 125,672 rows in a single day**.

On TI-1313 this silently corrupted an attribute finding. **144 of 433 campaign groups (33%) ran a different
frequency cap during the window than they carried at query time.** With current-state labels "No household
cap" pooled highest and the attribute read p=0.0002; with in-window labels it is **second from bottom** and
p=0.0112. The conclusion reversed.

**How to apply.** Before joining any config field to a historical window, look for the versioned twin in the
**`archives`** dataset (`archives.frequency_cap_archives` mirrors `dso.frequency_caps` plus `version`,
`archive_create_time`, `datastream_metadata`). It covered **433 of 433** here. Pattern:

```sql
ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
-- WHERE update_time <= <window end>, then keep rn = 1
```

`region-us-central1.INFORMATION_SCHEMA.TABLES` is the way to find a twin (`LIKE '%<thing>%'`). **Do not use
`update_time` on the live table as an "advertiser edited this" marker** - it behaves as a pipeline refresh
stamp, and gating on it excluded 418 of 433 rows. The archive is large; budget ~3.5 GB for a naive scan.
Fields with the same exposure: geo targeting, audience config, attribution window, customer-file exclusion.
Detail in `knowledge/data_catalog.md` (18). Related: [[feedback_bq_workflow]], [[feedback_contradictions_are_appended]].
