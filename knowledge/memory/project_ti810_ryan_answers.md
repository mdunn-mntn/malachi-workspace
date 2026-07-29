---
name: TI-810 Ryan deployment answers
description: Ryan's answers on feature store backfill, DAG changes, dev→prod copy process (2026-04-02)
type: project
doc_type: memory
keywords: [ti810_ryan_answers, ryan, feature store backfill, dag changes, dev to prod, gsutil cp, aug_log hourly, PR #962]
domain: [project, repos]
lifecycle: active
last_verified: 2026-04-02
---
Ryan's answers on TI-810 deployment (2026-04-02):

- **Backfill range:** 30 days is enough
- **Hourly backfill:** Must run every hour AND the daily rollup. Daily depends on hourly — run it like production.
- **Dev→prod copy:** `gsutil -m cp -r` is correct
- **DAG changes:** YES, add DAG changes to the PR. It should be prod-ready once merged.
- **Don't overwrite:** Don't name models the same as existing ones (our names are all new — no conflict)
- **Column naming:** Ryan will inspect output after one day test. Won't be too strict but "maybe a little"

**Why:** These answers define the deployment sequence. The hourly requirement means backfill will take significantly longer (~30 days × 24 hours = 720 hourly runs for aug_log alone).

**How to apply:** Follow this exact sequence. DAG changes go in PR #962 on the feature branch. Don't skip hourly runs for aug_log — the daily model reads from hourly output via read_model.
