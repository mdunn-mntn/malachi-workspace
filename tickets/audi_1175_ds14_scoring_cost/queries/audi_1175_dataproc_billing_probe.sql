-- AUDI-1175 Gate B: does the audience_intent scoring bill actually drop, and is there a committed-use discount?
-- STATUS: NOT runnable by the analyst — GCP billing export + Dataproc IAM are access-walled
--   (dataproc.batches.list = PERMISSION_DENIED on dw-main-*; no gcp_billing_export dataset reachable, 2026-07-28).
-- Hand to whoever owns GCP billing / the audience_intent DAG (data-platform / finops). ~10 min.
--
-- Route 1 — GCP Billing BigQuery export (gives actual $, net of credits):
--   Point <billing_project>.<billing_dataset>.gcp_billing_export_v1_* at the real export, then:
SELECT
  DATE(usage_start_time)                                              AS day,
  sku.description                                                     AS sku,
  SUM(cost)                                                           AS gross_usd,
  SUM((SELECT IFNULL(SUM(c.amount),0) FROM UNNEST(credits) c))        AS credits_usd,   -- <-- a CUD/commit shows here
  SUM(cost) + SUM((SELECT IFNULL(SUM(c.amount),0) FROM UNNEST(credits) c)) AS net_usd
FROM `<billing_project>.<billing_dataset>.gcp_billing_export_v1_XXXXXX`
WHERE service.description = 'Cloud Dataproc'
  AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  AND EXISTS (SELECT 1 FROM UNNEST(labels) l
              WHERE l.key = 'goog-dataproc-batch-id' AND l.value LIKE 'aud-int-%')  -- the scoring batches
GROUP BY 1,2 ORDER BY day DESC, net_usd DESC;
-- Gate B PASSES if net_usd tracks gross (no committed-use discount / minimum-spend floor absorbing the cut).
-- Gate B FAILS (bill won't drop) if a large negative credits_usd shows a CUD covering these SKUs.
--
-- Route 2 — per-batch DCU-seconds (needs dataproc.batches.get IAM):
--   gcloud dataproc batches list  --project=<proj> --region=us-central1 --filter='batchId:aud-int-*'
--   gcloud dataproc batches describe <batch-id> --region=us-central1 \
--     --format='value(runtimeInfo.approximateUsage.milliDcuSeconds, runtimeInfo.approximateUsage.shuffleStorageGbSeconds)'
--   Convert: Standard $0.060/DCU-hr, Premium $0.089/DCU-hr; sum per-batch, compare gated vs current.
