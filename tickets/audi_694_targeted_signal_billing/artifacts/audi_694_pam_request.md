# PAM request — enriched_impressions read (AUDI-694)

**Target:** `mntn-analytics-prod-01.analytics_curated.enriched_impressions`
**Entitlement:** `bq-read` (roles/bigquery.dataViewer + jobUser + connectionUser + storage.objectViewer)
**Max duration:** 14400s (4h). **Approver:** DevOps. All grants logged, auto-revoked at expiry.

Discovered 2026-08-17 via:
`gcloud pam entitlements search --caller-access-type=grant-requester --project=mntn-analytics-prod-01 --location=global`

## Command

```bash
gcloud pam grants create \
  --entitlement=bq-read \
  --project=mntn-analytics-prod-01 \
  --location=global \
  --requested-duration=14400s \
  --justification="AUDI-694: read analytics_curated.enriched_impressions to size DDP vendor-crediting exposure as CRM inclusion audiences migrate DS4 to DS63. Read-only, no writes. Queries are partition-filtered to a single billing month and data_source_id in (4,47,63)."
```

## What it unblocks

Four questions that cannot be answered from `dw-main-gold.reporting` alone, because the gold `ddp_*`
tables only contain impressions the pipeline *already* matched:

1. **DS63 volume inside the billing scope.** How many `data_source_id = 63` impressions fall inside
   `channel_id = 8 AND funnel_level = 1 AND objective_id = 1`, by month. The gold tables cannot show
   what the scope filter excluded.
2. **Migration split.** DS63-only vs dual-run (DS4 and DS63 on the same `ad_served_id`). Dual-run
   matters because the winner rank partitions by `data_source_id`, so one CRM audience can win two
   slots and dilute every other vendor.
3. **The DS47 negative.** Whether DS47 ever produces a `category_info` entry. Evidence says no
   (exclusion-only, `crm_exclusion_data_source`), but confirming it is the answer to the ticket as
   literally written.
4. **The $0-filler counterfactual.** Re-run the winner logic over `ddp_all_matches_cpm_202607` with
   DS63 synthesized at cpm 0, and diff `ddp_usage_report_ds*` against the shipped
   `mt_temp_ddp_reports_2026_07`. Needs the impression population to synthesize the missing rows.

## Not blocking

The headline divisor finding (4.7x MM parity, 259x preemption) was measured without this access, off
`dw-main-gold.reporting.ddp_crm_graph_cpm`. See summary.md §4.7.

## Prior art

- 4h windows are the known constraint: IMP-011 records a `sqlmesh plan` failing because it ran >24h
  against 8h PAM windows. Scope each session to one question.
- AUDI-1170 used ~4 grant windows for a backfill (`dataproc-runtime-actas` + `dataproc-submit`).

## Submitted

- **Grant ID:** `5d0f053c-4d4e-4e5b-8c94-c78ad1e66fef`
- **Requested:** 2026-08-17 22:54:12 UTC by malachi@mountain.com
- **State:** `APPROVAL_AWAITED` (DevOps). Request itself expires 2026-08-18 22:54 UTC if not actioned.
- **Duration once approved:** 14400s (4h) from approval.
- Check: `gcloud pam grants describe 5d0f053c-4d4e-4e5b-8c94-c78ad1e66fef --entitlement=bq-read --project=mntn-analytics-prod-01 --location=global --format="value(state)"`
