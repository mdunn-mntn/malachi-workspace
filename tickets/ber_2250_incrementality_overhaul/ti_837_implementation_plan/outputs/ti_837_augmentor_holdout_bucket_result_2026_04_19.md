# TI-837 — augmentor_log holdout bucket verification result

**Query:** `queries/ti_837_augmentor_holdout_bucket_verification.sql`
**Run date:** 2026-04-20
**Source:** `dw-main-bronze.raw.augmentor_log`, one hour on 2026-04-19 00:00–01:00 UTC
**Advertiser:** 31357 (WGU)
**Scan:** ~22 GB (one hourly partition)

## Result

| in_holdout_bucket | n_rows        | unique_ips |
|-------------------|---------------|------------|
| false             | 1,246,877,127 | 16,465,297 |
| true              |   114,582,413 |  1,826,814 |

- **Unique IPs in holdout bucket: 1,826,814 / 18,292,111 = 10.0%** — exactly the
  uniform expectation for a 10% holdout. Confirms augmentor_log is advertiser-agnostic
  and IP-complete.
- **Rows in holdout bucket: 114,582,413 / 1,361,459,540 = 8.4%** — slightly below 10%.
  Suggests holdout IPs may have slightly fewer augmentor events per IP than non-holdout
  IPs (frequency-cap or re-augmentation difference). Does not block the methodology
  but worth a follow-up with Ryan/Zach.

## Interpretation

Alex Knorr's read is correct; Ryan Kleck's is wrong. The ghost bidding pipeline can
be built using existing `augmentor_log` (for pseudo-exposure) and `cost_impression_log`
(for actual exposure) without an ETL change from Zach/Jordan or a bidder-side change
from Kevaughn. The Apr 22 multi-party meeting becomes a walkthrough/alignment session
rather than a scoping session.
