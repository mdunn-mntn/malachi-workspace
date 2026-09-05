---
name: reference_gsutil_reauth_false_zero
description: "gsutil's expired-reauth failure is SILENT on stdout — ReauthUnattendedError goes to stderr while stdout gets an EMPTY listing, so `gsutil ls … 2>/dev/null | grep -c` reads a full GCS prefix as ZERO objects and corrupts any measurement built on it; fix is `gcloud auth login` in an interactive terminal, and use `gcloud storage` for listings and copies"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [gsutil false zero, ReauthUnattendedError, gsutil ls empty, reauth expired, gcloud auth login, silent empty listing, stderr swallowed, 2>/dev/null grep -c, false missing partition, gcs listing count wrong, zero objects wrong, measurement corruption, gcloud storage ls, gcloud storage cp, partition completeness check, AUDI-1321, AUDI-1191, stale gsutil processes, gsutil hang mac]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-09-05
---

**An expired gcloud reauth makes `gsutil ls` return an EMPTY listing on stdout while the error goes to
stderr.** The exit path is not what you would expect from a failed command: stdout is a clean, empty
result. So the common one-liner

    gsutil ls gs://bucket/prefix/ 2>/dev/null | grep -c .

reports **0 objects for a prefix that is full**, and every conclusion drawn from it ("the partition never
landed", "the backfill wrote nothing", "the day is missing") is an authentication artifact, not data.

**Why it matters more than a normal auth error:** it does not fail loudly, it fails as a NUMBER. A hang or a
non-zero exit gets noticed; a plausible zero gets written into a summary. Observed on this Mac 2026-08-28
(blocked savings-log verification, AUDI-1191) and again during AUDI-1321.

**How to apply:**
- **Fix:** `gcloud auth login` in an interactive terminal. Reauth cannot be refreshed non-interactively (the
  Astro/Airflow token can — the two auth systems are independent, see [[reference_airflow_log_puller]]).
- **Never discard stderr on a listing you intend to count.** Drop the `2>/dev/null`, or check the exit code,
  or assert a known-good sibling prefix returns non-zero in the same breath.
- **Prefer `gcloud storage ls` / `gcloud storage cp`** for both listings and copies; `gsutil` on this Mac is
  additionally pathologically slow on bulk work ([[reference_gcloud_storage_over_gsutil]]).
- **A zero that decides something must be confirmed by a second method** (a `gcloud storage du`, the
  producer's task log, or a receipt/row count) before it goes in a doc or a ticket.

[[reference_gcloud_storage_over_gsutil]] [[feedback_validated_is_not_correct]]
