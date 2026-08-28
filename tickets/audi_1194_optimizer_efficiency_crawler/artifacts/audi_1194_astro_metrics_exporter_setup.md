# Astro metrics exporter setup (unblocks the pod profiler)

Goal: pod-level CPU/memory metrics for the prod deployment land in Google Cloud Managed
Prometheus, where `pod_profile.py` can read requested-vs-used per task pod.

## CORRECTION 2026-08-28: the direct-push design below is superseded — a relay is required

Astro's Universal Metrics Exporter supports ONLY Prometheus remote-write with static basic/bearer
auth, and Google Managed Prometheus requires OAuth. Astro therefore cannot push to GMP directly.
The working design is a **relay: an OTel collector on Cloud Run** (service account with
`roles/monitoring.metricWriter`) that accepts Astro's remote-write and forwards to GMP with OAuth.

**Devops owns the relay: ticket DEV-8821** (DEV board, DevOps Request form, Request Type
Infrastructure Improvement), linked "Relates To" AUDI-1241.

## Steps (post-relay)

1. Devops stands up the OTel collector on Cloud Run per DEV-8821 (metricWriter SA, remote-write
   receiver, GMP exporter).
2. In the Astro UI: Deployment `prod` -> Details -> Advanced -> Universal Metrics Exporter, add a
   Prometheus destination pointing at the relay endpoint, with the static auth the relay expects.
3. Verify from the laptop: query `prometheus.googleapis.com/...` metric types in Metrics Explorer
   on `mntn-prj-prod-00`.

## Then (me)

`pod_profile.py`: query Managed Prometheus for per-pod peak/avg usage vs requests, per dag/task
labels Astro attaches, record under `surface="pod"` with core-hours as the unit; same ledger,
digest, dashboard path as bq/dbx. Own PR with tests once metrics are visible.

## Superseded original flow (kept for the record — WRONG, no OAuth path)

The 2026-08-28 (earlier) draft had Astro remote-write straight to
`https://monitoring.googleapis.com/v1/projects/mntn-prj-prod-00/location/global/prometheus/api/v1/write`
with a bearer token. GMP rejects static bearers; do not retry this route.
