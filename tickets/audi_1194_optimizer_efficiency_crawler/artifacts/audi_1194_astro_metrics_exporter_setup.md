# Astro metrics exporter setup (unblocks the pod profiler)

Goal: pod-level CPU/memory metrics for the prod deployment land in Google Cloud Managed
Prometheus, where `pod_profile.py` can read requested-vs-used per task pod.

## Steps (Malachi, ~15 min, Astro UI + one gcloud command)

1. In GCP: enable Managed Service for Prometheus on `mntn-prj-prod-00` (it is on by default in
   most projects; verify with `gcloud services list --enabled | grep monitoring`).
2. Create a service account key-less ingest path: Astro pushes remote-write, so create the
   endpoint URL: `https://monitoring.googleapis.com/v1/projects/mntn-prj-prod-00/location/global/prometheus/api/v1/write`.
3. In the Astro UI: Deployment `prod` -> Details -> Advanced -> Universal Metrics Exporter
   (docs: astronomer.io/docs/astro/export-metrics). Add a Prometheus destination with that URL.
4. Auth: choose Bearer token; mint one scoped to `monitoring.write` (ask in #devops if org policy
   blocks long-lived tokens; a GCP service account with `roles/monitoring.metricWriter` is the
   fallback, its token pasted as the bearer).
5. Save; Astro starts shipping StatsD + infrastructure metrics (pod CPU/memory) within minutes.
6. Verify from the laptop: `gcloud monitoring metrics list --project mntn-prj-prod-00 | grep -i airflow`
   (or query `prometheus.googleapis.com/...` metric types in Metrics Explorer).

## Then (me)

`pod_profile.py`: query Managed Prometheus for per-pod peak/avg usage vs requests, per dag/task
labels Astro attaches, record under `surface="pod"` with core-hours as the unit; same ledger,
digest, dashboard path as bq/dbx. Own PR with tests once metrics are visible.
