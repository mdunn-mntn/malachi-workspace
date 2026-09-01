---
name: reference_astro_metrics_relay
description: DEV-8821 pod-metrics pipeline (Astro → Cloud Run relay → Grafana Alloy → Google Telemetry API/GMP) — FULLY LIVE 2026-09-01; the four-fix ladder, the receiver's label requirements, the probe/diagnosis gotchas, and the counter-read rule (relayed counters land under the /unknown descriptor variant and are invisible to PromQL — read them via the Monitoring v3 API); v3 read gotcha (2026-09-01): timeSeries.list returns points NEWEST FIRST and possibly sparse — subtract in timestamp order and divide by the span between point timestamps (airflow-ti PR 1259).
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [astro metrics relay, astro-metrics-relay, DEV-8821, prometheus remote-write, remote-write v1, grafana alloy, GMP, google managed prometheus, promql endpoint, container_ metrics, pod_profile, metrics exports, otel collector cloud run, monitoring.googleapis.com prometheus, serviceusage mntn-prj-prod-00, astro_metrics_relay keychain, name regex matcher unsupported, builtin metric names filter, gcp.project_id, cloud.region, 200 points batch cap, dropped_items, job instance labels, GFE 404 ingress internal-only, telemetry.googleapis.com, mntn-devops 5193, mntn-devops 5210, mntn-devops 5218, mntn-devops 5220, google incident feed, snappy protobuf probe, kube_pod_status_phase, metric descriptor unknown variant, no metric-type metadata, counter not promql queryable, container_cpu_usage_seconds_total, timeSeries.list v3 api, metricDescriptors.delete, breakglass-editor descriptor delete, staleness NaN NumberDataPoint, target_info duplicate timeseries, pod surface PR 1257, combined PR 1258, points newest first, timeSeries point order, sparse points rate divisor, PR 1259 pod rate fix, optimizer_pod report, dag-processor cpu, worker-default downsize]
domain: [infra]
lifecycle: active
last_verified: 2026-09-01
---
**The DEV-8821 pipeline is FULLY LIVE (verified 2026-09-01 20:10 UTC): zero drops,
`kube_pod_status_phase` 162 series, `container_memory_working_set_bytes` 35 series,
`container_cpu*` filling (counters readable ONLY via the v3 API — see the counter-read rule below).** It delivers the pod-metrics push previously recorded as impossible
(Astro's exporter can't OAuth to GMP directly; the relay is the OAuth hop).

- **Relay:** Cloud Run **`astro-metrics-relay`** in `mntn-prj-prod-00`. Remote-write URL
  `https://astro-metrics-relay-r64eabgqfq-uc.a.run.app/api/v1/write`, basic-auth user
  `astro-metrics`, password in Malachi's login Keychain under service **`astro_metrics_relay`**.
- **Astro side:** prod deployment Metrics Export with LABELS rows `job=astro-prod` /
  `instance=prod`. The Sunday (08-31) export had silently VANISHED — re-created 2026-09-01
  16:33 UTC. If samples stop, confirm the export still exists before debugging the relay.

## The four-fix ladder (each found from a live error after the previous fix deployed)
1. **mntn-devops PR 5193** — ingress INTERNAL_ONLY→ALL, plus engine swap OTel→**Grafana Alloy**:
   Astro sends Prometheus remote-write **v1** and the pinned OTel receiver was v2-only.
2. **PR 5210 (v0.2.1)** — stamps resource attribute **`gcp.project_id`** (the Telemetry API,
   telemetry.googleapis.com, requires it; `otelcol.auth.google` only authenticates, never
   stamps attributes).
3. **PR 5218 (v0.2.2)** — stamps **`cloud.region`** from the `GCP_LOCATION` env; missing it
   fails with "write for resource failed: Unrecognized region or location".
4. **PR 5220 (v0.2.3)** — caps Alloy batches at **200 points** ("A maximum of 200 points can be
   written in a single request", `dropped_items=368`); capping splits requests, loses nothing.

**Receiver label requirement:** BOTH `job` and `instance` labels on EVERY series, else 500
"job or instance cannot be found from labels" (probe matrix 2026-09-01: both=204, either alone
=500, neither=500). Stamped via the LABELS rows in the Astro Metrics Export UI, which applies
them to every exported series.

## Metric types: read relayed counters via the Monitoring v3 API, never PromQL (2026-09-01)
Astro's remote-write carries NO metric-type metadata, so every relayed metric lands under the
GMP descriptor variant `<name>/unknown`. Gauges under `/unknown` ARE PromQL-queryable
(`container_memory_working_set_bytes` proves it). A `_total`-suffixed counter under `/unknown`
is NOT: `container_cpu_usage_seconds_total` samples are visible via the v3 `timeSeries.list`
API while PromQL returns empty — and stayed empty 20+ minutes after the colliding empty
`/counter` descriptor was removed, so the collision was not the cause. **Rule: read relayed
counters via the Cloud Monitoring v3 API; PromQL only for gauges.**
- The stale empty `/counter` descriptor for cpu was DELETED 2026-09-01 via PAM
  `breakglass-editor` on `mntn-prj-prod-00` (`roles/writer` covers
  `monitoring.metricDescriptors.delete`); the `malachi_e2e_check` test descriptor was deleted in
  the same grant. Entitlement detail: memory `feedback_bq_workflow`.
- Benign relay noise, do not chase: staleness-NaN points rejected as "NumberDataPoint had an
  unrecognized or unset value" on pod churn, and `target_info` "Duplicate TimeSeries" warnings.

## Reading the v3 API: point order and sparse points (2026-09-01, airflow-ti PR 1259)
- **`timeSeries.list` returns points NEWEST FIRST.** The pod profiler's first prod run (sweep
  `manual__22:36`, first `optimizer_pod` report) read 0 cores everywhere because it computed the
  cpu rate as oldest-minus-newest — negative, filtered to 0, `exec_h` NULL. Subtract in
  TIMESTAMP order (or take the newest point for cumulative/limit values); never assume oldest
  first.
- **Points can be SPARSE.** Divide rates by the span between the actual point TIMESTAMPS, never
  by point count x scrape interval — sparse points otherwise inflate the rate.
- Fix PR #1259 verified live: dag-processor at 55% of its cpu limit; worker-default 0.875 cores
  = 11% of its 8-core limit (a real downsize candidate).

## Diagnosis gotchas (keep)
- **GFE generic 404 with ZERO Cloud Run request-log entries = ingress internal-only signature.**
  Auth-independent (valid and invalid credentials get the same 404); a 403 would mean invoker
  IAM, an app-level 404 would log a request.
- **`label/__name__/values` enumerates metric DESCRIPTORS and ignores start/end bounds** — a
  name listed there is NEVER proof of recent samples; confirm with `query_range` or the v3
  `timeSeries` API. Also: a `__name__` REGEX matcher is unsupported, and the endpoint returns
  ~18k built-in Google metric names (filter out names containing `:` or `/` to see the
  prometheus-ingested ones).
- **Hand-rolled PRW v1 probe:** minimal protobuf + pure-python snappy framing
  (`varint(len) + 0xf0 + len-1 + data`), header `Content-Encoding: snappy`, and `job` +
  `instance` labels or it 500s.
- **Relay logs ARE readable by Malachi's account** — the earlier `serviceusage` denial on
  `mntn-prj-prod-00` is gone (the old "no relay log reads" line was stale and is corrected here).
- **Google incident feed diagnosis pattern:** `https://status.cloud.google.com/incidents.json`.
  An open us-central1-b incident (2026-09-01) explained BOTH the transient INTERNAL 500s on
  forwards and the Cloud Run instance crashloops; check the feed before chasing our-side causes
  for transient drops.
- **GMP PromQL read endpoint:**
  `https://monitoring.googleapis.com/v1/projects/mntn-prj-prod-00/location/global/prometheus/api/v1/*`.

`airflow_optimizer/pod_profile.py` (requested-vs-used per task pod, ledger surface `"pod"`)
shipped via COMBINED airflow-ti PR 1258 (1257 closed as superseded) — MERGED + LIVE 2026-09-01
on image deploy-2026-09-01T22-22-40; mntn-devops PR 5224 merged (`roles/monitoring.viewer`
synced) and `OPTIMIZER_POD_PROJECT=mntn-prj-prod-00` set. Its first prod run exposed the v3
point-order bug above (fix PR 1259, verified live). See
[[project_airflow_optimizer]]; setup history in
`tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_astro_metrics_exporter_setup.md`;
deploy mechanics in [[reference_astro_deploy_mechanics]].
