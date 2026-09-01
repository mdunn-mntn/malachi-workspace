---
name: reference_astro_metrics_relay
description: DEV-8821 pod-metrics pipeline (Astro → Cloud Run relay → Grafana Alloy → Google Telemetry API/GMP) — FULLY LIVE 2026-09-01; the four-fix ladder, the receiver's label requirements, and the probe/diagnosis gotchas.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [astro metrics relay, astro-metrics-relay, DEV-8821, prometheus remote-write, remote-write v1, grafana alloy, GMP, google managed prometheus, promql endpoint, container_ metrics, pod_profile, metrics exports, otel collector cloud run, monitoring.googleapis.com prometheus, serviceusage mntn-prj-prod-00, astro_metrics_relay keychain, name regex matcher unsupported, builtin metric names filter, gcp.project_id, cloud.region, 200 points batch cap, dropped_items, job instance labels, GFE 404 ingress internal-only, telemetry.googleapis.com, mntn-devops 5193, mntn-devops 5210, mntn-devops 5218, mntn-devops 5220, google incident feed, snappy protobuf probe, kube_pod_status_phase]
domain: [infra]
lifecycle: active
last_verified: 2026-09-01
---
**The DEV-8821 pipeline is FULLY LIVE (verified 2026-09-01 20:10 UTC): zero drops,
`kube_pod_status_phase` 162 series, `container_memory_working_set_bytes` 35 series,
`container_cpu*` filling.** It delivers the pod-metrics push previously recorded as impossible
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

Next: `airflow_optimizer/pod_profile.py` reads requested-vs-used per task pod, ledger surface
`"pod"`. See [[project_airflow_optimizer]]; setup history in
`tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_astro_metrics_exporter_setup.md`;
deploy mechanics in [[reference_astro_deploy_mechanics]].
