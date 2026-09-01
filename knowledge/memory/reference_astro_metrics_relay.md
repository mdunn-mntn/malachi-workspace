---
name: reference_astro_metrics_relay
description: DEV-8821 pod-metrics path — the Cloud Run astro-metrics-relay endpoint/auth, the Astro Metrics Exports config, and the GMP PromQL query-endpoint gotchas.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [astro metrics relay, astro-metrics-relay, DEV-8821, prometheus remote-write, GMP, google managed prometheus, promql endpoint, container_ metrics, pod_profile, metrics exports, otel collector cloud run, monitoring.googleapis.com prometheus, serviceusage mntn-prj-prod-00, astro_metrics_relay keychain, name regex matcher unsupported, builtin metric names filter]
domain: [infra]
lifecycle: active
last_verified: 2026-08-31
---
**The DEV-8821 relay is LIVE (2026-08-31)** — it DELIVERS the pod-metrics push previously recorded as impossible (Astro's exporter can't OAuth to GMP directly; the relay is the OAuth hop).

- **Relay:** Cloud Run **`astro-metrics-relay`** in `mntn-prj-prod-00`. Remote-write URL
  `https://astro-metrics-relay-r64eabgqfq-uc.a.run.app/api/v1/write`, basic-auth user
  `astro-metrics`, password in Malachi's login Keychain under service **`astro_metrics_relay`**.
- **Astro side:** prod deployment Metrics Exports configured ~19:45 UTC 2026-08-31.
- **GMP PromQL read endpoint:**
  `https://monitoring.googleapis.com/v1/projects/mntn-prj-prod-00/location/global/prometheus/api/v1/*`.
  Gotchas: a `__name__` REGEX matcher is unsupported; `label/__name__/values` returns ~18k
  built-in Google metric names — filter out names containing `:` or `/` to see the
  prometheus-ingested ones; `label/__name__/values` enumerates metric DESCRIPTORS and ignores
  start/end bounds, so a name listed there does NOT prove recent samples — confirm with
  `query_range` or the v3 `timeSeries` API. (Superseded 2026-08-31 20:04 UTC: the earlier
  serviceusage denial on `mntn-prj-prod-00` no longer blocks `gcloud logging read` — relay log
  reads work from Malachi's account now.)
- **BLOCKED 2026-08-31 20:06 UTC — relay unreachable from outside the VPC.** External POST to
  `/api/v1/write` returns the Google Front End's generic 404 with valid AND invalid basic auth,
  and the service has ZERO Cloud Run request-log entries ever: the signature of ingress set to
  internal-only (a 403 would mean invoker IAM; an app-level 404 would log a request). Astro's
  dedicated cluster runs outside MNTN's VPC, so its exporter gets the same 404 and no samples
  land. Service is Terraform-provisioned (`goog-terraform-provisioned`, created 18:18 UTC
  2026-08-31) — fix is Cristina/mountain-devops setting ingress to all (basic auth already
  gates the app). `container_*`/`kube_*` metric descriptors DO exist in the project with zero
  samples in 30d: consistent with an internal-side collector start, not with Astro delivery.
- **2026-09-01: ingress FIXED (mntn-devops PR 5193, Alloy v0.2.0), Astro POSTs arriving.**
  The Sunday Metrics Export had vanished - re-created 16:33 UTC and traffic landed in
  minutes. Remaining blocker verified on real traffic: the Telemetry API (OTLP,
  telemetry.googleapis.com) REQUIRES resource attribute `gcp.project_id`; config.alloy
  never sets it, so Alloy drops every batch (InvalidArgument, "Exporting failed. Dropping
  data."). `otelcol.auth.google{project=...}` only authenticates, it does not stamp the
  attribute. GMP-side smoke test: a hand-rolled PRW v1 protobuf (pure-python snappy: varint
  len + 0xf0 literal block) POSTs fine; the receiver also requires `job` and `instance`
  labels or it 500s — BOTH are required (probe matrix 2026-09-01: both=204, job-only=500,
  instance-only=500). Astro's exporter sends neither, so 200/200 real batches bounced;
  the fix is adding job + instance under LABELS in the Astro Metrics Export UI, which
  stamps them on every exported series.
- Once series land, `airflow_optimizer/pod_profile.py` reads requested-vs-used per task pod,
  ledger surface `"pod"`. See [[project_airflow_optimizer]]; setup history in
  `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_astro_metrics_exporter_setup.md`.
