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
  prometheus-ingested ones; Malachi LACKS `serviceusage` on `mntn-prj-prod-00`, so relay LOG
  reads fail while the monitoring query API works.
- **Verification pending:** no `container_*` series yet; Cristina is checking the relay logs.
  Once series land, `airflow_optimizer/pod_profile.py` reads requested-vs-used per task pod,
  ledger surface `"pod"`. See [[project_airflow_optimizer]]; setup history in
  `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_astro_metrics_exporter_setup.md`.
