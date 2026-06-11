# Kafka Secret Migrations — Tracking Page

**Purpose:** identify every Kafka credential in `SteelHouse/mntn-argocd` still on the **old way (SOPS)** that needs to move to the **correct way (Vault + External Secrets / ESO)**. This page *identifies* the work — it does **not** make the changes. Each owning squad makes its own change.

**Source of truth:** authoritative file lists pulled from the `apps-v3/secrets/` (SOPS) and `apps-v3/external-secrets/` (ESO) trees on `main`, cross-referenced. Consumer counts/squads from the service audit ([../outputs/kafka_audit_report.md](../outputs/kafka_audit_report.md)).

### Canonical example (per Alex Knorr)
- **Old way (SOPS):** [`apps-v3/secrets/bidder/confluent-credentials/confluent-credentials-secrets-prod.enc.yaml`](https://github.com/SteelHouse/mntn-argocd/blob/main/apps-v3/secrets/bidder/confluent-credentials/confluent-credentials-secrets-prod.enc.yaml)
- **Correct way (ESO):** [`apps-v3/external-secrets/targeting/kafka-prod-rw/kafka-prod-rw-dev.yaml`](https://github.com/SteelHouse/mntn-argocd/blob/main/apps-v3/external-secrets/targeting/kafka-prod-rw/kafka-prod-rw-dev.yaml)

### Status legend
- **🔴 BUILD ESO** — no ESO exists yet for this secret. Create the ExternalSecret (point at the Vault `teams/.../kafka-*-rw` key), repoint consumers, then delete the SOPS file.
- **🟡 CUTOVER** — the ESO already exists; just repoint the consuming services off the SOPS secret, then delete the SOPS file.
- **🟢 DONE** — already on ESO, no SOPS remaining.
- **⚪ IGNORE** — out of scope (POC / orphaned-unreferenced).

---

## Summary

| Status | Kafka secrets | Notes |
|---|---:|---|
| 🔴 BUILD ESO | 11 | bidder (1 → covers 17 svcs), rplat (2), eventmap/rap (2), perf-pacing (2), CDS (3), identity-prod (1) |
| 🟡 CUTOVER | 6 | targeting (2), attribution-qa (2), pixel (2) — ESO already provisioned |
| 🟢 DONE | 6 | identity-qa, attribution-prod, vvs-prod, pixel-prod, mobile-event-consumer, pixel-signals |
| ⚪ IGNORE | 2 | memdb-rapid (POC, per Zach), bidder/kafka-username-password (orphan) |

**17 SOPS Kafka secrets need action** (11 build + 6 cutover). The biggest single win: **bidder** — one ESO covers ~17 services.

---

## 🔴 BUILD ESO (no ESO exists yet)

| Squad | Old way — SOPS file (delete after) | Namespace | Consumers | Correct way — build this ESO |
|---|---|---|---|---|
| **bidder** | [`secrets/bidder/confluent-credentials/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/bidder/confluent-credentials) (dev+prod) | `bidder` | ~17 svcs | new `external-secrets/bidder/...` → Vault `teams/team-engineering-bidder/kafka-{dev,prod}-rw`; `targetName: confluent-credentials` (keys bootstrap/username/password) — **one ESO covers all 17** |
| **rplat** | [`secrets/connectors-qa/confluent-jaas/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/connectors-qa/confluent-jaas) | `connectors-qa` | sh-kafka-connect, schema-registry | new `external-secrets/connectors-qa/...` (`targetName: confluent-jaas`) |
| **rplat** | [`secrets/connectors-prod/confluent-jaas/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/connectors-prod/confluent-jaas) | `connectors-prod` | sh-kafka-connect, schema-registry | new `external-secrets/connectors-prod/...` |
| **eventmap/rap** | [`secrets/rap/confluent-jaas/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/rap/confluent-jaas) (dev+prod) | `rap`* | analytics-eventmap | new ESO (`targetName: confluent-jaas`) |
| **eventmap/rap** | [`secrets/rap/kafka-confluent-secret/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/rap/kafka-confluent-secret) (dev+prod) | `rap`* | analytics-eventmap | new ESO (`targetName: kafka-confluent-secret`, keys username/password/bootstrap-servers) |
| **perf-pacing** | [`secrets/prod-optimization/kafka-auth-config/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/prod-optimization/kafka-auth-config) | `prod-optimization` | idso-dco-master | new ESO (`targetName: kafka-auth-config`, key `config`) |
| **perf-pacing** | [`secrets/burnin-optimization/kafka-auth-config/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/burnin-optimization/kafka-auth-config) | `burnin-optimization` | idso-dco-master | new ESO (`targetName: kafka-auth-config`) |
| **CDS** | [`secrets/dev-cds/cds-secrets-dev/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/dev-cds/cds-secrets-dev) | `dev-cds` | ads-fcap-service, facade (+~15 envFrom) | new ESO — **split `KAFKA_*` out of the shared bundle** to shrink blast radius |
| **CDS** | [`secrets/qa-cds/cds-secrets-qa/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/qa-cds/cds-secrets-qa) | `qa-cds` | CDS services | new ESO (split Kafka keys out) |
| **CDS** | [`secrets/prod-cds/cds-secrets-prod/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/prod-cds/cds-secrets-prod) | `prod-cds` | CDS services | new ESO (split Kafka keys out) |
| **identity** | [`secrets/prod-identity/id-service/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/prod-identity/id-service) | `prod-identity` | id-service, household-enrichment-consumer | new `external-secrets/prod-identity/kafka-confluent/` (`targetName: id-service`) — mirror the merged QA PR (ID-334) |

\* `rap` secrets: namespace per `config.yaml` (services run in `analytics-eventmap*`) — confirm the namespace field before acting.

## 🟡 CUTOVER (ESO already exists — repoint consumers, then delete SOPS)

| Squad | Old way — SOPS file (delete after) | Namespace | Consumers | Correct way — ESO that exists |
|---|---|---|---|---|
| **targeting** | [`secrets/qa-targeting/confluent-cloud-secret/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/qa-targeting/confluent-cloud-secret) | `qa-targeting` | 8 svcs | [`external-secrets/targeting/kafka-dev-rw`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/external-secrets/targeting/kafka-dev-rw) + [`kafka-prod-rw`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/external-secrets/targeting/kafka-prod-rw) |
| **targeting** | [`secrets/prod-targeting/confluent-cloud-secret/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/prod-targeting/confluent-cloud-secret) | `prod-targeting` | 8 svcs | [`external-secrets/targeting/kafka-prod-rw`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/external-secrets/targeting/kafka-prod-rw) |
| **attribution** | [`secrets/qa-attribution/confluent-jaas/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/qa-attribution/confluent-jaas) | `qa-attribution` | impression-tag-service, attribution-consumer | [`external-secrets/qa-attribution/kafka-confluent`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/external-secrets/qa-attribution/kafka-confluent) (prod already done) |
| **attribution (vvs)** | [`secrets/qa-vvs/confluent-jaas/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/qa-vvs/confluent-jaas) | `qa-vvs` | cookie-sync, attr-ingestor, verified-visits | [`external-secrets/qa-vvs/kafka-confluent`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/external-secrets/qa-vvs/kafka-confluent) (prod already done) |
| **pixel** | [`secrets/qa-pixel/confluent/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/qa-pixel/confluent) | `qa-pixel` | trpx & peers | [`external-secrets/qa-pixel/kafka-confluent`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/external-secrets/qa-pixel/kafka-confluent) (prod already done) |
| **pixel** | [`secrets/pixel/confluent-cloud-secret/`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/secrets/pixel/confluent-cloud-secret) (dev+prod) | `pixel` (bare) | pixel-validation-service | [`external-secrets/{qa,prod}-pixel/kafka-confluent`](https://github.com/SteelHouse/mntn-argocd/tree/main/apps-v3/external-secrets/prod-pixel/kafka-confluent) — ⚠ namespace + secret-name + key mismatch; move workload to qa-/prod-pixel |

## 🟢 DONE (already migrated)

- `external-secrets/qa-identity/kafka-confluent` (QA id-service — SOPS removed, PR ID-334)
- `external-secrets/prod-attribution/kafka-confluent` · `prod-vvs/kafka-confluent` · `prod-pixel/kafka-confluent`
- `external-secrets/attribution/mobile-event-consumer` (the clean reference)
- `external-secrets/attribution/pixel-signals` (ESO exists — verify `targetName`)

## ⚪ IGNORE

- `secrets/memdb-rapid/kafka-username-password/` — POC, per Zach (2026-06-11).
- `secrets/bidder/kafka-username-password/` — orphaned, no consumers found → candidate for **deletion**, not migration.

---

## JAAS-string gotcha (affects how cutover is done, not whether)

JVM Kafka apps need a single `sasl.jaas.config` string; the Java client has no separate username/password property. librdkafka apps (e.g. `membership-etl`) take username/password natively and map straight onto ESO `KEY`/`SECRET`. So for **JVM** consumers (e.g. targeting `membership-updates-aggregator`, attribution `confluent-jaas` consumers) the owning squad must either (A) build the JAAS string in-app from injected username/password, or (B) template a `sasl_jaas_config` key in the ExternalSecret. Flag this per-service when assigning.

## Coverage notes / residual risk

- SOPS + ESO file lists are exhaustive from the repo tree (verified, not sampled).
- Caught non-obvious names (`id-service`, `pixel-signals`, `cds-secrets`, `kafka-auth-config`) via the service audit, not just keyword match.
- **Residual risk:** a Kafka credential bundled inside a generic `*-secrets` envFrom bundle under a name with no Kafka/Confluent keyword (CDS `cds-secrets` is the known case) could exist in another squad. Low likelihood given the consumer-side audit swept all squads for `KAFKA_*` env refs.
