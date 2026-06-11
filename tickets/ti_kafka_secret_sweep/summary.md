# Kafka Secret Sweep — ArgoCD (mntn-argocd) — Working Summary

**Jira:** _not yet created — assign a TI number and rename this folder (`ti_xxx_kafka_secret_sweep`) at grooming._
**Status:** Investigation complete. Targeting cutover pending a DevOps clarification.
**Owner:** Malachi Dunn (TI)
**Source request:** Mike Dolt → team, relaying Zach Schoenberger (#engineering, 2026-06-11): "go through ArgoCD and make sure all the Kafka services are updated." Part of the company-wide secrets-cleanup fire drill (Richard, 2026-06-09 — all eng work paused until plaintext secrets rotated/removed from source control).

---

## 1. Introduction / Problem

Kafka credentials live in each service's ArgoCD config (the shared Kafka lib `opm` does **not** hold them). The cleanup requires every Kafka-connecting service to move its credential off the **old SOPS-in-git pattern** onto the **new managed pattern** (Vault + External Secrets Operator, or Workload Identity), and to consume the **rotated** value. This ticket inventories every Kafka service in `SteelHouse/mntn-argocd` `apps-v3/`, classifies its secret-management state, and scopes the remaining work — with targeting as the primary deliverable.

## 2. The secret-management model (mntn-argocd)

```
service values.yaml → secretKeyRef {name, key} → K8s Secret, backed by ONE of:
  OLD ❌  SOPS in git    apps-v3/secrets/<ns>/<name>/...enc.yaml   (chart: managed-secrets) — static, not auto-rotated
  NEW ✅  Vault + ESO    apps-v3/external-secrets/<ns>/<name>/      (chart: external-secrets) — DevOps owns value, auto-rotated
  BEST ✅ Workload Identity — no secret at all
```

- SOP: `SteelHouse/mntn-devops/docs/standard_operating_procedures/052-secrets-management-strategy.md` (ranks: eliminate > Vault/ESO > SOPS-fallback).
- Engineers **cannot decrypt SOPS** (DevOps-only). Validate by behavior (ExternalSecret `Ready=True`, pod `Ready`, Kafka flowing), never by reading the value.
- Canonical "done" migration: identity PR **#42971** (ID-334) — deleted SOPS `secrets/qa-identity/id-service/`, added `external-secrets/qa-identity/kafka-confluent/` (`targetName: id-service`) pulling from Vault `teams/team-engineering-engineering/kafka-prod-rw`.
- `.enc.yaml` files leave **key names in plaintext** (only values encrypted) — safe to read which keys exist.

## 3. Targeting deep-dive (verified by hand)

**State: CUTOVER_PENDING (half-done migration).**

| | Consumed **today** | Already **created** (unused) |
|---|---|---|
| K8s secret | `confluent-cloud-secret` | `kafka-dev-rw` / `kafka-prod-rw` |
| Keys | `sasl_jaas_config`, `confluent_gcp_api_key`, `confluent_gcp_api_secret`, `qa_*` | `KEY`, `SECRET` |
| Backed by | **SOPS** `secrets/{qa,prod}-targeting/confluent-cloud-secret/` (sops-dev key) | **Vault/ESO** `external-secrets/targeting/kafka-{dev,prod}-rw/` → Vault `teams/team-engineering-engineering/kafka-{dev,prod}-rw` |

- One shared `confluent-cloud-secret` per namespace serves **~8 services**: membership-updates-aggregator, audience-consumer, clickpass-consumer, crm-integration-consumer, signals, shopper-graph, segmentation-journal, ip-vertical-classification.
- The Vault-backed ExternalSecrets exist but **nothing references them** — the cutover was never finished.
- The "add a sasl_jaas_config key" comment in `membership-updates-aggregator/values-qa.yaml` is **stale** — the key already exists in the SOPS secret; the service works today via SOPS. This is a *migration*, not a *fix*.

**The one real decision (JAAS reshaping):** ESO exposes raw `KEY`/`SECRET`; membership-updates-aggregator wants a full `sasl_jaas_config` JAAS string. Either (A) add an ESO `template` that builds `sasl_jaas_config` from Vault `KEY`/`SECRET` (no app change — preferred), or (B) change the app to consume `KEY`/`SECRET` (or `confluent_gcp_api_key`/`secret`) directly. The other 7 targeting services consume `confluent_gcp_api_key`/`secret` and are a straight repoint.

## 4. Cross-team inventory

Full report: [outputs/kafka_audit_report.md](outputs/kafka_audit_report.md) · raw per-team JSON: [outputs/kafka_audit_raw.json](outputs/kafka_audit_raw.json).

- **~39 Kafka services across 9 squads.** 1 fully migrated (attribution `mobile-event-consumer` — the ESO reference). 38 need work.
- **CUTOVER_PENDING (ESO exists, finish + delete SOPS):** targeting (8), most of attribution (7), pixel-validation-service.
- **NEEDS_MIGRATION_SOPS (build ESO from scratch):** bidder (17, all share `confluent-credentials` → **one** ESO fixes all), rplat (2, `confluent-jaas`), identity-prod (2, QA done), eventmap/rap (shared `analytics-eventmap`), performance-pacing idso-dco-master, creative-delivery CDS (`cds-secrets`, self-hosted SASL).
- **ROTATION_PENDING:** integrations `kwai` (live on Vault; delete orphaned SOPS + rotate).
- **NEEDS_REVIEW:** attribution `pixel-signals` (`signals` secret not locatable in-repo).
- **No Kafka:** creative-suite, data-platform, data-engineering, select (select=RabbitMQ).
- ⚠️ Cross-team rows are agent-derived — verify a squad's specifics (esp. inferred Vault paths like `teams/team-engineering-bidder/...`) with that squad + DevOps before acting.

## 5. Plaintext leaks (scrub + rotate)

Same Confluent cluster API key/username **`FYFR7DKPP2DLXQW5`** exposed in two values-file comments (password masked):
- `apps-v3/targeting/membership-updates-aggregator/values-qa.yaml`
- `apps-v3/attribution/mtag-aggregator/values-qa.yaml`

Shared key across targeting + attribution → coordinate rotation.

## 6. Open questions / clarifications for DevOps (Zach)

1. The targeting `kafka-dev-rw`/`kafka-prod-rw` ExternalSecrets expose `KEY`/`SECRET`, but services read SOPS `confluent-cloud-secret`. Is finishing that cutover the remaining work?
2. For `sasl_jaas_config` consumers (membership-updates-aggregator), should the ExternalSecret **template** the JAAS string from Vault KEY/SECRET (no app change), or should the app change to consume KEY/SECRET?
3. Confirm `teams/team-engineering-engineering/kafka-prod-rw` holds the rotated prod credential; what's the qa Vault entry for `crm-integration-consumer`'s `qa_*` keys?

## 7. Next steps

- [ ] Send §6 questions to Zach / DevOps; confirm reshape choice.
- [ ] Draft targeting cutover PR (qa first): repoint 8 services → ESO, template `sasl_jaas_config`, delete SOPS, scrub leaked key. Reviewed PR to branch-protected `mntn-argocd`.
- [ ] Validate (ESO `Ready`, pods healthy, Kafka flowing); roll qa → prod.
- [ ] Decide ownership of the cross-team sweep vs handing each squad its rows.
- [ ] Create Jira ticket; rename this folder with the TI number.

## 8. Method / provenance

Cross-team inventory produced by a 16-agent parallel sweep of `apps-v3/` (read-only; no repo changes). Targeting rows independently verified by hand against the live files. No SOPS values decrypted.
