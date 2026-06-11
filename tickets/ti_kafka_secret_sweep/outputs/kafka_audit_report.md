# Kafka Secret-Management Audit — SteelHouse/mntn-argocd

## 1. Executive Summary

The audit covers **39 Kafka services** across 9 squads (4 squads — creative-suite, data-platform, data-engineering, select — have no Kafka workloads). Of these, **only 1 is fully migrated** (attribution's `mobile-event-consumer`, the clean ESO/Vault reference pattern); the remaining **38 need work**. The dominant pattern splits two ways: **17 services are CUTOVER_PENDING** (the half-done state — ESO/Vault is already provisioned but the service still consumes the legacy SOPS secret, so the cutover just needs finishing), and **20 are NEEDS_MIGRATION_SOPS** (no ESO exists at all — a from-scratch ExternalSecret build). One service (`pixel-signals`) is NEEDS_REVIEW (backing store not locatable in-repo). The work clusters into a few shared-secret migrations that each cut over many services at once: bidder's `confluent-credentials` (17 services), targeting's `confluent-cloud-secret` (8), and rplat's `confluent-jaas` (2). Two plaintext leaks of the same Confluent API key/username (`FYFR7DKPP2DLXQW5`) were found in code comments and must be scrubbed and rotated.

## 2. Master Inventory

| Squad | Service | Namespace | Consumes secret (name:key) | Backing | Status | Action |
|---|---|---|---|---|---|---|
| targeting | audience-consumer | qa-targeting, prod-targeting | confluent-cloud-secret:confluent_gcp_api_key/secret | SOPS | CUTOVER_PENDING | Repoint secretKeyRef to ESO kafka-{dev,prod}-rw (KEY/SECRET); delete SOPS |
| targeting | clickpass-consumer | qa-targeting, prod-targeting | confluent-cloud-secret:confluent_gcp_api_key/secret | SOPS | CUTOVER_PENDING | Repoint to ESO kafka-{dev,prod}-rw; retire SOPS |
| targeting | signals | qa-targeting, prod-targeting | confluent-cloud-secret:confluent_gcp_api_key/secret | SOPS | CUTOVER_PENDING | Repoint CONFLUENT_CLOUD_CLUSTER_API_KEY/SECRET to ESO; retire SOPS |
| targeting | shopper-graph | qa-targeting, prod-targeting | confluent-cloud-secret:confluent_gcp_api_key/secret | SOPS | CUTOVER_PENDING | Repoint API key/secret to ESO; bootstrap host can stay |
| targeting | ip-vertical-classification | qa-targeting, prod-targeting | confluent-cloud-secret:confluent_gcp_api_key/secret | SOPS | CUTOVER_PENDING | Repoint to ESO; retire SOPS |
| targeting | segmentation-journal | qa-targeting, prod-targeting | confluent-cloud-secret:confluent_gcp_api_key/secret | SOPS | CUTOVER_PENDING | Repoint in values-{qa,prod}-ip.yaml to ESO; retire SOPS |
| targeting | crm-integration-consumer | qa-targeting, prod-targeting | confluent-cloud-secret:confluent_gcp_api_key/secret + qa_* | SOPS | CUTOVER_PENDING | Repoint all 4 refs to ESO; verify qa Vault entry holds QA creds |
| targeting | membership-updates-aggregator | qa-targeting, prod-targeting | confluent-cloud-secret:sasl_jaas_config | SOPS | CUTOVER_PENDING | Scrub+rotate leaked key; derive JAAS from KEY/SECRET or add JAAS to Vault; repoint |
| attribution | mtag-aggregator | qa-pixel, prod-pixel | confluent-cloud-secret:sasl_jaas_config | SOPS | CUTOVER_PENDING | Repoint to kafka-confluent ESO; scrub leaked username; fix dangling ref |
| attribution | impression-tag-service | qa-attribution, prod-attribution | confluent-jaas:jaas (+ kafka-confluent prod) | SOPS | CUTOVER_PENDING | Build JAAS from kafka-confluent user/pass; drop confluent-jaas; delete SOPS |
| attribution | attribution-consumer | qa-attribution, prod-attribution | confluent-jaas:jaas | SOPS | CUTOVER_PENDING | Migrate to kafka-confluent ESO (derive JAAS); delete SOPS |
| attribution | cookie-sync-service | qa-vvs, prod-vvs | confluent-jaas:JAAS_CONFIG (qa) / kafka-confluent (prod) | SOPS | CUTOVER_PENDING | Repoint qa to ESO kafka-confluent to match prod |
| attribution | attr-ingestor | qa-vvs, prod-vvs | confluent-jaas:JAAS_CONFIG | SOPS | CUTOVER_PENDING | Migrate to kafka-confluent ESO; coordinate shared SOPS deletion |
| attribution | verified-visits | qa-vvs, prod-vvs | confluent-jaas:JAAS_CONFIG | SOPS | CUTOVER_PENDING | Migrate to kafka-confluent ESO; retire shared SOPS after peers move |
| attribution | trpx | qa-pixel, prod-pixel | kafka-confluent (qa) / confluent:JAAS (prod) | SOPS | CUTOVER_PENDING | Repoint prod to ESO kafka-confluent; delete secrets/qa-pixel/confluent |
| pixel | pixel-validation-service | pixel (qa-pixel/prod-pixel ESO) | confluent-cloud-secret:confluent_cloud_cluster_api_key/secret | SOPS | CUTOVER_PENDING | Move workload to qa-/prod-pixel, repoint to kafka-confluent, delete SOPS, rotate |
| attribution | pixel-signals | qa-pixel, prod-pixel | signals:KAFKA_SASL_USERNAME/PASSWORD(_PROD) | NONE_FOUND | NEEDS_REVIEW | Locate where 'signals' secret is provisioned; migrate SASL keys onto kafka-confluent ESO |
| bidder | win-aggregator-logging | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Build one ESO (targetName confluent-credentials) for the namespace — cuts over all 17 |
| bidder | win-aggregator-logging-expiring | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared confluent-credentials → ESO cutover |
| bidder | win-aggregator-logging-noexpire | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | win-aggregator-frequency | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | win-aggregator-frequency-expiring | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | win-aggregator-frequency-noexpire | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | win-aggregator-spend-expiring | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | win-aggregator-spend-noexpire | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | cdc-win-aggregator | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | impression-consumer-service | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | membership-consumer | bidder | confluent-credentials:username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | membership-consumer-oracle | bidder | confluent-credentials:username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | membership-consumer-recency | bidder | confluent-credentials:username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | rtb-membership-consumer-service | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | rtb-recency-consumer-service | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | notification-v2 | bidder | confluent-credentials:bootstrap/username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| bidder | beeswax-audience-consumer | bidder | confluent-credentials:username/password | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared cutover |
| rplat | sh-kafka-connect | connectors-qa, connectors-prod | confluent-jaas:jaas | SOPS | NEEDS_MIGRATION_SOPS | Build ESO (targetName confluent-jaas) per namespace; delete SOPS; rotate |
| rplat | schema-registry | connectors-qa, connectors-prod | confluent-jaas:jaas | SOPS | NEEDS_MIGRATION_SOPS | Covered by shared confluent-jaas → ESO migration |
| identity | id-service | qa-identity, prod-identity | id-service:confluent_gcp_api_key/secret | SOPS | NEEDS_MIGRATION_SOPS | QA done; create prod-identity ESO (targetName id-service), delete prod SOPS, rotate |
| identity | household-enrichment-consumer | qa-identity, prod-identity | id-service:confluent_gcp_api_key/secret | SOPS | NEEDS_MIGRATION_SOPS | Auto-covered once prod id-service ESO exists (shares same secret) |
| eventmap | eventmap (filter-fanout + api-service) | analytics-eventmap-dev, analytics-eventmap | kafka-confluent-secret:username/password/bootstrap-servers + confluent-jaas:jaas | SOPS | NEEDS_MIGRATION_SOPS | Build ESO for both secrets; delete SOPS; optionally derive JAAS from user/pass |
| rap | analytics-eventmap | analytics-eventmap-dev, analytics-eventmap | kafka-confluent-secret:username/password/bootstrap-servers + confluent-jaas:jaas | SOPS | NEEDS_MIGRATION_SOPS | Same as eventmap entry — build ESO, delete SOPS, rotate |
| performance-pacing | idso-dco-master | prod-optimization, burnin-optimization | kafka-auth-config:config | SOPS | NEEDS_MIGRATION_SOPS | Build ESO (targetName kafka-auth-config, key config) per namespace; delete SOPS |
| creative-delivery | ads-fcap-service | qa-cds, prod-cds | cds-secrets-{env}:KAFKA_ADDRESS/USERNAME/PASSWORD | SOPS | NEEDS_MIGRATION_SOPS | From-scratch ESO build for cds namespaces; repoint refs; self-hosted SASL |
| creative-delivery | facade | qa-cds, prod-cds | cds-secrets-{env}:KAFKA_ADDRESS/USERNAME/PASSWORD (envFrom) | SOPS | NEEDS_MIGRATION_SOPS | Same ESO build; picks up via envFrom; validate Spring env names |
| creative-delivery | cds-secrets shared bundle (~15 envFrom consumers) | qa-cds, prod-cds | cds-secrets-{env}:KAFKA_ADDRESS/USERNAME/PASSWORD | SOPS | NEEDS_MIGRATION_SOPS | Split Kafka into own ESO secret to shrink blast radius; confirm real consumers |
| attribution | mobile-event-consumer | qa-pixel, prod-pixel | kafka-confluent:username/password/bootstrap-servers | ESO_VAULT | **MIGRATED_DONE** | None — reference pattern |
| integrations | kwai | integrations | Vault secret/data/integrations/kwai:KAFKA_SASL_USERNAME/PASSWORD | ESO_VAULT (vault-webhook) | ROTATION_PENDING | Delete orphaned SOPS secrets/integrations/kwai/; rotate Confluent SASL key |

## 3. Priority Actions

### CUTOVER_PENDING (ESO already provisioned — finish the cutover, then delete SOPS)

**Targeting** — `confluent-cloud-secret` is live (managed-secrets SOPS) while `kafka-dev-rw`/`kafka-prod-rw` ESO secrets already exist in both namespaces but are unreferenced. Re-point each service's secretKeyRef to the ESO secret (keys `KEY`/`SECRET`), then delete the SOPS dirs once no consumer remains. **Key gotcha:** SOPS keys (`confluent_gcp_api_key/secret`) ≠ ESO keys (`KEY`/`SECRET`), so every ref must be rewritten, not just renamed.
- `audience-consumer`, `clickpass-consumer`, `signals`, `shopper-graph`, `ip-vertical-classification`, `segmentation-journal` — straight repoint to ESO.
- `crm-integration-consumer` — repoint all 4 refs; verify the qa Vault entry (`kafka-dev-rw`) holds correct QA cluster creds (qa currently uses `qa_confluent_gcp_api_key/secret`).
- `membership-updates-aggregator` — two-part: (1) scrub+rotate leaked key `FYFR7DKPP2DLXQW5`; (2) ESO only exposes `KEY`/`SECRET`, not a JAAS string, so either add a JAAS property to Vault or have the app assemble JAAS from `KEY`/`SECRET`.

**Attribution** — `kafka-confluent` ESO secret (Vault `groups/{nonprod,prod}/shared/kafka-confluent`) is provisioned in all six namespaces but most services still read legacy SOPS. Retire shared SOPS secrets only after all consumers in each namespace repoint.
- `mtag-aggregator` — repoint to `kafka-confluent`; fix dangling `confluent-cloud-secret` ref (doesn't exist in qa-/prod-pixel); scrub leaked username.
- `impression-tag-service`, `attribution-consumer` — build JAAS from `kafka-confluent` user/pass, drop `confluent-jaas`, delete `secrets/qa-attribution/confluent-jaas`.
- `cookie-sync-service`, `trpx` — env-split (one env already on ESO); repoint the lagging env (cookie-sync qa, trpx prod), then delete the qa SOPS secret.
- `attr-ingestor`, `verified-visits` — migrate to `kafka-confluent`; coordinate shared `secrets/qa-vvs/confluent-jaas` deletion (shared with cookie-sync-qa).

**Pixel** — `pixel-validation-service` (Deployment + nightly CronJob, qa+prod): three mismatches block cutover — namespace (`pixel` vs `qa-/prod-pixel`), secret name (`confluent-cloud-secret` vs `kafka-confluent`), and key names. Move workload into `qa-/prod-pixel` (or add a pixel-namespace ESO with `targetName=confluent-cloud-secret`), repoint the 4 values files, delete `secrets/pixel/confluent-cloud-secret/`, rotate the static API key. Mirror identity PR ID-334.

### NEEDS_MIGRATION_SOPS (no ESO exists — build it from scratch, then delete SOPS)

- **Bidder (17 services)** — single shared `confluent-credentials` secret in bare `bidder` namespace. Add **one** ExternalSecret (`targetName: confluent-credentials`, keys bootstrap/username/password from Vault `teams/team-engineering-bidder/kafka-{dev,prod}-rw`); all 17 cut over with zero per-service changes. Then delete `secrets/bidder/confluent-credentials/`. **Cleanup:** also delete the orphaned, unreferenced SOPS secret `secrets/bidder/kafka-username-password/` (zero consumers).
- **rplat (2 services)** — `sh-kafka-connect` + `schema-registry` share `confluent-jaas:jaas` in `connectors-qa`/`connectors-prod`. One ESO per namespace (`targetName: confluent-jaas`) resolves both; delete SOPS; rotate (static since 2025-08-27).
- **identity (2 services)** — QA is done (ID-334 template). Create `external-secrets/prod-identity/kafka-confluent/` (`targetName: id-service`) pointed at the correct **prod** Vault RW key, delete `secrets/prod-identity/id-service/`, rotate. `household-enrichment-consumer` auto-migrates (same secret name).
- **eventmap / rap (same workload, double-registered)** — `analytics-eventmap` consumes `kafka-confluent-secret` + `confluent-jaas` (SOPS under `secrets/rap/`). Build ESO for both `analytics-eventmap-dev`/`analytics-eventmap`; keep secret names identical (no values change); optionally collapse `confluent-jaas` by deriving JAAS in-chart; delete the two SOPS dirs; rotate (static since 2025-10-21).
- **performance-pacing (1 service)** — `idso-dco-master` uses Spring Kafka `kafka-auth-config:config` (single JAAS key) in `prod-optimization`/`burnin-optimization`. Create ESO per namespace (`targetName: kafka-auth-config`, key `config`); delete SOPS.
- **creative-delivery (CDS)** — self-hosted SASL Kafka (`KAFKA_ADDRESS/USERNAME/PASSWORD`) bundled in shared `cds-secrets-{env}` (no ESO exists for cds namespaces). Stand up ESO from scratch, **ideally splitting Kafka into its own secret** to shrink the 17-service envFrom blast radius. Migrate explicit consumers `ads-fcap-service` + `facade` first; confirm with the squad which of the ~15 envFrom-only services actually open a Kafka client (`ad-service` likely — ships a Kafka Loki dashboard); then strip `KAFKA_*` from the SOPS bundle.

### Other

- **integrations `kwai` (ROTATION_PENDING)** — live consumption is already on Vault (vault-secrets-webhook), but the orphaned SOPS secret `secrets/integrations/kwai/` was never deleted and still ships a static, un-rotated Confluent SASL credential. Delete the SOPS dir (per ID-334 done pattern) and rotate the Vault Confluent key.
- **attribution `pixel-signals` (NEEDS_REVIEW)** — real Kafka producer pulling SASL creds from a static K8s secret `signals` with no SOPS dir and no ESO anywhere in-repo. Locate where `signals` is provisioned, then migrate its Kafka SASL keys onto the already-provisioned `kafka-confluent` ESO.

## 4. Plaintext Leaks Found

Two occurrences of the **same** Confluent cluster API key / SASL username `FYFR7DKPP2DLXQW5` sit in plaintext code comments (password masked, but the key ID is exposed). Scrub from Git and rotate the key.

| File | Exposed |
|---|---|
| `apps-v3/targeting/membership-updates-aggregator/values-qa.yaml` | Confluent API key/username `FYFR7DKPP2DLXQW5` in a sample JAAS string comment (password masked) |
| `apps-v3/attribution/mtag-aggregator/values-qa.yaml` | Confluent SASL username `FYFR7DKPP2DLXQW5` in a JAAS PlainLoginModule comment (password masked) |

No other plaintext Kafka credentials found. (Note: non-Kafka plaintext keys were flagged out-of-scope in creative-suite and data-engineering — e.g. a hardcoded Neo4j `neo4j/neo4j123` in `data-eng-ai/values-qa.yaml`, plus publishable client-side keys — but none are Kafka credentials.)

## 5. Squads With No Kafka

**creative-suite, data-platform, data-engineering, and select** have zero Kafka services (select uses RabbitMQ; data-engineering's "confluence" reference is Atlassian Confluence, not Confluent).