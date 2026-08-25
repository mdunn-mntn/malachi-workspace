# Reducing segment write load and moving the bidder to Kafka

*Bidder & membership-db · proposal*

| | |
|---|---|
| **Status** | investigation complete, proposal for review |
| **Owner** | esalinger |
| **Audience** | bidder team, membership-db team; FYI to Data Platform |

## Contents

1. [Summary](#summary)
2. [Two findings that shaped this](#two-findings-that-shaped-this)
3. [The problem](#the-problem)
4. [Proposal](#proposal)
5. [Change detection: digest in the ETL sidecar](#change-detection-digest-in-the-etl-sidecar)
6. [Write timestamp: use the Kafka broker timestamp](#write-timestamp-use-the-kafka-broker-timestamp)
7. [TTL: remove it, and make removal push-based](#ttl-remove-it-and-make-removal-push-based)
8. [Work breakdown](#work-breakdown)
9. [Cutover sequencing, and existing data in Scylla](#cutover-sequencing-and-existing-data-in-scylla)
10. [Context for the membership-db conversation](#context-for-the-membership-db-conversation)
11. [Why the GCS dump must keep being produced](#why-the-gcs-dump-must-keep-being-produced)
12. [Bugs found along the way](#bugs-found-along-the-way-independent-of-this-project)
13. [Reference](#reference)

## Summary

The `rtb-membership-consumer-service` receives segment data over two paths from the same producer: a Kafka topic (activity-driven) and a 4-hourly GCS full-snapshot dump. The GCS path accounts for **92.9% empty records** and peaks at **402,907 records/sec** — at the throughput ceiling that previously forced a correctness fix to be reverted.

This proposes that the bidder **stop consuming** the GCS dump, while membership-db **continues producing** it for the analytics lineage that depends on it. Coverage is restored by adding change-only emission to the existing Kafka topic.

## Two findings that shaped this

1. **The GCS segment dump is a shared data product, not a bidder delivery mechanism.** It feeds attribution, incrementality, and reporting across four teams. The bidder is one of roughly eight consumers. Deleting its production is not a bidder-team decision — and is not necessary.
2. **Consumption and production are separable.** Stopping the bidder's consumption is almost entirely within bidder-team control.

## The problem

Production's refresh strategy has **no emission filter at all**. `ParallelDiskSingleEvalNoUpdate` (`server/config/config-prod-c4hm32.yml:31`) emits every household every sweep, unconditionally.

Its sibling `SingleDiskSingleEvalNoUpdate` appears to have a filter — `!response.update_deltas.is_empty()` (`refresh.rs:288`) — but it is dead code. `update_deltas` is built as a single-entry map whenever `default_advertiser_id` is `Some(...)` (`query.rs:142`), and refresh always passes `Some(1)` (`refresh.rs:102`). The map is never empty.

### Measured, 24h, namespace `bidder`

| Measure | Value |
|---|---|
| GCS segment records processed | 10.60B |
| ...entirely empty | 9.85B (**92.9%**) |
| Peak rate | **402,907/s** |
| Duty cycle | 43.8% (~1.75 of every 4 hours) |
| Kafka messages | 1.03B |
| Non-empty GCS output | 0.75B — *less than Kafka's total* |

Every empty record still executes a full `REPLACE` to east and west with collection tombstones (`gcs.rs:929-936` — the counter increments, then the record is sent through; there is no `continue`).

The peak is the crux. Commit `1f43fe7` reverted BID-3440's read-before-write correctness fix because it *"never moved file-processing throughput toward the 350-400k TPS the pipeline needs."* The sweep sits at that ceiling for 44% of wall-clock, and 93% of what it pushes through conveys nothing.

## Proposal

1. **membership-db keeps writing the GCS dump exactly as today.** No cadence change, no volume change, no analytics breakage.
2. **Add change-only emission to Kafka** — additive. The sweep continues its GCS write *and* publishes changed households to `segment-updates-burnin-proto`.
3. **`rtb-membership-consumer-service` deletes its GCS segment consumer** and gets full coverage from Kafka.

Result: ~93% Scylla write reduction, the 402k/s peak eliminated, dormant-household coverage retained, and no downstream breakage.

## Change detection: digest in the ETL sidecar

Production runs the **MCRocks** engine (`server/config/config-prod-c4hm32.yml:2`), where `membership_map` is never populated (`server/src/model/mc/mod.rs:958`) and `apply_segments`' persistence calls are commented out (`mc/mod.rs:669-682`, `:698-722`).

**There is no stored membership state to diff against.** This also explains the over-emission from the other direction: with nothing stored, every evaluated-true category reads as "added" on every pass.

History settles what that means. Those `set_categories` calls were **never live** — they were born commented out in `cbd5d52b2d8939341d9bd9aacbd78b215df820f6` (2025-02-19, PR #312, "Feature/multi column"). A `git log -S` on the commented line returns exactly that one commit, and the pre-multi-column code did not persist categories this way either. No TODO, no ticket, untouched for 18 months. It is an unfinished migration artifact that has hardened into de facto design.

So change detection needs **new state**. The ETL sidecar is the right home:

- It already receives the full `UpdateResponse` stream — including `score`, `tags`, and `version`, which are *never persisted anywhere* — and already transforms per-record (`etl/src/service/mod.rs:256-316`). A digest compare slots in as one more `try_filter_map`.
- Zero membership-db storage changes. A `digest` column family would be mechanically clean (CFs are created lazily at open, `engine.rs:56-69`, so no store rebuild) but would convert prod's currently **read-only** sweep into a full-store *write* sweep — WAL, memtable, and compaction across every household every pass.
- Full fidelity. Any membership-map-based diff would be presence-only and blind to score, tag (including `holdout`, which the consumer keys on at `segment_update.rs:52-57`), version, and campaign-remap changes.
- Fails safe. A restart re-emits everything once; `local_file_dir` is provisioned if it should be persisted.

A ready-made md5-to-u64 idiom exists at `common/src/model/condition/condition_v2.rs:1038-1042`. Note that `impl Hash for CategoryInfo` hashes only `category_id` (`household.rs:117-121`) and is **not** usable here.

### Ordering constraint: advance the digest only on confirmed publish

> The digest must be updated **after** a successful publish, never optimistically. Without a TTL there is no clock to clean up after a lost message, so a household whose emission failed but whose digest advanced would never re-emit — it would stay stale indefinitely.

Post-publish update makes the next sweep retry automatically. This should be backed by producer-side retries and a producer DLQ; a publish that did not reach Kafka must not count as done.

Note that the consumer-side DLQ (`rtb.membership-consumer.dlq`, `constants.rs:10-11`) does not cover this — it catches consumer failures, while the digest lives producer-side in the sidecar. There is also no DLQ redrive consumer in the repo today, so redrive is currently a manual operation.

## Write timestamp: use the Kafka broker timestamp

Today the CQL write timestamp is the user's *last-activity* time: `segment_update.rs:76` → `scylla.rs:351` → `household_profiles.rs:115` → `USING TIMESTAMP` at `:157`. Upstream can day-truncate that to midnight UTC.

**Do not widen proto field 2.** It looks unread by serving code, but it drives analytics partitioning — `GCP-Importer:spark/custom-jobs/tmul_burnin_08_filter.py:163,170-172` (with an explicit *"generate dt/hh partitions based on epoch timestamp"* comment) and `sqlmesh:models/dw-main-bronze/raw/tpa_membership_update_log.sql` (`INCREMENTAL_BY_TIME_RANGE`, `partitioned_by date(time)`). Microsecond values would corrupt both.

The **Kafka broker timestamp** is available on every consumed message, is the correct notion of publish order, is monotonic and non-truncatable, and survives replay — which matters because `KAFKA_AUTO_OFFSET_RESET = "earliest"` (`constants.rs:19`) means a consumer-group reset replays from the start of retention. No proto change, no producer change, no analytics exposure.

> **Implementation trap:** materialize the timestamp into `params` once per logical record, before `write_both` (`scylla.rs:288-327`). Both statements are `set_is_idempotent(true)` (`scylla.rs:108,115`), so the driver may speculatively retry; generating the clock inside a retry would break idempotency.

## TTL: remove it, and make removal push-based

**Segment writes should carry no TTL.**

A finite TTL is fundamentally incompatible with change-only emission. Under change-only, a household whose membership is *stable* generates no emissions, therefore no writes, therefore nothing refreshes its TTL. A household that has been a valid member continuously past the TTL window is silently deleted — and the longest-tenured, most stable memberships are hit first. The TTL cannot serve as the expiry mechanism because nothing refreshes it.

Making a finite TTL safe would require a "touch" mechanism — tracking last-emit time and forcing re-emission before expiry — which reintroduces volume and a second scheduling concern.

`DEFAULT_TTL_SECS` is hardcoded in the shared `rtb-scylla-models` crate (`household_profiles.rs:42`) and used by other services, so removing it for this path is a shared-crate change.

### Removal: each path deletes its own columns

Instead of TTL, removal is pushed. When a path's data goes empty for a household, it **deletes its own columns** rather than writing empty collections:

- Segment path owns `segments`, `geo_version`, `segment_scores`, `holdout_campaign_ids`, `timestamp_segments`.
- Intent path owns `advertiser_scores`, `campaign_scores`, `timestamp_intent_scores`.

The row disappears naturally once both have deleted. No cross-path coordination, no read-before-write, and it composes correctly regardless of which path empties first.

Tombstone cost is not a concern here: the current design generates collection tombstones 9.85B times a day, so a delete on an actual state transition is negligible by comparison.

(Read-before-write would also be affordable now — BID-3440's version died at 402k/s, and change-only emission removes the volume that killed it. But the per-path delete needs no read at all, so it is preferred.)

### The one gap: MDB's garbage collector deletes households silently

Push-based removal covers category expiry correctly. An emptied-but-still-present entry **does** emit: `Inner::exe` unconditionally calls `update_response` with `default_advertiser_id = Some(1)`, so `update_deltas` is never empty and the record always goes out (`refresh.rs:78-113`, `query.rs:112-152`).

The gap is entry deletion. MDB runs a **daily garbage collector** that hard-deletes emptied households:

- `garbage_collect.rs:108-111` calls `entry.delete()` when `GarbageCollectRes::IsEmpty` — no categories, location, geo, metadata, or scores remain (`server/src/model/mc/mod.rs:397-400`).
- `MCRocksHandle::delete` (`handle.rs:631-642`) removes **only the index key**. Its own doc comment says so.
- The index CF is what drives the sweep (`engine.rs:266-268`). Once the key is gone the household is invisible to the sweep **permanently** — `rebuild_index()` is commented out for the TPA engine in prod (`lib.rs:99`).
- Schedule: `gc_cron: '0 0 9 ? * ? *'` (`server/config/config.yml:80`), **not overridden by any environment profile**. Daily at 09:00, prod included.
- It emits no Kafka message and no metric — only an aggregate log line (`garbage_collect.rs:120`).

> Without a TTL, a household deleted this way strands its ScyllaDB row forever.

In steady state there is normally a ≥1-day gap between a household emptying and GC deleting it, and the sweep runs every 4 hours, so the removal emission usually goes out first. Stranding arises when:

- **The sweep is down, disabled, or fails mid-run while GC continues.** `SingleDiskSingleEval` aborts the entire run on first error (`refresh.rs:232-235`); GC is unconditional in every profile.
- **The `Delete` gRPC or `delete-ips` bulk CLI is used** (`membership_db.rs:306-338`, `utils/src/main.rs:449-480`). Both are immediate, unconditional, and silent — the RPC's synthetic response goes to the gRPC caller only and never reaches Kafka (`etl/src/service/grpc.rs:75-81`).

**Fix — either is small:**

1. Emit a final "no segments" `UpdateResponse` before `entry.delete()` in GC, and have `_delete` publish through the ETL producer. The eval already produces a valid empty-delta record, so no new message construction is needed.
2. Stop deleting the index key in GC and let the emptied entry continue to be swept — it already emits correctly — accepting index-CF growth.

**Verified absent:** there is no privacy/GDPR/CCPA deletion path. A repo-wide search for `gdpr|ccpa|opt.out|forget|erase|dsr|purge|privacy` returned no matches in any deletion context; `server/src/service/rest.rs` exposes only `/prometheus`, `etl/src/service/rest.rs` has no delete route, and nothing in `ansible/` deletes households. `full_delete` (`handle.rs:570-629`) and `delete_cf_key` (`engine.rs:131-135`) both have **zero callers** — dead code since `cbd5d52b` (2025-02-19).

**Campaign deactivation needs no mechanism.** The bidder resolves segments through `by_segment_id`, rebuilt from Redis `rtb:campaigns:mntn`, whose publisher SQL emits only campaign groups with a live flight (`campaign-metadata-service/utils/sqlQueries.py:252-273`). A dead segment id is a lookup miss. `segment_scores` keys are intersected against surviving auction candidates (`MetadataService.kt:75`) and can only ever *suppress* a bid, never create one. Stale campaign data is inert in both directions.

## Work breakdown

### membership-db team

1. Digest-based change detection in the ETL sidecar (`etl/src/service/mod.rs:256-316`).
2. Kafka producer handle on `ServiceInner` — it has none today (`etl/src/service/mod.rs:90-102`); the producer is private to `KafkaEtlServiceInner` (`etl/src/etl/kafka.rs:36`).
3. Rate-limit the emission path (`send_segment_logs`, `etl/src/kafka/mod.rs:108-136`, currently unthrottled).
4. Producer-side durability: retries and a producer DLQ, with the digest advancing only on confirmed publish. A publish that did not reach Kafka must not count as done.
5. Close the garbage-collector gap — either emit a final empty record before `entry.delete()` (`garbage_collect.rs:108-111`) and publish `_delete` through the ETL producer, or stop deleting the index key. Without this, GC-deleted households strand their ScyllaDB rows permanently under a no-TTL design.

### Bidder team

6. Kafka broker timestamp as `cql_timestamp`, split from the `timestamp_segments` column so that column keeps meaning activity time.
7. Delete `run_segment_batch_consumer` (`gcs.rs:509-702`) and `GCS_SEGMENT_SUBSCRIPTION` (`config.rs:249`), plus the bucket notification (`mntn-devops:.../gcs-rtb-segment-updates/bucket-notification.yaml`).
8. Replace the Pub/Sub freshness page (`mntn-argocd:apps-v3/bidder/alerts/prod/membership-consumer.yaml`, 3h warn / 3.5h page) with Kafka-lag alerting.
9. Add the segment-path DLQ.
10. Remove the TTL on segment writes (shared-crate change), and implement delete-on-empty for the segment columns.
11. Cutover sequencing — see below. Step 3 (one final sweep) is required to clear inherited TTLs.

### Unchanged

The proto, the message format, the GCS dump's production, the analytics lineage, the bidder read path, the intent pipeline.

## Cutover sequencing, and existing data in Scylla

**Existing TTLs are not cleared by a config change.** Scylla stamps expiry per cell at write time. Removing the TTL affects only future writes; every row already in the table keeps the 7-day clock it was stamped with and will expire 7 days after its last write. Those rows must be *rewritten* to clear it.

The sweep itself is the rewrite tool. Because this plan keeps membership-db producing the GCS dump, one final sweep performs the migration:

1. Deploy change-only Kafka emission (membership-db); verify the topic carries changes.
2. Deploy the consumer changes: no TTL on writes, delete-on-empty per path.
3. **Let exactly one full GCS sweep run** — it rewrites every household with no TTL, clearing the inherited 7-day expiry.
4. Then delete the GCS consumer.

> Step 3 is required, not optional: skip it and every row not otherwise touched expires 7 days after cutover. Overwriting a cell with a no-TTL write clears the previous TTL, so a single full pass is sufficient.

**Rollback is trivial.** Because the dump keeps being produced, re-enabling the Pub/Sub subscription restores full-snapshot writes at any time. This is a reversible change, unlike the original "delete the GCS path entirely" framing.

### Two transition hazards

**Unit correctness on the broker timestamp.** Kafka broker timestamps are **milliseconds**; `cql_timestamp` is **microseconds**. Passing millis where micros are expected puts every write 1000× below existing cells — precisely the failure documented at `household_profiles.rs:29-33`: writes silently drop, with no error, until TTL expiry. Existing cells were written as `activity_epoch × 1e6`, so a correctly converted broker timestamp lands in the same magnitude and wins normally.

**Per-cell TTL means partial rows.** A Kafka segment write refreshes only `segments`, `geo_version`, `segment_scores`, `holdout_campaign_ids`, and `timestamp_segments`. It does not touch `advertiser_scores` / `campaign_scores` / `timestamp_intent_scores`, which stay on the intent pipeline's clock. Already true today, but it means rows can be partially populated.

**Dormant households** expire after cutover at whatever TTL is set — the intended behavior, since they would have aged out of MDB's 30-day membership anyway. The open decision is whether the window should be 7 days (do nothing) or 30 (sweep-as-backfill above). That is a recall-versus-storage call and should be settled before the membership-db conversation.

## Context for the membership-db conversation

- Prod has been on `ParallelDiskSingleEvalNoUpdate` continuously since `ed3c2d513e1ddbfadfa5d01b6f75540467d42b9d` (2023-02-16, PR #131, *"adding ability to refresh in parallel"*).
- `update_membership_entry: Some(false)` came from `f7a69186fe4dd60667e17da23de8accb233401d1` (2022-06-03, PR #85, *"lots of refresh updates for migration to no cache layer"*) — the same commit that added both the flag and the first NoUpdate strategy. Migration-era.
- **No open work conflicts.** Zero issues; one open PR (#425, HHID, unrelated). But active branches are heavily **SlateDB** (`feature/slatedb`, `danh/feature/slatedb`, `slatedb-jaime`, `danh-merge-hhid-slatedb`) — a storage-engine migration in flight. Worth asking whether that lands first, since it would moot any MCRocks-level design and might restore `membership_map` persistence.
- Useful precedent: PR #327, *"TGT-4014: MembershipDB: Move from pull model to push"* — the one PR in the repo with a substantive writeup.
- **Caveat on all of the above:** this repo has essentially no PR discussion. Empty bodies, no human review comments, one-line commit messages. Intent is inferred from code, not stated.

## Why the GCS dump must keep being produced

Deleting it would sever:

| Consumer | Downstream |
|---|---|
| `GCP-Importer` `tmul-burnin-batch` (`config/importers.yaml:1028-1034`) | BQ bronze → `dw-main-silver.aggregates.tpa_membership_update_log_uber` |
| `airflow` gateway_hourly / ip_data_daily | Cloudberry `logdata.tpa_membership_update_log` |
| `airflow-reporting` `tmul_gateway`, `tmul_ip_data` | BigQuery `logdata.tpa_membership_update_log` |
| `airflow` `tmul_holdout_segments_by_day` (+ Cloudberry variant) | holdout / incrementality |
| `airflow` `tmul_by_day` | gates the attribution lift pipeline |
| `airflow` / `airflow-ti` memdb-batch monitors | asserts 128 `_SUCCESS` files every 4h |

Only `airflow-ti:materialize_mntn_first_party` is Kafka-fed and would survive.

Separately, **change-only semantics on the Kafka topic would break the roster jobs** if they consumed it. `tpa_membership_update_log_uber.sql` reads `in_segments` for a single `dt`, explodes per segment, and clusters by `[date, advertiser_id, campaign_id]` — a daily audience roster, not an event stream. Under change-only emission each partition would become "IPs that changed that day," and every population count, coverage %, and reach figure would silently collapse. This is why emission is **additive** rather than replacing the GCS dump.

## Bugs found along the way (independent of this project)

- **No `epoch == 0` guard in the consumer.** A proto3 absent `uint32` decodes to `0`, giving `cql_timestamp = 0` — permanently below every stored cell, so every subsequent write for that IP is silently dropped until TTL expiry. The producer guards this on one path only (`kafka.rs:210-212`). **Live today.**
- **The two segment paths disagree on holdouts.** `segment_json_to_update` (`gcs.rs:864-882`) does not filter holdout-tagged segments out of `segments`/`segment_scores`; the Kafka mapper (`segment_update.rs:40-63`) does. Both full-replace, so any household with a holdout segment flip-flops on a 4-hour cycle. Resolves itself once the bidder stops consuming GCS.
- **`segment_scores`' freshness gate reads `timestamp_intent_scores`**, not `timestamp_segments` (`HouseholdProfileService.kt:53`, `ThresholdService.kt:281`) — segment data gated by the daily intent pipeline's timestamp.
- **A likely-dead DAG, not bidder-owned:** `airflow:dags/targeting/tpa_membership_log_kafka.py` reads `s3://sh-datalake-prod/topics/...` with a 15-minute partition path, but `sh-kafka-connect` commit `30e5f8bf` (2025-08-21) moved the sink to GCS `mntn-analytics-raw` with an hourly template. It is probably skipping every run. Unverified at runtime.
- **GCS refresh captures the clock once per file** (`etl/src/service/mod.rs:262`), and `us_as_sec` truncation at `kafka.rs:312` destroys sub-second ordering. Both matter only if someone starts reading field 2 — but they are bugs now.

## Reference

- **Prod topic:** `segment-updates-burnin-proto` (`mntn-argocd:apps-v3/targeting/membership-etl/values-prod.yml:114`). The `segment-updates-proto` default in `constants.rs:9` carries no prod traffic; the legacy topic's only reader, the Kotlin `membership-consumer`, was decommissioned 2026-07-10 in `698c51afc4d56b0a5ee9829e6780bf3b5d2a9230`.
- **Sole producer:** `membership-db/etl`, confirmed org-wide across 742 non-archived repos. `KAFKA__PRODUCER__PROTO_PUB_TOPIC` exists in exactly 3 files, all `membership-etl`.
- **Two ETL deployments:** k8s (ArgoCD, 40 replicas, `refresh_enabled: false`) runs the Kafka activity path; VM (ansible/systemd on `membership-db-prod-c4hm32-[0:63]`, `refresh_enabled: true`) runs the sweep. The split is systematic across every environment — k8s pods have `local_mdb_host: null` and no co-located MDB.
- **No reverse index** exists in membership-db: households are keyed only by IP, `campaign_id` is never persisted, and households are distributed by `MD5(ip) % 64` (`common/src/lib.rs:64-68`). Per-campaign recompute would cost a full 64-shard scan, which is why this proposal uses the existing sweep instead.
