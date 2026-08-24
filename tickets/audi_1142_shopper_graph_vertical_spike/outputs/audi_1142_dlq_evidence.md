# AUDI-1142: DLQ evidence for Select recommendation failures vs Shopper Graph /vertical errors

Desk evidence only (GitHub reads, no VPN/RMQ UI). All cites are `SteelHouse/select-app@eaf611f` (main, read 2026-08-24).

## 1. What feeds select-dlq

Queue topology (`select-app@packages/queue/src/constants.ts`, `packages/queue/src/queue-schemas.ts`):
- Names are versioned: `select-queue.v<N>` / `select-retry.v<N>` / `select-dlq.v<N>`. Current `VERSION = 5`; the June incident was on `.v3`.
- `select-queue.v5` is declared with `x-dead-letter-exchange: ''` and `x-dead-letter-routing-key: select-dlq.v5`, so any `nack(msg, false, false)` on the main queue dead-letters straight into the DLQ.
- `select-retry.v5` has `x-message-ttl: 600000` (10 min) and dead-letters back to `select-queue.v5`. Retry cadence is therefore ~10 min per attempt.

Paths into the DLQ (`select-app@apps/select-queue-listener/src/listener.ts`):
- `RETRY_MAX_ATTEMPTS = 5`. `scheduleRetry` republishes to the retry queue with `x-retry-count` incremented; at `retryCount >= 5` it logs `Max retry attempts reached, sending to dead letter` and nacks without requeue (DLQ).
- Messages failing `SelectMessageQueueSchema` parse are nacked to DLQ immediately ("Invalid message format").
- Non-retryable custom codes go straight to DLQ: `ORDER_ITEM_NOT_FOUND_DB_ERROR`, `ORDER_ITEM_MISSING_LATEST_VERSION` (order-item-setup-due), `OFFERING_NOT_FOUND_DB_ERROR` (inventory-canceled), principal parse failures (offering-updated), campaign/order-item advertiser mismatch (associate-campaign).

## 2. Alert threshold

`select-app@apps/select-observability/observability/alerts/manual/prod/dlq-alert.yaml`:
- Grafana rule `select-dlq-messages-alert` ("select DLQ Messages Detected"), group `select-rmq-alerts`, evaluated every 1m over the last hour.
- Expr: `sum(rabbitmq_detailed_queue_messages_ready{namespace="select", job=~"select-prod-rabbitmq-cluster", queue=~".*dlq.*"})`.
- Threshold: **> 0**, sustained `for: 2m`. So a single dead-lettered message pages after 2 minutes.
- Notifies receiver `mntn-select-alerts`; annotation links the DLQ runbook and the `select-rmq-overview` Grafana dashboard.

Runbook (`select-app@docs/runbooks/dlq-investigation.md`): on-call inspects `select-dlq.<latest version>` via the RMQ admin UI, groups errors, requeues by republishing the payload to `select-queue.v*` with `delivery_mode: 2`, purges after documenting, and records everything in a weekly incident report under `docs/incidents/`.

## 3. Recorded incident volume/dates

`select-app@docs/incidents/2026/Q2/2026-06-01-select-recommendations-dlq.md`:
- 2026-06-01: **486 messages** in `select-dlq.v3`, all topic `generate-advertiser-recommendations`, all at `x-retry-count: 5`. Dead-letter events logged 10:25-10:55 UTC.
- Root cause: **Gary -> audience-service** (prod-targeting) availability churn (`ECONNRESET`, `connect ETIMEDOUT 10.105.17.215:80`, pod scheduling pressure, autoscaler node scale-down), via `garyRestClient.audience.getStandaloneSegments(...)`. **Not a Shopper Graph failure.**
- Resolution: one message requeued successfully at 11:39, full rerun retriggered 11:46 and stayed healthy; no Select code change.

Shopper-graph-specific DLQ volume: **none recorded**. GitHub code search for `shopper` under `docs/incidents/` returns 0 hits; the other Q2 DLQ incident (`docs/incidents/2026/Q2/2026-05-19-DLQ-messages-for-associate-campaign.md`) is associate-campaign, not Shopper Graph. Absence of a recorded incident is expected given the retry-disable behavior in §4: shopper-graph failures on the queue path are acked, so they never accumulate in the DLQ.

## 4. SHOPPER_GRAPH_CLIENT_ERROR handling and retry semantics

Error origin (`select-app@packages/shopper-graph-client/src/index.ts`):
- `getAdvertiserVerticalsById` calls `GET {SHOPPER_GRAPH_URL}/vertical?advertiser_id=...`. Fetch/network failure or any non-ok, non-404 status returns `customCode: 'SHOPPER_GRAPH_CLIENT_ERROR'` (payload carries status/body/endpoint). 404 is treated as "no assigned verticals" and returns success with empty rows. Schema-parse failure returns `SHOPPER_GRAPH_RESPONSE_PARSE_ERROR`. Same `SHOPPER_GRAPH_CLIENT_ERROR` code is emitted by `/autopilot`, `/autopilot_from_url` (plus `SHOPPER_GRAPH_BUSY_ERROR` on 429), and `/search_term`.
- Sole caller of `getAdvertiserVerticalsById`: `select-app@packages/domain/src/advertisers/index.ts` (advertiser embedding path).

Queue-listener retry disable (`select-app@apps/select-queue-listener/src/listener.ts`, `packages/env/src/select-queue-listener.ts`):
- `isShopperGraphError = env.SHOPPER_GRAPH_CLIENT_ERROR_RETRY_DISABLED && error.customCode === 'SHOPPER_GRAPH_CLIENT_ERROR'`.
- Applied in the `generate-advertiser-embedding` topic handler: on a shopper-graph error the handler `break`s and the message is **acked as success** — no retry, no DLQ.
- `SHOPPER_GRAPH_CLIENT_ERROR_RETRY_DISABLED: z.coerce.boolean().default(true)`. In-code comments call it "Temporary env to avoid QA queue jamming up with doomed tasks" because ShopperGraph QA reads `coredbdev.public.advertisers`, which is not synced to QA core db. **The default is true, so unless prod overrides the env var, prod also silently swallows shopper-graph client errors on this path** — real `/vertical` outages produce no DLQ messages and no DLQ alert for embedding generation.

Temporal workflow path (`select-app@apps/select-workflow/src/workflows/select-recommendations.ts`, `packages/workflow-core/src/activities/advertiser-recommendations.ts`):
- Recommendations now run as a Temporal workflow, not queue-listener topics. `SHOPPER_GRAPH_CLIENT_ERROR` / `SHOPPER_GRAPH_RESPONSE_PARSE_ERROR` are declared failure codes on `resolveAudienceExpressionActivity`, `computeAdvertiserEmbeddingSimilarityFeaturesActivity`, and `generateExplainabilityActivity`.
- The workflow maps them to `ApplicationFailure` with `nonRetryable: false` (steps `resolve_audience_expression`, `compute_advertiser_embedding_similarity_features`, `explainability`), i.e. Temporal retries them; failures surface as workflow failures/OTel spans, not RMQ DLQ messages.
- Removal-of-error handling is being tracked in `select-app@specs/2026-08-07-1800-remove-select-error/` (references `SHOPPER_GRAPH_CLIENT_ERROR` in `_trace.md` / `_parity-roster.md`).

## 5. Summary for the spike

- The DLQ alert fires at >0 messages for 2m, so DLQ evidence is high-signal, but there is no shopper-graph DLQ evidence to find: the one recorded recommendations DLQ incident (2026-06-01, 486 msgs) was Gary/audience-service, and the embedding path acks shopper-graph errors by design (`SHOPPER_GRAPH_CLIENT_ERROR_RETRY_DISABLED` defaults true everywhere, including prod).
- Consequence: Shopper Graph `/vertical` outages are invisible in the DLQ/alert surface for `generate-advertiser-embedding` and show up only as Temporal workflow retries/failures (OTel `workflow.failure.cause_type = SHOPPER_GRAPH_CLIENT_ERROR`) on the recommendations path. Any volume quantification has to come from Grafana/OTel traces, not RMQ.
