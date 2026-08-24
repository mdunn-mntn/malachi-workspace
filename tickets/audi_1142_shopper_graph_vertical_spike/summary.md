---
doc_type: ticket
title: "AUDI-1142: shopper graph vertical spike"
status: in_progress
date: 2026-08-24
summary: "SPIKE: estimate AUDI-1086 (Shopper Graph /vertical optimizations for Select recommendations)"
result: "in progress"
question: "What is the per-work-item story-point cost to give Select a company_url-capable /vertical and fix the shared-domain cache miss (AUDI-1086), and what evidence supports it?"
framing_state: locked
---

# AUDI-1142: shopper graph vertical spike

**Jira:** https://mntn.atlassian.net/browse/AUDI-1142
**Status:** in_progress
**Date Started:** 2026-08-24
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** What is the per-work-item story-point cost to deliver AUDI-1086 (company_url-capable /vertical + fix for the two-AIDs-one-domain cache miss + Brian's new-endpoints/separate-pod proposal), and what root-cause evidence and blast-radius numbers support it?
- **Goal (why / the decision):** Bryce Wagg needs the lift estimate to schedule AUDI-1086 on the priorities list (H2 ask from Mike Allen/Select, target ~Sept 1). Epic AUDI-1087 Support Select Recommendations. North-star tie: Select products are a named Tier-2 priority; this is a direct PM ask.
- **Objective (done-when):** A per-item SP estimate with root-cause evidence posted as a comment on AUDI-1086 and noted on AUDI-1142; spike then closes. Binary: the comment exists and each line item carries an SP number plus its evidence, or it doesn't.
- **Approach (how):** Code evidence from SteelHouse/shopper_graph@6626756 (done in planning: /vertical never consults fpa.mm_domain_map; /autopilot does). Close 2 remaining code unknowns (who POSTs /vertical; gary-ql read path). Quantify blast radius: BQ (shared-domain AID counts from integrationprod.advertisers.company_url; hoteled AIDs lacking a fpa_advertiser_verticals row; ~561-row domain-mismatch DQ check) + one day of prod pod logs (gs://mntn-data-archive-prod/ti_argocd_logs/shopper_graph/) + DLQ volume. Cost 3 work items against the /autopilot_from_url pattern.
- **What would change the answer:** Bryce's reply to the scope question (estimate-only vs design work in my scope) expands or holds the frame. If the hoteled-AID miss population and DLQ volume are both ~0, the caching fix drops from reliability item to cost/latency item and the estimate collapses to the new-endpoint work.

## 1. Introduction
MNTN Select builds audience recommendations by calling the Shopper Graph service (SteelHouse/shopper_graph = the MNTN Matched backend, DS team owns it). Four endpoints are used: /vertical, /autopilot, /autopilot_from_url, /search_term. For sales, Select wants to query before an advertiser_id exists; /vertical is the only endpoint with no company_url-friendly version. AUDI-1086 asks for that endpoint plus a fix to the /vertical caching behavior; this spike (AUDI-1142) researches the work and produces its story-point estimate. Once the estimate is posted, the spike closes.

## 2. The Problem
- AUDI-1086 (Mike Allen, 2026-07-07): "If the cache doesn't hit on the AID (which can happen when two AIDs share the same domain), it falls back to the domain (making it required, not optional as the schema indicates), and we were getting DLQs."
- Brian McAdams (comment, 2026-07-07): the endpoints (except /autopilot_from_url) were built for MNTN Matched only; proposes NEW endpoints reusing the same code, plus a separate Argo pod (rudimentary throttling exists because /autopilot_from_url overuse can crash prod MNTN Matched).
- Reported by: Mike Allen (Select). Affects: Select recommendation builds (DLQs), sales-agent pre-advertiser queries (impossible today for verticals).
- Scope check in flight: Malachi asked Bryce whether the ask is estimate-only (design belongs to Brian/Mike). Step 6 below gates on the answer.

## 3. Plan of Action
1. Scaffold ticket + frame. DONE 2026-08-24.
2. Clone SteelHouse/shopper_graph locally (@6626756, same commit as evidence). DONE.
3. Close code unknowns: who POSTs /vertical (select-app client only GETs); confirm gary-ql verticals are DB-read-only (out of blast radius).
4. Quantify (BQ via bq_run.sh): domains shared by >1 active advertiser; of those, AIDs lacking a fpa_advertiser_verticals row (true miss population, type=1 filter); reconfirm ~561-row mm_domain_map vs company_url mismatch.
5. Quantify (prod evidence): one day of exported pod logs (VERTICAL_HANDLER completions by status, scraping failures); DLQ volume from select-app incident docs / Grafana alert history.
6. Cost 3 work items (patch /vertical with domain fallback; /vertical_from_url on the autopilot_from_url pattern + select-app client method; separate Argo app + queue-listener URL flip). Draft linted Jira comment on AUDI-1086. Post + close gated on Bryce's scope reply.
7. /capture: route durable facts to knowledge/, self-review entry, commit throughout.

## 4. Investigation & Findings

### Root cause (from code, SteelHouse/shopper_graph @ main 6626756, read 2026-08-24)
- The /vertical "cache" is Postgres `fpa.advertiser_verticals` keyed on advertiser_id (`middleware/k8s/shopper_graph_wrapper/vertical_wrapper.py`, `VerticalHandler.handler()`). GET returns 404 on no row. POST with no existing row and no vertical_id falls to the expensive path: look up `company_url` from `public.advertisers` (400 if empty), scrape the site, GPT-classify, embed.
- **`/vertical` never consults `fpa.mm_domain_map`** (the hoteling map: multiple AIDs sharing one domain -> root AID). `/autopilot` DOES consult it on cache miss (`autopilot_wrapper.py` calls `domain_map.get_mapping(domain)` and reuses the root advertiser's profile). So a hoteled AID2 sharing AID1's domain misses the AID-keyed lookup and re-scrapes/re-classifies the same domain; scrape failures return 4xx/5xx -> select-queue-listener job fails -> RabbitMQ `select-dlq.<version>` (select-app `docs/runbooks/dlq-investigation.md`; alert `apps/select-observability/observability/alerts/manual/prod/dlq-alert.yaml`).
- Schema-vs-reality confirmed in `middleware/k8s/api_spec.yaml`: POST /vertical `companyUrl` is `required: false`; GET /vertical has NO company_url param; advertiser_id `required: true` on both. On any AID with empty/wrong `public.advertisers.company_url`, the miss path 400s, so company_url is effectively required.
- Template for the new endpoint exists: `/autopilot_from_url` (`autopilot_wrapper.autopilot_from_url_handler`): domain-map lookup first, then `Semaphore(1)` per-pod throttle returning 429 + Retry-After: 30. Effective global concurrency = pod count (3 replicas, HPA to 8 at CPU-70%, `mntn-argocd/apps-v3/targeting/shopper-graph/hpa.yaml`). select-app client special-cases the 429 (`SHOPPER_GRAPH_BUSY_ERROR`).
- Client behavior (`select-app packages/shopper-graph-client/src/index.ts` @ 4be8abe): `getAdvertiserVerticalsById` only GETs `/vertical?advertiser_id=`; 404 treated as empty rows; no client-side retry. Retry/DLQ lives in the queue layer (`SHOPPER_GRAPH_CLIENT_ERROR_RETRY_DISABLED=false` prod / `true` QA, `mntn-argocd/apps-v3/select/select-queue-listener/values-{prod,qa}.yaml`).
- Isolation status quo: ONE Deployment in prod-targeting serves all endpoints; no per-endpoint isolation. Pod logs export every 30 min to `gs://mntn-data-archive-prod/ti_argocd_logs/shopper_graph/<date>/<HH-MM>.jsonl`.
- Known DQ landmine: ~561 `fpa.mm_domain_map` rows where domain != `advertisers.company_url` (knowledge/data_catalog.md:2944, found 2026-04-20). A domain-fallback fix could silently reuse the WRONG root profile on those rows.

### Adversarial verify of the code claims (local clone @6626756, 2026-08-24)
All five claims held; two precision fixes to the root-cause narrative:
- The scrape+classify path is NOT gated on a cache miss. Any POST /vertical without `vertical_id` runs the full scrape+GPT path (`vertical_wrapper.py:664,700`, fine-tuned model `ft:gpt-4.1-mini-2025-04-14:mntn::BMhxthUA`); an existing row only changes INSERT to UPDATE (`:741-743`). "Caching" on the POST path is nonexistent, not merely miss-prone.
- The `public.advertisers.company_url` fetch happens only when the request supplies no `company_url` param (`:648`), not unconditionally.
- Confirmed verbatim: zero `domain` references in vertical_wrapper.py; `/vertical` route (`api.py:164-181`) never touches DomainMapHandler; autopilot consults `domain_map.get_mapping` on miss (`autopilot_wrapper.py:312-327`) and in from_url (`:424-432`); api_spec.yaml POST companyUrl `required: false` (`:682-688`), GET has no company_url param (`:425-431`); `Semaphore(1)` + 429/Retry-After 30 (`:32,:465-471`).

### Who calls POST /vertical (outputs/audi_1142_vertical_callers.md)
All callers send advertiser_id+company_url with no vertical_id, so all hit the expensive path; nobody uses the cheap set-branch:
1. **airflow-ti precache DAG** (dominant): `models/vertical_categorization/verticals_auto_assignment.py`, up to 200 AIDs/run, cron `0,30 * * * *` (`dags/machine_learning/mntn_match_verticals_precache_v1_1.py`).
2. **gary-ql** `storeCompanyVertical()` (`src/utils/services/MntnMatched.ts`): fired per registration / first company_url set, fire-and-forget, errors swallowed.
3. njs-rmq-scraper: QA host only.
select-app client is GET-only. gary-ql vertical READS are DB-direct (sequelize over fpa.advertiser_verticals + Redis 1h TTL), never shopper-graph HTTP: GraphQL reads are out of blast radius.

### BQ blast radius (queries/audi_1142_blast_radius.sql, outputs/audi_1142_blast_radius.md, 2026-08-24)
- 37,802 active advertisers (deleted=FALSE, is_test=FALSE); 37,696 with a valid normalized domain.
- 2,018 domains shared by >1 AID; 9,211 AIDs (24.4%) sit on shared domains. Composition is dominated by placeholder/social URLs (youtube.com 955 AIDs, google 326, mountain.com 294, instagram 259, facebook 251, gmail 238, tiktok 229, example.com 70); genuine franchise hoteling is far smaller (orangetheory.com 149, metalsupermarkets.com 103; BAE's curated `dw-main-gold.bae.v_aid_flagged_dup_domain` = 823 AIDs / 312 domains).
- True shared-domain miss population: 2,740 shared-domain AIDs have NO fpa_advertiser_verticals row. Context: 8,025 active AIDs (21.2%) have no vertical row at all.
- **fpa.mm_domain_map is NOT in BigQuery** (region-wide INFORMATION_SCHEMA sweep, us-central1 + US, bronze/silver/gold: zero hits). The ~561 mismatch count stays a Postgres-side 2026-04-20 observation; reconfirming needs a coredb read (DS/targeting).

### Prod log evidence (outputs/audi_1142_prod_log_evidence.md, 2026-08-22/23)
- **97% of POST /vertical requests fail.** 08-23: 643 VERTICAL_HANDLER COMPLETE = 626x400 + 17x200. 08-22: 636 = 622x400 + 14x200.
- Failure mode is NOT the scrape exception (SCRAPING FAILED fired zero times): scrape returns invalid text (bot-challenge/too-short) -> self-selection fallback -> fallback almost never succeeds (0 and 3) -> 400.
- 563 of the failing AIDs are IDENTICAL across both days (90% overlap): a recurring re-failing population the precache DAG re-hammers daily (batch spike hour 00 UTC). Zero GETs observed; zero /autopilot_from_url traffic; zero 429s.
- /autopilot for context: 551 and 526 completions/day.

### DLQ evidence (outputs/audi_1142_dlq_evidence.md, select-app@eaf611f)
**The ticket's "we were getting DLQs" is not reproducible from current code paths; the absence is structural:**
- On the embedding path, `SHOPPER_GRAPH_CLIENT_ERROR` + `SHOPPER_GRAPH_CLIENT_ERROR_RETRY_DISABLED` (code default TRUE, a QA workaround) means the message is ACKED as success: no retry, no DLQ. /vertical outages are invisible to the DLQ alert on that path.
- Recommendations moved to Temporal (`apps/select-workflow`): shopper-graph failures surface as workflow retries / OTel spans, not RMQ DLQ messages.
- The one recorded DLQ incident (2026-06-01, 486 msgs in select-dlq.v3) was Gary -> audience-service ECONNRESET, not Shopper Graph. No incident doc mentions shopper.
- DLQ mechanics: select-queue.v5 -> select-dlq.v5, 5 retries x ~10 min TTL; Grafana alert fires on >0 ready for 2m.

## 5. Solution

### Story-point estimate for AUDI-1086 (drafted 2026-08-24, posting gated on Bryce's scope reply)
| Item | SP | Anchor |
|---|---|---|
| 1. New `/vertical_from_url` endpoint (autopilot_from_url pattern: domain-map lookup, Semaphore throttle, spec, tests) + select-app client method | 3 | Pattern exists end-to-end in `autopilot_wrapper.py:424-476`; client 429 handling already present |
| 2. Domain-map fallback in `/vertical` (hoteled AID inherits root vertical, mirroring `autopilot_wrapper.py:312-327`) | 2 | Same handler, same DomainMapHandler; touches one wrapper + spec |
| 3. Validation guard on item 2 (mm_domain_map vs company_url mismatch, ~561 rows Apr 2026; `DomainAdvertiserMismatch` guard exists in domain_map_wrapper.py write path) | +2 | Only if DS wants the guard/backfill in scope |
| 4. Separate Argo app for Select-facing endpoints (mntn-argocd second app + hostname + select-queue-listener URL flip per env) | 2 | Same image, config-only; mostly infra review cycles |
Core total: 7 SP; 9 with the DQ guard.
Risks flagged, not pointed: DS-team review latency (cannot self-merge shopper_graph); QA data availability (shopper-graph QA reads coredbdev, unsynced; Select actively uses QA); the 97% scrape-failure rate means a domain-map fallback helps only AIDs whose root HAS a vertical row: the 563-AID recurring-failure population needs a scraper/bot-challenge fix or backfill, which is a separate work item not in AUDI-1086's text.

## 6. Questions Answered
- **Q:** What is the /vertical cache and why does it miss when two AIDs share a domain?
  **A:** It is not a cache object; it is the `fpa.advertiser_verticals` Postgres table keyed on advertiser_id. Sharing a domain does not share the row, and unlike /autopilot, /vertical never falls back to `fpa.mm_domain_map`, so the second AID pays the full scrape+classify path or fails. Worse: POST without vertical_id re-scrapes even on a hit.
- **Q:** Are the DLQs in AUDI-1086's description still happening?
  **A:** Not via /vertical today, structurally (errors acked on the embedding path; recommendations path is Temporal, which retries instead of dead-lettering). The pain today is the 97% POST failure rate and 8,025 active advertisers with no vertical row.
- **Q:** Is company_url optional as the schema says?
  **A:** Spec says optional on POST (api_spec.yaml:682-688); in practice the expensive path 400s without a resolvable URL, and GET does not accept company_url at all.

## 7. Data Documentation Updates
(pending /capture; queued facts: mm_domain_map absent from BQ; shopper-graph pod-log export path + markers; POST /vertical always-scrape semantics; select DLQ structural bypass for shopper-graph errors; gsutil -m hang on this Mac)

## 8. Open Items / Follow-ups
- Bryce's reply on spike scope (estimate-only vs design input) — gates posting the estimate + closing.
- fpa.mm_domain_map producer still unidentified (open unknown carried from ti_1058); table not mirrored to BQ, needs coredb access to requantify the ~561 mismatches.
- 563-AID recurring scrape-failure population (bot challenges) is daily wasted LLM spend on the precache DAG; separate durable-fix candidate, not in AUDI-1086 scope.
