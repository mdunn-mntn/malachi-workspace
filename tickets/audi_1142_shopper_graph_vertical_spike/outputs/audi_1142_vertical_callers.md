# AUDI-1142: /vertical POST callers and gary-ql read-path verification

Date: 2026-08-24. Local clone: /Users/malachi/Developer/work/mntn/shopper_graph @6626756. All remote cites are default-branch HEAD at time of search (gary-ql@230bead, airflow-ti@504fe94, dbt@ffb547d, select-app@eaf611f, airflow@f2e80a1, njs-rmq-scraper@11f7333).

## Endpoint behavior recap (shopper_graph, local)

`shopper_graph@6626756 middleware/k8s/api.py:164` routes `/vertical` GET+POST to `VerticalHandler.handler` (`middleware/k8s/shopper_graph_wrapper/vertical_wrapper.py:581-796`). Branches:

- GET: SELECT from `fpa.advertiser_verticals`, cheap (vertical_wrapper.py:602-618).
- POST with `vertical_id` + `vertical_type`: direct UPDATE, cheap (vertical_wrapper.py:620-645).
- POST without `vertical_id` (only `advertiser_id`, optional `company_url`): `scrape_company_url` -> fine-tuned OpenAI `ft:gpt-4.1-mini` classify -> Databricks embedding cosine similarity -> INSERT/UPDATE. This is the expensive scrape path (vertical_wrapper.py:647-796).

## UNKNOWN 1: who POSTs /vertical

Every production POST caller found sends `advertiser_id` + `company_url` and NO `vertical_id`, so every one of them hits the expensive scrape+LLM path. No code caller of the cheap `vertical_id` set-branch was found anywhere in the org (gary-ql writes vertical rows straight to Postgres instead, see Unknown 2).

### 1. gary-ql (prod, user-facing GraphQL/REST backend). 4 call sites, one shared client fn

`gary-ql@230bead src/utils/services/MntnMatched.ts` `storeCompanyVertical(advertiser_id, company_url)`:

```ts
return Request.post(
  `${url}/vertical?advertiser_id=${advertiser_id}&company_url=${company_url}`,
  {},
);
```

`url` = `config.apis.mntn_matched.url` = shopper-graph host (`src/config/production-gcp.json`). Call sites, all fire-and-forget (`.catch` -> log only):

| Call site | Trigger |
|---|---|
| `gary-ql@230bead src/gql/root/register.ts` (register mutation, `if (url)` block) | every self-serve / assisted registration that supplies a company URL |
| `gary-ql@230bead src/controllers/v1.0/register.ts` (`handleRegister`, POST /register) | REST registration path; `company_url` is a required body field, so fires on every REST registration |
| `gary-ql@230bead src/gql/types/Advertiser/mutationResolver.ts` (`company_url` field resolver) | advertiser update that sets `company_url` for the FIRST time (`!currentAdvertiser.company_url && newCompanyUrl`) |
| `gary-ql@230bead src/data/Advertiser.ts:1662` (`runExistingTenantBootstrapAfterEffects`) | tenant-bootstrap advertiser creation, `if (companyUrl)` |

Blast radius: expensive path, one call per new advertiser or first-URL-set. Fire-and-forget, so latency does not block the user flow, but failures are silent (log line only) and the callers assume manual backfill ("will be manually backfilled" comment in both register paths).

### 2. airflow-ti batch backfill (prod, every 30 min, up to 200 scrapes/run)

`airflow-ti@504fe94 models/vertical_categorization/verticals_auto_assignment.py` (Databricks job, alias `verticals_auto_assignment_v2`):

```python
res = requests.post(
    "https://shopper-graph.in.mountain.com/vertical",
    params={"advertiser_id": advertiser_id, "company_url": company_url}
)
```

Selects advertisers lacking `fpa.advertiser_verticals` rows (active campaign groups, Salesforce OnBoarding, self sign-ups, no-SF-status; inactive-100k-MUV branch commented out), applies an exponential retry backoff on prior non-200s, caps at 200 POSTs per run. Scheduled by `airflow-ti@504fe94 dags/machine_learning/mntn_match_verticals_precache_v1_1.py` cron `0,30 * * * *` (task `auto_assign_verticals`, `ModelPysparkDbxJobOperator(model_id="verticals_auto_assignment")`). This is the dominant expensive-path traffic source: up to 9,600 scrape+LLM calls/day ceiling.

Legacy duplicate: `airflow@f2e80a1 dags/machine_learning/mntn_match_verticals_pre_cache_v1_1.py` (hourly) still has an `auto_assign_verticals` task that runs `dbt build --select models/vertical_categorization/verticals_auto_assignment.py`, but that model file no longer exists in `dbt@ffb547d ml_squad/models/vertical_categorization/` (directory listing has no `verticals_auto_assignment.py`), so the legacy task selects nothing. The dbt copy's POST loop lives on only in the airflow-ti Databricks version above.

### 3. njs-rmq-scraper (QA host only)

`njs-rmq-scraper@11f7333 media-plan/src/lib/mntn/index.ts` `getVerticalInfo`:

```ts
fetch(`https://shopper-graph-qa.in.mountain.com/vertical?advertiser_id=${advertiserId}&company_url=${domain}`, { method: "POST" })
```

POSTs the expensive path but hard-coded to the QA deployment. Not prod blast radius; would become prod traffic only if the host were changed.

### Checked and NOT /vertical POST callers

- `select-app@eaf611f packages/shopper-graph-client/src/index.ts`: full file read; the only `/vertical` call is `getAdvertiserVerticalsById` with `method: 'GET'`. Other methods hit `/autopilot`, `/autopilot_from_url`, `/search_term`, all GET. `packages/domain/src/advertisers/index.ts` and `packages/domain/src/recommendations/index.ts` consume this GET-only client.
- `dbt@ffb547d ml_squad/models/vertical_categorization/verticals_pre_cache.py` (hourly via airflow-ti DAG above, task `pre_cache_verticals`): POSTs `/autopilot` and `/search_term` for advertisers created in the last hour. Adjacent load on the same service, but never `/vertical`.
- `gary-ql@230bead src/controllers/v1.0/advertiserRegenerateProfile.ts`: POSTs `/autopilot_regenerate`, not `/vertical`.
- `dbt@ffb547d ml_squad/models/vertical_categorization/missing_domains.py` + `airflow@f2e80a1 dags/tpa/missing_domains.py`: Spark anti-join over GCS parquet, no HTTP.
- `airflow-ti@504fe94 dags/vertical_classification/vertical_classification_submit.py` (+ `spark/vertical_classification/update_website_verticals.py`): separate domain-level pipeline using its own OpenAI Batch API and JDBC reads of `fpa.advertiser_verticals`; no shopper-graph HTTP.
- `audience-service@7f7ac07 src/main/kotlin/com/steelhouse/audiencesvc/model/AdvertiserVertical.kt`: own DB model over `fpa.advertiser_verticals`; no shopper-graph HTTP call found in the org-wide host or `/vertical` searches.
- `olympus`: only doc hits (`docs/tickets/PERML-643/plan.md`, `apps/mediaplan/docs/query-optimization.md`); no code caller.
- shopper_graph self-calls: local grep of `dbt/`, `autopilot/`, `scripts/`, `middleware/` found no `requests.post` to `/vertical`; `dbt/models/mntn_matched/pre_batch/product_uniques.py:49` POST is Vault auth. `middleware/k8s/utils/audience_service.py` calls audience-service (verticalDataSource=46 sync), not shopper-graph.

Search coverage: org-wide `"/vertical"` search (117 hits, pages 1-2 = 60 reviewed; rest are frontend assets/docs by name), org `"shopper-graph.in.mountain.com"` (21/21 reviewed), `"shopper-graph" vertical` (30/30 reviewed), `SHOPPER_GRAPH` env (page 1 of 158; consumers traced to the GET-only select-app client and shopper_graph's own tooling). Any POST caller must reference the host, gary-ql's `config.apis.mntn_matched`, or select-app's `SHOPPER_GRAPH_URL`; all three surfaces traced.

## UNKNOWN 2: gary-ql vertical READS never touch shopper-graph HTTP

Confirmed. GraphQL vertical reads are DB-only:

- `gary-ql@230bead src/utils/models/tpa/AdvertiserVertical.ts`: `getVerticalsByAdvertiserId` is raw sequelize SQL `SELECT * FROM fpa.advertiser_verticals WHERE advertiser_id = :advertiserId`; `listUniqueCompanyVerticals` likewise; `createAdvertiserVertical` is a direct `INSERT INTO fpa.advertiser_verticals` (writes bypass shopper-graph entirely too).
- `gary-ql@230bead src/data/AdvertiserVertical.ts`: thin wrapper over the above three functions; no HTTP.
- `gary-ql@230bead src/data/MntnMatched.ts` `getAdvertiserVerticalCategories`: sequelize model `FpaAdvertiserVertical.findAll` + Redis cache (`advertiser_vertical_categories:` keys, 1h TTL); feeds `getReachMetrics`. No HTTP.
- Exhaustive check: repo search `repo:SteelHouse/gary-ql mntn_matched` (27 hits, all listed and triaged). The ONLY shopper-graph HTTP calls in gary-ql are `src/utils/services/MntnMatched.ts` (POST /autopilot, POST /search_term, GET /random_keyword, POST /vertical) and `src/controllers/v1.0/advertiserRegenerateProfile.ts` (POST /autopilot_regenerate). None are read paths for verticals.

Conclusion: a change to `/vertical` cannot affect gary-ql GraphQL vertical reads. gary-ql's exposure is write-side only: the four fire-and-forget `storeCompanyVertical` POSTs above, all on the expensive scrape branch, all error-swallowing.
