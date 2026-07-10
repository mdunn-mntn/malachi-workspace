# Data Knowledge — MNTN BigQuery

## Architecture

### Full Data Stack (top to bottom)
```
bronze.integrationprod  ← Postgres CDC replica (Datastream). Source for config/dimension data.
bronze.raw              ← Raw event tables (bidder, augmentor, pixel). Short TTLs.
  ↓ SQLMesh (bronze layer)
bronze.sqlmesh__raw     ← Bronze SQLMesh models (versioned)
bronze.raw.*  (VIEWs)  ← View aliases to bronze.sqlmesh__raw
  ↓ SQLMesh (silver layer)
silver.sqlmesh__logdata / summarydata / aggregates  ← Silver SQLMesh models (versioned)
silver.logdata / summarydata / aggregates  (VIEWs)  ← Clean view aliases (query these!)
silver.core  (VIEWs)   ← Direct views over bronze.integrationprod.core_* (no SQLMesh)
silver.fpa   (VIEWs)   ← Direct views over bronze.integrationprod.fpa_* (no SQLMesh)
```

### SQLMesh Repo & Model Conventions

**Repo:** `git@github.com:SteelHouse/sqlmesh.git`

**Directory structure mirrors medallion layers:**
```
models/
├── dw-main-bronze/raw/              ← bronze ingestion (hourly incremental)
├── dw-main-bronze/integrationprod/  ← CDC dimension tables
├── dw-main-silver/logdata/          ← VIEWs reshaping bronze → silver
├── dw-main-silver/ber_stg/          ← heavy incremental models (visits, conversions)
├── dw-main-silver/aggregates/       ← rollups
└── dw-main-gold/                    ← end-product tables
```

**Config:** `config.py` (not YAML). Gateways: `bronze`, `silver`, `gold`. Each maps to `dw-main-{layer}` project. State DB: GCP Postgres (`dw-main-bronze:us-central1:data-platform-state`). Dialect: `bigquery`.

**Common INCREMENTAL_BY_TIME_RANGE patterns (from existing models):**
- `cron '@hourly'` — standard for event-level tables
- `lookback 48` — reprocess 48 hours for late-arriving data
- `batch_size 168` (7 days) or `49` — chunks for backfill
- `forward_only TRUE` — no automatic rebackfill on schema changes
- `partition_expiration_days` — set in `physical_properties`
- Date filter: `time >= @start_dt AND time < @end_dt` (TIMESTAMP macros)
- Hardcoded lookbacks (e.g., 90-day event_log scan) go in the SQL, not the MODEL config

**Registered owners** (`owners.py`): `targeting-infrastructure`, `ber`, `RPLAT`, `bae`, `test`. Each has a Slack channel for audit/failure alerts.

### SQLMesh Table Tags (Supported vs. Unsupported)
SQLMesh model definitions can carry `tags` that link a table to a topic in the internal data
documentation app. A table is considered **supported** if it appears under a topic via a tag;
all other tables are **unsupported**.

Tags are added in the `MODEL()` block of a SQLMesh `.sql` model file:
```sql
MODEL (
  name logdata.impression_log,
  tags ['impressions_raw'],
  ...
);
```

Topic definitions (title, description, subtopics → tag mappings) live in a separate YAML file
maintained by the data platform team. Tags are part of the regular dev workflow and can be
linted/reviewed in PRs. When looking for the canonical table for a use case, filter by
"supported" in the data doc app — it immediately excludes dev/staging tables.

Reference: `documentation/docs/data_documentation_app.md`

### SQLMesh Versioned Table Pattern
`dw-main-silver.logdata`, `dw-main-silver.summarydata`, and `dw-main-silver.aggregates` are
**view layers only**. Every table is a VIEW pointing to a physically versioned table in the
corresponding `sqlmesh__*` dataset:

```
logdata.impression_log  (VIEW)
  → sqlmesh__logdata.logdata__impression_log__4185451957  (VIEW)
      → [upstream: coredw / Postgres source]

logdata.spend_log  (VIEW)
  → sqlmesh__logdata.logdata__spend_log__4068879977  (TABLE, HOUR partition on auction_timestamp)
```

The numeric hash suffix (e.g. `__4185451957`) is the SQLMesh model version. The view in `logdata`
always points to the current production version. Do NOT query hashed tables directly — always
use the clean alias in `logdata.*`.

### Partition Filter Best Practice — Silver Log Tables

**Critical:** Silver layer views (`logdata.*`, `summarydata.*`) are UNION ALL views of two underlying tables:
- **Recent table**: `dw-main-bronze.sqlmesh__raw.raw__*` — partitioned by `time` (TIMESTAMP, DAY partition)
- **History table**: `dw-main-bronze.sqlmesh__history.history__*` — partitioned by `date_column`

**Physical table retention (verified 2026-03-16):**

| Physical Table | Dataset | Earliest Data | TTL | Size (approx) |
|---|---|---|---|---|
| `history__event_log__1601996237` | `sqlmesh__history` | 2025-01-01 | none | ~12 TB |
| `history__impression_log__2959498407` | `sqlmesh__history` | 2025-01-01 | none | ~10 TB |
| `history__viewability_log__3987498553` | `sqlmesh__history` | 2025-04-08 | none | ~1.2 TB |
| `raw__event_log__2961306213` | `sqlmesh__raw` | 2026-01-01 | 365d | ~1.6 TB |
| `raw__impression_log__1553762165` | `sqlmesh__raw` | 2025-08-25 | 90d | — |
| `raw__viewability_log__4234484773` | `sqlmesh__raw` | 2025-12-31 | 90d | ~216 GB |

**No BQ table at any layer has data before 2025-01-01.** Pre-2025 data only exists in Greenplum coreDW (deprecated April 30, 2026). When searching for historical IP/impression data, querying the physical tables directly (bypassing the silver VIEW) does NOT extend coverage — it only avoids the UNION overhead.

BQ can push down filters to the underlying `time`-partitioned raw tables only with **direct TIMESTAMP comparisons**. Wrapping the column in `DATE()` defeats partition pruning.

**Correct (enables partition pruning):**
```sql
WHERE time >= TIMESTAMP('2026-02-04') AND time < TIMESTAMP('2026-02-11')
```

**Wrong (prevents partition pruning — scans all partitions):**
```sql
WHERE DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
```

SQLMesh model date parameters (`@start_dt`, `@end_dt`) are already TIMESTAMP type — use them directly without wrapping. Confirmed 2026-03-06 (queries using `DATE()` ran 9+ minutes vs near-instant with `TIMESTAMP()`).

### IP Search Optimization — Cross-Stage/Cross-Table Queries

Searching for a single IP across full table history (event_log, impression_log, viewability_log) is extremely expensive because these tables are partitioned by time, not IP. There is no IP index.

**Measured costs (TI-650, 2026-03-16):**

| Query Pattern | GB Billed | Wall Time | Partitions |
|---|---|---|---|
| event_log full scan (`DATE(time)`) | 14,677 GB | 35 min | 440 |
| impression_log full scan (`DATE(time)`) | 11,829 GB | 23 min | 440 |
| viewability_log full scan (`DATE(time)`) | 1,563 GB | 5 min | 343 |
| clickpass_log no date filter | 110 GB | 29s | 2,238 |
| IP funnel trace (single-day, TIMESTAMP) | 1,136 GB | 39s | 18 |

**Optimization rules for IP searches:**

1. **Use TIMESTAMP(), not DATE()** — `DATE(time)` defeats partition pruning on every table (documented above). Use `time >= TIMESTAMP('2025-07-01') AND time < TIMESTAMP('2026-03-01')` instead of `DATE(time) BETWEEN '2025-07-01' AND '2026-02-28'`.

2. **Narrow the date range** — If you know the campaign group creation date (e.g. 2025-07-11) and the VV date (e.g. 2026-02-04), search only that window. Don't scan from 2025-01-01 "to be thorough" — it adds months of unnecessary partitions.

3. **Drop LIKE wildcards for IP matching** — `ip LIKE '216.126.34.185%'` is unnecessary. IPs in silver log tables do not carry CIDR suffixes (`/32`, `/24`). Use exact match: `ip = '216.126.34.185'`. The LIKE adds string comparison overhead on billions of rows. **Exception:** `event_log.ip` has `/32` CIDR suffix on ALL pre-2026 data — use `SPLIT(ip, '/')[SAFE_OFFSET(0)]` when matching event_log IPs across time periods (see CIDR bullet under Stage Progression).

4. **Always add a date filter to clickpass_log** — Even when filtering by `ad_served_id`, clickpass_log has 2,238+ partitions. Without a date filter, BQ scans all of them (110 GB). Add `AND time >= TIMESTAMP('2026-02-04') AND time < TIMESTAMP('2026-02-05')` to scan 1 partition instead.

5. **Use aggregation for verification** — If you only need to confirm zero S1/S2 rows exist, use `GROUP BY campaign_group_id, campaign_id, funnel_level` with `COUNT(*)` instead of returning raw rows. Same bytes scanned but less output shuffling.

6. **Query tables individually, not UNION ALL** — The cross-stage UNION ALL query (event_log + viewability_log + impression_log) forces BQ to process all three in one job. Running them as three separate queries lets you skip tables early (e.g., if viewability_log returns 0 rows, you save time on interpretation, and individual queries may get better slot allocation).

Some `sqlmesh__logdata` tables are themselves VIEWs referencing other datasets:
- `bid_attempted_log` and `bid_events_log` → both reference `bidder_bid_events` (same data, different filters)
- `bid_logs` → references Beeswax bid_logs upstream
- `auction_log` → references `v_augmentor_log`
- `win_logs` → references Beeswax win_logs upstream
- `icloud_vv_log` → references icloud_vv table upstream

### Slot Contention — Never Run Large Queries Simultaneously

The adhoc BQ reservation has limited slots. Running two 4+ TB queries concurrently causes 3-5x runtime inflation (queries that take 2-5 min solo take 12+ hours concurrent). Always run large queries sequentially.

**Learned from TI-650 (2026-03-18):** Two 18 TB queries run simultaneously took 12+ hours each instead of ~2 hours solo. The adhoc reservation does not auto-scale — concurrent jobs compete for the same fixed slot pool.

**Rule:** One large query at a time. Queue the next after the previous completes. For small queries (<1 TB), concurrent execution is fine.

### Upstream Source Systems
- **Postgres (coredw)**: impression_log, click_log, clickpass_log, event_log, conversion_log,
  guid_log, viewability_log come from Postgres. Evidence: `conversion_log` has `inet` fields
  cast to STRING, and clickpass_log comments note "doesn't exist in coredw."
- **Bidder service**: bidder_bid_events, bidder_auction_events are written directly by the bidder.
- **Beeswax exchange**: bid_logs, win_logs come from Beeswax (external DSP).
- **Spend pipeline**: spend_log is produced by the spend/billing pipeline from bidder events + wins.

---

## Business Logic

### is_new Column
`is_new = TRUE` means the guid/cookie is a **first-time visitor to that advertiser's site**
(i.e., no prior recorded page view for that advertiser_id + guid combo). Appears in:
clickpass_log, guid_log, cost_impression_log, visits, ui_visits.

### visit_facts vs visits vs ui_visits
- `visits` (silver.summarydata): row-level, one row per visit event. Partitioned by time (DAY).
- `ui_visits` (silver.summarydata): row-level VIEW on top of `visits`, used by the UI.
  Same schema as visits but with added computed fields (visit_day, source_type, etc.).
  Note: `ip` column has a known upstream bug — two ip fields exist temporarily.
- `visit_facts` (silver.summarydata): **pre-aggregated**, one row per advertiser+campaign+geo+device+hour.
  Use for performance reporting. Partition: DAY on `hour`, cluster: advertiser_id, campaign_id.

### conversions vs conversion_facts
- `conversions`: row-level, one row per conversion event. 60-day rolling retention.
- `conversion_facts`: pre-aggregated by advertiser+campaign+geo+hour. Use for reporting.

### all_facts VIEW
`summarydata.all_facts` is the kitchen-sink reporting view: joins visit_facts + conversion_facts
+ spend_facts + more. Very wide (150+ columns). Use only when you need cross-metric analysis.
Prefer the individual facts tables when possible (faster, cheaper).

### competing_ prefixed columns
Columns prefixed with `competing_` in visit_facts and conversion_facts represent metrics for
impressions where the advertiser was NOT the attributed touch (i.e., they were "competing"
for credit). Used in incremental attribution analysis.

### Per-advertiser CVR can exceed 100% (multi-touch attribution artifact)
When computing per-advertiser CVR = `conversions / visits` from the standard summarydata facts
(`conversion_facts.click_conversions + view_conversions + competing_view_conversions` divided
by `visit_facts.clicks + views + competing_views`), you can legitimately get CVR > 1.0
(>100%) for some advertisers. **This is NOT a bug** — it's the result of multi-touch
attribution counting the same conversion against multiple advertiser-visits across the
funnel, while visits are counted once per advertiser-visit.

Empirically observed (BQ run 2026-06-10 over 2026-05-06 → 2026-06-08, MM-prospecting only):

| Advertiser | Vertical | Per-advertiser CVR | Notes |
|---|---|---|---|
| **Rafi Law Group** | Legal Services | **1349%** | Extreme MTA — small visit volume with re-credited conversions |
| **Station Casinos** | Hotels/Resorts | 108% | Casino/hospitality advertisers tend to have MTA-heavy patterns |
| **Angi (32766)** | Home Services & Repairs | **207%** | Top-25 advertiser by impressions; MTA dominates |
| **Mountain Mike's Pizza** | Fast Casual Dining | 62% | Restaurant verticals often >50% |
| **SpotHero, Inc.** | Live Music & Comedy | 48% | High MTA via competing_view_conversions |
| **LongHorn Steakhouse** | Casual Dining | 44% | |
| **Goldfish Swim School** | Fitness Studios | 30% | |
| **Global X ETFs** | Investments | 24% | |

**Practical implications for any analysis that uses per-advertiser CVR:**

1. **Leave-one-out / leverage analyses on control composition** (see [TI-961 control composition diagnostic](../tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py)): these advertisers will dominate the pool's CVR if not winsorized. Single-advertiser leave-one-out impact on a 1000-advertiser pool can exceed 0.5pp.
2. **CUPED / variance-reduction methods** that estimate `θ = Cov(post, pre) / Var(pre)` on rates will have θ dominated by these advertisers. Winsorize per-advertiser rates at the 99th percentile (or apply a physical cap like CVR ≤ 50%) before computing θ.
3. **Don't filter these out blindly** — they're real advertisers spending real money. The MTA-amplified CVR reflects how the attribution model credits their conversions across the funnel; it's not a data-quality issue to "clean up." Apply winsorization at the analysis layer for noise control, not at the data layer as a filter.
4. **For dashboards / business reporting**, prefer the `summarydata.sum_by_advertiser_by_day` aggregates which use the standard attribution model; for incrementality / lift analyses where you care about advertiser-level rates, be explicit about winsorization in the methodology.

Discovered 2026-06-10 via TI-961 leverage diagnostic — Tier 5 (Fangorn-off) pool CVR appeared structurally inflated at 6.5% vs treated tiers at 2-4%; root cause was a handful of these MTA-heavy advertisers dragging the pooled rate up.

### Fangorn-state identification — BQ proxy vs Postgres authoritative
Identifying which advertisers currently have Fangorn audience scoring ON:
- **Authoritative source:** `tpa.fangorn_advertiser_inclusion` (Postgres, accessed via Databricks JDBC). One row per advertiser with `fangorn_rollout_tier_num` and `fangorn_advertiser_inclusion_date`. Postgres-only — no BQ mirror exists.
- **BQ proxy (use when Postgres isn't reachable):** advertisers with at least one DS46 reference in `audience_audience_segments.expression` are Fangorn-on. See [data_catalog.md](data_catalog.md) §audience_audience_segments for the canonical SQL pattern.
- **Caveat:** the BQ proxy lags by recent flips — BQ snapshot 2026-06-10 showed 464 distinct DS46-overlay advertisers vs Postgres' 770 advertisers across Tiers 1-4. Most likely missing the Tier 4 cohort that flipped on 2026-06-04. For analyses that require exact tier identification (especially tier-level treatment effect estimation), always use Postgres.
- **The audience-overlay mechanism** itself: Fangorn switch updates `audience_audience_segments` (per-segment overlay) to add DS46 references, while leaving the base `audience_audiences.expression` (per-audience top-level) showing DS13/DS19 (MM). So DS46 in segments → Fangorn-on, DS13/DS19 in segments → MM/non-Fangorn. See [reference_fangorn_audience_overlay](../../.claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory/reference_fangorn_audience_overlay.md).

### probattr_ prefixed columns
Columns prefixed with `probattr_` = probabilistic attribution model metrics, as opposed to
deterministic last-touch or last-tv-touch attribution.

`summarydata.sum_by_*_by_day` (advertiser / campaign_group / campaign) exposes 20 `probattr_*`
columns: `probattr_views`, `probattr_view_conversions`, `probattr_view_order_value`,
`probattr_site_visitors`, `probattr_new_site_visitors`, `probattr_existing_site_visitors`,
`probattr_new_visitors`, `probattr_last_touch_views`, `probattr_last_touch_view_conversions`,
`probattr_last_touch_view_order_value`, plus `probattr_competing_*` mirrors of each. These are
the right surface for ROAS/CPA-style modeling that needs probabilistically-attributed (rather
than deterministic last-touch) outcome signals. Discovered TI-832 (2026-05-05).

### sum_by_*_by_day post-BQ-migration column drops (TI-832, 2026-05-05)
On `dw-main-silver.summarydata.sum_by_advertiser_by_day`, `sum_by_campaign_group_by_day`, and
`sum_by_campaign_by_day`, the BQ migration **dropped** the cost-side duplicates of the spend
columns:
- `data_cost`, `fee_cost`, `partner_cost` — gone from all three views
- `legacy_spend` — gone from `sum_by_campaign_group_by_day`

The `*_spend` columns (`media_spend`, `data_spend`, `platform_spend`) remain and replace them.
Any code with `SELECT *` followed by an explicit `*_cost` projection will crash. Caught
3 prod failures in `feature_store_setup_model` DAG (`summary_advertiser_id`,
`summary_campaign_group_id`, `summary_campaign_id`).

`sum_by_advertiser_by_day` also exposes `raw_*` un-attributed metrics not present on the
campaign / campaign_group views: `raw_conversions`, `raw_order_value`, `raw_visits`,
`raw_new_site_visitors`, `raw_existing_site_visitors`. Plus `new_to_file` and `visitors`.

### conversion_log: silver SQLMesh hides bronze.raw corruption (TI-832, 2026-05-06; mechanism corrected 2026-07-08)
Bronze (`dw-main-bronze.raw.conversion_log`) and silver (`dw-main-silver.logdata.conversion_log`)
diverge on **amounts, not rows**: the silver view wraps `order_amt`/`order_amt_usd` in
`CASE WHEN abs(...) >= 100000000 THEN NULL` — **any per-row amount ≥ $100M is NULLed but the row
is KEPT** (verified 2026-07-08: Harley May–Jun 2026 bronze 8,619 rows = silver 8,619 rows, all
silver `order_amt` NULL). The earlier "silver strips these rows" reading was imprecise — the
original TI-832 evidence (bronze 7,366 rows with `order_amt > $1B`, silver 0 rows *matching that
amount filter*) is consistent with NULLing: the rows are in silver, they just no longer match an
amount predicate. Row-count analyses are bronze≡silver; amount aggregates are not — a ≥$100M
injected fire shows in silver as an amount-coverage DROP, never a sum spike.

The 4 advertisers with corrupt `order_amt`:
- **34957 Harley Mid Funnel (Agency: MediaHub)** — 5,499 rows, ~$1.78T cluster (magnitude
  ≈ ms-since-epoch for ~April 2026 — pixel writing a timestamp into `order_amt`)
- **33903 Bioharvest Ltd** — 1,107 rows, ~$7.4T cluster (different encoding bug)
- **32023 Tarte** — 759 rows, ~$6.5T cluster, only **1 distinct order_id** firing 759 times
  across 64 IPs (pixel retransmission on top of corrupt amount)
- **63746 Networking Today** — 1 row, $1.21B (one-off)

**Implications:**
1. Anything that aggregates `order_amt` from the GCS parquet sink (e.g., `conv_log_ip`
   feature store model, anything reading the parquet directly) will surface these extreme
   values. Always plan training-time outlier handling (Matt's V2 trims top 1%).
2. Anything that queries silver via BQ won't see them — the issue looks invisible from
   the most-common entry point. **For data quality investigations on conversion_log, query
   bronze.raw, not silver.**
3. Pixel ops owns conversion-pixel integration: route to **Ashley Pineda Varela** (per Zach
   2026-05-06). Surfaced via TI-832 outlier sheet.

### Conversion pixel payload anatomy — how order_amt/order_id/conversion_type are born (WGU-REV, 2026-07-08)
`conversion_log.query` (JSON) holds the raw pixel GET params; three of them are the direct source of the
structured columns:
- **`shoamt`** → `order_amt`. The ingest parser (upstream of bronze — bronze `order_amt` is already
  final) **digit-extracts** whatever string arrives: `shoamt=1` → 1; `"' and 6957=6964--"` → 69,576,964;
  a no-digit string like the literal unreplaced macro `"ORDER AMOUNT"` → NULL. There is NO validation —
  garbage with digits becomes plausible-looking revenue (this is how the WGU pentest polluted Feb 2026).
- **`shoid`** → `order_id`. Advertisers often send a page slug or a constant (WGU: `lead`,
  `application-status`) — do not assume it's a transaction ID.
- **`type`** → `conversion_type`. Absent param → NULL in conversion_log, but **`ui_conversions` renders
  NULL conversion_type as the string sentinel `'-101'`** (matches the `core_advertiser_conversion_types`
  sentinel row) — never filter `conversion_type IS NULL` in ui_conversions.

**Amount guardrails, empirically bounded (WGU pentest rows, Feb 2026):** silver SQLMesh nulls `order_amt`
(keeps the row) somewhere between $69.6M (kept) and $621.6M (stripped); the attribution layer
(`ui_conversions`) additionally drops amounts somewhere between $590,132 (kept) and $5,736,771 (dropped —
plausibly a ~$1M cap; mechanism/location not found). So bronze ⊃ silver ⊃ ui_conversions on extreme amounts.

**Retention corrections (verified 2026-07-08):** `bronze.raw.conversion_log` holds ~**9 months** (WGU rows
begin mid-Oct 2025; Sep 2025 fully absent) — NOT a 10–90d TTL. Silver `conversion_log` floor ≈ **2024-01**
(no partitions exist before that). Any bronze-vs-silver comparison older than ~9 months is impossible.

### Detecting an advertiser pixel/tag change from OUR side (playbook, WGU-REV 2026-07-08)
We can't see the client's tag manager (per Kevin Cipriani, changes happen in Adobe Launch/GTM we have no
access to) — but every fire lands in conversion_log with its full payload, so changes are reconstructable
from the receiving side. Escalating steps, each pins one dimension:
1. **Date new event types** — `core_advertiser_conversion_types` WHERE advertiser_id=X ORDER BY create_time.
   Auto-registered at FIRST fire (verified to the second; invariant re-verified platform-wide 2026-07-08 —
   only 2/40,437 in-window registrations lack a same-month fire, both source-31 offline batch regs).
   **Exclude sentinels with `NOT REGEXP_CONTAINS(conversion_type, r'^-[0-9]+$')`** — SIX platform
   pseudo-types exist (`-100/-101/-102/-105/-106/-107`), not just the two documented ones; the regex matches
   exactly those six and no genuine/pentest type. Junk/SQLi strings here = pixel fuzzing/pentest.
2. **Monthly shape scan** — conversion_log per month: COUNT, DISTINCT ip, COUNTIF(order_amt IS NOT NULL),
   SUM(order_amt), n_types. Red flags: `sum == count` ($1 placeholder — report the LAST matching month, the
   era's end, not the first), `sum = 0` with n_amt>0 ($0-placeholder variant — coverage stays 100%),
   n_amt→0 (amount param broke), rows 2–3× at flat spend (firing scope changed), a SUM spike at flat n_amt
   (injected amounts — silver keeps sub-$100M fakes, e.g. WGU Feb'26 $222.9M), rows/ips ratio jump
   (single-IP flood/pentest), n_types spike (test/pentest). **Scaffold the months (GENERATE_DATE_ARRAY),
   don't GROUP BY alone** — zero-fire months emit no row, so a TOTAL tag stop (the worst failure) is
   invisible and adjacent-row deltas silently straddle gaps; 7.8% of advertisers have ≥1 mid-history
   zero-fire month (audit 2026-07-08). Month-truncate scan bounds — partial boundary months fabricate
   volume steps.
3. **Daily zoom** on the suspect window by IFNULL(conversion_type,'<NULL>') → exact cutover day + overlap.
4. **Page breakdown** — NET.HOST(referer) + `REGEXP_EXTRACT(referer, r'[?&]step=([^&]+)')` + path, pre vs
   post → where each tag fires; also surfaces stage/dev/qa hosts counted as conversions.
5. **Payload diff** — TO_JSON_STRING(query) samples pre vs post → which params changed (type/shoamt/shoid),
   unfilled template macros (literal `shoamt=ORDER AMOUNT`), pixel host (`px.steelhousemedia.com` = legacy
   SteelHouse-era tag vs `px.mountain.com` = current).
6. **Rogue/legacy-AID sweep** — GROUP BY advertiser_id over `NET.HOST(referer) LIKE '%<domain>%'` for one
   month, then check each AID exists in `integrationprod.advertisers`. No row = dead account; its fires are
   dark (no attribution, no reporting) — how WGU's lead event went missing (AID 10942).
7. **Rule out MNTN-side** — core_pixel_integrations update_time, archives_advertiser_setting_archives,
   VV windows via archives_advertiser_archives (live `advertisers.conversion_window` is a STRING in
   HOURS, '720:00:00' = 30d — normalize before comparing to the archive's day-grain interval).

### WGU (31357) "revenue" — it was NEVER real, and the Sep-Oct 2025 cliff is a client-side retag (WGU-REV, 2026-07-08)
Full investigation of the "WGU revenue → 0 after Sep 2025" chart (TI-1037 dashboard, prospecting obj=1/fl=1
scope). Key facts, all verified in BQ:
1. **The pre-cliff "revenue" was a $1-per-lead placeholder**: WGU's old untyped pixel (conversion_type NULL,
   fired on the `inquiryv4.wgu.edu` inquiry form) hardcoded `shoid=lead&shoamt=1`. For ALL queryable history
   (2024-01 → 2025-10-02), `order_amt=1` is the only value, and `sum_by_campaign_by_day` revenue ==
   conversions EXACTLY every month. WGU dashboard "Revenue" has never represented dollars.
2. **Cutover = client-side tag replacement, 2025-09-30 → 10-02 (3-day overlap)**: new
   `conversion_type='app_submitted'` auto-registered 2025-09-30 22:57:04; last $1 flowed 2025-10-02. Zero
   MNTN-side config changes (pixel_integrations untouched since 2020; settings archive quiet; source id 23
   unchanged). The new tag ships the **literal unreplaced macro `shoamt=ORDER%20AMOUNT`** on 100% of rows
   (all 57,785 Jun 2026 rows) → order_amt NULL → revenue $0 from 2025-10-03.
3. **Event semantics changed too — the bigger integrity issue**: `app_submitted` fires on `apply.wgu.edu`
   portal PAGEVIEWS (application-status checks 28K/mo, transcript requests 2K/mo, form steps), not just
   submissions. Attributed conversions jumped 3.2× overnight (ui_conversions 8,226 Sep → 25,935 Oct 2025).
   WGU has **no ROAS goals** (54+ CPA goal groups, $2–600), so $0 revenue breaks nothing operationally —
   but any **CPA/conversion trend spanning Oct 2025 is apples-to-oranges** (denominator = leads before,
   portal pageviews after).
4. **Feb 2026 $833,883.40 "revenue" = pentest pollution**: Burp Suite scan (callbacks to oastify.com) from
   single IP 136.60.22.42 on 2026-02-07 fuzzed the pixel on `apply.wgu.edu/duplicate` → 75 junk injection
   conversion_types + 34 digit-extracted fake amounts (bronze $71.68T → silver $222.9M → attributed
   $833,883.40, reconciles to the dollar). Confined to obj=4 retargeting campaigns; the prospecting chart
   shows $0 for Feb. Treat as fake; whether the pentest was WGU-sanctioned is unknown (→ Pixel Ops).
5. **May 2026 volume spike (125,940 rows) = untyped landing-page pixel burst**: NULL-type pixel deployed on
   `www.wgu.edu` lead LPs 2026-04-30 → 05-16 (~4.4–5.4K real distinct-IP rows/day, IPs≈guids≈0.93×rows, not
   refires), then scoped down to ~100–300/day. **RESURGED 2026-06-24** (805 fires) → ~1,800–2,500/day from
   06-25, still running as of 2026-07-08 — confirms CS pixel-QA internal note ("increased raw conversions
   returned from 6/24 to present"); both windows = the same untyped LP tag cycling on/off in WGU's Adobe
   Launch. Timing of the Apr 30 onset correlated with orca-integration (2026-04-29) + Tealium CRM mappings
   (2026-04-30) onboarding — causality unverified.
6. **Non-prod pollution**: ~692 Jun 2026 "conversions" from `apply.stage/development/local.wgu.edu` +
   `inquiryv4.qa.wgu.edu` referers count as real conversions. Platform-wide filter question open.
7. **WGU's core LEAD event now reports to a DEAD advertiser ID (10942)** — found 2026-07-08 after a CS
   pixel QA flagged an "AID 10942" pixel. AID 10942 has NO row in `integrationprod.advertisers` (orphaned/
   legacy account), yet its pixel fires at scale on WGU's site: Jun 2026 = 18,016 fires / 16,865 IPs on
   `inquiryv4.wgu.edu/?step=whatAreYourGoals` (the exact page where 31357's lead pixel lived pre-retag)
   + 3,727 on the older `inquiry.wgu.edu` form. It is the legacy SteelHouse-era tag: `shaid=10942`,
   hardcoded `shoamt=1`, no `type`/`shoid`, `mnthst=px.steelhousemedia.com` (new 31357 tag uses
   `px.mountain.com`), and passes `shopid=<Salesforce record id>` per lead. History: ~4K/mo on
   inquiry.wgu.edu Jan–Aug 2025 → expanded at the Sep-2025 retag (19.5K Sep, spread to apply.* + stage/dev)
   → ~18–24K/mo since. **≈226K fires since Jan 2025 land on a nonexistent account: no attribution, no
   reporting — WGU's lead conversions are dark, not gone.** Rows exist in conversion_log under 10942 if
   historical recovery is ever wanted. Meanwhile 31357's `lead` order_id is a residual ~114/mo.
Neither pixel version ever sent email/phone. Route pixel-ingest validation gap + pentest question to
**Ashley Pineda Varela** (Pixel Ops). Related: § "WGU (31357) YoY comparisons are confounded by two 2025
tracking breaks" (adds the **Jul 2025 visit-tracking step** — visits re-based ~1.2%→2.2% IVR, separate from
this conversion-pixel story) and the pixel-registry forensics section (core_advertiser_conversion_types
sentinels, data_sources id 23). WGU stance (Imani Clark, 2026-07-08): **lead-focused — revenue tracking is
not a WGU priority**; open items are event taxonomy (lead vs app-submitted vs status-check), re-pointing the
10942 lead pixel to 31357, and CPA-goal resets when firing scope is fixed.

### attribution_model_id
- `attribution_model_type_id = 0` should be treated as `1` (last-touch) — known business rule.
  See `ui_visits` column description comment.
- Present in clickpass_log, ui_visits, ui_conversions, conversions, visits.
- Distinct values for advertiser 37775 (7-day sample): 1, 2, 3, 9, 10, 11. Model 9 most common (49%).

### guid (user/device cookie ID)
- Present in every log table: clickpass_log, event_log, cost_impression_log, impression_log, ui_visits, viewability_log, etc.
- Persists across multiple VVs — same user/device across visits. Top user: 123 VVs in 7 days (advertiser 37775).
- clickpass_log.guid matches ui_visits.guid 99.99% (same session context).
- clickpass_log.guid matches event_log.guid ~60% (different context — impression vs visit).
- `original_guid` (clickpass_log only) = pre-reattribution guid. Differs from guid in ~16% of VVs.
- `page_view_guid` (clickpass_log only) = GUID from page view signal.
- **guid as S1 resolution key (v11):** ~18-23% of previously unresolved S2/S3 VVs can be linked to an S1 VV via matching guid (same user at different IP). Additional ~7-9% can be linked to an S1 impression via guid. guid-based tiers are the highest-impact new resolution paths after IP-based methods.

### viewability_log and VV IP lineage
- viewability_log has ad_served_id, ip, bid_ip, guid, campaign_id, time — same schema as CIL for IP purposes.
- **Not useful for S1 resolution:** For advertiser 37775, zero S1 impressions exist in viewability_log. CIL already covers display impressions comprehensively.
- Zach suggested investigating it for display viewable inventory IPs, but empirical check shows no incremental coverage.
- **Multiple rows per ad_served_id (display only):** `viewability_type_id` 1=measurable, 2=viewable. Each display impression produces two viewability events. CTV does NOT go through viewability_log — uses event_log VAST events instead. When joining, either GROUP BY to dedup or keep both rows if you need the type breakdown.
- **viewability_log is the display equivalent of event_log:** Use it to trace viewable display impressions in the pipeline (clickpass → viewability_log → win_logs → bid_logs).

### pa_model_id
Probabilistic attribution model ID. Present in visit_facts, conversion_facts, visits, ui_visits.

### Epoch Units (CRITICAL — varies by table)
| Table | epoch column | Unit |
|-------|-------------|------|
| spend_log | auction_epoch | **nanoseconds** |
| bidder_bid_events | auction_epoch | **microseconds** |
| bidder_bid_events | epoch | **milliseconds** |
| bidder_auction_events | epoch | (milliseconds, not explicitly documented) |
| impression_log | epoch | (seconds — inherited from Postgres) |
| v_augmentor_log | epoch | **milliseconds** |
| clickpass_log | epoch | (seconds) |
Always verify epoch units before using for time math.

### spend_log.auction_id vs exchange_auction_id
- `auction_id` = `<exchange_id>.<auction_id>` — MNTN's composite identifier (also called mntn_auction_id)
- `exchange_auction_id` = the raw auction ID from the exchange itself

### device_type ENUM
Valid device_type values are defined in `dw-main-bronze.integrationprod.device_type`.
Always join there for human-readable labels.

### bidder_bid_events vs bid_attempted_log vs bid_events_log
All three expose the same underlying `bidder_bid_events` table:
- `bid_attempted_log` = attempted bids (may or may not have won)
- `bid_events_log` = same view as bid_attempted_log (currently identical, may have filter differences)
- Use `spend_log` for **won** impressions with cost data.

### partner_id
Appears in spend_log, bidder_bid_events, bidder_auction_events:
- Describes which exchange partner (Beeswax vs MNTN bidder)

### is_test Flag
`is_test = TRUE` in spend_log and bidder_auction_events means test/QA auctions — **exclude from
production analysis**.

**GOTCHA (TI-504):** `is_test` campaigns have structurally lower IVR — **8-53% of production campaigns even within the same intent tier**. Cannot compare test vs non-test campaign performance directly. Use within-test-group comparisons (control vs treatment) for experiments. The cause is unknown but likely involves delivery priority, creative/budget differences, or bidder behavior differences for test campaigns.

### advertiser_household_score (HHST)
Available on `cost_impression_log`. Segments impressions by intent tier:
- **HI (High Intent):** HHST >= 6666
- **MI (Mid Intent):** HHST 3333-6665
- **Max Reach / PP:** HHST 1-3332

Old prospecting campaigns serve **89-99% HI tier** traffic. Virtually no MI/PP historical baseline exists for prospecting campaigns.

### ip vs ip_raw vs original_ip vs bid_ip vs impression_ip

Zach explained the full IP column taxonomy on 2026-02-25 call and confirmed in docx review 2026-03-03:

| Column | Present in | What it is |
|--------|-----------|------------|
| `ip` | most tables | **The IP used for all logic** — enriched/preferred IP. VV tracing, targeting, and geo all use this. |
| `ip_raw` | clickpass_log, event_log, others | Raw IP before MNTN enrichment. Usually identical to `ip`. |
| `original_ip` | event_log, cost_impression_log, others | Pre-iCloud Private Relay IP — raw TCP connection IP from x-forwarded-for header. MNTN overrides this with a more accurate device IP stored in `ip`. Use `ip` for analysis; `original_ip` for audit/debug only. |
| `bid_ip` | event_log, click_log, impression_log | IP at bid/auction time for the associated impression. In event_log, `bid_ip` = win_log.ip = cost_impression_log.ip at 100% (validated 30,502 rows, TI-650). The gold column for IP lineage — no need to join win_log or CIL. **cost_impression_log.ip IS the bid_ip** — confirmed by joining CIL to impression_log on impression_id=ttd_impression_id: 794,050/794,050 (100%) match bid_ip; only 745,169 (93.8%) match render_ip. When they differ, render_ip is internal (10.x.x.x NAT/proxy), CIL.ip = public bid_ip. CIL also has `advertiser_id` (impression_log does not), making it far cheaper to query for single-advertiser analysis. |
| `impression_ip` | ui_visits | Bid IP carried forward from impression_log onto the visit record. Matches event_log.bid_ip at 95.8–100% (mismatch ~2–4% for CTV-heavy advertisers where impression_ip may reference a different impression than last-touch ad_served_id). Fallback for non-CTV VVs where event_log has no row. |

**Rule:** Use `ip` for analysis. `bid_ip` to trace back to bid time. `impression_ip` as non-CTV fallback. `original_ip` only for pre-relay audit.

### icloud_ tables
`icloud_vv_log`, `icloud_guids`, `icloud_ipv4`, `icloud_ipv6` relate to Apple iCloud Private Relay
traffic handling. IPs from iCloud relay require special treatment for geo-targeting.

---

## Data Quality Notes

### Known Issues
1. **ui_visits.ip**: upstream bug — two IP columns (ip + ip_raw) present. Comment says "revert once ip is fixed upstream."
2. **clickpass_log.first_touch_time**: "doesn't exist in coredw, but keep it just in case" — unreliable, may be NULL.
3. **conversion_log._col_23**: unnamed JSON column (raw artifact from Postgres migration).
4. **cost_impression_log.recency_elapsed_time**: INTERVAL type — BQ doesn't support INTERVAL in all contexts.
5. **clickpass_log.first_touch_ad_served_id NULL (~40%)**: The lookup for `first_touch_ad_served_id` requires a CTV impression with `funnel_level=1` and `objective_id=1` from the same campaign group. The system searches on **both bid_ip AND ip of the attributable impression** (Sharad, 2026-03-04). Open question: does "both" mean OR (either match) or AND (both must match)? A9a results show ft_null is 54.85% when mutated vs 38.19% when not (+16.66pp delta), but mutation explains only ~15% of NULLs. Sharad: *"The fact that we are not able to find such records for a high number of VVs points to some issue in the targeting."* The 40% NULL rate is a known problem, not a design choice.

5b. **FT vs LT is lens-invariant at the ADVERTISER TOTAL (AUDI-1070, 2026-06-30):** `clickpass_log` has exactly **one row per visit**; `first_touch_ad_served_id` vs `ad_served_id` only re-route *which campaign / funnel-stage* gets credit. So FT vs LT does **NOT** change advertiser-total visit counts, VR, or total ROAS — the difference shows up only at the **campaign / funnel-stage** grain (FT pushes credit onto the S1 prospecting impression). When a stakeholder reports a "FT vs LT" gap at the account level, it's almost always a **window or lookback** difference, not the attribution lens per se.

5c. **`sum_by_advertiser_by_day` has NO first-touch column (AUDI-1070):** its headline cols (`views`, `view_conversions`, `view_order_value`, `raw_visits`) ≈ the `last_touch_*` cols (verified <0.1% diff). The table does **not** honor `reporting_style` — the client UI applies First-Touch via a *separate* attribution engine. You cannot reproduce a client's FT number from this table; a true FT series requires a log rebuild (e.g., TI-650 `audit.vv_ip_lineage`, which is ephemeral, not a live table). `first_touch_time` is unusable (NULL pre-2026, epoch-zero garbage after).

5d. **Platform-wide FT-attribution regime change in 2024-Q3→Q4 (AUDI-1070):** `ft_eq_lt` (share of visits where first-touch impression = last-touch impression) **collapsed simultaneously across unrelated advertisers** between Q2 and Q4 2024 — e.g. Avon 88.6%→3.4%, HexClad 100%→52.7% (→13.9% by 26-Q1); FT-null jumped from ~0.6% to ~65-75%. Because it hit unrelated advertisers on the same calendar boundary, it's a **platform engine change** (multi-touch journeys became the norm / FT semantics changed), not advertiser behavior. **Consequence for client YoY:** any **client-UI (FT)** comparison of **2024 vs 2025** straddles this break — pre-break FT≈LT (high ROAS), post-break FT diverges to credit S1 prospecting (mechanically lower ROAS for prospecting-heavy CTV). A chunk of perceived FT "decline" is this lens shift, not performance. Always hold attribution lens AND window constant across both years of any client-facing YoY.

5e. **Client UI / API numbers come from CHAPI → ClickHouse, NOT from BigQuery (AUDI-1070, Lauren Gregg / Lilit 2026-06-30).** The advertiser-facing Reporting UI (and the `/data` API) is served by **CHAPI** (`github.com/SteelHouse/chapi`, `make run` → `curl localhost:9000/data` with advertiser key + `nodatatiercheck` admin key) querying **ClickHouse**. It's an elaborate query builder — to verify a client number, run CHAPI locally or query prod/qa ClickHouse; **do not try to rebuild it in BQ**. Measurement team owns the authoritative coredb/BQ source tables. **Conversions in CHAPI use last-touch + last-TV-touch ONLY (Lilit, Measurement) — there is NO first-touch conversion table.** The FT/LT (`reporting_style`) lens applies to **visits**, not conversions ⇒ revenue and ROAS are last-touch in every system, both years; the FT switch does not move AID-total revenue/ROAS. **⚠️ PARTIALLY SUPERSEDED (see §5h): the "conversions are last-touch/last-tv-touch only, no first-touch conversion table" part is CORRECT and schema-confirmed; but "does not move AID revenue/ROAS" is WRONG — `industry_standard` adds `competing_*` to conversions+order-value, moving Avon prospecting ROAS 17.3→22.1.**

5f. **The UI "Total Verified Visits" = `clickpass_log` raw ROW count, not `sum_by_advertiser` views+clicks (AUDI-1070).** For Avon (31921) Jan–May: UI VV 692,888 / 598,436 ≈ `clickpass_log` `COUNT(*)` 686,963 / 591,016 (**~99% match**); the `sum_by_advertiser_by_day` headline `views+clicks` is only 526,929 / 443,049 (tighter last-touch dedup); distinct `page_view_guid` is just 252,813 / 240,267 (clickpass carries ~2.7 attribution rows per page view). **CHAPI counts attributed visit-touch rows, so it runs ~1.276× the `sum_by_advertiser` rollup on visits/conversions/revenue/ROAS — and that factor is STABLE across years, so it CANCELS in YoY.** Practical rule: a naive `sum_by_advertiser` pull will not match the client's level, but it reproduces the client's YoY direction/magnitude (Avon ROAS +19% in both UI 22.12→26.36 and rollup 17.33→20.68). `conversion_log` raw is the un-attributed firehose (Avon 171K orders / $8.7M ≈ 6.8× attributed) — must be attribution-joined before comparing to the UI. **Mechanism — CORRECTED 2026-06-30 (supersedes earlier "CTV" draft; see §5e-bis):** that factor is the **`competing_*` credit** the `industry_standard` reporting style ADDS to last-touch — NOT CTV/`last_tv_touch`. Reproduced EXACTLY in BQ `all_facts` via last-touch + `competing_*`, **both years** (VV 692,888 / 598,436 EXACT; ROAS 22.1 / 26.4 EXACT). The lt+tv "+CTV" mechanism an earlier version of this line claimed was wrong. **⚠️ LABEL SUPERSEDED (see §5h): `competing_*` is NOT "first-touch" — it is a competitive-scenario credit orthogonal to touch-order (`competing_last_touch_*` exists). The math here is correct; the "(FIRST-TOUCH)" tag was a misnomer.**

5e-bis. **CHAPI metadata XML proves the UI headline is FIRST-TOUCH-INCLUSIVE (`competing_*`), reproducible in BQ `summarydata.all_facts` (CHAPI codebase read + BQ recon, 2026-06-30).** Reading SteelHouse/chapi's metric registry (`src/main/resources/r2-metadata.xml` = "new"/industry_standard; `r2-metadata-legacy.xml` = "legacy"/last_touch; chosen per-request by `advertiser_settings.reporting_style` via `AdvertiserFilter.kt`→`Metadata.set`). The Graph (AID-wide) table → ClickHouse `summarydata.all_facts_by_day_ramp_combined` = copy of BQ `dw-main-silver.summarydata.all_facts` (same 179 physical cols). In the **new** XML the headline metrics ADD first-touch (`competing_*`) to last-touch: `Views = views + competing_views`; `ViewConversions = view_conversions + competing_view_conversions`; `ViewOrderValue = view_order_value + competing_view_order_value`. The rendered sample query (`docs/sample-clickhouse-query.sql`, AID 37963) literally emits `sum(clicks)+(sum(views)+sum(competing_views))` for Verified Visits and the matching `competing_*` adds for conv/OV. **BQ recon for Avon (31921), Jan 1–Jun 1 2025, AID-wide, hour∈[01-01,06-01):** PA/first-touch form → **VV 692,888 (UI 692,888 EXACT)**, ROAS **22.09** (UI 22.12), ConvRate **4.41%** (UI 4.42%), CPA **$2.39** (UI $2.39), Spend **$73,077.84** (UI $73,077.81), Households Reached HLL **2,653,138** (UI 2,677,801, ~0.9% HLL-algo+refresh). The plain **last-touch** form gives VV **526,929** / ROAS **17.33** / ConvRate 4.55% — does NOT match. ⇒ **Avon is on `reporting_style='new'/'industry_standard'`, and the FT switch DOES move AID-total revenue/ROAS** (22.1 PA vs 17.3 last-touch). **This DIRECTLY CONTRADICTS the verbal AUDI-1070 note in 5e/§2 below** ("conversions last-touch+last-TV-touch only; FT does not move AID revenue/ROAS"; "+CTV" mechanism). On the actual CHAPI columns the bridge knob is `competing_*` (first-touch), NOT `last_tv_touch_*` — CHAPI's generated UI SQL has **0 references** to `last_tv_touch_*`. NOTE: BQ `all_facts` has **no `offline_primary_*` columns** (present in ClickHouse, ~0 for Avon, drop in BQ) and time column is **`hour` (DATETIME)**, not `day`. Prospecting scope = `objective_id IN (1,5,6)` (CHAPI `marketing_objective_id` alias collapses 5/6/7→1; 7/Ego ~0 vol for Avon) → VV **272,218 (UI 272,218 EXACT)**, spend $56,813 (UI ~$56,833). Reach: `HLL_COUNT.MERGE(uniques)` (BYTES sketch, cheap) = 2,653,138; `COUNT(DISTINCT UNNEST(uniques_arr))` = 2,659,958 — both ~0.3–0.9% under UI (engine-dependent, never bit-exact). **✅ RESOLVED 2026-06-30 (independent re-verification, BOTH years):** reproduced 2026 too — VV **598,436 EXACT**, ROAS **26.36 EXACT**, CPA **$2.03** / CVR **5.26%** EXACT — via last-touch + `competing_*`. The code (`competing_*`) is correct; the verbal "last-TV-touch" note in **5e is superseded** (Lilit was describing the conversion→impression attribution event layer, not the reporting column selection CHAPI sums for the headline). Reproduction query: `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/queries/avon_chapi_exact_reproduction.sql`. **⚠️ The "first-touch" LABEL used throughout this note is a MISNOMER — `competing_*` is NOT first-touch. See §5h (authoritative).**

5h. **FINAL reconciliation — `industry_standard` = last-touch + `competing_*`; "first-touch" is a MISNOMER for it (AUDI-1070, Lilit/Measurement + schema proof, 2026-07-01).** Supersedes the "first-touch" LABEL in §5e-bis/§5f and the "revenue doesn't move" claim in §5e. Settled empirically on `dw-main-silver.summarydata.all_facts` (180 cols):
   - **`competing_*` is ORTHOGONAL to touch-order — NOT first-touch.** `competing_last_touch_views`, `competing_last_touch_view_conversions`, and `competing_last_tv_touch_view_conversions` all EXIST as columns → a row can be "competing" AND last-touch simultaneously, so `competing_*` is a **competitive-scenario credit**, not a re-timing to first touch. Calling `industry_standard`/"new" reporting "first-touch" is a Prod-Ops shorthand/misnomer (Johnny); Measurement (Lilit) confirms conversions are matched **last-touch / last-tv-touch ONLY**.
   - **Conversions & order-value have NO first-touch column** (only `last_touch_*`, `last_tv_touch_*`, `competing_*`, `probattr_*`). The ONLY `first_touch_*` column in the whole table is `first_touch_visits` (VISITS only) — which is why the visit-level `ft_eq_lt` analysis (§5d) is legitimate *for visits*, but the **client UI headline sums `competing_*`, not `first_touch_visits`.**
   - **`industry_standard` DOES move revenue/ROAS** vs plain last-touch (Avon prospecting 17.3 → 22.1), because `competing_*` adds `competing_view_conversions` + `competing_view_order_value`, not just visits. So §5e's "FT switch does not move AID revenue" is WRONG; §5e-bis's math (Avon reproduced to the dollar, both years, via last-touch + `competing_*`) is RIGHT — only its "first-touch" LABEL was wrong.
   - **Practical rule:** relabel any "first-touch (FT)" → **`industry_standard` (last-touch + `competing_*`)**. Hold `reporting_style` constant across BOTH years of any client YoY (a mid-window last-touch→industry_standard migration inflates the apparent drop). Every AUDI-1070 decline conclusion is UNCHANGED — both the plain-last-touch and the industry_standard lenses decline (HexClad) / rise (Avon); only the label was corrected. See memory `reference_attribution_industry_standard_ft`.

5i. **Identifying WHICH advertiser (and lens) a client chart is — the spend-match method (AUDI-1070, 2026-07-01).** A client screenshot ("Performance Report – MoM", pink Visits / blue Spend / green ROAS) often arrives with **no advertiser label** or the **wrong** one. **Spend is lens-invariant and scope-specific → it is the fingerprint.** To identify: pull monthly `SUM(media_spend+data_spend+platform_spend)` for each candidate advertiser at each scope (AID-wide vs prospecting `objective_id IN (1,5,6)`) and match the chart's blue bars **to the dollar**; then reproduce the pink Visits + green ROAS by trying **both** lenses (last-touch vs `industry_standard`). **Worked case:** a chart handed to us as "Avon" was actually **HexClad** — monthly prospecting spend $139k–$903k (Nov '25 = $903,423 = chart $903.4k) is HexClad's range, not Avon's ($6k–$26k). Reproduced HexClad's every month to the **exact visit + ROAS** on `industry_standard` (all_facts, obj IN (1,5,6): Nov VV 708,513 / 8.17× EXACT). A second "Avon" chart **was** Avon — spend $6k–$26k, reproduced to the exact visit + ROAS on **last-touch**. **Lesson: never trust the label on a client chart; fingerprint it by spend first.**

5j. **The MoM "Performance Report" widget renders a DIFFERENT lens per advertiser — Avon = last-touch, HexClad = `industry_standard` (AUDI-1070, 2026-07-01).** Reproduced BOTH client MoM charts to the exact monthly visit + ROAS, but with **different lenses**: HexClad's needed last-touch + `competing_*` (`industry_standard`); Avon's needed **plain last-touch** (adding competing overshot Avon's visits ~1.3× and ROAS ~1.2×). So `reporting_style` is **per-advertiser**, and a given widget can differ from the summary cards. **Rule: before comparing any BQ pull to a client chart, reproduce it BOTH ways (LT and LT+competing) and let the exact match tell you the lens** — do not assume. Scope for the MoM chart = **prospecting `objective_id IN (1,5,6)`** (confirmed by the spend match), not AID-wide. Query pattern: `queries/avon_chapi_exact_reproduction.sql` (swap advertiser_id; toggle the competing_* terms).

5k. **Stable HI-share ≠ stable ROAS — why monthly prospecting ROAS swings wildly on a gated, healthy advertiser (AUDI-1070, 2026-07-01).** Avon stayed gated-HI all window (except the Nov 19–Jan 6 holiday gate-off) yet its client MoM ROAS swings **4.3×–16.8×**. This is **expected and healthy**, not audience degradation. Decompose `ROAS = conversions × AOV / spend`:
   - **AOV is the tell — it's FLAT ($47–56 every one of 17 months).** So the swing is **conversions-per-DOLLAR**, not basket size and not composition.
   - **Driver 1 — diminishing-returns envelope: ROAS moves INVERSELY with spend level.** Avon's two highest-ROAS months (Apr '25 16.06×, Jul '25 16.81×) are its two **lowest-spend** months (~$7.3k); its **highest-spend** month (Nov '25 $25,585) sits at **5.92×**. Spend low → you serve only the cream of HI (high efficiency); spend hard → you re-serve / reach deeper into the finite HI pool and marginal ROAS falls. **Same finite-HI-pool saturation mechanic as Caraway** — Avon just mostly operates at the efficient low-spend end (that discipline is *why* it's healthy).
   - **Driver 2 — small-volume + seasonal + attribution-lag noise.** Only ~740–3,500 conversions/month; at identical $7.3k spend, Apr/Jul booked ~2,300 conversions but Aug/Sep only ~750–950 (summer demand lull + view-through timing) → scatter around the envelope.
   - **The misconception to correct out loud:** HI-share tells you **WHO** you reached (composition); ROAS is **revenue-per-DOLLAR** (marginal efficiency), which depends on **how hard you spent into the pool** + conversion timing — so composition being frozen does NOT pin ROAS. **Monthly wobble ≠ trend:** aggregate Avon Jan–May prospecting ROAS is **7.93× (2025) → 8.59× (2026), +8%** on **−18% spend** = healthy. Chart: `artifacts/avon_spend_roas_envelope.png`; data: `outputs/avon_mom_lt_decomposition.csv`. See memory `reference_stable_hi_not_stable_roas`.

5g. **"Why don't my numbers match the UI?" — the 3-knob reconciliation (AUDI-1070, 2026-06-30).** Any gap between a self-computed ROAS/visits/conv number and the client UI/API decomposes into **exactly three knobs** (+ a small pipeline residual). Diagnose in this order:
   1. **SCOPE — the biggest knob.** The client "Performance Report – MoM" chart (pink-visits/blue-spend/green-ROAS) and a naive API pull are often the **prospecting group** (CHAPI scope = **`objective_id`**, prospecting = `objective_id IN (1,5,6)`, NOT AID-wide); the UI summary cards are **all campaigns**. **Proof of scope = match the spend** (the chart's blue bars sum to the prospecting figure $56,833/$46,614, not the AID-wide $73K/$64K). The lift from ~9× (prospecting) to ~22–26× (account) is the **dedicated TV Retargeting campaigns (`objective_id=4`, ~50× pooled)** — NOT "mid-funnel S2/S3" (those run ~0–13×). Use `objective_id` (CHAPI's actual filter); `funnel_level` is a messy separator. Join `campaign_id → bronze.integrationprod.campaigns` (PK `campaign_id`; no `silver.core.campaigns`).
   2. **ATTRIBUTION (reporting style) — the BQ↔UI knob.** The UI runs `industry_standard`/NEW style = last-touch + **`competing_*` (FIRST-TOUCH)** cols; plain last-touch omits `competing_*`. Avon all-stages: last-touch ROAS 17.3 → **+ `competing_*` → 22.1 EXACT** (reproduced in `all_facts`, §5e-bis). **NOT `last_tv_touch`/CTV** (that was an earlier wrong draft; lt+tv coincidentally overshoots to 23.5). Legacy advertisers (`reporting_style` api2.*) use last-touch only.
   3. **AGGREGATION — the within-scope knob.** A period ROAS is **Σrevenue ÷ Σspend (aggregate)**, never the **average of monthly ROAS**. The two diverge whenever spend varies month-to-month (low-spend high-ROAS months get full weight in a simple average). Avon prospecting: aggregate 9.40 vs avg-of-months 8.94; even clean BQ shows agg 12.5 vs mean 14.0. **Always report the aggregate; the "decline" everyone quoted only existed in the average-of-monthly.**
   Bridge (Avon Jan–May 2025): API/prospecting **9.4×** → +mid-funnel stages (scope) → BQ all-stages-no-CTV **17.3×** → +CTV (attribution) → UI **22.1×**. Every scope/source is UP YoY (prospecting +10%, AID-wide +19%). Canonical artifacts: `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts/audi_1070_avon_three_source_bridge.png` + `outputs/avon_source_reconciliation.csv`.

### BigQuery Behavioral Gotchas
- **GENERATE_UUID() is non-deterministic across CTE references (TI-650, 2026-03-19):** BQ CTEs are NOT guaranteed to be materialized. If a CTE uses `GENERATE_UUID()` and is referenced in multiple places (e.g., 3 UNION ALL blocks), each reference re-evaluates the function and gets a DIFFERENT UUID. Fix: use a deterministic hash like `TO_HEX(MD5(key_column))` or `FORMAT('%s-...', SUBSTR(TO_HEX(MD5(key)), ...))` when the UUID must be stable across references.
- **Display impression timing gap (TI-650, 2026-03-19):** Display impressions can be served 2-4 weeks before the user visits the site and triggers the VV. When tracing S3 VVs back to their impression via `ad_served_id`, a ±7d window around the VV time misses 35% of display impressions. CTV is same-day. **Use ±30d for the 5-source trace when display campaigns are in scope.** Verified on 24+ advertisers.

### Retention / TTL
| Table | Retention |
|-------|-----------|
| silver.logdata.clickpass_log | **No TTL** — confirmed 2026-03-03 (expirationTime: none, no partition expiry) |
| silver.logdata.event_log | **No TTL** — confirmed 2026-03-03 (expirationTime: none, no partition expiry) |
| bidder_bid_events | 90 days (expirationMs on partition) |
| bid_logs (silver.logdata) | **TTL confirmed empirically (TI-650, 2026-03-23):** 10/10 tested ad_served_ids had impression_log records but bid_logs records gone (no time filter). impression_log.ip for display is internal NAT (10.105.x.x) — useless without bid_logs join. |
| bid_logs_enriched | 90 days |
| event_log_filtered | 60 days |
| conversions | 60 days |
| spend_log_tmp (in logdata) | Unknown — likely short-term staging |
| spend_log | No expiry — HOUR partitioned |

---

## Join Keys Reference

| Join | Left | Right | Key |
|------|------|-------|-----|
| Visit → Impression | summarydata.visits | logdata.impression_log | ad_served_id |
| Conversion → Visit | summarydata.conversions | summarydata.visits | guid + advertiser_id |
| Spend → Bidder | logdata.spend_log | logdata.bidder_bid_events | auction_id / bid_id |
| Bidder bid → auction | bidder_bid_events | bidder_auction_events | auction_id |
| Any → Campaign | any | silver.core.* | campaign_id |
| Any → Advertiser | any | silver.core.* | advertiser_id |
| device_type label | any | bronze.integrationprod.device_type | device_type |

---

## Dataset Disambiguation

### logdata vs sqlmesh__logdata
Query `logdata.*` — it always points to the current version. Never query `sqlmesh__logdata.*` directly.

### summarydata vs sqlmesh__summarydata
Same rule: query `summarydata.*`. Never query `sqlmesh__summarydata.*` directly.

### spend_log vs win_logs vs cost_impression_log
- `spend_log`: MNTN bidder pipeline output — wins with cost data. Source of truth for billing.
- `win_logs`: Beeswax win notification log — external exchange perspective.
- `cost_impression_log`: enriched impression-level spend, joined with geo/device/segment data.
  90-day rolling. Best for impression-level cost analysis.

### silver.core vs bronze.integrationprod
`silver.core` is a thin view layer — every table is `SELECT * FROM bronze.integrationprod.core_*`.
For schemas, always reference `bronze.integrationprod`. The `core_` prefix is stripped in silver.core
(e.g. `integrationprod.core_flights` → `silver.core.flights`).

**Tables NOT exposed in silver.core** (because they don't have a `core_` prefix in bronze):
`campaigns`, `campaign_groups`, `advertisers`, `advertiser_configs`, and most other top-level
integrationprod tables. For these, query `bronze.integrationprod.<table>` directly.

### bronze.raw physical vs silver enriched (key differences)
| Aspect | bronze.raw | silver.logdata |
|--------|-----------|----------------|
| device_type | INTEGER | STRING (already joined to ENUM) |
| auction_timestamp | INTEGER (raw epoch) | TIMESTAMP (converted) |
| geo_type / video_placement | INTEGER | STRING |
| bid_placed / bid_dropped | BOOLEAN (present) | Absent — filtered out |
| _source_file / _batch_id | Present | Absent |
| site_page / site_referrer / content_* | Present (raw) | Absent or condensed |
| TTL | 90 days (bidder), 10 days (augmentor) | No expiry on most silver tables |

### aggregates vs facts tables
- `silver.aggregates.agg__daily_sum_by_campaign`: **pre-computed daily rollup**, best for campaign-level
  trend analysis. Cheaper to query than visit_facts/conversion_facts.
- `silver.summarydata.visit_facts / conversion_facts / spend_facts`: **hourly granularity**, more
  flexible for custom date bucketing, geo breakdowns, and device-level analysis.
- Use `agg__daily_sum_by_campaign` when you just need daily campaign totals.
- Use the individual facts tables when you need sub-day granularity or geo/device dimensions.

---

## Entity Hierarchy & Key Relationships

```
advertisers (bronze.integrationprod.advertisers)
  └── campaign_groups (campaign_group_id → advertiser_id)
        └── core_flights (flight_id → campaign_group_id)   ← budget period
        └── campaigns (campaign_id → campaign_group_id)
              └── core_creative_groups (group_id → campaign_id)
                    └── core_creative_groups_x_creatives
                          └── core_creatives (creative_id → advertiser_id)
```

### Extended Join Keys (Phase 2 additions)
| Join | Left | Right | Key |
|------|------|-------|-----|
| Campaign → Campaign Group | campaigns | campaign_groups | campaign_group_id |
| Campaign Group → Advertiser | campaign_groups | advertisers | advertiser_id |
| Campaign Group → Flight | campaign_groups | core_flights | active_flight_id |
| Creative → Advertiser | core_creatives | advertisers | advertiser_id |
| Creative Group → Campaign | core_creative_groups | campaigns | campaign_id |
| Any spend → Flight | spend_log | core_flights | flight_id |
| Conversion source | summarydata.conversions | integrationprod.data_sources | conversion_source_id |
| PMP deal → Campaign Group | core_private_marketplace_deals | campaign_groups | campaign_group_id |
| Audience size | aggregates.audience_hll_by_day | integrationprod.audience_segments | segment_id |
| MNTN → Beeswax advertiser | integrationprod.advertisers | beeswax_advertiser_mappings | advertiser_id |
| MNTN → Beeswax line item | integrationprod.campaigns | beeswax_line_item_mappings | campaign_id |

---

## Datastream Replication (bronze.integrationprod)

All `bronze.integrationprod` tables are Postgres replicas via GCP Datastream (CDC).
Most tables include a `datastream_metadata RECORD`:
- `uuid` — Datastream replication event UUID
- `source_timestamp` — Epoch ms of the Postgres WAL change event

**Do not use `datastream_metadata.source_timestamp` as a proxy for `update_time`** — it is
the CDC event timestamp, not the application-layer update time. Use `update_time` instead.

Tables without `datastream_metadata` (e.g. `advertisers`, `campaigns`, `campaign_groups`)
are likely replicated via a different mechanism or are missing the field intentionally.

---

## TTL / Retention Summary (Phase 2 additions)

| Table | Project.Dataset | Retention |
|-------|----------------|-----------|
| bronze.raw.bidder_bid_events | dw-main-bronze.raw | 90 days |
| bronze.raw.bidder_auction_events | dw-main-bronze.raw | 90 days |
| bronze.raw.bidder_beeswax_win_notifications | dw-main-bronze.raw | 90 days |
| bronze.raw.bidder_win_notifications | dw-main-bronze.raw | 90 days |
| bronze.raw.bidder_price_events | dw-main-bronze.raw | 90 days |
| bronze.raw.augmentor_log | dw-main-bronze.raw | **10 days** (+ partition filter required) |
| bronze.raw.bid_price_log | dw-main-bronze.raw | **10 days** (+ partition filter required) |
| bronze.raw.tmul_daily | dw-main-bronze.raw | **14 days** |
| bronze.raw.page_view_signal_log | dw-main-bronze.raw | 90 days |

---

## is_test / deleted Filters (bronze.integrationprod)

Always apply these filters when joining dimension tables for production analysis:
```sql
-- Advertisers
WHERE deleted = FALSE AND is_test = FALSE

-- Campaigns
WHERE deleted = FALSE AND is_test = FALSE

-- Campaign Groups
WHERE deleted = FALSE AND is_test = FALSE
```
Missing these filters will include internal test accounts and deleted entities in counts/metrics.

**Caveat — `is_test=TRUE` advertisers can have real production spend.** Verified 2026-06-03 (TI-ADHOC advertiser scoring filter): 8 advertisers flagged `is_test=TRUE` in the `advertisers` table spent **$459k of production media** in a 30-day window (2.8% of total). Either historical mis-flagging never cleaned up, or internal MNTN accounts running real budget. **Implication for any "score this cohort" / "include this cohort" filter:** if you blindly apply `is_test=FALSE`, you silently drop a small live-spend population. For production-spend-driven workflows (scoring, billing, reporting), consider keying off spend or campaign-liveness rather than the `is_test` flag.

---

## Advertising Concepts & Domain Logic

### MNTN Product Identification (PTV / Select / QuickFrame)
Product line is stamped on `campaign_groups.product_id` (INT64). Lookup is
`bronze.integrationprod.core_products`:

**Source of truth (confirmed by Ray, 2026-05-05):** `campaign_groups.product_id` in **coredb**
(the Postgres production database). The UI sets this value when users manage Select vs PTV
campaigns. That stamp flows org-wide — it is the canonical filter element for customer reporting
AND for invoices. Application-derived flags like `is_select_cid` are bespoke (bidder/camperbid
context) and should circle back to UI values but aren't the source.

**BQ replicas of coredb's `campaign_groups`** — four variants exist in `bronze.integrationprod`,
all derived from the same Postgres source. Verified 2026-05-05 to agree exactly on Select counts
(all show 260 active product_id=2 groups, zero per-row disagreement on `product_id`):

| Table | What it is | Use when |
|---|---|---|
| **`public_campaign_groups_raw`** | Datastream CDC landing table — closest BQ replica of coredb's Postgres `public.campaign_groups`. Has `datastream_metadata`, PK, clustered, `max_staleness=15min`. | Pipelines / Spark / Databricks — closest to source, supports incremental reads via `datastream_metadata.source_timestamp`. |
| `public_campaign_groups` | SQLMesh versioned view over the CDC raw. | Equivalent to raw with SQLMesh applied. |
| `campaign_groups` | SQLMesh-derived analytical table with cleaned/enriched columns (`update_time_raw`, `has_audience_raw`, `display_creatives_status_id_raw`). | Day-to-day analytics — slightly fewer rows than CDC due to SQLMesh filters. |
| `campaign_groups_raw` | Another SQLMesh-derived intermediate, ~same columns as analytical `campaign_groups`. | Internal staging — usually no reason to query directly. |

**`product_id` is immutable in practice.** Verified across 735,704 archive row versions in
`archives_campaign_group_archives` covering 117,393 distinct campaign groups: zero groups have ever
had `product_id` changed. It is set at creation by the UI and never modified.

| product_id | name | Notes |
|------------|------|-------|
| 1 | PTV | Performance TV — the main product. ~120,899 groups (2010-present). |
| 2 | **Select** | MNTN's media marketplace / PMP product (Hannah's org). 378 groups, first created 2025-07-31. |
| 3 | QuickFrame | Listed in `core_products` but no campaign_groups attached (as of 2026-05-05). |

**Canonical filter for MNTN Select campaigns/groups:**
```sql
-- Pipeline / Spark / Databricks (CDC source of truth):
SELECT campaign_group_id, advertiser_id, name
FROM `dw-main-bronze.integrationprod.public_campaign_groups_raw`
WHERE product_id = 2 AND deleted = FALSE AND is_test = FALSE;

-- Day-to-day analytics (cleaner column set, ~equivalent counts):
SELECT campaign_group_id, advertiser_id, name
FROM `dw-main-bronze.integrationprod.campaign_groups`
WHERE product_id = 2 AND deleted = FALSE AND is_test = FALSE;

-- Select campaigns (line items) — product_id is on the GROUP, not the campaign:
SELECT c.*
FROM `dw-main-bronze.integrationprod.campaigns` c
JOIN `dw-main-bronze.integrationprod.public_campaign_groups_raw` cg
  ON c.campaign_group_id = cg.campaign_group_id
WHERE cg.product_id = 2
  AND c.deleted = FALSE AND cg.deleted = FALSE
  AND c.is_test = FALSE AND cg.is_test = FALSE;
```

**Important:**
- `product_id` lives **only on `campaign_groups`** — not on `campaigns`. Always filter at the group
  level and join down.
- `bronze.integrationprod.camperbid_mmm_training_data.is_select_cid` exists but is a derived
  training-data flag, **not** the source of truth. Use `campaign_groups.product_id = 2`.
- Select started **2025-07-31** — any historical analysis before that date will have zero rows.

**Cross-validation (verified 2026-05-05):**
- `product_id` is fully populated — only values 1 (PTV) and 2 (Select), no NULLs, no other values.
- `product_id=2` and `is_select_cid=TRUE` agree **100%** on bidder pipeline data
  (280 Select campaigns ↔ 17,093 PTV campaigns, no mixed cases in last 30d).
- 252/260 active product_id=2 groups (97%) have PMP deals attached.
- The 8 Select groups without PMP deals are all internal QA test campaigns
  (advertisers 45842, 45983 — names like "alex_test_prod_X", "bryan test", "NHL Live Sports Test Campaign").
  `is_test` flag is unset on these groups, so a strict filter should also exclude
  test-account advertisers, not just `is_test=FALSE` on the group.

**Don't use PMP-deal attachment as a Select proxy.** 28 PTV (`product_id=1`) groups also have PMP deals
attached — all are **Pause Ads** campaigns (named "Pause Ads", "EX-49 Pause Ads V2", "Hugo Pause Ads",
"Earnings Pause Ad Campaign", etc.). Pause Ads are a separate PTV feature that uses PMP inventory but
is NOT MNTN Select. Filtering by PMP-deal attachment alone would over-count Select by ~10%.

**Related Select/PMP tables** (for deal-level or pricing context):
- `mntnselect_offering_versions` — Select offering catalog (offerings, PMP deals, run windows, CPMs)
- `core_private_marketplace_deals` / `core_private_marketplace_groups` — PMP deal hierarchy
- `core_campaign_group_x_private_marketplace_deals` — group ↔ PMP deal mapping
- `core_select_advertiser_margins` / `core_select_margins` — Select-specific pricing
- `invoice_select_publisher_invoices` — Select publisher invoicing

### campaign_status_id mapping (`core_campaign_statuses`)
| id | name | Live? |
|---|---|---|
| 1 | Ready | not delivering (set up, not started) |
| 3 | **Live** | yes — delivering |
| 4 | **Pause By Advertisers** | paused by the advertiser/agency |
| 5 | Pause By MNTN | paused by MNTN ops |
| 7 | Inactive | ended / off |
| 8 | Deleted | — |
| 9 | Legacy Archived | — |

To find an advertiser's **currently-delivering** campaigns: `campaign_status_id = 3` (or confirm with recent `all_facts` impressions). Don't infer "live" from `deleted=FALSE` alone — paused (4/5) and inactive (7) rows are also non-deleted.

### MNTN Select campaigns are geo-only / unscored → Fangorn is a no-op for them
Empirically (iMemories AID 37423, TI-Kale-eval 2026-06-17), live **MNTN Select** (`product_id=2`) prospecting campaigns run as **geo-only "all-US" reach**: the audience expression carries only `geos:{location_ids:[237]}` (US) + DS14 bid-routing + DS16 funnel tags + an `rtc` score directive + the 10% holdout bucket — **no MM layer (no DS13/DS19/DS46), no buyer interest segments.** Delivery is therefore **100% unscored** (`cost_impression_log.household_score = -1`, HHST=0); only a handful of `advertiser_household_score>0` rows (RTC firing for the rare recent-site-visitor). This is the "Geo-only / no buyer audience layer" cohort (TI-999 §) and the structural state of MNTN Select today (Select targets all-US until the MM Awareness Audience for Select ships).

**DS16 = internal funnel/delivery tags, NOT interest targeting (TI-1037, Kindred 35094, 2026-07-02).** Category names in `fpa.categories` (data_source_id=16): **7291="Impressions", 787280="Wins", and 178328x="CampaignGroupID"** (one per campaign group). A newer campaign-build template injects a DS16 clause that **includes the campaign's own `CampaignGroupID` tag and excludes the `Impressions`+`Wins` tags** — an internal pacing/frequency-suppression construct, not a buyer audience. So a DS16 leaf in an audience expression is a template artifact; do not count it as interest breadth. Older prospecting campaigns lack it, so its presence/absence across a fleet is a clean **template-drift signal** (e.g., Kindred's 3 Q1-2026 HiPop launches carry DS16, the flagship/LowPop/MidPop don't). DS16 names live in `fpa.categories` (which carries DS13/14/16/21); DS35 names live in `bronze.tpa.categories`.

**Flights: `core_flights` is the full flight history; `dso_campaign_group_flight_budgets` is CURRENT-only (TI-1037 + Tofer/Prod Ops, 2026-07-02).** `bronze.integrationprod.core_flights` links to the group via **`campaign_group_id` directly** and carries `start_time`/`end_time`/`budget`/`status_id` for **every** flight ever set — pull Start/End from here for a flight timeline. Do NOT go through `dso_campaign_group_flight_budgets` for history: it only holds the current/latest flight per group (a single recent row), and `campaign_groups.active_flight_id` is stale. **Flights are set MANUALLY, one per launch, by Prod Ops (Tofer)** — officially since early 2025, informally for years before, so **coverage may be partial for older campaigns**. Key implication: a group can deliver **continuously** yet be composed of **many short back-to-back flights** (Kindred High Pop = 114 flights, 38 ≤3d) — so delivery-run gap detection (`sum_by_campaign_by_day`) shows ONE continuous run while the scheduled-flight view shows heavy fragmentation. Short flights (≤3d/<72h) auto-set HHST=0. `status_id` seen: 3, 8 (both real). [[reference_hhst_pacing_lever]]

**Geo `location_ids` in audience expressions = DMA tiering, via `dw-main-silver.geo.location_data` (TI-1037 workflow, 2026-07-02).** The `geos.where` op-tree carries `location_ids` (MNTN internal ids, NOT Nielsen codes). Decode: `geo.location_data` WHERE `location_type_id=4` (=Nielsen DMA; exactly **210 US DMAs**) → `location`=DMA name (e.g. "New York, NY"), `metro_id`=Nielsen 3-digit code (loc 541=NY, 606=LA, 464=Chicago…). NO population/rank column exists → tier by DMA-set size / Nielsen rank. `geo.locations` 404s; use `location_data`. **Advertisers commonly GEO-SLICE prospecting into DMA population tiers** (e.g. Kindred: High/HiPop=top-20 majors ≈40% of US TV-HH, Mid=38, Low=152 long-tail; 20+38+152=210=full US, disjoint). **`geos.where` = `AND[ include{small set}, NOT{large set} ]` is tier-partitioning PLUMBING, not audience-narrowing** — the NOT-block is the mechanical complement of the whitelist; don't misread it as a heavy-exclusion red flag. Watch instead: geo mix-shift (top-market flagship winding down while spend spreads into low-value long-tail = dilution), tiny footprints, and fragmentation (multiple campaigns on the identical DMA set). Interest-narrowing (the real red flag) = MM(DS19) **AND** 3P(DS35) [narrows] vs **OR** [additive/broadens]; a 3P segment only limits reach if AND-required. Reusable decode = TI-1037 perf_report module 12 (audience logic) + **module 12b (geo/DMA deep-dive)**. 3P segment SIZES: `external_ddm.data_source_category_sizes` (3P-only) is a GCS-external table over `gs://mntn-data-monitoring/` — **access-gated** (needs Storage Object Viewer); names decode from `bronze.tpa.categories` (data_source_id=35).

**TWO geo id systems — bridge on the Nielsen code (TI-1037 module 12b, 2026-07-02).** Audience expressions use `location_id` (internal `geo.location_data` id); delivery/summary tables use `metro_id` (Nielsen code). They DIFFER in value (loc 541 = NY, but Nielsen 501 = NY; loc 606 = LA, Nielsen 803 = LA). Join them via `geo.location_data.metro_id === cost_impression_log.metro_id === summarydata.metros.metro_id` (all = Nielsen). **DMA-grain sources:** only **CIL (`logdata.cost_impression_log`)** carries `metro_id` at impression grain (filter `DATE(time)`; row-count = impressions, `media_cost` = spend). **CORRECTION (verified 2026-07-05): CIL is NOT 90-day TTL — it retains full history to the 2025-01-01 GCP data floor** (80.9M rows on 2025-01-15, 70.8M on 2025-09-15, queried Jul 2026). So per-DMA delivery, HI-share, and score-bucket pulls CAN span P1 vs P2 (earlier "P1 out of window" notes were wrong). The real early-2025 limit is the **score COLUMN** (`household_score`/`advertiser_household_score` NULL before 2025-06 logging onset, recoverable from `model_params` back to 2025-05-06 — see data_catalog §CIL), not row retention. `summarydata.metros` = dim only (metro_id→name→country, no facts). `summarydata.sum_by_region_by_day` = has full history + full KPIs (imps/views/conv/spend) **but grain is STATE (`region`), NOT DMA** — use for YoY geo-mix at state level, not DMA. There is **no** DMA-grain fact table with history. For per-tier YoY performance use `sum_by_campaign_by_day` grouped by `campaign_group_id` (tiers = separate campaigns): visits=`views+clicks`, conv=`click+view_conversions`, spend=`media+data+platform_spend`, revenue=`click+view_order_value`; needs a `day` partition-elim bound. **Geo-mix dilution, quantified (Kindred):** P1 (Jan–May '25) ran the top-20 High Pop tier ONLY (1 campaign, ROAS 9.74x); P2 fragmented into 6 campaigns spanning all 210 DMAs → blended prospecting ROAS **1.81x**. Decomposition: the **flagship top-20 campaign itself collapsed** (9.74→2.39x, VR 11.6→5.1‰, CVR 8.7→5.6%) = ~90% of the drop; the rest is fragmentation into lower-ROAS Mid (1.73x)/Low (1.31x) tiers + 3 new same-geo interest-VARIANTS (Harter/Motherhood/Mom-Focus, 1.18–1.35x) that dilute the blend. Lesson: a "geo dilution" hypothesis is really TWO effects — flagship degradation (first-order) vs mix-shift into worse tiers/audiences (second-order); measure both, don't conflate.

**DS16 = the advertiser's OWN funnel tags → a "net-new reach" gate (TI-1037 module 12c, 2026-07-02).** `data_source_id=16` in an audience expression is NOT a 3P/interest source — it is the advertiser's own funnel state, decoded via `bronze.tpa.categories` (key = `data_source_category_id`, path in `path_from_root`/`display_name`): `7291`="Impressions" (households the advertiser has served, ANY campaign), `787280`="Wins", plus per-CampaignGroup impression nodes (e.g. Kindred `1783281/1783302/1783323`). A common variant-campaign gate is `AND ( NOT DS16[7291,787280] OR DS16[own-campaign-group] )` = **target a household iff NEVER impressed/won by the advertiser, OR already owned by this campaign** = a **net-new-reach / sequential-ownership gate**, NOT audience narrowing by interest. Kindred's 3 Q1-2026 "HiPop" variants (Harter/Motherhood/Mom-Focus) each add this gate (base/Mid/Low do not); it steers them off the pool the ungated base already saturated → they fish the smaller, lower-quality residual → ROAS 1.18–1.35x vs base 2.39x. So "why is my variant's audience so much smaller?" can be a funnel gate, not a 3P AND. Other DS meanings seen here: DS14[1]="Beeswax Bidder" (plumbing), DS47=CRM-upload exclusion lists, DS21=own converters, DS34=own funnel, all inside the shared `NOT(...)` hygiene block. **All 6 Kindred prospecting campaigns join MM(DS19) and 3P(DS35) with OR (additive) — no MM-AND-3P narrowing anywhere;** the "required-3P narrows MM" red flag was NOT present. Reusable interest-logic decode = module 12c.

**Geo whitelist: national vs DMA-slice — `location_id` classifies it (TI-1037 module 00, 2026-07-04).** In `geos.where`, `location_id=237` = **"United States"** (country, `location_type_id=2`) = a NATIONAL campaign (all-US), NOT a narrow 1-DMA slice — don't false-flag it as narrow. DMA (`location_type_id=4`) ids sit in the **~461–672** block (the 210 US DMAs). So classify: national = `237 ∈ ids ∧ no DMA ids`; else count DMA ids in 461–672 for the tier (narrow ≤25 / thin ≥120). Kindred slices DMAs (top-20/mid-38/long-152); The Bouqs runs mostly NATIONAL (loc 237) with one Valentine's DMA-sliced campaign (84 DMAs). Reusable classifier = module 00 `geo_class()`.

**Prospecting HI-share is advertiser-specific — audit it (TI-1037 modules 00/00b, 2026-07-04).** The score-bucket mix of REACHED households (`household_score`≥8001=HI) varies hugely by advertiser and is a top prospecting-quality flag. Kindred prospecting reaches ~80–88% HI uniformly (quality fine; decline is conversion efficiency). **The Bouqs eCommerce (32147) is the opposite**: its big national prospecting campaigns reach mostly Mid/MaxReach + unscored — 595017 "eComm" = 5.0M reach at **4% HI / 43% unscored / 0.54x ROAS** (scaling into low-intent supply at a loss); v2 frequency campaigns 6–20% HI; only Subscriptions-prospecting (580914) is high-HI (83% / 9.36x) and the tiny original frequency campaigns are 93–96% HI but ~4–7K reach. Flag = low HI-share (<70%) or high unscored (>30%). Same account-shape as Kindred though: **retargeting is the engine** (Bouqs 10.9x / 81% of revenue; Kindred 26.5x / 85%), MT-S2 near-0 ROAS.

**A "campaign group" is a FULL FUNNEL, not one campaign — audit at campaign×objective grain (TI-1037 module 00, 2026-07-02).** Each MNTN `campaign_group` bundles one campaign per funnel stage: **stage = `objective_id`** (1=Prospecting, 4=Retargeting, 5=Multi-Touch S2, 6=Multi-Touch S3, 7=Ego). `funnel_level` is NOT a clean stage key — inside the all-retargeting group it is reused as a sub-tier (general/5+PV/cart), so a retargeting campaign can show `funnel_level=1` while `objective_id=4`. **Consequence: grouping delivery by `campaign_group_id` CONFLATES stages** — e.g. Kindred group 69884 "CTV Prospecting High Pop" = Prospecting(F1,CTV) + MT-S2(F2,display) + MT-S3(F3,display) + Ego; its group-level reach/ROAS (as used in TI-1037 modules 12b/12c) blended CTV prospecting with display retargeting. Always filter/split by `objective_id`. Also: **prospecting runs on CTV (`channel_id=8`), the Multi-Touch stages on DISPLAY (`channel_id=1`)** — channel mixes inside one "CTV" group. **Where the money is (Kindred, Jan–May '26): RETARGETING = ~26x ROAS, ~85% of revenue on ~28% of spend (15,758 conv); Prospecting = 62% of spend but 13% of revenue at ~1.9x.** The prospecting YoY decline is a top-funnel-REACH story, not the revenue engine — always map stage economics first (module 00) before deep-diving one stage. Multi-Touch S2 spent ~$18K for ~0 last-touch conv (assist-only / attribution).

**HLL reach + overlap on `sum_by_campaign_by_day` (BQ-native; TI-1037 module 12c, 2026-07-02).** The reach columns `uniques`, `new_users_reached`, `existing_users_reached`, `site_visitors` (all BYTES) are **BQ-native HLL++ sketches** — `HLL_COUNT.MERGE(uniques)` gives distinct households REACHED (served); `uniques ≈ new_users_reached + existing_users_reached`. (Prospecting is ~99% `new_users_reached`, so the new/existing split does NOT discriminate a net-new gate — measure WHICH households via overlap instead.) **Cheap set overlap without a raw-IP scan:** conditional merge builds any subset's union sketch in one pass — `HLL_COUNT.MERGE(IF(grp IN (a,b), uniques, NULL))` (aggregates skip NULL) — then intersection by inclusion-exclusion `A∩B = reach(A)+reach(B)−reach(A∪B)`. Kindred proof: each gated variant reached ~435K households = ~26% of base's 1.64M, ~72% net-new vs base (base∩variant ~27%), ~90% mutually disjoint across the 3 variants (variant∩variant ~9%) = a 3-way mutually-exclusive creative split. Needs a `day` partition-elim bound; the campaigns→group join over impression-grain rows bills ~4–5GB for a 5-month window (reserved capacity, fine). NB `agg__daily_sum_by_campaign.uniques` is the UNRELIABLE one (~0, starts Sep 2025) — that caveat is a DIFFERENT table; `sum_by_campaign_by_day.uniques` works.

**A campaign's audience is NOT stable under a fixed `campaign_id` (TI-1037, 2026-07-02).** Both the `audience_id`
AND the data-source composition change over time — the segment expression is versioned in
`silver.archives.audience_segment_archives`. Example: Kindred prospecting campaign **261318** changed its audience **8×**
— `audience_id` swapped **22666 → 31114** (2024-09-21), DS13-only(vertical) → DS19(keyword), **DS35 (3P) added 2025-05**,
**DS13 (vertical) re-added 2025-10** then dropped 2025-12, **DS21/34 (retgt-excl) added 2025-11**. **Implication:** never
assume the *current* expression reflects a past period — for any period-over-period audience question, reconstruct the
**as-of-period** expression from the archive (`REGEXP_EXTRACT_ALL(expression, r'"data_source_id":([0-9]+)')`, ORDER BY
`create_time` — `version` is non-monotonic). Corollary for score analysis: DS19 being present in a period means the
household was *scoreable* that period; if scores are still missing it's the CIL logging onset (2025-06), not a missing
MM audience. [[reference_fangorn_audience_overlay]]

**Fangorn candidacy rule (load-bearing):** the `onFangorn` flip swaps **DS13 → DS46** during segment breakdown (audience_products.md). A campaign with **no DS13/DS19 has nothing to swap**, so flipping `onFangorn` changes nothing — Fangorn / Intelligent Intent Scoring only affects campaigns that use the **MM batch scoring layer**. Geo-only Select, pure-3P, pure-1P, and retargeting campaigns are unaffected regardless of rollout tier. So "is advertiser X a Fangorn candidate?" reduces first to **"does X run a live MM (DS13/DS19) prospecting campaign?"** — if not, the rollout criteria are N/A until they do. (iMemories' only real MM-scored prospecting — PTV campaign 466109, DS19 69-keyword + HHST=6666 + RTC — is **paused** since ~Nov 2025; their live campaigns are the geo-only Select Winback test.)

**"MM = has DS19" is an undercount — empirical DS13/DS19/DS46 co-occurrence (TI-1037, 2026-07-08).** Alyson's working definition (MM = DS19 present) vs the segment-level reality. Measured on live prospecting campaigns (`objective_id=1 AND funnel_level=1`, delivered impressions>0 in the trailing 45d) at the **bidder-facing segment level** (`dw-main-silver.audience.audience_segments`, `expression_type_id=2 AND is_targeted=TRUE`, DS ids regex-extracted). Query: `tickets/ti_1037_audience_diagnostic_tool/queries/ti_1037_mm_ds_cooccurrence.sql`.

| DS13 | DS19 | DS46 | campaigns | advertisers | 45d spend | % spend | official name (Matt Brorby, 2026-07-08) |
|---|---|---|---|---|---|---|---|
| — | ✓ | — | 1,559 | 859 | $18.7M | 42.7% | **Keyword-Only / "MM Core"** (Matt's tier association: Max Reach) — bids IP states 2/4/6; CANNOT bid pure-PP IPs (state 5 matches only on a vertical anchor) |
| — | — | — | 1,042 | 450 | $11.7M | 26.8% | **Not MM** — 3P-only / 1P / IP-list / CRM / DS14-only run-of-network |
| — | ✓ | ✓ | 1,314 | 606 | $8.3M | 18.9% | **MM Core + Peak Performance v2** — the Fangorn flagship; keyword layer kept, vertical anchor on PP v2 scoring |
| — | — | ✓ | 235 | 115 | $2.8M | 6.5% | **Peak Performance v2 only** ("vertical only") — ex-vertical-only, flip swapped 13→46; no keyword layer; delivers the PP band ONLY (6666–8000; the v2 HI band requires DS19 — verified, see scoring-generations bullet) |
| ✓ | ✓ | — | 403 | 286 | $1.7M | 4.0% | **MM Core + Peak Performance (v1)** — the config that shipped as the Oct-2025 PP product (canonical detector = DS13+DS19+RTC, TI-896); only DS19-bearing config that also bids the 8000 tier |
| ✓ | — | — | 57 | 42 | $0.5M | 1.1% | **Peak Performance (v1) only** ("vertical only") — bids all in-vertical IPs → HI 10000 + PP 8000 fixed points (v1 is categorical: HI IPs are vertical members, so they match the anchor leaf); not yet flipped to v2 |
| ✓ | — | ✓ | **0** | 0 | — | — | impossible — Fangorn flip swaps 13→46 |
| ✓ | ✓ | ✓ | **0** | 0 | — | — | impossible — same reason |

- **AUTHORITATIVE component naming (Matt Brorby, TI team, 2026-07-08).** Products are named per COMPONENT, and each product name is the IP-tier name applied to the component that unlocks that tier: **DS19 = "MM Core" / Keyword-Only** (unlocks the Max Reach tier — Matt: "Max Reach but I would call it Keyword Only campaigns or MM Core"); **DS13 vertical anchor = "Peak Performance"** (unlocks the 8000 PP tier); **DS13 with bucket ids = "Expanded Peak Performance"** — named but UNSHIPPED (would unlock the MI tier; confirmed zero live campaigns carry bucket ids); **DS46 = "Peak Performance v2"** (Fangorn — the v2 of the vertical-scoring component). HI needs no component of its own — reachable via keywords or the vertical anchor. Combo configs read compositionally: DS13+DS19 = "MM Core + PP", DS19+DS46 = "MM Core + PP v2" (the flagship). MI is the only tier whose product (Expanded PP) never shipped. **Matt endorsed the 2×3 grid + these labels for internal naming consistency (2026-07-08) and notes he colloquially says "vertical only" for the PP-only (no-DS19) campaigns** — so "vertical-only" in conversation = a DS13- or DS46-anchored campaign with no keyword layer. **Published as a Confluence page (TAR space, under TI Projects):** https://mntn.atlassian.net/wiki/spaces/TAR/pages/3691708511 — the org-shareable version of this section; update BOTH if the taxonomy changes.
- **SCORING GENERATIONS — v1 = categorical fixed points, v2 = score bands where THE LABEL FOLLOWS THE SCORE (verified 2026-07-08, 7d of delivered CIL, RTC-excluded, all live v1/v2 prospecting).** Per the Fangorn methodology page (https://mntn.atlassian.net/wiki/spaces/TAR/pages/3414917161): Fangorn = continuous 0–1 intent score; raw boundaries 0.6/0.8 divide MaxReach/MI/HI, transformed onto the legacy 3333/6666 pacing points. Empirical delivered-score distribution:
  - **v1 (DS13): fixed points ONLY** — exactly 8000 (3.1M imps) and exactly 10000 (4.5M) + MaxReach 1–3332 (full random band) + MI 3333–6665; **ZERO impressions at 6666–7999 or 8001–9999**.
  - **v2 (DS46): two model passes per IP, each a continuous band with a pin at its top** — PP pass → **6666–8000** (3.8M imps over 1,206 distinct values + 2.1M pinned exactly 8000); HI pass → **8001–10000** (11.0M over 1,868 values + 2.0M pinned exactly 10000). An IP that structurally matches vertical/keywords but scores <0.8 raw lands in MI/MaxReach — **qualifying criteria feed the model; only the score puts an IP in the HI/PP groups** (Malachi's "two scores, only above-bar counts" reading, confirmed).
  - **The v2 HI band REQUIRES the keyword layer**: 100% of 8001–10000 delivery sits on DS19-carrying campaigns; DS46-only ("vertical only") campaigns top out at 8000 (1,087 stray imps of 11.1M above it). Consequence: post-flip, a vertical-only advertiser's ceiling is the PP band — under v1 the same config delivered 10000s (categorical HI IPs matched the vertical leaf).
  - Open per the methodology doc itself: how/whether Campaign-HI vs PP survives under continuous scoring (legacy split = keyword overlap), and the proposed Fangorn+BUK blend (DCG rank weights → saturating K → additive F=(1−γ)s+γK, Matt's preference) folding keywords into ONE score. Exact per-pass production cutoffs are not documented — the band mapping above is the empirical read. Query: variant 3 in `ti_1037_mm_ds_cooccurrence.sql`.

- **Misreading guard: adding DS13 to DS19 BROADENS, it does not narrow.** Include leaves are OR-joined (module 12c), so DS13+DS19 ("PP config") is not a hotter intersection than DS19-alone — the vertical∩keyword intersection IPs are the HI 10000 tier and are biddable by BOTH configs (they match DS19). DS13's marginal contribution = the state-5 vertical-no-keyword IPs scored exactly 8000 (the PP tier), unreachable from a DS19-only expression. "Peak Performance" names the product that can DELIVER the 8000 tier (DS13+DS19+RTC, HHST 6666 → HI+PP only), not a narrower audience.
- **The OLD MM 2.0 IP-state table (Alyson's sheet, pre-Fangorn) decodes the product names.** An IP's state = (in bucket?, in vertical?, has keywords?) → tier: state 6 = **HI 10000** (vertical ∩ keywords), state 5 = **PP 8000** (vertical, no keywords), state 4 = **MI 3333–6665** (bucket-not-vertical + keywords), state 3 = MI **not biddable** (bucket, no keywords — no DS leaf matches), state 2 = **MaxReach** (keywords outside bucket; sheet logs score NULL, Venn says 1–3332 random), state 1 = not in audience. **Biddability = the expression contains a DS leaf matching the state**: keyword states need DS19, bucket/vertical states need a vertical anchor (DS13/DS46). That is WHY the shipped "Peak Performance" product pairs DS13 with DS19 — the anchor is required to reach the 8000-tier (vertical-no-keyword) IPs. HI/PP/MI/MaxReach are **IP score tiers, not campaign types**; per the component naming above, "Peak Performance" also names the vertical-anchor component itself (DS13 = v1, DS46 = v2).
- **Bucket vs vertical inside DS13**: bucket = **3-digit** DS13 segment ids (industry), vertical = **6-digit** DS13 segment ids (subindustry). (Same sheet; matches [[reference_fangorn_two_model_passes]] — Fangorn's HI + PP passes replace these two membership tests with continuous scores.)
- **BUT: live expressions carry ONLY the vertical id — bucket ids appear in ZERO live DS13/DS46 leaves (verified 2026-07-08, all 4,610 live prospecting campaigns).** The leaf is `{"data_source_id":13,"category_ids":[<6-digit vertical>]}` (or `:46`, identical shape), and the id MATCHES the campaign's `score_type:rtc` id (= `fpa_advertiser_verticals` vertical). So bucket-vs-vertical is a SCORING-side concept only (bucket-not-vertical membership ⇒ MI tier) — never a config axis. **The whole config space is a 2×3 grid: keyword layer (DS19 y/n) × vertical-anchor generation (none / DS13 legacy tier scoring / DS46 Fangorn)** — 6 cells, exactly the 6 observed combinations. DS13 and DS46 are the same slot, two generations — which is WHY they never co-occur.

- **The two empty cells are exactly DS13∧DS46** — empirical confirmation of the candidacy rule above: the `onFangorn` flip SWAPS DS13→DS46 at segment level, so they never coexist. **DS19 survives the flip** (DS19+DS46 = 18.9% of spend).
- **Every other combination exists**: DS46 without DS19 (235 campaigns / 115 advertisers — former vertical-only audiences now Fangorn-flipped), DS19 without DS13 (the dominant state), DS13 without DS19 (57 campaigns — legacy vertical-only, not yet flipped).
- **Implication: "MM = DS19" misses DS46-only (6.5% of spend) + DS13-only (1.1%) — ~7.6% of prospecting spend / ~157 advertisers that ARE MM-batch-scored.** The robust segment-level MM test is `DS19 ∪ DS13 ∪ DS46` (the TI-1037 dashboard rule). Layer matters too: the TEMPLATE level (`audience.audiences`) still shows DS13/DS19 after a Fangorn flip, so a template-level DS19 check and a segment-level one disagree on flipped advertisers.
- **No-MM cell (26.8% of spend) composition**, top sets by spend: DS14-only run-of-network ($2.5M / 42 adv / 128 campaigns), 3P-only (DS35 LiveRamp IP, DS18 Dstillery, **DS17 = ShareThis** — newly decoded), IP lists (DS8), 1P (DS2), CRM excludes (DS4/47), own-funnel excludes (DS21/34 — 198 campaigns run bare DS14+21+34, i.e. untargeted reach with visitor/converter suppression).

**Canonical rollout-priority scorer (Alex Knorr):** `databricks_targeting` repo, branch `aknorr/fangorn`, `fangorn/rollout/Fangorn Rollout Advertiser Campaign Merge.ipynb`. This is the authoritative implementation of the 4-criteria ranking (supersedes the Slack-paraphrased version). Mechanics worth knowing:
- **Universe / gating** (an advertiser is only *rankable* if they pass all of these): `campaign_groups.campaign_group_status='LIVE'` AND `objective_id IN (1,3,5)` AND `campaigns.campaign_status_id IN (1,3)`, then the campaign must have a **sustained `threshold > 0`** (held ≥60 min) in `silver.archives.household_score_threshold_archives` over the analysis window (e.g. 2026-03-01→2026-06-02). **No sustained HHST>0 in the window ⇒ no row ⇒ unrankable.** This is why a geo-only/HHST=0 advertiser (iMemories) simply doesn't appear — confirmed: their HHST archive in-window is empty.
- **Vertical source = `integrationprod.fpa_advertiser_verticals` WHERE type=1** (NOT `advertisers.advertiser_vertical_id`, which is often NULL even when a vertical exists). iMemories = vertical **116001 "Gifts & Specialty Stores"** there (matches the `rtc id:116001` in their expressions).
- **Score Opportunity / Size Stability / HHST-relief are computed at the VERTICAL level**, not per-advertiser audience: from `gs://mntn-data-archive-prod/fangorn_14day_lookback_vertical/dt=<snap>` (per-IP `model_score`) ∩ `vertical_categorizations/ip_vertical_associations`. Key per-vertical stats: `assoc_median_fangorn_score` (median Fangorn raw score of the vertical's associated IPs — <0.80 = clear win, 0.80–0.865 = incremental, >0.865 = "nervous") and `ratio_high_mid_vs_assoc` (Fangorn high+mid IPs ÷ associated IPs — >1 means the pool grows under Fangorn). **So "if reactivated, where would advertiser X rank?" is mostly a question about X's *vertical*'s Fangorn distribution**, not a per-advertiser IP scan (don't scan `household_scoring__prospecting_intent__v1` per-advertiser — it's a slow external full-year scan; use the vertical aggregate the notebook builds).
- **Weights:** HHST×ratio 30% · Score Opportunity 25% · Size Stability (asymmetric, peak at ratio=1) 20% · **Scale = log(`campaign_group_budget`) 25%** (budget, NOT `all_facts` media_spend — so the iMemories $0-spend quirk doesn't affect this criterion).

### Some advertisers log ~$0 spend in the fact tables despite millions of impressions
`summarydata.all_facts` / `sum_by_*_by_day` (media/data/platform_spend) and `cost_impression_log.media_cost` can be **≈0** for an advertiser even with large real impression volume. Verified for iMemories (AID 37423): ~$0 spend in **every** month across **both PTV (2025: 5–7M imps/mo) and Select (2026)** — so this is an **advertiser-level pattern, not a product (Select) pattern.** Cause not confirmed (likely a managed-service / agency / house billing arrangement where media cost isn't recorded in the standard spend path). **Implication:** never treat `all_facts.media_spend = 0` as "no delivery," and don't use it for the Fangorn "Scale/Budget" criterion on such advertisers — cross-check impressions and, for Select, the Select/PMP invoicing tables (`invoice_select_publisher_invoices`, `mntnselect_offering_versions` CPMs). (Whether this $0-spend pattern is widespread or iMemories-specific is unverified — flag before generalizing.)

### Pause Ads (PTV feature, not Select)
"Pause Ads" is a separate product feature that runs on PTV campaign groups (`product_id = 1`) but
uses PMP deal inventory via `core_campaign_group_x_private_marketplace_deals`. Identifiable by
campaign group name patterns like "Pause Ads", "Pause Ad Campaign", "EX-49 Pause Ads V2". 28 such
groups exist as of 2026-05-05. **Do not confuse with MNTN Select** — different product line,
different `product_id`. (Verified 2026-05-05.)

### RTC (Real-Time Conquest)
Real-Time Conquest is a CTV prospecting targeting mode. The bidder targets IP addresses identified
as households actively watching competitors based on real-time data.

**How to identify RTC impressions:**
- In `logdata.cost_impression_log`, filter: `model_params ~ 'realtime_conquest_score=10000'`
- Campaign filter: `funnel_level = 1` (pure prospecting), `channel_id = 8` (CTV)
- `data_source_id = 19` = RTC data source
- RTC GA release: August 13, 2025

**IVR, CPM, CPV** are the primary KPIs for RTC monitoring. RTC impressions generally show higher
IVR than non-RTC on the same campaign segment.

### NTB (New-to-Brand) — Definitive Clarification
`is_new = TRUE` means the IP/household has not had a prior page view or purchase for that advertiser
**within the client-side JavaScript pixel's lookback window.**

**CRITICAL:** `is_new` is determined by a **client-side JavaScript pixel** (not a backend table lookup
and not auditable via SQL joins). The pixel fires and checks the browser's first-party data.
This means:
- The NTB flag is NOT derived from MNTN's internal data (no SQL query can reproduce it exactly)
- 41–56% disagreement between `clickpass_log.is_new` and `ui_visits.is_new` across advertisers —
  this disagreement is real and expected; they represent different evaluation points
- Cross-device visits are the primary driver of NTB misclassification (61.2% mutation rate when
  cross-device is involved)

### Pre/Post Analysis Pattern
Standard pattern for measuring feature release impact:
1. Define a release date (e.g., July 15/22, 2025 for vertical classification changes)
2. Require 7+ days of data in both pre and post windows
3. Use `summarydata.sum_by_campaign_group_by_day` (Greenplum) for daily rollups
4. Use `audience.audience_segments` with `expression_type_id = 2` (TPA) to filter campaign groups
5. Use `fpa.advertiser_verticals` with `type = 1` for primary vertical only
6. Use `dso.valid_campaign_groups` to filter to valid (active, non-test) campaign groups
7. Use `r2.advertiser_settings` to filter on `reporting_style = 'last_touch'` when attribution matters
8. Use `competing_*` columns in visit_facts/conversion_facts for non-last-touch advertisers

### Jaguar / DS13 / Audience Intent Scoring
Jaguar is MNTN's IP scoring model that predicts household purchase intent.

**Architecture:**
- Input: `bronze.raw.tmul_daily` → membership DB → bidder
- Scores stored in `cost_impression_log.model_params` as key=value pairs (e.g., `score=0.8523`)
- `data_source_id = 13` — canonical `data_sources.name` is **"MNTN Vertical Categorization"**. In war-room language (Bryce Wagg, 2026-04-22) this DS is called **"Peak Performance"** and carries the Jaguar intent scoring. Same DS id, two names depending on the audience — code = Vertical Categorization, product = Peak Performance.
- **Canonical Peak Performance segment-level detector (TI-896, verified):** expression must contain ALL THREE of `"score_type":"rtc"`, `"data_source_id":13`, `"data_source_id":19`. Neither DS13-alone nor DS13+DS19-without-RTC is sufficient — DS13-alone over-counts by ~12pp (legacy Vertical Categorization use); DS13+DS19-without-RTC has ~1% false-positive baseline (legacy hybrid Interest+Keywords audiences that predate PP). All three signals together give near-zero pre-Oct-2025 baseline, matching the formal launch date.
- **Template-level (in `archives_audiences_archives`) uses the compact `{"interest":{"include":[{"or":[...DS13...DS19...]}]}}` schema**; segment-level (`archives_audience_segment_archives`) uses the translated `{"select":[...],"categories":{"where":{...}},"geos":{...}}` form, which adds auxiliary DS ids (DS14 global flag, holdout MD5 bucketing). Structural "pure DS13+DS19" checks only work at template level.
- Scores applied at bid time — not stored long-term in BQ event tables
- Pipeline is DS13 (not DS2 or DS4)

### Ecommerce Classifier
Domain-level ecommerce classifier that assigns an `ecommerce_score` to each domain.

**Key details:**
- Input data: `s3://mntn-data-archive-prod/site_visit_signal_batch_ecommerce_test/classified_data/dt=<date>/hh=<hour>/`
- 251M website visits used for training/evaluation
- `registered_domain` column is the join key
- Recommended thresholds: P90 ≈ 0.9181 (whitelist), P10 ≈ 0.0002 (blocklist)
- Downstream work: TI-200 whitelist/blocklist uses these thresholds

### Site Visit Signal & DDP vendors (TI-1027, 2026-06-16)
The **site-visit-signal pipeline** is the substrate feeding MNTN Matched's domain→vertical layer. Lineage
(`SteelHouse/airflow-ti`):
- **Raw vendor drops:** `gs://mntn-data-partners/partners/{5x5,33across,predactiv,cybba}/…` (external) + internal
  `guid_log`, `augmentor_log`. 5x5's raw path: `partners/5x5/ip_to_url/y=/m=/d=/h=/*.parquet`, cols `_COL_0`=ip,
  `_COL_1`=url, `_COL_2`=epoch(sec); delivered in ~2-hour batches.
- **Processing:** `spark/fpa/dsid{NN}_*_processing.py`, DAG `fpa_site_visit_batch_serverless` (`@hourly`, Dataproc
  serverless). `ENABLED_DSIDS = [23,25,26,28,30,36]`; per-DS lag hours (5x5=5h, augmentor/guid=1h, 33across=8h).
  Two outputs per vendor: stage-1 `gs://mntn-data-archive-{env}/fpa_vendor_log/data_source_id=NN/` (raw archive),
  stage-2 `…/signals/site_visit_signal/dt=/hh=/data_source_id=NN/`.
- **Unified `site_visit_signal` schema** (all vendors): `uid, advertiser_id, ip, url, query_parameters, user_agent,
  time, data_source_id, dt, hh`. **Separable by `data_source_id`.** ~250 GiB/day total. The BQ
  table `…zzz_temp.site_visit_signal` is **manual / not auto-populated** (populate DAG trigger commented out).
  - **How to query it (no BQ table, read-only, no DDL):** point a BQ *temporary external table* at the GCS parquet —
    ```
    URIS=""; for d in 09 10 ... 15; do URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-06-${d}/*.parquet,"; done; URIS="${URIS%,}"
    bq query --external_table_definition="svs::PARQUET=${URIS}" 'SELECT ... FROM svs ...'
    ```
    Target `*.parquet` (skips `_SUCCESS` markers). List one `dt=` prefix per day to window/prune (no Hive auto-partition
    needed — `dt`, `hh`, `data_source_id` are real columns in the files). ~1 day ≈ 285 GB / ~16 s scan; ~30 days ≈ 8.5 TB.
    `NET.REG_DOMAIN(url)` = registered domain (matches the consumer's tldextract eTLD+1). Same pattern works for
    `fpa_vendor_log` and any GCS-parquet dataset lacking a BQ landing.
- **Field population by source (AUDI-1089 q1b, hour slice 2026-07-01 hh=12):** `ip`/`time`/`uid` = 100% for
  every source; `url` ~100% everywhere except guid_log 79.9% (33A API 97.9%, Cybba 99.6%);
  **`query_parameters` = 0% for ALL sources (dead column)**; **`advertiser_id` populated ONLY by DS23 guid_log**
  (internal, 100%) — every external vendor sends 0; **`user_agent` only from 33Across/Sovrn/33A API (~100%) +
  internal guid_log/augmentor (~99.5%)** — Justuno/5x5/Predactiv/Cybba/Klickly send none. URL richness (30d median
  % with path): Klickly 100 (all Shopify product/checkout URLs), Sovrn 92 (**modal URL malformed — doubled
  protocol `https://mail.yahoo.comhttps://…`**), Justuno 91, Cybba 79, Predactiv 75, 33Across 68 (webmail-heavy),
  **33A API 26 (modal = openwebmp.com RTB endpoint — ad-infra, not user browsing), 5x5 4 (domain-only feed)**.
- **Content quality by source (AUDI-1089 q1c, same hour slice):** **Sovrn (DS33): 77% of URLs malformed —
  doubled protocol — and fail `NET.REG_DOMAIN`; only ~23% of the feed is usable by the
  domain classifier** (vendor-side bug, not sporadic). Pattern: `https://<bare-domain>` + the full URL
  concatenated (e.g. `https://mail.yahoo.comhttps://mail.yahoo.com/?n=1`) — the second half is a VALID url,
  recoverable by splitting on the second `https://`. **CAUTION: internal augmentor (DS30) shows the SAME doubled-protocol pattern at 1.2%** (e.g. tripadvisor.com doubled) — the malformation may be introduced in shared MNTN-side svs processing rather than (or in addition to) vendor-side; check the raw vendor drop (`gs://mntn-data-partners/partners/…`) before filing the vendor bug report. Cybba (DS36) fails differently: 6.2% truncated hosts (`https://www.drudgerep/`); Predactiv 0.7% embedded whitespace. Klickly (DS39) = 94% `myshopify.com` reg-domain (98%
  top-5; 117 distinct reg-domains but **629 distinct store hosts**/hr — NET.REG_DOMAIN collapses
  Shopify store subdomains, so host-level diversity is ~5x the reg-domain count). Hosts-vs-reg-domains ratio per source is now a standing q1c metric (hosts >> domains ⇒ subdomain-structured feed). 5x5 (DS25) = 53%
  `outbrain.com` (widget network) + 1.6% Googlebot IPs. 33Across (DS28): 6.4% of rows from Googlebot IPs
  (66.249.x) + 5.7% bot UAs. 33A API (DS40): top-5 domains 58% incl. openwebmp.com RTB endpoints. Clean
  everywhere: uid ~unique per row, no timestamp batch-stamping, private/reserved IPs ~0.
- **Consumer-side filters (what junk actually survives — Ryan Kleck Slack + airflow-ti code, 2026-07-10):**
  - **Vertical/DS13 path:** `aug_log_ip_vertical_id_hourly.py` hard-excludes `BLOCKED_DOMAIN_NAMES =
    ("yahoo.com", "aol.com", "easybrain.com")` (applied to registrable domain post-tldextract) + an
    ecommerce blocklist CSV (`…/vertical_categorizations/ecommerce_domain_whitelist/ecommerce_blocklist.csv`).
    So 33Across's 25% mail.yahoo.com never reaches DS13. **DS19 (keywords) treatment of yahoo.com is
    UNVERIFIED** — Ryan: "we might use it???" — check the MM keyword path.
  - **svs feature model** (`site_visit_signal_advertiser_id_dsc_id.py`): excludes DS23, drops
    steelhouse.com / googlesyndication.com / gtm-msr.appspot.com URLs, keys on `urlsplit().hostname`.
    NOTE: urlsplit does NOT validate TLDs — Sovrn's doubled-protocol URLs are NOT dropped here; they
    produce garbage hosts like `mail.yahoo.comhttps` (they die later at classification, since garbage
    never enters wcv).
  - **MemDB membership log** ("what we actually put into MemDB", per Ryan):
    `gs://mntn-data-tpa-{env}/tpa_membership_update_log/v2/dt=/hh=/` (monitored by
    `dags/monitor_memdb_batch_output.py`).
  - **Billing follows USE, not delivery (Ryan) — but junk DOES get billed (empirical, AUDI-1089 q1d):**
    vendors are credited only when their data lands on MM-targeted serves, and the consumption funnel is
    tiny (June 2026: 0.23–6.8% of delivered rows billed; 0.57–7.9% of delivered domains billed). HOWEVER
    the billed-domain lists prove junk survives to billing: **Sovrn's top billed "domains" ARE the
    malformed garbage hosts** (msn.comhttps 3.3%, biblegateway.comhttps 2.3%, yahoo.comhttps 2.2% of
    attributed imps); **33A API's top billed domains are cookie-sync endpoints** (cookies.nextmillmedia.com
    9.2%, sync.programmaticx.ai 8.2%); **www.yahoo.com IS billed for 33Across** (1.9%) despite the DS13
    block → reaches billing via another path (DS19?). "Fail to parse → not paid" only holds for
    NET.REG_DOMAIN-style parsing; urlsplit-surviving garbage is scored and PAID. An "Invalid URL Alert"
    email also exists for parse failures (not found in airflow-ti clone — lives elsewhere).
  - **Meter grain gotcha:** `usage_reporting_data.dt` = month-end snapshot ONLY (last day of
    reporting_month) — mid-month dt filters return zero rows. `domains.list` (billed domains RECORD) is
    populated only for MM site-visit CPM vendors (24/28/33/36/40); imps-with-domain-attribution: Justuno
    80%, Cybba 86%, but only ~48–55% for 28/33/40 (rest = unattributed aggregate credit rows).
  - **33Across provenance (Ryan, UNVERIFIED):** believed to resell the same Magnite auction/bidstream data
    we already receive — would explain its webmail-heavy, low-uniqueness profile (30% unique domains).
- **BILLING HARD LOGIC (AP-3779 + Victor via Ryan Kleck, 2026-07-10):** credit goes to the **FIRST DDP to
  report an (ip, url/composite_key) for a given date — paid only if the signal is used for targeting**
  (grain per Ryan on AUDI-647: one ip × composite_key per day). The row-level "used" table is
  **`data_archive_prod.targeted_signal` — ATHENA/AWS only, no BQ/GCS copy**: `uid, ip,
  data_source_category_id, time, data_source_id, source_data_source_id, dt`; a subset of svs where dsc_id
  is derivable (via `product_categorization`); svs→ts is 1:many; prod since 2025-05-02, DS19 backfilled
  from 2025-04-20. Billing chain: svs → targeted_signal (used rows, source_data_source_id = credited DDP)
  → `prod.mntn_matched.mntn_matched_reporting` (Athena) → `coredw.usage_reporting_data` (BQ, month-end
  snapshots) → monthly vendor payout (contact: **Maya Triman**). The "1/N split on shared IPs" note may be
  a different layer (serve-level credit split) — impressions decimals exist; exact interplay with
  first-reporter-wins UNVERIFIED.
- **Augmentor displacement (AUDI-647 + airflow-ti git, 2026-07-10):** DS30 augmentor_log added to svs
  2026-05-07 (re-enabled with dedupe 05-12, `fpa_vendor_log_batch_ingestion_consolidated.py`). Under
  first-reporter-wins, our free Magnite-derived augmentor rows now displace DDP credits for overlapping
  (ip,url) — Ryan's Apr estimate: save ~$17K/mo (33Across) + ~$4K/mo (33A API); actual June-vs-May bill
  drops: **33Across −$19K, 33A API −$9.7K** (first fully-displaced month after 30d signal aging). The
  Magnite-resell overlap is real and already partially defunded. AUDI-647 method: match svs rows to
  augmentor by ip + canonical page/referrer (strip query string).
- **ENABLED_DSIDS gotcha:** the consolidated ingestion DAG (`fpa_vendor_log_batch_ingestion_consolidated.py`)
  runs [23,25,26,28,30,36] as of the Jun 9 clone, yet DS33/39/40 appear in svs — a second ingestion path
  exists for Sovrn/Klickly/33A API (UNVERIFIED which).
- **"Monthly Summary by DDP.xlsx"** (from Maya via Ryan — **SENSITIVE: local only, gitignored, never post
  to Jira**): usage_reporting_data rollup Dec 2025–Mar 2026, metered vendors only (LiveRamp reported as
  DS(11,35) combined). Confirms the meter IS the payment basis; flat fees (25/26/39) still not in any table.
- **Consumers:** `distinct_site_visit_signal_domains.py` (31-day read; regex-strips url to `protocol+domain`;
  **excludes DS23**, includes DS25) → OpenAI `ddp_vertical_classification_api` → `update_website_verticals.py` →
  **production domain→vertical table** `gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/`
  (cols `domain_name, vertical_id, vertical_name, bucket_id`; ~1.42M classified domains) → feature store
  `site_visit_signal_advertiser_id_dsc_id` → `mntn_match_incrementals_submit` (MNTN Matched scoring).
  **Implication:** site-visit DDP value lives in distinct DOMAIN→vertical coverage, not IP reach; and only the domain
  matters (URL path is stripped), so a domain-only feed loses nothing on this path.

### `tpa.direct_data_partners` (vendor billing/usage registry)
View `dw-main-silver.tpa.direct_data_partners` (filter `is_current=true`). Key cols: `data_source_id`,
`data_partner_name`, **`billing_type`** (`flat_fee` | `fixed_cpm` | `variable_cpm`), `fixed_cpm`, `enabled`,
**`used_in_mntn_match`**, `used_in_interests`, `type`, `valid_from/to`, `notes`. This is the source of truth for
"what do we pay for which DS and does it feed MM." MM site-visit DDPs (`used_in_mntn_match=true`): 24 Justuno,
**25 5x5**, 26 Predactiv, 28 33Across, 33 Sovrn, 36 Cybba, 39 Klickly, 40 33Across API (27 LaunchLabs disabled).
**Peer MM-DDP rate = $0.50 CPM** (28/33/36/24/40); 5x5+26+39 are `flat_fee`. Interest-side (11/35 LiveRamp,
17 ShareThis $0.95, 18 Dstillery) and CRM (22 Experian flat_fee, 29 deepsync) are separate, not MM.
**Note:** `ds_catalog.md`'s 0/0/0 "no current use" for these reflects IPDSC/prospecting-expression usage only —
the MM site-visit path is separate and active. (Corrected in ds_catalog for 24/25/26/28.)
**Notes-field finds (2026-07-10):** 5x5 (DS25): "we provide report but only impression counts - unknown if
this was shared with the customer" (relevant to its renewal); ShareThis (DS17): "(starting May usage,
previously $1.2)" — a live rate cut recorded only in `notes`. Other columns: `go_live_date`,
`external_reporting_required`, `report_under_data_source_id`, `primary_data_source_id`.
Canonical roster+cost query: `tickets/audi_1089_ddp_vendor_evaluations/queries/canonical/q0_roster_cost.sql`.
**Gotchas (verified 2026-07-09):** rows appear in **duplicate pairs even with `is_current=true`** (CDC dupes —
`SELECT DISTINCT` or dedupe on `data_source_id`+`valid_from`). **DS26 (Predactiv) has a broken SCD:** FOUR
`is_current=true` rows with conflicting `used_in_mntn_match` (two true @ valid_from 2025-10-17, two false @
2025-01-01) — take the row with the latest `valid_from` (MM=true is correct; DS26 delivers into site_visit_signal
daily). Column is `data_partner_name`, not `partner_name`.

### `coredw.usage_reporting_data` — the DDP metered-billing table (AUDI-1089, 2026-07-10)
`dw-main-bronze.coredw.usage_reporting_data` (cols: `dt`, `data_source_id`, `impressions`, `usage` ($),
`reporting_month`, `domains` RECORD, `segment_name`, …) is **the actual DDP usage meter — for ALL CPM-billed
DDPs, not just MM site-visit** (corrected 2026-07-10, canonical q0): MM 24/28/33/36/40 @ $0.50; interests
**17 ShareThis @ $0.95** and **35 LiveRamp IP (variable_cpm, implied $1.19–1.32/CPM, $243–446K/mo ≈ $3.4M/yr
run-rate — the single largest DDP bill, ~4× the whole site-visit CPM roster)**; CRM 29 deepsync @ $0.50.
Generalized meter math: **`usage = impressions × (registry fixed_cpm / 1000)` exactly** per month (Jan–Jun
2026, every fixed-CPM source); `impressions` carries decimals = 1/N credit shares. ShareThis registry notes
say the rate dropped $1.20 → $0.95 "starting May usage", yet the meter implies $0.95 for ALL of Jan–Jun 2026 —
history appears restated at the current rate (or the note's timing is off); don't trend ShareThis dollars
across the rate change without checking. The meter
(bae-sql-utility "ddp/usage reporting") credits vendors on **MM-targeted serves** (via targeted_signal DS13/19,
30-day lookback) with a **1/N credit split across co-matching vendors** — i.e. per-use billing accrues on
SHARED IPs ("waterfall on usage basis", per Paulo). Monthly per-domain reports are emailed to each vendor from
partnerbilling@ (`ddpmonthlyusageemail-<Vendor>.py`). **Jun 2026 run-rates:** 33Across $35.2K/mo, 33Across API
$14.7K/mo, Sovrn $9.7K/mo, Justuno $6.4K/mo, Cybba $1.8K/mo ≈ **$812K/yr total CPM-vendor spend** (declining
Apr→Jun). Flat-fee vendors (5x5/Predactiv/Klickly) are NOT here. Use `reporting_month` for month rollups.
Other notes: **Justuno DS24 ingest is now a dedicated hourly S3 file-drop** (s3://mntn-data-partner-justuno →
EMR → fpa_vendor_log), pixel-topic path is legacy. **Sovrn (FMX) is separately a PMP inventory partner**
(gary-ql core.partners id 68) — DS33 data-feed decisions don't touch inventory deals.

### Site-visit vendor raw schemas, richness, and the "discard" finding (TI-1027, 2026-06-17)
Each vendor's RAW feed schema (from the airflow-ti processing jobs) vs what we KEEP in `site_visit_signal`
(only `ip, url, user_agent, time`):
- **5x5 (25):** raw parquet `_COL_0`(ip), `_COL_1`(url), `_COL_2`(epoch sec) — **positional, no column names, no
  metadata, no user_agent.** Thinnest feed + **schema-fragility risk** (a vendor-side column reorder silently
  corrupts ingestion). url is **96% bare domain** (only 3.8% carry a path).
- **Cybba (36):** ip/url/time, no user_agent. **Predactiv (26):** sends `userAgent` but the job NULLs it.
- **33Across (28):** TSV.gz TIMESTAMP/CLIENT_IP/USER_AGENT/PAGE_URL — keeps user_agent.
- **guid_log (23, internal):** ip/product_referer/query/ua_raw/advertiser_id/time — richest. **augmentor (30,
  internal):** ip/ua/page/referrer/placement.
- **Pixel vendors (Justuno 24, Sovrn 33, Klickly 39, 33Across API 40):** raw `pixel_page_view_signal` carries
  `event_id, mobile, query_str (incl. referer, user_agent, GPP consent), referer, url, user_agent` — but
  **`site_visit_signal` drops event_id/mobile/query_str/referer.**
- **DISCARD FINDING (full magnitude — TI-1027 raw audit 2026-06-17):** `site_visit_signal` keeps only
  `ip, url, user_agent, time`, but the **raw dumps carry far more** that we drop at ingestion:
  - **Predactiv = 26 raw cols, we keep 4** in site_visit_signal. Dropped THERE: full geo (`geo_city/postal/dma/...`),
    `domain_industries` (**firmographics — B2B**), `concepts`/`keywords`/`entities` (pre-computed topic
    classification w/ confidence scores), `deviceType`/`os`/`browserFamily`.
    **CORRECTION (AUDI-1089 lineage sweep, 2026-07-10): the hashed emails are NOT dropped** — a separate
    severity-1 hourly DAG (`hashed_email_ds_26_signals`) explodes `hem_sha256` from the raw Predactiv feed into
    `hashed_email_signal`; `HEMSignalReader` hardcodes DS26 among 5 HEM sources feeding tpa_export/IPDSC
    CRM-identity resolution. **Predactiv is the only site-visit DDP with a hard non-MM production dependency** —
    dropping it breaks a CRM/identity input, not just MM signal.
  - **33Across = 32 raw cols (TSV.gz), we keep 4.** Dropped: `PAGE_CATEGORY(_KEYWORDS)` ×2, `TITLE`, geo
    (`ZIP/DMA/REGION/MAXMIND_GEO_ID`), device client-hints (`SEC_CH_UA_*`), `LANGUAGE`, **consent (`GPP/GPC/US_PRIVACY/DNT`)**.
  - **Pixel feeds (24/33/39/40):** drop `event_id/mobile/referer/query_str` (incl. GPP consent).
  - **5x5 (ip/url/epoch) & Cybba (ip/url/time) are genuinely thin** — nothing to tap without asking the vendor.
  - **So most "more value" is a PIPELINE change (parse cols we already pay for), not new vendor cost.** Also a
    **compliance flag**: consent fields (GPP/GPC) arrive raw and are dropped — confirm downstream handling.
  - **Raw dump roots** (Sean Yang): batch `gs://mntn-data-partners/partners/<vendor>/`; streaming
    `gs://mntn-data-archive-prod/pixel_page_view_signal/` (rawer JSON: `gs://mntn-analytics-raw/topics/pixel-page-view-signal/`).
    Full audit map + per-vendor columns: `tickets/ti_1027_5x5_data_evaluation/artifacts/ti_1027_raw_data_audit.md`.

### DDP billing base = per 1,000 **impressions served** (TI-1027, user-confirmed 2026-06-17)
The `fixed_cpm` (e.g. $0.50 for the MM-DDP peers) is **cost per 1,000 impressions served**, and the per-impression
cost is in **`cost_impression_log`** (`media_spend`, `data_spend`, `platform_spend` per impression). A vendor's
market-equivalent cost ≈ (impressions its data touches) × CPM / 1000 — measurable by joining the vendor's IP set to
CIL (5x5 touches ~34.35M impr/day → ~$6.3M/yr CPM-equivalent ceiling). Flat-fee vendors (5x5, Predactiv, Klickly)
pay fixed regardless of volume. Reusable valuation method: `documentation/docs/data_vendor_valuation_framework.md`.
- **Empirical CIL impression economics (2026-06-15, 56.6M impr/day):** **media ≈ $10.74 CPM** ($0.0107/impr — normal
  CTV), **data ≈ $1.07 CPM** ($0.00107/impr). Confirms `fixed_cpm` is **per 1,000** (per-person would be ~1000×). Use
  these as the per-impression cost anchors for any "value of impressions" calc.
- **"Per 1,000 of WHAT" — pin this down in any CPM negotiation:** *impressions served* (the ad-tech CPM, what CIL
  uses — 5x5 ≈ 34M/day) vs *records delivered* (a data-licensing CPM — 5x5 ≈ 93M rows/day ≈ 2.8B/mo). Same "$0.50 CPM"
  on records is a far bigger base than on impressions. Flat-fee sidesteps it; CPM quotes must specify the base.

### site_visit_signal has NO TTL; targeting uses a 30-day window — measure uniqueness over 30 days (TI-1027 + Ryan Kleck, 2026-06-17)
`site_visit_signal` is **daily dt partitions with no TTL** — data goes back to 2025-08-31 and grows unbounded (Ryan:
"we should probably put a TTL on it"). **But targeting only uses the last ~30 days.** Implications:
- Any query against `site_visit_signal` must **filter `dt`** or it silently mixes long-expired data.
- **Vendor uniqueness/overlap must be measured over the 30-day targeting window, not a short snapshot.** A 7-day
  snapshot *overstates* cross-vendor overlap: vendors deliver on irregular cadences, so a pair "also seen elsewhere"
  may have been delivered weeks ago and is about to expire. The targeting-truthful metric is **sole-or-freshest
  within 30 days** (per (ip,domain): does any *other* vendor deliver it within the window, and who's most recent?).
- 5x5 example: 7-day snapshot = 77% unique pairs; over the 30-day window, split four ways — **69.8% sole** +
  **1.2% 5x5-freshest = ~71% irreplaceable**; **24.4% tied** (another vendor same-day — a copy survives, so NOT a
  clean win); **4.6% other-fresher**. "Overlap ≠ covered, but a same-day tie *is* covered" → honest floor ~71%, not
  the 95% that lumped ties in. Bake this 4-way split into any vendor-overlap analysis.

### Site-visit vendors are ADDITIVE, not redundant (TI-1027, 2026-06-17)
Across all ~447M distinct (IP×domain) pairs in a day, **76% come from exactly ONE vendor** (16% from 2, 7% from 3+).
Per shared IP, stacking vendors *adds* domains: an IP seen by N vendors has ~70% more unique domains than the best
single vendor provides (e.g. 5-vendor IP: 11.1 union vs 6.7 best-single = 1.65×; overlap only ~15–29%). So the value
of running multiple site-visit vendors is real — they contribute net-new household→site observations, they don't
just re-report the same ones. **Value metrics must be on distinct (IP,domain) anchored on the UNION, never raw
events (inflated ~2.8× by repeats) or the sum-across-vendors (double-counts the ~24% overlap).**

### Value a PAID site-visit vendor against the FREE internal baseline (TI-1027, 2026-06-17)
Two `site_visit_signal` sources cost nothing: **DS23 guid_log** (MNTN's own pixel) and **DS30 augmentor_log** (the
bidstream — a free byproduct of being in the auction; SSPs send bid requests, we don't pay for the signal). So the
correct value test for a *paid* vendor is **net-new vs DS23∪DS30 (the free baseline)** — not "unique vs all vendors"
(the other DDPs are also costs). For 5x5: of 33.1M daily (IP×domain) pairs, only **17.9% are already in the free
logs**; **82.1% net-new vs free, 72.5% net-new AND classifiable**. The unit is the **(IP×domain) pair = a site
visit** — an IP with no domain has no behavioral value; count distinct (IP, classifiable-domain), never raw IPs.
- **CAVEAT (Ryan Kleck, 2026-06-17):** DS30 augmentor was added to `site_visit_signal` only **~April 2026** — it is
  **absent in older partitions**, so the free baseline (and any vendor-vs-free comparison) is only valid on recent
  data. guid_log (DS23) confirmed; augmentor = DS30.

### MNTN Matched pipeline — DS13 (vertical) vs DS19 (keyword), verified flow (TI-1058, Ryan Kleck 2026-06-26)
Code: `SteelHouse/shopper_graph` dbt (`dbt/models/mntn_matched/`) + an `openai/` Batch job dir; DAGs in
`SteelHouse/airflow-ti` (`dags/machine_learning/mntn_match_incrementals_{submit,fetch}`). Both DSes read
`site_visit_signal` but are **two separate flows**:
- **DS13 (vertical) — cached, refreshed ~every few months, NOT daily.** distinct domains → e-commerce classifier
  (yes/no cutoff) → Common Crawl homepage HTML (`website_home_pages`) → OpenAI → vertical → stored
  `vertical_categorizations/website_crawl_verticals/` (domain→vertical, ~1.42M) → feature store
  `site_visit_signal_advertiser_id_dsc_id`. Run manually by Victor+Ryan. DS13 historically used the DS19 product
  flow's `industry` field but **no longer does**. (Exact DS13 file/DAG locations still being confirmed.)
- **DS19 (keyword) — daily, the OpenAI cost driver.** `product_uniques.py` strips query params (URL→`product_name`;
  `product_sku` hardcoded literal `1`; `composite_key = product_name_1`), **anti-joins on `composite_key`** vs stored
  → only new URLs → `openai_batch_input_raw.py` groups by (product_name, sku, domain) with
  `collect_set(data_source_id)`, builds a **gpt-4o-mini** Batch request (strict json_schema:
  product_industry/subindustry/**category**/subcategory; max_tokens 1000) → `openai_batch_input_formatted.py` keeps
  `rn=1` per `custom_id` → JSONL (~45K/file) → submit DAG (`submit_batch.py`, OpenAI Files+Batch API,
  `completion_window=24h`, batch_id→`openai_batch_submissions/`) → ~24h async → fetch DAG (`transition_batch.py` +
  `fetch_results.py`) → `openai_batch_results_joined.py` (join on `custom_id`) → `product_categorization_temp.py`:
  **DS19 keyword = the OpenAI `product_category` field**; Step1 exact match vs `product_category_reassignment`, Step2
  **BGE-large** (`system.ai.bge_large_en_v1_5/3`, 1024-d, local Databricks/free) vector search vs
  `etl_mm_taxonomy_vector_index` @ threshold 0.6, **Step3 auto-add new keywords is currently COMMENTED OUT**
  ("post migration" TODO) → `product_categorization` → `tpa_export`/`mntn_matched_taxonomy_bq`/reporting → DS19.
- **`data_source_id` does NOT multiply OpenAI cost.** Dedup is on `composite_key` (the query-stripped URL); a URL is
  sent to OpenAI **once** regardless of how many vendors report it. `data_source_id` is retained only for **billing
  attribution** to the source vendor. (augmentor_log DS30 duplicate URLs are absorbed by the anti-join.)
- **Known waste (→ TI-1060 cost work):** `product_sku` is always `1` (dead prompt tokens on every request);
  homepage-description join is hardcoded to `apollaperformance.com` only (enrichment off elsewhere — likely
  leftover/test); prompt has missing spaces; taxonomy auto-add disabled. BGE-large is free + already in-pipeline →
  candidate to replace gpt-4o-mini for many URLs.

### Vertical Classification
`fpa.advertiser_verticals` (Greenplum/BQ) stores the advertiser→vertical mapping.
- `type = 1` = primary vertical (use this for filtering)
- Vertical IDs: 101001, 119001, 120002 referenced in TI-221/TI-270 analyses
- **`advertiser_name` column is UNRELIABLE** — it's a denormalized, write-once field that is never updated. Two known issues:
  1. **Empty name regression (since 2025-12-23):** 79–82% of new advertisers have empty string. 4,366+ advertisers affected.
  2. **Stale names:** 7% of populated names differ from current `advertisers.company_name` (customers renamed after FPA row was created).
  - **Always JOIN to `advertisers.company_name`** for the current name — never use `fpa_advertiser_verticals.advertiser_name`.
- **Shortcut for vertical/bucket lookups:** `tpa.dim_vertical` in coredb (created by Ryan Kleck) — PK is `vertical_id`, pre-joined with bucket info (`bucket_id`, `bucket_name`, `vertical_bucket_name`, `verticals_in_bucket`). Use this instead of self-joining `advertiser_verticals` type=0 + type=1 when you just need the vertical→bucket mapping.

---

## IPDSC Pipeline & MES Architecture

### What is IPDSC
IPDSC (IP Data Source Category) is the process that maps IP addresses to audience segment
category_ids by data source. It is the bridge between HEM (hashed email) CRM uploads and
the IPs that the bidder actually targets.

**IPDSC data location (BQ):**
- GCS path: `gs://mntn-data-archive-prod/ipdsc/dt=<date>/data_source_id=<id>/`
- BQ external table: `dw-main-bronze.external.ipdsc__v1`
- Format: Parquet, partitioned by `dt` (STRING) and `data_source_id` (INTEGER)
- No expiration — historical data available

**How to query ipdsc__v1:**
```sql
SELECT DISTINCT ip, dscid.element AS category_id
FROM `dw-main-bronze.external.ipdsc__v1` t
  , UNNEST(t.data_source_category_ids.list) AS dscid
WHERE t.data_source_id = 4          -- DS4 = CRM
  AND t.dt = '2025-11-25'           -- choose a date during campaign flight
  AND dscid.element IN (17077, 17079)  -- audience_upload_ids / category_ids
```

**Key fact:** `category_id` in ipdsc__v1 = `audience_upload_id` in tpa.audience_upload_hashed_emails
= `data_source_category_id` in integrationprod.audience_uploads — these are all the SAME value.

### MES (Membership Enrichment Service) Pipeline
MES is the enrichment service that processes impressions and validates audience membership.

**IPDSC block list (data_source_ids never used for targeting):**
- DS 2 — OPM/real-time (blocked in MES inner join)
- DS 14 — (blocked)
- DS 42 — (blocked)

**35-day lookback** for non-Oracle data sources in MES.

**impression_enrichment.py**: The MES pipeline uses an inner join against ipdsc — IPs not in
the ipdsc file for the relevant data source are dropped. This is the root cause of HH discrepancy
investigations (TI-644, MM-44) where targeting audiences appear smaller than expected.

### Data Source (DS) Type Reference
| DS ID | Name | Type | In IPDSC | In tmul_daily | Notes |
|-------|------|------|----------|---------------|-------|
| 1 | Oracle | Legacy 3P | NO (legacy) | — | **Legacy Oracle DS; no longer in IPDSC** (Sean Yang, TI team, 2026-05-29). Still in MNTN's taxonomy so buyers can pick it on UI (may have been disabled by AUD — unsure). 553 active prospecting campaigns positively reference it / $5.97M / 30d — those clauses are likely dead-weight since no IPs deliver via Oracle. |
| 2 | MNTN First Party / OPM | Real-time | NO | YES | Always in tmul_daily; never in ipdsc block list |
| 3 | (Third Party) | — | — | YES | In tmul_daily |
| 4 | CRM | Batch upload | YES | NO | HEM → IP via Verisk identity graph; in ipdsc, NOT in tmul_daily rows |
| 9 | MNTN Campaigns (MNTN Select household audiences) | First-party non-pixel | — | — | Categories are "MNTN Select: Households Reached in [Deal Name]" — Andor, NFL Live + Playoffs, A Night at the Movies, etc. 86 audience-expression refs (50 prospecting + 36 multi-touch). MNTN-owned first-party data sourced from Select impression exposure; distinct from MNTN pixel and from advertiser CRM. |
| 11 | LiveRamp (legacy) | Deprecated 3P | NO (legacy) | — | **Deprecated old LiveRamp** (Sean Yang, TI team, 2026-05-29). Used device_id→IP mapping; DS35 replaced it (LiveRamp now sends IPs directly). Retained in `tpa.categories` only because reporting still needs the historical category references. |
| 13 | MNTN Vertical Categorization (bucket+vertical for MM 2.0) | Jaguar model | — | — | DS13 = bucket (industry) + vertical (subindustry). Score stored in `household_score` via MM scoring. Bryce-product-name = "Peak Performance". |
| 14 | MNTN Global Data | Freshness filter | YES | — | **Freshness / eligibility filter** (Sean Yang, TI team, 2026-05-29): auto-attached to all expressions; narrows eligibility to IPs MNTN has recently observed in `guid_log` (4-day window) and `augmentor_log` (1-day window). The 5 categories (Beeswax Bidder / Magnite / Index Exchange / IP Ends In .0 / ROOT) represent the source channels that count as "recent." Every audience expression implicitly only bids on IPs MNTN has recently observed via these streams. NOT bid routing as previously framed. |
| 16 | MNTN Taxonomy | Taxonomy | NO | — | Real-time; not in ipdsc. Per-advertiser identifiers + internal event taxonomy (PageViews/Conversions/etc.). |
| 19 | MNTN Matched (keywords for MM 2.0) | MM batch + parallel RTC | — | — | DS19 = keyword half of MM 2.0 state table. **MM and RTC are independent pipelines** producing similar outputs but via separate code paths (Sean Yang, TI team, 2026-05-29 revised): MM batch fills `household_score` via IPDSC scoring against DS19 keyword config; RTC fills `realtime_conquest_score` in real time when an IP matches recent-visit criteria. Both pathways implicitly use the advertiser's account-level DS19 keyword configuration. **Audience-expression `score` block** (Jordan Piepkow + Ryan Kleck + Matt Brorby, TI team, 2026-05-29 / 2026-06-01): the `select.score.types` array tells the bidder how to evaluate IPs for bidding. Each entry is `{score_type: "rtc" \| ..., id: <int>}`. **The `id` is a `vertical_id`** (Ryan Kleck): the RTC score gives 10K to IPs matching that vertical in real-time. **HHST gates effectiveness** (Ryan Kleck): "the score doesn't matter if the HHST is not set." So even though `score_type=rtc` appears in 99.9% of prospecting expressions, RTC only affects bidding when the campaign has HHST set. For campaigns without HHST, RTC is a no-op. **RTC is the first check in the bidder scoring waterfall** (Matt Brorby, 2026-06-01): RTC takes precedence — if RTC fires for an IP (realtime_conquest_score=10000), the bid happens via RTC regardless of Fangorn/MM/3P scoring on the same IP. Implication for measurement: to attribute an impression to a non-RTC signal (e.g., 3P), filter to `realtime_conquest_score != 10000` so RTC didn't fire on that impression. For Fangorn heterogeneous-effect analysis: filter to where Fangorn-score > HHST at time of bid. Causal impact analysis at the advertiser level (Fangorn-on vs Fangorn-off) doesn't need this filter — that's adversiser-level treatment, not per-IP heterogeneity. |
| 17 | ShareThis | Bought 3P interest | YES (65M rows/day) | — | One of only three "bought 3P" sources with material IPDSC volume (TI-999) |
| 18 | Dstillery | Bought 3P interest | YES (32M rows/day) | — | One of only three "bought 3P" sources with material IPDSC volume (TI-999) |
| 21 | MNTN Conversion | Real-time | NO | — | Conversion-based exclusions |
| 34 | MNTN Pageview | Real-time | NO | — | Page view-based exclusions. **Near-duplicate DS355420 "MNTN PageView" exists in the high-ID range — pixel team to confirm whether DS34 is being deprecated.** |
| 35 | LiveRamp IP | Bought 3P interest | YES (104M rows/day) | — | Current LiveRamp. IPs delivered directly (vs DS11 legacy device_id→IP mapping). Dominant 3P by volume. |
| 38 | MNTN UI Audience Keywords (BUK) | Queued MM signal | — | — | **Feature being rolled out, not yet active** (Sean Yang, TI team, 2026-05-29). 52.7M categories already loaded. **BUK augments DS19, does not replace it** (Alex Knorr, 2026-05-29): BUK leverages DS19 as an input and replaces the LLM-generated keyword pipeline, but DS19 (MNTN Matched V2) stays in production to handle cold-start (new advertisers/new keywords don't get BUK recommendations). Steady-state MM = DS13 + DS19 (V2) + DS38 (BUK) combined. |
| 42 | — | — | — | — | Blocked in MES |

### 1P / 3P / MM definitions (per Victor Savitskiy, 2026-05-28)

The 1P / 3P / MM distinction is about **who provided the data**, and which of them get scored by MNTN:

| Term | What it is | Provided by | Purpose | DSes (working set) | MNTN-scored? |
|---|---|---|---|---|---|
| **1P** | Customer/account data the advertiser uploaded | The advertiser | Retarget known customers | DS4 CRM, DS8 IP List, DS47 CRM Identity Graph | **No** |
| **3P** | Behavioral / interest segments bought from external data providers | External 3P vendor | Prospect against described interests | DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp IP | **No** |
| **MM** (Mountain Match) | MNTN's targeting product — IPs scored by MNTN's models using verticals, keywords, behavioral signals | MNTN | Prospect via MNTN-derived per-IP quality | DS13 Vertical Categorization, DS19 MNTN Matched V2 (LLM-derived keywords, current prod), DS38 BUK / UI Audience Keywords (queued — **augments** DS19, doesn't replace; DS19 stays for cold-start cases per Alex Knorr 2026-05-29), DS46 ML Audience Intent (Fangorn). **RTC vs MM = independent pipelines** (Sean Yang, TI team, 2026-05-29, revised reading): MM is a batch process that scores IPs through IPDSC and fills `household_score`; RTC is an independent real-time match-and-tag pipeline that fills `realtime_conquest_score` with a flat 10k when an IP matches recent-site-visitor criteria. They produce similar score outputs but through separate code paths. Sean's earlier framing "RTC is literally the same as MM, but real-time" was an oversimplification — they share the goal (per-IP intent scoring) but not the implementation. **AUD team to confirm definitively.** Steady-state MM = DS13 + DS19 + DS38 combined; RTC stays parallel. | **Yes** — produces `household_score` (batch) and separately `realtime_conquest_score` (RTC pipeline) |

**Empirical layering** (TI-999, 30d window ending 2026-05-28):
- **72% of 3P-only campaigns also use an MM signal in their expression**; those drive 83% of 3P-only impressions/spend.
- "Pure 3P, no MM" is the minority: 28% of 3P-only campaigns, ~17% of impressions/spend.
- 35% of 1P-only campaigns also use MM; they drive 52% of 1P-only spend.
- Implication: when you see "good" delivery on a 3P campaign, it's almost always MM doing the scoring underneath — 3P alone does not bring scored IPs.

**How the bidder combines signals** (per Victor Savitskiy, 2026-05-28):
- MM campaigns use **AND-type intersection** for targeting clauses: every filter (geo, 3P segment, MM signal) NARROWS the eligible IP set.
- Victor example: "if we add geo to campaign — it will narrow down scored audience." Same applies to layering 3P onto MM: 3P doesn't bring new IPs into the scored set; it narrows the MM-scored set to IPs that also match the 3P segment.
- Within a single source, categories can be OR'd (e.g., `"op":"or"` between LiveRamp segments). Top-level combination across sources is AND.
- Reinterprets the "3P+MM has 88% scored delivery" finding: this is MM scoring its already-scored universe, narrowed by 3P. NOT 3P pulling in scored IPs.

**Naming-pitfall warning:** in informal usage, "1P scoring" is sometimes said to mean MM scoring (because MM is MNTN's own scoring system, vs 3P which is bought). The strict Victor definitions above are canonical: 1P is the advertiser's CRM upload, NOT MNTN-derived scoring. If a conversation says "1P scoring" with no context, clarify whether they mean strict-1P (CRM) or MM. Per Malachi correction in Victor Slack thread, 2026-05-28.

### Bidder Scoring Reality (TI-999 empirical, 2026-05-28)

Every impression's `cost_impression_log.model_params` carries **three score fields** — they are separate scoring systems, not variants of one.

| Field | What it is | Distribution on 2026-05-26 (61M imps) |
|---|---|---|
| `household_score` | **General/main per-IP scoring system.** Graduated 0-10000. This is what "HI / PP / mid-band" actually means in the bidder. Applied broadly — not gated by audience-expression `score_type`. | 65.4% = -1 (unscored), 15.4% = 10000, 11.1% = 8k-10k (HI band), 3.3% = 5k-8k, 4.3% = 1k-5k (PP-ish), 0.6% = 1-999 |
| `advertiser_household_score` | **Per-advertiser scoring** (Mountain Match-style; advertiser-tuned). Mostly binary in delivery with a small graduated tail. | 70.2% = -1, 28.8% = 10000, 0.6% = 5k-8k, 0.4% = 1k-5k |
| `realtime_conquest_score` | **RTC — Real-Time Conquesting qualifier.** Binary BY DESIGN — applies to recent-site visitors only (not a graduated score, a qualifier flag). | 95.4% = -1, 4.6% = 10000 |

**1. `household_score` is the main scoring system** — graduated full-range, broadly applied. ~35% of all delivered impressions have a positive household score.

**2. RTC is binary by design and applies only to recent-site visitors** — don't confuse RTC's binary behavior with the bidder's general scoring (`household_score` is graduated).

**3. Audience expressions reference only `score_type=rtc` or have no score block** (270k active expressions: 82% rtc, 18% none). But `household_score` is applied by the bidder regardless of the expression — it's a system-level scoring layer, not opted-in per-campaign.

**4. 3P-using prospecting at the bucket level — but this is an ARTIFACT of mixing:**

| Campaign class | household_score = -1 | 8k-10k (HI) | 10000 (top) | Any positive |
|---|---:|---:|---:|---:|
| Prospecting + 3P | 33.2% | 23.5% | 22.5% | 66.8% |
| Prospecting, no 3P | 74.2% | 7.5% | 13.6% | 25.8% |
| Retargeting (CRM/IP-list) | 68.9% | 9.9% | 14.4% | 31.0% |

**Splitting prospecting+3P by whether it ALSO uses RTC reveals the actual driver:**

| Sub-bucket | Unscored (-1) | HI band (8k+) | Any positive |
|---|---:|---:|---:|
| **3P PURE** (no RTC, no BUK, no other internal targeting) | **73.6%** | 18.8% | 26.4% |
| **3P + RTC** | 12.0% | 60.3% | 88.0% |

**Pure-3P delivery (~74% unscored) is essentially identical to no-3P prospecting (~74% unscored).** When 3P is mixed with RTC, the scored share jumps to 88%, but RTC is pulling in the scored IPs — not 3P. 3P-only filtering does not preferentially hit scored IPs; the bidder ends up at roughly the same scored/unscored mix as a prospecting campaign with no scored-signal source at all.

**5. What's missing — per-segment quality scoring.** `household_score` ranks individual IPs. The bidder has no signal saying "this LiveRamp segment is higher-quality than that one" — that's the gap TI-956 / Alex's per-dscid composite scoring framework would fill. Per-segment scoring complements (not replaces) per-IP household scoring.

**6. Active bought-3P set is small.** Of 60+ DSes in `data_sources`, only three carry material daily IPDSC volume AND fit "bought third-party interest":
- DS35 LiveRamp IP (~104M rows/day, 213k active categories)
- DS17 ShareThis (~65M rows/day, 1,850 active categories — taxonomy 100% >2yr stale)
- DS18 Dstillery (~32M rows/day, 3,303 active categories — taxonomy 100% >2yr stale)

Most other named 3P providers (Sovrn, Cybba, Bombora, Captify, 33Across, Klickly, Oracle, Experian, OnAudience, Liftlab) are registered but deliver zero IPDSC volume today.

**7. CRM (DS4) is per-advertiser, NOT a shared catalog.** Each advertiser's CRM upload is private to their campaigns. Do NOT compare universe-level CRM IP counts (227M, summed across all advertisers' uploads) to the LiveRamp/ShareThis/Dstillery shared catalog — apples-to-oranges.

**7b. 3P demographic (income/age) data is unreliable — don't stack providers; pick one, treat as coarse (TI-1026, 2026-06-17).** Multiple LiveRamp (DS35) providers offer the *same* demo attribute (HHI bands from Equifax/IXI, Experian, TransUnion, Oracle), but:
- They barely agree: the 3 income providers agreed on only **0.36%** of who's "low-income" (Equifax 2.89M / TransUnion 4.45M / Experian 12.60M flagged; all-three overlap = 65,571 of an 18.34M union). IP-level income is an inferred estimate, not verified.
- **Equifax/IXI "Income 360" is asset/financial-capacity-based and skews affluent** — only 3.6% of households labeled <$30K and 41% labeled $150K+; it **under-counts low-income**. **Experian HHI is the more realistic income signal** (~10.2% <$25K, peak $50–75K). So "Equifax flags 4× fewer low-income" = Equifax under-labeling, not Experian padding.
- **Never stack providers for one attribute** — exclude/include clauses are OR'd, so stacking = the *union* of flags = you inherit every provider's errors (income example: 3 providers exclude 18.3M vs 2.9M for the most conservative). Pick one; recognize it's a coarse directional filter. The durable fix is per-segment quality scoring (TI-956) + a recommended-segment-per-attribute surface. Data: `tickets/ti_1026_orange_theory_audience_eval/outputs/ti_1026_income_distribution.csv`, `ti_1026_income_provider_agreement.csv`.

**8. Clause-structure bidder semantics + MM-ceiling pacing-overflow (TI-999 Finding 15, 2026-05-28 PM).** Empirically validated via Pass 1-12b over 15,529 active campaigns + cost_impression_log delivery distribution + per-advertiser cross-bucket ceiling test. **Three structurally distinct positive-clause patterns + one exclusion pattern, each with different bidder behavior.**

**A. Clause-structure semantics (verified delivery 2026-05-26):**

| Pattern | Buyer-written expression | Bidder behavior | Empirical % unscored |
|---|---|---|---|
| MM only | `OR(MM)` or just `MM` | Bid on MM-scored IPs | 4.2% (baseline noise) |
| **MM OR 3P** (union/expand) | `OR(MM_clause, 3P_clause)` | Bid on (MM ∪ 3P) IPs ranked by `household_score`; reach overflow to unscored 3P-added IPs when MM ceiling hits | **14.1%** (3.3x baseline) |
| **MM AND 3P** (intersect/narrow) | `AND(MM_clause, 3P_clause)` | Bid on (MM ∩ 3P) IPs — narrows MM to subset also in 3P; eligibility stays scored | **8.4%** (close to MM_only) |
| MM OR 3P **+** AND 3P too (hybrid) | `AND(OR(MM, 3P_A), 3P_B, ...)` | Multiple 3P clauses with mixed semantics; produces HIGHEST unscored share | **56%** |
| **MM AND NOT 3P** (exclude) | `AND(MM, NOT(3P))` | Remove 3P-matched IPs from MM scored set; cleanest narrowing | **0.4%** |
| **MM AND NOT 1P** (CRM suppress) | `AND(MM, NOT(CRM))` | Remove customers from MM prospecting; standard hygiene | 6.7% |

**B. The clause-structure check matters more than polarity alone.** Pass 1-11 lumped "inclusion" together; Pass 12 split it into OR-additive vs AND-intersect — these have OPPOSITE delivery effects despite both being positive polarity. AST parse must track OR-group-id per clause (via the JS UDF in TI-999 query Pass 12).

**C. Bidder mental model (best supported by empirical pattern):**

1. **Eligibility = expression match.** Per-bid-request evaluation. SSP sends bid request → check if IP matches campaign's audience expression → bid eligible.
2. **Within eligibility, `household_score` shapes preference + CPM.** Bidder prefers scored IPs while they are available in the bid stream + pacing windows.
3. **MM ceiling exists per campaign per MM segment per day** — saturation point where scored MM IPs in the bid stream are exhausted relative to pacing budget.
4. **When ceiling hits, bidder falls through to unscored eligible IPs** — most relevant for OR-additive 3P clauses that brought in non-MM-scored IPs.

**D. Empirical ceiling-bound test (FICO):**
- FICO MM_only campaign: $41.7K spend, 71.5K scored imps/day. Hits MM ceiling.
- FICO MM+3P_incl_only campaign: $168.5K spend (4x), 60.1K scored imps/day (essentially same ceiling) + 236K unscored 3P-added imps.
- **Same MM ceiling regardless of campaign size; extra budget overflows to OR-added unscored IPs.**

**E. Pattern frequency (30d ending 2026-05-28):**
- MM OR 3P (union): 523 camps, $2.15M / 30d (25.3% of MM spend) — DOMINANT 3P inclusion pattern
- MM AND 3P (intersect): 41 camps, $161K / 30d (1.9%) — RARE
- MM AND NOT 1P (CRM suppress): 296 camps, $1.88M / 30d (22.2%) — DOMINANT 1P pattern
- Buyers overwhelmingly use OR for 3P inclusion and AND-NOT for 1P. The "use 3P AS narrowing" pattern is barely used.

**F. Methodology lesson — prior prospecting filter was over-broad.** "Exclude any campaign with DS4/8/47 reference" dropped 296 MM-prospecting campaigns ($1.88M / 30d) that use 1P only NEGATIVELY. Polarity-aware version excludes only positive 1P clauses.

**Usage patterns (TI-999 Finding 15 Pass 2 — 30d ending 2026-05-28):**
- **3P clauses are overwhelmingly INCLUSION-only** (5a: 85% of MM_plus_3P; 609 campaigns / $2.76M / 30d).
- **1P clauses are overwhelmingly EXCLUSION-only** (6b: 92% of MM_plus_1P; 296 campaigns / $1.88M / 30d). Classic CRM-suppression-from-prospecting pattern.
- **MM + 3P excl_only is ESSENTIALLY NONEXISTENT** (7 campaigns, $20K). Buyers do not use 3P as a negative filter.

**Methodology correction for prospecting filters:** the prior rule "exclude any campaign whose expression references DS4 / DS8 / DS47" (memory `feedback_crm_excluded_from_prospecting`) is over-broad — it removes 296 MM-prospecting campaigns ($1.88M / 30d) that use 1P only as *exclusion* (customer suppression), which is core prospecting hygiene. **Polarity-aware version:** exclude only campaigns with 1P-family DS in POSITIVE clauses; 1P in negative clauses means the campaign is using CRM as suppression and remains prospecting.

**TI-956 prize zone (quantified from Finding 15 Pass 3):**
- MM + 3P inclusion campaigns deliver $643K / 30d (~$7.7M annualized) on unscored IPs — these are 3P-added IPs the household score knows nothing about.
- Pure 3P-only campaigns deliver $2.15M / 30d (~$25.8M annualized) on unscored IPs — every targeting decision in these campaigns rides entirely on 3P quality.
- Combined ~$50M+ annualized of unscored-delivery is reached via 3P inclusion clauses. **Per-segment quality scoring (TI-956) gives buyers control over this entire zone** by letting them choose 3P segments more likely to land on productive IPs (vs blind picks today).

**Revision history:**
- v1 (2026-05-28 AM): incorrectly claimed the bidder had only RTC scoring. Corrected.
- v2 (2026-05-28 PM, TI-999 Finding 15): added Item 8 — clause polarity semantics. Refuted the AND-intersection verbal model for inclusion; confirmed AND-NOT for exclusion. Updated prospecting-exclusion rule to be polarity-aware.

### CRM Upload Flow (DS 4)
1. Advertiser uploads CSV of hashed emails (HEMs) → stored in `tpa.audience_upload_hashed_emails`
2. Verisk identity graph resolves HEMs → IP addresses
3. IPs stored in `external.ipdsc__v1` (GCS-backed Parquet) for the relevant `dt` and `data_source_id=4`
4. Bidder reads ipdsc → targets those IPs
5. `audience_upload_ips` is **empty for email uploads** — only populated for direct IP uploads

**Match rate (HEM → IP):** ~61–63% typical (stored in `integrationprod.audience_uploads.match_rate`).
`match_rate * entry_count` = estimated IP count (use ipdsc query for actual count).

**HEM deduplication:** Filter `pre_hash_case = 'UPPERCASE'` to count unique emails — each HEM
is stored in UPPERCASE, LOWERCASE, and ORIGINAL case variants.

**Empty HEM hash:** SHA256 of empty string =
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
This appears when a row has no email value. Exclude from counts.

### LiveRamp Cross-IP Identity Linkage
**Canonical DS ids (verified 2026-04-22 against `bronze.integrationprod.data_sources`):**
- **DS3 = "MNTN Third Party"** (not LiveRamp — this note's original framing was wrong)
- **DS11 = "LiveRamp"** (audience-segment-based)
- **DS35 = "LiveRamp IP"** (IP-based variant — this is what Bryce called out as "3P" in the TI-896 war-room scope)

LiveRamp maps IPs to a shared identity graph (household/person level). When LiveRamp links IP_A ↔ IP_B,
both IPs receive identical segment membership entries in `tpa_membership_update_log` with `data_source_id` in {11, 35}.

**Full canonical DS map — query `bronze.integrationprod.data_sources` before relying on any tribal-knowledge DS id.** Known gotchas from that lookup (2026-04-22):
- DS2 = MNTN First Party
- DS19 = MNTN Matched (= "Keywords" in war-room/product language per Bryce)
- DS38 = MNTN UI Audience Keywords (different thing — UI-keyword strings, ~40M rows)
- Per-advertiser audiences appear in the 1000+ range with names like "{AID} - First Party Audience" / "Third Party Audience" / "Control Group Audience" / "Extension Audience". Any MM/3P/CRM count that excludes these will undercount.

**Key limitation for cross-stage tracing:** If an S1 impression is served to IP_A, and LiveRamp links IP_A to IP_B,
IP_B enters the S2 targeting segment. The S2 VV at IP_B cannot be traced back to the S1 impression at IP_A
because there is no IP↔IP linkage table in BQ. This accounts for ~20% of unresolved S2/S3 VVs in the
VV IP lineage audit (TI-650).

**How to detect LiveRamp-linked IPs:** Find IPs entering the same DS3 segments at the same timestamp (±1 min)
in `tpa_membership_update_log`. Segment overlap >50% indicates identity-level linkage (not coincidental).
Example: 96/140 segments shared (68.6% overlap) between linked IPs.

**No IP→IP mapping table exists in BQ.** `ipdsc__v1` only maps IP → data_source_id, not IP → IP.
LiveRamp's identity graph is external to MNTN's data warehouse.

---

## Spark BQ connector — silver-table type quirks (TI-837, 2026-04-30)

When reading silver tables (`dw-main-silver.logdata.*`) into Spark via `databricks-connect`, the BQ connector's schema resolver fails on two source-schema types that exist in `cost_impression_log`:

- **`recency_elapsed_time` (BQ `INTERVAL`)** — connector throws `Unexpected type: INTERVAL`. There is **no `selectedFields` workaround** because the connector reads the FULL table schema before applying projection. The INTERVAL column is needed by Dustin's data team's pipelines (per Dustin Niehoff 2026-04-30) and is not going to be removed; we live with it.
- **`media_spend` / `data_spend` / `platform_spend` (BQ `BIGNUMERIC(76)`)** — Spark's max DECIMAL precision is 38. Connector option `bigNumericDefaultPrecision=38` truncates these on read.

**Workaround (canonical, Victor Savitskiy 2026-04-30):** use `query` mode + materialization to `dw-main-bronze.external` (sanctioned scratch dataset, Terragrunt-managed). Push column projection down to BQ so the Spark connector only sees the result schema.

```python
df = (
    spark.read.format("bigquery")
    .option("parentProject", "dw-main-bronze")  # set ALL THREE (Victor)
    .option("billingProject", "dw-main-bronze")
    .option("project", "dw-main-bronze")
    .option("viewsEnabled", "true")
    .option("materializationDataset", "external")
    .option("bigNumericDefaultPrecision", "38")
    .option("bigNumericDefaultScale", "9")
    .load("""SELECT advertiser_id, ip, campaign_id FROM `dw-main-silver.logdata.cost_impression_log` WHERE ...""")
)
```

Full pattern + alternative (Dustin's `temporaryGcsBucket=dataproc-temp-us-central1-754673906299-me0b3bsh`) documented in `.claude/databricks_setup.md`.

**Other silver tables — same pattern works** for `clickpass_log` and `guid_log`. `guid_log` is also archived to `gs://mntn-data-archive-prod/guid_log/` (per Victor) — for high-volume reads, prefer GCS direct over the BQ connector.

---

## Campaign Holdout / Incrementality Measurement

### 10% Holdout Group (All Campaigns)
Every campaign has a **10% holdout group** — IPs that are never served impressions.

**Mechanism (Matt Brorby, 2026-04-07):**
- When an IP enters the targeting pipeline, it goes through a hash function
- Last 2 digits of the hash determine group assignment: < 10 → holdout, ≥ 10 → targeted
- This is **pure random assignment by IP** — the holdout should have the same intent tier distribution as the targeted 90%
- The holdout group is used for the incrementality dashboard in the UI

**How the holdout works (Nicholas + Zach, 2026-04-07):**
- The holdout is embedded IN the audience segment expression JSON as a where clause
- **1000 buckets** — holdout = range 0-99 (10%), targeted = range 100-999 (90%)
- The hash uses a **prefix (e.g., ex46)** — this is DIFFERENT from experiment bucket hashing (which hashes on the IP address directly). They are independent random assignments.
- Expression lives in `audience_segment_campaigns.expression` (expression_type = 2 only — type 1 is legacy, not read)
- Literally has the name "holdout" in the JSON — can be identified by parsing the expression

**How to get holdout IP lists:**
- **Option 1 — MD5 bucket hash (PREFERRED, Zach 2026-04-07):** Compute the bucket for any IP directly. No TMUL needed. **This is the cheapest approach by far.**

```sql
-- Greenplum/Postgres (Zach's confirmed function):
SELECT md5('{AID}:100.17.100.240') AS ip_hash,
       (('x' || substr(md5('{AID}:100.17.100.240'), 1, 16))::bit(64)::bigint % 1000) AS bucket;
-- If bucket is 0-99 inclusive → holdout. 100-999 → targeted.
```

**Holdout enforcement validated empirically across all campaign types (TI-837, 2026-04-29):**
Counted served IPs from `cost_impression_log` for 30-advertiser v5 cohort on 2026-04-23, computed `MD5(advertiser_id:ip) mod 1000` per row, binned by bucket:

| objective_id | funnel_level | what it is | n_served_ips | frac_in_holdout (0-99) |
|---|---|---|---|---|
| 1 | 1 | Stage 1 prospecting | 2,422,278 | **0.000%** |
| 1 | 2 | S2 within objective=Prospecting | 5,917 | **0.000%** |
| 1 | 3 | S3 within objective=Prospecting | 752 | **0.000%** |
| 4 | 1 | Stage 1 within Retargeting CAMPAIGN | 796,499 | **0.000%** |
| 4 | 2 | Stage 2 within Retargeting CAMPAIGN | 300,261 | **0.000%** |
| 4 | 3 | Stage 3 within Retargeting CAMPAIGN | 121,337 | **0.000%** |
| 5 | 2 | Multi-Touch (S2 obj-coded) | 1,571,506 | **0.000%** |
| 6 | 3 | MTFF (S3 obj-coded) | 213,996 | **0.000%** |

**Total: 5.43M served IPs across all 8 cells, 0 in holdout bucket.** Holdout hash is enforced for every campaign type and funnel level — including formal Retargeting (objective_id=4) campaigns. Prospecting baseline at 0% confirms the analyst-side hash matches the production bidder's. Query: `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_validate_holdout_on_retargeting.sql`.

**Critical details:**
- The hash input is **`{AID}:{IP}`** — the advertiser ID is prefixed. This means holdout assignment is **per-advertiser per-IP**, not global per-IP.
- Takes first 16 hex chars of the MD5, casts to 64-bit integer, mod 1000
- Zach calls this the `rust_equivalent` — the SQL replicates what the Rust-based audience service does at runtime
- 1000 buckets: 0-99 = holdout (10%), 100-999 = targeted (90%)
- Replace `{AID}` with the actual advertiser_id (integer)
- **BQ equivalent needs testing** — MD5 returns bytes in BQ, not hex string. Will need `TO_HEX(MD5(...))` and then a cast approach.

- **Option 2 (Zach, expensive fallback):** Use `external.tpa_membership_update_log__v2` (TMUL v2) — logs which IPs are in which segments. **Expensive for 30-day windows** — dry_run first. Only use if the hash approach doesn't match.
- **Option 3 (not yet built):** Nick wants Jordan/Zach to build a tool that takes an audience expression and returns matching IPs. Doesn't exist yet.

**Key tables for audience expressions:**
- `audience_segment_campaigns` — the real table. Maps 1:1 with campaign_id. Contains the expression JSON. Filter: `expression_type = 2`.
- `audience.audiences` — just a wrapper/template. Don't use for analysis.
- Nick (experimentation team) has a streamlined query to extract expressions — ask him.

**Important: Stage 1 campaigns only for holdout analysis.** S2/S3 campaigns target people already hit by S1 ads — they're downstream of the targeting decision. Use `funnel_level = 1`.

**MemDB holdout hash (Ryan Kleck, 2026-04-07):** MemDB already hashes IPs for the existing 10% holdout mechanism. This same mechanism could potentially be extended/reused for decile-based audience splits (AUD-5221/TI-831). Investigate before building a new hashing system.

---

## Intent Scoring Architecture (advertiser_household_score / HHST)

Reference diagram: `documentation/architecture/audience_intent_scoring.png`

### Venn Diagram: Bucket > Vertical > Keywords
- **DS13** = buckets and verticals
- **Bucket** = Industry (broad)
- **Vertical** = Subindustry (narrow, within a bucket)
- **Keywords** = DS19 keyword matches (narrowest, within a vertical)

### Campaign Level Scores (HHST on cost_impression_log)
| Tier | HHST Range | Criteria | Score Assignment |
|------|-----------|----------|-----------------|
| **High Intent (HI)** | 6666-10000 (always 10000) | IPs that fall in the **vertical** (subindustry) | Always flat 10000 |
| **Mid Intent (MI)** | 3333-6665 | IPs in the **bucket** (industry) but NOT the vertical. Ranked by # of page views, most recent page view. | Variable within range |
| **Max Reach / Peak Performance (PP)** | 1-3332 | All other IPs in the audience not in the bucket or vertical | Random score in range |

### Advertiser Level Scores (same ranges, different criteria)
| Tier | HHST Range | Criteria |
|------|-----------|----------|
| **HI** | 6666-10000 (always 10000) | IPs in the vertical |
| **MI** | 3333-6665 | IPs in bucket but NOT vertical, **>1 page view** in the bucket |
| **Max Reach** | 1-3332 | IPs in bucket but NOT vertical, **exactly 1 page view** — random score |

### Current Production Reality (as of 2026-04-08)
- **69.9%** of impressions: HHST = 10000 (HI — all scored IPs get flat 10000)
- **28.7%** of impressions: HHST = -1 (unscored — no Fangorn score / not in any segment)
- **1.4%** of impressions: HHST in MI range (3333-6665) — real variation exists here
- **~0%** in PP/Max Reach range (1-3332)
- **~0%** in HI sub-range (6666-9999) — everything is flat 10000

**Implication:** Per-tier incrementality analysis is not meaningful until continuous scoring replaces the flat 10000. Aggregate analysis (holdout vs targeted, all tiers pooled) is the correct approach.

### The HHST GATE vs the household_score VALUE — and how it filters 3P (TI-1026)

Two distinct things, easy to conflate:
- **`cost_impression_log.household_score`** = the delivered IP's *score value* (0-10000, or -1 unscored). The HI/MI/PP tiers above describe this value.
- **`dso.household_score_thresholds`** (VIEW; cols: `advertiser_id, campaign_group_id, campaign_id, threshold`) = the per-campaign **GATE** — the minimum `household_score` an IP must have for the bidder to bid on it. This is the "HHST" Ryan Kleck means by "the score doesn't matter if the HHST is not set." **`threshold = 0` → no gate (bid on everyone, scored or not). ~64% of campaigns (20,982 of ~30k) run threshold=0.** Common non-zero gates: 10000, 6666, 9501.

**Mechanism — why OR-include 3P on an MM campaign usually does nothing (or delivers garbage):**
MM-prospecting audiences are typically `(MNTN Matched keywords OR bought 3P segments) AND geo`. Only the MNTN
Matched (DS19 keyword) IPs get scored; **3P-only IPs (those matching a bought segment but no keyword) are unscored
(household_score = -1).** So:
- **Campaign with a score gate (threshold > 0):** unscored 3P-only IPs can't clear the gate → **filtered out → the
  3P segments contribute ~nothing.** Empirical (TI-1026, Orange Theory campaign 319137, threshold=6501, 14d):
  **82.3% of delivery scored ≥6501, only 1.5% unscored.**
- **Campaign with no gate (threshold = 0):** the bidder bids on the unscored 3P-only IPs → low-intent traffic.
  Empirical (OTF campaign 319133, threshold=0): **99.96% of delivery was unscored (-1).** This is the source of
  "3P / non-MNTN-matched audiences perform far worse" complaints.
- **Takeaway for audience eval:** bought-3P OR-include is not a usable reach lever — gate on → filtered; gate off →
  unscored garbage. To add *scored* reach: lower the gate, broaden/clean keywords, or widen geo. Connects to
  TI-999 Finding 14 (pure-3P delivery is ~74% unscored) and [[reference_rtc_hhst_gating]].
- **Query delivered scores:** `cost_impression_log` (filter `time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL N DAY)`
  to prune; it's a 90-day rolling SQLMesh view). Has `campaign_id, ip, household_score, advertiser_household_score, model_params` + geo cols.

### The bidder uses the SEGMENT expression, not the user's audience expression (TI-1026, Alex Knorr 2026-06-12)

`audience.audiences.expression` = the **user's selections** (keywords DS19, interests DS35, geo). The bidder actually
evaluates the **per-campaign `audience.audience_segments.expression`** (translated `{select, categories, geos, version}`
form), which AND-layers platform automation on top. Every MNTN campaign's segment expression adds:
- **DS14 cat [1] "MNTN Global Data", `op:"any"` — a ~7-DAY AUGMENTOR-LOG ACTIVITY FILTER.** The IP must have appeared
  in the bidstream (`logdata.v_augmentor_log`, ip/time; ~613 GB/day, ~10-day TTL) in the last ~7 days to be eligible.
  **DS14 is NOT materialized in IPDSC** (zero ipdsc rows) — computed at bid time. **Distinct from the 30-day RTC/site-
  visit scoring lookback** — DS14 is an *eligibility/recency* gate, scoring is *quality*. Both apply.
- **DS34 (Pageview) + DS21 (Conversion), cat = advertiser_id** — the advertiser's own pixels = past-visitor/converter
  retargeting clauses (prospecting hygiene exclusions).
- **Holdout/experiment bucket:** `select.count.holdout = bucket(md5{prefix:"<advid>:", num_buckets, bucket_beg/end})`.
- **Score directive:** `select.score.types=[{score_type, id}]` (id = vertical_id for RTC; gated by HHST).

**Implications:** (1) the **UI audience-size / `eval_batch` on the USER expression OVERSTATES the deliverable** — it
omits DS14, holdout, and retargeting clauses. (2) Any audience-size/funnel analysis built on `audience.audiences` is
the user-selection size; the true targetable set is `∩ DS14-active(7d) ∩ not-past-visitor ∩ holdout-bucket`. (3) The
DS14 filter is the platform's formal "availability" gate — it's why a campaign's deliverable ≈ its recently-active pool.
To inspect: pull `audience_segments.expression`, `jq '.categories.where'` (root `op:"and"`), look for `data_source_id:14`.

### Where the UI audience-size number lives (TI-1026, Nick Martin/Matt Brorby/Jordan Piepkow 2026-06-15)

The "audience size" shown in the UI = the size of the audience_segment for the **stage-1 campaign**. Sources:
- **`dw-main-silver.perml.flight_cid_day_audience_sizes`** (ACCESSIBLE; cols: `campaign_id, campaign_group_id,
  rpt_day, funnel_audience_size, total_audience_size, tmul_funnel_audience_size, tmul_total_audience_size`). The
  **stage-1 campaign** is the one whose `funnel_audience_size = total_audience_size` (no funnel narrowing); that
  value is the UI number. Deeper-funnel campaigns show a small `funnel_audience_size`. `tmul_*` = TMUL-resolved.
  **Date floor = 2025-02-01** (the sibling `perml.stg_mmmp_audience_sizes_historical` TABLE also floors at 2025-02-01;
  the `flight_cid_day_audience_sizes` VIEW runs to current). **No 2024 addressable-pool history exists in either** — a
  true pre-2025 baseline of the addressable pool is unavailable here (use CIL distinct `ip` as an unscored served-supply
  proxy back to 2024). Keyed by campaign_id (no advertiser_id) → join `bronze.integrationprod.campaigns` to get
  advertiser_id; `funnel_level=1` filters to prospecting/stage-1. An advertiser can have multiple stage-1 campaigns incl.
  small "TV Retargeting" ones (~3M) — the **addressable prospecting pool = MAX(total_audience_size) per day across the
  AID's stage-1 campaigns** (picks the real prospecting campaign), then average daily across the month. **AUDI-1070 use:
  monthly addressable-pool trend per advertiser is a valid SUPPLY-SIDE time series** — Avon's stage-1 pool (same campaign
  259556 throughout) contracted −26% (89.0M Feb'25 → 65.6M Jun'25) into the July-2025 "performance fire," then stabilized
  ~63-77M; HexClad's pool fell −55% across 2026 (~82M→34M). Combine with the ~5× overstatement caveat below: anchor on the
  *relative trend* (same-campaign/same-config), not the absolute level.
- **`dw-main-bronze.external_ddm.segment_sizes`** (segment_id, audience_size, campaign_id) — authoritative, but
  GCS-backed at `gs://mntn-data-monitoring/audience-metrics/segment-sizes/*.parquet` (needs bucket IAM; may be
  access-denied). Same numbers as the perml table.
- **`audience-service …/eval_batch`** API (VPN/corp-only) computes it on demand from advertiserId + expression.

**⚠ The UI size OVERSTATES the deliverable audience.** It reflects the raw user expression (≈ MM ∪ 3P **national**)
and does **not** appear to apply the radii geo fence or the DS14 activity filter — so it can be ~5× the IPs a campaign
can actually reach (TI-1026: UI 9.7M vs ~1.9M reached). It also **inflates with 3P-OR** even though the score gate
won't bid those IPs. Don't anchor reach/pacing decisions on the UI size; use realized reach (`cost_impression_log`).

### How deliverability is actually set — peer pacing to 96% of budget (Chris Addy deep-dive, TI-1037, 2026-06-18)

**There is NO predictive targetable-IP model.** The platform does **not** compute "what % of an audience's IPs will be
biddable during a given period" — no such model exists. So a diagnostic must never promise a targetable-% figure;
answer deliverability empirically (realized reach/frequency + peer pacing), not by an audience-size calculation.

**The deliverability target is empirical-by-analogy.** The operative method: look at what **comparable campaigns**
required (over roughly the **last 60–90 days**) to reach **96% of budget** — the platform's de-facto "fully delivered"
bar — then judge how much *this* campaign should spend by comparing its spend to that peer envelope, scaled by
**flight (campaign) length**. There is no closed-form "this audience can deliver X impressions"; it's "campaigns
shaped like this one historically hit 96% of budget at spend level Y over length Z."

**Diagnostic implication (TI-1037 step 9 = a peer-pacing benchmark, sibling of the step-7 peer-VR benchmark):**
cohort comparable campaigns (CTV, vertical, geo footprint, budget tier, audience shape, HHST on/off) over 60–90d;
compute the spend/pacing that got peers to ≥96% of budget; place the target campaign in that distribution.
- Peers at this budget hit 96% but ours doesn't → **not a budget problem**; it's audience/targeting (cross-check the
  DS14-active pool + realized reach for a delivery pause vs genuine exhaustion).
- Even peers can't sustain 96% at this budget → **budget too high for the audience shape**: lower budget, lengthen the
  flight, or grow the pool (geo radius / keyword layer).
- Queryable in-tool from spend (`spend_log`, **nanosecond epoch**) + budget cap + delivery logs — no external Olympus
  dependency. The **budget/cap field still needs locating** (campaign config / dso). The comparable-campaign selection
  is the design-sensitive part — a bad cohort yields a meaningless benchmark.

**Distinct from `deliverability_classification`** (the Media-Plan guardrail risk bucket, high/medium/low — see the
Media Plan section): that is a *categorical risk classifier* from per-network spend thresholds + audience size +
blocked networks + budget; it is NOT the campaign-level deliverability *target*, which is the peer-pacing-to-96% method
above.

### 3P-vs-MM overlap is only an intent signal when MM is TARGETED — always base-rate it (TI-1037 iMemories, 2026-06-23)
When evaluating bought 3P (DS35) interest segments, the "% of the segment's IPs that also match the advertiser's MM
(DS19) keyword layer" reads as *redundancy / intent alignment* — **but only relative to the base rate**
`MM_distinct / ipdsc_population`. If the advertiser's MM keyword universe is **near-universal**, every 3P segment —
including deliberately-unrelated ones — overlaps MM at that base rate, so the metric carries **zero** discriminating
signal. **Always run a control** (overlap of an unrelated segment with this advertiser's MM) or compute the base rate
before interpreting overlap.
- **iMemories (aid 37423):** 211 DS19 keywords → **174.5M-IP** MM universe (≈ most of the US ipdsc population). Its own
  15 3P segments overlapped MM 67–73%; three unrelated OTF fitness segments overlapped 67.1–67.5% → identical → overlap
  is base-rate noise, cannot rank the segments.
- **Contrast OTF (TI-1026):** MM ~4.6M (targeted) → 3P overlap ~12% was a genuine low-intent signal.
- A near-universal MM universe is itself the headline finding: the keyword layer is **barely targeting**, so the lever
  is curating MM keywords, not picking 3P. (And under an HHST gate, unscored 3P is filtered regardless — segment choice
  is moot for delivery until the gate lowers.) ipdsc gives **membership**, not delivered performance — a further reason
  not to rank 3P on reach/overlap alone.

### Special Values
- **10000** = High Intent (HI) — flat score for all vertical-matched IPs. Currently 69.9% of impressions.
- **8000** = Peak Performance (PP) — **was active Jan-Feb 2026, currently minimal** (as of 2026-04-08). Targeting logic: serve HI (10000) first, then expand to PP (8000) if pacing allows. Waterfall: HI → PP. Top advertisers with PP data: 34185, 36232, 37158, 34838. Most PP activity ended by late February. Sporadic single-digit impressions in March-April.
- **3333-6665** = Mid Intent (MI) — bucket-matched IPs not in the vertical. 1.4% of impressions.
- **-1** = unscored (no Fangorn/intent score assigned). 28.7% of impressions.
- **-4** = rare edge case (9 impressions observed)

**Once PP (8000) goes live**, per-tier incrementality analysis becomes possible: compare HI (10000) vs PP (8000) vs MI (3333-6665) holdout/targeted visit rates.

### Daily Prospecting Scores Distribution Monitor (GCS, PROD)

A daily distribution monitor emails the latest scoring landscape to `targeting-infrastructure@mountain.com` and `machine-learning-squad@mountain.com`. Subject: `MNTN Prospecting Scores Distribution GCS - YYYY-MM-DD (PROD)`. Source: `gs://household-scoring-prod/output/scoring/prospecting_intent/`. Sample PDF saved at `tickets/ti_999_interest_segment_sizing/artifacts/ti_999_prospecting_scores_distribution_gcs_2026_05_31_PROD.pdf`.

**Headline (2026-05-31 snapshot — empirically locked):**

| Cohort | Funnel Level | Count of Scores | Distinct Scores | Distinct Campaigns | Distinct Advertisers |
|---|---|---:|---:|---:|---:|
| Total | — | 443.6B | 10,000 | 6,536 | 1,352 |
| Fangorn-on | 1 (S1) | 23.2B | **10,000** (graduated) | 463 | 292 |
| Fangorn-on | 2 (S2) | 15.5B | 9,999 | 321 | 209 |
| Fangorn-on | 3 (S3) | 15.7B | **1** (flat 10,000) | 322 | 209 |
| Fangorn-on | 4 (S4) | 0 | 0 | 0 | 0 |
| Non-Fangorn | 1 (S1) | 154.3B | **6,667** (discrete buckets) | 2,171 | 1,060 |
| Non-Fangorn | 2 (S2) | 118.8B | 6,667 | 1,652 | 777 |
| Non-Fangorn | 3 (S3) | 116.1B | **1** (flat 10,000) | 1,607 | 777 |
| Non-Fangorn | 4 (S4) | 0 | 0 | 0 | 0 |

**Key findings (lock these — they answer many TI-999 questions):**

1. **Fangorn-on vs Non-Fangorn produce structurally different score distributions:**
   - **Fangorn-on**: scores span the full continuous range 1-10,000 (10,000 distinct values). Distribution is concentrated below ~3300 (Max Reach floor at ~48M per bucket) then ramps down through Mid / Peak / High.
   - **Non-Fangorn**: only 6,667 distinct scores — discrete buckets dominate. Volume concentrates at **8000 (Peak)** and **10,000 (High)** as point masses, plus graduated Max Reach (1-3332) and Mid (3333-6665). High = ~225M per FL1 bucket, Mid block ~100M per bucket.
2. **S3 funnel level flattens to a single score of 10,000** for BOTH Fangorn-on and Non-Fangorn — no graduated scoring at S3. S3 is retargeting / down-funnel; the bidder doesn't differentiate within it.
3. **S4 has zero scores** for both cohorts — S4 doesn't use `household_score` at all (likely pixel-exclusion-driven retargeting only).
4. **Fangorn rollout = ~22% of S1 advertisers** (292 of ~1,352 distinct advertisers total).
5. **Intent tier ranges (locked from this monitor):**
   - **High Intent**: 8001-10000 (with 10000 as a discrete point mass for Non-Fangorn)
   - **Peak**: 6666-7900 (with 8000 as discrete point mass for Non-Fangorn — note 8000 sits *between* Peak and High in this monitor)
   - **Mid**: 3333-6665
   - **Max Reach**: 1-3332

**Implication for TI-999 3P-performance work:**
- 3P-only baseline campaigns may be Fangorn-on OR Non-Fangorn. Performance will be affected by the underlying score distribution. **Segment the 3P-only baseline by Fangorn status** when comparing CVR / IVR.
- Most of MNTN's score volume (~270B of ~443B) is Non-Fangorn S1+S2. The "Mid" bucket (3333-6665) is the dominant graduated range for Non-Fangorn.
- For 3P-only campaigns at S3, scoring is uniformly 10,000 — no scoring variance to confound 3P measurement at S3.

### Fangorn audience-overlay mechanic — audience vs audience_segments distinction (Ryan Kleck, 2026-06-01)

**When MNTN switches an advertiser to Fangorn, the change uses the "audience overlay" feature.** Per Ryan Kleck (2026-06-01):

> "when we switch someone to Fangorn we use the 'audience overlay' feature that changes their audience expression in the audience_segments table, but it does NOT change the base expression in audience table... so those will still have DS13 in audience table (if they have peak performance on)"

**Implication: two layers of expression state for Fangorn-on advertisers:**

| Table | Contains | What it shows for a Fangorn-on advertiser |
|---|---|---|
| `audience.audiences` (template / base) | The buyer's configured audience template | Still references **DS13 (Vertical)** and **DS19 (Keywords)** — the buyer's actual MM 2.0 config |
| `audience.audience_segments` (active / bidder-facing) | The compiled, overlaid expression the bidder sees | References **DS46** (Fangorn scoring) as the active overlay |

**So when we see DS46 in audience_segments without DS13/DS19, the advertiser is STILL using DS13/DS19-style targeting at the base layer.** The Fangorn overlay just tells the scoring pipeline to use Fangorn instead of the legacy scorer. The scoring substrate (which IPs are evaluated) is still DS13 vertical + DS19 keywords from the base configuration.

**Practical implications for TI-999:**

- **Pass 21+ "MM-touching" buckets** (which match on DS13/19/38/46 in audience_segments) capture campaigns where DS46 appears via Fangorn overlay AS WELL AS campaigns with DS13/DS19 directly. Both are MM; the difference is scoring algorithm (batch MM scorer vs Fangorn).
- **A campaign with DS46 alone in audience_segments is still effectively running MM** because the underlying DS13/DS19 config in the `audiences` base table is what gets scored. Fangorn replaces the scoring algorithm, not the audience substrate.
- **Confirming the locked logic** from earlier: scoring requires DS13 or DS19 in the EXPRESSION (whether base or overlaid). Fangorn-on campaigns have DS13/DS19 in `audiences` even if their `audience_segments` row shows DS46 instead.
- **DS38 status:** Ryan not sure about DS38 (BUK) but per Alex Knorr (2026-05-29) BUK augments DS19 rather than replacing it — same logic likely applies.

**Why this matters for "Are DS38/DS46 MM?":**

Yes. Both are MM in the sense that:
- DS46 (Fangorn) is the Fangorn scoring overlay applied on top of an existing DS13/DS19 MM 2.0 configuration. The campaign is MM with Fangorn scoring.
- DS38 (BUK) is the keyword pipeline replacement — augments DS19, doesn't replace MM. Still MM.

The "MM" group `{DS13, DS19, DS38, DS46}` is correct as the union of all things that signal "this is an MM 2.0 campaign," regardless of which scoring algorithm is active.

### MM advertiser adoption — canonical answer + the DS19 swing (TI-999, locked 2026-06-16)

**`DS19` is ALWAYS part of MM (it's the keyword half — "MNTN Matched").** Any "% of advertisers using MM" stat must include DS19. Excluding it cuts the headline roughly in half and is wrong.

Canonical current adoption (window 2026-05-01→06-15, distinct advertisers de-duped from live `audience_segments` expressions, MM = DS13/19/(38)/46):

| Cut | Rate | Counts |
|---|---|---|
| **Active advertisers using MM** | **~83%** | 1,756 / 2,119 |
| Prospecting advertisers (objective 1/5/6) using MM | ~87% | 1,755 / 2,015 |
| Active **campaigns** (prospecting) using MM | 28% | 3,471 / 12,410 |

The same query with DS19 **excluded** yields only ~47% of advertisers — this is the source of every historical "~half of advertisers use MM" figure, and it is the wrong definition. Query: `tickets/ti_999_interest_segment_sizing/queries/ti_999_mm_adoption_current.sql` (full 2×2×2 sensitivity table).

**Counting gotchas (load-bearing):**
- Count MM from **live `audience_segments`** (actual per-campaign targeting), NOT `audience_audiences` (templates — not what gets targeted). Matching MM at the campaign-**group** level off the templates table over-counts ~5 pts (~88% vs true ~83%).
- Venn-bucket `n_advertisers` columns OVERLAP across buckets — never sum them; de-dupe with `LOGICAL_OR(has_mm) GROUP BY advertiser_id`.
- Advertiser % uses an all-active denominator (any objective); restricting the denominator to prospecting-only drops ~1,700 retargeting/awareness-only (mostly non-MM) advertisers and inflates the rate.

### Scoring pipeline scope — which campaigns get scored at all (Ryan Kleck, 2026-06-01)

**Scoring happens at the expression level, gated on DS reference:**

> "we score every campaign that has DS13 or DS19 in its audience expression" — Ryan Kleck, 2026-06-01

So MM scoring (Fangorn-on OR Non-Fangorn) is generated only for campaigns whose audience expression references **DS13 or DS19** (positively). The pipeline writes per-(advertiser, campaign, IP, score) rows to GCS for those campaigns.

**Two-stage gate (Ryan):**
1. **Scoring stage:** If DS13 OR DS19 is in the expression → the campaign gets scored (rows written to GCS).
2. **Bidder stage:** Even when scored, "the bidder won't use scores unless the HHST is set."

So a campaign can be **scored-but-not-acted-on** if it has DS13/DS19 in the expression but no HHST configured. The score exists in GCS but doesn't gate bidding decisions.

**Architectural detail — DSes are not a bidder concept (Ryan Kleck, 2026-06-01):**

> "the bidder doesn't know about DSs.. that's a MemDB concept. Then MemDB says, 'ok this IP belongs to this segment/campaign, and this one, etc' and we give the bidder ALL our scores."

So the data flow is:
1. **Audience expression** references DSes (DS13, DS17, DS19, etc.) and their categories.
2. **MemDB** translates audience expressions into IP-level membership: it knows which IPs belong to which segments / campaigns.
3. **Bidder** receives IP × campaign membership from MemDB along with ALL available scores for those IPs (whatever the scoring pipeline produced). The bidder operates on scores + HHST, not on DS references directly.

**Implication:** DS-level audience semantics live in the audience platform (MemDB), not in the bidder. Bid-side analyses (e.g., `bid_events`) won't show DS references; they'll show IP + scores + campaign. To reason about DS-level audience composition, you must work upstream from the bidder, via the expression or MemDB membership.

**Surprise empirical finding (TI-999, 2026-06-01) — Ryan didn't expect this:** MM+3P combinations dominate prospecting spend, not MM-only or 3P-only. From Pass 21 buckets: MM+3P = 1,133 campaigns / $7.25M (22.6% spend), MM+3P+CRM = 306 / $3.32M (10.3%), MM+CRM = 566 / $5.08M (15.8%) — vs MM-only at 1,194 / $6.02M (18.7%) and 3P-only at 439 / $1.41M (4.4%). Ryan's mental model was that buyers usually pick MM-only OR 3P-only, "tricky" edge case to combine. Empirically MM+3P combinations are the **majority of prospecting spend**, which raises a structural question: if 3P IPs that aren't also in DS13/19 don't get scored, what does adding 3P to an MM campaign actually do at the bidder level?

**Working hypothesis (to validate):** the value of MM+3P (vs MM-only) shows up only when MM-scored IP supply is exhausted relative to pacing budget — the bidder falls through to unscored 3P-eligible IPs to fill remaining budget. This matches the earlier "MM ceiling" finding from TI-999 (Pass 12/13). For most campaigns where MM-scored IPs are plentiful, adding 3P is no-op — the 3P clause doesn't bring scored IPs in (only MM does), it just narrows or widens eligibility. **Worth validating with Ryan / Zach when this lands in the deck.**

**Implication for TI-999 3P-only baseline (LOCKED methodology):**

- **True 3P-only campaigns** (no DS13/DS19/DS38/DS46 anywhere in the expression — per Pass 21 = 439 campaigns) are **NOT scored at all** by the MM pipeline. No `household_score` rows are written for these in GCS.
- For these campaigns, the bidder evaluates eligibility on **3P category match + DS14 freshness + geo + holdout**, with no MM scoring layer to bias bid selection.
- **RTC may still apply if HHST is set on the campaign** (Ryan's earlier wrinkle). RTC populates `realtime_conquest_score` separately.
- **3P-only baseline (439 camps / $1.41M) is the cleanest MM-scoring-free 3P cohort for measuring segment quality**, modulo RTC for the HHST-enabled subset.

**For Pass 24 / clean 3P attribution:** filter to 3P-only campaigns (NO DS13/19/38/46) AND filter impressions to `realtime_conquest_score != 10000` (RTC didn't fire). What's left is bidding driven purely by 3P + freshness + geo eligibility, no MM scoring contamination.

### Fangorn raw-score → HHST score-band mapping (Ryan Kleck, 2026-06-01) — LOCKED LOGIC

**Fangorn outputs continuous 0-1 raw scores. The downstream scoring job applies tier mapping** based on (a) the Fangorn raw value and (b) the IP's membership in DS13 Bucket / Vertical and DS19 Keywords:

```
if Fangorn raw > 0.8:
    if IP in DS13 (Vertical) ∩ DS19 (Keywords) → HI band (8000-10000)
    elif IP in DS13 (Bucket/Vertical) but NOT in DS19 → PP band (6666-8000)

elif Fangorn raw 0.6-0.8:
    → MI band (3334-6665), regardless of DS13/DS19 overlap

else (no Fangorn score, or raw < 0.6, or no DS13/DS19 score):
    → Max Reach band (1-3332). **Just a random number, NOT Fangorn-derived.**
```

**Critical clarifications from Ryan (2026-06-01):**

1. **PP and HI are NOT Fangorn-internal concepts.** Fangorn outputs a single 0-1 raw score; the scoring job applies the PP/HI split based on DS13 Vertical ∩ DS19 Keywords overlap.
2. **MI is the same band regardless of DS membership** — Fangorn raw 0.6-0.8 always maps to Mid Intent, whether the IP is in DS13/DS19 or not.
3. **Max Reach is just a random number** — "no fangorn at all." Max Reach is the fallback for IPs without Fangorn scores OR with Fangorn scores < 0.6. The 1-3332 range has no scoring semantics.
4. **Why the score-band distribution looks the way it does:**
   - The Max Reach band (1-3332) is uniform and high-volume because it's the random fallback for many IPs.
   - The Mid band (3333-6665) is graduated and represents the Fangorn 0.6-0.8 raw distribution.
   - The PP band (6666-8000) is graduated and represents Fangorn raw > 0.8 + (Vertical AND NOT Keywords).
   - The HI band (8000-10000) is graduated and represents Fangorn raw > 0.8 + (Vertical AND Keywords).
   - The tail-off near 7900-8000 and 9900-10000 is because Fangorn rarely produces raw scores > 0.99.

**Implication for analysis:**

- An IP scored 1-3332 (Max Reach) is **not Fangorn-evaluated** — random assignment, no intent signal.
- An IP scored 3333-6665 (Mid) IS Fangorn-evaluated with raw 0.6-0.8, regardless of which DS layers it sits in.
- An IP scored 6666-8000 (PP) IS Fangorn-evaluated with raw > 0.8 and is in DS13 Vertical but NOT DS19 Keywords.
- An IP scored 8000-10000 (HI) IS Fangorn-evaluated with raw > 0.8 and is in BOTH DS13 Vertical AND DS19 Keywords.

### MM + 3P intersection mechanics — LOCKED LOGIC (Ryan Kleck + Venn diagram, 2026-06-01)

**The critical structural insight:** when a buyer adds a 3P-include layer to an MM campaign (the majority of prospecting spend is this pattern), the bidder effectively **narrows MM** to the intersection of MM-scored IPs and 3P segment members.

**Mechanically, step-by-step (with HHST > 0 — the standard production setting):**

```
1. MM campaign (DS13/19 in expression) → scoring pipeline scores IPs in DS13/19.
   - IPs in DS13 ∩ DS19 → HI / PP band scores
   - IPs in DS13 only → PP band scores
   - IPs in DS19 only → MI band scores (or maybe PP, unclear)
   - IPs in neither → Max Reach random
2. Buyer adds 3P-include (DS17/18/35 in expression as positive clause).
3. MemDB translates the expression into IP × campaign membership:
   - With AND semantics: only IPs in (DS13/19 ∩ 3P) are included in the campaign membership.
   - With OR semantics: IPs in DS13/19 ∪ 3P are all included.
4. Bidder receives MemDB membership + scores for those IPs.
5. With HHST > 0, bidder requires score ≥ HHST to bid:
   - IPs in (DS13/19 ∩ 3P): scored (because in DS13/19) → eligible to bid based on score
   - IPs in 3P only (not in DS13/19): NO SCORE → fail HHST → NOT bid on
6. Net result: bidder bids on (DS13/19 ∩ 3P) IPs only.
```

**The Venn picture (Ryan's diagram, 2026-06-01):**

```
        ┌────────────────────┐
        │  Bucket (DS13 T0)  │
        │   ┌────────────┐   │     <- DS13 Vertical (T1) is a subset of Bucket (T0)
        │   │  Vertical  │   │        Peak Performance = inside Vertical
        │   │   ┌────────┼───┼────┐
        │   │   │   HI   │   │    │ <- HI = Vertical ∩ Keywords
        │   │   │        │   │ MI │
        └───┼───┘        │   │    │
            │  Keywords  │   │    │ <- MI = Keywords only (or partial overlap)
            │  (DS19)    │   │    │
            │  Max reach │   │    │
            └────────────┘   │    │
                             └────┘

Add a 3P circle that overlaps Bucket/DS13:
- Where 3P ∩ Bucket (DS13) → IPs are scored AND in 3P → bid on (red square area in Ryan's diagram)
- Where 3P falls outside Bucket → IPs are in 3P but NOT scored → with HHST > 0, NOT bid on
```

**Plain-English consequence:**

Adding 3P-include to an MM campaign **does not bring 3P-only IPs into bidding** (because they're unscored and HHST > 0 filters them out). It also doesn't expand the addressable pool. Instead, **3P narrows MM to the 3P-segment-intersected subset of MM-scored IPs**.

So when buyers combine MM with 3P (which is the majority of prospecting spend per Pass 21):
- They are not "expanding audience via 3P interest segment diversification."
- They are using 3P as a **narrowing filter** on MM scoring.
- The 3P segment's quality determines **which slice of MM-scored IPs ends up being bid on**.
- A bad 3P segment narrows MM to a subset that may be no more valuable than (and could be less valuable than) the full MM-scored audience.

**Spend semantics clarification (Malachi, 2026-06-01):**

- **Audience size does NOT determine spend.** Advertisers are only charged when MNTN bids on AND wins an impression. So adding a 3P clause to MM doesn't "cost more" — it just changes WHO can be targeted, not how much gets spent.
- Implication for the "theater" framing: when an OR-include 3P clause has no effect on bidder behavior, **the buyer isn't wasting money on theater** — they're spending the same amount on the same MM delivery, just with a UI label that misrepresents what's being targeted.
- The real harm is **decoupled targeting intent** — the buyer believes they're combining MM with a specific interest segment, but mechanically the 3P clause changes nothing. So budget gets spent on MM delivery while the buyer reports back to their team "we ran MM+3P-A campaigns" believing 3P-A had effect.
- For AND-include cases (5% of MM+3P-incl spend), 3P quality DOES affect which MM-scored IPs the bidder sees — so curation has real lift there.
- For OR-include cases (80% of MM+3P-incl spend), the 3P clause is bidder-inert — curation can't improve those campaigns' performance because the 3P didn't affect delivery to begin with. But ranking these 3P segments still has UI value: it would let MNTN show "your selected 3P segment X is low quality" or guide buyers toward more impactful selections in their next campaign.

**Implication for TI-999 curation case (LOCK FOR DECK):**

This is the strongest argument for 3P-segment curation MNTN can make:
1. Buyers think they're combining MM with interest segments to expand or diversify.
2. Mechanically: 80% of MM+3P-include spend is OR semantics where the 3P clause doesn't affect bidder behavior (3P-only IPs aren't scored, fail HHST). Only 5% is AND semantics where 3P genuinely narrows MM.
3. **For the AND-include 5%: 3P quality directly determines MM delivery quality** — curation has real lift.
4. **For the OR-include 80%: 3P quality determines what's REPORTED to the buyer about their targeting** — curation prevents buyers from believing they're targeting low-quality segments when they're actually getting MM-only delivery. UI/attribution honesty is the value, not delivery-quality lift.
5. TI-956's per-segment scoring framework is the operational fix for both.

**Exception cases worth flagging:**

- If HHST is NOT set (or set to 0): bidder ignores scores → 3P expands MM by bringing unscored 3P-only IPs into bidding. This is the bad pattern — unscored IPs perform worse, and the buyer is unknowingly broadening to a low-quality audience.
- The mix of HHST-set vs HHST-not-set across MM+3P campaigns determines whether 3P is "narrowing" or "expanding" in practice. We don't yet know the HHST distribution across MM+3P campaigns. Pending investigation.

**Diagram source:** stored in `tickets/ti_999_interest_segment_sizing/artifacts/` once saved (Ryan's hand-drawn Venn, sent in Slack 2026-06-01).

### Why the Fangorn distribution shape isn't smooth (locked explanation)

The Fangorn-on histogram looks like (a) a high floor below 3,300 (~48M scores per 10-wide bucket), (b) a sharp drop-off to ~10-15M per bucket between 3,300 and 6,665, then (c) two clear spikes around 6,666-7,900 (PP) and 8,001-10,000 (HI), each tapering downward.

**Why each feature exists:**

1. **The staunch drop-off at score 3,300** is the **tier boundary between Max Reach (1-3,332) and Mid (3,333-6,665)** in the downstream scoring job's mapping — not a Fangorn-internal artifact. Fangorn's raw distribution is continuous; the tier-mapping function squashes Fangorn raw ≤ 0.5 into Max Reach with much higher per-bucket density than it squashes Fangorn raw 0.5-0.8 into Mid. The boundary at 3,300 is operational, not statistical.

2. **The two spikes around PP and HI** reflect **Fangorn's natural high-intent distribution mapped into the PP and HI bands** via linear mapping of raw 0.8-1.0. The peak of each spike sits around Fangorn raw ~0.85-0.9 (the densest part of the high-intent distribution). The downward taper toward 7,900 (top of PP) and 10,000 (top of HI) reflects Fangorn raw scores 0.95-1.0 being rare. So the spike shape IS the shape of Fangorn high-intent calibration.

3. **The "raw Fangorn distribution would be a smooth curve"** intuition is correct, but the scoring job transforms it. The tier-mapping function is piecewise (different mapping for each tier), which creates the discrete-looking shape. If you plotted Fangorn raw 0-1 directly without the tier transformation, you'd see a smoother distribution.

### Bidder System Design & Caching Architecture (Abbas + Ryan, 2026-06-09, TI-1016)

Full bidder-team sys-design walkthrough. Source: `tickets/ti_1016_memdb_bidder_cache_optimization/meetings/ti_1016_02_abbas_bidder_sys_design_caching_2026_06_09.txt`. Confluence: [Aerospike Datastore — Household Profile](https://mntn.atlassian.net/wiki/spaces/BP/pages/2927263763/Aerospike+Datastore#Household-Profile).

**Authority caveat:** Abbas moved to the **performance-pacing team**; the in-flight membership-consumer rework is owned by **Eric** (secondary: **Alkaif**). Treat "future state" items as directional, not locked.

#### End-to-end flow

```
SSPs (Magnite, Index Exchange, Freewheel, Pubmatic) — millions req/sec
  │   direct path = "Mountain Bidder";  Beeswax = proxy/middleman (aggregates
  │   exchange reqs → forwards to us → relays our response back to the exchange)
  ▼
Campaign service  — "do we want to bid on this IP?" → hits the membership cache
  ▼
Aerospike (HOUSEHOLD PROFILE)  ── KEY = IP ADDRESS, value = record w/ fields:
  segments, intent scores, segment scores, geo version, holdout IPs
  • ~300M IP keys, 3–5 TB, single-digit-ms latency
  • hit DIRECTLY every bid (no in-bidder in-memory tier: 3TB too big, and with
    300M evenly-distributed keys a subset cache has ~0 hit rate → not worth it)
  ▼
bid decision → VAST markup returned (Mountain Bidder: in bid resp or win-notif
  resp; Beeswax: hits the ad-markup service directly)

SPEND PIPELINE (right side):
SSP/Beeswax win-notif → Notification service (HTTP webhook) → raw wins to
  ScyllaDB (DEDUP — each win once, no double-counted spend) → Kafka →
  3 aggregators {frequency, spend, logs→GCS}  (whole pipeline ≈ 1 min)
```

**Latency budgets:** Mountain Bidder ~**200 ms** to respond; **Beeswax tighter, 15 ms** timeout window; Aerospike lookups single-digit ms.

#### Storage tiers — what lives where

- **Aerospike (hot, per-IP, must be fresh):** household profile (segments + intent scores + segment scores + geo + holdout IPs), **spend data, recency data, frequency data** (freq capping), and currently metadata.
- **Redis (slow-changing "static" data):** flight data, flight budgets, thresholds, weights — anything that doesn't change day-to-day. Bidder pulls Redis metadata on a **cron every 5–10 min** (NOT real-time → a stopped flight can keep spending up to ~10 min until the next pull; roadmap = notification-based updates).
- **Migration:** Aerospike → **ScyllaDB** ("our future is Scylla") + Redis. Driver: Aerospike is expensive / poor support; Scylla cheaper. Decision above Abbas.

#### How data lands in the caches (loaders)

1. **Python cache loaders (simplest):** read **CoreDB (Postgres) or BigQuery** → write near-identical JSON blobs to Redis/Aerospike. Minimal transforms (live schedules e.g. World Cup do a little).
2. **Membership consumer (the path TI scores travel):** scoring team writes scores to a **GCS bucket** → GCS event trigger → **PubSub** → **RabbitMQ** (messages carry GCS file URLs). Consumer downloads + processes the (large) file → writes **Aerospike**. Logic: intent scores **batched** (not line-by-line); **IP with empty segment list → deleted** from Aerospike; special handling for holdout IPs. Runs in **Kubernetes**. Handles everything household-profile-related.
3. **PCS (Pacing Controller Service) + Campaign Metadata Service (CMS):** perf-pacing team write the **static pacing data** (flight budgets/thresholds/weights) via a separate service → currently Aerospike, moving to Redis.

#### Where scores live (resolves a recurring question)

**Intent/MM scores are NOT stored in MembershipDB.** Scoring team → **GCS** (durable source) → membership consumer → **Aerospike** (serving copy). MembershipDB emits the **segments**; the membership consumer writes those too. So: GCS = system of record for scores, Aerospike = bid-time serving store, MembershipDB = segment/membership authority + holdout logic.

#### Holdout logic (ghost-bidding / BER-2250 relevance)

**Holdout logic lives in MembershipDB**; holdout IPs are mirrored into the Aerospike household profile. Ryan flagged that moving **geo-radius targeting into the bidder** would force the ghost-bid holdout logic to move there too (or a new mechanism) — a reason to be cautious about pushing geo logic bidder-side. Many IPs lack MaxMind geo data; the bid request itself carries geo.

#### In-flight / future state (low-confidence — confirm w/ Eric)

- Adding a **dedup cache** on the membership-consumer write path (don't write every time).
- **Splitting the membership consumer** into separate **recency** and **membership** consumers.
- Likely **read GCS directly**, skipping RabbitMQ, if PubSub limits allow.
- Optimization surfaced (TI-1016): **don't write intent scores for IPs that have no segments** — the score-write path currently skips that check; Abbas + Ryan agree it "probably should" (a conditional write). The original "don't store 3P-OR-include IPs" idea is largely inert because key = IP and only in-use IPs are stored.

### Bidder — CANONICAL reference (Confluence BP space, captured 2026-06-09)

Source of truth for the bidder platform. Confluence: [Bidder (BP space)](https://mntn.atlassian.net/wiki/spaces/BP/pages/1860010029/Bidder). Local copy: `documentation/docs/bidder_platform_confluence_reference.pdf`. Where this conflicts with meeting notes above, **this wins** (it's the maintained reference). The Abbas section above adds operational color (latency budgets, current ownership, migration status) not in the wiki.

**What the bidder does:** decides whether MNTN should bid on a given impression, based on (a) the price MNTN is willing to pay and (b) a set of metrics evaluated against thresholds. Built on **OpenRTB 2.5** + Beeswax's proto tweaks (`openrtb.proto`, `openrtb_common.proto`). Historically a custom bidder agent inside Beeswax; being replaced by the in-house **"MNTN Bidder."**

**RTB lifecycle terminology (memorize — used everywhere):**
1. **Auction** ("bid request") = we're told about an opportunity.
2. **Bid** ("bid response") = we submit a price for a campaign on that auction.
3. **Win** = our bid won the auction. **win rate = wins / bids.**
4. **Impression** ("imp") = the winning creative is actually shown. **use rate = imps / wins.**

#### Three service groups (each is a repo under github.com/SteelHouse)

**A. Cache loaders** — make data from other sources low-latency-accessible to the bidding services (so the hot path never queries Postgres directly). Three:

1. **membership-consumer** — receives household-profile updates (IP → segments) from the targeting team and records them in **Aerospike**; also consumes impression/pixel hits. Deployments:
   - **cse** = live membership consumer (reads **Kafka** → Aerospike)
   - **oracle** = batch membership consumer (reads **GCS** → Aerospike)  ← this is the path TI score dumps travel
   - **recency** = recency consumer (reads Kafka → Aerospike)
   - *(Reconciles Abbas's "splitting recency vs membership": the repo already has these as distinct deployments; the work is making them independent services.)*
2. **rtb-cache-loaders** — services that periodically read **Postgres → Aerospike**, fully replacing a data set each run. Caches: `deals` (PMPs + Exclusives — generally MNTN Select / MSS info), `settings` (PMPs + Exclusives), `segment-mapping` (Beeswax segment ids ↔ Mountain segment ids), plus creatives, publisher pricing, "many more." Source Postgres tables are in the repo's queries.
3. **beeswax-audience-consumer** — pushes audience user-id + segment keys to Beeswax over HTTP (so Beeswax recognizes our segment ids), from Kafka. **Goes away once MNTN Bidder is fully released** (all campaign groups migrated).

Plus a special one owned by **Performance Pacing (PER squad)**: **campaign-metadata-service (CMS)** — writes pacing data calculated by PER, read by the JVM bidder / campaign service.

**B. Bidding services** — billions of daily bid requests → a bidding service → bids on behalf of eligible campaigns. Generate two critical log streams: **"auction" logs (fka "augmentor" logs)** and **"bid" logs (fka "bid price logs" / "BPL")**. Two eras run in parallel during migration:

- **Beeswax era (being retired):**
  - **rtb-augmentor-service-rs** — reviews the incoming auction request, responds to Beeswax with applicable segment ids + creative specs (single-digit ms, using cache-loader caches). Writes **auction logs**. Caches: `household-profiles` (= segments for IPs), `segment-mapping`.
  - **rtb-bidder-service (JVM Bidder)** — Beeswax sends it the eligible campaigns; it produces bids/campaign, handling **pacing (IHP = In-House Pacing), fcap, pricing**. **Beeswax quirk: we don't learn win/loss until much later (minutes) via the spend pipeline** — significant downstream implication. Writes **bid logs**. Its `household-profiles` cache = "everything but segments": `geo_version`, `timestamp`, `hhs:timestamp`, `hhs:segment:ttl`, `hhs:campaign` (per-campaign household scores, present on ~63% of records), `hhs:advertiser` (per-advertiser scores, ~35%), `hhs:segment` (usually empty), `segments` (~4%). *(The Confluence "Bidder" PDF rendered these bins as `is:*`; the authoritative Aerospike datastore dump shows they are `hhs:*` = household score.)* Also caches `recency`, `settings`, `spend`, `price`, `bid volume`/`inflight bids` (IHP).
- **MNTN Bidder era (going forward):**
  - **rtb-campaign-service (Campaign service)** — the unified bidder. Receives auction notifications, checks IP + creative specs against campaign segments, calculates and returns bids (or no-bid). Effectively the old Augmentor **plus** Bidder collapsed into one (it calls the **Pacing Engine** internally instead of handing off to Beeswax). Writes **both** auction + bid logs (in the `/v2/` GCS folder space). Caches: **Redis** (internal fcap tracking) + **Aerospike** (`frequency` = per-IP wins from the spend pipeline; `household-profiles` = segments for IPs).
  - **Pacing Engine** = the JVM Bidder re-written as a **Rust crate inside the campaign service**; supports PTV, Select, Media Plan (same pacing behavior reused across products).

**C. Spend pipeline** — records auction wins → tracks spend + frequency (feeds pacing/fcap and "win" logs). Critical; dropping data is very bad. Three services:

1. **rtb-notification-service-rs** — SSPs (incl. Beeswax) fire **win notifications ("NURL")** via HTTP (we hand them the NURL in bid responses). Writes to **ScyllaDB** for **dedup**, and the write triggers **CDC** events onto a Kafka topic. Future: also loss/billing notifications (**LURL** + **BURL**) — not supported by all SSPs (e.g. Magnite doesn't). Cache: **`rtb.wins` (ScyllaDB)** = wins tracking.
2. **rtb-win-aggregator-service** — consumes the CDC Kafka topic three ways: (i) update **spend in Aerospike** (for IHP), (ii) **write win logs to GCS**, (iii) update **frequency in Aerospike** (for fcap).
3. **rtb-impression-consumer-service** — listens to the **`vast_impression` Kafka topic** from ad_service (via events service) to learn which wins actually rendered; updates ScyllaDB with `impression_timestamp` for `mntn_auction_id`. **SSP-only** (Beeswax only sends wins that already have impressions). *(This is the diagram's "Impression service / VAST Start consumer.")*

Shared repos: `rtb-rs` (config/logging crates), `rtb-proto` (shared protos; builds python/rust/kotlin), `bidder-automation-core` (e2e test libs), `rtb-performance-test` (Locust).

#### Price + threshold logic — the analyst-facing DW tables (high value)

**Bid price.** Comes from the DW view **`summarydata.publisher_adsize_metrics`** = average CPI (avg of win prices over the **last 3 days**) per publisher × ad size (Height × Width × duration). If no price for the requested ad size → fall back to avg CPM for that ad size across all publishers. The base price is scaled per-campaign by **`pace_multiplier`** in **`sync.creative_metadata`** (default 1; DCO updates the underlying table).

**Eligibility thresholds.** Each creative has thresholds from **`sync.creative_metadata`**: `recency_threshold`, `recency_floor_threshold`, `household_score_threshold`, `viewability_score_threshold`, `publisher_price_threshold`. **A threshold with a null or zero value is not evaluated.** If any evaluated threshold fails, the creative is ineligible for that impression.

| Threshold | Metric it's compared to | Threshold table (`dso.*`) | Rule |
|-----------|------------------------|---------------------------|------|
| **Recency** + **Recency Floor** | `recency` = epoch of last visit to the campaign's AID (source: `vast_impression`, guidv2 Kafka stream), converted to a duration | `dso.recency_score_thresholds` | eligible iff `recency_floor < recency_duration < recency_threshold`; missing bound = that side not checked |
| **Household Score (HHST)** | IP's household score (value source: `tpa`) | `dso.household_score_thresholds` | score ≥ threshold to bid (below → ineligible) |
| **Viewability Score** | `viewability_rate` in **`logdata.publisher_adsize_metrics`** | `dso.viewability_score_thresholds` | publisher viewability ≥ threshold |
| **Publisher Price** | `avg_cpi` in `summarydata.publisher_adsize_metrics` (also the value used in bid-price calc) | `dso.cpm_thresholds` | bid only if publisher price ≤ threshold |
| **Publisher Performance** | `score` in `summarydata.publisher_adsize_metrics` | `dso.publisher_performance_thresholds` + `dso.network_performance_threshold` | performance score ≥ threshold |

`publisher_adsize_metrics` columns: `site, width, height, duration, avg_cpi, min_cpi, max_cpi, viewability_rate, score`. Note the **two views**: `summarydata.publisher_adsize_metrics` (price) and `logdata.publisher_adsize_metrics` (viewability).

**Recency Threshold = MAXIMUM age** ("don't show if last visit older than X"); **Recency Floor = MINIMUM age** ("don't show if visited more recently than Y"). Worked examples: 30-min recency threshold + 15-min-ago visit → eligible; 10-min floor + 5-min-ago visit → ineligible.

**Ghost Bids:** advertisers measure incrementality via **holdout segments** — the bidder logs a **Bid Drop Reason as late as possible** as a `ghost-bid`, so Reporting can query them downstream. (Ties to BER-2250.)

#### GCS log buckets

- Auctions: `bidder-auction-events-prod-{east,west}` ; Bids: `bidder-bid-events-prod-{east,west}` (Beeswax era under `/topics/rtb-bid-events/` & `/topics/rtb-bid-price-events/`; MNTN-Bidder era under `/v2/`).
- Wins: `bidder-win-notifications-{dev,prod}-central`.
- **Log-lineage note for BQ:** `bidder_auction_events` ← "auction" logs (fka augmentor logs); `bidder_bid_events` ← "bid" logs (fka bid price logs / BPL). CDC = Change Data Control.

### HHST — what it is and what gates it (Ryan Kleck, 2026-06-01)

**HHST = Household Score Threshold.** A campaign-level (or advertiser-level — see below) threshold setting that controls whether the bidder uses MM/RTC/Fangorn scores to gate bidding.

- **HHST set:** bidder filters bid eligibility by score ≥ HHST. Only IPs scoring above the threshold get bid on.
- **HHST not set:** bidder ignores scores entirely, regardless of what's in the `score` block of the audience expression. RTC, Fangorn, MM batch — all become no-ops for bidding decisions.

**Critical correction from Ryan (2026-06-01):** "we also do advertiser level scores, but again the bidder doesn't use those unless HHST is set... **so whether the bidder uses scores is really more about HHST being set or not**." The HHST gate applies to:
- `household_score` (per-IP, per-campaign or per-advertiser, populated by MM/Fangorn batch)
- `advertiser_household_score` (per-IP, per-advertiser, populated separately)
- `realtime_conquest_score` (RTC, per-IP, real-time)

For ALL of these, **the bidder's use of the score is gated by HHST**. Score presence in `cost_impression_log.model_params` is independent of the bidder's use of that score. If HHST isn't set, the score is just metadata — it doesn't affect bid eligibility.

**Two-stage gate (re-emphasized):**
1. Stage 1 — scoring pipeline: writes scores to GCS if DS13/19 is in the expression, regardless of HHST.
2. Stage 2 — bidder consumption: uses the score for bid filtering ONLY if HHST is set on the campaign.

This makes HHST the **most important campaign-level scoring switch** — far more important than expression-level features (DS clauses, `score_type=rtc`). The expression tells the bidder "this is how to score IPs in principle"; HHST tells the bidder "actually use that score."

**Where HHST lives in the schema:** unconfirmed. Likely a campaign/advertiser config table or a bidder runtime setting. Ryan's pointer for finding it: lookup bids for the campaign_id in `bid_events` and check whether the threshold is set on the bidder side.

### Bidder-side score logging — empirical finding (2026-06-01)

**Where the 0-10000 MM/Fangorn scores DON'T live: `silver.logdata.bidder_bid_events`.**

Empirical probe on `bidder_bid_events` for 2026-05-28 (33.1B rows):
- `household_score`: 99.738% = 0, 0.262% = negative. **Zero positive values.** No 0-10000 range observed.
- `advertiser_household_score`: same exact split — 99.738% = 0, 0.262% = negative.
- `conquest_score`: same exact split — 99.738% = 0, 0.262% = negative.
- `household_score_threshold` (HHST): **100% = 0.** Every row.

The three score fields have identical distributions, suggesting they're either copies of each other or all use 0 as a "not populated" placeholder.

**Implications:**
1. **The actual 0-10000 MM/Fangorn scores aren't surfaced in `bidder_bid_events`.** Either they're consumed upstream (in IPDSC / scoring pipeline / `augmentor_log`) and only a pass/fail boolean propagates to the bid-event row, OR `bidder_bid_events` is a downstream-filtered view that drops the raw score post-decision.
2. **HHST = 0 in `bidder_bid_events` is the "applied" value the bidder used, not the configured value.** The configured HHST (per campaign/advertiser) is elsewhere; by the time we see it here, every row reads 0.
3. **For the "what does no score look like in logs?" question** (Ryan Kleck didn't know, 2026-06-01): in `bidder_bid_events` the answer is **`household_score = 0` (NOT NULL)**. Per user expectation, the 0.262% negative values are likely `-1` sentinel for explicitly-unscored IPs — needs follow-up to confirm.
4. **MM scoring monitor / Pass 21 / Pass 26 bucket math is unaffected** — none of that work depends on bidder_bid_events scores. The IPDSC scoring pipeline and the GCS prospecting scores monitor are the correct sources for the 0-10000 distributions.

**Open question (worth probing later):** which table actually carries the 0-10000 scores at bid time? Candidates: `augmentor_log`, `bid_logs`, `cost_impression_log.model_params`, or per-IP scoring snapshots in `prospecting_intent_daily`. The first two have 10-day / 90-day TTLs respectively.

### Fangorn post-flip HHST trajectory — cohort baseline (TI-1017, 2026-06-02)

Across **316 advertisers** flipped onto Fangorn between 2026-04-01 and 2026-06-02 (filtered to >1K impressions in both the 7-day pre-flip and 14-day post-flip windows), the typical Fangorn flip barely shifts the bid-time HHST distribution:

| Stat | HHST=10000 share Δ (post − pre, pp) |
|------|------:|
| min (worst collapse) | -57.5 |
| p5 | -22.5 |
| p25 | -6.5 |
| **median** | **-1.6** |
| p75 | +0.3 |
| p95 | +5.1 |
| max | +44.1 |

- **66% of advertisers (208/316)** were within ±5 pp of pre-flip HHST=10000 share.
- **Only 4% (13/316)** had drops ≥30 pp post-flip.
- **Use as a reference baseline** for any future Fangorn-escalation. If an advertiser's HHST=10000 share dropped <20 pp post-flip, that's normal Fangorn re-calibration. ≥30 pp is the top-4% severity bucket — investigate audience design and YoY spend scaling.

**Cohort detection pattern:** `advertiser_id` set = `SELECT advertiser_id FROM dw-main-bronze.integrationprod.audience_advertiser_configurations WHERE vertical_data_source = 46`. Flip date = `DATE(MIN(TIMESTAMP_MILLIS(datastream_metadata.source_timestamp)), "America/Los_Angeles")` (matches TI-921's canonical flip-detection pattern).

**HHST-collapse failure mode (Autocamp case study, TI-1017):** Severe post-flip HHST drops cluster around advertisers with three coupled drivers:
1. **OR-semantics audience expression** between DS46 (MM) and DS19 (keywords), e.g. `(any(DS46, [vertical_id]) OR any(DS19, [keyword_ids]))`. With HHST > 0, the bid-eligible HI pool is the **MM ∩ DS19-keyword intersection at raw>0.8**, which is much smaller than either pool alone.
2. **High pre-flip HHST setting** (HHST=10000 specifically). The campaign was running pure-HI before the flip, so any narrowing of the HI pool surfaces as a pacing miss.
3. **Spend scaling outpacing the HI pool**, especially YoY. If the advertiser's daily spend has grown faster than their HI-eligible IP pool, the bidder hits the audience ceiling at HHST=10000 and has to drop HHST to fill spend.

When all three are present, the bidder progressively drops HHST → ends up serving the majority of impressions in unscored mode (HHST=-1). Counter-intuitively this often **improves IVR** (Fangorn's score quality lifts even the unscored fallback pool), but the operator-facing optics look alarming. *Mitigation framing for advertiser-facing teams:* the HHST trajectory in the UI is the bidder's pacing-adaptive response, not a Fangorn quality regression.

### Bidder "Bidder IPs Available by Intent Tier" chart — semantics (TI-1017, 2026-06-02)

The chart in the bidder UI titled "Bidder IPs Available by Intent Tier" (the one showing the High Intent / Peak Performance / Mid Intent / Max Reach stacked bars by day) shows **bid-time tier classification of impressions actually bid on**, not **audience composition**. So an IP can be in the audience and still not appear in this chart's High Intent slice if it isn't bid on at the moment in question. When HHST collapses to fill pacing, the chart's tier mix shifts (e.g., HI → Max Reach) even though audience membership is unchanged. APEX is the right reference for audience-composition questions.

### Unscored IPs degrade attribution match rate — conversion CVR is undercounted (TI-1017, 2026-06-02)

**Empirically observed on Autocamp campaign 570106 after 5/18 Fangorn flip** (via the "Audience Quality" report in the campaign Reports tab, surfaced by Trixy):

| Metric | Pre-Fangorn (5/01–5/17) | Post-Fangorn (5/18+) |
|--------|------------------------:|---------------------:|
| Visit Match Rate (`matched_raw_pv / wins`) | ~95% | ~80% |
| Conversion Match Rate (`matched_raw_conv / wins`) | ~70% | dropped to ~20%, recovered to ~40% |

**Mechanism.** Post-Fangorn, the campaign was serving ~55% of impressions in HHST-unscored mode (HHST=-1, bidder ignores scores). Unscored IPs are by definition outside the MM scoring substrate (not in DS13/DS19) and have **lower identity-graph coverage** — they're less likely to appear in DS34 (CRM), DS21 (retargeting), or other identity-resolution sources. So:

- **Visit attribution** still works reasonably well (~80% match rate post-flip) because visit attribution uses simpler signals — IP / cookie within session, short lookback.
- **Conversion attribution** craters (~70% → ~20–40%) because conversion matching requires longer-term identity persistence (multi-day lookback, cross-device, household-level resolution). Unscored IPs don't have this coverage.

**Measurement implication.** When the bidder serves a meaningful share of impressions on unscored IPs:
- **Measured CVR (conv/visit) drops** because we can't see the conversions that occur. True CVR may be flat or higher.
- **Measured ROAS may be undercounted** by the same factor — order amount attribution depends on conversion attribution.
- **Visit metrics are robust**: IVR (visit/imp), reach, completion rate are unaffected by the attribution silo.

**For Fangorn-flip diagnostics, treat conversion-based KPIs with a match-rate caveat.** Use visit-based metrics (IVR, reach, completion rate) as the primary signal for measuring Fangorn impact. If you must report on CVR / ROAS post-flip, pull the conversion match rate from the Audience Quality report and either (a) normalize the conversion count by `1 / match_rate` to estimate true conversions, or (b) restrict the CVR comparison to the scored-IP subset of impressions (filter `cost_impression_log` to `advertiser_household_score >= 1`).

**This is a structural feature of the unscored-IP fallback path, not Fangorn-specific.** Any time the bidder runs in HHST=-1 mode (pacing pressure, low-supply day, audience exhaustion), expect conversion attribution to degrade in proportion to the unscored share. Worth noting on the operator side: an HHST drop carries both a quality cost (lower-intent IPs) AND a measurement cost (undercounted conversions).

**Source:** "Audience Quality" report in campaign reports → Analytics tab → Audience Quality. Two charts: Visit Match Rate (`matched_raw_pv / wins`) and Conversion Match Rate (`matched_raw_conv / wins`). Other reports on the same tab: Audience Changes, TOW Changes, FPA Bleed, Day0 IVR.

### Intent Tier Thresholds (Prospecting Scoring Pipeline)
Source: `gs://household-scoring-prod/output/scoring/prospecting_intent/` — daily per IP/advertiser/campaign. Scores retained only **35 days** in active storage.

| Tier | Score Range | Criteria |
|------|------------|----------|
| **High Intent** | 10000 | Vertical + keyword match |
| **Peak** | 7000-9999 | Vertical only (no keyword) |
| **Mid Intent** | 3333-6999 | Keyword only (no vertical) |
| **Max Reach** | <3333 | Neither vertical nor keyword |

**Coverage rates in ITT analysis (Alex Knorr pre-analysis, April 2026):**
- High intent: **3.4% median** coverage of treatment group (NOT 14% as initially estimated in meetings)
- Peak: **0.2%** median coverage
- Mid intent: **0.04%** median coverage
- LATE (Wald estimator) only credible above ~4-5% coverage → only high-intent barely crosses this threshold
- 10 advertisers across 8 verticals, 25-day post-period (Mar 21 – Apr 14)
- External table: `dw-main-bronze.external.TI_835_prospecting_scores`
- Full report: `reports/TI_835_Pre_Analysis_v4.html` in SteelHouse/databricks_targeting (branch TI-835)

**Use for incrementality analysis:**
- Compare visit rates between 10% holdout (no impressions ever) vs 90% targeted group
- Use ITT (Intent to Treat): compare ALL IPs in 90% group, not just those who actually received impressions
- Why ITT: only a fraction of the 90% actually gets impressions (budget-constrained). Comparing only impression-recipients vs holdout introduces selection bias (impression receipt correlates with behavioral differences like watching TV at that time)

**guid_log vs clickpass_log for incrementality (TI-835):**
- **guid_log** captures ALL pixel visits (direct, organic, paid, VV — everything). Both holdout and targeted IPs visit at the same rate → ~0% lift. This measures total site traffic, which MNTN ads barely move.
- **clickpass_log** captures VV-attributed visits only (user clicked through from MNTN ad). Targeted group has 2-8x more attributed visits than holdout → massive lift. This measures MNTN attribution signal.
- Use clickpass_log for "does our targeting drive attributed visits?" Use guid_log for "does our targeting drive total site traffic?"

**BQ holdout hash (ported from Greenplum, TI-835):**
```sql
-- BQ equivalent of MD5('{AID}:{IP}') unsigned mod 1000:
MOD(
  ABS(
    CAST(
      CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(CAST(advertiser_id AS STRING), ':', ip))), 1, 16))
    AS INT64)
  ),
  1000
)
-- Bucket 0-99 = holdout (10%), 100-999 = targeted (90%)
-- Per-advertiser per-IP assignment
```

**Key gotcha — ITT dilution:**
- If the audience is 10M but the budget only reaches 10% of them, the 90% targeted group's visit rate is diluted by the 80% who were eligible but never actually received an impression
- The true treatment effect is on the impression-recipients, but ITT gives you the unbiased average effect across the full eligible group
- For a finer analysis (actual treatment effect on the treated), need to model the impression-receipt probability — more complex, not needed for initial analysis

### advertiser_high scoring fanout — no liveness filter (Zach + Ryan, 2026-05-26)
Source: `SteelHouse/airflow-ti` → `spark/audience_intent/advertiser_high.py` (lines 86-101).

Pipeline scores at the **vertical** level (IP × vertical_id × household_score), then fans out to every advertiser via:
```python
high_intent_cats = advertiser_verticals.filter(F.col("type") == 1).select("advertiser_id", "vertical_id")
final_output = vertical_scores.join(F.broadcast(high_intent_cats), on="vertical_id", how="inner")
```
The **only** filter on the advertiser side is `type == 1` (high-intent vertical mapping). There is **no active/live/churn filter**.

**Consequence:** every IP gets a score row for **every** advertiser that has ever had a `type=1` row in `advertiser_verticals` — ~25K advertisers — even though only **~300-400 are actually live** (Zach 2026-05-26). ~60-80x compute/storage waste downstream.

**Symptom that surfaced this:** a single IP returning ~20k advertiser_high score rows. That's expected under current code, but it shouldn't be.

**Why a naive "filter to live" isn't enough:** advertisers whose campaign starts tomorrow need to be scored today (cold-start). The fix needs to include scheduled launches, not just currently-active campaigns. Owner of the fix: Victor Savitskiy (Ryan handed off 2026-05-26 while OOO 3 days).

---

## Audience System Architecture

### audience.audiences vs audience.audience_segments (Greenplum)
These are two distinct concepts, often confused:

| Table | Purpose | Used for targeting? |
|-------|---------|-------------------|
| `audience.audiences` | Named audience templates (reusable definitions) | NO — template only |
| `audience.audience_segments` | Actual targeting expressions for campaigns | YES — this drives delivery |

**Implication:** Querying `audiences` alone will NOT reveal which audience was actually used for
targeting. Always join to `audience.audience_segments` for campaign-level analysis.

- **`expression_type_id` = 1 vs 2 (corrected 2026-04-23 via Jordan + empirical check):** Type 1 is NOT "legacy / ignore it" as earlier framed — it's the **OPM (first-party retargeting) audience system** with text-format expressions against pixel/user attributes. Type 2 is TPA (third-party targeting) in JSON format. Both are live, BUT see the `is_targeted` rule below — only type=2 rows are ever the active live targeting form.
- **`is_targeted` semantics (verified empirically 2026-04-30, TI-837):** for any audience that has both representations, the type=1 (OPM) row has `is_targeted=FALSE` and the type=2 (TPA) row has `is_targeted=TRUE`. Org-wide retargeting (`objective_id=4`): 0 of 64,202 type=1 rows are `is_targeted`; 39,440 of 55,447 type=2 rows are `is_targeted`. **Implication:** OPM/type=1 rows are source representations that get wrapped into the TPA/type=2 row at bidder evaluation time. Only the type=2 row carries the holdout clause and is the one the bidder uses. **For analyses that count "actively targeted retargeting audiences," use `expression_type_id = 2 AND is_targeted = TRUE` and you will not miss any audiences** — every retargeting audience appears as a type=2 row when active.
  - **`expression_type_id = 1` (OPM / FPA):** text-format expressions like `UserNumPageViews <= 22`, `UserLastVisitTime <= 0,day`, `UserPageViews contains [...]`, `UserNumVisits >= 1 AND UserAvgVisitDuration <= 60,min`. Matches users by MNTN pixel / site-tracking attributes. Used for retargeting audiences.
  - **`expression_type_id = 2` (TPA):** JSON expressions with `categories.where.value[]` structure, referencing `data_source_id` + `category_ids`. This is the prospecting / audience-targeting system.
  - **Link between the two:** when a TPA expression contains `"data_source_id": 2, "category_ids": [X, Y]`, the category_ids X/Y are **OPM `audience_id` values** (from the expression_type_id=1 table). So DS2 in a TPA expression = pointer to an OPM retargeting audience. Same mechanism for `blockFirstParty` exclusion (adds DS2 exclusion categories pointing at OPM page-view / conversion audiences).
  - **TI-896 earlier filter `expression_type_id = 2 AND is_targeted = TRUE`** is correct for the TPA/prospecting lane — OPM (type 1) is a separate system. But type 1 is NOT "to ignore" broadly; it's the FPA data used elsewhere in the bidder.
- **Per-advertiser data source IDs** (`data_sources.data_source_type_id = 2`, IDs ≥1000) come in 6 named container types: `{AID} - First Party Audience`, `{AID} - Third Party Audience`, `{AID} - Control Group Audience`, `{AID} - Extension Audience`, `{AID} - Prospecting Campaign`, `{AID} - Retargeting Campaign`. **Mostly absent from segment-archive expression JSON, but NOT universally absent** — empirical correction 2026-05-29 (TI-999): at least one advertiser (AID 36678) references its own per-advertiser DS `36678 - Prospecting Campaign` (DS69734) in 6 active expressions. The earlier TI-896 cohort sample (April 2026, found 0 references) only looked at the four audience container types (First/Third Party / Control / Extension); the two campaign container types (Prospecting Campaign / Retargeting Campaign) — which exist for 11,721 advertisers — do appear in some expressions. **Implication for audience-bucket detectors:** don't assume only IDs 1-99 appear in expressions. Either (a) enumerate all referenced DS IDs from a sample of expressions, or (b) join to `data_sources` and filter by `data_source_type_id` after-the-fact rather than by ID range. Audience-bucket detectors that match by name pattern (e.g., `name LIKE '% - Prospecting Campaign'`) WILL fire on at least some expressions.
- **DS2 is OPM segments, NOT Mountain Matched** (Alyson + Zach 2026-04-22, Slack). Bryce's earlier mapping (DS2 = MM) was wrong. DS2 in segment-archive expressions usage breakdown: ~99.7% in inclusion clauses (`op: "any"`), ~0.3% in exclusion clauses (`op: "not"` per V4 in TI-896). Treat DS2 trajectory as OPM-segment-targeting, not MM.
- **DS2 architecture (authoritative, per Jordan Piepkow Slack 2026-04-23, referencing `SegmentExpressionService.kt:134-178` and `DataSource.kt:31`):** DS2 = "MNTN First Party", `CommonDataSource.MNTNFirstParty`. DS2 is a **container for first-party (OPM) segment IDs** — each DS2 `category_id` inside an audience expression is a **pointer to an OPM segment** (pageview segment, conversion segment, etc.), NOT the expression itself.
  - **When processing TPA expressions**, the expression service extracts DS2 categories to detect `isFpaAudience` (whether the audience uses first-party data).
  - **OPM-type expressions get wrapped** into a TpaExpressionV1 with `MNTNFirstParty.dataSourceId (2)` — encoding "this OPM segment should be resolved via DS2."
  - **`AdvertiserConfiguration.blockFirstParty`** flag controls whether first-party data gets excluded. When ON, exclusion categories are added under DS2 to block page-view / conversion segments from targeting (`SegmentExpressionService.kt:662, 690`).
  - **Python migration script** strips DS2 entries from expressions during processing (alongside DS8/IP List) — they're removed from or/and clauses. DS2 references are somewhat vestigial / tooling-dependent.
  - **DS2 appears in an expression when** the audience references OPM first-party data (targeting or blocking). Audiences with no first-party references (pure 3P, pure CRM, pure keyword) won't have DS2 entries.
  - **Membership cache:** `tmul_daily` holds DS2 + DS3 segment memberships per IP (14-day TTL) for fast bidder lookups.
  - **Category_id space is independent** — DS2 category_ids (278889, 514936, 555664, 565599, etc.) don't appear under DS13/DS19/DS35 (verified in TI-896 2026-04-22 via ipdsc sample).
- **DS2 disappearance observed Oct 27 2025 in TI-896** (spend-weighted share 73-79% → 42-46%) is therefore most likely a product-side change: (a) MMv3 default audience template stopped auto-adding OPM pageview/conversion references, or (b) `blockFirstParty` default behavior changed so fewer advertisers have first-party exclusion active. Real audience-side change worth surfacing; not a pipeline artifact.
- **Mountain Matched the product (MM 2.0) is not detectable by simple DS-id matching.** It's the umbrella audience system with score tiers (Max Reach / Mid Intent / Peak Performance / High Intent per state table; see `mntn_business.md`). Open question for Ryan / Zach: what's the authoritative MM-product detector at the database level?
- **Canonical `data_sources.name` vs Bryce war-room labels (TI-896, 2026-04-22):**
  - DS2 canonical name = "MNTN First Party"; Bryce called it "Mountain Matched (MM)" — wrong, per Alyson + Zach 2026-04-22 it's OPM
  - DS19 canonical name = "MNTN Matched"; Bryce called it "Keywords"
  - Use canonical / authoritative labels (per Zach), not Bryce's product labels.
- **Audience-bucket detector convention (used in TI-221, TI-270, TI-896):** filter `expression LIKE '%"data_source_id":N,%'` (Postgres) or `REGEXP_CONTAINS(expression, r'"data_source_id"\s*:\s*N\b')` (BQ). Always with `expression_type_id = 2 AND is_targeted = TRUE`.
- `audience.audience_segment_campaigns` maps which audience segment is active for each campaign — **1:1 mapping with campaign_id** (not campaign_group_id). This is the real thing that drives delivery.

### Audience Expression Structure (Nicholas, 2026-04-07)
The expression JSON (type 2) has 3-4 overarching AND clauses:
1. **selects** — category selections
2. **categories** — DS19 keywords, data source filters, CRM blocks, visitor/converter lookbacks (30d)
3. **geos** — geography targeting (usually US)
4. **holdout/buckets** (optional) — bucket range for holdout (0-99 out of 1000) or experiment groups (e.g., 0-500)

The relationship chain: `campaign_groups` (template) → `campaigns` (actual bidding entities) → `audience_segment_campaigns` (1:1 with campaign) → `audience_segments` (expression JSON).

`audience.audiences` is just another template wrapper — like campaign_groups is to campaigns. Don't query it directly for analysis.

### audience.campaign_segment_history Contamination
`audience.campaign_segment_history` blends both `audiences` (template) and `audience_segments`
(targeting). This table is the source of contamination bugs — using it to determine "what audience
was this campaign using" is unreliable because template objects appear alongside active targeting.
Use `audience_segment_campaigns` + `audience_segments` instead.

**BQ equivalent:** `summarydata.v_campaign_group_segment_history` (VIEW) — same issue applies;
verify against `audience_segment_campaigns` for production analysis.

### Prospecting vs Retargeting (Audience Type)
- **The prospecting/retargeting axis is `objective_id`, NOT `funnel_level`.** Use `objective_id IN (1, 5, 6)` for prospecting (or `NOT IN (2, 4, 7)` to exclude onsite, retargeting, ego). `funnel_level` is MNTN product stage (S1/S2/S3) — every stage contains both prospecting and retargeting campaigns. Verified empirically 2026-05-05: 21,639 retargeting campaigns sit inside `funnel_level=1` (~27% of Stage 1).
- For RTC monitoring specifically: filter `funnel_level = 1 AND objective_id IN (1, 5, 6)` — RTC is a prospecting product at Stage 1.
- **Anti-pattern:** filtering `funnel_level = 1` alone to mean "prospecting only" — it includes Stage 1 retargeting.

### Three Universal Rules for Audiences + Holdout (Zach Schoenberger, AUTHORITATIVE, 2026-04-30)
Zach Schoenberger is the highest-confidence source for audience-platform questions — when in doubt, defer to him.

1. **CRM lists are only usable in PROSPECTING campaigns, never retargeting.** A common misconception is that "retargeting on a CRM list" exists; it doesn't. CRM lists are an MNTN data source (`data_source_id=4`) used to find/match new IPs through prospecting campaigns. Retargeting (`objective_id=4`) targets users who have already engaged with the advertiser's site — page-views, conversions, OPM-resolved past visitors — not CRM uploads.
2. **Every campaign has a 10% holdout.** Universal. No exceptions for retargeting, CRM-targeted prospecting, or any other type. The 10% MD5(advertiser_id:ip) mod 1000 < 100 holdout is enforced by the bidder for every campaign.
3. **Every campaign has an audience expression.** "In order for us to buy ads for any campaign they need an audience." There is no edge case of a campaign with no audience expression — if it's eligible to bid, it has an `audience_segments` row. Empirically: this row is `expression_type_id=2 AND is_targeted=TRUE`.

**Implication for TI-837:** Alex K's concern "do retargeting / CRM campaigns have holdout?" was conflating two things. CRM lives on prospecting (which we always knew enforces holdout); retargeting is OPM-based site-visitor audiences (which we empirically verified also enforces holdout, 0/5.43M served IPs in holdout bucket). Both layers covered, no gap.

---

## Stage 3 VV Pipeline & IP Mutation Audit

### Campaign Stage Definitions (Zach, 2026-03-03 & 2026-03-04)

Stages are campaign targeting stages, not event types. Each stage targets a different IP audience.

| Stage | Segment populated by | Business meaning |
|-------|---------------------|-----------------|
| Stage 1 | Campaign setup (initial audience — customer data, lookalike, etc.) | All targetable IPs |
| Stage 2 | Stage 1 VAST Impression IPs | Users who were served an ad and it played |
| Stage 3 | **IPs that had a verified visit** (from any stage's impression) | Retargeting audience |

**Key rules:**
- Stage 2 is populated ONLY from Stage 1 VAST IPs.
- Stage 3 = any IP that had a VV. Two paths: (1) Stage 1 impression → VV → Stage 3, or (2) Stage 1 → Stage 2 impression → VV → Stage 3. Attribution doesn't follow the stage sequence — a VV can be attributed to any stage's impression.
- **Cross-stage key is vast_ip (event_log.ip), NOT bid_ip (empirically proven, 2026-03-10).** Either/or join: `prev.vast_start_ip = next.bid_ip OR prev.vast_impression_ip = next.bid_ip`. VAST event order: impression fires FIRST (creative loaded), start fires SECOND (playback begins). vast_start marginally better cross-stage (+256 matches on 487K pairs) but difference is noise. Either/or gains +351 matches (0.05%) — adopted in v9. 1.558% match neither (structural — CGNAT/SSAI/IPv6/VPN). No deterministic cross-stage ID exists besides IP (Finding #28).
- **bid_ip ≠ vast_ip in ~1.2% of impressions (3.54M/288.7M).** Five mechanisms cause the difference: CGNAT /24 rotation (35%), CGNAT wider /16 pool (25%), carrier /8 reallocation (6%), SSAI proxy — VAST callback from AWS server not user device (6%), dual-stack IPv4→IPv6 (12%), other — VPN/CDN/genuine network switch (16%).
- **event_log.ip has CIDR notation (`/32` or `/128` suffix) on ALL pre-2026 data.** Confirmed: 526M rows Nov-Dec 2025 = 100% CIDR, 393M rows Jan-Feb 2026 = 0% CIDR. Other tables (impression_log, viewability_log, clickpass_log, bid_logs, win_logs) have NO CIDR suffix in Feb 2026 data — this is event_log-specific. `/32` = IPv4, `/128` = IPv6. Exact string matching (`ip = 'x.x.x.x'`) will miss pre-2026 rows. Fix: `SPLIT(ip, '/')[SAFE_OFFSET(0)]` for reliable matching across time periods. `bid_ip` column does NOT have this issue (always bare IP). Any cross-stage query with lookback into 2025 MUST strip CIDR. Discovered TI-650 v16 2026-03-13.
- **CRM campaign_groups:** Identifiable by campaign_group name containing "CRM". These may use different targeting pools (data_source_id=4, email→IP). However, every VV in the prospecting funnel (objective_id IN 1, 5, 6) MUST follow the IP path (S3 bid_ip → prior S2/S1 VV, S2 bid_ip → S1 event_log). If resolution fails, the cause is insufficient lookback, table TTL, or a data bug — not identity graph bypass.
- **4 IPs per stage in audit table (collapsed from 6):** vast_start_ip, vast_impression_ip (both event_log.ip, different event_type_raw, 99.85% identical), serve_ip (impression_log.ip, 93.6% = bid_ip), bid_ip (= win_ip = segment_ip, 100%). Dropped: win_ip (=bid_ip 100%) and segment_ip (=bid_ip 100%, Zach confirmed). win_logs.impression_ip_address is infrastructure/CDN IP, not user.
- `first_touch_ad_served_id` always points to a Stage 1 impression (by definition: `funnel_level=1, objective_id=1`). `ad_served_id` (last touch) can point to Stage 1, 2, or 3.
- Stage 3 exists for retargeting — "last touch is king in ad tech" (Zach). Users who already visited are highest-intent, so we keep serving to maintain last-touch attribution credit.
- Scale: Stage 1 ~8.5M IPs → ~10K get impressions → ~2K enter Stage 3 (Zach's example).
- **Campaign groups are exclusive.** A VV in campaign_group 1 for an advertiser does NOT allow another campaign_group 2 for the same advertiser to target that IP at a higher stage. Each campaign_group's funnel is independent. Concurrent or previous campaign_groups have zero bearing on each other's stage progression. (Zach, 2026-03-05)
- **IPs accumulate stages within a campaign_group, never removed.** Frequency capping (14-day) handles dedup, not targeting removal. Budget: S1 ~75-80%, S2 ~5-10%, S3 = remainder.
- **Campaign ID = Stage (1:1).** Determine via campaigns.funnel_level (1=S1, 2=S2, 3=S3, 4=Ego). Bidder has no concept of stages.
- **objective_id reference (from `core.objectives` + Ray). Prospecting filter = IN (1, 5, 6):**
  - 1 = Prospecting (CTV Prospecting)
  - 2 = Onsite (ads on customer's own website)
  - 3 = Prospecting (duplicate of 1, not actively used)
  - 4 = Retargeting
  - 5 = Multi-Touch (S2 prospecting, newer naming convention)
  - 6 = Multi-Touch Full Funnel (MT+ = Stage 3, newer naming convention)
  - 7 = Ego (employee targeting — targeting advertiser's own employees)
- **CRITICAL: objective_id is UNRELIABLE as a stage indicator (Ray, 2026-03-11).** During the "TV Only" UI migration, the UI team stopped setting objective_id correctly and pivoted to using funnel_level instead. Result: 48,934 "Beeswax Television Multi-Touch Plus" campaigns (S3/MT+) have `objective_id=1` instead of `6`. The old parent/child campaign_group structure (parent=obj 1/channel 8, child=obj 5+6/channel 1) was collapsed into a single campaign_group with 3 campaigns, and objectives were not updated. **funnel_level is the authoritative stage indicator, not objective_id.**
- **CRITICAL: funnel_level ≠ prospecting.** Retargeting campaigns (objective_id=4) exist at every funnel_level (1/2/3). For prospecting-only analysis, use `objective_id NOT IN (4, 7)` (safer) or `objective_id IN (1, 5, 6)` per Ray — both work because mislabeled MT+ campaigns have objective_id=1 (included either way). Excludes Retargeting (4), Ego (7). `objective_id IN (1,5,6)` also excludes Onsite (2) and unused dup (3).
- **campaign_group_id scoping required for cross-stage IP linking (TI-650 v14).** All cross-stage IP matching MUST be within the same `campaign_group_id` — matching across groups is coincidental, not funnel trace. `campaign_group_id` is unique across advertisers (only `0` is shared as null/default). Using `advertiser_id` instead inflates resolution rates ~5pp. Zach directive 2026-03-12.
- **objective_id × funnel_level distribution (2026-03-12):** S1: obj=1 (52K prosp) + obj=4 (20K retarget). S2: obj=1 (43K broken) + obj=5 (64K MT prosp) + obj=4 (19K retarget). S3: obj=1 (43K broken) + obj=6 (60K MT+ prosp) + obj=4 (19K retarget). The 43K S2/S3 campaigns with obj=1 are from the TV Only UI migration break.
- **Zero-chain advertisers in multi-advertiser analysis:** 4/10 top advertisers had zero S3→S2→S1 chain resolution despite having S2 campaigns in the campaigns table. Cause: those S2 campaigns had zero prospecting vast events in the 90-day lookback — they serve only retargeting (obj=4).
- **VV attribution = stack model.** Impressions stacked; page view checks top (most recent). Everything behind is ineligible.
- **VVS cross-device linking (Sharad, confirmed):** The Verified Visit Service links visits to impressions in two layers: (1) **IP match** — find impressions served to the same IP as the page view IP (primary), (2) **GA Client ID expansion** — using the page view's GA Client ID, find all IPs that Client ID has been seen with in the previous few days, then look for impressions on any of those IPs. Validations and filtering applied at each layer. See: Nimeshi Fernando's "Verified Visit Service (VVS) Business Logic" Confluence doc.
- **VVS determination logic (Nimeshi Fernando, Confluence):** Full decision tree: (1) advertiser_id valid? → (2) IP blocklist check (`segmentation.ip_blocklist`) → (3) GUID blocklist check (`segmentation.guid_blocklist`) → (4) cross-device config check (`vvs.cross_device_config` in Aurora DB) → (5) GUID match (`attribution_model_id=1`) → (6) IP match (`attribution_model_id=2`, includes CTV household_whitelist + iCloud IPv4 filter + GUID-to-IP count check) → (7) repeat with `viewable=false` impressions → (8) GA Client ID match (`attribution_model_id=3`, via `cookie.gaid_ip_mapping`) → eligibility checks (duplicate visit, TTL/acquisition window, advertiser TTL 45-day max) → (12) referral blocking / tamp detection (utm_source, utm_medium, utm_campaign, utm_content, gclid, cid, cmmmc). TRPX fires every page view; only first in session is eligible. VV window = 14-45 days per advertiser.
- **VV (Verified-Visit) lookback WINDOW — source of truth + change history (TI-1037 workflow, 2026-07-02):** the VV
  windows live on the **advertiser row** and its version archive `bronze.integrationprod.archives_advertiser_archives`
  (NOT `advertiser_configurations` — its `page_view_lookback_window` is a taxonomy/RT-membership lookback, a different
  concept, and `conversion_lookback_window` is NULL for most advertisers; NOT `r2_advertiser_settings` — no window cols).
  INTERVAL fields, magnitude in the DAY component: **PRO_Window (prospecting VV) = `clickpass_acquisition_ttl`**,
  **RT_Window (retargeting VV) = `clickpass_click_ttl`**, **conversion window (SEPARATE) = `conversion_window`**
  (+ `click_/view_/invoice_conversion_window`). Change history: `EXTRACT(DAY FROM ...)`, **ORDER BY `update_time`**
  (version non-monotonic if unsorted; `create_time` = account-creation stamp, not edit time), LAG-collapse to changes.
  Reusable change-log = TI-1037 perf_report module 11. Kindred 35094: 45/45 (Oct'23) → **30/14 (2025-08-08)** →
  **14/14 (2026-04-08)** — progressive PRO shortening 45→30→14; conversion window constant 30d its whole life.
  **⚠ The archive LAGS: `archives_advertiser_archives` stopped at v245/2026-03-27 (PRO=30) but the live table had the
  2026-04-08 PRO 30→14 edit — UNION the live `advertisers` row (its TTL is a STRING 'N days', parse the leading int) to
  the archive so recent changes aren't missed. Also: two windows per advertiser — PRO (prospecting) vs RT (retargeting) —
  a P2 window can STRADDLE a mid-period change.**
- **A CONVERSION requires a VERIFIED VISIT within the VV window (mechanism, high confidence, TI-1037 workflow 2026-07-02):**
  a UI-reported conversion (`from_verified_impression=TRUE`) is attributed to the SAME impression that produced a VV, via
  the SAME VVS engine — **100% of such conversions co-occur with a VV on the same `ad_served_id`** (Kindred 1,133/1,133;
  21,611 conv across 7 advertisers all 100%), sharing the `attribution_model_id` map above. **Official chaining (confirmed
  by Prod Ops Johnny + the internal knowledge app, 2026-07-02): impression →(VV window, from the impression)→ VERIFIED VISIT
  →(conversion window, 30d default, from the VISIT)→ conversion.** The VV window starts at the impression; the conversion
  window starts at the VISIT — two distinct clocks. So the VV window gates whether a visit exists AT ALL, and a conversion
  can only be credited if it follows a VV. Since `conversion_lookback_window`
  is unset and the **VV/page-view window is the only populated per-adv lookback**, **shortening the VV window yields fewer
  visits → fewer conversion anchors → can lower conversions/CVR/ROAS even though the conversion window is unchanged/separate**
  — on a **~window-length LAG** (old 31-45d-out attributions
  stop connecting only after the new window cycles through, so a naive same-day pre/post at the change date sees nothing).
  So Malachi's mechanism is correct; Mike Dolt's "separate fields" is factually true but does NOT make conversions
  window-independent. **Caveat (honesty):** the mechanism is proven, but the empirical MAGNITUDE is not cleanly isolated —
  Kindred's ~Sep'25 CVR move is confounded by a co-timed spend burst (netting it out weakens the window signal), and Bouqs
  is invalid (account pause 6d after its change); need a natural-experiment advertiser delivering continuously at stable
  spend 45+ days post-change to quote a number. **Always flag a VV-window change (module 11) before attributing any
  YoY/MoM visit/conversion/CVR change** — it's a measurement confound. [[reference_attribution_industry_standard_ft]]
- **VVS attribution_model_id reference:** 1-3 = Last Touch (guid/ip/ga_client_id), 4-6 = Last TV Touch (guid/ip/ga_client_id), 7-8 = Offline Attribution, 9-14 = Competing (guid/ip/ga_client_id variants), 15-16 = Impression-based (ip). Non-competing = 1-8, Competing = 9-14. Competing VVs stored in `competing_vv` Kafka topic.
- **PV_GUID_LOCK:** VVS stores impression GUID + page view GUID. PV GUID TTL = 30 min of inactivity (resets each TRPX fire). Handles IP changes mid-session. Advertisers with `pv_guid_lock = true` in `advertiser_configs`.
- **TRPX (tracking pixel):** Installed on advertiser webpages. Fires HTTP POST to VVS on every page view. Sends: ip, guid, gaid (GA Client ID), advertiserId, UTM params, referrer, userAgent, xForwardedFor, epoch. Response: `isSuccessful=true` (Last Touch VV) or `isSuccessful=false` (rejection or Competing VV). TRPX also sends GA data to attribution-consumer → Measurement Protocol → advertiser's GA property → logged in `analytics_request_log`.
- **CTV vs display attribution:** No preference between media types — treated identically in last-touch attribution. (Sharad, ATT, 2026-03-06)
- **Non-viewable display impressions:** Appear ONLY in `impression_log`, never in `event_log`. `event_log` only contains viewable CTV impressions (`vast_impression` events). For IP lineage tracing, `COALESCE(event_log.bid_ip, impression_log.bid_ip)` is required — `event_log` preferred; `impression_log` fallback for non-viewable display. (Sharad, ATT, 2026-03-06)
- **bid_ip COALESCE fallback pattern (TI-650 validation, Zach confirmed 2026-03-24):** `event_log.bid_ip` was intentionally designed to match `bid_logs.ip` — safe to use as fallback when bid_logs records are purged (90d TTL). Four tables store `bid_ip` as a denormalized column: `event_log`, `impression_log`, `viewability_log`, `click_log`. When `bid_logs.ip` is NULL (purged), COALESCE from these. Pattern: `COALESCE(NULLIF(bid_logs.ip, '0.0.0.0'), impression_log.bid_ip, event_log.bid_ip, viewability_log.bid_ip)`. Note: bid_ip may not exist in viewability_log for all events — Zach was uncertain if it was added for display viewability. Recovers ~50% of bid_logs-purged VVs; remaining ~50% have NULL bid_ip across all tables (pipeline didn't write it). `clickpass_log` does NOT have a bid_ip column — Zach acknowledged this is a gap ("that's a whole other thing").
- **bid_logs has multiple rows per auction_id (Zach, 2026-03-24):** A single auction is eligible for multiple campaigns, so `bid_logs` can have multiple rows for the same `auction_id` (one per campaign). Always dedup: `QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1` when joining via `impression_log.ttd_impression_id = bid_logs.auction_id`.
- **GCP data floor: January 1, 2025 (Zach, 2026-03-24).** No BQ table (silver or bronze) has data before 2025-01-01. Pre-2025 data only exists in Greenplum coreDW (deprecated April 30, 2026). All-time lookback queries are bounded by this floor.
- **90-day lookback sufficient for 99%+ of advertisers (Zach, 2026-03-24).** Most advertisers hit 99% resolution at 60 days. 90 days is overkill for the general case. Only WGU (31357) is a known outlier requiring >90d lookback. Other long-lookback cases (Zazzle, Ferguson Home) may be "neon pixel accounts" with special advertiser configurations — check `advertiser_configs` table for custom VV attribution windows. Optimization: run 90d lookback first, then all-time scan only for unresolved VVs.
- **Advertiser configurations for VV attribution windows:** `advertiser_configs` table (likely `bronze.integrationprod`) contains per-advertiser settings including custom attribution windows. Advertisers with extended lookback (like WGU) have special configs. Use this to identify which accounts will have edge cases in VV→impression resolution. (Zach, 2026-03-24)
- **Table design:** must support ALL stages per VV row. Stage 1 VV = S2/S3 cols NULL. Stage 3 VV = entire row full. Pipeline via SQLMesh. 90-day retention.
- **Deployment guidance (Dustin/dplat, 2026-03-05):** Silver layer is the correct location. SQLMesh recommended — handles orchestration and idempotency. Consider hourly materialization of source data first, then run the larger model over the reduced dataset. Set retention in the SQLMesh model or at table creation. Tag the table with the owning team. For very large batch processes, Spark + Airflow may be better.
- **VV-window shortening → CVR crash appears with a ~30-day LAG, not immediately (Kindred 35094 natural experiment, TI-1037).** Kindred shortened its VV lookback windows on **2025-08-08** (PRO 45→30, RT 45→14, per the VV Window Change Log dashboard — note this window value is NOT in any BQ table: `silver.audience.advertiser_configurations.page_view_lookback_window`=180 is a taxonomy/block lookback, `conversion_lookback_window` is NULL; the config archive has NULL for both; the reporting-layer VV window lives outside BQ, so trust the dashboard for the date). Daily `sum_by_campaign_by_day` (obj=1/funnel=1 prospecting + AID-wide, 7-day rolling): **CVR did NOT drop at 8/8** — it stayed elevated (AID ~12–15%, PRO ~9–11%) through **early September**, actually peaking the week ending 8/25 (AID CVR 15.5%). CVR then **crashed ~2× the week of 2025-09-08** (AID 13.2%→6.5%, PRO 10.4%→5.0%), i.e. **~30 days after the change** — exactly the signature of long-tail VVs (visits attributed to old 31–45-day-out impressions) aging out of the shortened window: the old-window conversions were still connecting through late Aug, and the connectable pool didn't collapse until ~30d of new-window impressions had accumulated. **This supports Malachi's hypothesis** (a conversion must connect to a VV; shrink the VV pool and CVR falls) **over Mike Dolt's "windows are separate so it shouldn't matter"** — but only with a lag, which is why a naive same-day pre/post at the change date sees nothing. **CONFOUND — quantify before attributing to VV window:** a discrete **1-week prospecting spend burst Sep 10–16** (prospecting imp/day ~20k→~90k, 4×, then snapped back to ~20k on 9/17) mechanically crashed the **visit rate** that specific week (AID VR 1.0%→0.58%, PRO 0.87%→0.46%) while conversions stayed flat-low (~9–26/day) — so that week's number reflects over-scaling/AUDI-1070 dilution, not the window. The **visit-rate** move is dominated by spend-scaling (denominator), the **CVR** move (conv/visit) is the VV-window-lag signal; separate them by working in conv/visit, not conv/imp. Also confounded: LowPop MT-S3 group 96108 launched 8/16 (tiny, 814k imp / 185 conv over 5 mo — negligible), and Dec holiday gate-off (outside this window). Net: **both visits and conversions dropped; conversions dropped proportionally more (CVR fell ~half); the CVR discontinuity is at ~+30d, consistent with VV-window aging, but a spend burst is superimposed and must be netted out.** Method note: use 7-day trailing rolling to see the lagged step cleanly; daily is too noisy (weekend flighting cycles) and calendar-week blocks can straddle the 9/8 break.

- **VV-window shortening lowers ABSOLUTE visits & conversions & visit-rate, but its effect on CVR (conv/visit) is AMBIGUOUS (Lizz Joslen, 2026-07-06 — refines the Kindred entry above).** Shortening the VV window (e.g. 30→14d) removes the visits that occurred in the dropped trailing window (days 15–30 after impression). That mechanically cuts **absolute visits**, **absolute conversions**, and **visit rate** (visits/impressions, since impressions are unchanged) — those *will* fall (if there were any visits in that tail). But **CVR = conversions ÷ visits is NOT guaranteed to move in either direction**: it only *falls* if the dropped tail visits converted at a **higher** rate than the retained early visits; if the tail converted **worse**, shortening **raises** CVR. Lizz's example: day-0–14 = 200 visits/50 conv (25%), day-15–30 = 30 visits/2 conv (6.7%) → 30-day CVR ≈22%, 14-day CVR =25% (CVR rose when shortened). Also the conversion window can still attach a conversion to a retained early visit, further muddying it. **Implication for diagnoses:** attribute the visit-rate / absolute-count drop to the window, but do NOT assume the conversion-RATE drop is measurement — the CVR decline may be real and must be checked empirically (as the Kindred entry did: its CVR *did* crash ~+30d because its aged-out long-tail visits happened to convert better). Frame the VV flag as "fewer absolute visits & conversions + lower visit rate; CVR effect ambiguous."

- **"The Bouqs" = AID 32147 (eCommerce), always (Johnny/Prod Ops, 2026-07-06).** Two live Bouqs advertiser accounts exist: **32147 "The Bouqs - eCommerce Unit"** (active — this is what "the Bouqs" refers to in NP/analysis) and **31906 "The Bouqs - Subscriptions"** (went DARK Dec 2025). Confusingly, the eCommerce unit (32147) *also* contains a prospecting campaign-group named **"Subscriptions" (116732)** — that is a campaign *within* 32147, NOT the separate 31906 Subscriptions advertiser. So "Subscriptions" is ambiguous: it's both a dead advertiser (31906) and a live campaign inside eCommerce (116732). Default to 32147 for any "Bouqs" analysis.

- **DS21 = MNTN Conversion, DS34 = MNTN Pageview — and EXCLUDING them is prospecting hygiene, not "retargeting" (Johnny, 2026-07-06; corrects a swapped/mislabeled chart legend).** In the shared prospecting `NOT(...)` block, `DS21` (the advertiser's own **converters**) and `DS34` (the advertiser's own **site-visitors/pageviewers**) are EXCLUDED so prospecting does not re-serve people who already converted or visited the site (i.e. don't prospect to the retargeting pool). Phrase it as **"excludes own converters (DS21) + own site-visitors (DS34) — standard suppression,"** NOT "added DS21/DS34 to exclude retargeting" (which reads backwards) and NOT "DS21=pageview / DS34=conversion" (they are the reverse). Authoritative names: `integrationprod.audience_data_sources` (data_catalog §DS registry).

### The IP Pipeline per Stage (empirically validated 2026-03-10)

Within a single CTV ad serve, there are only **2 distinct user IPs** per stage:

| IP | Table | Column | What it is | Validated |
|----|-------|--------|------------|-----------|
| **bid_ip** | event_log | bid_ip | Targeting identity (= win_ip = serve_ip = segment_ip) | bid=win: 38.2M rows, 47 differ (0.0001%) |
| **vast_ip** | event_log | ip | VAST playback IP — **enters next stage's segment** | bid≠vast: 1.02% (CGNAT /24 rotation) |
| redirect_ip | clickpass_log | ip | Redirect/visit IP (mutation boundary) | — |
| visit_ip | ui_visits | ip | Page view IP | — |
| impression_ip | ui_visits | impression_ip | Pixel-side IP (mobile/CGNAT fallback) | — |

Additional validations:
- vast_impression_ip ≈ vast_start_ip: 99.95% match (374/812,609 differ)
- win_logs.impression_ip_address: infrastructure/CDN IP (68.67.x.x MNTN infra, AWS IPs), NOT user IP
- **win_logs Beeswax→MNTN ID mapping (validated 2026-03-13):** `campaign_alt_id` = MNTN campaign_group_id; `line_item_alt_id` (STRING→INT64) = MNTN campaign_id; `creative_alt_id` = MNTN creative_id (unverified). Join: `CAST(w.line_item_alt_id AS INT64) = c.campaign_id`. Also join to event_log via `win_logs.auction_id = event_log.td_impression_id`.
- **CTV vs display identification:** At campaign level: `campaigns.channel_id = 8` = Television/CTV, `= 1` = Multi-Touch/display. At impression level: `win_logs.placement_type` (`VIDEO`/`BANNER`), `cost_impression_log.partner_ad_format` (`VIDEO`/`BANNER`). A single campaign_group can contain both CTV and display campaigns.
- **Impression trace paths — VV back to bid (confirmed by Zach 2026-03-13):** Key difference: for display, impression_log comes BEFORE win_logs (opposite of CTV). For viewable display, auction_id/ad_served_id let you skip impression_log and go straight to win_logs.
  - CTV: clickpass → event_log → win_logs → impression_log → bid_logs
  - Display viewable: clickpass → viewability_log → win_logs → bid_logs
  - Display non-viewable: clickpass → impression_log → win_logs → bid_logs
- **CTV chronological timestamp order (confirmed 2026-03-20, 225K VVs):** bid_time ≤ win_time ≤ impression_log_time ≤ event_log_time ≤ vv_time. Win notification precedes impression log entry in CTV pipeline. Display order: bid_time ≤ win_time ≤ impression_log_time ≤ vv_time (viewability_time ≤ vv_time).
- **0.0.0.0 placeholder IPs in event_log:** Some CTV VVs resolve to `0.0.0.0` via event_log. These are invalid/placeholder IPs — treat as equivalent to NULL for resolution purposes.
- **Display impression timing gap (TI-650, 2026-03-19):** Display impressions can be served 2-4 weeks before the user visits the site and triggers the VV. When tracing S3 VVs back to their impression via `ad_served_id`, a ±7d window around the VV time misses 35% of display impressions (Kindred Bravely: 768/2203 = 35% no_ip at ±7d, 0% at ±30d). CTV is same-day. **Use ±30d for the 5-source trace when display campaigns are in scope.** Verified on 3 advertisers: ±30d recovers 100% of impressions.
- **channels reference table:** `bronze.integrationprod.channels` — 10 rows. 1=Multi-Touch, 2=Email, 3=In-App, 4=Mobile Web, 5=Platform Fee, 6=Real Time Offers, 7=Social, 8=Television, 9=Ad Serving, 10=Onsite Offers.
- **v15 forensic trace (2026-03-12, 50 VVs):** IP is 100% identical across ALL 8 source tables (event_log, impression_log, CIL, bid_logs, win_logs, clickpass_log, ui_visits). serve_ip = bid_ip at 100%. Adding any source table to S1 pool has zero impact on resolution. The 8% unresolved S3 VVs entered via identity graph, not via MNTN impression.
- **bid_events_log is nearly empty** — only advertiser 32167 has data. Not useful for general IP lookups. Use bid_logs (Beeswax-native) instead.

**Cross-stage link (CORRECTED v20, 2026-03-16):**
- **S1 → S2 (impression-based):** `S2.bid_ip ≈ S1.vast_start_ip OR S1.vast_impression_ip`. S2 targeting = "had an S1 impression." Search event_log/viewability_log/impression_log for the S1 impression IP.
- **S1/S2 → S3 (VV-based):** `S3.bid_ip = prior_S1_or_S2.clickpass_log.ip`. S3 targeting = "had a prior S1 or S2 **verified visit**." Search `clickpass_log` for prior S1/S2 VV, NOT impression tables. Then: `prior_VV.ad_served_id → CIL.ip` to get the prior impression's bid_ip (may differ from VV ip due to cross-device!). For S2 VV → S1 chain: use the S2 impression's bid_ip to search S1 event_log.
- **Key insight:** In cross-device scenarios (CTV ad → phone visit), the VV clickpass IP ≠ the impression bid IP. The VV's clickpass IP is what enters the next stage's targeting segment. Prior analysis (v14-v18) searched impression tables and found zero — because the IP never had an S1/S2 impression, only an S2 VV. This was the ~8% "unresolved ceiling" — many are now traceable via the clickpass_log VV bridge.
- `first_touch_ad_served_id` links S3/S2→S1 directly (skips S2) but only 25-51% available.

### IP Mutation Key Findings (TI-650)
- **100% of mutation occurs at the VAST→redirect boundary** (Stage 3, CIL→EL or EL→redirect)
- **Aggregate mutation rate:** ~21.2% (14.28% inter-impression bid IP mutation for multi-impression VVs)
- **Cross-device is the primary driver:** 61.2% mutation when cross-device flag is set
- **Mutation range:** 1.2–33.4% across 15 advertisers in the reference dataset
- **Retargeting VV rate:** 59.8% of CTV VVs have a prior VV on the same bid_ip (= Stage 3 retargeting)
- **first_touch_ad_served_id bridge:** verified — 99.4% of ft UUIDs resolve to a real vast_impression
  in event_log. 30% of VVs with ft_id are multi-impression (ft ≠ lt).
- **Phantom NTB estimate:** ~4,006 events/day for advertiser 37775 (caused by IP mutation making
  verified NTB visits look like they came from households already in the ad graph)
- **Win → CIL join:** 100% reliable — always use `win_log.ad_served_id → CIL.ad_served_id`

### Cross-Stage VV Linking Research (TI-650, 2026-03-09)

The current audit table joins prior_vv_pool on IP (bid_ip or redirect_ip match), but IPs change
between stages, so ~60% of S2/S3 VVs cannot find their S1 origin via IP alone. Research into
alternative cross-stage identifiers:

**first_touch_ad_served_id** — the best available cross-stage link:
- Population rate: 44.2% globally (all advertisers, Feb 4-11 2026)
- For adv 37775: S1=91% populated, S2=32%, S3=34%
- When populated AND found in clickpass_log, 100% point to Stage 1 impressions (0% S2, 0% S3)
- However, ~40% of ft_populated S2/S3 rows cannot find their ft_asid in the 90-day CP lookback
  (these may reference impressions older than 90 days or from pre-BQ era)
- IP match rate (S2/S3 VV IP vs S1 ft VV IP): S2=64.5%, S3=71.9%
- GUID match rate (S2/S3 VV guid vs S1 ft VV guid): S2=25.2%, S3=42.9%
  (lower than IP because GUIDs are browser-specific — cross-device VVs get different GUIDs)

**guid (browser cookie)** — limited cross-stage utility:
- 607 GUIDs appear in all 3 stages (S1+S2+S3) within same campaign_group (1 week, adv 37775)
- 8,348 GUIDs appear in 2 stages (1,805 S1+S2; 2,382 S1+S3; 4,161 S2+S3)
- 185,265 GUIDs appear in only 1 stage (94% of all GUIDs)
- Problem: GUIDs are browser-level, CTV impressions use device GUIDs, visits use browser GUIDs.
  Cross-device attribution (CTV ad → mobile/desktop visit) always yields different GUIDs.

**ga_client_ids (Google Analytics Client ID)** — even more limited:
- 159 GA Client IDs span all 3 stages; 2,852 span 2 stages; 152,697 span only 1 stage
- GA Client ID is browser-specific AND requires GA to be installed — not available for all visits.
- Population rate: ~74-77% across stages for adv 37775.

**For S2/S3 VVs where first_touch IS NULL (the hard cases):**
- S2: 35,832 ft-NULL VVs — only 878 (2.5%) have a GUID match to any S1 VV in same campaign_group,
  571 (1.6%) have an IP match, 150 (0.4%) have a GA match. 34,702 (96.8%) have NO match at all.
- S3: 42,778 ft-NULL VVs — only 1,370 (3.2%) GUID match, 1,502 (3.5%) IP match, 245 (0.6%) GA
  match. 40,396 (94.4%) have NO match at all.
- **Conclusion:** For ft-NULL VVs, none of the available identifiers can reliably trace back to S1.
  These VVs are fundamentally unlinkable with current data — the S1 impression that triggered the
  funnel progression is either too old, or the VVS determined the VV without an S1 match
  (e.g., via GA Client ID cross-device expansion to an IP with no S1 clickpass record).

**event_log.td_impression_id = cost_impression_log.impression_id** — confirmed join:
- 100% populated in event_log (38.7M/38.7M on 1 day)
- Joins reliably to CIL.impression_id (ad_served_id matches at near-100%)
- Useful for enriching impression-level data but does NOT help cross-stage linking
  (it links within a single impression event, not across funnel stages)

**cost_impression_log.model_params** — no audience/segment identifiers:
- Contains: geo_version, device_type_group, flight_id, campaign_id, campaign_group_id,
  advertiser_id, pmp_deal_id, household_score, advertiser_household_score, realtime_conquest_score
- Does NOT contain: segment_id, audience_id, audience_upload_id, or any targeting segment reference
- Cannot determine which audience segment was targeted for a given impression

**Bottom line (CORRECTED 2026-03-16):** Cross-stage linking depends on which stage you're entering. S1→S2 uses **vast_ip** (impression-based). S1/S2→S3 uses **clickpass_log.ip** (VV-based). The prior analysis that found a ~92% IP ceiling for S3 was searching the wrong table — impression tables instead of clickpass_log. The VV's clickpass IP is what enters S3 targeting, and in cross-device cases it differs from the impression bid IP entirely.

**v20 resolution rates (10 advertisers, Feb 4–11, 90-day lookback, prospecting obj 1,5,6, campaign_group_id scoped, VV bridge):**
- S2: 97.95–99.87% (unchanged — S2→S1 impression-based link was already correct)
- S3: 74.54–99.47% (massive improvement from v14's 58.56–96.56%)
- VV bridge resolves most cross-device S3 VVs that were previously unresolvable
- adv 37775: 91.98% → **99.05%** (+7.07pp, unresolved dropped 1,761 → 75)
- adv 42097: 61.59% → **98.48%** (+36.89pp — was the worst, now nearly resolved)
- adv 34835: 81.45% → **99.34%** (+17.89pp)
- adv 31357: 58.56% → **74.54%** (+15.98pp, still lowest — heavy identity-graph population)

**The "92% ceiling" was wrong (corrected 2026-03-16).** Prior analysis (v14-v18) found ~92% S3 resolution via impression tables. This was an artifact of searching the wrong table. With the VV bridge (clickpass_log), the true ceiling is **~99%** for most advertisers. The remaining ~1% are IPs with no prior MNTN VV or impression in the same campaign group (true identity-graph-only entries).

**Retargeting pool impact (tested 2026-03-12):** Adding retargeting campaigns (obj=4) to the S1 pool resolves 110 additional S3 VVs for adv 37775 — IPs whose first MNTN impression was retargeting, not prospecting. Business decision: audit scope = "first prospecting touch" vs "first MNTN touch."

**Irreducible floor (updated 2026-03-16):** With VV bridge, only 75 S3 VVs unresolved for adv 37775 (0.33% of CIL cohort). These have no prior S1/S2 VV or impression IP match within the campaign group. Plus 1,074 VVs with no CIL record (pipeline gap, not TTL). Prior 567-unresolved GUID bridge analysis used the wrong cross-stage methodology — most of those 567 are now resolved via VV bridge.

**campaign_group_id scoping (Zach directive, 2026-03-12):** Cross-stage IP linking MUST be scoped within the same `campaign_group_id`. A VV in one campaign group cannot be linked to an impression in a different campaign group — that would be a coincidental IP match, not a real funnel trace. `campaign_group_id` is unique across advertisers. This constraint must be enforced in the production `vv_ip_lineage` model.

**Former "zero-chain" advertisers now resolved:** 4/10 advertisers (31276, 31357, 34835, 42097) had zero S3→S2→S1 chain in v14 because no S2 VAST event matched S3 bid_ip. v20 reveals they all had substantial VV-based chains — the old query was searching event_log instead of clickpass_log. Example: adv 42097 went from 0 chain resolutions to 11,399 via S2 VV chain.

**S1 pool lookback: 90 days is sufficient for 100% resolution (TI-650 v21c, 2026-03-17).** Initial analysis using MIN(impression_time) showed 186-day max gap, but this was biased — selecting the oldest of multiple matches. Using MOST RECENT prior S1 match: max 69 days, median 6 days, P95 29 days, P99 35 days. Verified: full v8 query (5-source IP trace + 4-table S1 pool + CIDR fix) with 90d lookback = 68,498/68,498 = **100% resolved**, matching the 180d result. **For production: use 90d lookback** — confirmed sufficient, halves scan cost vs 180d.

### Notable Advertisers
- **31357 = WGU (Western Governors University).** ~30% of MNTN monthly spend (entire business). Largest single advertiser. Has abnormally long S3 lookback window per Zach. Online degree program. Any analysis using this advertiser as test case should note it's an extreme outlier in spend and funnel depth. (Zach confirmed 2026-03-17)

### attribution_model_id Clarification (from TI-650)
- `ad_served_id` = **last-touch** attribution — the most recent impression that led to the VV
- `first_touch` = the first impression in the multi-impression sequence (NULL ~40% — permanent,
  confirmed by Zach: "no post processing" means first_touch NULL is not backfilled)
- `first_touch` NULL rate inversely correlates with age (54% at <1hr, 18% at 14-21 days) —
  this confirms batch processing, not a lookback limitation
- 91.76% of VVs have ad_served_id more recent than first_touch (as expected for last-touch)

### MES Pipeline Architecture (3-Stage Verified Visit Model)
Each stage can be up to 30 days apart. IP mutation BETWEEN stages is expected.
IP mutation WITHIN a stage (Stage 3's internal Bid→CIL→EL→Redirect→Visit chain) is the audit subject.

Stage 3 VV production audit table: `audit.vv_ip_lineage` (renamed from stage3_vv_ip_lineage)
- All stages (S1/S2/S3), partitioned by `trace_date`, clustered by `advertiser_id` + `vv_stage`
- **v12 architecture (target):** 2-link S1 resolution (`imp_direct`: S1 vast_start_ip = bid_ip; `imp_visit`: S1 vast_start_ip = impression_ip). Replaces v11's 10-tier cascade — all other tiers empirically proven redundant.
- Stage-based column naming: `s3_*`/`s2_*`/`s1_*` (each stage has 5 IPs + impression_time + guid)
- 90-day lookback (Zach confirmed max chain = 88 days: 14+30+14+30). Production default 120d for WGU outlier.
- **S3 resolution (v3, 10 advertisers, bid_ip only): 99.83%** (138,317/138,557 resolved, 44 unresolved)
- **S3 resolution (v2, 20 advertisers, 5-source): 99.83%** (225,491/225,872 resolved, 381 unresolved)
- Previous "~11% ceiling" was inflated by retargeting campaigns. Zach: retargeting not relevant. Filter with `objective_id IN (1, 5, 6)`.
- Remaining unresolved = lookback window insufficient, source table TTL truncation, or data bug. NOT identity graph entry — every VV MUST follow the IP path. 100% resolution achievable with sufficient lookback.
- Full schema reference: `tickets/ti_650_stage_3_vv_audit/artifacts/ti_650_column_reference.md`

---

## Greenplum (coreDW) Patterns

### coreDW Deprecation
**Deprecation date: April 30, 2026.** After this date, coreDW (Greenplum) will no longer receive updates.
BQ silver is the validated replacement — the full-scale run of Stage 3 VV audit matched GP within 0.12pp.

**Important:** BQ bronze.raw is a **non-random subset of Greenplum data (~25% of GP volume).**
Always use BQ silver (not bronze.raw) as the GP replacement.

### Greenplum-Specific SQL Syntax
Common GP-specific patterns that don't translate directly to BQ:
- `~` operator = regex match (BQ: use `REGEXP_CONTAINS`)
- `::` casting (BQ: use `CAST()`)
- `host(ip)` = strip /32 CIDR notation from inet type (BQ: use `NET.IP_FROM_STRING` or `SPLIT`)
- INET type: `ui_visits.ip` and other IP columns in GP are INET type — use `host(ip)` to get plain string
- Temp tables: `CREATE TEMP TABLE` is standard GP syntax; BQ uses CTEs or `CREATE TEMP TABLE` differently

### Key Greenplum Tables (not in BQ catalog)
These exist in Greenplum coreDW but may not have BQ equivalents:

| Table | Schema | Purpose |
|-------|--------|---------|
| `tpa.membership_updates_logs` | Greenplum | TPA membership update log |
| `summarydata.sum_by_campaign_group_by_day` | Greenplum | Daily pre-aggregated rollup by campaign group |
| `dso.valid_campaign_groups` | Greenplum | Active/valid campaign groups for DSO analysis |
| `fpa.advertiser_verticals` | Greenplum | Advertiser → vertical mapping (`type=1` = primary) |
| `r2.advertiser_settings` | Greenplum | Advertiser-level settings (`reporting_style='last_touch'`) |
| `audience.campaign_segment_history` | Greenplum | Campaign segment change history (see contamination warning above) |
| `summarydata.v_campaign_group_segment_history` | Greenplum | VIEW — segment history by campaign group |
| `audience.audience_segment_campaigns` | Greenplum | Maps active audience segment → campaign_group |
| `audience.data_sources` | Greenplum | Data source registry (DS IDs, names, types) |
| `geo.locations` | Greenplum | Geo location reference table (location_id, state/country names) |
| `public.campaign_groups` (aliased as `campaign_groups_raw`) | Greenplum | Campaign group dimension |
| `logdata.impression_log` | Greenplum | All bids (won or lost) — IP columns: ip, ip_raw, bid_ip, original_ip |

### IP Columns in Greenplum impression_log
`logdata.impression_log` has 4 distinct IP columns:
- `ip` — INET type, the primary IP (cast to text for joins: `il.ip::text`)
- `ip_raw` — raw IP before processing
- `bid_ip` — the IP used in the bid
- `original_ip` — pre-proxy original IP

---

## Email / Conversion Analysis Patterns

### Email Columns in conversion_log
`logdata.conversion_log` has two email-related columns:
- `email` — hashed email from pixel (SHA256)
- `email_data` — additional email metadata

**Empty email hash:** `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
(SHA256 of empty string) — appears when the pixel fires but no email value is present. Always
exclude this hash when counting qualifying emailled conversions.

**Email prevalence threshold:** ~0.5 is used in NTB/email analysis to determine which advertisers
have sufficient email data to include in the analysis.

### conversion_log.query Field — Two Email Extraction Patterns (Greenplum)

The `query` column in `logdata.conversion_log` stores pixel query string data in two different
formats. Classify first, then extract accordingly:

```sql
-- Step 1: Classify format
CASE
  WHEN query LIKE '{%' AND query NOT LIKE '%{%22%' THEN 'json'
  WHEN query LIKE '%=%' THEN 'querystring'
  ELSE 'other'
END AS query_format

-- Step 2: Extract email_data based on format
CASE
  WHEN query_format = 'json'
    AND (query::json->>'email_data') IS NOT NULL
    THEN query::json->>'email_data'
  WHEN query_format = 'querystring'
    AND query LIKE '%email_data=%'
    THEN split_part(split_part(query, 'email_data=', 2), '&', 1)
END AS email_data
```

Both `email` and `email_data` fields can be the source of email signals — always check both and
use `COALESCE(email, email_data)` for combined NTB analysis. Prevalence threshold: ≥0.5
(50%) to include an advertiser in NTB email analysis.

### ui_conversions vs conversions
`summarydata.ui_conversions` (Greenplum) uses `order_amt` for purchase amount.
**Do NOT use `order_amt_usd`** — this column is NULL in ui_conversions. Use `order_amt` directly.

### `ui_conversions.order_amt` reporting gap (TI-917, 2026-05-05)
`order_amt` is **only populated when the advertiser's conversion pixel carries a dollar value**. For top-50 advertisers (April 2026 cohort), **18 of 50 (36%) report $0** — they fire the conversion pixel but don't pass an order amount. Affected verticals are predictable: education (e.g. WGU, our top spender), services, lead-gen, financial. Implication: any revenue-based or iROAS analysis silently drops these advertisers. **Always check the converting-IPs count and `SUM(order_amt) > 0` before promising a revenue readout.**

Reference data: `tickets/ber_2250_incrementality_overhaul/ti_917_combined_loom/outputs/ti_917_revenue_sigma_per_advertiser.json` has the per-advertiser revenue stats (treated_ips, converting_ips, total_revenue, μ, σ, p50/p95/max).

### ui_visits.impression_time
`ui_visits.impression_time` = the timestamp of the original impression (NOT the visit time).
Use this when filtering visits by the period when ads were being served (e.g., "visits attributed
to impressions during Oct 17 – Dec 4, 2025").

---

## tmul_daily vs tpa_membership_update_log — Schema Differences

These two tables cover related data but have different structures and must NOT be used interchangeably.

| Aspect | tmul_daily | tpa_membership_update_log |
|--------|-----------|--------------------------|
| Type | Snapshot (daily state) | Change log (deltas) |
| Partition | `time` TIMESTAMP (hourly, 08:00 UTC) | `dt` STRING + `hh` STRING (zero-padded) |
| TTL | **14 days** | No stated expiration; data from 2025-11-21 |
| Data Sources | DS 2 and DS 3 ONLY | DS 2 and DS 3 (DS 4 NOT confirmed) |
| Size | ~32B rows, ~14.5TB | Unknown |
| `id` column | IP address | IP address |
| Unnest path | `UNNEST(td.in_segments.list) AS isl` → `isl.element.segment_id`, `isl.element.advertiser_id`, `isl.element.campaign_id` | `UNNEST(td.in_segments.segments) AS isl` → `isl.segment_id` (no `.element`) |
| Snapshot time | 08:00 UTC daily | Event-driven |

**Key gotcha:** DS 4 (CRM data) does NOT appear in tmul_daily at the row level. CRM membership
is resolved via the identity graph and stored in ipdsc__v1 instead.

---

## Additional Gotchas (TI-748 findings)

- **`funnel_level` is on `campaigns`, NOT `campaign_groups`**: `campaign_groups` has no `funnel_level` column. Note: `funnel_level=1` ≠ "prospecting only" (Stage 1 also contains retargeting campaigns); for the prospecting/retargeting split use `objective_id IN (1, 5, 6)`. See "Prospecting vs Retargeting (Audience Type)" above.
- **`agg__daily_sum_by_campaign` effective start: 2025-09-01**: Despite the GCP data floor of 2025-01-01, the aggregates table only has data from September 2025 onwards.
- **`agg__daily_sum_by_campaign` STILL STALE — lagging weeks behind** (re-confirmed 2026-06-04, TI-XXX advertiser-prefill calculator): MAX(day)=2026-04-30 when current date is 2026-06-04 (35 days behind). For trailing-30d / "current state" analysis, source from `cost_impression_log` + `clickpass_log` + `ui_conversions` directly (or the fact-table tip in `data_catalog.md`). Aggregate is still acceptable for trailing-12mo *patterns* where the pattern matters more than the most-recent month. Pipeline owner still unknown — flag if blocking work.
- **`uniques` in `agg__daily_sum_by_campaign` is unreliable for per-advertiser analysis**: The column exists but often contains zeros or values that don't aggregate meaningfully at the campaign level. Do not use VVR (vv/uniques) as a metric from this table.
- **Low-impression weeks produce extreme rate metrics**: When campaigns pause but VVs still attribute (lookback window), you get weeks with e.g. 7 impressions and 2,564 VVs (IVR=366). Always filter weeks with <1,000 impressions when computing rate metrics.
- **`r2_advertiser_settings` has no `deleted` column**: Unlike most integrationprod tables, this table has no deleted/is_test flags. All rows are valid.
- **`sum_by_campaign_by_day` starts 2024-01-01**: 15+ months of history. Use for experiments needing long pre-periods (52 weeks). `agg__daily_sum_by_campaign` only starts Sep 2025. Same columns except `uniques` is HLL BYTES (not usable as integer count).
- **`sum_by_campaign_by_day` does NOT have a `revenue` column** (discovered TI-956, 2026-06-08). Has `media_spend`, `data_spend`, `platform_spend`, `impressions`, `site_visitors` (HLL), `click_conversions`, `view_conversions`, `clicks` — but not revenue. Revenue is computed downstream, typically from advertiser-reported conversions × pricing. If your job needs revenue, source it from advertiser settings or pixel-side conversion logs, not from this campaign-day rollup.
- **`silver.audience.audience_segments` does NOT have an `advertiser_id` column** (discovered TI-956, 2026-06-08). It has `campaign_id`, `expression`, `expression_type_id`, `is_targeted`, `update_time` etc. To get the advertiser for an audience expression, JOIN to `bronze.integrationprod.campaigns USING (campaign_id)` and pull `c.advertiser_id`.
- **`media_plan_publishers.name` matches `sum_by_ctv_network_by_day.domain` exactly**: "CBS" = "CBS", "HBO Max" = "HBO Max". Cross-validated against `cost_impression_log` — same publishers, same rank order.
- **`sum_by_ctv_network_by_day` is validated against `cost_impression_log`**: Top-5 publishers match (same names, same ranking). Count differences are from campaign-level filtering. Valid proxy for publisher-level analysis.
- **CRITICAL — scope media plan queries to plan campaign groups only**: When analyzing media plan delivery, ONLY include impressions from campaigns in `media_plan.campaign_group_id`. Including ALL advertiser campaigns produces wildly incorrect results (appeared like plans weren't followed when they actually were ±3%). Lesson learned painfully in TI-748.
- **`media_plan_publishers.badge_state`**: `RECOMMENDED` (user accepted MNTN allocation), `USER_MODIFIED` (user changed), `USER_ADDED` (user added a publisher). Use to filter recommended-only plans.
- **Media Plan algorithm pipeline (Chris Addy + olympus repo, 2026-03-27)**: semantic search (FAISS + Gemini embeddings, top 40) → spend capacity filter (≥$0.50/hr, added Feb 3 2026) → multi-signal scoring → softmax(alpha=5.0) allocation → drop <0.5% → enforce min=10/max=15 → cap 12% per network → LLM rationale (Gemini 2.0 Flash). Score weights: performance 25%, quality 25%, semantic 20%, ML prediction 10%, spendability 8%, CPM efficiency 6%, scale 4%, accessibility 2%. Performance composite blends advertiser (50%), vertical (30%), network (20%) history. The spend capacity filter acts as a hard gate BEFORE scoring — low-inventory networks get filtered out before scoring happens.
- **CRITICAL config change Feb 3, 2026** (olympus commit `555234f`, PERML-412): `max_networks` changed from 25 → 15, `min_networks` from 15 → 10, AND spend capacity filtering added. Every plan before this date has 25-26 publishers; every plan after has exactly 16. This single change explains the TI-748 performance split.
- **ML model feature skew**: The ML prediction model (10% weight) has 52 input features but only 11 receive real data at inference — 41 are zeroed. Top-importance features (advertiser_mean_score, trend features) are completely missing. Fix planned but not yet implemented (olympus `specs/backlog/ml-primary-scoring-mode.md`).
- **Media Plan config parameters**: `alpha=5.0` (softmax temperature — higher = more concentrated), `max_networks=15`, `min_networks=10`, `max_allocation=12%`, `min_allocation=0.5%`. Plans with >15 publishers were likely generated under old config or overrides.
- **Per-publisher scores available in API response**: `score_semantic`, `score_performance_advertiser`, `score_performance_vertical`, `score_performance_network`, `spendability_score`, `score_cpm_efficiency`, `score_scale`, `score` (final combined). Chris Addy checking if persisted to BQ or just API response logs.
- **Media Plan algorithm source code**: `github.com/steelhouse/olympus` repo. Has docs populated for Claude Code exploration. Contact: Chris Addy (tech lead).
- **`deliverability_classification`**: Categorical delivery risk — "high" (expect full spend), "medium" (moderate underspend risk), "low" (high underspend risk). Computed by guardrail model: per-network daily spend thresholds, audience size, blocked networks, budget constraints. Final = worst individual guardrail. In-flight override: >3 days in + >90% pace → upgraded to "high". HHI (Herfindahl index) tracked in metrics but NOT a classification factor yet.
- **Flex Targeting = typically 10% budget reserve**: Not assigned to specific networks. Bidder uses for real-time optimization — un-recommended publisher impressions (e.g., Tubi Entertainment) come entirely from this flex pool. Explains ±3% deviation between recommended and actual allocations.
- **Without media plan, bidder does NOT optimize network allocation**: Impressions go to a huge inventory pool managed by the inventory team's deal commitments (e.g., "told HBO we'd spend $1M in Q1"). No performance-based network optimization. Manual adjustments only when customers complain about concentration. This explains pre-adoption spread of 131-183 publishers.
- **Media plan generated for BOTH Prospecting and Retargeting**: But only surfaced to customer for Prospecting campaigns. Retargeting plans run on backend — customer has no knowledge of them and cannot edit.
- **Beta selection bias**: Advertisers are hand-picked by PEX/CS (identified candidates with past interest), then validated by Toph (production ops) for pacing risk. NOT randomized. Important confound for causal analysis.
- **M1 beta released 2025-10-20**: TV-Only Prospecting only. No opt-in — required for all AIDs in beta. Changes only before campaign launch (mid-flight not supported in M1).
- **M2 (EOM January 2026)**: UI reskin — media plan auto-applied when user clicks the step. Happy path = don't edit. Secret bypass: skip media plan step entirely.
- **Dynamic media plan coming (TBD release)**: Recurring regeneration/rebalancing during campaign flight. Blocks planned experiment — can't test static version if dynamic is imminent.
- **`advertiser_configurations.conversion_lookback_window`**: NULL for most advertisers (use system default of 30 days). Only ~620 advertisers have non-null values (mostly 30, some 33/35/45/60 days).

---

## Keyword Targeting & BUK (Bottoms Up Keywords)

### Mountain Match V2 (Current Production)
- Flow: Scrape advertiser homepage (Common Crawl) → LLM describes products/services → LLM generates 20 parent keywords → LLM expands each to 10 search terms (200 total) → Embedding alignment maps to closest DS19 `data_source_category_id` → collapses to <200 unique DS19 keywords → audience expression
- Autopilot endpoint handles the LLM steps; Search Term endpoint handles DS19 alignment
- Parent keywords = user-facing labels in UI. Child keywords = DS19 IDs in the audience expression (not shown to customers)
- Once generated, keywords are static — no dynamic updates based on customer behavior or seasonality
- If homepage scrape fails, falls back to URL-based description (usually poor quality)

### BUK (Bottoms Up Keywords) — ALS Model
- Flow: `good_log` + `conversion_log` (30-day window) → build advertiser × DS19 keyword interaction matrix → Train implicit ALS model → Generate ranked DS19 recommendations per advertiser → k-means cluster by embedding into ~20 groups → LLM generates parent keyword labels/descriptions → audience expression
- **Confidence signal**: Weighted blend of distinct IP count, conversion count, cart volume, avg daily IPs, avg daily conversions (log1p transformed, configurable weights per signal)
- **Score adjustments**: (1) Popularity penalty — log odds ratio of keyword rarity, suppresses generic keywords like "accessories", "web services". (2) Advertiser lift — how popular keyword is for this advertiser vs. global average
- **Threshold**: Single percentile-based cutoff on model scores (~top 42%). Replaces fixed 200-keyword rule. Stronger-signal advertisers naturally get more keywords
- **Cold start**: Advertisers not in training data can't get ALS recommendations. Current fallback = vertical averages. Planned = MM V2 fallback
- **"Web services" pollution**: Google Tag Manager URLs get classified as web services by the LLM, associating this keyword with nearly every advertiser. Popularity penalty suppresses it

### Data Sources Overview
- **`bronze.integrationprod.data_sources`**: Master dimension table for all data sources
- **`bronze.integrationprod.categories`**: Category names/descriptions per data source (hierarchical)
- **`bronze.integrationprod.keyword_categories`**: Keyword strings for DS38 (MNTN UI Audience Keywords, ~40M rows)
- **`bronze.external.ipdsc__v1`**: IP-to-data-source-category mapping. Columns: `ip`, `data_source_id`, `data_source_category_ids` (ARRAY), `dt`. Filter by `dt` and `data_source_id`. **Gotchas discovered during TI-956 (2026-06-08):**
   - **`dt` is a hive-partitioned STRING column** (not DATE). Compare as STRING in WHERE clauses (`WHERE dt BETWEEN '2026-05-09' AND '2026-06-07'`). Casting to DATE in WHERE defeats partition pruning. In SELECT, wrap with `DATE(dt)` if you need DATE typing.
   - **`data_source_category_ids` schema differs by reader**: via BQ external table it surfaces as Parquet legacy LIST encoding `STRUCT<list: ARRAY<STRUCT<element: BIGINT>>>` (unwrap with `ARRAY(SELECT element FROM UNNEST(data_source_category_ids.list))` in BQ); via raw Spark Parquet read it surfaces as flat `ARRAY<BIGINT>` directly.
   - **Don't read 30+ days through the BQ external table for Spark jobs**: the spark-bigquery connector materializes the query result to a temp BQ table, and large window scans (~3B rows over 30d) exceed BQ shuffle limits ("Resources exceeded"). Read directly from GCS Parquet instead — `spark.read.parquet("gs://mntn-data-archive-prod/ipdsc")` with `.filter(F.col("dt").between(...))` gets hive partition pruning and bypasses BQ entirely. Path layout: `gs://mntn-data-archive-prod/ipdsc/dt=YYYY-MM-DD/data_source_id={DS}/*.parquet`.
   - **3P segment delivery into ipdsc is BURSTY/INTERMITTENT — never declare a segment "delivers nothing" from a single week (TI-1026 validation, 2026-06-11).** Under DS35, a given 3P category id (`c.element`) typically appears on only **2-4 distinct `dt` partitions per 30-day window**, with multi-million-IP bursts on the refresh days and total absence otherwise. A 7-day snapshot will show many segments as ZERO purely because their refresh fell outside the window — NOT because they don't deliver. Concrete proof: 6 Orange Theory 3P ids that read ZERO in 2026-06-04..06-10 (3 "Epsilon" 1000997189/1000999629/1000999639 + 3 "Commerce Signals" 1009501941/1011707151/1011707271) each delivered **1.3M-6.7M distinct IPs** in 2026-05-12..06-03 (last refresh dt = 06-03, one day before the snapshot opened). Conversely Stirista 1006088981 (the snapshot's #1, 2.1M IPs) was ABSENT from 05-25..05-31. **The id namespace is consistent** — the same DS35 `c.element` ids that "deliver" and those that read "zero" live in the same space; absence in a window = timing, not mis-mapping. For customer-facing reach claims use a ≥30-day window (or GCS-direct multi-day read) and report per-segment last-delivery `dt`, not a one-week count.

### DS13: "MNTN Vertical Categorization"
- Manually curated vertical buckets used to classify advertisers into industry categories
- 37 top-level verticals (Apparel, Electronics, Pets, Real Estate, etc.) with 148 leaf categories
- Lookup: `categories WHERE data_source_id = 13`
- The `data_source_category_id` values ARE the vertical identifiers

### DS19: "MNTN Matched" (Keyword Matching)
- The keyword matching data source — `data_source_category_id` values are linked to targetable keywords
- `visible = false` in `data_sources` table (internal only)
- ~20,000 keywords representing product categories (e.g., "Dog Beds" = 905072, "Pet Accessories" = 922262)
- Used in audience expressions for both MM V2 and BUK
- In `ipdsc__v1`: each IP has an array of DS19 `data_source_category_ids` it's been matched to
- NOTE: DS19 category IDs do NOT have rows in the `categories` table with `data_source_id = 19`. They share IDs with DS16 ("MNTN Taxonomy Data") in the `categories` table. The human-readable product category names (e.g., "Dog Beds" for ID 905072) come from the URL classification pipeline: advertiser URLs are classified via LLM/embedding into a product category name + DS19 `data_source_category_id`. These name mappings live in the BUK feature store (Databricks/Airflow pipeline output), not in a BQ dimension table
- DS38 ("MNTN UI Audience Keywords") in `keyword_categories` contains user-facing keyword strings (~40M rows) but uses different `data_source_category_id` values than DS19

### DS16: "MNTN Taxonomy Data"
- Per-advertiser taxonomy tree with hierarchy: ROOT → AdvertiserID → {PageViews, Conversions, Impressions, VV, Wins} → {Prospecting, MultiTouch, Retargeting} → CampaignGroupID → CampaignID
- 1.6M+ categories in `categories WHERE data_source_id = 16`
- Shares `data_source_category_id` values with DS19 (same IDs appear in both contexts)

### Shopper Graph API
- Internal API at `shopper-graph.in.mountain.com/autopilot?advertiser_id={id}` (Tailscale VPN required)
- Returns both MM V2 keywords and BUK keywords per advertiser in a single response
- BUK payload includes parent keyword groups with child keyword IDs and model version hash
- Currently: every DAG retrain overwrites ALL advertiser keywords (not idempotent — planned fix)

### Fangorn Tier 1 Production Launch (2026-04-30, DAGs completed early 2026-05-01)
**Per-advertiser switch:** `bronze.integrationprod.audience_advertiser_configurations.vertical_data_source = 46`. When set, the Audience Service swaps DS13 → DS46 in segment breakdown expressions at query time. Persisted base expression unchanged → UI audience sizes do NOT change.

**May 1 launch AIDs (3 advertisers, all flipped 08:00–08:01 UTC):**
| AID | Advertiser | Vertical | Reporting style |
|-----|------------|----------|-----------------|
| 32320 | Biz2Credit | 111004 — Lending & Brokerage | industry_standard |
| 38659 | Big Blue Bubble Inc. | 110001 — Games & Comics | industry_standard |
| 32233 | University of Northwestern Ohio | 107000 — Colleges & Universities | industry_standard |

**Authoritative inclusion query** (Mode dashboards, monitoring, etc.):
```sql
SELECT advertiser_id FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations` WHERE vertical_data_source = 46;
```

**Empirical count (2026-06-01):** 364 advertisers (2.56% of 14,196 total configs) have `vertical_data_source = 46`. The remaining 13,832 (97.44%) have NULL = default DS13 (Vertical) substrate. Most of the 14k configs are inactive/historical accounts; the 364 corresponds roughly to "Fangorn-on advertisers across all stages." Per prior memory snapshot, ~22% of S1 advertisers are on Fangorn → 364 is in the right ballpark for the active subset.

**Note (corrected 2026-05-05):** `tpa.fangorn_advertiser_inclusion` IS the source-of-truth inclusion list — but it lives in TPA-service Postgres, NOT in BQ. Schema: `(advertiser_id, fangorn_advertiser_inclusion_date)`. The `_date` column is the planned PT flip date. Joins to `audience.advertiser_configurations` (also Postgres). Updates happen when Matt/Ryan run a rollout — Ryan's `select ff.advertiser_id, cc.vertical_data_source FROM tpa.fangorn_advertiser_inclusion ff JOIN audience.advertiser_configurations cc ON ff.advertiser_id = cc.advertiser_id WHERE fangorn_advertiser_inclusion_date = 'YYYY-MM-DD'` is the canonical Postgres-side query. The BQ-observable effect is `audience_advertiser_configurations.vertical_data_source = 46`, which propagates after the nightly household-scoring run completes (midnight-1am PT). Fangorn-targeted bidding starts the next morning.

**Flip-time detection in BQ:** `TIMESTAMP_MILLIS(datastream_metadata.source_timestamp)` on `audience_advertiser_configurations` reliably gives the moment `vertical_data_source` was last set per advertiser. The `update_time` column is frequently NULL — don't use it. Verified 2026-05-05 against the 3 May-1 launch AIDs (all source_ts = 08:00-08:01 UTC matching the 08:00 UTC DAG completion) and 46538 authenTEAK (source_ts = 2026-05-05 22:26 UTC, the day Wave 2 vanguard flip happened).

**Rollback mechanism:** `UPDATE audience.advertiser_configurations SET vertical_data_source = NULL WHERE vertical_data_source = 46;` — query-time only, no audience re-ingestion needed.

**Score data path:** `gs://household-scoring-prod/output/scoring/fangorn_prospecting_scoring/year=YYYY/month=MM/day=DD/`. BQ surface tables: `bronze.household_scoring.advertiser_intent_daily` and `prospecting_intent_daily` (daily partitioned, populated by overnight DAG).

**DS46 in IPDSC:** `bronze.external.ipdsc__v1` carries DS46 alongside DS13. DS46 fully populated (3.1B rows back to 2026-01-29). DS46 row volume is ~16% of DS13 (consistent with a more selective Fangorn-scored layer).

**Tiering:** 369 Tier-1 advertisers approved (44% of fleet); staged rollout. Tier 2 (40%) and Tier 3 (16%) follow.

**Tier-1 rollout actuals (live tracking):**
- **Wave 1 (2026-05-01):** 3 AIDs — 32320 Biz2Credit, 38659 Big Blue Bubble, 32233 UNW Ohio.
- **Wave 2 vanguard (2026-05-05):** 1 AID — 46538 authenTEAK (Outdoor Furniture & Goods, full KPI suite).
- **Wave 2 main (2026-05-06):** ~50 AIDs — Matt + Ryan pushed via TPA inclusion API at 2026-05-05 ~3:42 PM PT; effective after tonight's household-scoring run. AID list available next morning via `vertical_data_source = 46` auto-detect or the discovery query at `tickets/ti_921_fangorn_lift_dashboard/queries/ti_921_discover_new_flips.sql`.

The Mode dashboard / TI-921 pipeline auto-detects new flips via `vertical_data_source = 46`. Out of scope for TI-457: Tier 2 / Tier 3 rollouts, continuous scoring full GA (TI-606/TI-816).

### Continuous Scoring (Fangorn + Keywords)
- Combines Fangorn intent score (s, 0-1) with BUK keyword evidence score (K, 0-1)
- Keyword score: `K = 1 - exp(-β * Σ 1/log2(rank+1))` where sum is over matched keywords
- Blending options: geometric `F = s^(1-γ) · K^γ` or linear `F = (1-γ)s + γK`. γ=0.25 (intent-dominant)
- Missing score fallback: `COALESCE(fangorn_score, keywords_score, 0)`
- Final score mapped to bidder scale: <0.6 = Max Reach (0-3333), 0.6-0.8 = Mid Intent (3333-6666), 0.8+ = High Intent (6666-10000)
- Rollout: (1) Fangorn release, (2) continuous scoring with Fangorn + MM V2 equal-rank keywords, (3) wire in BUK rankings
- DDP site visit signals already incorporated into BUK model training (confirmed Alex 2026-03-31)

### Keyword Value: Advertiser-Specific, Not Universal (TI-804, 2026-04-02)
- Per-advertiser keyword ranking: **184x visit rate differential** (top-5 vs bottom keywords)
- Global keyword ranking (across all advertisers): only **3x range**, correlation with BUK rank = 0.11
- Keyword quality is advertiser-specific: "Dog Beds" is rank-1 for K9 Ballistics, irrelevant for Rocket Lawyer
- BUK's ALS collaborative filtering captures this per-advertiser signal; MM V2's LLM homepage scrape cannot
- 93% of tested advertisers show >10x lift, all 15 verticals positive
- Validates continuous scoring for keywords (not just verticals) — the keyword signal is real and massive

### BUK Pipeline
- Runs as Airflow DAG in `airflow-ti` repo (SteelHouse/airflow-ti)
- Training and prediction on Databricks (job compute = 1/4 cost of interactive)
- Predictions output to GCS: `gs://targeting-infra-vertex-pipelines-prod/bottom-up-keywords/batch-predictions/dt={date}/` (parquet, ~18MB, includes advertiser_name, vertical_name, product_category, rank, score_adj)
- Feature store: recently migrated from Databricks-only to Airflow VS (Vertex/Spark)
- Local dev: Astronomer (`astro dev start/stop`), uv venv with python 3.11

### BUK Beta Customers (as of 2026-03-31)
- 40279 West Bend Insurance — live campaign
- 45594 Samy's Camera — live campaign
- 33129 Apollo.io, 37336 Global Rescue, 33610 Amsterdam Printing, 48687 Apolla, 35374 Experience Scottsdale — talked, not all live yet

## Feature Store & Bidstream Feature Analysis (TI-789/790)

### Key Finding: Pre-Visit vs Feedback Features
When building IP-level feature vectors to predict visits (IVR), features split into two categories:
- **Pre-visit features** (available at bid time): bidstream, impression, and win data. AUC ~0.896.
- **Feedback features** (available after site visit): guid_log pixel events, conversion_log purchase data. AUC ~0.999 but leaky — presence of guid_log data implies a visit already happened.

**Do NOT mix pre-visit and feedback features in a single targeting model** — guid_log/conversion_log features will dominate and mask the real predictive signal from bidstream features. Use feedback features for retraining, scoring returning visitors, and identity resolution.

### Top Pre-Visit Features for Targeting (by SHAP)
1. `al_avg_segments` (augmentor_log) — average MNTN segments on the IP
2. `ci_pct_new` (cost_impression_log) — % impressions where IP is "new"
3. `ci_pct_rtc` (cost_impression_log) — % RTC-targeted impressions
4. `ci_total_cost` (cost_impression_log) — total media spend on IP
5. `wl_avg_price` (win_logs) — average clearing price
6. `al_n_auctions` (augmentor_log) — auction volume for this IP
7. `wl_n_models` (win_logs) — device model diversity

### Bronze-Only Fields in augmentor_log
The silver view (`v_augmentor_log`) drops several fields present in bronze (`raw.augmentor_log`):
- `iab_categories` — IAB content taxonomy (30% fill). Key for vertical classification.
- `categories` — additional content categories (13% fill)
- `isp` — ISP name (10% fill)
- `page`, `referrer` — page URL and referrer (15%, 4%)
- `is_blocked`, `blocking_site` — brand safety (0% — no signal)

Must use `bronze.raw.augmentor_log` for iab_categories, NOT the silver view.

### content_genre in bidder_auction_events
- 87% fill, 37K+ distinct raw values
- Case-inconsistent: "Entertainment" vs "entertainment" vs "GENRE_COMEDY"
- Comma-delimited multi-genre: "sitcom,comedy"
- Normalize: `LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)])`, strip `genre_` prefix
- After normalization: ~50-100 clean genres
- Strong IP-level differentiation: residential IPs show 99%+ concentration in single genres

### guid_log product Field is JSON in Silver
In silver.logdata.guid_log, `product` is JSON type (not RECORD like bronze):
- Use `JSON_VALUE(product, '$.CATEGORY')`, not `product.CATEGORY`
- Fields: CATEGORY, BRAND, NAME, AMOUNT, SKU, INVENTORY_COUNT, CURRENCY, IMG_URL, REFERRER, AVAILABLE_UNTIL

### conversion_log query Field is JSON with key_value Array
Structure: `{"key_value": [{"KEY": "shoid", "value": "xxx"}, ...]}`
- Use `TO_JSON_STRING(query)` with `REGEXP_CONTAINS` for searching
- Key fields inside: `shoamt` (75%), `shpt` (74%), `ga_client_id` (67%), `email_data` (2.3%), `androidId/idfa/adid` (~3%)

### cost_impression_log Gotchas
- `recency_elapsed_time` is INTERVAL type — extract with `EXTRACT(SECOND FROM ...) + EXTRACT(MINUTE FROM ...)*60 + ...`
- `household_score = -1` means unscored
- `advertiser_household_score = 10000` means RTC conquest
- `partner_ad_format` is authoritative for VIDEO vs BANNER

### augmentor_log TTL and Archives
- BQ TTL: 10 days. Parquet archive: ~30 days at `gs://mntn-data-archive-prod/augmentor_log/region={east,west}/dt=YYYY-MM-DD/hh=HH`
- Ryan's pipeline (`aug_log_ip_vertical_id_hourly.py`) reads from parquet, runs hourly, maps domains to vertical IDs via tldextract
- Output: `gs://mntn-data-archive-prod/feature_store/feature_group_1_source/` partitioned by dt/hh
- Pipeline code: `steelhouse/airflow-ti` repo, `models/feature_store/feature_group_1_source/`

### BQ 6-hour wall + slot competition + double-aggregation = silent timeout (TI-933, 2026-05-06)

BigQuery interactive queries have a **hard 6-hour execution limit** (cannot be raised; max is 6 hours). Long-running ATT-style lift queries on `augmentor_log` over multi-day windows can hit this wall and die with zero output, returning `Operation timed out after 6.0 hours. Consider reducing the amount of work performed by your operation so that it can complete within this limit.` Three lessons learned at once on TI-933:

1. **Don't run two heavy queries in parallel on the same reservation.** Both 7d and 14d Select lift queries hit the 6h wall at exactly the same point (94/128 stages, 73% complete, 692 slot-hours each). They shared `dw-main-bronze:us-central1.adhoc` slots and effectively halved each other's throughput. Run sequentially. The original TI-917 v5 query (4 segments × 7d) finished in 6h running solo; mine (1 segment × 7d) timed out running parallel with another query.

2. **Don't double-aggregate when one can be derived from the other in Python.** A query that produces both per-(advertiser, arm) AND pooled-(arm) rows via two CTEs runs the same heavy 4-way LEFT JOIN twice — once at per-advertiser grouping, once at pooled grouping. Each pass shuffles billions of rows. Drop the pooled CTE and reconstruct in Python: `pooled_n_ips = SUM(per_adv_n_ips)`, `pooled_visitors = SUM(per_adv_visitors)`, `pooled_rate = SUM(visitors)/SUM(ips)`. Mathematically identical because (advertiser_id, ip) pairs are unique across advertisers. This is what killed TI-933 v2 — `S15: Output` was reading 9.6B records doing the second-pass pooled join.

3. **Always pull `bq show -j --format=prettyjson` on a failed/timed-out job to see WHERE it died.** Stages-completed (94/128), last running stage (`S15: Output`), and records-read (9.6B) on that stage tells you whether the bottleneck was the augmentor scan (early stages) or the join/aggregate output (late stages). On TI-933 it was the latter, which pointed directly at the double-aggregation issue rather than augmentor cost.

4. **For heavy lift / multi-day augmentor queries, push to Databricks instead of BQ.** No 6-hour wall, GCS-native parquet reads (augmentor archive at `gs://mntn-data-archive-prod/augmentor_log/` retains ~30 days vs BQ's 10), shuffle-heavy joins benefit from memory-optimized clusters. Use BQ Spark connector for tables not yet in GCS. See airflow-ti repo for Spark job patterns.

### BQ dry-run is unreliable on federated tables (confirmed 2026-04-27)
A query touching `household_scoring__prospecting_intent__v1` (Parquet external table over `gs://household-scoring-prod/...`) dry-runs at 610 GB but actually processes **18.1 TB** when run — ~30× under-estimate. The estimator cannot see into the federated source's actual scan footprint. Rule: for any query that joins a federated/external table, treat the dry-run as a lower bound only. Sample on a smaller window (1 day) before committing to a larger run.

### `bq query` crashes on SQL strings starting with `--` (workaround: pipe via stdin) (2026-04-27)
The `bq` CLI uses Google's `absl` flag parser. When the SQL is passed as a positional argument that begins with `--` (e.g., a SQL block-leading comment line `-- TI-XXX: ...`), absl interprets the entire SQL string as an unknown flag and tries to compute Levenshtein distance suggestions against known flags. The recursive distance function blows Python's recursion limit and crashes with `RecursionError: maximum recursion depth exceeded` from `_damerau_levenshtein`. No query is dispatched — the bq process never reaches BigQuery.

**Workaround:** pipe SQL to `bq query` via stdin instead of passing as a positional argument. Works through the `bq_run.sh` wrapper too:
```bash
bash .claude/scripts/bq_run.sh --ticket "TI-XXX" --label "..." \
  --use_legacy_sql=false --format=prettyjson --max_rows=500 --project_id=dw-main-silver \
  < path/to/query.sql > output.json
```

Alternative: strip the leading `--` comment from the SQL before passing positionally (workable but brittle). Stdin is the durable fix.

### `clickpass_log` cannot do apples-to-apples holdout comparisons (2026-04-27)
By definition, a `clickpass_log` row requires (a) a MNTN impression AND (b) a subsequent visit matched to that impression. Holdout IPs (per-advertiser hash bucket 0-99) by construction don't get served impressions for that advertiser → essentially cannot generate clickpass rows for that advertiser. So any clickpass-based "lift" calculation between treatment and holdout will show enormous lift simply because the table's existence requires the treatment.

The non-zero clickpass entries observed for holdouts are spillover: the holdout hash is per-(AID, IP), so an IP that's a Zazzle holdout might be served Ferguson Home ads, get a Ferguson clickpass row, and visit Zazzle in the same window — but the visit gets attributed to Ferguson, not Zazzle. These are tiny.

**Implication for incrementality measurement:** use `guid_log` as the honest test of total-traffic causation. Use `clickpass_log` only to quantify *attribution capture* — the wedge between the two = visits MNTN takes credit for that would have happened anyway. For Zazzle high-intent ATT (TI-837, 1-day, 2026-04-27): clickpass lift +1.49pp / guid lift +1.30pp → ~78% of MNTN-attributed visits would have happened anyway via other channels.

### Canonical prospecting_intent table (used in TI-837)
`dw-main-bronze.external.household_scoring__prospecting_intent__v1` — federated Parquet table over `gs://household-scoring-prod/output/scoring/prospecting_intent/year=YYYY/month=MM/day=DD/`. 10-day rolling retention in BQ; deeper history (35-day) accessible via raw GCS. Schema: `ip, advertiser_id, campaign_group_id, campaign_id, household_score, year, month, day`.

**Gotcha (TI-933, 2026-05-06): partition filter pushdown breaks across month boundaries with `OR`.** When the analysis window spans two months (e.g., 2026-04-29 → 2026-05-05), do NOT write `WHERE (year='2026' AND month='04' AND day IN (...)) OR (year='2026' AND month='05' AND day IN (...))`. The federated Parquet planner scans all partitions and tries to read expired files (e.g., `month=03/day=31`), failing with `Not found: Files gs://household-scoring-prod/.../part-*.parquet`. **Fix: split into per-month CTEs and `UNION ALL`** — the planner gets two clean partition-pruned scans:
```sql
prospecting_apr AS (SELECT advertiser_id, ip FROM ...v1 WHERE year='2026' AND month='04' AND day IN (...) AND ...),
prospecting_may AS (SELECT advertiser_id, ip FROM ...v1 WHERE year='2026' AND month='05' AND day IN (...) AND ...),
prospecting AS (SELECT DISTINCT advertiser_id, ip FROM (SELECT * FROM prospecting_apr UNION ALL SELECT * FROM prospecting_may))
```

**Intent tier from household_score** (per Alex Knorr's TI-835 thresholds):
- 10000 = `high` (vertical + keyword match)
- 7000-9999 = `peak` (vertical only)
- 3333-6999 = `mid` (keyword only)
- <3333 = `max_reach`

**Coverage gap:** keyword-only advertisers (e.g., WGU 31357) are NOT in this table — they're scored via a different pipeline (Mountain Match V2 / DS19 keyword match without DS13 vertical scoring). For TI-835's 9-advertiser sample: 7 are present (Ferguson Home, Ancient Nutrition, First Watch, HexClad, Clayton Homes, Zazzle, Northern Tool); 2 are absent (Angi 32766, REVOLVE 53308) — same exclusion pattern as WGU.

### augmentor_log `mntn_segments` for holdout IPs (confirmed 2026-04-22)
When a holdout IP appears in `augmentor_log`, the `mntn_segments` array **does NOT contain the segment the IP is a holdout of**. The filter logic strips the segment at audience evaluation time — holdouts are not qualified to bid on, so the matching segment is never attached to the row.

This means you **cannot** use `mntn_segments` as a proxy for "would we have bid on this IP if not for the holdout." The implication for ghost-bidding / lift measurement:
- Pick the target audience externally — reconstruct it from `prospecting_scores` / DS13 + DS19 overlap or from `audience_segments.expression`
- Intersect that targetable IP universe with the holdout hash (`MD5('{AID}:{IP}')` mod 1000 ∈ 0-99)
- Look those IPs up in `augmentor_log` inside the campaign window — appearances prove biddability (IP was seen by the augmentor, so it was eligible for *some* bid request) even without the segment match on the row itself
- Matt Brorby verified on a few test IPs; Malachi and Alex Knorr corroborated on Zoom 2026-04-22

Confusingly, the 2026-04-20 verification that "10% of unique IPs in augmentor_log hash into buckets 0-99" is still correct — holdout IPs are present in the log, just not attached to the segment they're a holdout of. The two facts are consistent.

**Also:** advertiser_id 90 is a MNTN PSA advertiser. PSA impressions are served to holdout IPs intentionally (they show up in `cost_impression_log` for buckets 0-99). Exclude AID 90 from any holdout-based lift analysis — Alex Knorr hit this as a confusing anomaly before the PSA fact was surfaced.

### Parquet vs BQ Schema Differences (confirmed TI-810)
- **augmentor_log parquet LIST fields** (`pmp`, `iab_categories`, `mntn_segments`): Parquet legacy LIST format = `struct<list: array<struct<element: T>>>`. `F.size(F.col("pmp.list"))` fails in Spark — interpreted as map subscript. Use `F.col("pmp").isNotNull()` instead.
- **guid_log `product` column**: STRUCT in parquet (`{amount, brand, category, currency, ...}`), but appears as flat columns in BQ silver view. Cannot compare to string `"null"` — use `.isNotNull()`.
- **General rule**: Always inspect raw parquet schema before writing Spark aggregations — BQ silver views enrich/flatten types differently from raw parquet.

### private_marketplace_deals Table
- Reference table for PMP deal names and IDs — not built by us, comes from Beeswax/exchanges
- DS42 (select team) converts PMP string IDs to integer `data_source_category_id` values
- Has `name`, `floor_price`, `channel_id` columns
- Example: `SELECT * FROM private_marketplace_deals WHERE lower(name) LIKE '%nba%'` for sports deals

### IPv6 in augmentor_log
- When IP field is blank, IPv6 field is often populated
- IPv6 can link to household ID via identity graph
- Consider as fallback for blank-IP rows when building IP-level features

### IAB Categories — Practical Limitations (from Alex's TI-791 analysis)
- Top categories are too generic: "Arts & Entertainment", "Television" dominate across all device types
- Only ~20% of CTV rows have any IAB categories
- IAB alone is insufficient for vertical classification — need domain parsing too
- Simple rules mapping (not embeddings) is more practical given the generic taxonomy
- CTV vs non-CTV is the primary delimiter — classification logic should split on device_type first

### Scale Reference (per day, 2026-03-29)
| Table | Distinct IPs | Rows |
|-------|-------------|------|
| guid_log | 31.2M | ~340M |
| win_logs | 11.7M | ~68M |
| cost_impression_log | 11.7M | ~68M |
| conversion_log | 10.7M | ~63M |
| clickpass_log (visits) | 563K | ~1M |
| augmentor_log | 23.6M (1hr) | 1.2B/hr |
| bidder_auction_events | — | 112M/hr |

Daily IVR base rate: ~4.8% (563K visitors / 11.7M impressed IPs).

<!-- slack-extracted: 2026-04-08-full -->
- ### BigQuery Standardized Timezone Conversion Functions

Standardized functions are available in BigQuery (bronze, silver, and gold datasets) to convert UTC timestamps to advertiser-local time, consistent with CoreDW behavior:

- `public.timetz(time, time_zone)` → returns `DATETIME`
- `public.hourtz(time, time_zone)` → returns `DATETIME`
- `public.datetz(time, time_zone)` → returns `DATE`

**Why these exist:** Created late in the CoreDW→BigQuery migration after inconsistencies were found with native approaches like `TIMESTAMP_TRUNC(..., time_zone)` and `DATETIME_TRUNC(..., time_zone)`.

**Reporting pipeline convention:**
- `timestamp` columns → UTC
- `datetime` columns → Advertiser local time

Use these functions instead of native BigQuery truncation functions when timezone conversion is needed.
- ### CoreDB DDL Change Management — Migration to Alembic (In Progress)

As of April 2026, CoreDB DDL changes are executed manually without a migration framework. This makes it difficult to link schema changes to tickets, enforce parity across prod/QA/dev environments, or reason about triggers.

An effort is actively underway (led by the platform team, target mid-May 2026) to adopt a migration system similar to Alembic that would:
- Link DPLAT tickets to pull requests
- Enforce schema parity between prod, QA, and dev
- Enable local testing

Until then, DDL changes to CoreDB are coordinated via the #data-platform channel.
- ### Data Engineering MCP Server

An internal Model Context Protocol (MCP) server is available for data engineering tooling at `https://data-eng-ai.in.mountain.com/`. It is deployed via GitHub Actions to Argo.

**Current tools include:**
- GCS folder size lookups
- Row count queries against Parquet files
- Parquet schema inspection
- TI on-call utilities
- Dataproc batch analysis with code change recommendations (partially functional)

**How to contribute:** Merge tool additions to `main`, then run the GitHub Action to generate and deploy an Argo PR.

**Slack bot:** A Slack bot interface is also available for interacting with the MCP (e.g., Parquet schema queries). Contact the owner (Ryan Kleck) to be added to the bot's channel.

<!-- slack-extracted: 2026-04-08-review -->
- ### BigQuery Write Patterns — Bulk Loading vs. Direct INSERT

BigQuery is optimized for bulk loading rather than row-by-row `INSERT` statements. Direct `INSERT` queries have a daily quota and are discouraged by Google's quota structure.

**Preferred ingestion patterns:**
- Write Parquet files to GCS, then load to BigQuery (standard pattern for services ingesting from external sources)
- Use the BigQuery Python client's load job method (e.g., `load_table_from_dataframe`), which uses load jobs rather than streaming inserts
- Spark jobs writing directly to BigQuery are also an established pattern

**Load job limits:**
- 1,500 load jobs per table per day
- 100,000 load jobs per project per day
- Max job size: 15 TB per load job
- Jobs fail if runtime exceeds 6 hours

**Anti-pattern:** Do not insert data into an OLTP database (e.g., CoreDB) and then dump it to BigQuery. Write directly to GCS/BigQuery from the service.
- ### Fangorn Continuous Scoring — IVR Evaluation Methodology

IVR rates for Fangorn are not evaluated at the individual keyword level (there is no 1:1 keyword-to-visit association). Instead, evaluation is done at the **IP level using a unified score**:

1. Each IP is assigned a unified score based on its keyword rankings from BUK.
2. IPs are binned by unified score (e.g., IPs scoring ≥ 0.9).
3. Visits and impressions are summed across all IPs in a score bin to calculate IVR for that bin.

This approach means a visit is not attributed to a single keyword — it's attributed to the IP's overall intent score. The result shows how IPs with high unified intent scores perform relative to lower-scored IPs.

**Implication:** Adding 100% of keywords to High Intent is suboptimal because individual keywords have vastly different IVR rates and don't all perform at the average HI level. Continuous scoring addresses this by ordering keywords more precisely by intent signal strength.
- ### Audience Size Expansion — Keyword Recommendation Complexity

Recommending a specific keyword addition to achieve a target IP count increase (e.g., "add 50,000 IPs") is not straightforward due to IP intersection:

- The marginal IP gain from adding a keyword cannot be estimated without a full evaluation against MemDB, because IP sets across keywords overlap.
- Evaluating how much a category grows an audience in real time is considered a hard blocker for any productized recommendation.
- DAR (Dynamic Audience Recommendations) produces a ranked list of keywords; marginal IP gain could theoretically be computed sequentially from rank 1 to rank N, but this functionality does not exist today.
- IPDSC could be used as an approximation, but would be slow.

**Current workaround:** The existing MemDB hash mechanism (used for holdout bucketing) could potentially be reused for approximate audience sizing, but no automated recommendation tooling exists.
- ### `core.advertiser_default_values` — Campaign Budget Floor/Ceiling Overrides

The `core.advertiser_default_values` table allows per-advertiser overrides of campaign model settings, including budget floor and ceiling by stage (e.g., stage 15, 23, 25, 42, 43) and campaign status.

**Key behavior:**
- Values are stored as a JSONB column `campaign_values` with keys like `budget_floor`, `budget_ceiling`, and `campaign_status_id`
- Stage 23 and 43 with `campaign_status_id: 7` disable those stages (e.g., for advertisers not tracking Verified Visits)
- Settings flow downstream and affect active campaigns when updated

**Known issue (April 2026):** Many advertisers created before the budget minimum feature was introduced are missing records in this table. When an advertiser has CART disabled, the default values record may not have been created, causing budget floor/ceiling to not apply correctly. 

**Workaround:** Toggle CART ON then OFF in the Advertiser Info page in Command Center — this creates the missing record, fixes existing budget allocations on live campaigns, and syncs everything downstream. A migration to back-populate all affected advertisers is tracked in PRO-497.

<!-- slack-extracted: 2026-04-14 -->
- **Offline Upload Values — Future-Dated Conversions Issue**

The table `ui.offline_upload_values` can contain conversions with future timestamps if the customer uploads data that includes future-dated records. This was confirmed via `upload_id = 27358`, which had a `max(time)` of `2026-04-17` at a time when that date had not yet occurred.

**Root cause:** The upstream discrepancy originates from the third-party data provider (xdd). Data can fail to match hashed values at the time of the initial run but recover later.

**Resolution pattern:** Re-triggering the pipeline after xdd data recovers will produce correct match results (e.g., 1,458 conversions surfaced on rerun).

**Action item for data quality:** Ensure offline uploads do not contain conversions with future dates before processing. No other downstream action is required once data recovers. (via Lilit, #reporting_helpdesk_ask_anything, 2026-04-13)

<!-- slack-extracted: 2026-04-16 -->
- **Fangorn Experiment — Mid-Intent + Peak Performance Logic Bug:** A logic error was identified in the Mid-Intent + Peak Performance audience expression during the Fangorn experiment. The bug affected the Treatment side only.

**Root cause:** On the Control side, the audience expression uses a 6-digit `vertical_id` for High Intent and a 3-digit `vertical_id` for Mid Intent. On the Treatment side, the `advertiser_id` was mistakenly used for both High and Mid Intent instead.

When Peak Performance was enabled and the threshold dropped to 3333:
- **Treatment (correct behavior):** ORs in the 6-digit `vertical_id` → targets the vertical OR (bucket AND keywords)
- **Control (buggy behavior):** ORed in the `advertiser_id` → accidentally targeted Bucket OR Keywords, which simplified to just the bucket at the 3333 threshold

**Fix for rollout:** Mimic DS13 exactly — use a 6-digit `vertical_id` for High Intent and a 3-digit `vertical_id` for Mid Intent on the Treatment side. The `advertiser_id` will no longer be used for intent-tier differentiation in audience expressions. (via Ryan Kleck, #dev_fangorn-model_ex, 2026-04-01)
- **Datastream Replication — Primary Key Policy:** When adding tables to Datastream replication, BigQuery does not use serialized/synthetic primary key columns. PKs should be data columns that are meaningfully unique (e.g., `location_id`), not auto-generated serial columns. Adding a serial column purely for replication purposes is discouraged because it is not representative of the data. (via Dustin Niehoff, #data-platform, 2026-04-01)

<!-- slack-extracted: 2026-04-17 -->
- **cost_impression_log — Customer-Centric Spend View and PSA Exclusion**

`cost_impression_log` is designed to depict spend and costs from a customer-centric perspective. PSAs (Public Service Announcements — impressions served when no paid ad is available) are excluded from `cost_impression_log` because they are not charged to the customer. This means `cost_impression_log` spend figures will not match raw cache or Beeswax totals when PSAs are present for a given campaign. The discrepancy is expected behavior, not a data quality issue.

Practical implication: If a campaign shows higher spend in Beeswax/cache than in `cost_impression_log`, check whether PSAs were served for that campaign ID during the period in question. Query `impression_log` filtering on `original_cid` to confirm PSA volume. (via ray, #reporting_helpdesk_ask_anything, 2026-04-16)
- **PSA Root Cause Pattern — Ad-Service Cache and NULL update_time**

A PSA spike incident (April 15–16, 2026) revealed a structural risk in the ad-service cache refresh logic:

- The cache performs full refreshes filtering on `update_time`, but a significant number of historical VAST metadata rows have **NULL `update_time`** because that column was not always tracked.
- Those NULL-`update_time` rows are never picked up in refresh cycles, so they are effectively frozen in cache.
- After ticket CDS-3414 (~April 1, 2026) changed from full updates to batch updates, any creative IDs (CRIDs) composed entirely of NULL-`update_time` rows stopped being refreshed.
- Redis has a 14-day TTL, so those rows naturally expired around April 15, resulting in missing Redis keys → full PSAs for affected campaigns.
- **Impact was limited to older creatives** (pre-dating `update_time` tracking). Newer Select creatives with populated `update_time` were unaffected.
- Remediation: fix deployed ~12:53 ET April 16. Follow-up work planned to ensure `update_time` is properly maintained going forward. (via bermudez, #mission-control, 2026-04-16)

<!-- slack-extracted: 2026-04-21 -->
- **DS46 — ML Audience Intent Scoring Model (Fangorn):** DS46 is the data source ID for MNTN's ML-based audience intent scoring model, associated with the Fangorn scoring system. **[SUPERSEDED — Fangorn/DS46 subsequently LAUNCHED: Tier 1 ~2026-04-30, rolling per-advertiser migration; DS46 appears in advertiser expressions and delivery shows continuous scores. HexClad flipped Jun 4–5 2026. See the Fangorn detector section. The "turned off" note below reflects the 2026-04-20 state only.]** As of 2026-04-20, DS46 was deliberately turned off and is NOT in production. The team ran an experiment using it, then stopped populating IPDSC (IP-to-DSC mapping) with DS46 scores in order to allow MemDB (MembershipDB) to clear its cache, which has a 30-day TTL. Once MemDB has cleared the old data, DS46 population will resume and the model will roll out. Declining row counts in DS46 monitoring during this period are expected and not indicative of a bug. Two BigQuery tables can be used to monitor DS46 volume: `dw-main-bronze.external_ddm.data_source_sizes` (high-level, filter `data_source_id = 46`) and `dw-main-bronze.external_ddm.data_source_category_sizes` (exploded by DSCID category). (via Ryan Kleck, #mission-control, 2026-04-20)
- **Fangorn Continuous Scoring — Two Flavors:** There are two variants of continuous scoring involving Fangorn: (1) Fangorn-only continuous scoring — scores go directly to the bidder team; no MembershipDB dependency. (2) Fangorn + Keyword continuous scoring — requires Fangorn scores to be in MembershipDB. The reason scores must be in MemDB for the second variant is to support proper audience size display in the UI. Sequencing: Fangorn-only continuous scoring ships first (no MemDB dependency), then Fangorn/Keyword continuous scoring (requires MemDB). (via Ryan Kleck, #tgt-infrastructure-squad, 2026-04-20)
- **mntn-analytics-raw GCS Bucket — TTL and Cost Risk:** The GCS bucket `mntn-analytics-raw` stores all Kafka data dumps and has a 1-year TTL. As of April 2026, at its current growth rate it was projected to cost $310K/month by September 2026. The data platform team was evaluating reducing the TTL to 90 days. Primary consumers are various applications and the data warehouse. This is a cost-critical decision requiring input from all teams that consume Kafka data from this bucket. (via scotty, #chapter-data-engineering, 2026-04-20)

<!-- slack-extracted: 2026-04-22 -->
- **tpa.categories — Data Source (DS) 11 vs. DS 35**

In the `tpa.categories` table, Data Source 11 is deprecated. Data Source 35 is the current live source. When the same audience category ID appears under both DS 11 and DS 35 (e.g., for Liveramp audiences), DS 35 should be used. (via zach.schoenberger, #targeting-squad, 2026-04-21)
- **Pacing Incident (2026-04-21) — Root Cause: Daily Budget Doubling via flightspendraw / flightimpressionsserved**

A systemic pacing failure affecting 1000+ CGIDs (>90% underspend on 4/20, followed by overspend on 4/21) was traced to daily budgets doubling day-over-day. The root cause involved the `flightspendraw` and `flightimpressionsserved` fields. Contributing factors:
- A BID deployment on 2026-04-21 00:30 UTC (splitting geo version and intent score reads) was initially suspected but ruled unlikely to be the root cause and was reverted as a precaution.
- The long-term fix requires a DevOps change to a DB table TTL (tracked in DEV-7296).
- Short-term mitigations: PAC manually adjusted daily budget caps and synced to Beeswax; HHST was manually reset by Forrest.

**Impact:** No underspend on 4/21, but significant overspend. Full overspend impact list shared with PEX the following day. (via Johnny, #mission-control, 2026-04-21)

<!-- slack-extracted: 2026-04-24 -->
- ### analytics_request_log — GA3 Volume Drop Root Cause Pattern

A drop in `analytics_request_log` total volume that is isolated to GA3 (while GA4 volume remains normal) indicates a reduction in cross-device requests, not a data pipeline failure. Diagnostic indicators:

1. **GA4 volume stays normal** — GA4 is `cross_device = false`, so it is unaffected by cross-device resolution issues.
2. **Distinct `ad_served_id` volume stays consistent** — all relevant matched impressions are still represented; the drop is in cross-device enrichment only.
3. **Distinct `ga_client_id` volume drops in tandem with GA3 volume** — the ratio of `ga_client_id`s per IP drives GA3 cross-device request volume.
4. **The `analytics_request_log` to `clickpass_log` monitor will not alert** in this scenario, because matched impressions are still present.

Root cause observed 2026-04-22 (hr19 UTC): truncation of `partner_sync_by_advertiser_v3` in ScyllaDB caused the drop. Restoring that table restored GA3 volume. (via Nish, #mission-control, 2026-04-23)
- ### Experimental Campaigns — Sync Disabled (Monitoring Implication)

Experimental (EX) campaigns have syncing to BX (the bidder exchange layer) intentionally disabled as part of the experiment design. This has a known monitoring side effect: any audit or monitor that relies on synced data (e.g., publisher/network blocklist sync, app bundle sync) will fire incorrectly for these campaigns even when the campaign configuration itself is correct.

**Recommended handling:** Exclude experimental CGIDs from sync-dependent monitors on a per-campaign basis, or implement logic to suppress alerts for campaigns flagged as experimental. Creating a dedicated App Bundle Sync Monitor that accounts for this distinction has been identified as a future improvement (DM-4375).

**Example (Audit #9, 2026-04-23):** An advertiser updated their AID block list after the experiment campaign was already running. The update was never propagated to BX because syncing was disabled, causing Audit #9 (DSP Control - Publisher & Network Blocking) to fire. The audit fire was technically correct but operationally expected given the sync-disabled state. (via Tom Manuel, #mission-control, 2026-04-23)

<!-- slack-extracted: 2026-04-26 -->
- ## RabbitMQ Consumer Outage Pattern — Segment/Campaign Mis-Targeting Risk

When RabbitMQ consumers stop processing messages (due to acknowledgment timeout errors or other failures), the following impacts occur:

- **Segment or campaign mis-targeting** — audience updates handled through the batch-job path (RabbitMQ) are delayed or dropped.
- **Intent scores not updated** — intent scoring pipeline depends on RabbitMQ consumers; a multi-day outage means stale intent scores for the affected period.
- **Spend impact** — similar to the 4/1 incident, campaign groups with recently updated audiences may see no spend or significant underspend during the consumer downtime window.
- **Kafka path is independent** — Kafka consumers can remain functional even when RabbitMQ consumers fail. This means audience updates flowing through Kafka (e.g., standard audience refresh) may still work while batch-job-path updates (RabbitMQ) do not.

**Detection gap:** Current alerting does NOT surface when a large backlog of messages is sitting in the RabbitMQ queue waiting to be consumed. The `message-delivered` metric dropping and the `messages ready` backlog count rising are the two Grafana signals to watch.

**Grafana dashboard for RabbitMQ:** `grafana.prod.in.mountain.com/d/Kn5xm-gZk/rabbitmq-overview` — use the `mntn-gke-bidder-01-prometheus` datasource, `bidder` namespace, `rabbitmq-rtb` cluster.
- Panel 14: message-delivered metric (drop = consumers stopped)
- Panel 9: messages ready/backlog count (rise = consumers falling behind)

**Baseline note:** There is always a baseline of ~2M unprocessed messages in the backlog for historical reasons (Bidder team to confirm). Monitor for count increasing *above* this baseline.

**Incident timeline (April 2026):** Issue began between 4/22 ~04:00 UTC and 4/23 ~01:00 UTC; consumers recovered ~4/25 14:15 UTC. Estimated ~2-day impact window. A prior similar incident occurred 4/1 (caused by Aerospike issue), resulting in no spend/underspend for CGs with newly updated audiences from 4/1 hr0 UTC through 4/2 hr13 UTC. (via Changxing Cao, #production-ops, 2026-04-25)

<!-- slack-extracted: 2026-04-28 -->
- ## summarydata.visits.elapsed_time — Time-to-Visit After Impression

`summarydata.visits.elapsed_time` captures the delta between an impression's timestamp and the subsequent page view timestamp for verified visits. Key details:
- Applies to records sourced from VVS's clickpass_log output (i.e., `visits.click = false`).
- The impression used for attribution is logged by clickpass_log at redirect time, and elapsed_time is computed from that impression's `time` field.
- Useful for calculating lift on visits and understanding per-advertiser visit lag distributions.
- Filters on `click = false` are recommended to isolate VV-sourced records. (via ray, #reporting_helpdesk_ask_anything, 2026-04-28)
- ## HHST (Household Score Threshold) — Behavior, Recent Changes, and Known Issues

### What HHST Does
HHST controls the minimum intent score required for a household to be targeted. Lowering the threshold expands the audience pool; raising it restricts to higher-intent households. The system reacts to campaign pacing: underspending campaigns trigger HHST decreases to expand reach.

### March 2026 Change — Switch from Beeswax Win Notifications to CIL
On approximately March 16, 2026, the HHST DAG was changed to use the Cost Impression Log (CIL) as the pacing signal instead of Beeswax (Bx) win notifications. Previously, Bx win notifications caused HHST to not properly adjust for some campaigns. The CIL-based approach reduced instances of HHST failing to respond to underspending campaigns.

**Side effect:** The CIL change coincided with a decline in audience quality metrics (pv_perc) and IVR beginning mid-March 2026. Increased sensitivity to pacing has caused more aggressive HHST decreases on underperforming campaigns.

### April 1, 2026 — Sub-Second Bidding Migration
A sub-second bidding solution was rolled out on April 1, 2026, migrating more campaign groups to that solution. This contributed to additional underspend, which in turn drove further HHST decreases.

### Asymmetry in HHST Bump Logic
The HHST bump-up procedure is slow relative to the drop-down procedure:
- Campaigns pacing well in mid-intent receive a +300 point bump.
- Underspending campaigns can drop thousands of points (e.g., 10,000 → 3,400 in a single adjustment).
- Recovery from a large drop can take ~2 weeks even when the campaign subsequently paces well.

### Pending Fix — Max Reach Scoring
The root cause of slow HHST recovery is that max-reach IPs stopped being scored, removing fine-grained HHST control over the max-reach bucket. A proposed fix assigns random scores to max-reach IPs at bid time (bidder-side), restoring fine-grained HHST control within the max-reach bucket. (via Tofer, #production-ops, 2026-04-27)
- ## Audience Size Drop to Zero — Pixel Mapping / OPM Expression Conflicts

When a campaign group's audience size drops to zero overnight without a visible UI change in the activity log, a common root cause is that the advertiser's pixel data and exclusion rules have converged — i.e., the IPs being collected by the pixel are the same set of IPs being excluded by the campaign's audience configuration (e.g., Login Domain / Current Student Exclusion overlapping with a 1+ Page View audience).

This is distinct from segment deprecation. Possible triggers include changes to the advertiser's pixel mapping or the OPM expression logic, even if the pixel continues firing. Resolution requires the advertiser to update their pixel configuration or audience exclusion rules to separate the included and excluded populations. (via zach.schoenberger, #mission-control, 2026-04-27)

## Databricks ↔ BigQuery — read paths for the big logs (TI-837, 2026-04-28)

For high-volume scans (augmentor_log, guid_log specifically), reading directly from GCS via Spark on Databricks is dramatically cheaper and faster than scanning through BigQuery. BQ slot contention + scan billing avoided. Per Victor Savitskiy (#data-platform):

| Table | GCS path | BQ-only? | Read pattern |
|---|---|---|---|
| `bronze.raw.augmentor_log` | `gs://mntn-data-archive-prod/augmentor_log/` | No | **Read GCS Parquet directly via Spark.** Full historical archive, no 10-day TTL constraint. |
| `silver.logdata.guid_log` | `gs://mntn-data-archive-prod/guid_log/` | No | **Read GCS Parquet directly via Spark.** |
| `silver.logdata.cost_impression_log` | n/a | **BQ-only** | Spark BigQuery connector, table-only mode (resolves SQLMesh physical at runtime). Efficient streaming, no materialization needed. |
| `silver.logdata.clickpass_log` | n/a (complicated view) | BQ-only | Spark BigQuery connector with `materializationDataset` + `viewsEnabled=true`. BQ materializes a temp table; output-size limit ~200M rows on the result. Medium data size, queryable. |
| `bronze.external.household_scoring__prospecting_intent__v1` | `gs://household-scoring-prod/output/scoring/prospecting_intent/` | No | Hive-partitioned Parquet (year/month/day). Read from GCS directly. |
| `bronze.integrationprod.campaigns` | n/a | BQ-only | Tiny (508k rows / 100 MB). Either BQ or coredb. |

**Spark BigQuery connector pattern (Victor):**

```python
# Option 1: views/queries supported, ~200M output row limit, extra materialization cost
spark.read.format("bigquery") \
  .option("parentProject", "dw-main-bronze") \
  .option("billingProject", "dw-main-bronze") \
  .option("project", "dw-main-bronze") \
  .option("materializationDataset", "external") \
  .option("viewsEnabled", "true") \
  .load("table_or_query")

# Option 2: tables-only, no size limit, no extra cost
spark.read.format("bigquery") \
  .option("parentProject", "dw-main-bronze") \
  .option("billingProject", "dw-main-bronze") \
  .option("project", "dw-main-bronze") \
  .load("table_name")
```

**All 3 project-related properties must be set** or extra costs apply.

**Third option:** `airflow_vs` reader in `airflow_ti` — gives choice of compute engine (databricks / dataproc / dataproc-serverless). Useful for production pipelines, not interactive.

**Speedup estimate (TI-837 case):** v1 = 87 min wall on BQ for 30 advertisers, 7-day window. Hypothesis (untested): reading augmentor + guid from GCS via Spark on a high-compute Databricks cluster could cut to 10-20 min while also avoiding BQ scan billing. Awaiting first run to confirm.


### Databricks compute economics (Victor Savitskiy, 2026-04-28, TI-837)

**Job compute is 3× cheaper than interactive cluster compute.** Use job compute (Job → Add new job cluster) for any run >20-30 min OR using >16-20 cores. Interactive cluster is for development only ("make sure syntax works on a tiny dataset"). Same workload that costs $210 on interactive cluster runs ~$70 on job compute.

**Required tags on every job/cluster** (cost tracking):
- `project = TI-XXX` (or relevant ticket)
- `squad = ML` (universal label even for non-ML squads — convention)
- `env = Dev`

**Cluster setup defaults (Victor):** autoscale min 1 max 2 (or more for compute-heavy), Advanced → access mode = `dedicated`, **uncheck** "Use preemptible workers" for exploration (preemptible only for production where you've handled node-reclaim tolerance).

### Reading from GCS via Spark — explicit partition filters required

Per Victor: "Augmentor log is tricky. When reading from S3 [GCS], specify explicitly partitions — it speeds up quite a bit." Spark partition pruning needs explicit predicates in the `.filter()` chain (or `WHERE` SQL), not just downstream WHERE clauses, because directory pruning is decided at scan plan time. Pattern:

```python
spark.read.parquet("gs://mntn-data-archive-prod/augmentor_log/") \
  .filter("year = '2026' AND month = '04' AND day BETWEEN '20' AND '26'")
```

Without the filter pushed at read level, Spark scans every partition (orders of magnitude slower).

### Running Databricks compute from a local laptop — three patterns

1. **Databricks Connect / Spark Connect** — full notebook locally, compute on cluster. Limitation: not all Spark APIs supported.
2. **`airflow_ti` enhancement** — trigger Databricks/Dataproc job from local, fetch result back. Currently saves to Parquet only (line ~290 in `vertical_auto_assignment`); needs a small enhancement to print to system output. Choice of compute engine: Databricks / Dataproc / Dataproc-serverless.
3. **Job in Databricks UI** — create job, run, view output. Manual, not local-friendly but cheapest for one-shot runs.

For analysis-style work where output is small (e.g., per-cell ATT counts ~360 rows), Databricks Connect works fine. For bulk processing with huge output, save to Parquet and analyze locally afterward.

<!-- slack-extracted: 2026-04-30 -->
- ## Media Plan Race Condition — Campaign No-Spend Bug

A confirmed race condition exists during **flight date changes** on campaigns using Media Plans. When a campaign's flight end date is modified, there is a gap window during which media plan regeneration fails, leaving the campaign without an active media plan and causing it to pause (no spend). This issue was confirmed in production on 2026-04-28/29 affecting Media Plan experiment campaigns. The fix involved updating campaign end dates and patching the media plan display. **Implication for monitoring:** If experiment campaigns using Media Plans suddenly stop spending after a flight date modification, check for this race condition first. (via Jen Wang, #mission-control, 2026-04-29)
- ## Probabilistic Attribution (Comprehensive Reporting) — iCloud VV Behavior

MNTN's **Probabilistic Attribution** feature (labeled "Comprehensive Reporting" in the UI) handles visits and conversions from iCloud Private Relay IPs (Apple's pseudo-VPN). Prior to this feature, iCloud visit/conversion events were discarded if no other signal was found. With the feature enabled:

- iCloud visits are logged to a **separate table** and do not enter regular visit volume directly.
- Reporting takes a **weighted approach** using guids per iCloud IP to estimate attributable volume from iCloud visits.
- A similar weighted approach applies to conversions.
- At launch, average lift was ~7% on fewer than 50% of advertisers. After backend teams fixed upstream iCloud blocking bugs, the average lift dropped to ~1%.
- **New customers** have this enabled by default. **Existing customers** have it off (toggle available in UI).
- There is **no way to differentiate** regular vs. probabilistic visit/conversion volume in reporting — a tooltip in the UI explains the methodology but volume is blended.
- Only one iteration has shipped, and it was scoped to iCloud IPs only.

**Data implication:** When analyzing visit or conversion volume for advertisers with Comprehensive Reporting enabled, be aware that a small share (~1%) may originate from probabilistic iCloud attribution rather than deterministic matching. (via ray, #q1-2026-performance-churn-investigation-how-am-i-alive-what-is-life-i-wanna-die, 2026-04-29)
- ## Dynamic Take Rates — Feature Status: Disabled

MNTN tested **Dynamic Take Rates** (lowering take rates based on campaign performance) with a small pilot of approximately 20 small/new advertisers, many of whom subsequently churned. The feature had a design flaw: it could lower take rates but had no mechanism to raise them. For remaining advertisers that did not churn, take rates were gradually restored to the global level. **The feature has been disabled.** No ongoing behavioral impact expected. (via ray, #q1-2026-performance-churn-investigation-how-am-i-alive-what-is-life-i-wanna-die, 2026-04-29)

<!-- ti_884: 2026-04-30 -->
## CUPED ρ on MNTN visit-rate data — measured 2026-04-30 (TI-884)

Measured Pearson correlation of per-IP visit indicator (binary 0/1) between two
adjacent 30-day windows (Feb 2026 vs Mar 2026), filtered to IPs treated by Stage 1
campaigns in BOTH windows. Used to compute the CUPED SE multiplier
`sqrt(1 - ρ²)` for variance-reduction stack in power analysis.

| advertiser_id | name | n_ips_both_periods | mean Feb visit | mean Mar visit | ρ | CUPED SE multiplier |
|---|---|---|---|---|---|---|
| 31357 | WGU | 10.1M | 13.5% | 11.7% | 0.461 | 0.887 |
| 30506 | Vivint | 3.2M | 4.5% | 1.7% | 0.170 | 0.985 |
| 31276 | Ferguson Home | 2.2M | 26.8% | 24.0% | 0.441 | 0.897 |
| **mean** | — | — | — | — | **0.357** | **0.934** |

**Implication:** MNTN-specific CUPED gives only ~7% SE reduction (0.934 multiplier),
significantly less than the ~13% literature midpoint (sqrt(1-0.5²)=0.866). Driver:
high binary-outcome variance and moderate cross-period IP retention. Use 0.934
in headline post-stack MDE numbers. ρ varies by advertiser type — higher for
brands with repeat-traffic IPs (Ferguson, WGU); lower for Vivint where Feb→Mar
visit rates are very different.

Query: `tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/queries/ti_884_cuped_rho_measurement.sql`.

<!-- slack-extracted: 2026-05-01 -->
- ## DS9 (MNTN Campaigns) in MemDB

DS9 (data source 9, representing MNTN Campaigns / "Select Campaigns") **is** populated in MemDB and has been since its inception. It does not have entries in `tpa.categories` because it predates the current form of that table. The `/totals` endpoint is based on default requirements of DS14/category 1; if IPs are from category 1000, they will not appear in totals without an explicit adjustment to the totals request. This is distinct from DS42 (MNTN Select), which is also in MemDB. (via zach.schoenberger, #targeting-squad, 2026-04-30)
- ## CRM Lists — Prospecting Only; All Campaigns Have Holdouts

- CRM lists are only usable in **prospecting campaigns** (not retargeting).
- All campaigns (including retargeting) have a holdout group.
- Every delivering campaign requires an audience expression — this is true for retargeting campaigns as well. (via zach.schoenberger, #targeting-squad, 2026-04-30)
- ## Audience Service (AS) — vertical_data_source Upsert Flow (Fangorn)

When a user toggles a vertical data source in the Command Center UI and saves, the normal flow is:
1. Gary-QL calls Audience Service (AS).
2. AS upserts the `vertical_data_source` value.
3. AS updates the advertiser's segments.
4. AS publishes a downstream event (message) that the consumer picks up to execute the swap flow (segment expression regeneration).

This flow can be triggered manually via a PUT method call to AS even without the UI toggle being deployed. The SQL-only path (direct DB update) does **not** kick off the swap flow; the PUT method to AS must be used to trigger downstream segment regeneration. (via Jaime Mutale, #targeting-squad, 2026-04-30)
- ## coredb/integrationprod Health Monitoring

- **pgbouncer-specific dashboard:** `http://monitor-coredw-prod.in.mountain.com:3000/d/c0f42b11.../live-monitor` (internal)
- **Full coredb instance metrics:** GCP Console → `https://console.cloud.google.com/sql/instances/coredb-prod/overview?project=mntn-csql-core-db-prod` — dropdown provides multiple metric views.

For latency blips connecting to coredb from application services, check GCP Console first before escalating. (via mohan, #data-platform, 2026-04-30)
- ## Open Market Bidding Logic — Bid Below Impression Floor Drops

When MNTN bids on open market (non-deal) inventory via Beeswax:
- Publishers set a floor price for open market supply (the "impression bid floor"). This floor is not constant — it varies by inventory and is not MNTN-specific.
- MNTN's bid price for open market is based on a rolling ~7-day average CPM paid for that publisher (computed by `coredw/lds/functions/populate_publisher_adsize_metrics.sql`).
- If MNTN's average publisher CPM is below the publisher's floor, the bid is **dropped on MNTN's side** (not sent to Beeswax). This was an explicit design choice: a previous attempt to always meet the impression floor resulted in average CPM inflation without a corresponding spend increase, so it was reverted.
- **PMP deals use fixed pricing** and are not subject to this drop logic — MNTN has never responded well to biddable deals and negotiates fixed-price PMPs instead.
- **Known limitation:** Since bid prices are based on historical win averages, prices will not self-adjust upward if market floors rise. The Performance Pricing team's ML-based solution is intended to address this longer-term. (via Abbas, #production-ops, 2026-04-30)

<!-- slack-extracted: 2026-05-05 -->
- **AdDB → BigQuery Replication (Upcoming Migration):** A maintenance window was scheduled on AdDB to enable replication from AdDB to BigQuery. Services reading AdDB were asked to temporarily shut down (or extend caches) during the 30-minute window. Key operational notes:
- `creative-sync-service` specifically must be turned off during AdDB maintenance; other services can likely remain on with stale cache.
- Default cache TTL for services reading AdDB is ~5 minutes.
- Attribution Squad supports AdDB reads and can coordinate downtime via Nate Gardner / `U040UFVBVDW`. (via Mike Dolzer, #data-platform, 2025-05-04)

<!-- slack-extracted: 2026-05-06 -->
- **Audience Segment Report Pipeline — Cadence and Availability**

- The audience segment report is a daily batch job.
- Job kicks off at **10:15 UTC** and takes approximately **6 hours** to complete, making results available around **9:00 AM PT**.
- The general target SLA for graph/all_facts data is also 9 AM in the corresponding timezone (ongoing batch job).
- 9 AM ET / 6 AM PT stated in help desk documentation is **inaccurate** for audience segment data — it will not be available by 6 AM PT.
- DAG monitoring starting point: https://cloud.astronomer.io/cmcvc7plk045e01o49xkio5lc/dags (Astronomer/Airflow). (via ray, #reporting_helpdesk_ask_anything, 2026-05-05)

<!-- slack-extracted: 2026-05-07 -->
- ## Lost VAST % Alert — Historical False-Alarm Root Cause

The Lost VAST % monitoring alert had a bug: old logic from 2023 filtered checks only on `partner_id = 8`, causing records with `partner_id = 79` to be incorrectly flagged as missing VAST. This produced false positives that were only detectable after a large drop event from Beeswax exposed the logic gap. The alert was corrected to include both partner IDs. A follow-up ticket exists to build a separate VAST % monitor focused on the MNTN bidder specifically. (via Harry Connelly, #mission-control, 2026-05-06)
- ## Pixel Issues in conversion_log — Escalation Path

When anomalous conversion amounts are detected in `conversion_log` (e.g., suspiciously high values for specific advertisers), this is typically a pixel configuration issue rather than a data pipeline bug. The correct escalation path is to route these to **Pixel Ops** — starting with **Ashley Pineda Varela**. (via zach.schoenberger, #targeting-squad, 2026-05-06)
- ## Shopify Identity Links — Trustworthiness in Graph

For Shopify-sourced identity data:
- **Email and phone links** from Shopify orders can be treated as high-trust ("truthy") links in the identity graph.
- **IP, guid, and ga_client_id** links associated via Shopify order-ID-matched `conversion_log` rows should also be treated as legitimate links, but with nuance: if these identifiers appear connected to many other IDs in the dataset, they are likely **shared identifiers** (e.g., household or device shared by multiple people).
- Shared IDs should still be tracked in the graph for **exclusion** purposes but should **not** be used for inclusion/targeting, and should ideally be flagged as shared in the graph model. (via Jack Barbey, #identity_core_dev, 2026-05-06)

<!-- slack-extracted: 2026-05-09 -->
- **DS13 → DS46 Migration: Reporting Impact (Fangorn)**

As part of the Fangorn rollout, campaign groups on Mountain Match are being migrated from `data_source_id: 13` (DS13) to `data_source_id: 46` (DS46) in the `audience.audience_segments` table. This is an overlay/mutation applied on the fly and does not change the underlying `audience.audiences` expression table.

**Impact:**
- **Bidder mechanics:** Not affected — the bidder reads from `audience_segments` which is updated correctly.
- **Reporting/dashboards:** Any query or view that detects MM campaigns by looking for DS13 (or DS19) in audience expressions will fail to identify migrated campaigns. This includes `audience.audience_type_alpha`, `bi.v_feature_date`, and any ad hoc queries using `expression LIKE '%:13,%'` or equivalent.
- **Audience size concern:** Some DS13 verticals show significantly different audience sizes when compared to DS46 equivalents. This was flagged as a risk but the rollout is proceeding with awareness of this.

**Action required for affected teams (DM, BAE, and others):** Update queries that filter or identify MM campaigns using DS13/DS19 to also include DS46. (via Matt Brorby, #fangorn_launch_day, 2026-05-08)

<!-- slack-extracted: 2026-05-12 -->
- ## stable* vs regular ROAS/CPA/ConversionRate metrics

`stableroas`, `stablecpa`, and `stableconversionrate` metrics are directly connected to ramping data and budget change reporting. They represent a weighted split of ROAS/CPA/Conversion Rate for the cohort of impressions/households from the *existing* budget, versus the ramping behavior of incremental dollars.

**Rule of thumb:** For standard reporting widgets (e.g., conversion/visits reporting in Express), use `roas`, `cpa`, `conversionrate`, and `visits` — not the `stable*` variants. The `stable*` metrics are purpose-built for ramping/budget-change analysis contexts. (via Al Beretta, #reporting_helpdesk_ask_anything, 2026-05-11)
- ## Databricks → BigQuery Spark Connector: Known Transient Failures & Configuration

The Identity team's weekly graph build job on Databricks has experienced recurring `INTERNAL: http2 exception` / `StatusRuntimeException` errors when pushing queries to BigQuery and reading results back via the Spark BigQuery connector. These appear to be transient gRPC-level failures.

**Key configuration options to set:**
- `temporaryGcsBucket` — The connector sometimes requires a temp GCS bucket for intermediate result storage; not specifying one can cause failures.
- `viewsEnabled` — Must be set if the query touches any BigQuery views.
- `materializationDataset` — Required when materializing query results; currently set to `aggregates` in the Identity graph job.
- `parentProject` / `materializationProject` — Currently both set to `dw-main-silver` for Identity graph jobs. Recommendation: switch `parentProject` to `mntn-identity-prod` so job history is disambiguated from other teams in the BigQuery Job History UI.

**Mitigation approaches applied:**
- Increase Spark `maxFailures` from 4 to 6 to tolerate transient connector errors.
- Tune BQ options and connection timeouts in the BQ writer code.
- Materialized views persist in BQ for 24 hours before deletion — a failed job can be repaired and will skip already-completed steps.

**Job history filter:** Use `databricks-compute@mntn-databricks.iam.gserviceaccount.com` as the User filter in BigQuery → Job History to find Identity team queries.

**Code location:** `parentProject` for BQ jobs is defined in `src/main/scala/com/mntn/idg/graph/datasets/graphDatasets.scala`. (via Jack Barbey, #identity_core_dev, 2026-05-11)
- ## HHST Pacing State: Max Reach (MR) Fix — TI Team IP Scoring Change (May 2026)

Starting around May 1, 2026, the TI team began scoring Max Reach (MR) IPs independently (separate from Fangorn scoring). This change fixed a "max reach trap" where HHST would remain stuck in MR state for extended periods even when pacing recovered.

**Effect:** HHST now reacts more quickly and transitions back to Mid Intent (MI) when pacing recovers. As a result:
1. More IPs are scored (rather than unscored/MR).
2. More campaign groups (CGs) benefit, shifting audience from MR (unscored) to MI or higher intent levels.
3. In theory, performance across CGs should improve due to better audience quality.

This change is observable in pacing signals: HHST MR reduced while HI/PP/MI levels increased — this is expected and indicates healthy pacing behavior, not a problem. (via Swapnil Patil, #mission-control, 2026-05-11)

<!-- slack-extracted: 2026-05-15 -->
- **Realtime Spend Source — Migration from R2DS to CHAPI**

The UI layer's realtime spend calculation (used when a user edits a live flight budget) was sourcing from the `r2ds` service. An incident on 2026-05-13 revealed that r2ds experienced a ~50% failure rate, causing Gary (GaryQL) to calculate incorrect spend for flights being updated, which contributed to an overspend incident.

Three identified remediation actions:
1. **Migrate realtime spend queries from r2ds to CHAPI** — CHAPI is considered more reliable.
2. **Update realtime spend queries to use the new BQ-sourced table** — the existing tables used are being deprecated post-BQ migration.
3. **Improve fallback/error handling logic** in the event realtime spend cannot be retrieved (especially critical until CHAPI migration is complete).

**Caution:** This is mission-critical UI code. Changes require thorough testing. QA environments lack real spend data, making validation difficult. Recommended approach: deploy changes behind a feature flag and validate against test AIDs or experiment accounts with pre-existing spend in production. (via Tom Manuel, #mission-control, 2026-05-14)
- **Flight Budget Edit Validation Bug — Budget Lowered Below Amount Already Spent**

A bug was identified (2026-05-13) where the UI allowed a user to save a flight budget lower than the amount already spent on that flight (e.g., a flight with $11,744 in spend was saved with a $972 budget). The UI validation logic is intended to prevent users from setting a budget below current spend, but this guard failed.

The consequence is that MNTN absorbs the difference between actual spend and the incorrectly saved budget amount, since billing is based on the saved budget figure. This was escalated as a P0 incident (`#inc-20260513-budget-change`, PS-8109).

**Root cause investigation:** Tom Manuel found the issue via Pendo session recordings. The realtime spend source (r2ds) failure likely contributed — if Gary calculated spend as zero or incorrect at the time of the edit, the validation threshold would be wrong. (via Tom Manuel, #mission-control, 2026-05-13)

<!-- slack-extracted: 2026-05-19 -->
- **MNTN Pixel and CRM Match Rate:** The MNTN pixel is used for CRM list matching, but it is not required. Pixel data is an enhancement only — it improves CRM match rates when the advertiser can send data, but CRM matching can still occur without pixel setup. If an advertiser does not have the pixel configured, CRM match rates will simply not benefit from pixel-based enrichment. Absence of the pixel is not the root cause of zero match rates. (via zach.schoenberger, #targeting-squad, 2026-05-18)
- **Audience Service — Exclusion Logic (PR #254 correction):** In the audience service (GitHub: audience-service/pull/254), a logic bug was identified where a rule intended to apply only to exclusions was incorrectly being applied to both inclusions and exclusions. The correct behavior is: the relevant logic should apply to exclusions only. This was flagged as a deviation from the Fangorn mirror pattern. (via zach.schoenberger, #targeting-squad, 2026-05-18)
- **VV of NTB Drop (sys signal since 3/17) — False Alarm:** A previously flagged drop in Verified Visit (VV) of New-to-Brand (NTB) metrics since 2026-03-17 was determined to be a false alarm. The issue is on the monitoring data source side, not a true feature or attribution issue. (via Johnny, #mission-control, 2026-05-18)
- **Offline Attribution (sys606) — Pipeline Bug (2026-05-16):** A bug was introduced into the offline attribution pipeline causing a data quality cliff-drop beginning 2026-05-16. The bug was identified and fixed. The SM606 monitoring chart should reflect improved numbers after the fix, though the chart may require a daily refresh to display the corrected data. (via ray, #mission-control, 2026-05-18)
- **Identity Graph Monitoring Dashboard Pattern (Alexander Jerneck):** For identity/DS model monitoring dashboards, the established pattern is: (1) calculate metrics in a Databricks notebook, (2) save results to a BigQuery table, (3) build the dashboard in HTML reading from the BQ tables, (4) publish to Mode. Convenience scripts for pushing/pulling notebooks from Databricks, running dashboards locally, and publishing to Mode exist in the `dsanalysis` repo under the `graph-heuristics/id-166` branch. (via Alexander Jerneck, #identity_core_dev, 2026-05-18)

<!-- slack-extracted: 2026-05-20 -->
- ## CRM Upload Match Rate — IP Type Behavior

Match rate for CRM audience uploads is defined differently depending on upload type:

- **Email / Phone uploads:** Match rate reflects how many records were successfully converted to IP addresses for targeting/exclusion. Calculated by the Spark pipeline (`crm_match_rate_gcp.py`, DAG: `crm_match_rate_dag.py`) and stored in `ui.audience_uploads.match_rate`.
- **IP uploads:** Match rate is not calculated or stored — the `match_rate` column in `ui.audience_uploads` is `NULL` for all records where `audience_upload_type_id = 3` (IP type). This is expected behavior, not a data bug.

**Why:** When the upload is already a list of IPs, no conversion step is needed, so a match rate is conceptually N/A (or effectively 100%). The existing Spark match rate pipeline only handles email and phone types.

**UI treatment:** The recommended approach is to display `N/A` in the UI for IP-type uploads rather than showing `--` (null) or hard-coding `100%`, to avoid confusion about what the metric means. Additionally, surfacing the upload type in the display table provides context for why some rows show a percentage and others show N/A.

**Table:** `ui.audience_uploads` — `match_rate` column is null for IP-type uploads by design. (via Macie Kluting, #targeting-squad, 2026-05-19)

<!-- slack-extracted: 2026-05-22 -->
- ## CRM IP Audience Upload — MemDB Sizing Behavior

When a user uploads a CRM list consisting entirely of IP addresses, the audience size may appear missing in the UI and return no result from the `/totals` endpoint of `audience-service`. This is **not necessarily a processing failure** — the IPs may be present in `audience_uploads` and `audience_upload_ips` tables and correctly loaded into membership DB (memDB), but still show no UI size for the following reasons:

1. **Sampling vs. prod endpoint confusion:** `membership-gateway-as.prod.in.mountain.com/totals` is the **sampling** endpoint, not production. Use `membership-gateway.prod.in.mountain.com/totals` to check actual membership DB contents. A small IP list may return zero results in the sample but be present in prod.
2. **Small list behavior:** For interest/geo segments, sizes below 1,000 show as `< 1,000`. For CRM uploads, no such fallback label is displayed — the size field simply appears blank even if targeting is active.
3. **Targeting still occurs:** IPs present in memDB will be targeted when they appear in the bidstream, even if no UI size is shown. For very small lists (e.g., ~36 unique IPs), match frequency will be low by probability.

**RabbitMQ ACK timeout** (30-min default) has been flagged as a potential failure mode for large CRM files that take longer to process — pod restarts can prevent the TPA refresh from populating memDB. Increasing the ACK timeout beyond 30 minutes in broker config is a suggested mitigation for that specific failure path. (via Jordan Piepkow, #targeting-squad, 2026-05-21)
- ## Mountain Match (Targeting) — DMA Usage Scope

DMA (Designated Market Area) is used in MNTN's platform for **customer-facing geo targeting** (advertisers can filter by DMA in campaign setup), but it is **not used as a model input, scoring feature, or pipeline column** within the Mountain Match targeting infrastructure.

Specifically (confirmed by the targeting infrastructure squad):
- **Model inputs:** DMAs are not used
- **Scoring outputs:** No DMA references surfaced to customers, PEX, or external APIs via Mountain Match
- **Pipeline/BQ schemas (squad-owned):** No DMA column references of consequence
- **Customer-facing UI:** DMA-based geo filtering is available (handled by the PRO UI squad and reporting layer, not the targeting squad)
- **Reporting side:** DMA data is available in reporting outputs

No references to `DMA` or `Geo type ID 4 = DMA` were found in targeting infrastructure repos. (via Matt Brorby, #tgt-infrastructure-squad, 2026-05-21)

<!-- slack-extracted: 2026-05-27 -->
- ## Audience Intent Scoring Scope — Active Advertiser Filtering

The audience intent scoring pipeline (`spark/audience_intent/advertiser_high.py`) currently scores every IP for every advertiser that has a vertical assignment — approximately 25,000 advertisers. This is excessive: MNTN only has ~300–400 live advertisers at any given time.

**Problem:** Generating scores for ~25K advertisers instead of ~400 active ones causes the membership loader (bidder-side) to crash due to data volume, and the problem worsens over time.

**Expected behavior:** Scores should only be generated for advertisers with live campaigns **or** campaigns set to go live imminently. The definition of "active" for scoring purposes should include:
- Advertisers with currently live campaigns
- Advertisers with campaigns scheduled to launch soon (same-day launches do occur)
- Recently onboarded advertisers (as a buffer)

**Note:** Defining "active" is non-trivial — advertisers can churn and return — so the filtering logic needs to account for imminent launches rather than just current live status. (via Zach Schoenberger, #tgt-infrastructure-squad, 2026-05-27)
- ## Ghost Bids — Bidder Feature (Deployed 2026-05-27)

**What it is:** Ghost Bids are a new bidder feature that allows IPs/segments previously excluded from bidding to pass through the bidder and be tagged with a failure reason rather than being hard-excluded. This enables data collection on traffic that would have been suppressed. **Critical for BER-2250 / TI-886 incrementality measurement.**

**Failure reason identifiers — raw GCS field names (per Ryan Kleck DM, 2026-06-01):**
- Beeswax Bidder: `threshold_failure_reasons = 'ghostBid'` (camelCase) — raw avro at `gs://bidder-price-events-prod-east/topics/rtb-bid-price-events/date=YYYY-MM-DD/`
- MNTN Bidder: `bid_dropped_reason = 'ghost-bid'` (dashed) — raw parquet at `gs://bidder-bid-events-prod-east/v2/YYYY-MM-DD/HH`

**BigQuery silver — canonical query (verified 2026-06-01, project `dw-main-silver`):**

```sql
SELECT DATE(time) AS dt, COUNT(*) AS ghost_bids
FROM dw-main-silver.logdata.bidder_bid_events
WHERE DATE(time) >= '2026-05-27'
  AND threshold_failure_reasons = 'ghost-bid'
GROUP BY 1 ORDER BY 1
```

- **Column:** `logdata.bidder_bid_events.threshold_failure_reasons` (STRING). Also surfaced via `logdata.bid_attempted_log` (same underlying table).
- **Value:** `'ghost-bid'` (dashed). NOT `'ghostBid'` — BQ silver normalizes to the MNTN-bidder convention even though Ryan's note used the Beeswax camelCase in one example. Filter literally on `= 'ghost-bid'`.
- **Observed volume — 2026-05-30:** 752,981 ghost-bid rows in `bidder_bid_events` (full-day). Other top non-null reasons same day: `dailyTermImpressionRateLimited` (971M), `metadata:ctv-blocked-by-block-list` (711M) — so ghost-bid is a small minority of all dropped-bid records, sized like ~10% of post-pacing successful bids per Ryan.
- **`bidder_auction_events.auction_dropped_reason` is NOT the ghost-bid surface.** It carries auction-level drops (`global-allow-list-rejection`, `no-candidates-after-pacing-engine`, etc.); zero `ghost`-matching values 2026-05-25 → 2026-05-31. Ignore it for ghost-bid analysis.

**Ghost wins are NOT logged.** Bidder only emits ghost-bid records; whether those bids would have won is unknown. To estimate ghost wins, apply the campaign or advertiser win-rate to ghost-bid volume. See [Ghost Win Simulation Discussion](https://mntn.atlassian.net/wiki/spaces/DATA/pages/3608150103/Ghost+Win+Simulation+Discussion) for the in-flight design debate (Scylla push, etc.).

**No backfill — data from 2026-05-27 forward only.** Cohort + experiment design needs to assume the deploy date as patient zero.

**Expected impact on metrics:** ~10% increase in bid drop reasons (≈10% of successful bid count). Does **not** affect pacing, deliverability, or campaign performance metrics.

**Monitoring note:** When reviewing bid drop reason trends in dashboards or queries, account for the step-change increase starting 2026-05-27. (via Ryan Kleck, #mission-control 2026-05-27 + DM 2026-06-01)

**Using `bidder_bid_events` for incrementality cohorts (verified 2026-06-01 schema):**

| Field | Type | Use in lift analysis |
|---|---|---|
| `threshold_failure_reasons` | STRING | Arm label. `'ghost-bid'` = holdout. NULL/empty + `has_price = TRUE` = ITT treatment (passed eligibility, sent to exchange). |
| `advertiser_id` | INTEGER | Cohort dimension |
| `campaign_id`, `campaign_group_id`, `creative_id`, `line_item` | INTEGER | Cohort dimension |
| `objective_id` | INTEGER | **On the bid event row directly** — no campaigns-dim join needed for prospecting / retargeting cuts (`IN (1,5,6)` = prospecting, `=4` = retargeting). |
| `channel_id` | INTEGER | 8 = CTV, 1 = display |
| `ip` | STRING | Household identifier (same grain as v5 cohort) |
| `household_score`, `advertiser_household_score` | INTEGER | Tier stratification directly per bid — no separate scan of `household_scoring__prospecting_intent__v1` |
| `has_price`, `price` | BOOLEAN / INTEGER | "Passed bidder, sent to exchange" filter. Ghost bids have `has_price = FALSE` per Confluence page. |
| `auction_id`, `exchange_auction_id`, `bid_id` | STRING | Join keys to `win_logs` (for ATT-won cohort) |
| `time` | TIMESTAMP | Bid timestamp — use `DATE(time)` for partition filtering |

**NOT on `bidder_bid_events`:**
- `funnel_level` — still need `bronze.integrationprod.campaigns` dim to split Stage 1 vs Stage 2/3 prospecting
- "Won at exchange" indicator — only inferrable by joining `win_logs` on `auction_id`
- "Impression rendered" indicator — only inferrable by chaining bid_events → win_logs → impression_log / cost_impression_log

**Cohort patterns for incrementality:**

```sql
-- Holdout cohort (one table)
SELECT DISTINCT advertiser_id, ip, household_score, objective_id
FROM dw-main-silver.logdata.bidder_bid_events
WHERE DATE(time) BETWEEN @start AND @end
  AND threshold_failure_reasons = 'ghost-bid'
  AND ip IS NOT NULL AND ip != '0.0.0.0';

-- ITT treatment cohort (same table — bid placed)
SELECT DISTINCT advertiser_id, ip, household_score, objective_id
FROM dw-main-silver.logdata.bidder_bid_events
WHERE DATE(time) BETWEEN @start AND @end
  AND (threshold_failure_reasons IS NULL OR threshold_failure_reasons = '')
  AND has_price = TRUE
  AND ip IS NOT NULL AND ip != '0.0.0.0';

-- ATT-served treatment cohort (separate table, same as v5)
SELECT DISTINCT advertiser_id, ip
FROM dw-main-silver.logdata.cost_impression_log
WHERE DATE(time) BETWEEN @start AND @end
  AND ip IS NOT NULL AND ip != '0.0.0.0';
```

Then LEFT JOIN to `guid_log` (causal lift) and/or `clickpass_log` (attribution wedge) per IP × advertiser. See `knowledge/experimentation.md` § "Bidder-Level Ghost Bids — Live Stream Methodology" for the full methodology, including the ITT vs ATT trade-off, window-ceiling change (10 → 90 days), and what survives unchanged from TI-837 v5.

**Empirical verification on 2026-05-30 (verified 2026-06-01):**

| Arm | rows | distinct advertisers | distinct campaigns | distinct IPs | has_price=TRUE | price>0 | objective_id populated |
|---|---:|---:|---:|---:|---:|---:|---:|
| `'ghost-bid'` | 752,981 | 22 | 106 | 180,879 | **0** | 752,981 (100%) | 752,981 (100%) |
| eligible-no-failure | 3,948,214 | 22 | 127 | 887,088 | 3,948,214 (100%) | 3,948,214 (100%) | 3,948,214 (100%) |
| other-failure | 69.17B | 22 | 145 | 30.0M | 0 | 769K | 69.17B (100%) |

- **Ghost-bid rows carry full attribution** — `advertiser_id`, `campaign_id`, `objective_id`, `household_score`, `ip`, `price` are all 100% populated on ghost-bid rows. Cohort can be built off `bidder_bid_events` alone.
- **`has_price = FALSE` on every ghost-bid row, but `price` IS logged.** Matches the Confluence page note (`hasPrice=false, price still logged`). **Use `has_price = TRUE` as the ITT-treatment filter** — it cleanly excludes ghost bids. Do NOT filter on `price > 0` to exclude ghosts (price is populated for both).
- **Holdout fraction ≈ 16%** (753K / (753K + 3.95M)) for the day, consistent with a ~16% production holdout sample. Matches Ryan's "~10% of successful bid count" rough sizing.

**⚠️ Coverage caveat — `bidder_bid_events` is MNTN-bidder ONLY (confirmed by Malachi 2026-06-02):** only **22 distinct advertisers** appear in the entire 2026-05-30 table across all arms, against ~300-400 live MNTN advertisers. The Confluence page describes two separate bidders — Beeswax (`rtb-bidder-service` Kotlin) and MNTN (`rtb-campaign-service` Rust) — and `bidder_bid_events` carries only the MNTN-bidder stream. The dashed `'ghost-bid'` value matches the MNTN-bidder naming convention. Beeswax-bidder bid events (with `'ghostBid'` camelCase) land in a different BQ table or aren't yet ingested to silver. **For incrementality analysis off this stream alone, cohort is capped at the MNTN-bidder advertiser set** — likely a subset of TI-837 Phase 2's 30 advertisers.

**Open verification items still TBD:**
- **Where do Beeswax-bidder ghost bids land in BQ silver?** Open question for Ryan. Until answered, full-platform coverage would require raw-GCS reads of `gs://bidder-price-events-prod-east/topics/rtb-bid-price-events/`.
- Scope of `holdout_cids` (Aerospike): ~~global random fraction, per-campaign-group, or per-advertiser?~~ **Partly resolved (Aerospike Datastore Confluence, 2026-06-09):** `holdout_cids` is a **per-IP array of `campaign_id`s** in the `rtb.household-profile` set (PK = IP). So holdout is recorded at the **(IP, campaign)** grain — an IP carries the explicit list of campaigns it is held out from. Still open: how that list is *assigned* (the hashing/bucketing that decides which IPs land in a campaign's holdout). See `knowledge/data_catalog.md` § "Aerospike `rtb` namespace".

<!-- slack-extracted: 2026-05-30 -->
- **Data Source (DS) Taxonomy — Key DS Definitions and Legacy Notes**

- **DS1:** Legacy Oracle data source. No longer present in IPDSC but still available in the taxonomy/UI. ~553 active prospecting campaigns reference it. May have been disabled by the AUD team — status should be confirmed.
- **DS11:** Legacy Liveramp data source (deprecated). Used device_id to map to IP for targeting. Retained in the TPA taxonomy because reporting still requires it, but not used for active targeting.
- **DS14:** MNTN global data — automatically added to all audience expressions to filter down to only IPs seen in `guid_log` (4-day window) and `augmentor_log` (1-day window). Functions as an activity recency filter.
- **DS19:** Used as an input source for the BUK model (see BUK/DAR entry).
- **DS35:** Current Liveramp data source. Liveramp now sends IP addresses directly (replacing the older device_id mapping approach of DS11).
- **DS46:** Mountain Match Peak Performance feature flag data source. Impressions associated with DS46 should be reported as Peak Performance in audience segment reporting, equivalent to how DS13 is handled (clean swap of DS46 in place of DS13).
- **DS9:** Related to MNTN Select campaign reach (limited internal knowledge).
- **DS34 vs DS355420:** Two MNTN Pageview data sources exist; DS355420 is not actively used.
- **DS2, DS21, DS34, DS43:** Multiple pixel data sources exist with different `data_source_category_id` values, each serving different purposes.
- **DS45 and DS48:** Both share the same `data_source_key = 'ojLY3uGYtq'`, which appears to be auto-generated based on creation timestamp (both created 2025-11-25). Possibly unintentional duplication. (via Sean Yang, #tgt-infrastructure-squad, 2026-05-28)
- **Acxiom Interest Segments — Vendor Source**

All Acxiom interest segments used by MNTN are sourced via LiveRamp. More broadly, 98.4% of all audience segments in the platform are from LiveRamp, and 99.6% of active, recently-updated segments are from LiveRamp. Other segment vendors include Sharethis and Dstillery, but these account for a very small share. This was confirmed for legal/compliance purposes. (via malachi, #tgt-infrastructure-squad, 2026-05-28)
- **IVR Anomaly During Underspend Incidents**

During periods of significantly reduced platform spend, IVR (Incremental Visit Rate or a related visit-rate metric) will appear artificially elevated. This is a known artifact: lower total spend reduces the denominator while visit counts may not drop proportionally, inflating the rate. This should be treated as a false signal during spend incidents and not interpreted as a genuine performance improvement. Confirmed during the 2026-05-28 underspend incident. (via Johnny, #mission-control, 2026-05-29)

<!-- slack-extracted: 2026-06-02 -->
- **BOS Pipeline Data Corruption from CoreDW Rebase (May 2026):** The data feeding BOS (Budget Optimization System) comes from the DAG table `camperbid_prod__bos__campaign_summary_hourly`. This data was incorrect for the month of May 2026, likely caused by the CoreDW migration/rebase on 2026-05-21 when BER rebased CIL. The corruption caused three distinct failure modes: (1) Some campaigns show Total Spend below CIL, causing BOS to continue overspending because it doesn't recognize the flight spend has been hit. (2) Some campaigns show Total Spend above CIL, causing BOS to underspend because it thinks the campaign has finished. (3) Some campaigns show lower media cost vs. CIL due to take rate adjustments while Total Spend is accurate, causing BOS to send an erroneously low media cost stop signal to the Bidder. A full pipeline refresh of BOS was run to confirm and remediate. (via Johnny, #mission-control, 2026-06-01)
- **`cost_impression_log` timezone:** The `cost_impression_log` table stores data in UTC timezone. When comparing spend/impression figures against MNTN UI numbers or external reporting that uses Arizona Time (ATZ), timezone conversion must be applied. (via Pratik, #production-ops, 2026-05-27)

- **`campaign_groups.update_time` = `GREATEST(ui_flights.update_time, campaign_groups_raw.update_time)`** — confirmed by Victor Savitskiy (#tgt-infrastructure-squad, 2026-06-03) and verified empirically same day. The Postgres view `public.campaign_groups` computes `update_time` as the GREATEST of the campaign_group's own raw update_time AND the most recent flight update_time for the group. There is a separate `update_time_raw` column that contains only the raw cg-level update_time. **Practical implication:** any "campaign was modified" check (e.g., advertiser scoring filter, freshness gating, ETL recency) should use `update_time`, not `update_time_raw`, because the GREATEST formula propagates flight-level edits (start/end shifts, budget changes, status flips on a child flight) up to the campaign_group row. Empirical correlation across 17,499 cgs in last 90 days: 98.8% follow GREATEST exactly; 14.7% have `update_time = flight_max_update` (most recent change was a flight edit). Status transitions always bump update_time (4,869 / 4,869 archive transitions). Source query: `tickets/ti_adhoc_advertiser_scoring_filter/queries/06_flight_vs_cg_update_correlation.sql`.

<!-- slack-extracted: 2026-06-03 -->
- ### MNTN SELECT Billing Type — Pending Separation from PTV Fixed CPM

As of 2026-06-02, MNTN SELECT campaigns share `billing_type_id = 2` with PTV Fixed CPM in `core.flight_billing_types`. These are treated as equivalent in the current billing pipeline, but the behaviors are distinct and should be separated. The proposed change is:
- `billing_type_id = 2` → PTV Fixed CPM
- `billing_type_id = 3` → MNTN SELECT (native impression cap)

The Gary service (MSS team) is responsible for implementing the change: Gary will need to map SELECT campaign groups to the new billing_type_id = 3 based on product_id. Downstream services that consume billing type (PCS, Pacing Engine, PROUI) will need to be audited for impact. The change should be wrapped in a feature flag and rolled out to test campaigns first given the difficulty of fully validating spend/budget changes in QA. Tracked in PER-6526. (via Mike Allen, #reporting_helpdesk_ask_anything, 2026-06-02)

<!-- slack-extracted: 2026-06-06 -->
- **Bidder Auction/Bid Data — Massive Volume Increase (May 2026)**

Beginning ~May 28, 2026, bidder auction and bid log data volumes increased **+3–10x**, driven by onboarding new SSPs to the MNTN Bidder in anticipation of the PTV migration. An additional **~10x increase** is expected before volume stabilizes, meaning total growth could reach ~100x from baseline.

**Impact:**
- A single SQLMesh model was attempting to process **1.8 PB of data daily** — approximately 1.4x the total historical volume of all of CoreDW.
- Several SQLMesh + BigQuery models dependent on bidder data are timing out or failing to complete.
- Affected teams include: Identity (ID), BID, and PERML.
- `augmentor_identity_daily` and `guid_identity_daily` SQLMesh models are directly impacted.

**Recommended mitigation paths:**
1. Switch to **direct GCS access** via Spark (e.g., Databricks) instead of SQLMesh + BQ.
2. Use **`raw` layer tables** in `dw-main-bronze.raw` dataset rather than silver SQLMesh models.
3. **Shared aggregation table strategy:** All teams that need to process auction data agree on a single aggregation format and share one upstream aggregated table, rather than each team independently aggregating raw bidder logs.
4. **Multi-level aggregation:** Aggregate at hourly granularity first, then roll up to daily, to reduce single-pass query cost.

**Note:** The `raw` data for bidder tables lives in GCS and in the `raw` dataset in `dw-main-bronze`. The volume growth affects only MNTN Bidder tables, not Beeswax exchange tables. (via scotty, Rogus, Jack Barbey, #data-platform, 2026-06-05)
- **Geo Targeting — Audience Expression is Source of Truth; Bidder Propagation Delay is a Separate Issue**

When evaluating whether a campaign is serving out-of-geo, the **audience expression is the authoritative source of truth**. If a geo is present in the expression, the campaign will serve to that geo.

A known, separate issue exists where the MNTN Bidder buys against **stale/delayed audience data** — meaning that when an audience change is made (e.g., a geo update), it takes longer than expected for that change to propagate to the bidder. This is distinct from serving to a wrong geo permanently; it means the bidder temporarily continues buying against the old audience configuration.

**Key clarification (Zach Schoenberger):** Any reported out-of-geo issues are attributable to the bidder's delayed audience data propagation, not to a targeting system failure. The targeting team itself is not experiencing issues — the root cause is bidder-side processing lag on audience changes. (via Jordan Piepkow, Zach Schoenberger, #targeting-squad, 2026-06-05)
- **Household Graph — Source Column Migration: String → Int Encoding**

The identity graph is undergoing a schema migration where the `source` column in `household_graph` is being converted from an **array of strings to an array of integers** (source IDs). The migration scope is intentionally limited to reduce blast radius:

- **Only `household_graph`** will use the new `array<int>` format for the `source` column.
- **`sourceObservations`** and **`ExclusionSource`** columns in intermediate tables will **retain the old string format** to avoid large backfills across many downstream tables.
- At the final household graph build step, a mapping is applied to convert source name strings → integer source IDs, using mappings defined in `graph.conf`.
- `sourceObservations` is referenced in many intermediate tables; `ExclusionSource` is used in excluded IDs datasets. Both can be migrated in a future pass but are deferred now. (via Jack Barbey, Weiang Li, #identity_core_dev, 2026-06-05)

<!-- slack-extracted: 2026-06-07 -->
- ## CIL (Cost Impression Log) — Source Tables and Unlinked Logic

CIL is built from two primary sources:
1. `spend_log`
2. `win_logs`

Impressions in CIL are flagged `unlinked = false` (good) when the `impression_id` is found in `impression_log`. If the `impression_id` is NOT found in `impression_log`, the row is flagged `unlinked = true` (bad).

`impression_log` itself is a combination of:
- `dw-main-bronze.external.impression__v1`
- `dw-main-bronze.external.vastimpression__v1`

The terminology is counterintuitive: `unlinked = false` means the impression WAS successfully linked (found in impression_log); `unlinked = true` means it was NOT found and is therefore missing enrichment.

The current lookback window on `impression_log` is **3 hours**. Late-arriving data beyond that window will not be captured on normal incremental runs and requires a manual restate.

**Source:** Lizz (confirmed in incident triage, 2026-06-06) (via Lizz, #mission-control, 2026-06-06)

<!-- slack-extracted: 2026-06-09 -->
- **cost_impression_log — `unlinked` flag and `ad_served_id` relationship**

`unlinked = FALSE` means an `impression_id` was found in `impression_log` (linked impression). `unlinked = TRUE` means no matching `impression_id` was found — these rows always have a null `ad_served_id`. Filtering `unlinked = FALSE` is the standard pattern for valid linked impressions; `ad_served_id IS NOT NULL` is not additionally required but causes no harm. Volume reference (June 2026): ~55–61M rows/day with `unlinked = FALSE`; ~141–184K rows/day with `unlinked = TRUE`. (via Sonali, #reporting_helpdesk_ask_anything, 2026-06-08)
- **geo_version field — closed-loop geo resolution pattern**

`geo_version` represents which MaxMind IP-to-geo resolution version was used at the start of the targeting cycle. The closed loop is: MaxMind resolves IP → geo → audience targeting returns IPs → bidding happens on those IPs → reporting looks up the geo of the winning IP using the same MaxMind version recorded in `geo_version`. This ensures reporting geo matches the geo used for targeting. World Cup targeting bypasses this closed loop, which is why `geo_version` can be empty/null for World Cup inventory — and why empty `geo_version` for those rows does not affect spend or normal reporting. (via ray, #data-platform, 2026-06-08)
- **device_ip vs device_ipv6 — IPv6 traffic split**

For IPv6 auction wins, the IP address is stored in `device_ipv6` rather than `device_ip`. `device_ip` will be null for these rows. Queries that need a single IP field per impression must coalesce `device_ip` and `device_ipv6`. This split was introduced as part of IPv6 targeting support and affects the CIL pipeline and any downstream model that reads `device_ip` directly. (via Abbas, #data-platform, 2026-06-08)
- **Spend charging requirements — minimum fields needed**

The only hard requirement for MNTN to charge a customer for an impression is that the impression appears in both (1) the source table (spend_log or win_log) and (2) impression_log. Fields such as `geo_version` and `device_ip` are not required for billing. The BOS service/pacing pipeline still uses `spend_pacing` (with a 24-hour lookback on live spend_log) before switching to the CIL. As long as CIL pipeline issues are resolved within 24 hours of onset, spend is unaffected. (via lizz, #data-platform, 2026-06-08)
- **Auction log aggregation — reading from GCS vs BigQuery**

For large-scale aggregations over bidder auction/bid logs, reading directly from GCS parquet sources (via Spark) is preferred over reading from BigQuery tables. The BQ-based approach for `auction_events_agg` scanned 13.2 TiB per hour of data and timed out in production. The Identity team's `augmentor_identity_daily` pipeline moved to GCS-based Spark processing for the same reason. When building new aggregations over auction or bid logs, evaluate Spark + GCS first before attempting a BigQuery SQLMesh model. (via scotty, #data-platform, 2026-06-08)
- **data_source_key — uniqueness requirement and known data quality issue**

The `data_source_key` column in the integrations/data_sources table must be unique and non-null. As of June 2026, at least three records share duplicate `data_source_key` values (DS45 HubSpot and DS48 Tealium share one key; Upwave introduced a third duplicate). Root cause is manual SQL INSERT copy-paste that carries over an existing key. One record in prod (data_source_id = 61, AppsFlyer v2) has `data_source_key` saved as the string `'false'`, which is a known anomaly — this prevents adding a simple unique+not-null constraint without first cleaning up the anomalous value. The key is used by at least one vendor API integration for data delivery. A unique constraint and not-null constraint are being evaluated (ticket AUD-5368). (via Macie Kluting, #targeting-squad, 2026-06-08)

<!-- slack-extracted: 2026-06-10 -->
- ## Bidder System Architecture — Membership Cache & Bid Path

**Bid path:** SSPs (Magnite, Index, Freewheel, Pubmatic) or Beeswax (as proxy) → Campaign service → membership cache check → bid with VAST. Response budget is ~200ms; Beeswax has a tighter ~15ms limit.

**Membership cache (Aerospike):**
- Key = IP address; ~300M IP keys; 3–5 TB total; single-digit-ms lookup.
- Hit on *every* bid request.
- No in-bidder in-memory tier — cache is too large, and with 300M evenly distributed keys a subset cache provides negligible hit rate benefit.
- Per-IP record contains: segments, intent scores, geo, holdout IPs, spend, recency, and frequency-cap data.

**Scoring pipeline:**
- Scores are written to GCS (not MembershipDB directly).
- Flow: scoring → GCS → membership consumer (GCS → PubSub → RabbitMQ) → Aerospike.
- MembershipDB owns segment data and holdout logic.

**Spend pipeline:**
- Win notifications → Notification service → ScyllaDB (deduplication, prevents double-counting) → Kafka → 3 aggregators (frequency, spend, logs → GCS).
- End-to-end latency: ~1 minute.

**Static/pacing data:**
- Flight budgets and thresholds live in Redis, pulled on a 5–10 minute cron.
- A stopped flight can continue spending for up to ~10 minutes until the next Redis pull (roadmap item: switch to notification-based updates).

**Migration in progress:** Team is migrating off Aerospike to ScyllaDB + Redis for cost and support reasons. (via malachi, #tgt-infrastructure-squad, 2026-06-09)
- ## SQLMesh Plan Best Practice — Always Run Full Environment Plan

When developing SQLMesh models, always run `sqlmesh plan <dev_env>` (full environment plan, without `--select-model`) before submitting a PR. Using `--select-model "+*model*+"` with the `+` selector does not reliably capture all upstream/downstream dependencies and can miss impacts or incorrectly include unrelated models.

**Why:** A full environment plan ensures that any dependency issues (up or downstream) are surfaced before the production plan runs. The `+` selector has been found experimentally to sometimes miss required captures.

**CI verify-impact check:** The CI pipeline runs a `verify-impact` script that compares tree snapshots against baseline. If a new or modified model has not been planned in any environment for the current code tree, CI will fail with a message listing the missing snapshot. Fix: run `sqlmesh plan <dev_env>` locally, which generates the new fingerprint and satisfies the check. (via Dustin Niehoff, #data-platform, 2026-06-09)

<!-- TI-1044 2026-06-23 -->
## B2B CVR power floor + ghost-bid holdout bias (TI-1044, ElevenLabs)
**B2B conversion-lift is effectively unmeasurable.** At a 0.062% CVR (ElevenLabs, AID 51660, B2B), the
Lewis-Rao MDE to detect a 5% relative lift needs **~$1.8–2.0M/mo** spend (10% holdout, 80% power); 2% needs
~$11M. The SAME 5% lift on **visits (3.07% base) needs only ~$36K** — a ~50× gap set purely by the base
rate. Rule of thumb: **lead incrementality on visit rate; treat conversion-lift as directional-only unless
spend clears the CVR MDE.** "No significant CVR lift" at sub-$2M spend is the expected output of an
underpowered test, not evidence of no effect. (Reuse `ti_884_mde_calculator.py`.)

**Ghost-bid / holdout negative-lift bias (Matt Brorby, 2026-06-23):** the 10% hash holdout is clean at
assignment, but after the bidder it skews. Real bids are **frequency-capped**; **ghost bids are not** — so
the most active (high-frequency, high-visit-rate, often cellular/high-attribution) IPs flow into the
holdout, inflating it to ~13% and biasing the holdout outcome rate **upward → measured lift biased
NEGATIVE.** Cannot be stratified away (frequency is post-treatment); only fixable in bidder code. New
ghost-bidding tables (bid-events-log, "ghost-bid" label) have a **10-day TTL**. Treat ghost-ad lift reads
as a lower bound.

**Attribution over-credit (Q3):** default advertiser conversion windows are **30-day** click-through,
view-through, AND conversion (`advertisers.conversion_window` / `view_conversion_window` /
`click_conversion_window`). The 30-day **view-through** window credits CTV for any conversion within 30
days of an impression (no click) → reported CVR overstates causal CVR, widening the attribution-vs-
incrementality gap.

---

## graph.usersreached / impression_facts.uniques — the "users reached" mixed-key gotcha (TI-1019, 2026-06-24)

The R2 "Graph" table `graph.usersreached` ("Households Reached", HLL distinct) = `dw-main-silver.summarydata.all_facts.uniques`, whose base table is `summarydata.impression_facts.uniques` (HLL++ sketch). For WGU (advertiser_id 31357, trailing 30d) this is **~32.1M**, ~2x the distinct served-IP count (`cost_impression_log` distinct `ip` ≈ 15.7M). This is NOT a broader/pre-bid impression universe — it is the SAME won/served universe, counted with a **mixed unique key**.

**Source model** (`SteelHouse/sqlmesh`, `models/dw-main-silver/summarydata/impression_facts.sql`): reads `logdata.cost_impression_log` (WON/served only: `unlinked = FALSE AND ad_served_id IS NOT NULL`, dedup 1-row-per impression_id). The `uniques` HLL key is conditional:
```sql
hll_count.INIT(CASE WHEN c.channel_id = 8 OR c.objective_id IN (5, 6) THEN l.ip ELSE l.guid END) AS uniques
```
- **CTV (channel_id = 8) and objectives 5/6** → unique key = `l.ip` (device IP; CTV has no cookie/guid).
- **Everything else (display)** → unique key = `l.guid` (browser/cookie identifier).

So `uniques` is a near-disjoint blend of distinct-IPs (CTV legs) + distinct-guids (display legs). guids are ~2.4x more numerous than IPs (multiple cookies/browsers per household IP + cookie churn), so the display half inflates the count far above distinct IPs.

**WGU 30d empirical decomposition** (reproduced the mixed key off `cost_impression_log` → 32.46M, matches the 32.1M base-table HLL):
- display rows (guid-leg): 138.3M rows → **18.40M distinct guids**
- CTV/obj-5/6 rows (ip-leg): 205.4M rows → **14.06M distinct IPs**
- 14.06M + 18.40M ≈ 32.46M (legs barely overlap — different key types)
- whole-table distinct `ip` = 15.53M (≈ calculator's 15.7M); whole-table distinct `guid` = 37.1M.

**Implication for MDE baseline:** `sitevisitors/usersreached` mixes apples (visitors = guids/IPs) over a guid-inflated denominator for display. For a "you can't drive a visit from an IP you never served" denominator, use distinct SERVED IPs = `count(distinct ip)` from `cost_impression_log` (15.7M), not `impression_facts.uniques`. (`impression_facts.site_visitors` does not exist — site_visitors lives in `visit_facts`; `all_facts` joins them.)

`all_facts.sql` (same repo) FULL OUTER JOINs impression_facts + visit_facts + spend_facts on 19 keys and passes `uniques`/`uniques_arr` straight through — no re-keying. There is NO bid_facts in SQLMesh (`bids = 0`), so the count cannot be "all bids" or a pre-bid/augmentor stream. Zach's "impression_log not usable for ctv" note is consistent: CTV reach must be IP-based (no guid), which is exactly the channel_id=8 branch.

### What the reporting "graph" table actually IS — R2 / CHAPI / ClickHouse (TI-1019, 2026-06-24)

The reporting "graph" namespace Chris Franz pulls from (`graph.usersreached`, `graph.sitevisitors`, CPM, imps-per-IP, etc.) is **not a standalone table** — it's the metric-name layer of **R2** (`SteelHouse/r2`, the reporting visualization UI; the Infographic/reach widgets live in `apps/r2vl/src/widgets/Infographic/`). R2 reads its metrics from **CHAPI** = "**C**lick**H**ouse **API**" (`SteelHouse/chapi`), the reporting metrics service. **The physical storage layer is ClickHouse, NOT BigQuery and NOT Greenplum r2.** (The `summarydata.reach_meters` Greenplum view in `db_repo` grants `select ... to svc_clickhouse_read` — confirming ClickHouse is the read layer; coredw is only an upstream export source for a few legacy groups.)

**Full lineage (most concrete form found):**
```
R2 UI (graph.usersreached / graph.sitevisitors, CPM, imps/IP)
  → CHAPI reporting service (ClickHouse API)
  → ClickHouse  summarydata.all_facts_local_daily.uniques / .site_visitors      ← physical storage
  → [Airflow-reporting DAG load_reporting_data.load_all_facts, @hourly,
     replace_partitions; export BQ→GCS Parquet → ext view v_ext_all_facts → CH]
  → BigQuery  dw-main-silver.summarydata.v_all_facts  (= SQLMesh all_facts view, owner 'ber')
  → BigQuery  summarydata.impression_facts.uniques  (HLL, CTV→ip / display→guid)  +  visit_facts.site_visitors
  → logdata.cost_impression_log  (served/WON impressions)
```
So `graph.usersreached` = ClickHouse `all_facts_local_daily.uniques` = BQ `all_facts.uniques` = `impression_facts.uniques` (the mixed IP/GUID HLL above). `graph.sitevisitors` = `all_facts.site_visitors` = `visit_facts.site_visitors` (resolved-IP-grained). The R2/ClickHouse copy is a faithful copy of BQ `all_facts` — it does NOT re-key, so the 32.1M / 2× mixed-key behavior is inherited verbatim from `impression_facts`.

**reach_meter link (separate widget, same storage pattern):** the CTV "Households Reached" reach_meter widget is a DIFFERENT, audience-segment-grained metric — ClickHouse `info.reach_meters`, loaded hourly (`load_reporting_data_hourly.load_reach_meters`, exchange_tables) from BQ `dw-main-silver.summarydata.reach_meters` (legacy Greenplum twin: `db_repo` `coredw/summarydata/views/reach_meters.sql`, which uses audience-segment `total_audience_reach` + `reach_ips.ips_reached_last_7_days`, CTV channel_id=8, third-party segments). It is NOT the same as per-advertiser `graph.usersreached`. The older Spark `aggregates.audience_hll_by_day` → GCS → CoreDW-external → ClickHouse-copy DAG (PR #1024 disabled it, breaking the UI reach_meter copy DAG; revive under discussion — slack_review_queue.md) is the audience-side reach pipeline, parallel to but distinct from the `all_facts`/CHAPI impression-side reach.

**Ownership / routing:** SQLMesh `all_facts`/`impression_facts` models are `owner 'ber'` (BER = the reporting/analytics-eng group). The CHAPI ClickHouse load DAGs live in `SteelHouse/airflow-reporting` (`dags/chapi/`), owned by the **data-platform / reporting** team — route ClickHouse/CHAPI/R2 reporting-metric questions to **#data-platform** (cross-cutting) or **#reporting_helpdesk_ask_anything** (Ray answers R2/graph metric-definition questions there; cf. the `graph.visits` vs `graph.sitevisitors` clarification, 2026-04-28). Verified-visit / attribution-grain (`site_visitors`, resolved `ip`) authority = Zach Schoenberger.

## archives_audience_segment_archives.version is NON-MONOTONIC — order by create_time (AUDI-1070, 2026-06-30)

**Gotcha (cost a wrong finding before it was caught by adversarial verification):** the `version` column on `bronze.integrationprod.archives_audience_segment_archives` (and it appears on the other `archives_*` audience/campaign archive tables too) is **NOT a monotonic revision counter** — it wraps/resets. Concretely for Avon campaign 259556: a row stamped `version=113` is dated `create_time = 2024-10-16`, while `version=101` is dated `2025-12-11`; there are even two distinct rows both numbered `v1` on 2025-10-29.

**Consequence:** `QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY version DESC)=1` returns a **stale snapshot** (here an Oct-2024 config), NOT the current one. This silently produces a wrong "latest expression" / wrong data_source_id set.

**Rule:** to get the latest (or as-of-date) targeting expression, **order by `create_time`, never by `version`.**
```sql
-- latest config per campaign:
QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY create_time DESC)=1
-- as-of a date (point-in-time config):
... WHERE create_time <= 'YYYY-MM-DD' ... ROW_NUMBER() OVER (ORDER BY create_time DESC)=1
```
Note: the campaign **dimension** archive `archives_campaign_archives` (name/objective/funnel_level) appears fine under `ORDER BY version DESC` in practice, but the **audience_segment** archive is the one that bites — when in doubt, order both by `create_time`. The earlier AUDI-1070 finding "Avon flagship audience is pure stable DS13, no DS19, unchanged across years" was an artifact of this trap; the corrected reading shows DS13→DS19 (MNTN Matched) added 2024-10-17 and RTC conquest scoring turned on 2025-09-29.

## Avon / DSO budget pacing — "% to cap" vs "% of nominal" reconciliation (AUDI-1070, 2026-06-30)

The Tableau **Over/Under Spend Monitoring → Daily Pacing Report**, column **"CGID Daily % to Cap"** = daily **media_cost ÷ DSO daily budget cap**, at campaign-group grain (Tofer, 2026-06-30).
- **DSO daily cap** = `bronze.integrationprod.dso_campaign_group_daily_budgets.budget`, latest per CG by `update_time`. **This table retains only the CURRENT row** (rewritten ~daily) — no reliable historical daily-cap; for history use `dso_campaign_group_flight_budgets` / `core_flights`.
- **Numerator** = `summarydata.sum_by_campaign_group_by_day.media_cost` (NOT media_spend; "% to cap" is in media-cost terms).
- **Active CGs only.** Paused groups keep a stale tiny daily cap ($8–38) with **0 delivery**; summing them into the denominator drags the ratio into the "40–60%" range — likely the cause of a wrong manual calc.

**The 99% vs ~40% reconciliation (both are true, different denominators):** for `dso_manage_budget=TRUE` groups DSO auto-sets the operative budget **well below nominal**. Avon (2026-06):
- CG **69271 "CTV Prospecting 2026"**: nominal `campaign_groups.budget` **$20,455** → DSO flight budget **$7,747 (38% of nominal)**, daily cap **$263**; spent ~$7,791 = **~100% of DSO budget, ~38% of nominal**.
- CG **69273 "CTV Retargeting 2026"**: nominal **$6,818** → DSO flight **$2,711 (40%)**, daily cap **$91**; spent ~$2,723 = **~100% of DSO, ~40% of nominal**.
⇒ "Avon is hitting budget" (≈99% to DSO cap) and "Avon delivers ~40% of budget" (% of nominal) are the SAME fact at two denominators. DSO throttles the operative budget to ~38–40% of nominal (deliverability/efficiency-constrained) and Avon then fills ~100% of that. Query: `tickets/audi_1070_.../queries/audi_1070_avon_budget_pacing.sql`.

## Avon (31921) reporting attribution FLIPPED last_touch -> first_touch between 2025 and 2026 (AUDI-1070, 2026-06-30)

`bronze.integrationprod.archives_advertiser_setting_archives.reporting_style` resolved **as-of**:
- **2025-03-15 (Jan-May 2025): `last_touch`**
- **2026-03-15 (Jan-May 2026): `industry_standard` (= FIRST TOUCH)** — also the current/latest value.

So Avon's client-facing lens was **Last-Touch in 2025 and First-Touch in 2026**. (The raw archive oscillates LT<->FT dozens of times incl. many same-day flips — a noisy/unstable CDC signal, and `version` is non-monotonic — so trust the resolved as-of values, not a single "switch date.") Implication for the AUDI-1070 client "decline": the MNTN UI honors `reporting_style`, so a client YoY of 2025 (LT) vs 2026 (FT) **mixes lenses**. First-Touch mechanically reports **lower** headline ROAS/visits for prospecting-heavy CTV because it credits the top-of-funnel prospecting impression (Avon prospecting campaign 259556 ROAS ~9x) instead of the cheap high-ROAS retargeting (~90-120x) that Last-Touch credits — so 2026-FT looks worse than 2025-LT even with identical real performance. Our BQ analysis is `sum_by_*` = **last-touch for BOTH years** (clean consistent lens) → no decline. **FT is NOT cleanly reconstructable in BQ:** `clickpass_log.first_touch_ad_served_id` is only ~28-36% populated for Avon (and `first_touch_time` is garbage); authoritative FT lives in the separate reporting engine. To reconcile the client view, re-pull BOTH years on ONE lens (ideally FT-both, re-attributing 2025). Visit COUNT itself is lens-free (clickpass = 1 row/visit) so the -16% visit drop is real in any lens and is spend-driven (-12% spend, +5% CPM).

### Audience-intent scoring tiers — HI / PP / Mid / MaxReach (Confluence TAR/3487891474, verified AUDI-1070 2026-06-30)
The `audience_intent` DAG (airflow-ti `dags/audience_intent/audience_intent.py`, daily ~3–7 AM UTC) writes two score sets to `gs://household-scoring-prod/output/scoring/{prospecting_intent, advertiser_intent}`. These are the `household_score` values seen in `cost_impression_log`.
- **Prospecting scores** — per `(ip, advertiser, campaign_group, campaign)`; **only for campaigns whose audience expression has DS13 or DS19**. Bands (EXACT, discrete):
  - **High Intent (HI) = 10,000** — IP in Vertical (**DS13**) **AND** in the campaign's Keywords (**DS19**).
  - **Peak Performance (PP) = 8,000** — IP in Vertical but **NOT** in Keywords. (Lower intent than HI — the keyword match is what separates them.)
  - **Mid Intent = 3,333–6,665** — in the mid-intent vertical bucket (DS13 "type 0"), engagement-ranked (page-views + recency → higher; no-engagement IPs get a random score in the lower part).
  - **Unscored / "Max Reach" = ≤0 / null** — outside bucket+vertical.
- **Advertiser scores** — per `(ip, advertiser_id)`, all advertisers, no campaign/expression filter. **HI = 10,000 (Vertical only); NO PP** (no keyword split); Mid = 3,333–6,665 (engagement-only tier, drops the no-engagement IPs). Scores are ~identical for advertisers in the same vertical.
- **Bidder priority order:** RTC (real-time conquest) score FIRST → else campaign-level prospecting score → else advertiser-level score → else unscored.
- **CIL fields:** `household_score` = the graduated prospecting/advertiser intent (10k HI / 8k PP / Mid / MaxReach). `advertiser_household_score` = the advertiser/RTC binary-ish (10,000 or unscored). **To split HI vs PP you MUST band `household_score` exactly (=10000 vs =8000); `>=6666` lumps HI+PP together.**
- **Application (HexClad AUDI-1070):** delivery falling out of HI (10k) into **PP (8k)** = the campaign exhausted the vertical∩keyword pool and the bidder served vertical-only IPs. HexClad prospecting PP share: **~0% (2025 Jun–Oct) → 25–34% (2026 Mar–May)** — the mechanism behind its visit-rate/ROAS collapse.

### Fangorn (continuous) vs non-Fangorn (bucketed) scoring + May 1 2026 changes (Confluence TAR/3584360466, verified AUDI-1070)
**Two scoring regimes coexist** (distinguishable in `cost_impression_log.household_score`):
- **Non-Fangorn = BUCKETED (discrete):** High = **exactly 10000**, Peak = **exactly 8000**, Mid = 3333–6665 (engagement continuous), Max Reach = 1–3332. Platform score DISTRIBUTION (May 2026 Confluence histogram — these are per-(IP × advertiser × campaign) **scoring ROWS, NOT unique IPs**; an IP is counted once per campaign it's scored against): High(10k) 14.0B rows vs Peak(8k) 33.0B rows → **~2.4× more prospecting scores land in PP than HI**, because HI requires vertical AND keyword (narrow) while PP needs only vertical (broad). So HI is the scarce tier for any campaign; scaling spend past a campaign's finite HI pool spills delivery into PP. (Do NOT read 14B/33B as IP counts — there are only ~4.3B IPv4 addresses.)
- **Fangorn = CONTINUOUS (XGBoost, TI-863):** High = **8001–10000 spread**, Peak = **6666–7900 spread**, Mid = 3333–6665, Max Reach = 1–3332. **Diagnostic: if High scores are spread across 8001–9999 (not concentrated at exactly 10000), the advertiser is on Fangorn; if ~all High = exactly 10000, it's bucketed.**
- **May 1 2026 release (PR airflow-ti #992):** (a) Fangorn continuous scoring for **3 launch advertisers only** (rollout gated by a control table); (b) **Max Reach re-scored** = random 1–3332 (was unscored, TI-911); (c) **Stage-2 campaigns scored like Stage-1** (TI-915).
- **HexClad (34611) is BUCKETED, NOT Fangorn** (0% of High scores in 8001–9999 through May 2026) despite DS46 in its expression — so the May 1 Fangorn change does not affect it. Its decline = the bucketed HI(10k)→PP(8k) fallback from scaling. Open Q: is DS46-in-expression-without-Fangorn-scoring intended (RTC ref) or a pending-rollout state?

## HHST intent gate is a PACING LEVER — thrashed daily, drives delivery composition overnight (AUDI-1070)
The Household Score Threshold (HHST) is the min household_score the bidder will serve for a campaign. It is NOT a static setting — for actively-managed advertisers it is changed constantly (HexClad prospecting: **51 changes Jan-May 2026**, range -1 to 10000) by pacing/optimization logic (and/or trading). **Each gate change inverts delivery the NEXT DAY**: set gate=10000 → ~100% High-Intent delivery overnight; set gate=0/-1 → ~12% HI, rest unscored/low, and daily impressions jump (no gate = more inventory). This is the mechanism behind STEEP, RAPID MoM/daily performance swings — they are config flips, not gradual audience decline or model degradation. When the gate is tight, MM still delivers 100% HI and performs fine.
- **Signature of "spend outran HI supply":** pacing RAMPS — gate stepped up ~+300/day (3333→3600→…→6666) then reset to 0. A controller opening the gate to find fill for a budget the HI pool can't absorb.
- **Source:** `silver.archives.household_score_threshold_archives` (= bronze integrationprod archive) — cols advertiser_id, campaign_group_id, campaign_id, threshold, update_time. Build a change-timeline with LAG(threshold) OVER (PARTITION BY campaign_id ORDER BY update_time) and keep rows where threshold != prev. Current live values: `silver.dso.household_score_thresholds`.
- **Analysis lesson:** ANY YoY/MoM advertiser KPI comparison must check the HHST gate history — a "decline" can be the time-average of a gate held loose (chasing spend) punctuated by brief HI-only windows. threshold=0 means NO gate (serve anyone), NOT "gate at zero score." Negative thresholds (-1, -100) also = no gate.

## Fangorn vs bucketed scoring — empirical detector (which advertisers are on Fangorn, and when they migrated) (AUDI-1070)
Fangorn = continuous XGBoost scores; bucketed = discrete. Detector on `logdata.cost_impression_log.household_score` (COALESCE with regex-parse of model_params for pre-2026):
- **Bucketed:** HI = exactly 10000, PP = exactly 8000, Mid = 3333-6665 range, nothing in the 8001-9999 or 6666-7999 bands.
- **Fangorn:** High spread **8001-9999** (continuous), Peak **6666-7899** — these bands become densely populated (100s-1000s of distinct values).
- **Test:** `ROUND(100*COUNTIF(hs BETWEEN 8001 AND 9999)/COUNT(*),2)` + `COUNT(DISTINCT IF(hs BETWEEN 8001 AND 9999, hs, NULL))`. ~0% + 0 distinct = bucketed; non-trivial % + many distinct = Fangorn.
- **Rolling migration:** the platform Fangorn rollout date (~May 1 2026) is NOT when a given advertiser flips — advertisers migrate on a rolling schedule. **HexClad flipped Jun 4-5, 2026** (day-level: 0% continuous through Jun 3 → 22.9% Jun 4 partial → fully migrated Jun 5, exactly-10000 delivery goes to 0%). Always verify per-advertiser migration date from CIL before attributing anything to Fangorn; don't assume the platform date.

## RTC (Real-Time Conquest) BYPASSES the HHST intent gate — a legitimate "non-HI under a high gate" source (AUDI-1070)
When you see non-HI delivery under a genuine HHST=10000 gate, the FIRST thing to rule out is RTC. RTC is a separate, higher-priority serving path that fires BEFORE the household-score gate and serves competitor-conquest households regardless of intent score. Detect with model_params LIKE "%realtime_conquest_score=10000%".
- Verified (HexClad Jan 2026, gate=10000 window): split by path — **normal gated path = 92% of imps, 99.99% household_score=10000** (gate binds ~perfectly); **RTC path = 8% of imps, 35% HI / 23% PP / 16% MI / 26% unscored** (mixed by design).
- So "% HI" swings depending on whether you include RTC. The daily HI-share queries that show ~100% HI under a 10000 gate EXCLUDE RTC (model_params NOT LIKE realtime_conquest). Including RTC drops it to ~95%.
- RTC was ABSENT for HexClad/Caraway/Avon in 2025 (realtime_conquest_score=-1) but ACTIVE for HexClad in Jan 2026 (~8% of prospecting imps) — RTC turning on is itself a 2026 change to watch.
- The gate binds on **household_score**, NOT advertiser_household_score. The two fields diverge ~10% in BOTH directions (advertiser_household_score logs ~3500 for ~10% of genuine hs=10000 imps; and ~36% of hs<10000 imps have ahs=10000). ALWAYS use household_score for gate/HI-status reasoning; advertiser_household_score will misclassify ~10% of true HI. Related [[reference_hhst_pacing_lever]].

## CORRECTION: MaxReach scoring was NOT globally turned off ~Nov 19 2025
Earlier TI-896-era context claimed MaxReach scoring turned off platform-wide ~Nov 19 2025. AUDI-1070 adversarial re-check REFUTES this (for HexClad): MaxReach (household_score 1-3332) was still 42% of delivery on Nov 19; it hit 0 on Nov 24-25 ONLY because the gate was set to 3334 (Mid floor mechanically excludes hs<3334), then REAPPEARED Nov 26-27 when the gate reverted to -1. MaxReach's late-Nov absence was a reversible GATE-FLOOR artifact, not a scoring shutoff. Do not cite "MaxReach off Nov 19" as a scoring-availability change.

## campaign_group_id = the CLIENT-facing campaign; campaign_id = internal funnel-stage sub-campaigns (AUDI-1070)
To the client/UI, "the campaign" is the **campaign_group_id**. Each group fans out into internal **campaign_id** sub-campaigns, one per funnel stage: obj=1 funnel=1 S1 Prospecting, obj=5/6 S2/S3 Multi-Touch, obj=7 Ego, obj=4 Retargeting. So per-campaign_id analysis (e.g. gate/HHST, which is set per campaign_id) rolls UP to the client campaign at the group level. When presenting to clients/stakeholders, aggregate to campaign_group_id; use campaign_id only for internal stage/gate mechanics.
- Group NAMES carry strategic intent (query `bronze.integrationprod.campaign_groups.name`, filter deleted=FALSE; note the table is full of test/archived junk — `test - *`, `* Copy 0X`, `archived`/`paused`). HexClad example: 93373 "CTV Prospecting High-Intent" ($2.73M flagship), 100739/100744 "Cell A BAU"/"Cell B Scale Up" (an Oct-2025 A/B scale-up test — Cell B = the spend that outran the HI pool), 111708 "CTV Prospecting - General Interest" (new Mar-2026 campaign = deliberate broadening beyond HI), 56957 "CTV Retargeting" ($614K, separate/healthy).
- Lesson: read the group names before analyzing — they often reveal the experiment/strategy (scale-up test, GI expansion) behind a spend or composition change.

## Short flights (<72 hours) push a 0 HHST gate — a driver of "gate thrash" (AUDI-1070; Tofer/PEX + quantified 2026-07-01)
Ops practice: a flight run under ~72 hours tends to get its **HHST gate set to 0 (max-reach / no intent gate) for deliverability** — you can't reliably pace spend against an intent-gated pool in <3 days. So an advertiser who adds spend in **short 1–3 day bursts** repeatedly runs ungated. **IMPORTANT: this is a TENDENCY, not a rule** (it's Tofer's manual practice — he misses some; see [[reference_hhst_pacing_lever]]).
- **Quantified (HexClad flagship group 93373, 73 active flights over Jul-2025→Jun-2026): short flights (≤72h) run ungated 45% of their days vs 28% for long flights (>72h); 14/31 short flights are FULLY ungated vs 9/42 long.** A real, material tendency — but far from 1:1.
- **CORRECTION (per Johnny's pushback 2026-07-01):** the gate is **THRASHED, not "forgotten / left at 0."** HexClad's flagship (446801) had **66 gate changes** — removed mid-Nov (holiday), RESTORED to 10000 Jan 5 (delivery recovered to ~80% HI), off again Feb 5, oscillating since. The earlier "manual Nov change then forgotten at 0" framing was wrong. What stacks: (a) deliberate holiday removal + big **mega-flights** ($112k/$165k/**$409k**/$180k budgets Nov-Jan) running ungated while spend hit ~$100k/day; (b) short-flight auto-0 churn; (c) Fangorn-era continuous ramps (3333→6666) interrupted by resets to 0.
- **How to SYNC gate + spend + flights (the diagnostic chart, AUDI-1070):** overlay on one time axis — (1) daily HHST from `silver.archives.household_score_threshold_archives` (QUALIFY row_number by campaign_id×day, eod threshold); (2) daily spend from `silver.summarydata.sum_by_campaign_by_day`; (3) flight boundaries from `silver.core.flights` (join campaign_group_id via `campaigns`; `start_time`, `end_time`, `TIMESTAMP_DIFF(...,HOUR)` = duration, `budget`, `status_id`=3 active). Mark short flights (≤72h) and check whether gate-off aligns with flight starts / spend spikes. Chart: `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/artifacts/hexclad_gate_spend_flights.png`; script `..._chart.py`; data `outputs/hexclad_flights.csv`, `_446801_daily_spend.csv`, `_gate_history_daily.csv`.
- Implication: a 0-gate is often NOT an MNTN model/audience problem — it's CLIENT campaign-management (short-flight bursts + holiday scale-ups). **PEX fix: run flights ≥72h so the gate stays engaged, and HOLD the gate through holiday scale-ups.** Related [[reference_hhst_pacing_lever]].

## within-HI visit rate is THE discriminator between gate-removal and over-scaling (AUDI-1070, 5-advertiser, 2026-07-02)
When a prospecting advertiser's visit rate / ROAS falls YoY, there are two finite-HI-pool failure modes that look similar on aggregate numbers but need OPPOSITE fixes. The decisive test is **within-HI visit rate** = (HI visits ÷ HI-served-impressions), measured monthly as spend scales (household_score>=8001; RTC-excluded-if-present; scores from 2025-05-06):
- **GATE-REMOVAL / THRASH:** served HI-share collapses, but **within-HI VR HOLDS or RISES** → the served-HI pool is HEALTHY, delivery just LEFT it. Fix = restore & hold the gate. (HexClad, The Bouqs, Kindred, Avon-before-recovery.) The Bouqs: HI-share 55%→4% while within-HI VR ROSE 0.30%→2.40%. Kindred: +65% spend but within-HI VR held ~0.7–1.1% (Pearson r(spend,VR) POSITIVE).
- **OVER-SCALING / SATURATION:** HI-share HOLDS (stays gated) but **within-HI VR FALLS with spend** → the finite HI pool is exhausted. Fix = pace spend / widen the pool. (Caraway: stayed 82–99% HI, within-HI VR −69% at +191% spend.)
- **TALLY (AUDI-1070, n=5): 4 of 5 declines are the GATE (HexClad, Avon, Bouqs, Kindred); only Caraway is true over-scaling.** So "MM is degrading" is false on all 5 — the dominant lever is the HHST gate (config/campaign-management), not the audience model. Even a hard-scaling advertiser (Kindred +65%) proved to be gate, not saturation, once within-HI VR was checked — DON'T assume high-spend-growth = over-scaling; verify with within-HI VR.
- **GOTCHA — the gate controls COMPOSITION, not blended VR:** at monthly grain, HI-share and overall VR do NOT necessarily co-move (The Bouqs monthly corr = −0.45). Do not claim "every fallen metric is downstream of the gate." The decisive gate evidence is COMPOSITIONAL (HI-share tracks the gate) + within-HI VR healthy (pool fine). Score is BINARY (~all 10000) → avg score is blind; within-HI VR is the only lens that separates the modes.
- **PORTFOLIO/MIX trap:** MT2/MT3 (obj=5/6) companion campaigns are **unscored BY DESIGN** (funnel-stage, not gate targets). They inflate the "% unscored" headline — e.g. The Bouqs May-2026 "71% unscored" was ~47% MT2/MT3-by-design; only the obj=1 stage-1 ungated share (595017 = 49%) is a real gate to fix. Always split obj=1 stage-1 (gateable) from obj=5/6 MT (unscored by design) before attributing "unscored" to a gate. Do NOT recommend gating the MT campaigns.
- **NEW-CAMPAIGN RAMP on re-gate:** when a gate is restored, HI-share (composition) recovers OVERNIGHT, but within-HI VR (performance) ramps ~4 weeks (TI-780). Present a re-gate as composition-proof, not performance-overnight-proof.
Reusable pack + flowchart: `documentation/docs/advertiser_yoy_diagnostic/`. Memory [[reference_within_hi_vr_discriminator]]. Related [[reference_hhst_pacing_lever]].

## Vertical reclassification (TI-33 / AUDI-33, TGT-4018/4019) resizes every vertical's IP set — a DEFINITION change, not an assignment change
MNTN redeployed the domain→vertical classifier to PROD **2025-07-14** (rollout 07-21/22): each domain → ChatGPT description → embedding/vectorizer → semantic-similarity match to a MNTN vertical, PLUS non-ecommerce URL filtering (TGT-4019 'quality' — e.g. ISP domains like yahoo.com blacklisted).
**IPs inherit their verticals from the domains they visit.** Redrawing domain→vertical membership therefore redraws each vertical's IP set: a single vertical can grow +50%+ while churning ~14% of its old IPs OUT. Two distinct things — don't conflate:
- **Definition change** (which IPs count as a vertical) — what this reclassification does; changes constantly at the platform level.
- **Assignment** (which vertical an advertiser targets, `fpa.advertiser_verticals` type=1) — stable long-term.
A vertical GROWING is not itself an HI-decline cause. But a broader ChatGPT/ecommerce net tends to add **vertical-ONLY** IPs, which score PP (8000, vertical-only) not HI (10000, vertical∩keyword) — the pool can grow 'in the wrong tier' (PP balloons, keyword-matched HI core stays limited). Source of truth for IP↔vertical is `prod.ml.ip_vertical_associations` (dt-partitioned; measure a reclassification's per-vertical churn/growth by diffing prod(old) vs dev(new) partitions). Note: an advertiser's vertical id can equal its RTC-model / DS46 id (same integer).

## HI pool is a FLOW, not a stock — the live ~30d pool is ~half the lifetime figure, and you pace against the flow (AUDI-1070)
The cumulative lifetime distinct-HI count is NOT the biddable pool. The **live pool = distinct HI IPs served in the trailing ~30 days** (HI has roughly a 30-day refresh/TTL); it peaks well below the lifetime nominal (~half) and never approaches it. Sizing HI spend against the lifetime figure massively overstates capacity.
- **HI replacement/inflow is roughly constant** (new-HI IPs/day, stable over months). This INFLOW rate — not the stock — sets sustainable HI spend.
- **Rising HI frequency (imps ÷ distinct) is the LEADING tightening signal.** When fresh HI tightens, the bidder RAISES frequency on existing HI (re-serving, up to the cap) BEFORE dropping to Mid. A simultaneous frequency↑ AND reach-per-$↓ in the same period is the genuine supply-tightening fingerprint.
- **Stage-1 prospecting (obj=1) RE-SERVES the same HI IP** (frequency ~1.3–2.4x/month) — distinct from Stage-3 / obj=4 site-visitor retargeting. Don't read prospecting frequency as retargeting.
HI is structurally scarce vs PP because HI = vertical(DS13)∩keyword(DS19) (narrow intersection) while PP = vertical-only (broad); platform histograms show ~2.4x more PP(8000) than HI(10000). Binding constraint at scale is HI SUPPLY, not budget — at constant spend, HI-share can swing 2–3x within a period. All CIL-derived pool figures are LOWER bounds (served IPs only); distinct-IP overcounts households (CGNAT/DHCP churn) → true household-level tightening is EARLIER and SHARPER than the distinct-IP series suggests.

## RTC is a distinct conquest population, not 'fast HI' — ~47% of RTC IPs never reach HI (AUDI-1070)
Only ~53% of RTC (Real-Time Conquest) IPs ever reach `household_score`=10000; ~47% never do. RTC serves competitor-conquest households regardless of intent score, so its composition is genuinely mixed (HI/PP/Mid/unscored) BY DESIGN — not a leak and not a fast path into the HI tier. Treat RTC-served IPs as their own population; do not fold them into HI-supply, pool-inflow, or pacing math (exclude on `realtime_conquest_score`>0). See also the RTC HHST-bypass section.

## CIL household-score logging has a hard floor — no score history before 2025-05-06 (AUDI-1070)
Household scores were only WRITTEN into `cost_impression_log` starting on a clean, platform-wide overnight cutover: the **typed columns** (`household_score`, `advertiser_household_score`) are 100% NULL before **2025-06-01** and ~0% NULL after; the scores are recoverable one cutover earlier — they first appear in the `model_params` STRING on **2025-05-06**. So the recoverable CIL score floor is 2025-05-06. This is a LOGGING change, not a scoring-pipeline onset (the bidder scored households earlier; CIL just didn't carry the columns). Consequence: any 'score distribution / scored-fraction / % under 8000 over time' question is un-answerable from CIL before 2025-05-06 — a pre-2025 baseline needs the Measurement/scoring team's GCS pull, not BQ. Recovery: `COALESCE(household_score, SAFE_CAST(REGEXP_EXTRACT(model_params, r'household_score=(-?\d+)') AS INT64))`.

## household_score is BINARY (10000 or unscored) and BLIND to within-HI quality — the score can't see a VR collapse (AUDI-1070 Caraway)
For non-Fangorn (bucketed) advertisers, `household_score` is effectively a binary flag: ~99-100% of SCORED prospecting impressions are EXACTLY 10000. So the **average scored-only household_score sits pinned at ~10,000 every gated month regardless of realized performance.** AUDI-1070 Caraway: avg scored-only score = 9,995 (Aug'25, VR 0.13%) and 10,000 (Mar'26, VR 0.15%) vs 9,968 (Jul'25, VR 0.37%) — the score is FLAT-MAXED while visit rate HALVED. Consequence: a within-HI quality collapse (over-scaling into marginal HI households that score 10000 but convert far worse) is **invisible to any score-based metric** — score dashboards show "nothing degraded" while VR craters, and the decline "never recovers" from the scoring view. Diagnostic rule: to see within-HI degradation, use realized VR by score-band + reach/frequency, NOT the average score. Two useful cuts: avg(IF(hs>0,hs,0)) tracks HI-share/composition; avg(IF(hs>0,hs,NULL)) is the scored-only quality (flat-maxed for bucketed). This binary-score blindness is exactly what **continuous scoring (Fangorn, DS46, 8001-9999) fixes** — it grades within HI so the bidder can prioritize the best HI. Related [[reference_hhst_pacing_lever]], the HI-pool-is-a-flow section.

## Two failure modes of a prospecting "decline" — gate-removal vs over-scaling (both = the finite-HI-supply ceiling) (AUDI-1070)
Addressable High-Intent is finite. A prospecting advertiser whose visits/ROAS decline YoY at higher spend is almost always one of two modes (diagnose which via the reusable pack in `documentation/docs/advertiser_yoy_diagnostic/`):
1. **GATE-REMOVAL (config) — delivery LEAVES HI.** The HHST gate is set to 0/max-reach (short-flight manual HHST=0, a deliberate change, and/or forgotten) → HI-share collapses (e.g. HexClad flagship 97.8%→31% HI, 34% unscored). Signature: HI-share drops in the score composition; gate-timeline shows the gate went to 0/-1 and stayed. Fix: restore the gate; run flights ≥72h; stop leaving it off.
2. **OVER-SCALING (pacing) — delivery STAYS in HI but overwhelms it.** The gate holds (HI-share stays 85–99%) but spend is scaled 2–3× past the sustainable HI supply → within-HI VR collapses (e.g. Caraway held ~85–99% HI yet VR −69% at +191% spend; Mar'26 99.9% HI / 0.15% VR vs Jul'25 99% HI / 0.37% VR — same HI-share, half the VR). Signature: HI-share HOLDS but VR falls; cumulative distinct-HI reach crosses the pool and brand-new share of reach falls below ~50% ("running on refresh"); HI frequency flat while reach grows (deeper into the pool, to weaker marginal HI). **Invisible to the binary household_score** (all 10000). Fix: pace HI spend to the sustainable rate; widen the pool (keywords); continuous scoring (Fangorn) to grade within HI.
KEY TELL to separate them: does HI-share DROP (mode 1) or HOLD while VR drops (mode 2)? Avon shows neither at its low flat spend (healthy control). Reusable diagnostic: `documentation/docs/advertiser_yoy_diagnostic/` (7 parameterized queries + playbook + `run_diagnostic.sh <AID> <win> <p1> <p2>`).

## CORRECTION (2026-07-01, Lilit/Measurement): "industry_standard = first-touch" is a MISNOMER — it's last-touch + competing_*
Prod Ops (Johnny) loosely called reporting_style `industry_standard` "first touch." **Measurement (Lilit) is authoritative: MNTN conversions use LAST-TOUCH or LAST-TV-TOUCH logic only — there is NO first-touch conversion table.** Confirmed in `silver.summarydata.all_facts`: the conversion/order-value columns are `last_touch_*`, `last_tv_touch_*`, `competing_*` (with sub-variants incl. `competing_last_touch_*` — so "competing" is ORTHOGONAL to touch-order, NOT first-touch), `probattr_*`, `*_assist_*` — **no `first_touch` column exists.**
- **What `industry_standard`/"new" reporting actually is:** last-touch conversions **+ the `competing_*` columns** (competitive-scenario / more-inclusive credit; exact definition is a Measurement/Compass question — confirm before repeating). This is what makes the client UI ROAS higher than a naive last-touch BQ pull, NOT a first-touch re-attribution. Avon prospecting: competing adds ~19% conversions / ~28% order-value on top of last-touch (LT ROAS 17.3 → industry_standard 22.1, reproduced to the dollar).
- **For CTV advertisers, last_touch == last_tv_touch** (every touch is a TV touch), so those columns coincide.
- **Relabel anywhere it says "first-touch (FT)" for the UI/industry_standard lens → "industry_standard = last-touch + competing_*".** The reconciliation and every YoY-decline conclusion are unchanged (both the plain-last-touch and industry_standard views decline); only the LABEL was wrong. Supersedes the earlier §5e-bis/§5g "first-touch (competing_*)" framing on the FIRST-TOUCH wording (the competing_* mechanism itself is correct). Ref: [[reference_attribution_industry_standard_ft]] (name kept, content corrected).

## Per-IP frequency: report MEDIANS, never means (CGNAT skew)
Mean imps/IP is dominated by shared/CGNAT IPs (one IP = hotel/apartment/carrier NAT = many devices).
Verified TI-1037 (Bouqs RT, Feb 2026-checked-2025 month): mean 46.8 vs **median 8** imps/IP; single worst
IP logged 8,020 imps in one month; top-100 IPs = 6.4% of all impressions from 0.09% of IPs. Use
`APPROX_QUANTILES(per_ip_imps, 100)[OFFSET(50)]` for frequency; show the mean only as parenthetical context.
Monthly frequency norms (Bouqs, Apr–May 2026): **prospecting median 1–2 / mean 1.6–1.7** (the familiar
"1–4 imps/IP"); **retargeting median 8–9 / mean 24–31, p90 66–94** — RT is inherently high-frequency
(small pool of site visitors hit repeatedly), so an RT mean of 25–47 is normal-with-skew, not a bug.
Also: non-Fangorn advertisers' household_score HI is EXACTLY 10000 — the graduated 8001–9999 band only
appears after a Fangorn flip, so `hs = 10000` vs `hs >= 8001` give identical results pre-Fangorn.

## Prospecting re-touch (recirculation) — DS16 is the only net-new gate (TI-1037, 2026-07-08)
Prospecting re-serves IPs the advertiser already touched unless **DS16 (net-new gate)** is on the campaign —
the VV/pageview (DS34) and conversion (DS21) excludes only remove visitors/converters, NOT previously-served
IPs. Clients that scale spend while holding HHST=10000 exhaust the net-new HI pool and end up re-touching up
to ~99% of previously-served IPs (per Malachi). Measure it: HI is the score ON THE BID
(each impression row's household_score = 10000 at bid time, NOT a monthly status — with HHST at 10000 the
bidder only serves an IP while it currently scores 10000); per month, split 10000-served IPs by whether their
first-ever 10000 bid predates the month ("10000 both times" — a prior touch at 8000 doesn't count). Month
grain is approximate: the 30-day score TTL doesn't align to calendar months, and within-month re-serves land
in frequency, not the new/re-touch split. Bouqs (Jan'25–May'26): re-touch share 0%→75%, dipping when new campaigns/geo open fresh pools;
cumulative distinct 10000-IPs 2.54M. CPD dashboard module 09rt.

## Pentest / scanner pollution in conversion_log revenue (WGU Feb 2026 case)

- **Third-party security scans of client sites fire the MNTN pixel with fuzzed params** and those rows land in `logdata.conversion_log` as real conversions. Canonical case: advertiser 31357 (WGU), **2026-02-07 19:38–21:52 UTC, single IP 136.60.22.42**, Burp Suite active scan (payload callbacks to `oastify.com` = Burp Collaborator) run against `https://apply.wgu.edu/duplicate` — the scanner fuzzed the outbound `px.mountain.com` pixel GET.
- Symptoms: **75 junk `conversion_type` values** (SQLi/XXE/SSTI/path-traversal strings, count=1 each, one day only) + **order_amt values fabricated by digit-extraction** from injection payloads in the `shoamt` param (e.g. `' and 6957=6964--` → $69,576,964; `sleep(20)` → $20). The pixel ingest strips non-digits and concatenates the rest into a NUMERIC amount.
- Impact chain (Feb 2026, advertiser 31357): bronze.raw 34 amt rows summing **$71.68T** → silver strips the 4 largest (\$621,621,621 / \$1.08T / \$14.7T / \$55.9T; rows kept, amt nulled — row counts identical 43,653) → silver 30 amt rows **$222,892,625.40** → attribution keeps 23 of 30 (all ≤$590,132; the 7 rows ≥$5,736,771 never appear in `ui_conversions`) → **$833,883.40** fake "Revenue" on the client dashboard via `sum_by_campaign_by_day`. Exact reconciliation: $222,892,625.40 − $222,058,742 (7 largest) = $833,883.40.
- **Silver corrupt-amount strip threshold sits between $69,576,964 (kept) and $621,621,621 (stripped)** — plausibly ≥$100M, exact rule unverified. A second, lower cap (between ~$590K and ~$5.7M) appears to exist in the summarydata conversions/attribution pipeline — mechanism unverified.
- Detection recipe: single-day burst of many distinct `conversion_type` values + `oastify.com`/injection strings + one IP → pentest, exclude from revenue. Check `TO_JSON_STRING(query)` → `shoamt` key for the raw payload.

## Conversion-pixel CONFIG registry — core_advertiser_conversion_types is AUTO-REGISTERED, data_source 23 = guid_log "MNTN Pixel" (WGU-REV, 2026-07-08)

**No MNTN table tracks advertiser-side pixel/tag changes** (edits live in their site/tag manager) — confirmed
by Kevin Cipriani 2026-07-08. Receiving-side reconstruction (conversion_log shape + this registry's
create_time) is the only detection path; dashboarded as CPD query "13 Pixel Health" + scorecard flag (TI-1037).
Where conversion config actually lives in `bronze.integrationprod` (no `conversion_sources` table exists):
- **`core_advertiser_conversion_types`** (advertiser_id, conversion_type, conversion_source_id, create_time; NO
  user_id, NO deleted col) — rows are **auto-created the instant a new `conversion_type` string first appears in
  conversion_log** (WGU `app_submitted` registry create_time 2025-09-30 22:57:04 == MIN(time) of first log event
  to the second). It records WHEN a new event fired, not WHO configured it — a new row here means the change was
  made in the CLIENT's website tag, not MNTN platform config.
- **Sentinel conversion_types**: `-100` (source -1, 93,180 advertisers) and `-101` (source 23, 7,546 advertisers)
  = "default/unnamed pixel conversion". All `-101` rows created ≥ 2025-01-10 — the platform-wide cutover of
  conversion_log.conversion_source_id NULL→23.
- **`data_sources` registry**: id **23 = name `guid_log`, display_name "MNTN Pixel", conversion_type_display_name
  "Website Event"** (created 2024-10-04, visible=false). Both -1 and 23 display to clients as "MNTN Pixel".
- `core_pixel_integrations`/`core_pixel_integration_types` = e-commerce integrations (shopify etc.; type 4 =
  "manual"); `ui_advertiser_pixel_infos` = pixel notes/conversion_point_url; `attr_advertiser_waypoints_event_mapping`
  + `attr_advertiser_selective_performance_config` = event-name classification (deleted flag on waypoints).
- `advertisers` row carries pixel flags: `populate_order_on_conversion`, `conv_pixel_opt_out`, `pixel_isolation`,
  `allow_duplicate_orders`, conversion windows.
- `core_advertisers_x_changes`+`core_changes` is a change-audit trail but can be EMPTY for an advertiser (WGU: 0 rows).

WGU (31357) specifics: revenue-drop root cause is CLIENT-SIDE tag change — old untyped $1-placeholder conversion
(type NULL, order_amt=1) last fired 2025-10-02; new `app_submitted` event (NO order_amt at all) went live
2025-09-30 22:57 (3-day overlap). No MNTN config change near the cutover (settings last touched 2025-05-15,
reporting_style=industry_standard since then; pixel_integrations untouched since 2020). **Feb 2026 "revenue"
anomaly = a PENTEST**: 74 SQL-injection/XSS/Burp-Collaborator (oastify.com) payload conversion_types registered
2026-02-07 19:38–22:27, with bogus order_amts summing $222.9M in conversion_log. WGU onboarded
"orca-integration" 2026-04-29 + Tealium CRM list mappings 2026-04-30 (offline_attribution=false) right before
the May 2026 conversion-volume spike (125,940 rows) — correlation, not verified causation.
**Deeper payload-level findings (WGU-REV, same day)** live in § "WGU (31357) revenue — it was NEVER real":
the new tag's amount param is the literal unfilled macro `shoamt=ORDER AMOUNT` (100% of fires); the LEAD
event now fires under **dead AID 10942** (legacy SteelHouse tag, ~18K/mo dark); the May spike = an untyped
LP tag that cycled off 05-16 and **resurged 06-24** (~1.8–2.5K/day, ongoing); the Feb pentest reconciles
bronze $71.7T → silver $222.9M → attributed **$833,883.40** to the dollar.

## WGU (31357) YoY comparisons are confounded by two 2025 tracking breaks (TI-1037, 2026-07-08)
Anyone comparing WGU across mid-2025 must know: (1) **Oct 2025 conversion-pixel change** — conversions
jumped 2,556→8,707/mo (3.4x overnight at flat spend) and order values stopped; before Oct'25 WGU passed
exactly **$1 order value per lead** (revenue == conversion count), after it $0 — so YoY "revenue −100% /
conversions +252%" is the pixel, not performance. (2) **Jul 2025 visit-tracking step** — visits 934k→1.65M
MoM at flat spend AND flat impressions; IVR re-based from ~1.2% to ~2.2% permanently. VV lookback ruled out
(PRO = 14d unchanged since 2020). Treat pre-Jul'25 visit levels and pre-Oct'25 conversion/revenue levels as
different measurement regimes.
Also (TI-1037, 2026-07-08): **WGU runs UNGATED** — its 5 core prospecting campaigns spent every delivering
day Jan'25–Jun'26 at HHST≤0 (905 campaign-days per half; the "905 of 925" scorecard row = 5 camps × 181d
plus new campaign **127483, WGU's first-ever gated campaign** — 20 gated days by Jun'26). And a third
tag-scope step: **May'26 pixel fires 2.8x** (44.7k→125.9k/mo) on top of the Jul'25/Oct'25 breaks.
