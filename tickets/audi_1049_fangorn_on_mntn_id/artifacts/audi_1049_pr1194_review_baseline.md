Nice work getting the whole parallel chain standing up end to end. Seven things I'd want resolved before this ships, then some smaller stuff.

## Blocking

**1. `hhdsc_geo` picks a biased representative IP → wrong geo in `hh_data`.**
`models/tpa_export/hhdsc_geo.py:63-74` uses `F.min("ip")` on a string column, so the pick is byte-lexicographic: `"73.14.200.0"` beats `"73.14.200.55"`. `_household_ips` (line 205) filters `id != '0.0.0.0'` but drops the `~ip LIKE '%.0'` filter that `hhdsc_ds_19.py:101` and `ipdsc_resolution_strategies.py` both apply, and there is no `ipdsc_icloud_ips` anti-join like `spark/data_source/populate_geo_source.py:148`. Net effect: the selection actively prefers network addresses and Private Relay POPs, the two things every sibling excludes. That's wrong lat/long/DMA shipped to MembershipDB, not missing geo. Add both filters before the `groupBy`, and consider `min(ip_num)` rather than `min(ip)`.

**2. Three scoring models are hardcoded to prod HHDSC.**
`HHDSC_BASE = "gs://mntn-data-archive-prod/hhdsc"` in `hh_vertical_high.py:13`, `hh_vertical_mid.py:14`, `hh_prospecting_keywords.py:12`, read with a raw `spark.read.parquet(*paths)` instead of `read_model`. The same PR reads the same partitions correctly via `read_model` in `tpa_hh_export.py:93` and `hhdsc_geo.py:190`. Because they never declare `hhdsc_ds_13` as an upstream, `read_location` can't redirect them — a dev run of this branch scores against production DS13. This is also why I'd want to re-check the dev validation: those three models never read the dev HHDSC output.

**3. `hh_fangorn_prospecting_scoring` dropped the empty-input guard.**
`fangorn_prospecting_scoring.py:121` on main raises `ValueError` when the lookback has no rows for the dt, and `hhdsc_ds_46.py:81` in this same PR keeps it. `hh_fangorn_prospecting_scoring.py:115-122` has no `isEmpty()` check. An empty day writes a valid empty partition, the High/Mid branches in `hh_prospecting_join` become no-ops, and the day ships with the entire Fangorn band missing and every task green.

**4. Every sensor in `hh_audience_intent` is `soft_fail=True` and nothing sets `trigger_rule`.**
`dags/audience_intent/hh_audience_intent.py:89-135`. A sensor timeout is SKIPPED, and every `all_success` descendant skips down to `hh_prospecting_join`, so the DagRun reports SUCCESS with no `hh_prospecting_intent` partition. `severity=5` means no page, and the Slack hook is `on_failure_callback` only, so nothing fires. The docstring at lines 31-32 says the opposite. Worth noting you went the other way in `hhdsc_build.py:66,79` in this same PR (`soft_fail=True` → `False`, pinned by a test) — I'd apply that here, or `trigger_rule="none_failed"` plus a watchdog.

**5. `attach_ipv4_households` drops the three things `household_resolution` exists to provide.**
`utils_model/mntn_graph_resolution.py:19-49` inner-joins and emits no `resolution_status`, so unresolved IPs vanish with no count logged anywhere. Compare `hhdsc_ds_13.py:105` in this same pipeline, which prints `coverage_metrics(resolved).show()` under "Coverage is a deliverable, not debug output". Three specific gaps against the on-main precedent:
- **Staleness.** `household_resolution.py:83` enforces `MAX_GRAPH_STALENESS_DAYS = 14`. I pulled the deployed `mntn_graph.zip`: `GraphConfig` has `max_graph_staleness_days`, but `_prepared_graph` constructs `GraphConfig(graph_version=..., as_of=...)` and never sets it, and `ids_to_households` gives you no way to pass one. So a stalled weekly build resolves silently against an arbitrarily old snapshot while `hhdsc_ds_13` raises and pages.
- **Crediting.** `guid_log_derived_household_id_vertical_id.py:206` calls `log_translation` for exactly this guid_log→IP→household resolution. None of the three new call sites credit anything, so these translations won't appear in `graph_translation_signal`.
- **Caching.** That same precedent `.persist()`es the per-IP lookup because `df` is consumed twice. `attach_ipv4_households` doesn't, so `hh_recency` scans up to 168 hourly partitions twice.

**6. `hh_vertical_mid` joins two different graph vintages and random-scores the difference.**
`hh_vertical_mid.py:255-274` reads 7 daily `hh_page_views` partitions (each resolved at its own day's `as_of`) and 1 `hh_recency` partition (the whole 7-day window resolved at the run date — your own docstring says so). The graph rebuilds weekly, so every window straddles a rebuild. A household that appears on one side but not the other fails the `page_views.isNotNull() & recency.isNotNull()` gate at line 146 and lands in the `without_activity` branch at line 183, which assigns `3333 + F.rand(seed=42) * (min_household_score - 3333)`. At IP grain that branch is safe because both sides key on the same stable `ip`. At household grain it means a household with real activity can get a random score. Pick one vintage for both jobs.

**7. DS19 is the only unguarded input to a prod export.**
`hhdsc_build.py:124` wires `run_date >> hhdsc_ds_19` with no sensor, while DS13 and DS46 are gated (and hardened to `soft_fail=False` here). `hhdsc_ds_19.py:93-95` reads the whole `shopper_graph/tpa_export/` directory and filters `dt == run_date`, so a late partition gives zero rows rather than an error, and there's no `isEmpty()` guard. `tpa_hh_export.py:150` then coalesces to `empty_cats` and ships `{"data_source_id": 19, "cats": []}` on every record into `gs://sh-dw-external-tables-prod/hh_data` with a fully green DAG.

## Non-blocking

- `hh_audience_intent.py:109` — `wait_for_ipdsc_geo` uses a 6h timeout against a producer the IP DAG guards with 18h. Match it.
- `hh_audience_intent.py:69` — `catchup=False` with a past `start_date` and no `params.dt`, so gaps are permanent with no manual refill path. `hhdsc_build` got `Param(dt)` + `get_dt` in this PR; this DAG didn't.
- Lookback windows raise only when *every* partition is missing (`load(..., optional=True)`), so 1 of 7 page-view days or 6 of 31 HHDSC days silently produces a full-looking partition scored against the wrong denominator. A coverage floor would help.
- `hhdsc_geo.py:210` takes the library default `use_shared_ids=False` while `mntn_graph_resolution.py:43` pins `True`. One PR, two shared-ID policies. (`True` matches the on-main feature-store precedent, so I think geo is the odd one out.)
- `hhdsc_geo._active_geo_version` uses `.first()` with no ordering and no guard, while `tpa_hh_export._resolve_geo_version` raises if there's more than one. Worth making the upstream as strict.
- `read_only_source/fangorn_household_14day_lookback_prod.py` is a second handle on a table `machine_learning/fangorn_household_14day_lookback.py` already owns. `hhdsc_ds_46.py:75` reads the writable one, `hh_fangorn_prospecting_scoring.py:116` the pinned one, so in one dev run they can read different builds.
- `hh_page_views.py:66` / `hh_recency.py:70` — the rewrite to a single `spark.read.parquet(*paths)` drops the siblings' per-partition try/except and flips `mergeSchema` to false. One unreadable hour now fails the whole 7-day job.
- `hh_page_views.py:135` / `hh_recency.py:107` — `source_ip_count` is a `countDistinct` persisted into daily partitions with no reader in the repo, and it isn't summable over the 7-day window like everything else here.
- `hh_prospecting_active_campaign_categories.py:20-95` — `FANGORN_CAMPAIGN_GROUPS` is a 40-id copy of `fangorn_campaign_groups` in `spark/audience_intent/prospecting_active_campaign_categories.py`. Equal today, nothing links them, no test.
- Tests: nothing references `HhVerticalMid`, `HhVerticalHigh`, `HhPageViews`, or `HhRecency`, which is where the household grain and all the scoring arithmetic live. In `test_hh_prospecting.py:238` the graph fake returns one row per ip by construction, so it asserts the mock rather than the one-household-per-IP invariant. In `test_tpa_hh_export.py:238` both fixture IPs mask to the same network under both prefixes, so swapping `F.min` for `F.max` still passes.

## Questions

1. For a 7-day window, should an event follow the household of its own day or of the run date? `hh_page_views` and `hh_recency` answer differently today and `hh_vertical_mid` joins both.
2. Was the second resolver (`utils_model/mntn_graph_resolution.py`) agreed with the AUDI-1167 owners, or should it fold into `household_resolution` before the Sept-4 gate? Right now `hh_page_views` imports `aggregate_to_household` from one and resolves with the other.
3. Where will the shadow-parity readout get resolution coverage from, given unresolved IPs are inner-joined away with no count logged?
4. Should `hh_data` geo exclude iCloud Private Relay the way `ip_data` does? As written the two exports will disagree on the same consumer's location.
5. Did the dev validation read a dev-written HHDSC partition anywhere, given `hh_vertical_high`, `hh_vertical_mid`, and `hh_prospecting_keywords` are hardcoded to the prod bucket?

## Checked and clean

- **No fan-out in the resolution join.** I pulled the deployed `mntn_graph.zip` and read it: `_best_household_lookup` ranks with `row_number()` and keeps `_rn == 1`, so `resolve_best_household=True` returns exactly one row per key. The inner join can't multiply rows.
- **Tiebreak matches the bidder.** The library orders `confidence_score DESC NULLS LAST, household_id ASC` and its docstring calls it ID-Service parity — lowest `household_id` on a tie, which is what id-service does. This is better than `household_resolution.load_graph_ids`, which takes the highest.
- **`IdType.IPV4` passed explicitly** in both `mntn_graph_resolution.py:41` and `hhdsc_geo.py:213`, so the old `IdTypeFamily.IP` → `{30,31,32}` expansion isn't in play. The shipped library has that fixed anyway (`IPV4 = 3000 = {30,32}`, `IPV6 = 3100`).
- **`use_shared_ids=True`** matches the on-main precedent in `guid_log_derived_household_id_vertical_id.py:198`, which documents it as deliberate.
- **Distinct counts are never summed** — `aggregate_to_household` is called only with `sum_cols` / `max_cols` / `set_cols`.
- **`dags/model_task_config.json` is in sync** — all 15 new compiled models present, `mntn_graph.zip` on the `python_file_uris` of exactly the four models that import it, and the read-only model correctly absent.
- **`tpa_hh_export` idempotency** — the `geo_version`-suffixed prefix, the `RuntimeError` on an unresolved version, and the audit-pointer early return hang together.
- **`hhdsc_build` hardening** — both sensors to `soft_fail=False` with a test pinning it, plus `dt` param and `get_dt` for backfills.
- **DS13/DS19 IP hygiene** — both keep the `0.0.0.0` and `%.0` filters and log `coverage_metrics` before dropping. DS19's IPv6 handling is documented with a measured figure.
