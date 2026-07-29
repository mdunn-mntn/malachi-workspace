---
name: reference_graph_usersreached_mixed_key
description: graph.usersreached / all_facts.uniques is a channel-conditional IP-OR-cookie HLL — ~2x the true served-IP count; not a per-IP reach
metadata: 
  node_type: memory
  type: reference
  originSessionId: b2e26231-0715-4211-9711-2f60a8021621
doc_type: memory
keywords: [graph_usersreached_mixed_key, graph, usersreached, mixed, key, graph.usersreached, all_facts.uniques, channel]
domain: [reference]
lifecycle: active
last_verified: 2026-06-25
---
`graph.usersreached` = `summarydata.all_facts.uniques` = `impression_facts.uniques`, defined as `HLL_COUNT.INIT(CASE WHEN channel_id = 8 OR objective_id IN (5,6) THEN l.ip ELSE l.guid END)` over the **served** `cost_impression_log` (`unlinked=FALSE AND ad_served_id IS NOT NULL`). So **CTV/video reach is counted by IP, display reach by `guid` (browser cookie)**. Cookies fan out ~2.4x per IP, so for a mixed/display advertiser `usersreached` runs ~**2x** the true distinct served IPs. WGU 30d: `usersreached` = 32.1M (14M CTV-IPs + 18.4M display-cookies) vs `count(distinct ip) from cost_impression_log` = 15.7M.

**Implication:** `usersreached` is NOT a per-IP/per-household reach. For anything needing distinct served IPs/households (MDE/power baselines — the holdout randomizes per-IP `MD5(advertiser_id:ip)`), use `count(distinct ip) from cost_impression_log` (served), NOT `graph.usersreached`. `graph.sitevisitors` (= `visit_facts.site_visitors`) IS always IP-keyed (`HLL_COUNT.INIT(ip)` from ui_visits).

**Can't fix by channel-splitting for mixed advertisers:** CTV-IP + display-IP can't be summed — ~33% of served IPs see both channels (WGU). Cross-channel-deduped served-IP needs a new always-IP array column or a CIL query. Exact in-window parity isn't graph-reachable (`impression_hour`/`day_number` live only in `ber_stg.visit_facts__base`, dropped before the graph layer). Full trace: `tickets/ber_2250_incrementality_overhaul/ti_1019_mde_calculator_advertiser_prefill/summary.md` §7g/7h/7i.

**ClickHouse reach mechanics (R2 graph), verified from chapi/airflow-reporting code 2026-06-25:** the BQ HLL++ sketch columns (`uniques`, `*_users_reached`) are **DEAD — never loaded to ClickHouse** (BQ HLL++ isn't mergeable by ClickHouse). The live path is the **`*_arr` raw-ID arrays**: CHAPI loads `uniques_arr` → ClickHouse `all_facts_local_daily.uniques_arr Array(Nullable(String))` (hourly) → MV `all_facts_by_day_mv` does `uniqArrayState(uniques_arr)` → `all_facts_local_by_day.uniques_arr AggregateFunction(uniqArrayState, ...)`. A 30-day `graph.usersreached` is `toInt64(uniqArrayMerge(uniques_arr))` over the window — an HLL **merge across days, NOT a `SUM()`**. So any new graph reach metric must emit an **`_arr` raw-ID array** (e.g. `users_reached_ip_arr = ARRAY_AGG(l.ip)`), not a BQ HLL sketch, and requires a coordinated change across 3 repos (sqlmesh model + chapi ClickHouse DDL/MV/r2-metadata + airflow-reporting CHAPI load config) plus a backfill. This is why sourcing per-IP MDE inputs from `cost_impression_log` (one small daily table) is the lighter, exact alternative.
