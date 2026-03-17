# TI-650: Stage 3 VV Audit — Session Prompt

## Context

Read `tickets/ti_650_stage_3_vv_audit/summary.md` for full context. This is the Stage 3 VV IP Lineage audit — building a production-grade audit table (`vv_ip_lineage`) that traces every verified visit back to its Stage 1 first-touch impression via IP address.

## Current State (as of 2026-03-12)

### Architecture: v14 — campaign_group_id scoped
- **2 cross-stage links:** `imp_direct` (bid_ip → vast_ip) + `imp_visit` (impression_ip → vast_ip)
- **Full S3→S2→S1 chain** — all pools scoped by `campaign_group_id` (not advertiser_id)
- **S2 resolution:** 97.95–99.87% across 10 advertisers (unchanged from v13)
- **S3 resolution:** 58.56–96.56% with campaign_group_id scoping (down ~5pp from v13)
- Key query: `queries/ti_650_resolution_rate_v14.sql`

### v14 vs v13 Impact (campaign_group_id scoping)
```
Adv 37775 S3:
  v13 (advertiser_id): 23,214 resolved (97.36%), 540 unresolved
  v14 (campaign_group_id): 21,931 resolved (91.98%), 1,761 unresolved
  Delta: -1,283 VVs (-5.38pp) — cross-group IP coincidence removed

Multi-advertiser S3 impact:
  Large drop (>5pp): 31357 (-11.92), 34835 (-13.47), 36743 (-5.06), 37775 (-5.38), 38710 (-6.38)
  Minimal drop (<1pp): 31276, 32766, 35237, 42097, 46104
```

### v14 Waterfall (adv 37775 S3)
```
23,844 total S3 VVs
  → 22,770 have CIL (95.5%)     | 1,074 no CIL (4.5%)
  → 21,009 IP-resolved (92.3%)  |   922 impression_ip resolved (85.8%)
  →  1,761 unresolved (7.7%)    |   152 no resolution path

Total resolved: 21,931 (91.98%)
Total unresolved: 1,913 (8.02%)
  - 980/1,761 cross-device (55.6%)
  - GUID bridge: [pending — query executing at session end]
```

### Investigation Status
- ✅ campaign_group_id uniqueness verified (only `0` shared across 3 advertisers)
- ✅ v14 query written and validated across 10 advertisers
- ✅ Unresolved profiled by campaign_group: all groups have S1 campaigns (has_s1 = 1)
- ✅ event_log bid_ip fallback tested: 0/1,074 no-CIL have bid events — NOT recoverable
- ⏳ GUID bridge on 1,761 unresolved: query was executing at session end (~50+ min runtime)

## What Needs to Be Done

### 1. Complete GUID bridge on v14 unresolved (check if results arrived)
- Query: `queries/ti_650_v14_guid_bridge.sql`
- BQ job ID: `perf_20260312_113314_60571`
- If completed, update waterfall with results
- If failed/timed out, re-run

### 2. Update SQLMesh model to v14 architecture
- Current model: `queries/ti_650_sqlmesh_model.sql` (v10.1 — very outdated)
- Needs: v14 chain architecture + campaign_group_id scoping
- Implementation plan: `artifacts/ti_650_implementation_plan.md`
- Decisions needed from dplat: dataset name, staging layer, slot allocation

### 3. Decide on GUID bridge in production
- With campaign_group_id scoping, more VVs are unresolved (1,761 vs 540)
- GUID bridge becomes more important but is very expensive (~50+ min for single advertiser)
- May be better as a separate batch/daily process rather than hourly

### 4. Retargeting scope decision (ask Zach)
- Should audit trace to "first prospecting touch" or "first MNTN touch of any kind"?
- With campaign_group_id scoping, retargeting campaigns in the same group would naturally be included if objective_id filter is loosened

## Scoping Rules (validated)
- **campaign_group_id scoping (v14+).** All cross-stage IP linking must be within same campaign_group_id.
- **campaign_group_id is unique across advertisers** (verified). No need to also join on advertiser_id.
- **funnel_level is authoritative for stage** (NOT objective_id — 48K S3 campaigns have obj=1)
- **Prospecting VVs:** `objective_id IN (1, 5, 6)` at the VV level
- **S1 pool:** scope by campaign_group_id + funnel_level = 1
- **90-day lookback.** Zach confirmed max = 88 days (14+30+14+30)

## Key Files

| File | Purpose |
|------|---------|
| `summary.md` | Full ticket context, all findings, architecture |
| `queries/ti_650_resolution_rate_v14.sql` | **v14: campaign_group_id scoped** (multi-advertiser) |
| `queries/ti_650_resolution_rate_v13.sql` | v13 chain query (baseline comparison) |
| `queries/ti_650_v14_guid_bridge.sql` | GUID bridge on v14 unresolved (1,761 VVs) |
| `queries/ti_650_sqlmesh_model.sql` | SQLMesh model (v10.1 — needs v14 update) |
| `outputs/ti_650_v14_campaign_group_resolution.md` | v14 results across 10 advertisers |
| `outputs/ti_650_v14_resolution_waterfall.md` | v14 waterfall (pending GUID bridge) |
| `outputs/ti_650_v13_resolution_rates.md` | v13 results (baseline) |
| `outputs/ti_650_resolution_waterfall.md` | v13 waterfall for Zach |
| `artifacts/ti_650_implementation_plan.md` | SQLMesh deployment plan |
| `artifacts/ti_650_zach_ray_comments.txt` | Stakeholder directives |

## BQ Access

Authenticated as `malachi@mountain.com`. Use `bq_run.sh` wrapper.

```bash
bash .claude/scripts/bq_run.sh --ticket "TI-650" --label "description" \
  --use_legacy_sql=false --format=prettyjson --max_rows=100 --project_id=dw-main-silver \
  'SQL HERE'
```

**Note:** SQL with `--` comments must be piped via heredoc to avoid bq flag parsing.

## Safety
- LIMIT on every raw SELECT. Date filter on every log table.
- `--dry_run` before unfamiliar queries. Read-only access.
- campaign_group_id is unique across advertisers — no need to also join on advertiser_id.
