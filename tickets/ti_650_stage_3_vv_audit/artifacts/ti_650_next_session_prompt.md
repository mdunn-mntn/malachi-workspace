# TI-650: Stage 3 VV Audit — Session Prompt

## Context

Read `tickets/ti_650_stage_3_vv_audit/summary.md` for full context. This is the Stage 3 VV IP Lineage audit — building a production-grade audit table (`vv_ip_lineage`) that traces every verified visit back to its Stage 1 first-touch impression via IP address.

## Current State (as of 2026-03-12)

### Architecture: v13 validated, 10 advertisers
- **2 cross-stage links:** `imp_direct` (bid_ip → vast_ip) + `imp_visit` (impression_ip → vast_ip)
- **Full S3→S2→S1 chain** — 59.5% of S3 VVs resolve through S2, 37.9% direct to S1
- **S2 resolution:** 97.95–99.87% across 10 advertisers
- **S3 resolution:** 62.51–97.83% (chain matters for 6/10)
- Key query: `queries/ti_650_resolution_rate_v13.sql`

### Unresolved Investigation: COMPLETE
All 4 queries from the investigation plan executed. Full results in `outputs/ti_650_resolution_waterfall.md`.

**Resolution waterfall (adv 37775, Feb 4–11, 90-day lookback):**
```
23,844 total S3 VVs
  → 22,770 have CIL record (95.5%)
  →   1,074 no CIL record (pipeline gap, NOT TTL — all <30d old)

Of 22,770 with CIL:
  → 22,203 resolved via IP (v13 chain + direct, prosp-only)     97.51%
  →    +110 via retargeting in S1 pool (business decision)       +0.48%
  →    567 unresolved after all-campaigns IP pool                 2.49%
  →    484 resolved via GUID bridge (guid_identity_daily)        85.4% of 567
  →     83 TRULY IRREDUCIBLE                                     0.36% of CIL
        (10 primary attribution = 0.04%, 73 competing)
```

**Key findings from unresolved investigation:**
- 567 unresolved: 95.1% IP never in S1, 69.8% T-Mobile CGNAT, 100% GUID bridge potential
- GUID bridge: 484/567 resolved (85.4%), only 83 truly irreducible
- 1,074 no-CIL: CIL TTL hypothesis DISPROVEN — pipeline gap, recoverable via event_log bid_ip
- Primary VV resolution rate: **99.96%** (only 10 primary VVs unresolvable)

### Critical Directive: campaign_group_id Scoping
**Zach (2026-03-12):** Cross-stage IP linking MUST be within the same `campaign_group_id`. Linking a VV to an impression in a different campaign group is invalid — coincidental IP match, not funnel trace. `campaign_group_id` is unique across advertisers.

This is NOT yet implemented in the queries or model. Current queries scope by advertiser_id + funnel_level + objective_id. Production model must add campaign_group_id constraint.

## What Needs to Be Done

### 1. Enforce campaign_group_id scoping (HIGHEST PRIORITY)
- Update the v13 resolution query to scope S1/S2 pools within campaign_group_id
- Re-run resolution rates to measure impact (rates may decrease)
- Key table: `bronze.integrationprod.campaigns` has `campaign_group_id` column
- campaigns → campaign_groups via campaign_group_id. campaign_groups → advertiser_id.

### 2. Update SQLMesh model to v13 + campaign_group_id
- Current model: `queries/ti_650_sqlmesh_model.sql` (v10.1 — outdated)
- Needs: v13 chain architecture + campaign_group_id scoping + event_log bid_ip fallback
- Implementation plan: `artifacts/ti_650_implementation_plan.md`
- Decisions needed from dplat: dataset name, staging layer, slot allocation

### 3. Add event_log bid_ip fallback for no-CIL VVs
- 1,074 VVs (4.5%) have event_log records but no CIL record
- event_log bid_ip can serve as fallback when CIL is missing
- Simple: add event_log lookup in the bid_ip CTE with COALESCE(cil.ip, el.bid_ip)

### 4. Decide on GUID bridge in production
- Recovers 85.4% of IP-unresolved but very expensive (~51 min for single advertiser)
- May be better as a separate batch/daily process rather than hourly
- Or include as optional enrichment column (NULL if not computed)

### 5. Retargeting scope decision (ask Zach)
- Should audit trace to "first prospecting touch" or "first MNTN touch of any kind"?
- Adding retargeting to S1 pool resolves 110 additional VVs
- With campaign_group_id scoping, retargeting campaigns in the same group would naturally be included

## Scoping Rules (validated)
- **funnel_level is authoritative for stage** (NOT objective_id — 48K S3 campaigns have obj=1)
- **Prospecting VVs:** `objective_id IN (1, 5, 6)` at the VV level
- **S1 pool:** scope by campaign_group_id + funnel_level = 1 (objective_id filter TBD)
- **90-day lookback.** Zach confirmed max = 88 days (14+30+14+30)

## Key Files

| File | Purpose |
|------|---------|
| `summary.md` | Full ticket context, all findings, architecture |
| `queries/ti_650_resolution_rate_v13.sql` | v13 chain query (multi-advertiser) |
| `queries/ti_650_systematic_trace.sql` | Production linkage query (v12, single advertiser) |
| `queries/ti_650_sqlmesh_model.sql` | SQLMesh model (v10.1 — needs update) |
| `queries/ti_650_unresolved_567_profile.sql` | 567 unresolved profiling |
| `queries/ti_650_unresolved_567_guid_bridge.sql` | GUID bridge resolution |
| `queries/ti_650_no_cil_profile.sql` | No-CIL characterization |
| `outputs/ti_650_resolution_waterfall.md` | Full resolution waterfall for Zach |
| `outputs/ti_650_v13_resolution_rates.md` | v13 results across 10 advertisers |
| `artifacts/ti_650_implementation_plan.md` | SQLMesh deployment plan |
| `artifacts/ti_650_pipeline_explained.md` | Pipeline reference |
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
