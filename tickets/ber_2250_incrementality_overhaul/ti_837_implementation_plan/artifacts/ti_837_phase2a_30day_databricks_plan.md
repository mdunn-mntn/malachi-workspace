# TI-837 Phase 2a — 30-day window on Databricks job cluster

**Goal:** Re-run the v5 lift analysis on a 30-day window using a Databricks job cluster.

**Why now:** the BQ 7-day xwin run hit the 6-hour query timeout twice on the 4-segment variant. 30 days is 4.3× more data and impossible on BQ (augmentor 10-day TTL). Job clusters are ~3× cheaper than the interactive cluster we used for the smoke test (per Victor).

## What changes vs v5

| Dimension | v5 (BQ) | Phase 2a (Databricks) |
|---|---|---|
| Window | 7 days (2026-04-20 → 04-26) | 30 days (target: 2026-04-01 → 04-30, TBD on exact dates) |
| Cohort | 30 advertisers | same 30 advertisers |
| Segments | 4 (all / prosp / stage1 / rtg) | 2 (prosp + rtg) — drop "all" + "stage1" for cost. Stage 1 already known ≈ zero. |
| Outcomes | clickpass + guid | same |
| augmentor | BQ scan (126 TB billed) | GCS Parquet direct read |
| guid_log | BQ | GCS Parquet direct read |
| cost_impression_log | BQ | BQ via Spark (Victor's query-mode pattern) |
| clickpass_log | BQ | BQ via Spark (Victor's query-mode pattern) |
| prospecting_intent_v1 | BQ | BQ via Spark (Victor's query-mode pattern) |
| Runtime budget | 6-hr BQ timeout | 4 hr wall, job cluster |
| Cost | ~$630 (BQ 7-day) | ~$200 est (job cluster 4 hr) |

## Cluster spec (job cluster — new, separate from interactive smoke-test cluster)

| Setting | Value | Why |
|---|---|---|
| Cluster mode | Job (single-use) | 3× cheaper than interactive per Victor |
| Spark version | Match databricks-connect 17.3.x | Already pinned for the Python 3.12 venv |
| Driver | c3d-highmem-16 | DRIVER_NOT_RESPONDING on c3d-standard-4 during smoke; -8 worked but headroom matters at 30-day scale |
| Workers | c3d-standard-8 autoscale 4–16 | Photon-enabled |
| Photon | Enabled | 2–4× speedup on Spark SQL/joins |
| Auto-terminate | Job-default (terminates on completion) | No risk of zombie cluster |
| Tags | `project=TI-837, squad=ML, env=Dev` | Matches policy from the smoke-test cluster |
| GCS perms | Inherited from existing service account | Already verified on `mntn-data-archive-prod` |

## Pipeline structure

The working Spark port at `artifacts/spark_lift_3adv_1day.py` becomes the basis. Three changes:

### 1. Parameterize the runner

Convert the script to accept:
- `--cohort` (path to CSV of advertiser_ids)
- `--start_date`, `--end_date` (UTC)
- `--segments` (`prosp,rtg`)
- `--outcomes` (`clickpass,guid`)
- `--output_path` (gs:// for results CSV)

Run mode = Databricks job, not local databricks-connect. The Python file gets uploaded as a job task; the cluster runs it directly. (databricks-connect is only for interactive smoke testing.)

### 2. Per-day GCS reads with explicit partition paths

Already verified working in the smoke test. For 30 days × 2 regions (augmentor):

```python
augmentor_paths = [
    f"gs://mntn-data-archive-prod/augmentor_log/region={r}/dt={d.isoformat()}/"
    for d in date_range
    for r in ("east", "west")
]
augmentor = (spark.read
    .option("basePath", "gs://mntn-data-archive-prod/augmentor_log/")
    .parquet(*augmentor_paths))
```

`guid_log` follows the same pattern but only `dt=` (no region).

### 3. BQ pulls via Victor's pattern (already working)

`cost_impression_log`, `clickpass_log`, `prospecting_intent_v1` — all use:

```python
SILVER_OPTS = dict(
    parentProject="dw-main-bronze",
    billingProject="dw-main-bronze",
    project="dw-main-bronze",
    viewsEnabled="true",
    materializationDataset="external",
    bigNumericDefaultPrecision="38",
    bigNumericDefaultScale="9",
)
```

Push the date filter into the SQL string passed to `.option("query", ...)` — don't pull 30 days of cost_impression_log into Spark and then filter; let BQ filter at scan.

## Cost / runtime estimates

| Stage | Estimate | Basis |
|---|---|---|
| augmentor GCS scan (30 days × 2 regions) | 30–40 min | Smoke test 1 day = ~5 min × 30 days, but parallelism dominates |
| guid_log GCS scan (30 days) | 15–20 min | smaller than augmentor |
| BQ pulls (cost_imp, clickpass, prosp_intent) — date-filtered | 30–60 min | 4× the 7-day BQ pull time in v5 |
| Joins + aggregations | 60–90 min | 30 advertisers × 2 segments × 3 tiers × 2 outcomes |
| Total wall | **2.5–4 hr** | comfortable inside job-cluster budget |
| BQ bytes billed | ~50–80 TB | 4× v5's 126 TB scaled down by dropping "all" + "stage1" |
| Job cluster cost | ~$150–250 | $50/hr × 3–5 hr |
| **Total** | **~$300–400** | vs $630 for BQ 7-day; cheaper despite 4× more data |

## Outputs

Write per-cell ATT to `gs://<staging>/ti_837_phase2a_30day/cells.csv`:

```
advertiser_id, segment, tier, outcome,
n_treated, n_holdout,
visit_rate_treated, visit_rate_holdout,
att_pp, ci_lower, ci_upper, variance,
n_passed_gate
```

Same schema as v5 outputs — drops directly into the existing pooling code. Pull the CSV down for IVW + sample-weighted + median pooling locally (same Python that built the v5 charts).

## Acceptance criteria

- Segment ordering reproduces: rtg ≫ prosp at high intent
- rtg sample-weighted ATT within ±5pp of v5's +28.89pp (loose check — 30-day means more visits per IP, so absolute lift may be larger)
- prosp sample-weighted ATT within ±2pp of v5's +0.43pp
- Per-advertiser % positive at high intent reproduces (rtg ~100%, prosp ~75%)
- Job cluster cost < $400 per BQ billing report

If any of those fail, debug before scaling further. If they pass, the Phase 2a window result becomes the headline going forward (longer window = more visit accumulation, less noise).

## Risks

| Risk | Mitigation |
|---|---|
| cost_impression_log 30-day BQ pull blows past memory | Push-down filter on date + segment in the query string; project only required columns |
| GCS service account perms drift | Verified on smoke-test cluster, but job cluster is new — re-test with a 1-day dry run before the full 30-day |
| MAX-tier collapse over 30 days | More IPs hit `score=10000` over a longer window. Check tier distribution before pooling — may need to bump tier-diversity gate |
| prospecting_intent_v1 join cost | Daily snapshots × 30 days = expensive. Compute MAX-tier once per IP across the window, persist intermediate to GCS |
| BQ billing surprise | Dry-run the BQ-side queries first; cap on byte-billed |

## Dependencies (before kicking this off)

1. Confirm 30-day window dates with Alex K — probably 2026-04-01 → 04-30 to align with the v5 cohort's pre-period
2. Provision the job cluster (separate from `5428-215533-4jodkdfs` interactive)
3. Upload `spark_lift_30day.py` (parameterized version) to Databricks workspace as a job task
4. 1-day dry run on the job cluster before the full 30-day to verify perms + Photon behavior
5. Schedule the run — overnight is fine since it's a job cluster

## Out of scope

- Stage 1 sub-segment — already proven zero in v5; not worth recomputing on 30 days
- "All campaigns combined" segment — implicit from the (rtg + prosp) decomposition
- Conversions outcome (Phase 2b) — separate workstream
- Bidder-level ghost bidding (Phase 3) — separate workstream

## Source files

- `artifacts/spark_lift_3adv_1day.py` — working Spark port, basis for the parameterized version
- `queries/ti_837_lift_analysis_30adv_7day_v5_segments.sql` — canonical v5 SQL, reference for segment definitions
- `meetings/ti_837_04_victor_dustin_slack_2026_04_30.md` — Victor + Dustin's Spark BQ-connector workaround
- `.claude/databricks_setup.md` — workspace + cluster provisioning notes
