---
doc_type: ticket
title: "AUDI-1170: Orchestration, backfill, and shadow validation for the household FS"
status: done
date: 2026-07-28
summary: "Wire L1→L2→L3 as an additive task group in feature_store_setup_model.py; backfill runner + shadow parity dashboards"
result: "90-day household FS backfill shipped and verified in prod (mirror 17 builds, L2/L3 90/90 daily, Jun+Jul monthly); method merged upstream airflow-ti#1180. Shadow-parity readout moved out of scope at close."
question: "Can we stand up the household FS as an additive hh_ task group that runs end-to-end daily and emits a first shadow-parity readout (household-vs-IP sizes, coverage, day-over-day stability) proving it's safe to consume?"
framing_state: locked
---

# AUDI-1170: Orchestration, backfill, and shadow validation

**Jira:** https://mntn.atlassian.net/browse/AUDI-1170
**Parent epic:** AUDI-1049 · **Build umbrella frame:** `../audi_1134_feature_store_build/summary.md`
**Status:** backlog · **Assignee:** Malachi — **ASSIGNED 2026-07-29** (run `/frame` to lock §0 before moving to `in_progress`; the framing gate blocks it while `framing_state: draft`)

---
## 0. Framing  ← LOCKED 2026-07-29 (framed via /frame; locally, no Jira write)
- **Question:** Can we stand up the household FS as an **additive `hh_` task group** in
  `feature_store_setup_model.py` (one schedule, no forked DAG) that runs **end-to-end daily** and emits a
  **first shadow-parity readout** (household-vs-IP audience sizes per vertical, resolution/coverage split,
  day-over-day household-count stability) — proving the household pipeline runs and is plausibly correct before
  anything consumes it?
- **Goal (why / the decision):** 1170 is the **"make it run + prove it's right"** layer — the **go/no-go on
  whether the household FS is safe for AUDI-1103 (train) and the HHDSC export to consume.** A failed parity
  readout (implausible sizes / poor coverage / high churn) blocks consumption and sends the resolution/graph-join
  back for rework; a clean one green-lights training for the **Sept-4 MVP.** Waiting: Brian (1103 train), Sean
  (HHDSC export). North-star: MM-AI identity-graph integration (ID-327).
- **Objective (done-when — binary, for Sept-4):** (1) the `hh_`-prefixed models run as an **additive task
  group** in `feature_store_setup_model.py` **end-to-end daily** (compiled `model_task_config.json`, dependency
  edges); (2) `docs/feature_store_naming_standards.md` updated with the `mntn_id` dimension; (3) a **first
  shadow-parity dashboard** exists (household-vs-IP audience sizes per vertical, resolution/coverage split,
  day-over-day household stability) **plus a proposed reconciliation band** (audience-size-delta + stability
  thresholds) for sign-off. **Full backfill (per AUDI-1101 depth) + the multi-day N-day shadow-run are
  fast-follows past the gate.**
- **Approach (how):** **Thin end-to-end `hh_` skeleton first** — minimal/stub L2/L3 household models — to stand
  up the orchestration + shadow harness **in parallel** with the real AUDI-1168/1169 (co-owned with Brian), then
  swap the real models in. Extend `feature_store_setup_model.py` + `model_task_config.json` with the `hh_` task
  group. **Note vs the ticket description:** for IPv4-only v1 the **L1 mirror is OPTIONAL (join the graph
  directly)** and resolution lands **inline at L2** (before L158 of `guid_log_derived_ip_vertical_id.py`), so the
  dependency edges are **graph-join@L2 → L3**, not the `L1mirror → L2 → L3` the description assumed. Backfill
  runner over `as_of_date` (60d BQ + `household_graph_parquet` fallback). Shadow-parity queries compare the
  `hh_` output vs the IP store. **Resolve first:** AUDI-1101 retention (backfill depth) + the **lookback-churn
  choice** (features follow the day's vs the snapshot's household — sets what "correct" parity means).
- **What would change the answer:** the shadow-parity readout comes back **implausible** — household audience
  sizes far off, poor resolution/coverage, or day-over-day household-count churn beyond the ~15% baseline (§6.4)
  — meaning the graph-join/resolution is wrong and must be reworked before 1103 trains. Or the skeleton shows the
  additive task group **can't run on the one schedule without a forked DAG**, forcing an orchestration redesign.

## 1. Introduction
Component 5 of 5. Orchestration + the trust layer. The **shadow-run parity check is the gate** — nothing
downstream (train, HHDSC export) consumes the household output until parity holds.

## 2. The Problem
The four models must run in order on one schedule (no forked DAG), backfill deep enough to train, and prove —
via shadow parity — that household audience sizes/coverage/stability are plausible before consumption.

## 3. Plan of Action
1. **Thin `hh_` skeleton** — minimal/stub L2/L3 household models to prove orchestration + the shadow harness now,
   in parallel with the real AUDI-1168/1169 (swap in later).
2. **Additive task group** + dependency edges (**graph-join@L2 → L3**; L1 mirror optional) in
   `feature_store_setup_model.py`; compile `model_task_config.json`.
3. **Shadow-parity dashboards** (the Sept-4 readout): household-vs-IP audience sizes per vertical,
   resolution/coverage split, day-over-day household stability (the HHID-churn check, §6.4) — **+ propose the
   reconciliation band** (size-delta + stability thresholds) for sign-off.
4. **Update `docs/feature_store_naming_standards.md`** with the `mntn_id` dimension.
5. **Fast-follows (past the Sept-4 gate):** backfill runner over `as_of_date` (60d BQ + `household_graph_parquet`
   fallback, depth per AUDI-1101) + the multi-day N-day shadow-run.

**Resolve first (empirical unknowns from the Approach):** AUDI-1101 retention answer (backfill depth); the
lookback-churn choice (features follow the day's vs the snapshot's household); whether the additive task group
runs on the one schedule without a forked DAG.

## 4. Investigation & Findings

### 4a. Codebase research 2026-08-03 (read current `main`; local checkout stale on TI-956, June 8, pre-#1156)
**The reframe: the build surface is much smaller than the ticket description assumed.**

- **Resolution + household aggregation is DONE and tested — not a stub.** PR **#1156** (AUDI-1166/1167, merged
  **2026-07-30**, 6 files) shipped `utils_model/household_resolution.py` (505 lines, 15 passing unit tests):
  - `resolve_households(df, id_columns, graph_df=None, spark=None, as_of_or_before=run_date, min_confidence=None)`
    — as-of, **max-confidence** IP→household join; **row-count invariant** (one `household_id` per row, no
    fan-out; unresolved tagged not dropped). Emits `household_id, resolution_status, resolved_from,
    resolution_confidence, resolution_is_shared`. `id_columns` ordered `[(col, IdType)]`; for v1 `[("ip", IdType.IPV4)]`.
  - `aggregate_to_household(resolved_df, sum_cols, min_cols, max_cols, count_distinct_cols, extra_group_by=("vertical_id",), identifier_col="ip")`
    — collapse to `household_id (× vertical)`; **every measure classified explicitly** (unclassified = dropped,
    never silently summed); emits `contributing_rows`, `source_identifier_count`. Distinct/HLL must NOT be summed.
  - `latest_graph_partition()` (newest `asOfDate ≤ date`, max revision; **raises if >14d stale**),
    `load_graph_ids(id_types=(IPV4,), current_only=True, dedupe=True)`, `coverage_metrics()`, `class IdType` (IPV4=30, MNTN_GUID=41, …).
- **L1 already wired.** `identity_graph_ip_household_id` (graph mirror; reads `gs://identity-graph-prod/mntn-graph/household_graph_parquet`;
  keyed `dt=graph asOfDate`; idempotent, no-ops between weekly builds; ~7 GiB IPv4 slice) and the `(ip,guid)`
  keyset `guid_log_ip_guid_advertiser_id` are **daily leaves** in `feature_store_setup_model.py` (no edges yet).
- **Household L2/L3 re-key is ~10 lines.** IP L2 `guid_log_derived_ip_vertical_id` (`GuidLogDerived(MultiSnapshotFileStorageBaseModel)`,
  `supported_snapshots()=["base","monthly"]`) has **zero HLL/distinct columns** — all `sum/min/max` + recency,
  IP-associative. Re-key = resolve `ip→household_id` on the daily aggregate (`ip_vertical_daily`, above L150),
  change `.groupBy("ip","vertical_id")` (L155) → `.groupBy("household_id","vertical_id")`, `select` `ip`→`household_id`,
  `repartition("household_id")`. Window block (L158+) unchanged. L3 pivot = pure `"ip"→"household_id"` rename
  (incl. `entity_id`, `entity_column_name`); single-shard guard (<10000 cols) holds at ~222 verticals.
- **No `hh_` L2/L3 on `main` yet** (AUDI-1168, Brian, In Progress — hasn't landed). **This is the overlap (D1).**
- **Orchestration = TWO DAGs.** `feature_store_setup_model` (daily 01:03 UTC) **and** `feature_store_snapshot`
  (monthly, day-15 runs the derived+pivot with `--snapshot monthly`). Training (AUDI-1103) is on **monthly** L3.
- **Model framework:** one file = one `BaseModel` subclass + `@compute.dataproc_batch` + `@model_config`
  (`location_root`), `model(self, run_date)`, `read_model("stem.Class")`, `df_write(df)`. Compile via
  `python model_upload.py --dryrun` → regenerates **committed** `dags/model_task_config.json` (stem-keyed,
  compute/spark only; paths live in `@model_config`). Never hand-edit it. New filename → own GCS prefix (no collision).
- **Backfill:** no built-in date-range runner. Single-date idempotent `overwrite`-by-`dt`. Dev = serial
  `python model_run.py <id> -a '{"run_date": d}'` loop (blocks per date, runs on `mntn-prj-dev-00`). Prod =
  a param'd DAG (`schedule=None`, `catchup=False`, dynamic task-mapping fan-out) modeled on
  `domain_vertical_mappings_backfill.py` + `feature_store_snapshot.py`. Backfill **L1 mirror first**.
- **Parity comparands (GCS parquet, not BQ):** IP L2 `gs://mntn-data-archive-prod/feature_store/feature_group_2_derived/guid_log_derived_ip_vertical_id`;
  IP L3 `.../feature_group_3_pivoted/guid_log_pivot_ip_vertical_id` (one row per ip, wide vertical cols). Ready-made
  score pair: IP `intent_score_map` vs HH `intent_score_household_map` under `gs://household-scoring-prod/output/scoring/`.
- **Monitor template:** `models/monitoring/vertical_size_monitor.py` (per-vertical distinct today-vs-yesterday +
  diff, writes parquet `dt=run_date` before email/Slack, wired as a `ModelPysparkBatchOperator` in its producer
  DAG). Reuse `coverage_metrics()` for the resolution/coverage split. Precedent re-key: `intent_score_household_map.py`.
- **Backfill depth (AUDI-1101 resolved):** `identity_graph_history` (id_type=30, BQ, ~60d) back to 2026-06-01,
  ~9 weekly snapshots — "plenty." Deeper → `household_graph_parquet` (~600 GB, `asOfDate`/`asOfDateRevisionNumber`).

### 4b. Two decisions pending Brian sync (branch the plan) — RESOLVED at the 2026-08-03 DE meeting (see §4c)

### 4c. Weekly DE meeting 2026-08-03 (Brian, Malachi, +1) — decisions
Transcript: `../meetings/audi_1049_02_weekly_de_meeting_2026_08_03.txt`. This was the sync.
- **D1 RESOLVED — Brian owns L2/L3 + all wiring.** His **PR #1166** (branch `AUDI-1168`, OPEN, base main) is a
  straight copy of the IP L2/L3, re-keyed to household via Sean's `resolve_households` helper, wired into
  **both** DAGs — daily (`[ip_rollup, identity_graph_ip_household_id] >> guid_log_derived_mntn_id_vertical_id >>
  guid_log_pivot_mntn_id_vertical_id`) and monthly (`_snap` tasks behind the day-15 gate). Team agreed to
  **ship it** as the POC and start on the backlog. **→ AUDI-1170's "additive task group + edges" deliverable is
  done in #1166; my job on it is to REVIEW (Brian asked to be pinged), not rebuild.**
- **First parity data point (informal):** household audience ≈ **45–50% of the IP count** (~190M). Expected
  (a household holds multiple IPs; ~1:2 with edge cases); aligns with Matt's ~50% loss mapping Fangorn to the
  graph. Team comfortable for a POC. **This is the seed of my shadow-parity readout** — I formalize per-vertical
  + coverage + day-over-day churn and set the reconciliation band.
- **Naming (the doc deliverable):** column is **`household_id`** (Jack, locked; Brian switched the column from
  `mountain_id`→`household_id`). Open: the **table/model name** still uses `mntn_id`
  (`guid_log_derived_mntn_id_vertical_id`); team **leans to making it consistent (`household_id`)** but wants a
  bigger-group confirm. → I own resolving this in `feature_store_naming_standards.md`; recommend `household_id`
  token to match column + identity team, flag the rename to Brian before #1166 merges.
- **D2 (Sept-4 cut) — not hard-locked, POC-first posture.** No explicit backfill-depth-for-training decision;
  team's stance = "get the POC out, fine-tune later" → aligns with the framing cut (parity gate first). Backfill
  depth for AUDI-1103 still to confirm with Brian/Matt.
- **Monthly snapshot NOT yet run** (Brian wired it but "didn't run the monthly"). → validation gap my
  backfill/shadow can cover (training reads monthly L3).
- **Aggregation correctness deferred** (Brian: "errors / aggregation logic will come out when we build models on
  top") — this is exactly the AUDI-1170 shadow + AUDI-1105 validation role.
- **Cost lever for backfill/tests (Brian, re Alyson's $35K/mo whiteboard):** a test run on **1 vertical + small
  cluster (4 workers) ≈ $1.28/day vs ~$191/day at 290 workers.** Use single-vertical/small-cluster for cheap
  validation backfills. Also: **no TTL on Fangorn storage currently** (should add one; FS storage $300/mo,
  Fangorn $2K/mo).

**Updated AUDI-1170 lane (post-meeting):** (1) **review Brian's #1166** (ping owed); (2) **backfill runner**
(cheap single-vertical test path first); (3) **shadow-parity monitor + reconciliation band** (extends the
45–50% data point); (4) **naming-standards doc** (`household_id` token); (5) **validate the monthly snapshot
path** (Brian hasn't run it). Orchestration/wiring itself = Brian's #1166.

### 4d. HHID FS Backfill meeting 2026-08-05 (Brian + Malachi) — AUDI-1170 collapses to the backfill
Transcript: `meetings/audi_1170_01_hhid_fs_backfill_2026_08_05.txt`.
- **#1166 MERGED 2026-08-05 16:33 UTC with `household_id` naming** (renamed from `mntn_id` — the review/meeting
  lean adopted). Household L2/L3 live in both prod DAGs; **first daily output is tonight** (merge landed after
  today's 01:03 UTC run → **0 prod household partitions** as of 11am PT, confirmed via gsutil).
- **AUDI-1170's remaining scope = the BACKFILL (+ the shadow-validation gate).** Brian: "since you already
  added the models... I just need to be in charge of the backfill." The "four models" = **L2 daily, L2 monthly,
  L3 daily, L3 monthly** (2 models × the daily-setup + monthly-snapshot DAGs), all wired in #1166.
- **Depth = 90-day daily backfill** (ticket "~90-day back-test"); **monthly ≥ 2 months.** Order **L2 → L3**
  (L3 built on L2). **Resolves D2: backfill IS on the critical path — training is slated NEXT SPRINT (AUDI-1103).**
- **⚠ Prereq I found (not raised in the meeting): backfill the L1 mirror FIRST.** Prod mirror has only
  `dt=2026-07-27` + `2026-08-03`; Brian's L2 loads the mirror `optional=False` per as-of graph date, so a
  90-day backfill **fails** until the mirror is materialized over the ~13 weekly asOfDates in the window (from
  `household_graph_parquet`). Order = **mirror → L2 → L3**.
- **⚠ Depth is graph-capped, not 90 days.** 90d back = 2026-05-07, but the graph only goes to ~**2026-06-01**
  (AUDI-1101; ~66 days). `latest_graph_partition` raises for run_dates before the earliest asOfDate → the
  household backfill **caps at ~2026-06-01**. Confirm 66d is enough (AUDI-1101 said "plenty") or whether
  `household_graph_parquet` has deeper history than the BQ `identity_graph_history` table.
- **Get Ryan Kleck's backfill scripts.** Brian (hasn't run a backfill himself) strongly recommends Ryan — has
  shell scripts + knows the gotchas. Open mechanism: **Airflow backfill tools vs a "master Dataproc job"**
  (Malachi recalls the FS backfill was a Dataproc job, not Airflow; Ryan's script may predate current
  GCP-access constraints → may need updating). Action: ping Ryan / set up a Ryan+Brian call.
- **Shadow-parity validation still mine** (not discussed, but the ticket's "prove it's right" gate remains).

**Action items (2026-08-05):** (1) ping Ryan for backfill scripts/pointers + mechanism (Dataproc master job vs
Airflow `backfill`); (2) backfill **mirror → L2 → L3** daily to ~2026-06-01 + monthly ≥2mo; (3) confirm the
graph-depth cap (66 vs 90d) with Ryan/Matt; (4) build the shadow-parity readout on the backfilled output + set
the reconciliation band.

### 4e. Backfill mechanism resolved (Ryan sync 2026-08-06 + hands-on) — full detail in memory [[reference_airflow3_backfill_scoping]]
- **Ryan has NO backfill script** (Brian's belief was wrong). Graph depth reaches **2026-04-20**, so the full
  **90-day** backfill is achievable (corrects the 66-day cap in §4c/§4d).
- **The Airflow-3 UI "Run Backfill" is WHOLE-DAG only — cannot scope to the 3 household tasks** (reruns all ~35
  tasks × 89 days). No per-task option; "Advanced" = run conf, not task select. This is a dead end.
- **Correct mechanism = run the 3 models in DEV via `model_run.py` (loop over dates) → `gsutil` copy new
  partitions dev→prod** (Ryan's endorsed "big-ass script"), OR a **dedicated backfill DAG** (schedule=None,
  Param dates, only the 3 models). `model_run.py` targets dev. Order **mirror → L2 → L3**; mirror only ~13
  weekly runs (skip 6 days). **Next: write that script / DAG.**
- **Cleanup episode:** a botched whole-DAG UI backfill (89 runs, May-8→Aug-6, v194) was triggered on the paused
  DAG. Resolved by confirming **0 non-terminal backfill runs** (only queued/running would execute on unpause;
  success/failed are inert) via the Airflow API (path prefix from `<base href>`, e.g. `/dokgryiq/api/v2/...`),
  then unpausing. Env: **Astronomer Runtime 3.1-9 / Airflow 3.1.5.**

### 4f. Backfill execution started 2026-08-06 (script + PAM access)
- **Script:** `artifacts/backfill_household_fs.sh` — phased dev+copy (smoke → mirror → l2 → l3 → copy),
  DRY_RUN default, resume-skip on existing dev partitions, copy is additive (never `-d`). Invokes
  `uv run python model_run.py` (bare `python` not on PATH).
- **Prereqs fixed:** airflow-ti moved TI-956 branch → `main` @ 8342a91 (209 commits; all 3 household model
  IDs present in `model_task_config.json`); `uv sync --group models`; ADC refreshed.
- **Access = PAM, not standing IAM** (Ryan): the Dataproc SA is hardcoded (`utils_runner/dataproc.py:36`);
  submit fails without `serviceAccountUser`. Requested via `gcloud pam grants create` on entitlements
  `dataproc-runtime-actas` + `dataproc-submit` (mntn-prj-dev-00, 4h max, devops 1-approval) → both ACTIVE
  same day. **A multi-hour phase needs a fresh 4h grant.** CLI recipe in [[reference_airflow3_backfill_scoping]].
- **Smoke test GREEN** (2026-05-15): took 3 attempts, each exposing one read-resolution fact (all in
  [[reference_airflow3_backfill_scoping]]): (1) `guid_log_ip_advertiser_id` L1 reads DEV → added `seed`
  phase (120 prod partitions → dev, `gcloud storage cp` server-side; `gsutil -m` died silently, macOS
  fork bug); (2) the graph mirror is read-only → **L2 always reads it from PROD** → added `copy-mirror`
  phase before `daily`; (3) final chain L2=17.5 GB, L3=10.2 GB at `dt=2026-05-16` in dev.
- **Locked run order: seed ✅ → mirror (13 weekly, x4 lanes) → copy-mirror (→prod) → daily (90 L2→L3
  pairs, x4 lanes, ~8-10h) → copy (→prod).** Parallel lanes per Ryan ("run them at the same time").
  Prod-write access confirmed (mirror dt=2026-05-11 copied). Fresh 4h PAM grant needed per window.

_Full plan of action: `~/.claude/plans/i-have-to-execute-snoopy-sutton.md` (approved 2026-08-03)._
_Research detail: this session's three Explore-agent reports (orchestration/PR#1156, L2/L3 internals, backfill/monitor)._

## 5. Solution

**The 90-day household FS backfill shipped 2026-08-06** (all in prod, verified):
- **Mechanism:** `artifacts/backfill_household_fs.sh` — dev `model_run.py` runs + `gcloud storage` copy to prod.
  Phases: seed (120 prod L1 guid_log partitions → dev) → mirror (17 graph builds, incl. gap-fills) →
  copy-mirror → daily (90 L2→L3 pairs, 4 lanes) → monthly (Jun+Jul snapshot pairs) → copy.
- **Prod result:** mirror 17 dts · L2 base 90/90 (dt 2026-05-09..08-06) · L3 base 90/90 · monthly
  2026-06-01 + 2026-07-01 both layers. 0 unresolved failures. Aug-1 monthly lands via the prod
  snapshot DAG on Aug-15.
- **Access:** ~4 PAM grant windows (dataproc-runtime-actas + dataproc-submit, 4h each).
- **Tooling merged upstream** (airflow-ti#1180, 2026-08-07): `scripts/model_backfill.sh` (generalized) +
  `docs/model_backfill.md` (methodology) + the batch-ID collision fix.
- **Ops surprises solved en route** (detail in [[reference_airflow3_backfill_scoping]]): per-model
  read-resolution (guid_log=dev, mirror=prod-always), graph builds every ~3-4d not weekly (weekly
  sampling missed 06-15/06-22/07-13 → reactive gap-fill), batch-ID minute-collision (local
  dataproc.py patch), gsutil -m macOS fork bug, monthly writes to `feature_group_X_monthly/` suffix
  dirs, never merge into an existing prod dt (part-file dedup guard).

## 6. Questions Answered
- **Q:** — **A:** —

## 7. Data Documentation Updates
_(document the shadow-parity methodology + household-stability metric)_

## 8. Open Items / Follow-ups

- **Review the ID team's pyspark household-resolution module before more FS work** (added 2026-08-07): a
  shared IDs-in→households-out interface with graph-use crediting logs, intended to replace the FS
  conversion piece — may supersede `household_resolution.py`. Requirement doc ambiguous; Sean awaiting
  Jack's reply (dev-mntn-id thread). Read the doc, post questions.
- **Dev cleanup pending user go:** ~2.7 TB backfill data in `mntn-data-archive-dev` (seed + L2/L3 copies).

### Status after 2026-08-03 research (what's resolved vs still open)
**Research-resolved** (see §4a): PR #1156 is real/merged (Q1 — build on merged helpers, not a fresh branch;
`(ip,guid)` L1 is for the GUID fast-follow, IPv4-only v1 uses the existing IP L1); the resolver interface is
`utils_model.household_resolution` — **call the shipped helpers, don't inline a parallel resolver** (Q2);
direct-graph-join works but the **mirror as `graph_df` is cheaper (~7 GiB vs ~200 GiB)** (Q3 — lean mirror,
confirm w/ Sean); additive task group + edges mechanics fully mapped (self-served).
**Still open — the Brian/Sean sync (see the plan's "Team questions"):** D1 split · D2 Sept-4 cut · Brian's L2
branch location · monthly-snapshot wiring · **naming token** (`household_id` vs `mntn_id` vs `hh_`) · lookback-churn semantics.

### Clarify with Sean Yang (FS lead + ticket author, now on DS13/19) — before/early in the build
1. **PR #1156 base + the `(ip,guid)` L1 table:** should the household `hh_` L2/L3 models build on your draft
   PR (airflow-ti #1156) or a fresh branch — and does its `(ip,guid)` L1 table stub anything I'd duplicate?
   Confirming your own question back: for IPv4-only Sept-4 we use the **existing IP-keyed guid_log L1 as-is**,
   and `(ip,guid)` is only for Ryan's GUID fast-follow — right?
2. **Resolver interface:** where will your `household_resolution.py` / `identity_graph_ip_mntn_id` stubs live,
   and their signature? Should my L2 resolution **call the stub (swap-ready)** or **inline the max-conf join**
   for v1? (Avoid building a parallel resolver.)
3. **Direct-graph-join vs mirror:** did your cost test on joining the full `household_graph_parquet` directly
   land — is direct-join OK for v1, or do we need the daily mirror (1166)? (Sets whether 1166 is real or stays optional.)
4. **DS13/19 seam:** now that you own DS13/19 — where's the boundary vs my Fangorn `guid_log` L2/L3 re-key? Do my
   `hh_` L2/L3 outputs feed your DS13/19 (`site_visit_signal`/`tpa_ipdsc_export`) work, or are they independent?
5. **Split with Brian:** preferred division of the real L2/L3 models (1168/1169) vs the 1170 orchestration/shadow
   skeleton across me + Brian?

### Decide with the group (Sean/Ryan/Matt)
6. **Lookback-churn:** do features follow the **day's household** or the **snapshot's household**? Defines what
   "correct" shadow parity even means (§7i / §6 open item).

### I'll self-serve from the airflow-ti clone (not Sean's time)
Exact `hh_` model naming pattern (naming-standards doc), the IP-side parity comparand tables/paths, L158
context in `guid_log_derived_ip_vertical_id.py`, and the additive-task-group mechanics in `feature_store_setup_model.py`.

- Backfill depth gated by 60-vs-90d retention (§6.1, AUDI-1101). Reconciliation band (§6.6) undefined — this
  ticket produces the parity numbers that band will be set against.
- **Convention confirmed (Slack 2026-07-29, epic §7h): REUSE the existing DAGs; add HHID work as new MODELS with
  an `hh_` prefix** — NOT a separate `feature_store_hhid_*` DAG set. This is the concrete form of the "additive
  task group, no forked DAG" scope. Resolution happens in L2 (IPv4-only), so the household models are L2/L3
  `hh_`-prefixed additions to `feature_store_setup_model.py`.
