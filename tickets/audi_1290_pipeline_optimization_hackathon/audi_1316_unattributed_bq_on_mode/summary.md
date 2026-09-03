---
doc_type: ticket
title: "AUDI-1316: Show unowned BigQuery spend on the cost dashboard"
status: in_progress
date: 2026-09-03
summary: "Mode query over JOBS_BY_PROJECT plus the Mode service-account grant, so unowned slot-hours are visible"
result: "No grant needed - mode-analytics@dw-main-bronze already holds bigquery.jobs.listAll on dw-main-bronze via medallion_bronze_reader; Mode SQL drafted and validated (0.178 GB, 10.6 slot-s per day of window), reconciles with the daily report at ~604 jobs/day and ~982 slot-h/day"
question: "Can the cost dashboard show unowned BigQuery slot-hours per day, and what grant does the Mode service account need to read them?"
framing_state: locked
---

# AUDI-1316: Show unowned BigQuery spend on the cost dashboard

**Jira:** https://mntn.atlassian.net/browse/AUDI-1316
**Status:** in_progress
**Date Started:** 2026-09-03
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-03 by the dispatcher from the ticket description and the AUDI-1278/1281 records it follows.
- **Question (the unknown):** Can the cost dashboard show unowned BigQuery slot-hours per day, and what grant does the Mode service account need to read them?
- **Goal (why / the decision):** AUDI-1278 measured 612 unowned jobs and 1,110 slot-hours a day but its measurement surface is a daily file nobody opens; the dashboard is where cost is actually read. Cost-reduction lever under epic AUDI-1290.
- **Objective (done-when):** A Mode section on report e81786de8403 showing unowned slot-hours per day by submitter, backed by a merged mntn-devops grant PR; the number falls as the AUDI-1278 labels merge.
- **Approach (how):** Verify the Mode connection's actual service account and its current denial on dw-main-bronze INFORMATION_SCHEMA.JOBS_BY_PROJECT before writing anything; mirror the spark-optimizer grant pattern in mntn-devops (crossplane ProjectIAMMember, sync-wave 3); draft the Mode SQL in artifacts/ and validate it with bq_run.sh under a date filter and a LIMIT; the Mode report itself is edited in the UI, so this ticket ships a grant PR plus a drafted query, not a Mode PR.
- **What would change the answer:** If the Mode connection already holds the read, the grant PR is unnecessary and the ticket is just the query. If Mode's principal is a user rather than a service account, the grant target changes and the devops ask has to name it.

## 1. Introduction
Follow-on to AUDI-1278, which measured the unowned BigQuery jobs at 612 jobs and 1,110 slot-hours a day and shipped labels for the airflow-ti share. The Mode cost dashboard reads the optimizer's finding ledger only, so the unowned bucket never reaches it; AUDI-1278 chose the daily report as its measurement surface instead. This ticket is that report's dashboard half.

## 2. The Problem
AUDI-1278 measured the unowned BigQuery bucket at 612 jobs/day and 1,109.9 slot-h/day and chose
the daily GCS file `gs://mntn-data-archive-prod/optimizer/optimizer_bq_<date>.md` as its
measurement surface (decision 0007). Nobody opens that file. The Mode cost dashboard
(report `e81786de8403`, Audience Intelligence space) is where BigQuery cost is actually read, and
its only BigQuery section ("BigQuery cost by task", query `3ead7301daa8`) reads
`mntn-prj-prod-00.optimizer.optimization_ledger WHERE surface = 'bq'`. The ledger never records
the unowned bucket by design (`bq_profile.reports()` attaches no finding to it, `ledger.record()`
skips finding-less reports, pinned by `test_heavy_task_is_a_finding_and_unattributed_is_not`), so
roughly 1,000 slot-hours a day of real spend is invisible on the dashboard that exists to show
spend. Affects the AUDI-1290 hackathon epic's cost-reduction measurement: the AUDI-1278 and
camperbid label merges have no visible before/after on the surface leadership reads.

## 3. Plan of Action
Written 2026-09-03 from §0 before execution; steps 1-3 were run first because §0's
"what would change the answer" makes the grant conditional on them.

1. **Establish the Mode principal empirically** (three independent sources, no guessing):
   the Mode API's data-source list for report `e81786de8403`; the mntn-devops config that
   names the Mode service account; and the live BigQuery dataset ACL on the dataset the
   report actually reads.
2. **Establish whether that principal is denied `bigquery.jobs.listAll` on dw-main-bronze**,
   from the IAM policy only (never by authenticating as Mode): read the live project policy
   for the principal's roles, then resolve each role to its permission list in the
   mntn-devops terraform that creates it.
3. **Branch on the result.** Denied -> mirror the AUDI-1241 spark-optimizer grant
   (`argocd-v2/.../base/<owning-project>/iam/<identity>/bq-profile-access.yaml`, crossplane
   `ProjectIAMMember`, sync-wave 3, kustomization entry, one comment block in the same voice).
   Already granted -> ship no grant PR, record why, and keep the grant YAML as a standby
   artifact in case the run 403s.
4. **Draft the Mode SQL** to `artifacts/audi_1316_mode_query.sql`, using the same
   unattributed definition as `bq_profile.py` (dw-main-bronze `region-us-central1`
   JOBS_BY_PROJECT, `user_email` in the two Airflow deployment service accounts, no
   `airflow-dag` / `airflow-task` label) so the dashboard number reconciles with the daily
   `optimizer_bq_<date>.md` report and falls as the AUDI-1278 labels merge.
5. **Validate the SQL** with `bq_run.sh`: `--dry_run` first, abort over 5 GB, then a real run
   under a date filter and a LIMIT; record the measured bytes billed and slot time.
6. **Write the hand-over**: `artifacts/audi_1316_pr_body.md` (lint `--kind pr`) and
   `artifacts/audi_1316_result_comment.txt` (lint `--kind completion`), naming report
   `e81786de8403` and the `opt-bq` section as where the query goes. The Mode report is edited
   in the UI; this ticket ships the query and the access verdict, not a Mode change.

## 4. Investigation & Findings

### 4.1 Which principal Mode queries BigQuery as (three independent sources, all agreeing)
The framing required this be established, not assumed.

1. **Mode API, data sources.** `GET https://app.mode.com/api/mntn/data_sources` (HTTP basic, keychain
   items `mode_api_token` / `mode_api_secret`) returns exactly two BigQuery adapters:
   `48787` "Google BigQuery" and `49672` "Google BigQuery - INT". `GET /api/mntn/reports/e81786de8403/queries`
   shows **all six** of the report's queries on `data_source_id = 48787`
   (`3ead7301daa8` BigQuery cost by task, `5a66e5fad18c` Savings headline, `6fdc8ae9ccf7` Applied fixes,
   `513a4a7a4a71` Savings by surface, `183d18f86de6` Top DAGs by findings, `a167f6ad0146` Findings over time).
   The API does not expose the underlying service account, so this fixes the connection, not the identity.
2. **mntn-devops config.** `terragrunt/gcp/resources/mntn/dplat/bronze/terragrunt.hcl:490` maps the
   multi-layer service-account state key `mode` to the GCP account id `mode-analytics`, and
   `modules/dw-medallion-layer/main.tf:96` declares the account `mode-analytics`. Full principal:
   `mode-analytics@dw-main-bronze.iam.gserviceaccount.com`. It is the only identity in the whole repo,
   and the only member in the live dw-main-bronze project policy, whose name contains "mode".
3. **Live BigQuery dataset ACL, the decisive one.** `bq show mntn-prj-prod-00:optimizer` lists
   `READER | mode-analytics@dw-main-bronze.iam.gserviceaccount.com`. That is the exact dataset every
   query on report `e81786de8403` reads. A dataset the report demonstrably reads (run `d2d0b89e9cef`
   succeeded 2026-09-01) grants read to exactly one non-project principal, and it is this one.

This corroborates the 2026-08-28 `SELECT SESSION_USER()` result recorded in `reference_mode_api`, and
was reached without authenticating as Mode.

**Side finding that pins the job project.** `mode-analytics@dw-main-bronze` has **no binding at all** in
the `mntn-prj-prod-00` project policy (`gcloud projects get-iam-policy mntn-prj-prod-00 --filter=...`
returned empty), so it holds no `bigquery.jobs.create` there. Its reads of
`mntn-prj-prod-00.optimizer.optimization_ledger` therefore cannot bill to mntn-prj-prod-00; the Mode
connection's job project must be `dw-main-bronze`, where `medallion_bronze_reader` supplies
`bigquery.jobs.create`. This matters for the deliverable: a fully-qualified
`` `dw-main-bronze`.`region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT `` reads the right project's
history regardless, and the job runs where the principal can create it.

### 4.2 The principal is NOT denied bigquery.jobs.listAll. The grant is unnecessary.
This overturns the assumption §0 inherited from AUDI-1278 and decision 0007.

- **Live role list.** `gcloud projects get-iam-policy dw-main-bronze --flatten="bindings[].members"
  --filter="bindings.members:mode-analytics@dw-main-bronze.iam.gserviceaccount.com"` returns exactly one
  role: `organizations/104640274931/roles/medallion_bronze_reader`.
- **What that role contains.** The role is created by `google_organization_iam_custom_role.layer_reader`
  in `terragrunt/gcp/resources/mntn/dplat/modules/dw-medallion-layer/iam.tf`, with
  `role_id = "medallion_${var.layer_type}_reader"`. `terragrunt/.../dplat/bronze/terragrunt.hcl:406-407`
  sets `project_id = "dw-main-bronze"`, `layer_type = "bronze"`, so the role id resolves to
  `medallion_bronze_reader`, matching the live binding exactly. Its permission list (iam.tf lines 19-43)
  includes **`bigquery.jobs.create` (26), `bigquery.jobs.get` (27), `bigquery.jobs.list` (28),
  `bigquery.jobs.listAll` (29)**, plus `bigquery.tables.getData`, `bigquery.reservations.use`.
- **Where the earlier claim went wrong.** `reference_bq_job_attribution`, `reference_mode_api` and
  decision 0007 all state that `iam_bronze_extras.tf` line 158 "grants only the layer reader role", and
  inferred a denial from that. The line is real (`google_project_iam_member.mode_analytics_access`, role
  `local.layer_reader_role_id`, member `mode-analytics@dw-main-bronze`), but the inference never opened
  the role definition. The layer reader role is not a narrow table-read role; it is a broad read role that
  deliberately carries job-history access.
- **Why the spark-optimizer precedent needed a grant and this does not.** `gcloud projects get-iam-policy
  dw-main-bronze` for `spark-optimizer@mntn-prj-prod-00` returns `roles/bigquery.jobUser` and
  `roles/bigquery.resourceViewer` and nothing else. spark-optimizer is not a medallion reader member, so
  before AUDI-1241 it held no role on dw-main-bronze at all and genuinely could not list jobs. The two
  cases are consistent, not contradictory.
- **AUDI-1241 is merged.** `argocd-v2/mgmt/platform/crossplane/managed-resources/prod/manifests/base/
  mntn-prj-prod-00/iam/spark-optimizer/bq-profile-access.yaml` on `origin/main` is byte-identical to the
  version on branch `audi-1241-spark-optimizer-bq-grants` (`diff` clean), and both grants are live in the
  project policy above.
- **Residual risk, stated plainly.** `gcloud iam roles describe medallion_bronze_reader
  --organization=104640274931` is PERMISSION_DENIED for `malachi@mountain.com` (`iam.roles.get`), so the
  live permission list could not be read directly; the evidence is the terraform that creates the role,
  plus the role existing live under exactly that id. If terraform drifted, the Mode run 403s. The
  discriminating test costs ten seconds: run the query in Mode. The standby grant is written and ready
  (§5.3) so a 403 is a paste, not an investigation.

### 4.3 Where the grant would go if it were needed
Not the AUDI-1241 path. The crossplane base directory is named for the project that **owns the identity**,
not the project being granted: spark-optimizer lives under `base/mntn-prj-prod-00/` while its
`ProjectIAMMember` targets `project: dw-main-bronze`. `mode-analytics` is owned by dw-main-bronze, so its
home is `base/dw-main-bronze/iam/mode-analytics/`. That base also uses a different file name than
spark-optimizer's `bq-profile-access.yaml`: every identity under `base/dw-main-bronze/iam/`
(`astro-atchurn-prod`, `jedi-media-spend-job`, `inflection`, `segment`) uses `bigquery-permissions.yaml`
with a per-identity `kustomization.yaml` and a `- <identity>` line in `base/dw-main-bronze/iam/kustomization.yaml`.
Both conventions point to the same path, recorded in the standby file's header.

### 4.4 The query, validated
`artifacts/audi_1316_mode_query.sql`. Uses the same unowned definition as `bq_profile.py` `PROFILE_SQL`
(dw-main-bronze `region-us-central1` `JOBS_BY_PROJECT`; `user_email` in `airflow-ti-prod@` and
`airflow-camperbid-prod@`; zero `airflow-dag` / `airflow-task` labels), so the dashboard number
reconciles with the daily `optimizer_bq_<date>.md` report and falls as the AUDI-1278 and camperbid label
merges land. 30-day window, grouped by day and submitter, `LIMIT 100` (30 days x 2 submitters = 60 rows).

**Measured cost.** Single-day run: **0.178 GB billed / 0.177 GB processed, 10.6 slot-seconds, 1.5 s wall**,
`billing_tier` 1, reservation `dw-main-bronze:us-central1.adhoc`, edition ENTERPRISE. The 30-day form
extrapolates to roughly 5.3 GB and 5 slot-minutes per refresh, and runs on the org reservation rather
than on-demand, so the bytes figure is informational.

**Gotcha: the dry-run estimate for `INFORMATION_SCHEMA.JOBS_BY_PROJECT` over-states the scan by ~27x.**
`--dry_run` returned an upper bound of 4,863,570,618 bytes (4.86 GB) for the one-day window that actually
billed 190,840,832 bytes (0.178 GB). It does scale with the window (1 day 4.86 GB, 7 days 16.1 GB,
30 days 36.2 GB), so the 7-day and 30-day forms both trip the workspace's 5 GB abort rule on the dry run
and **were not run**. Validation was therefore done as three separate single-day runs, each under the gate.

**Reconciliation against the AUDI-1278 baseline** (outputs/`audi_1316_unowned_by_day_*.csv`):

| day | camperbid jobs | camperbid slot-h | airflow-ti jobs | airflow-ti slot-h | total jobs | total slot-h |
|---|---|---|---|---|---|---|
| 2026-08-31 | 461 | 1,005.5 | 131 | 4.3 | 592 | 1,009.8 |
| 2026-09-01 | 454 | 977.6 | 166 | 0.0 | 620 | 977.6 |
| 2026-09-02 | 469 | 953.5 | 131 | 5.1 | 600 | 958.6 |

Three-day mean 604 jobs/day and 982 slot-h/day against AUDI-1278's seven-day mean of 612 jobs/day and
1,109.9 slot-h/day (08-26..09-01) and the daily report's 606 jobs/day and 1,104.7 slot-h/day (08-28..09-01).
Both overlapping days match AUDI-1278's daily-report figures exactly (08-31: 592 jobs / 1,009.8 slot-hours; 09-01: 620 / 977.6). The 11% sits against the seven-day mean, which is arithmetic across a different set of days, not a discrepancy. Consistent with the day-to-day
spread in camperbid's `bos__spend` reads rather than a definition mismatch. `airflow-ti-prod` slot-hours
round to 0.0 on 09-01 because its unowned jobs are cheap (the `dlv_pattern_identification` CTAS dominates
and did not run heavy that day); the 166-job count on that day is the correct signal, not the slot figure.

## 5. Solution

### 5.1 Verdict: ship the query, no devops PR
`mode-analytics@dw-main-bronze` already holds `bigquery.jobs.listAll` and `bigquery.jobs.create` on
dw-main-bronze through `medallion_bronze_reader`. No mntn-devops change is opened. The worktree
`audi-1316-mode-bq-job-history` is left untouched (`git status` clean), which is the correct outcome:
a PR granting a role a principal already has would be noise on a warehouse that needs the data-platform
team's sign-off.

### 5.2 The Mode query, and where it goes
`artifacts/audi_1316_mode_query.sql`. Hand-over, not an automated change: add it in the Mode UI to report
**`e81786de8403`** ("Spark Optimizer Savings", Audience Intelligence space) as a new query, embedded in
the existing **`opt-bq`** layout section that today holds only "BigQuery cost by task" (`3ead7301daa8`).
Name it so the pair reads as attributed vs unowned. Deliberately not done over the API even though
`POST /api/mntn/reports/<token>/queries` plus a layout `PATCH` is verified to work: the ticket's scope is
the query and the access verdict.

### 5.3 Standby grant
`artifacts/audi_1316_standby_grant.yaml`. Crossplane `ProjectIAMMember`, `roles/bigquery.resourceViewer`
on dw-main-bronze for `mode-analytics@dw-main-bronze`, sync-wave 3, `deletionPolicy: Orphan`, mirroring the
spark-optimizer resource. Header carries the repo path it would take and the kustomization line it needs.
Apply only on a 403. `artifacts/audi_1316_pr_body.md` is its PR body, written and linted, unopened.

## 6. Questions Answered
- **Q:** Which principal does Mode query dw-main-bronze as?
  **A:** `mode-analytics@dw-main-bronze.iam.gserviceaccount.com`, via BigQuery data source `48787`, which
  carries all six queries on report `e81786de8403`. Established from the Mode API, the mntn-devops
  terragrunt config, and the live dataset ACL on `mntn-prj-prod-00:optimizer`, without authenticating as Mode.
- **Q:** Is that principal denied `bigquery.jobs.listAll` on dw-main-bronze?
  **A:** No. Its one role there, `organizations/104640274931/roles/medallion_bronze_reader`, includes
  `bigquery.jobs.listAll` and `bigquery.jobs.create`. The prior record inferred a denial from the role's
  name without reading its permission list.
- **Q:** What grant does the Mode service account need?
  **A:** None. The AUDI-1241 spark-optimizer precedent was needed because that identity held no role on
  dw-main-bronze at all; it is not the comparable case.
- **Q:** Can the cost dashboard show unowned BigQuery slot-hours per day?
  **A:** Yes, with the drafted query alone. Validated at 0.178 GB and 10.6 slot-seconds per day of window,
  reconciling with the daily report at roughly 604 jobs/day and 982 slot-h/day.

## 7. Data Documentation Updates
Handed back to the dispatcher for `knowledge/` routing:
- `medallion_bronze_reader` (org 104640274931) **includes `bigquery.jobs.listAll`, `bigquery.jobs.create`, `bigquery.jobs.get`, `bigquery.jobs.list`** plus `bigquery.tables.getData` and `bigquery.reservations.use` — it is a broad read role, not a table-scoped role. Defined in mntn-devops `terragrunt/gcp/resources/mntn/dplat/modules/dw-medallion-layer/iam.tf:19-43`.
- **Correction, appended not overwritten:** `reference_bq_job_attribution`, `reference_mode_api`, and decision 0007 recorded that a Mode `JOBS_BY_PROJECT` query "would need `bigquery.jobs.listAll` via a mntn-devops PR". That is refuted by live project policy + role permission list, a better evidence class than terraform-inference-only. Residual risk: the org role definition itself is unreadable (`PERMISSION_DENIED` for `malachi@mountain.com`), so terraform-vs-live drift would show as a 403; the standby grant exists for that case.
- `mode-analytics@dw-main-bronze` has **no binding in the `mntn-prj-prod-00` project policy**; reads `optimizer.optimization_ledger` through a dataset-level READER ACL. The Mode connection's job project is therefore dw-main-bronze, not mntn-prj-prod-00.
- **`INFORMATION_SCHEMA.JOBS_BY_PROJECT` dry-run estimates over-state the scan by ~27x** (4.86 GB est vs 0.178 GB billed for one day). The estimate scales with the window, so multi-day queries trip the 5 GB abort gate while costing cents. Validate with single-day runs and extrapolate.
- Crossplane managed-resources convention: `base/<project>/iam/<identity>/`, where the project is the one **that owns the identity**, not necessarily the target project (AUDI-1241 `spark-optimizer` is owned by mntn-prj-prod-00 so goes under `base/mntn-prj-prod-00/iam/`; `mode-analytics` is owned by dw-main-bronze so goes under `base/dw-main-bronze/iam/`). File naming is per-base: `base/dw-main-bronze/iam/*` uses `bigquery-permissions.yaml`; `base/mntn-prj-prod-00/iam/spark-optimizer` uses `bq-profile-access.yaml`.
- AUDI-1241 `bq-profile-access.yaml` is merged to mntn-devops origin/main and both grants live in the project policy.
- [[reference_bq_job_attribution]], [[reference_mode_api]], [[reference_mntn_devops_permissions]], decision 0007.

## 7. Data Documentation Updates
Handed back to the dispatcher for routing; not written to `knowledge/` masters from this ticket.
- `medallion_bronze_reader` / `medallion_<layer>_reader` (org 104640274931), the custom role bound to
  `mode-analytics@dw-main-bronze` and to the medallion layer projects, **already includes
  `bigquery.jobs.listAll`, `bigquery.jobs.create`, `bigquery.jobs.get`, `bigquery.jobs.list`**
  (mntn-devops `terragrunt/gcp/resources/mntn/dplat/modules/dw-medallion-layer/iam.tf` lines 19-43).
  It is a broad read role, not a table-read role.
- **Correction, appended not overwritten.** `reference_bq_job_attribution`, `reference_mode_api` and
  decision 0007 record that a Mode `JOBS_BY_PROJECT` query "would need `bigquery.jobs.listAll`
  (`roles/bigquery.resourceViewer`) for `mode-analytics@dw-main-bronze` via a mntn-devops PR". That is
  refuted: the access is already held. The old claim was an inference from `iam_bronze_extras.tf:155-159`
  granting "only the layer reader role"; the new claim reads that role's permission list plus the live
  project policy, which is the better evidence class. Residual: the org role definition itself is
  unreadable at this account's permission level, so a terraform-vs-live drift would show up as a 403.
- `mode-analytics@dw-main-bronze` has **no binding in the `mntn-prj-prod-00` project policy**; it reads
  `optimizer.optimization_ledger` through a dataset-level READER ACL. The Mode connection's job project is
  therefore `dw-main-bronze`, not the project whose data the report reads.
- Crossplane managed-resources convention: `base/<project that owns the identity>/iam/<identity>/`, even
  when the `ProjectIAMMember` targets a different project. File naming is per-base:
  `base/dw-main-bronze/iam/*` uses `bigquery-permissions.yaml`; `base/mntn-prj-prod-00/iam/spark-optimizer`
  uses `bq-profile-access.yaml`.
- **`INFORMATION_SCHEMA.JOBS_BY_PROJECT` dry-run estimates over-state the real scan by roughly 27x**
  (4.86 GB estimated vs 0.178 GB billed for one day in dw-main-bronze). The estimate scales with the
  creation_time window (1d 4.86 GB, 7d 16.1 GB, 30d 36.2 GB), so multi-day forms trip the 5 GB abort rule
  while costing cents. Validate by single-day runs and extrapolate.
- AUDI-1241's `bq-profile-access.yaml` is merged to mntn-devops `origin/main` and both grants
  (`bigquery.jobUser`, `bigquery.resourceViewer` for `spark-optimizer@mntn-prj-prod-00` on dw-main-bronze)
  are live in the project policy.

## 8. Open Items / Follow-ups
1. **The Mode section is a UI paste, not shipped by this ticket.** Add
   `artifacts/audi_1316_mode_query.sql` to report `e81786de8403`, `opt-bq` section. Owner: user.
2. **Confirm the access verdict at run time.** The first Mode run is the discriminating test for the one
   piece of evidence that could not be read directly (the live permission list of
   `medallion_bronze_reader`). A 403 means terraform drift; apply `artifacts/audi_1316_standby_grant.yaml`
   with `artifacts/audi_1316_pr_body.md`. Anything else means the verdict holds.
3. **The number should fall, and that is attribution not savings.** After the airflow-ti #1278 merge expect
   roughly -147 jobs/day and -4 slot-h/day; after the camperbid Spark-property merge expect the bucket down
   to roughly 24 jobs/day and under 1 slot-h/day, with `bos__spend` rising correspondingly on the
   attributed side. Neither is a spend reduction; label the dashboard section so nobody reads it as one.
4. **Plan deviation.** §3 step 3 branched to "already granted", so no file was written into the
   mntn-devops worktree and no PR body was written for an opened PR. Both artifacts exist as standby in
   `artifacts/`. §3 anticipated this branch; nothing in the plan was proven wrong.
5. **Three-day validation, not seven.** The 5 GB dry-run gate capped each run at a one-day window, so the
   reconciliation used 08-31, 09-01 and 09-02 rather than matching AUDI-1278's seven-day window. The 11%
   slot-hour gap against the seven-day mean is unexplained beyond day-to-day spread; four more single-day
   runs would close it if anyone cares.

## Verification
Adversarial pass, 2026-09-03, against this file's own §0/§3/§5 and the mntn-devops worktree
(`/private/tmp/.../wt/audi_1316`, `git diff`, read-only). `gcloud` was live and authenticated
(`malachi@mountain.com`), so two of the IAM claims were re-run directly rather than trusted.

**Held up:**
- `gcloud projects get-iam-policy dw-main-bronze --filter=...mode-analytics...` reproduced live,
  right now: exactly one binding, `medallion_bronze_reader`. Matches §4.2 exactly.
- `gcloud iam roles describe medallion_bronze_reader --organization=104640274931` reproduced the
  exact `PERMISSION_DENIED` (`iam.roles.get`) cited as the residual risk — the terraform-based
  inference genuinely was the best evidence available, not a shortcut.
- `iam.tf` permission list, `terragrunt.hcl` project_id/layer_type/`mode` account mapping,
  `iam_bronze_extras.tf`'s `mode_analytics_access` resource — all match the cited lines.
  `test_heavy_task_is_a_finding_and_unattributed_is_not` exists in
  `airflow_optimizer/tests/test_bq_profile.py`.
- The 4 `bq_run.sh` log entries added to `knowledge/bq_perf_log.jsonl` match the "Measured cost"
  paragraph and the three CSVs exactly. Writes are confined to this ticket dir plus that one log
  file (the other dirty paths in the shared worktree at session start belong to AUDI-1277 and
  other concurrent work, not this ticket). The devops worktree is genuinely clean, no PR opened.
  `audi_1316_pr_body.md` / `audi_1316_result_comment.txt` pass `lint_comms.py --kind pr` /
  `--kind completion`.

**Defects:**
1. **The dry-run bytes claim has no evidence trail.** "4,863,570,618 bytes (4.86 GB) for the
   one-day window... 7 days 16.1 GB, 30 days 36.2 GB" (§4.4, §7) — the stated reason only
   single-day runs were possible, which drives open item #5 — appears nowhere in the diff.
   `bq_perf_log.jsonl` carries exactly 4 AUDI-1316 entries, all `phase:"full"`, zero dry-run; no
   output file records it either. Either the dry runs ran outside `bq_run.sh` (unlogged, against
   the standing rule) or the figures were never actually measured. Don't cite the "27x
   overstatement" gotcha or the single-day-only limitation as measured until this is resolved.
2. **§4.3's crossplane convention is contradicted by its own cited examples.** It claims "base
   dir = project owning the identity, not the project granted," but 3 of 4 identities it lists
   under `base/dw-main-bronze/iam/` (astro-atchurn-prod, jedi-media-spend-job, inflection) are
   owned by other projects (`hcdp-cmcv0v0ae01bk01ngimis9kjy`, `production-381211`,
   `dw-finance-compliance`) yet are filed under dw-main-bronze — the *target* project. The one
   real `mode-analytics` precedent already in the repo
   (`base/mntn-dm-prod-01/iam/mode-analytics/bucket-iam-data-monitoring.yaml`, a bucket grant on
   mntn-dm-prod-01 for an identity owned by dw-main-bronze) is filed under its target project too.
   The standby path (`base/dw-main-bronze/iam/mode-analytics/bigquery-permissions.yaml`) is still
   right here only because this grant is same-project (mode-analytics's home *is* dw-main-bronze,
   so owner and target coincide) — the stated general rule will misroute the next cross-project
   grant that cites it. Fix the rule to "target project," not "owning project."
3. **Open item #5's "11% gap... unexplained" is wrong; it's an exact match.** The two days that
   overlap AUDI-1278's own cited daily-report breakdown match this ticket's reconciliation table
   to the decimal: 08-31 is 592 jobs / 1,009.8 slot-h in both; 09-01 is 620 jobs / 977.6 slot-h in
   both. The 11% figure is an artifact of comparing this 3-day mean to AUDI-1278's *mismatched*
   7-day mean, which includes four additional, higher-volume days (08-26..08-30) not in this
   sample. Reconciliation is exact on every overlapping day; the "four more single-day runs" ask
   in item 5 is unnecessary and should be dropped.
4. **The Objective isn't met yet, and "done" overstates that.** §0 requires "A Mode section on
   report e81786de8403 showing unowned slot-hours per day" — that section doesn't exist (open item
   1, owner: user), and this file's own frontmatter still reads `status: in_progress`. The
   investigation, verdict, and artifacts are solid; the dashboard change itself is not shipped.

`jira_comment` (`artifacts/audi_1316_result_comment.txt`) is unchanged — none of the four defects
falsify anything it states.
