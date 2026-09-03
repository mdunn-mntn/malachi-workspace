---
name: reference_mntn_devops_permissions
description: How to get GCS/IAM/infra permission or config changes at MNTN — make a mountain-devops PR, Cristina approves, it self-deploys (don't file a DevOps ticket first)
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [devops, permissions, csz-mntn, gh pr ready, request reviewer, CODEOWNERS, draft PR stalled, mountain-devops, IAM, GCS write access, storage.objects, Cristina, terraform, self-service permission, dataproc service account, infra config change, cluster_log_conf, PR approver]
domain: [infra, routing-people, workflow]
lifecycle: active
last_verified: 2026-08-20
---
**To get a cloud permission / IAM / infra-config change at MNTN, make a PR against the `mountain-devops` repo and add Cristina as the approver — do NOT file a blocking DevOps ticket first** (Ryan Kleck, 2026-08-04).

**Why:** "DevOps is the new DBAs — they're there to block you." The self-service path is faster: clone/download the `mountain-devops` repo, make the change as a PR (e.g. grant a service account `storage.objects.create/list/get` on a GCS prefix), and ping Cristina ("my bot said I need this permission to write X to Y, can you check it?"). She approves most of the time; on merge it **self-deploys** and you're done.

**How to apply:**
- Need a GCS write, a new IAM binding, a Dataproc/Databricks config, etc. that you can't set yourself → **try the setting first to see the exact error/permission it wants**, then encode that as a `mountain-devops` PR rather than a ticket.
- Reviewer/approver = **Cristina Szumilo, GitHub `csz-mntn`** (DevOps). Approve → merge → auto-deploy.
- **Request the review explicitly — a PR nobody is assigned to just sits.** mntn-devops#4724 stayed a DRAFT with ZERO reviewers from 2026-08-07 to 2026-08-20 with all CI green and `mergeable: true`; nothing technical was blocking it. `gh pr ready <n>` then `gh pr edit <n> --add-reviewer SteelHouse/devops --add-reviewer csz-mntn`. CODEOWNERS on `mntn-devops` is the catch-all `* @SteelHouse/devops`, and every other open PR there requests that **team**, so request the team AND ping Cristina.
- **`gh pr update-branch`** handles drift: #4724 was 219 commits behind main and still `mergeable`/`rebaseable` because terragrunt IAM PRs add a new file rather than editing a shared one.
- Use this for the AUDI-1191 Databricks event-log delivery (the Databricks user can't write to the `spark-events` GCS folder → needs a mountain-devops PR for that grant). See [[project_airflow_debugger]].

## IAM at MNTN is Crossplane now, not a Terragrunt unit (2026-08-24)

**Cristina Szumilo migrated both AUDI service-account units to Crossplane** and merged them:
[#4992](https://github.com/SteelHouse/mntn-devops/pull/4992) for `spark-optimizer` (originally
#4971) and [#4990](https://github.com/SteelHouse/mntn-devops/pull/4990) for `airflow-debugger`
(originally my #4985, now **closed**). Her reason, worth carrying forward: **Crossplane syncs
automatically, so nobody has to remember to apply a plan when the file changes.** A Terragrunt IAM
unit is only as live as the last person who ran it.

**So: write the next IAM change as Crossplane manifests, not a Terragrunt unit.** Path shape:

```
argocd-v2/mgmt/platform/crossplane/managed-resources/prod/manifests/base/
  <project>/iam/<identity>/
    service-account.yaml      kind: ServiceAccount        (crossplane.io/external-name)
    project-permissions.yaml  kind: ProjectIAMMember      (one doc per role, incl. cross-project)
    bucket-iam.yaml           kind: BucketIAMMember       (condition supported, keep it)
    token-creator.yaml        kind: ServiceAccountIAMMember
    kustomization.yaml        + a line in the parent iam/kustomization.yaml
```

Details that matter, from reading the merged manifests:
- `apiVersion: storage.gcp.upbound.io/v1beta2`, provider `providerConfigRef: {name: default}`.
- **Sync order is `argocd.argoproj.io/sync-wave`** — the SA is an earlier wave than its bindings.
- **`deletionPolicy: Orphan`** on the bindings, so removing a manifest does not revoke live access.
- **A cross-project grant is just another `ProjectIAMMember` with a different `project:`** — it
  lives under the *identity's home project* directory, not the target project's.
- **IAM conditions survive the migration** — the `debugger/` prefix condition came across intact.

**Verify a migration by reading live IAM, not the diff.** A migration is exactly where a grant
gets quietly dropped. `gcloud projects get-iam-policy <project> --flatten="bindings[].members"
--filter="bindings.members:<sa>" --format="value(bindings.role)"` confirmed all five project
grants. Note that **bucket** IAM is not readable this way without `storage.buckets.getIamPolicy`,
which `malachi@mountain.com` does not have, so bucket bindings can only be manifest-verified.

Supersedes the Terragrunt-unit shape recorded above for anything new. See
[[reference_gcs_iam_creator_vs_user]] for the role choices themselves, which did not change.

## Medallion layer roles (org 104640274931)

**Org custom roles:** `medallion_bronze_reader`, `medallion_silver_reader`, `medallion_gold_reader` (defined in `terragrunt/gcp/resources/mntn/dplat/modules/dw-medallion-layer/iam.tf` lines 19-43) carry **broad read/write access**, not table-scoped:
- Reader roles include: `bigquery.jobs.create`, `bigquery.jobs.get`, `bigquery.jobs.list`, `bigquery.jobs.listAll`, `bigquery.tables.getData`, `bigquery.reservations.use` plus storage and cloud-sql reads.
- Writer roles add: `bigquery.tables.create`, `bigquery.tables.delete`, `bigquery.datasets.update` plus storage and cloud-sql writes.

These are bound to layer projects (e.g., `medallion_bronze_reader` on `dw-main-bronze`) and to service accounts that need layer-wide read/write access (e.g., `mode-analytics@dw-main-bronze` holds `medallion_bronze_reader` to query `INFORMATION_SCHEMA.JOBS_BY_PROJECT`). **Not table-scoped by role definition.**
