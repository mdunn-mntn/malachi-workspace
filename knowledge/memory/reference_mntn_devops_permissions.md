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
