---
doc_type: reference
title: Service Account Ask — every automation, every surface
summary: "The request list for moving this workspace's automations off a personal identity, rebuilt 2026-08-21 on the pattern AUDI-1194 actually shipped. Copy the spark-optimizer unit rather than re-deriving; do not ship a container. One row per identity with the shape, the owner, and what breaks without it."
last_verified: 2026-08-21
keywords: [service account, service accounts, non-human identity, bot account, deidentify, ask list, provisioning request, spark-optimizer, mntn-devops 4971, serviceAccountTokenCreator, CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT, storage.objectUser, astro deployment token, WORKSPACE_OWNER, databricks service principal, service-principal-secrets-proxy, IMP-050]
tags: [infra, identity, ask]
---

# Service Account Ask

**AUDI-1194 shipped this pattern to prod on 2026-08-21. Copy it; do not re-derive it.** The
`spark_optimizer_daily` DAG runs in `airflow-ti` under GSA `spark-optimizer@mntn-prj-prod-00`,
impersonated from the deployment's own ADC. Every remaining workload takes the same shape.

**The headline for the conversation: most of this is not a provisioning ask at all.** Two of the
five identities already exist, one is a copy of a merged Terragrunt unit, one is unnecessary if
the workload moves into a DAG, and one is genuinely open.

---

## Read this first: do not ship a container

The prior design here was Cloud Run Job + Cloud Scheduler + a GAR image + ArgoCD manifests + an
`mntn-helm` chart change. **All of it was built and then deleted.** Cristina Szumilo asked why a
job owned by AUDI was living in `mntn-devops` and attributed to the platform team, and moving the
workload into an `airflow-ti` DAG turned out cheaper than the design it replaced.

The move deleted the image, the GAR push, the ArgoCD manifests, the chart change — **and the
Astro API token**, because a DAG can enumerate DAGs locally instead of calling the REST API.

> **Before designing a store for a credential, check whether moving the workload removes the need
> for it.** That is the single most useful sentence to bring into the room.

The AUDI-1191 debugger took the same move on 2026-08-21 (PR #1214) — with one honest caveat worth
repeating in the room: **the move frees you from an API token only when the workload's input is DAG
metadata.** The optimizer's was. The debugger reads another task's *log*, which lives in
Astronomer's store, so it still needs a token (IMP-065). Check what a workload READS before
promising the move removes its credential.

---

## The ask, one row per identity

| # | Identity | State | What to do | Ask who |
|---|---|---|---|---|
| 1 | **GCP GSA** | **Done** — the debugger reuses `spark-optimizer@`, no new unit needed. For a future workload that wants its own, copy the 3-file unit at `terragrunt/gcp/resources/mntn/prod/platform/mntn-prj-prod-00/spark-optimizer/` (mntn-devops#4971) | mntn-devops review |
| 2 | **Astro Deployment API token** | **Now needed** (IMP-065) | The debugger reads task logs, which no DAG can read locally. Command below | **Ryan Kleck** (`WORKSPACE_OWNER`) |
| 3 | **Databricks service principal** | **Exists** — `spark_optimizer`, appId `07f36af7-614d-4d57-8143-2dbcd3cb58c2`, `CAN_USE` on warehouse `14b311ac86ee2ca2` | Mint a secret (below). Ask about `system.billing` only; `system.lakeflow` is Databricks-side | Databricks **account admin** (billing only) |
| 4 | **Jira bot identity** | Not started | Team-owned account with its own token, replacing `JIRA_API_TOKEN` in `~/.zshrc` | PMO / Jira admin |
| 5 | **GitHub** | — | **Ask for nothing.** The workloads read no repo | — |

---

## Row 1 — the two IAM details that a plausible precedent gets wrong

Both were caught in review of mntn-devops#4971 and both would have failed in production. Cite
them so the next PR does not repeat them.

**Impersonation is `roles/iam.serviceAccountTokenCreator`, not `roles/iam.workloadIdentityUser`.**
The DAG sets `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT`, so the *caller* is the deployment's
already-resolved GCP identity `airflow-ti-prod@mntn-prj-prod-00` — not a Kubernetes SA. Binding
the deployment's component KSAs with `workloadIdentityUser` is a **no-op**, and Cristina rejected
it; on a shared deployment it is also the wrong direction, since it would let any DAG in the
deployment become that account if the WI target ever moved. Peer to cite:
`terragrunt/.../mntn-gke-prod-01/iam/workload_identity/airflow_astronomer/`.

State the flow in the PR so a reviewer can check it in one line:

```
airflow-ti-prod (ADC) --TokenCreator--> impersonate <gsa> --> gcloud/gsutil as it
```

**GCS writes take `roles/storage.objectUser`, not `roles/storage.objectCreator`.** Creator grants
`storage.objects.create` only; replacing an object also needs `storage.objects.delete`. So a
creator-only grant works for date-stamped output and **fails from day two** for anything
republished under a stable object name — which is exactly the append-only ledger, the file that
separates a standing defect from a new one. The loss would have been quiet.

Keep the blast radius on the **IAM condition**, not on the weaker role:

```hcl
condition {
  title      = "optimizer-output-prefix-only"
  expression = "resource.name.startsWith(\"projects/_/buckets/<bucket>/objects/<prefix>/\")"
}
```

Rule of thumb: pick the role from what the workload does to the object, and the scope from the
condition. Reaching for `objectCreator` because it sounds least privileged buys nothing and
breaks the workload.

**Grants the shipped unit carries**, as the template to copy: `dataproc.viewer` on the project;
`storage.objectViewer` on `mntn-data-archive-prod` and the PHS temp bucket; `storage.objectUser`
conditioned to the output prefix.

---

## Row 2 — the Astro token, only if you actually need REST

**This is now a real ask, and it is the clearest one to lead with.** `airflow_debugger_daily`
(PR #1214) reads other tasks' logs, which on Astro Hosted live in Astronomer's store; a task's own
JWT is scoped to itself. Second user: the optimizer's `coverage.collect_local` is dead on Airflow 3
(`airflow session use is forbidden in this context`), and the REST API is the fix.

One token serves both. Neither is blocked meanwhile: the debugger treats a missing token as a skip
and runs green without it.

```bash
astro deployment token create \
  --deployment-id cmd6bd10c0gl901rfuokgryiq \
  --role DEPLOYMENT_ADMIN \
  --expiration 365 \
  --clean-output
```

Two things to say out loud when asking:

- **It needs `WORKSPACE_OWNER`.** Ryan Kleck has it. Precedent to point at: his
  `dag-run-duration-watchdog` token.
- **There is no read-only role in the org.** `DEPLOYMENT_ADMIN` is the floor, so this is not a
  least-privilege ask and should not be requested unless the REST path is actually needed.

---

## Row 3 — Databricks, the principal already exists

`spark_optimizer`, appId `07f36af7-614d-4d57-8143-2dbcd3cb58c2`, already holds `CAN_USE` on
warehouse `14b311ac86ee2ca2`. What remains is a secret and one grant.

```bash
databricks service-principal-secrets-proxy create 07f36af7-614d-4d57-8143-2dbcd3cb58c2
```

The non-proxy `databricks service-principal-secrets create` is **account-level and errors** —
that is the mistake to skip. An OAuth M2M secret is shown once; never echo it to a terminal or it
is in the scrollback. Store via Vault's Update Team Secret template under
`secrets/team-engineering-targeting/` (not the existing `databricks/` entry, which belongs to
ShopperGraph), and do not use `rotate-secret` on `mntn-team-credentials` — that breaks Vault
delivery (SOP 055).

**`system.lakeflow` IS an internal ask, corrected 2026-08-24.** The line here previously said it
was Databricks-side only, reading `lakeflow system schema can only be enabled by Databricks.` as a
wall. David Qiu (Databricks) settled it: lakeflow is Databricks-managed and enabled automatically,
so that error is expected and means nothing. The real blocker is `User does not have MANAGE on
Schema 'system.lakeflow'`, which needs a **metastore admin** — and MNTN has none assigned
(post-Nov-2023 accounts ship without one).

**Ask for the metastore admin group, not for individual schemas.** That one gate sits in front of
`system.lakeflow`, `system.billing`, `system.query`, `system.access` and `system.compute` alike, so
IMP-062's "is billing different" question is moot until it is set. Assignment is console-only
(Catalog > metastore `c5dc6763-eaae-4d6c-9ae2-7af6147595bb` > Metastore Admin > Edit) and must be a
group; then a member of that group runs `USE CATALOG ON CATALOG system` plus `USE SCHEMA`/`SELECT`
per schema.

---

## Row 5 — GitHub is deliberately empty

The debugger and the optimizer read no repo and open no PRs; that was a design constraint, not a
limitation. Publishing artifacts to GCS instead of committing them removes the only reason either
would touch GitHub. With the container path deleted, the OIDC and Octo STS questions are moot —
an `airflow-ti` DAG is inside the org's own deployment.

If a source lookup is ever wanted, the question for team-engineering-dev-ops is whether Octo STS
accepts a non-Actions issuer. **Never a PAT** — SOP 052's FAQ prohibits it outright.

---

## Already done, so it does not need asking

| Piece | State |
|---|---|
| **GSA + impersonation** | `spark-optimizer@mntn-prj-prod-00`, mntn-devops#4971 merged, verified in prod on run 1 (215 jobs, 290 findings) |
| **Databricks PAT** | Removed from `~/.databrickscfg` and the macOS keychain, both verified. `databricks_smoke.py` builds its session from the CLI OAuth profile with `$DATABRICKS_PROFILE` as the override — the service principal drops in with no code change |
| **Astro token plumbing** | `resolve_bearer()` checks `--token` then `$AIRFLOW_BEARER` before the astro context, and an explicitly supplied token is never auto-renewed from the personal one |
| **Decommissioned Slack bot** | Its LaunchAgent fired nightly from the 2026-06-10 decommission until 2026-08-20 with `SLACK_BOT_TOKEN` and `ANTHROPIC_API_KEY` in plaintext. Unloaded, plist deleted. **Both credentials still need revoking.** The other two agents carry no secrets |
| **`dataproc.viewer` on `mntn-prj-prod-00`** | Standing (DEV-8182) |

---

## What actually closes a workload

**IAM Policy Analyzer shows no personal-account binding remaining on the target resources.**

Supplementing the personal path is not the same as removing it — an automation that *can* use a
service account but falls back to ADC is still a bus factor of one. The check is the absence of
the old binding, not the presence of the new one.

---

## Still open

1. **`system.billing`** (IMP-062) — Databricks account admin. **`system.lakeflow` is off this
   list**: only Databricks can enable it, and a support ticket went in 2026-08-21.
2. **Jira bot identity** — nothing started.
3. **The debugger moved into a DAG on 2026-08-21** (airflow-ti PR #1214) and needed **no new
   identity** — it reuses `spark-optimizer@`. Two small follow-ups: an Airflow API token
   (IMP-065, `WORKSPACE_OWNER`, Ryan) because task logs are the one input a DAG cannot read
   locally, and widening the SA's `objectUser` condition to the `debugger/` prefix (IMP-066).

Mechanism detail: memory `reference_gcs_iam_creator_vs_user`. Workload and prod state:
`project_airflow_optimizer`. Standing inventory: `project_deidentify_personal_credentials`
(IMP-050). Superseded runner designs, kept only as history:
`tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_runner_and_identities.md` §3-§13.

### Databricks `system` schema access — the ladder, 2026-08-26

Two admin tiers, no substitution. `GRANT USE CATALOG ON CATALOG system` needs a **metastore
admin**; `GRANT USE SCHEMA` and `GRANT SELECT` on the schema each need an **account admin**.
Workspace `admins` confers neither. Alyson Lefkowitz holds both tiers. On this metastore
`account users` already has the catalog grant, so most asks are just the two schema lines.
Verify with `SHOW GRANTS ON SCHEMA <s>` through a SQL warehouse, not `databricks grants get`,
which served stale data for an hour after a live grant. Full ladder, error-to-rung mapping and
the ask shape that works: memory `reference_databricks_system_schema_grants`.
