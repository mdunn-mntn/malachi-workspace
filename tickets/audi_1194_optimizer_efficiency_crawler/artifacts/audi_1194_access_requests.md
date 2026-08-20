# AUDI-1194 — access requests

**BLUF: I'm moving an automated Spark efficiency optimizer off my laptop onto a scheduled
runner. It reads only — event logs, Dataproc metadata, DAG names, query plans — and writes one
GCS prefix. It needs a non-personal identity on four surfaces. Nothing here grants write access
to a repo, a DAG, or a table.**

Below is every ask, grouped by who owns it. Each is one line.

---

## DevOps — GCP (one mntn-devops PR)

1. **Service account `spark-optimizer@mntn-prj-prod-00.iam.gserviceaccount.com`**, with:
   - `roles/storage.objectViewer` on `gs://mntn-data-archive-prod`
   - `roles/storage.objectViewer` on `gs://dataproc-temp-us-central1-995798185124-svhwvc6j`
   - `roles/storage.objectCreator` on `gs://mntn-data-archive-prod/optimizer/` (prefix only)
   - `roles/dataproc.viewer` on `mntn-prj-prod-00`
2. **A Cloud Run job + Cloud Scheduler trigger** in `mntn-prj-prod-00`, running as that SA once
   a day. Scheduler SA needs `roles/run.jobsExecutor` on this job only.
3. **Merge [mntn-devops#4724](https://github.com/SteelHouse/mntn-devops/pull/4724)** — bucket-scoped
   `objectViewer` on the Dataproc PHS temp bucket for `audience-intelligence@mountain.com`. Open
   as a draft since 2026-08-07, CI green, no conflicts, now out of draft with reviewers requested.
   Without it the optimizer can see only half the fleet.

**Two questions I need answered before I can write the PR:**

4. **Where does the compute manifest for a scheduled Cloud Run job live?** I found the IAM for
   `daily-jedi-media-spend` in Terragrunt, but `kind: V2Job` greps empty in both
   `mntn-argocd` and `argocd-v2/mgmt/platform/crossplane`.
5. **Direct bindings on the service account, or add it to `audience-intelligence@`?** I'd prefer
   direct so the grant is visible to the IAM audit (it can't expand Workspace groups), but tell
   me if the group is the house pattern.

## Databricks — Victor / TI

6. **OK to create a service principal `spark_optimizer`?** The workspace has `prod_runner` and
   `dev_runner` in `producers_prod` / `producers_dev`. This one is read-only and should be in
   **no** `producers_*` group, which cuts across the naming convention, so I stopped rather than
   invent a third pattern. I have workspace admin and can create it myself on a yes.
7. **`CAN_USE` on SQL warehouse `14b311ac86ee2ca2`** and **`CAN_VIEW` on jobs** for that principal.
8. **`USE SCHEMA` on `system.lakeflow`** — currently denied. It's the only way to enumerate the
   ephemeral dbt job submissions; `jobs list` doesn't surface them.

## Astro / Airflow — Victor / TI

9. **Does a custom read-only deployment role exist?** The optimizer only calls `GET /dags` and
   `GET /dags/{id}/tasks`. The built-in options are `DEPLOYMENT_ADMIN` or a custom role, and I'd
   rather not hand a scheduled job admin. If none exists, I'll mint a `DEPLOYMENT_ADMIN` token
   for deployment `cmd6bd10c0gl901rfuokgryiq` with a 365-day expiry.

## Secrets — team-engineering-targeting

10. **A new `TeamSecret` sibling under `secrets/team-engineering-targeting/`** holding the Astro
    token and the Databricks client id/secret, via the **Update Team Secret** template in
    `mntn-team-credentials` (not `rotate-secret` — that breaks Vault delivery). The team already
    owns a `databricks` entry for ShopperGraph; I want a separate entry so revoking one workload
    doesn't hit the other. **Who approves that PR?**

## GitHub — no ask

11. The runner needs **no GitHub identity**. Artifacts publish to GCS instead of a repo, so
    there's nothing to commit. Flagging it so nobody grants it by default. If source lookup is
    wanted later, the question is whether Octo STS accepts a non-Actions OIDC issuer (SOP 060 is
    Actions-scoped), and the answer is never a PAT.

---

**Why now:** every credential the sweep uses today is mine and expires. The Astro token dies in
about an hour, the Databricks OAuth refresh is interactive, and the gcloud session dies with SSO.
A stale token produces a green cron run and an empty report, which is worse than a failure.

**Constraint I'm holding to:** no downloaded service-account key, and no long-lived credential on
a laptop. The Cloud Run job's attached identity covers GCP with no secret at all; only the two
third-party tokens need a store, and that store is Vault.
