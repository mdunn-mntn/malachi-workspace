---
name: project_deidentify_personal_credentials
description: "Next major work stream (raised 2026-08-20): move every automation off Malachi's personal identity onto dedicated non-human identities for GCP, Astro/Airflow, Databricks, Jira and GitHub, with identical capability and without reintroducing long-lived keys."
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [service account, service accounts, non-human identity, personal credentials, deidentify credentials, off my account, gcloud application-default, ADC, workload identity federation, impersonation, astro login, astro api token, deployment api token, AIRFLOW_BEARER, databricks OAuth, databricks PAT, dapi token, JIRA_API_TOKEN, gh auth, bus factor credentials, no local keys, key-free auth, automation identity]
domain: [infra, workflow, project]
lifecycle: active
last_verified: 2026-08-20
---
Raised 2026-08-20 as the likely next major work stream after AUDI-1191 and AUDI-1194 wrap.

**The ask list to walk into the conversation with: `on-call/service_account_ask.md`** — rebuilt 2026-08-21 on the pattern AUDI-1194 actually shipped. Two of the five identities already exist, one is a copy of a merged Terragrunt unit, one is unnecessary if the workload moves into a DAG.

**Goal:** every script, watcher, and automation in this workspace runs as a dedicated non-human identity with the same capabilities it has today, and none of it depends on Malachi's personal account.

**Current personal-identity dependencies (inventory as of 2026-08-20):**
- **GCP** — `gcloud auth application-default login` as `malachi@mountain.com`. Used by `bq_run.sh`, every `gcloud storage`/`dataproc batches` call, the Dataproc cancel path. Expires and interrupts long jobs; the expiry has twice been misread as "the partition is empty".
- **Astro / Airflow** — `airflow_api.py` reads the active-context bearer out of `~/.astro/config.yaml` from `astro login`. The token expires roughly hourly, which is why the pollers shell out to `astro deployment list` to refresh mid-run. **The code side is already done:** `resolve_bearer()` checks `--token` then `$AIRFLOW_BEARER` BEFORE the astro context, so a Deployment API token drops in with no code change. `astro deployment token create` mints one from the CLI (no UI needed, contrary to the note below). Minting it is a credential-creating act on the shared org, so it waits for an explicit go.
- **Databricks — DONE 2026-08-20.** `~/.databrickscfg` held BOTH an OAuth profile (`malachi@mountain.com`) and a **long-lived PAT under `[DEFAULT]`**; the PAT was the pattern MNTN decommissioned with the Slack bot on 2026-06-10. The `[DEFAULT]` stanza is **deleted**, no backup kept, and it was already dead server-side (`databricks auth profiles` said `Valid: NO`, `databricks tokens list` returns empty). `.claude/databricks_setup.md` no longer instructs recreating it. **Keychain cleared 2026-08-20 (IMP-049 done):** the `databricks-ti837` entry is deleted and verified absent, and `databricks_smoke.py` now builds its session from the CLI OAuth profile via `Config(profile=...)`, with `$DATABRICKS_PROFILE` as the override so a service principal on OAuth M2M drops in without a code change. **No Databricks token is read from disk or keychain anywhere in this workspace.**
- **Jira** — `JIRA_API_TOKEN` in `~/.zshrc`, a personal API token.
- **GitHub** — `gh` as `mdunn-mntn`; PRs, workflow dispatches, and repo reads all carry that identity. **The recommendation is to ask for nothing here** — the automations read no repo, and the paved road does not reach a GCP compute workload anyway.
- **Decommissioned but still scheduled, and still holding secrets (found 2026-08-20).** The `com.mntn.slack-knowledge-bot` LaunchAgent had been firing every midnight since the 2026-06-10 decommission, failing on a missing `slack_sdk`. Its `EnvironmentVariables` block held **`SLACK_BOT_TOKEN` and `ANTHROPIC_API_KEY` in plaintext** — for ten weeks after the surface was decommissioned. Agent unloaded and the plist **deleted, not archived**: archiving it into the repo is what surfaced the secrets, when GitHub push protection rejected the commit. **Treat both credentials as exposed and revoke them.**
  Two durable lessons: (1) decommissioning code does not decommission its scheduler entry — audit `launchctl list` (and `crontab -l`) when auditing identities, not just the repo; (2) a plist is a credential store. `~/Library/LaunchAgents/*.plist` is as much a place secrets hide as `~/.zshrc` or `~/.databrickscfg`, and nothing in this workspace was checking it.

**The constraint that shapes the design.** MNTN policy forbids long-lived local keys, so "make a service account and download a JSON key" is the wrong answer and would recreate the thing that killed `slack_bot/`. The shapes that fit: GCP **service-account impersonation** (`--impersonate-service-account`, no key file) or **Workload Identity Federation**; Astro **Deployment API tokens** scoped per deployment; Databricks **service principal with OAuth M2M**; a Jira/GitHub bot identity owned by the team rather than a person. See [[reference_pi5_server]] for why local keys are prohibited and [[reference_anthropic_api_key_keychain]] for the one sanctioned exception pattern.

**Why it matters beyond convenience.** Today every automation is a bus-factor of one, dies when a token expires mid-run, and attributes all prod actions to a single human in the audit log. Same class of risk as the Victor Savitskiy departure that stranded the Spark framework knowledge.

**Open:** who owns non-human identity provisioning at MNTN, whether Astro deployment tokens can dispatch the deploys we run today, and whether the Databricks service principal can read `system.billing` (see IMP-062).

Related: [[reference_airflow_ti_dev_testing]], [[reference_shopper_graph_deploy]], [[feedback_bq_workflow]].

**AUDI-1194 is the first workload through this (2026-08-20).** The daily Spark optimizer sweep is the pilot: it reads GCS + Dataproc, calls the Airflow API and Databricks, and today runs on a laptop under personal SSO. Full design + two Compass reviews: `tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_runner_and_identities.md`.

**SUPERSEDED 2026-08-21 — the runner design was built and then deleted.** The Cloud Run Job + Cloud Scheduler + GAR image + ArgoCD manifests + `mntn-helm` chart change all went away when the workload moved into an `airflow-ti` DAG, which was cheaper than the design it replaced and **removed the Astro API token dependency entirely** (a DAG enumerates DAGs locally instead of calling REST). The OIDC and Octo STS reasoning below is now moot for these workloads: a DAG runs inside the org's own deployment.

**The reusable lesson, and the one to lead with:** *before designing a store for a credential, check whether moving the workload removes the need for it.*

Still true and still reusable:
- **Secrets → Vault via `mntn-team-credentials`**, Update Team Secret template, sibling entry per workload. Not Secret Manager: a third-party credential fails SOP 052's Google-only test. See [[reference_compass]].
- **Direct bindings on the SA, not group membership** — the IAM audit cannot expand Workspace groups, so a group-routed grant is invisible to the org's own audit.
- **Prefer eliminating the credential over storing it.** SOP 052 leads with "if identity works, no secret is allowed."

Kept as history only (do not build from these): Cloud Run Job shape; the prod OIDC pool `mntn-prj-prod-gh-oidc` allow-listing 23 `SteelHouse/*` repos while `mdunn-mntn/malachi-workspace` is personal; Octo STS (SOP 060) being Actions-only and unreachable from a GCP compute workload.

**Verification that actually closes a workload:** IAM Policy Analyzer shows **no personal-account binding remaining** on the target resources. Supplementing the personal path is not the same as removing it.

**The GCP shape is settled and shipped (2026-08-21).** GSA `spark-optimizer@mntn-prj-prod-00` (mntn-devops#4971, merged), impersonated from the deployment's own ADC `airflow-ti-prod@` via `roles/iam.serviceAccountTokenCreator` and `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT` — **not** `workloadIdentityUser`, which is a no-op here and was rejected in review. GCS writes take `roles/storage.objectUser` scoped by an IAM condition on the prefix, **not** `objectCreator`, which cannot overwrite. Copy the 3-file unit at `terragrunt/gcp/resources/mntn/prod/platform/mntn-prj-prod-00/spark-optimizer/` for the next workload. Mechanism: [[reference_gcs_iam_creator_vs_user]].

**Astro tokens, corrected 2026-08-21:** `astro deployment token create --deployment-id cmd6bd10c0gl901rfuokgryiq --role DEPLOYMENT_ADMIN --expiration 365 --clean-output`. It needs **`WORKSPACE_OWNER`** — Ryan Kleck has it, Malachi does not — and **there is no read-only role in the org**, so `DEPLOYMENT_ADMIN` is the floor and this is not a least-privilege ask. Precedent: Ryan's `dag-run-duration-watchdog` token. Only worth requesting if a workload genuinely needs the REST API.

**Databricks, corrected 2026-08-21:** the service principal **already exists** — `spark_optimizer`, appId `07f36af7-614d-4d57-8143-2dbcd3cb58c2`, `CAN_USE` on warehouse `14b311ac86ee2ca2`. Mint its secret with `databricks service-principal-secrets-proxy create <id>`; the non-proxy `service-principal-secrets create` is account-level and errors. **`system.lakeflow` is not obtainable internally at all** — enabling it returns `lakeflow system schema can only be enabled by Databricks` (2026-08-21), so the route is a Databricks support ticket, filed that day. `system.billing` (IMP-062) is a separate schema and may still be an internal ask; do not carry the lakeflow answer over to it. See [[reference_databricks]].

**Ownership answers (Dustin Niehoff, #devops, 2026-08-20).** Two of the four unknowns close, and neither needs devops:
- **Databricks:** Victor set up all of it for TI; DPLAT wanted nothing to do with Databricks. Route to Victor / TI. Self-serve in practice, since `malachi@mountain.com` holds workspace `admins`.
- **Astro:** "astro owns them" — the deployment service accounts / API tokens are managed inside the Astro platform, not by MNTN devops. Mint from the Astro UI; the gate is Astro org access, not a devops ticket.

**Both of those closed 2026-08-21.** There is no Cloud Run manifest, because there is no Cloud Run job; the SA takes its own direct bindings in the copied Terragrunt unit. What is still open: `system.billing` (Databricks account admin; `system.lakeflow` is Databricks-side only), a Jira bot identity (nothing started), and moving the AUDI-1191 debugger off its laptop cron into an `airflow-ti` DAG — the same move, needing no new identity beyond a copy of the `spark-optimizer` unit.
