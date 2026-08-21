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

**The ask list to walk into the conversation with: `on-call/service_account_ask.md`** (5 identities, 4 owners, only 1 needs devops; row 5 is GitHub and is deliberately empty).

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

Settled there, and reusable for every other workload in this stream:
- **Shape: Cloud Run Job + Cloud Scheduler with an attached SA.** NOT GitHub Actions — the prod OIDC pool `mntn-prj-prod-gh-oidc` allow-lists 23 `SteelHouse/*` repos, and `mdunn-mntn/malachi-workspace` is personal. Putting a personal repo on a prod OIDC allow-list means anyone with push access can mint prod tokens.
- **Secrets → Vault via `mntn-team-credentials`**, Update Team Secret template, sibling entry per workload. Not Secret Manager: a third-party credential fails SOP 052's Google-only test. See [[reference_compass]] for the SOP 052/055/060/065 detail and the verified team path.
- **Direct bindings on the SA, not group membership** — the IAM audit cannot expand Workspace groups, so a group-routed grant is invisible to the org's own audit.
- **Prefer eliminating the credential over storing it.** SOP 052 leads with "if identity works, no secret is allowed". Publishing the sweep's artifacts to GCS instead of committing them removed the GitHub identity entirely — the cheapest secret is the one the design does not need.
- **Octo STS (SOP 060) is Actions-only** and does not reach a GCP compute workload, so there is currently no paved road for a Cloud Run job that needs to read a repo.

**Verification that actually closes a workload:** IAM Policy Analyzer shows **no personal-account binding remaining** on the target resources. Supplementing the personal path is not the same as removing it.

**Ownership answers (Dustin Niehoff, #devops, 2026-08-20).** Two of the four unknowns close, and neither needs devops:
- **Databricks:** Victor set up all of it for TI; DPLAT wanted nothing to do with Databricks. Route to Victor / TI. Self-serve in practice, since `malachi@mountain.com` holds workspace `admins`.
- **Astro:** "astro owns them" — the deployment service accounts / API tokens are managed inside the Astro platform, not by MNTN devops. Mint from the Astro UI; the gate is Astro org access, not a devops ticket.

Still open and genuinely devops: where a scheduled Cloud Run job manifest is defined, and whether a new runtime SA takes its own IAM bindings or joins `group:audience-intelligence@mountain.com`.
