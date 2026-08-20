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

**Goal:** every script, watcher, and automation in this workspace runs as a dedicated non-human identity with the same capabilities it has today, and none of it depends on Malachi's personal account.

**Current personal-identity dependencies (inventory as of 2026-08-20):**
- **GCP** — `gcloud auth application-default login` as `malachi@mountain.com`. Used by `bq_run.sh`, every `gcloud storage`/`dataproc batches` call, the Dataproc cancel path. Expires and interrupts long jobs; the expiry has twice been misread as "the partition is empty".
- **Astro / Airflow** — `airflow_api.py` reads the active-context bearer out of `~/.astro/config.yaml` from `astro login`. The token expires roughly hourly, which is why the pollers shell out to `astro deployment list` to refresh mid-run.
- **Databricks — DONE 2026-08-20.** `~/.databrickscfg` held BOTH an OAuth profile (`malachi@mountain.com`) and a **long-lived PAT under `[DEFAULT]`**; the PAT was the pattern MNTN decommissioned with the Slack bot on 2026-06-10. The `[DEFAULT]` stanza is **deleted**, no backup kept, and it was already dead server-side (`databricks auth profiles` said `Valid: NO`, `databricks tokens list` returns empty). `.claude/databricks_setup.md` no longer instructs recreating it. **Still open:** the macOS keychain entry `databricks-ti837` holds the dead token and `.claude/scripts/databricks_smoke.py` still reads it (IMP-049) — clear with `security delete-generic-password -s databricks-ti837 -a "$USER"`.
- **Jira** — `JIRA_API_TOKEN` in `~/.zshrc`, a personal API token.
- **GitHub** — `gh` as `mdunn-mntn`; PRs, workflow dispatches, and repo reads all carry that identity.

**The constraint that shapes the design.** MNTN policy forbids long-lived local keys, so "make a service account and download a JSON key" is the wrong answer and would recreate the thing that killed `slack_bot/`. The shapes that fit: GCP **service-account impersonation** (`--impersonate-service-account`, no key file) or **Workload Identity Federation**; Astro **Deployment API tokens** scoped per deployment; Databricks **service principal with OAuth M2M**; a Jira/GitHub bot identity owned by the team rather than a person. See [[reference_pi5_server]] for why local keys are prohibited and [[reference_anthropic_api_key_keychain]] for the one sanctioned exception pattern.

**Why it matters beyond convenience.** Today every automation is a bus-factor of one, dies when a token expires mid-run, and attributes all prod actions to a single human in the audit log. Same class of risk as the Victor Savitskiy departure that stranded the Spark framework knowledge.

**Open:** who owns non-human identity provisioning at MNTN, whether Astro deployment tokens can dispatch the deploys we run today, and whether the Databricks service principal can read `system.billing` (see IMP-048).

Related: [[reference_airflow_ti_dev_testing]], [[reference_shopper_graph_deploy]], [[feedback_bq_workflow]].
