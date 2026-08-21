---
doc_type: reference
title: Service Account Ask — every automation, every surface
summary: "The concrete request list for moving this workspace's automations off a personal identity. One row per identity to provision, with the shape, the owner to ask, and what breaks without it. Companion to memory project_deidentify_personal_credentials and the AUDI-1194 runner design."
last_verified: 2026-08-20
keywords: [service account, service accounts, non-human identity, bot account, deidentify, ask list, provisioning request, astro deployment token, databricks service principal, gcp impersonation, workload identity federation, jira bot, github bot, IMP-050]
tags: [infra, identity, ask]
---

# Service Account Ask

**Five identities, four owners, and only one of them needs devops.** Everything scheduled in this
workspace runs as `malachi@mountain.com` today. This is the list to walk into the conversation
with: what to provision, in what shape, and what stops working if it is not.

The binding constraint, before anything else: **MNTN policy forbids long-lived local keys.** A
downloaded service-account JSON key is the wrong answer and would recreate the thing that got
`slack_bot/` decommissioned on 2026-06-10. Every row below is a keyless or brokered shape.

**Worth raising in the room, because it is the argument for all of this:** auditing for this
document found a `SLACK_BOT_TOKEN` and an `ANTHROPIC_API_KEY` sitting in plaintext in a macOS
LaunchAgent plist, still firing nightly ten weeks after that surface was decommissioned. Nobody
was checking `launchctl list`, because the credential inventory only ever looked at the repo and
the shell profile. A plist is a credential store.

---

## The ask, one row per identity

| # | Identity | Covers | Shape (no keys) | Ask who | Without it |
|---|---|---|---|---|---|
| 1 | **GCP service account** — `ti-oncall-automation@mntn-prj-prod-00` | `bq_run.sh` (every query), `gcloud storage`/`dataproc batches` reads, the Dataproc cancel path, the debugger's Cloud Logging reads | **Impersonation** (`--impersonate-service-account`) or Workload Identity Federation. Never a downloaded key | team-engineering-dev-ops | ADC expires mid-run; the expiry has twice been misread as "the partition is empty" |
| 2 | **Astro Deployment API token** — `airflow-ti` deployment | `airflow_api.py`, `airflow_pull.sh`, the 10:00 on-call RCA job, the 11:00 optimizer sweep | Deployment API token, read-only. `astro deployment token create` mints it from the CLI | **Astro platform, not MNTN devops.** Gate is Astro org access (Dustin Niehoff, #devops, 2026-08-20) | The SSO token expires hourly; a long poller 401s partway through |
| 3 | **Databricks service principal** — proposed `spark_optimizer` | The optimizer's `EXPLAIN COST` reads, job/cluster inspection | **OAuth M2M**, secret in Vault under `secrets/team-engineering-targeting/`. In **no** `producers_*` group — it reads, it never produces | Victor / TI. Self-serve in practice: `malachi@mountain.com` holds workspace `admins` | No non-human path to Databricks at all |
| 4 | **Jira bot identity** | Ticket comments, transitions, issue creation | Team-owned account with its own API token, not `JIRA_API_TOKEN` in `~/.zshrc` | PMO / whoever owns Jira admin | Every automated board action is attributed to one human |
| 5 | **GitHub** | — | **Ask for nothing.** See below | — | — |

### Row 5 is deliberately empty, and that is the recommendation

The debugger and the optimizer read repos and open no PRs — that was settled as a design
constraint, not a limitation. Publishing artifacts to GCS instead of committing them removes the
only reason either would touch GitHub.

Two facts make "none" the *easy* answer rather than the cautious one:

- **The prod OIDC pool `mntn-prj-prod-gh-oidc` allow-lists 23 `SteelHouse/*` repos.**
  `mdunn-mntn/malachi-workspace` is personal. Putting a personal repo on a prod OIDC allow-list
  means anyone with push access to it can mint prod tokens.
- **Octo STS (SOP 060) is GitHub-Actions-only** by its own scope statement — it brokers a token
  using an Actions runner's OIDC token, which a GCP compute workload does not have. There is
  currently no paved road for a Cloud Run job that needs to read a repo.

If a source lookup is ever wanted, the question for team-engineering-dev-ops is whether Octo STS
accepts a non-Actions issuer — **not** a PAT, which SOP 052's FAQ prohibits outright.

---

## What is already done, so it does not need asking

| Piece | State |
|---|---|
| **Databricks PAT** | Removed from `~/.databrickscfg` and from the macOS keychain (`databricks-ti837`), both verified. `databricks_smoke.py` builds its session from the CLI OAuth profile, with `$DATABRICKS_PROFILE` as the override — **a service principal drops in with no code change** |
| **Astro token plumbing** | `resolve_bearer()` checks `--token` then `$AIRFLOW_BEARER` **before** the astro context. A Deployment API token drops in with no code change, and an explicitly supplied token is never auto-renewed from the personal context |
| **Decommissioned Slack bot** | Its LaunchAgent had been firing every midnight since the 2026-06-10 decommission, failing on a missing module — and its `EnvironmentVariables` held `SLACK_BOT_TOKEN` and `ANTHROPIC_API_KEY` in plaintext the whole time. Unloaded 2026-08-20 and the plist deleted. **Both credentials should be revoked.** The other two agents carry no secrets |
| **`dataproc.viewer` on `mntn-prj-prod-00`** | Standing grant, already written against `group:audience-intelligence@mountain.com` (DEV-8182) |
| **`objectViewer` on the PHS temp bucket** | [mntn-devops#4724](https://github.com/SteelHouse/mntn-devops/pull/4724), open since 2026-08-07 |

So the GCP half is largely **one mntn-devops PR**, not a provisioning project.

---

## Two design calls worth defending in the room

**Give the SA its own bindings; do not put it in the group.** The org's IAM audit cannot expand
Google Workspace group membership — that needs a standing Workspace admin role it does not hold.
A grant that reaches a principal *through a group* is therefore invisible to the org's own audit.
The group also contains humans, which blurs attribution. Direct bindings stay visible in CAI and
traceable to one IaC file. (Still open: whether the org nonetheless *prefers* the group pattern.
That is a human ruling, not a query — the dataset that would settle it is the one that cannot see
groups.)

**Prefer eliminating the credential over storing it.** SOP 052 leads with "if identity works, no
secret is allowed." Row 5 exists because of this: the cheapest secret is the one the design does
not need.

---

## What actually closes a workload

**IAM Policy Analyzer shows no personal-account binding remaining on the target resources.**

Supplementing the personal path is not the same as removing it — an automation that *can* use a
service account but falls back to ADC is still a bus factor of one. The check is the absence of
the old binding, not the presence of the new one.

---

## Still open, and genuinely devops

1. Where a scheduled Cloud Run job manifest is defined (the runner shape settled as Cloud Run Job
   + Cloud Scheduler with an attached SA).
2. Whether a new runtime SA takes its own IAM bindings or joins
   `group:audience-intelligence@mountain.com` — the design call above, needing a ruling.
3. Whether Astro Deployment API tokens can dispatch the deploys we run today, or only read.

Full per-workload design and the two Compass reviews behind it:
`tickets/audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_runner_and_identities.md`.
Standing inventory and lifecycle: memory `project_deidentify_personal_credentials` (IMP-050).
