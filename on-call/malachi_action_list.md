---
doc_type: reference
title: Action list — what only Malachi can do
summary: "The short list of things blocked on Malachi personally, as of 2026-08-21: three PRs, two credential actions, two external tickets, one share-out. Everything else in improvements_backlog.md is owned by another team or sits in the working queue."
last_verified: 2026-08-21
keywords: [action list, todo, my list, what do i have to do, open prs, blocked on me, merge order, rotate token, revoke credentials, IMP-064, IMP-065]
tags: [workflow, status]
---

# Action list

Generated 2026-08-21, updated 2026-08-24. Only items **blocked on Malachi personally**. The 44 open rows in
`improvements_backlog.md` are mostly owned by other teams; those are tracked there, not here.

---

## 1. PRs — merge order matters

| Order | PR | State | Action |
|---|---|---|---|
| ~~1~~ | ~~[mntn-devops#4985](https://github.com/SteelHouse/mntn-devops/pull/4985)~~ | **CLOSED, superseded** | Cristina rebuilt it as Crossplane in [#4990](https://github.com/SteelHouse/mntn-devops/pull/4990), **merged and synced 2026-08-24**. Nothing to do |
| ~~1~~ | ~~[airflow-ti#1214](https://github.com/SteelHouse/airflow-ti/pull/1214)~~ | **MERGED 2026-08-24** | **Live and registered** on bundle `2026-08-24T19:00:21`, 0 import errors — but **arrived PAUSED**. Unpause when you want it running; it fires immediately for 2026-08-23 |
| — | [airflow#1497](https://github.com/SteelHouse/airflow/pull/1497) | open since **2025-05-24** | `TGT-4076: Ddp week dev`. Fifteen months old. Merge, rebase or close |

**The blocker cleared on 2026-08-24.** Verified live, not assumed: `airflow-debugger@` now holds
`dataproc.viewer` + `logging.viewer` on `mntn-prj-prod-00` and `aiplatform.viewer` +
`dataproc.viewer` + `logging.viewer` on `mntn-targeting-prj-prod`. The two bucket grants and the
`debugger/` prefix condition are in the merged manifest; I cannot read bucket IAM to confirm them
live (`storage.buckets.getIamPolicy` denied), so that half is manifest-verified only.

---

## 2. Credentials — two are live exposures

| # | Action | Why now |
|---|---|---|
| **a** | **Revoke `SLACK_BOT_TOKEN` and `ANTHROPIC_API_KEY`** | Both sat in plaintext in the decommissioned Slack bot's LaunchAgent, firing nightly from 2026-06-10 to 2026-08-20. Agent unloaded and plist deleted, but the credentials themselves are still valid. IMP-064 |
| ~~b~~ | ~~Rotate the Astro token~~ | **Decided 2026-08-24: leave it.** Raised twice, declined twice. If it is ever rotated, two places need the new value: the Keychain entry `astro_deployment_token` and the `AIRFLOW_BEARER` deployment variable |
| ~~c~~ | ~~Set the deployment variables~~ | **DONE 2026-08-24.** `AIRFLOW_BEARER` (secret) + `AIRFLOW_API_BASE` are set; the deployment restarted and a verification run diagnosed 7 of 7, 4 deterministically. IMP-065 closed |

`AIRFLOW_API_BASE` is `https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/api/v2` — take
it from `astro deployment inspect`, do not construct it.

**The DAG is live, unpaused and fully verified end to end as of 2026-08-24.** The GCS publish was
the last unconfirmed step and it now works: `rca_2026-08-23.json` and `.md` landed under
`gs://mntn-data-archive-prod/debugger/` at 00:37Z, so the `objectUser` prefix condition holds. It
took a code fix (airflow-ti#1215): `gsutil cp` stats its destination first and `storage.objects.list`
is evaluated against the BUCKET, which an object-prefix IAM condition can never grant. A JSON API
media upload is one request against one object name and stays inside the condition.

**Triggering a manual run has one trap worth writing down.** The `logical_date` must fall inside
`[the DAG's start_date, now]` — 2026-08-21 onward here. Outside that window the run reports
**success with zero task instances**, which is indistinguishable from a clean run unless you check
`total_entries` on `/taskInstances`. Cost three attempts.

---

## 3. Databricks — one internal ask, not two

**Settled 2026-08-24 by David Qiu (Databricks).** `system.lakeflow` is Databricks-managed and
enabled automatically; the `lakeflow system schema can only be enabled by Databricks` error is
expected and ignorable. The 2026-08-21 reading of it as "nobody internal can help" was wrong, and
the support ticket it produced is closed. Nothing here is external any more.

| # | Ask | Who |
|---|---|---|
| **a** | **Assign a metastore admin group** on metastore `c5dc6763-eaae-4d6c-9ae2-7af6147595bb` | An MNTN **account admin**, in the account console (Catalog > metastore > Metastore Admin > Edit). Console-only: no API, no Terraform. Post-Nov-2023 accounts ship with none assigned, which is why `owner` reads `System user` and every `system.*` grant denies |
| **b** | Then the grants, run by a member of that group | `USE CATALOG ON CATALOG system` (easy to omit, blocks on its own), then `USE SCHEMA` + `SELECT` on `system.lakeflow` and on `system.billing`. Same gate for both, so they land together. IMP-062 |

`system.billing.usage` is what turns the flexible-node-types cost commitment from a promise into a
measurement. `system.lakeflow.job_run_timeline` is the only enumeration surface for ephemeral dbt
submissions.

---

## 4. Needs your calendar or your judgement

| # | Item | Note |
|---|---|---|
| **a** | Share the debugger + optimizer with the team | IMP-025. The 14-tab workbook and the live demo are built and ready; this needs a slot, not more work |
| **b** | TI On Call Playbook | Four **NEEDS INPUT** sections are Sean's or Brian's to answer, then it pastes to Confluence. Due EOM |
| **c** | IMP-024 — the 242x skew finding | Route `Update Vertical Categorization` to its model owner. Verified, just needs sending |
| **d** | IMP-022 — Phase 3 in-DAG auto-fire | **Held by your decision.** Auto-PR permanently dropped. Nothing happens until you say so |

---

## What is NOT on your list

- **My working queue:** IMP-033, IMP-046, IMP-061 (optimizer detectors and ranking), IMP-063
  (deferrable Dataproc, blocked on a runtime bump).
- **Other teams' rows:** IMP-001 through IMP-018, IMP-034 through IMP-061 excluding the above —
  logged so they are not forgotten, owned elsewhere. `improvements_backlog.md` is the tracker.
- **IMP-050** as a whole is now mostly answered: the pattern is settled and shipped, and
  `on-call/service_account_ask.md` is the list to walk into the conversation with.
