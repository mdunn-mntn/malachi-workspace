---
doc_type: reference
title: Action list — what only Malachi can do
summary: "The short list of things blocked on Malachi personally, as of 2026-08-21: three PRs, two credential actions, two external tickets, one share-out. Everything else in improvements_backlog.md is owned by another team or sits in the working queue."
last_verified: 2026-08-21
keywords: [action list, todo, my list, what do i have to do, open prs, blocked on me, merge order, rotate token, revoke credentials, IMP-064, IMP-065]
tags: [workflow, status]
---

# Action list

Generated 2026-08-21. Only items **blocked on Malachi personally**. The 44 open rows in
`improvements_backlog.md` are mostly owned by other teams; those are tracked there, not here.

---

## 1. PRs — merge order matters

| Order | PR | State | Action |
|---|---|---|---|
| ~~1~~ | ~~[mntn-devops#4985](https://github.com/SteelHouse/mntn-devops/pull/4985)~~ | **CLOSED, superseded** | Cristina rebuilt it as Crossplane in [#4990](https://github.com/SteelHouse/mntn-devops/pull/4990), **merged and synced 2026-08-24**. Nothing to do |
| ~~1~~ | ~~[airflow-ti#1214](https://github.com/SteelHouse/airflow-ti/pull/1214)~~ | **MERGED 2026-08-24** | Not live yet: the Astro bundle is still on `2026-08-21T20:02`. Check `bundle_version`, not the green deploy |
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
| **b** | **Rotate the Astro deployment token**, then update the one Keychain entry | It was pasted into a chat. `astro deployment token rotate --deployment-id cmd6bd10c0gl901rfuokgryiq`, then `security add-generic-password -a "$USER" -s astro_deployment_token -w '<new>' -U` |
| **c** | Set `AIRFLOW_BEARER` + `AIRFLOW_API_BASE` as **secret** deployment variables in Astro | The Keychain copy only serves the laptop. Without this the DAG itself skips instead of diagnosing. IMP-065 |

`AIRFLOW_API_BASE` is `https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/api/v2` — take
it from `astro deployment inspect`, do not construct it.

---

## 3. External tickets — nobody at MNTN can do these

| # | Ask | Who |
|---|---|---|
| **a** | `system.lakeflow` enablement | **Databricks support.** Enabling it returns `lakeflow system schema can only be enabled by Databricks`, so no customer-side admin can. You said you would file this |
| **b** | `system.billing` read (`USE SCHEMA` + `SELECT ON system.billing.usage`) | Databricks **account/metastore admin**. Different schema, not known to carry the lakeflow restriction. This is what turns the flexible-node-types cost commitment from a promise into a measurement. IMP-062 |

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
