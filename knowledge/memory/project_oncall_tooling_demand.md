---
name: project_oncall_tooling_demand
description: Ryan Kleck asked unprompted (2026-09-04) for AI/MCP tooling for team on-call, which is external demand for IMP-107's setup path; his own PagerDuty MCP bot died to the same token revocation that killed our Slack bot.
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [on-call tooling demand, Ryan Kleck, PagerDuty MCP, IMP-107, adoption setup path, AUDI-1325, MCP bot revoked tokens, security incident token revocation, Compass alternative, basecamp compass, Bryce Wagg, confluence incident board, TI On Call Playbook, sync up on on-call, company-wide adoption]
domain: [project, infra, routing-people]
lifecycle: active
last_verified: 2026-09-04
---
**Ryan Kleck asked, unprompted in Slack on 2026-09-04, whether there is an AI/MCP tool that helps
with team on-call** — specifically something that integrates with PagerDuty and reads notes from
Confluence to help triage alerts. Bryce Wagg redirected him to Malachi. Malachi answered honestly:
no PagerDuty integration, but there is a Confluence board tracking past incidents and their
solutions (the TI On Call Playbook, page `2908061697`). Ryan asked to sync up.

**Why this matters:** it is the first external pull for [[project_airflow_debugger]] /
[[project_airflow_optimizer]] adoption. IMP-107 (the setup path a stranger can follow: access to
request, env vars, what a correct first run looks like) was ranked P5 and explicitly DEFERRED in the
2026-09-04 priority plan, on the reasoning that correctness had to come before sharing. A peer team
lead asking for it unprompted is a reason to revisit that tier, not a reason to drop the
correctness work that preceded it.

**How to apply:** when the sync happens, the honest position is that the debugger and optimizer are
wired to this squad today (`OPTIMIZER_BQ_SAS` is our service account, `phs.TEAM` is our batch label,
the Slack channels and the AUDI Jira project are ours) and that nothing states the prerequisites a
new team must satisfy. That gap IS IMP-107. Coverage numbers to bring, measured 2026-09-04 under
AUDI-1329: the debugger names a root cause for 88% of the failures Airflow reports failed, 46% of
all failure events, and 0% of the silent class; the optimizer scans 28% of the fleet.

**Ryan already built this and lost it.** He had an MCP bot integrated with PagerDuty, Confluence,
git and Slack, and revoked all its tokens after the security incident, which killed it. That is the
same policy that decommissioned our Slack bot on 2026-06-10 and the same constraint the LLM-layer
question (IMP-109) sits behind — see [[reference_pi5_server]] and
[[reference_anthropic_api_key_keychain]]. Any joint proposal has to answer the credential story
first, and the relevant fact is that `OPENAI_API_KEY` already exists on the prod Astro deployment
and is read by `include/dbx/kube_operators.py`; the open question is ownership, not feasibility.

**Ryan also named Compass/basecamp as an alternative** worth considering before building anything
new — see [[compass-mntn-infra-investigator-atlas-code-mcp]].
