---
name: reference_mntn_public_mcp
description: "MNTN Public MCP beta (https://mcp.ex.mountain.com/mcp) — customer-facing read-only remote HTTP MCP over advertisers/campaigns/audiences/reporting, OAuth 2.0 dynamic client registration; registered in this repo's .mcp.json; NOT the internal data-eng on-call MCP"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [mntn public mcp, mcp.ex.mountain.com, mntn mcp, public mcp, mcp beta, mcp quickstart, custom connector, remote http mcp, oauth dynamic client registration, streamable http, benny, daniella kubiak, rob mckee, evan klemen, maribel, mntn account data, read-only mcp, IMP-047]
domain: [workflow, infra, product]
lifecycle: active
last_verified: 2026-08-19
---

**MNTN ships a customer-facing MCP server.** Announced to eng 2026-08-19 by Benny (demo by Rob McKee at
Eng All Hands, with Evan Klemen and Maribel). Beta, pre-GA, opened for feedback before external rollout.
Feedback goes to **Benny or Daniella Kubiak**.

**This is NOT [[reference_data_eng_mcp]].** That one is the internal Dataproc/Spark on-call diagnosis
server at `data-eng-ai.in.mountain.com` (Ryan Kleck, AUDI-1190). Different server, different audience,
different data. Both answer to the keyword "mcp", so check the hostname before assuming which is meant.

## Connection facts (from the quickstart, verified 2026-08-19)

| | |
|---|---|
| Server URL | `https://mcp.ex.mountain.com/mcp` (exact, no trailing slash) |
| Transport | HTTP, streamable |
| Auth | OAuth 2.0 with **dynamic client registration**, redirects to MNTN login |
| Scope | Read-only. Retrieval operations only, no write or delete |
| Data | Advertisers, campaigns, audiences, reporting |
| Prereq | An active MNTN account **with API access enabled** |

Never paste an MNTN password or API token into a client config. The OAuth flow is the only credential
path, and it is the guide's own instruction.

## How it is wired here

Registered in the repo's `.mcp.json` as `"mntn": {"type": "http", "url": "https://mcp.ex.mountain.com/mcp"}`.
`.claude/settings.local.json` already sets `enableAllProjectMcpServers: true`, so the entry is live with
no further config. Complete the handshake with `/mcp` in an **interactive** session; it cannot run in a
non-interactive one.

Two gotchas worth remembering:
- **`claude mcp add` does not work on this machine.** `claude mcp list` returns `Error: claude native
  binary not installed`. Edit `.mcp.json` directly.
- **Use the Claude Code path, not the Claude.ai connector path.** The guide's Team/Enterprise route needs
  a workspace Owner to add the connector org-wide before any member can connect. `.mcp.json` sidesteps
  that admin dependency for the same URL and the same OAuth flow.

Guide troubleshooting: a connection timeout means a malformed URL; an OAuth screen that never appears or
a login that loops means clear cookies for `mcp.ex.mountain.com` and confirm API access is on for the
account; connected-but-empty means ask the assistant to list advertisers first and reference the returned ID.

## Source doc

`documentation/docs/mntn_public_mcp_quickstart.pdf`. **Untracked on purpose**, since `.gitignore` line 27
is `*.pdf` and no PDF on the docs shelf is in git. The table above is therefore the durable copy of the
connection facts. Drive original is `1FMmeVfvfK80S57CBa2utsUBu_ns_rt0_VOZ7Sc1Ui9M`, not readable from this
machine (gcloud carries no Drive scope, the Drive MCP is authed to a personal gmail, see
[[reference_mntn_google_drive_access]]).

## Evaluation status: NOT YET RUN

Registered but **not yet authorized**, so nothing about its output is verified. The advertised prompts are
week-to-date advertiser performance, active campaigns with month-to-date spend, last week's conversions by
media market, and audiences linked to a campaign.

The quickstart **ships no tool inventory**, so the callable surface is unknown until a client connects and
introspects. That gap is itself a feedback item.

Planned ground-truth diff (IMP-047), each item chosen because the warehouse answer is known and the
metric is a documented trap:

| Check | What would be wrong |
|---|---|
| Conversions and revenue | using `ui_conversions.order_amt_usd`, which is always NULL, instead of `order_amt` |
| Conversions by media market | metro resolution, the soft spot behind IMP-017 `location_data metro_id` |
| Audiences on a campaign | returning `audiences` (templates) when the targeting lives in `audience_segments` |
| New vs returning | inheriting the client JS pixel `is_new` mismatch, where 41 to 56 percent is normal |
| Unique visitors across Sep 2025 | reading `agg__daily_sum_by_campaign`, where uniques collapse to ~0 |
| Funnel stage split | keying on `objective_id`, which is unreliable, rather than `funnel_level` |
| CTV vs display split | `channel_id` 8 is CTV, 1 is display |

Ground truth comes from `bq_run.sh` only. Any confirmed disagreement is a customer-facing defect, not a
beta nit, and gets appended to `knowledge/data_knowledge.md` beside the verified line rather than
replacing it.
