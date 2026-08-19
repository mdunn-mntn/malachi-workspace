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

## Auth topology (verified unauthenticated, 2026-08-19)

Probed with plain `curl`, no credentials. An unauthenticated POST to the MCP endpoint returns
`401 {"error":"unauthorized","error_description":"JWT token required"}` with
`WWW-Authenticate: Bearer resource_metadata="https://mcp.ex.mountain.com/.well-known/oauth-protected-resource/mcp"`,
so it implements RFC 9728 discovery correctly.

- Protected-resource metadata resolves at both `/.well-known/oauth-protected-resource` and
  `.../oauth-protected-resource/mcp`. `mcp_protocol_version` is `2025-06-18`.
- Authorization server is **`https://auth-proxy.ex.mountain.com`**, a separate host from the MCP server.
- Scopes are `openid profile email offline_access` plus **`mcp:read`**. Read-only is enforced at the
  token scope, not only by convention, which is a stronger guarantee than the guide's prose claim.
- Endpoints: auth `/oidc/auth`, token `/oidc/token`, dynamic registration `/oidc/reg`, PKCE S256.

**Defect found: the authorization server publishes two disjoint key sets.** Same issuer, two discovery
documents, two different `jwks_uri`, and zero shared key ids:

| Discovery doc | `jwks_uri` | key ids served |
|---|---|---|
| `/.well-known/oauth-authorization-server` | `/.well-known/jwks.json` | `mntn-auth-1`, `mntn-auth-2` |
| `/.well-known/openid-configuration` | `/oidc/jwks` | `mntn-oidc-1` |

Both return HTTP 200. Any client or downstream service that validates a token against the set it did not
issue from fails signature verification. The two documents also disagree on `grant_types_supported`
(`implicit` appears only in the OIDC doc), `response_types_supported`, and
`token_endpoint_auth_methods_supported`. **RESOLVED 2026-08-19: Benny confirmed the second key set is deadweight and no longer relevant**, so this
is dead config rather than a live signature risk. Kept here because both documents still resolve and a
client that discovers via `openid-configuration` still reads the orphan `jwks_uri`; if it disappears,
that is the cleanup landing.

## Tool surface (17 tools, inventoried 2026-08-19)

`list_advertisers` `get_advertiser` · `list_campaigns` `get_campaign` `get_flight` · `list_audiences`
`get_audience` `get_campaign_audience` `list_audience_campaigns` · `list_geo_lists`
`get_geo_list_locations` `search_geo_locations` · `search_keywords` · `list_reference_data` ·
`get_reporting_metadata` `run_report` · `submit_feedback`.

`get_reporting_metadata` must be called before `run_report`; only its curated `Table.Column` tokens are
accepted. Reporting metrics are `Graph.{Impressions, Spend, TVSpend, Visits, VisitRate, CostPerVisit,
VisitAssists, Conversions, OrderValue, ConversionRate, AverageOrderValue, ConversionAssists, ROAS, CPA,
UsersReached, TVCommercialsAired, Multi-Touch*}`; dimensions are `Graph.{Day,Week,Month,Quarter,Year}`
plus `MarketingObjectiveInfo.Name`, `ChannelInfo.Name`, `CampaignInfo.Name`,
`CreativeInfo.CreativeGroupName`, `StateInfo.Name`, `MediaMarketInfo.Name`, `CityInfo.Name`.
`submit_feedback` is an in-band channel to MNTN engineering; it requires the user's explicit yes.

## Warehouse diff — the numbers are right (verified 2026-08-19)

WGU (advertiser 31357), 2026-08-01 to 2026-08-07, `run_report` against
`silver.summarydata.sum_by_advertiser_by_day`:

| Metric | Result across all 7 days |
|---|---|
| Impressions | **exact** every day |
| Spend | **exact** to the cent |
| Visits | **exact** every day |
| Conversions | **exact** every day |
| UsersReached | within **-0.82% to +1.30%** of `HLL_COUNT.EXTRACT(uniques)`, i.e. inside HLL++ error |
| OrderValue | `$0.00`, and the warehouse is genuinely 0 for WGU. **Not** the `order_amt_usd` trap |

**It reports the UI / `industry_standard` lens**, meaning default last-touch **plus `competing_*`**, not
the raw last-touch headline. On 2026-08-01 that is 186,235 visits, and the default lens would have been
158,821, a 17% gap. Matching the UI is the correct choice for a customer-facing API, and it means an MCP
number can be compared to `views + clicks + competing_views` but never to `views + clicks` alone.

A `MarketingObjectiveInfo.Name` x `ChannelInfo.Name` grid also matched the warehouse exactly on all four
cells (Prospecting/Retargeting x Television/Multi-Touch), where `Television` is `channel_id = 8` and
`Multi-Touch` is `channel_id = 1`. `MediaMarketInfo.Name` returns clean DMA labels with no "Other"
bucket in the top 8, so it does not show the PS-8614 pattern; its values were **not** diffed.

## Defect: a multi-dimension report silently drops a dimension by default

`run_report` with `sum: ["MarketingObjectiveInfo.Name", "ChannelInfo.Name"]` and the default
`fullName: false` returns rows keyed by the column **short** name. Both dimensions are named `name`, so
they collapse into a single `Name` key and only the channel survives:

```
{"Impressions": "15,990,494", "Name": "Television"}
{"Impressions": "18,178,330", "Name": "Television"}
```

Two rows, same label, different numbers, and the strategy is gone. The row totals are correct and the
grid is complete, so nothing looks broken; the data is just unattributable. Setting **`fullName: true`**
returns `MarketingObjectiveInfo.Name` and `ChannelInfo.Name` as separate keys and resolves it.

**Always pass `fullName: true` when grouping by more than one dimension.** This is worst for an LLM
client, which will narrate the collapsed rows confidently. Filed to MNTN engineering 2026-08-19 via the
server's own `submit_feedback` tool, id **`fb_fea70a02-7235-46b3-a475-2f678049dcbd`**, category `bug`,
severity `medium`. **Benny confirmed 2026-08-19: being fixed and deployed** — re-test before relying on
the default. That tool posts under the **MNTN OAuth identity**, not the Claude account, and its
contract requires showing the user the exact text and getting an explicit yes before sending.

Minor: `MarketingObjectiveInfo.Name` help text says "Deprecated, use CampaignStrategyInfo.Name", but
`CampaignStrategyInfo.Name` is not in the accepted token enum.

## Join key: MCP "campaign" is our campaign_group

Verified on 10 WGU campaigns: every MCP `campaign.id` resolves in `silver.public.campaign_groups` and
**none** resolves in `silver.public.campaigns`. MNTN's public model is campaign owns flights, with no
line item; our warehouse keeps an extra internal stage layer below it. So an MCP campaign id joins to
`campaign_group_id`, never `campaign_id`.

There is **no funnel-stage dimension** in the reporting catalog. MCP campaign 24081 covers internal
campaigns spanning `objective_id` 1, 5, 6 and 7 and `funnel_level` 1 through 4, all flattened into the
single strategy "Prospecting". The MCP `objective` field is correct at group grain, so the
"`objective_id` is unreliable" caveat in `knowledge/data_knowledge.md` applies to the internal per-stage
row, not to what this API returns.

