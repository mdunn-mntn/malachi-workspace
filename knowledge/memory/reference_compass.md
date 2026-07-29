---
name: compass-mntn-infra-investigator-atlas-code-mcp
description: "What Compass is, how to access it (Atlas Code MCP endpoint, compass-query, A2A), coverage gaps, secret-rotation policy"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 6b830d36-17fc-4962-b8c0-c9c838b6e689
doc_type: memory
keywords: [compass, access, atlas, code, endpoint, query, coverage, gaps]
domain: [reference]
lifecycle: active
last_verified: 2026-06-10
---
**Compass** = MNTN's internal multi-agent AI infrastructure investigator, embedded in Backstage (Internal Developer Platform). Router → parallel domain Advisors (Infra/Billing/GCP/Knowledge/GitHub) → live evidence knowledge graph with contradiction detection + provenance + readiness-based synthesis. Read-only (remediation via GitOps PRs). Built on MCP + Google A2A + ContextForge (IBM MCP gateway) + Grafana LGTM. Whitepaper 2026-03; org rollout 2026-06 (Harvey Yau). Questions: **#dev-basecamp**.

**Access:**
- **Atlas Code MCP** (300+ codebase/GCP/docs tools) → Claude Code: `claude mcp add mountain --transport http https://agent-gateway.management.in.mountain.com/mcp/code`
- **Compass-as-a-service:** HTTP `POST /api/basecamp-chat/a2a`, MCP `compass-query` tool via ContextForge, or Google A2A (`/message:send` / `:stream`).
- **Shareable conversation links:** `basecamp.in.mountain.com/mcp-chat?join=<uuid>`.

**Coverage gap:** Quickframe (QF) platforms mostly NOT covered (built outside core infra) — only thin billing/GCP. Don't use for deep QF work.

**Relevant to our killed Slack bot:** Compass's Slack integration + knowledge-doc authoring are ROADMAP (§9), not shipped — so Compass does NOT yet replace a scheduled Slack-scrape→markdown pipeline. See [[reference_pi5_server]] (bot decommissioned 2026-06-10) and `knowledge/mntn_business.md` Compass section.

**Secret policy (paired):** no local-env API keys / no local Slack apps. Secrets → SOPS-encrypted in ArgoCD repo, rotated via Basecamp tool (KMS Decrypt disabled for individuals; Vault optional/unsupported).
