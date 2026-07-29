---
name: project_qfai_transfer_interest
description: Malachi interested in moving to QuickFrame AI (QFAI) as SWE; Richard Girges (CTO) call 2026-07-14; stack + team facts from Anne Whitman; keep OFF the org-visible workspace repo
metadata: 
  node_type: memory
  type: project
  originSessionId: 54557611-656e-4aef-9231-a931ab21ea99
doc_type: memory
keywords: [qfai, quickframe ai, transfer, richard girges, anne whitman, creative-suite, emily sgroi, ber-2250, swe move]
domain: [project, routing-people]
lifecycle: active
last_verified: 2026-07-14
---
Malachi is exploring a move to the QFAI (QuickFrame AI) team as a SWE — "given my manager will let me go" (Kale not yet asked as of 2026-07-14). Richard Girges (CTO) called him about QFAI on 2026-07-14; purpose unconfirmed (recruiting / analytics ask / diligence).

**Team facts (via Anne Whitman, QFAI eng, ex-QFMP — QFMP was shut down and her team absorbed):**
- Stack: TypeScript monorepo `SteelHouse/creative-suite` — React 19 micro-frontends (Module Federation), Slate.js + Remotion editor, Fastify BFF + tRPC, Prisma + Postgres (Neon), Temporal workflows, Argo/K8s, Auth0. AI gen via OpenAI, Vertex, FAL, ElevenLabs (Seedance 2.0 default video model as of 2026-07).
- Repo (checked 2026-07-14): ~29MB TS, 8 apps (editor, server, media-service, render-service, spark, admin...) + 11 packages (QFDS design system), ~100 commits/8 days, 28 contributors, `.claude/` dir + Claude skills in repo (AI-native team). Credits/tiered-subscription billing live; heavy mobile push.
- Manager: Emily (Jaffe) Sgroi — universally praised. Tejas Widjonarko = Sr TPM. Pace is intense (Richard sent a "whoa" intensity message to the team ~July 2026); team got a comp week off. Anne's candid frustrations (planning churn, scrapped work, merge conflicts) are CONFIDENTIAL — never attribute or repeat.
- QFAI analytics is thin: Anne (FE-focused) part-times Datadog/RUM, Segment + BigQuery work just starting. QFAI creative tracking = Tableau + `viva-server` Neon DB. Compass barely covers QF platforms.

**Richard's #engineering-team recruiting message (~July 2026, the "whoa" message):** openly recruiting "a couple of SWEs" for QFAI, bypassing leads ("man of the people"). Product scope he listed: AI model integrations, SSR React website, generation-orchestration UI/BE, core experience, monetization/billing, **analytics**. QFAI = **separate business from MNTN PTV**, B2C scale: 200–1,300 signups/day, target 10K/day by EOM July (":dead:"). Culture per Richard himself: no roadmap beyond a few weeks, all Kanban, he gets heavily involved and "stresses the team out"; wants "insane, a little unstable, and/or want a change of pace." Transfers contingent on active priorities, team capacity, "how angry your manager gets at me" (Richard owns the manager negotiation). Tone: irreverent, self-deprecating — match energy, no corporate-speak.

**Sensitivities:** Malachi is primary owner of BER-2250 (Kale's #1 Q2 priority) — any transfer needs a transition story ("active priorities" contingency). Don't lead with QFAI→PTV creative-performance synergy — Richard explicitly frames QFAI as standalone B2C. Never commit career-move material to the workspace repo (org-visible). Related: [[mntn_leadership_chain]], [[project_incrementality_experiment]].
