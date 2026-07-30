---
name: Zach Schoenberger is authoritative on audience platform / holdout
description: For audience-platform, audience expression, holdout enforcement, and retargeting-vs-prospecting questions, Zach Schoenberger (Sr Principal Architect) is the highest-confidence source at MNTN. Defer to him over docs, code reads, or other team members.
type: reference
originSessionId: 1ddedb6a-ff08-4281-8285-aab919ee6906
doc_type: memory
keywords: [audience platform authority, Zach Schoenberger, holdout enforcement, audience expression, retargeting vs prospecting, CRM lists, Jordan Piepkow, SegmentExpressionService]
domain: [routing-people, audience-scoring]
lifecycle: active
last_verified: 2026-07-29
---
**Authority:** Zach Schoenberger — Sr Principal Architect, primary owner of the audience platform + bidder evaluation logic.

**Use when:**
- Anyone asks how audience expressions are evaluated, wrapped, or stored
- Anyone questions holdout enforcement (which campaigns have it, how it's enforced)
- Questions about CRM lists / OPM / TPA / first-party audience plumbing
- Disagreements between code reads, docs, or other team members on audience-platform behavior

**Three universal rules he confirmed 2026-04-30 (in `#targeting` Slack):**
1. CRM lists are only usable in prospecting campaigns, never retargeting.
2. Every campaign has a 10% holdout. Universal — no exceptions.
3. Every campaign has an audience expression. No campaigns without one.

These three are now in `knowledge/data_knowledge.md` under "Prospecting vs Retargeting (Audience Type)."

**Other notable Zach calls of authority:**
- 2026-03-03 / 03-04: Stage definitions (S1/S2/S3 are targeting stages, not event types)
- 2026-03-13: Impression trace paths (CTV vs display order, where bid_ip lives)
- 2026-03-24: bid_ip COALESCE fallback pattern when bid_logs.ip is purged
- 2026-04-30: page_view vs visit clarification (guid_log = page views, clickpass_log = visits)

**Co-expert for SegmentExpressionService.kt code-level questions:** Jordan Piepkow (Staff SWE) — referenced for the OPM→TPA wrap mechanism (`MNTNFirstParty.dataSourceId=2`).

**How to apply:** when an audience-platform question comes up, ask: "Is this a Zach question?" If yes, default to either checking memory for prior Zach answers or pinging him directly. Don't propagate uncertain audience-platform claims through analysis — get them confirmed.
