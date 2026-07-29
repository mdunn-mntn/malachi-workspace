---
name: Slack channel routing for cross-cutting questions
description: Which Slack channels to ask in based on the topic — UI vs reporting vs data engineering vs free-for-all
type: reference
originSessionId: 8c1b4f2e-277c-4422-8d1b-abc2a76e9d06
doc_type: memory
keywords: [slack_channel_routing, slack, channel, routing, channels, based, topic, reporting]
domain: [reference]
lifecycle: active
last_verified: 2026-07-16
---
Per Ray (2026-05-05), there is no single Slack channel that cleanly covers cross-cutting
UI + reporting + bidder-team questions. Routing guidance:

- **`#chapter-ui`** — UI side / how the product manages campaign data, Salesforce integration
- **`#chapter-data-engineering`** — data pipelines, Datastream/CDC, SQLMesh, BQ replication
- **`#chapter-data-analytics`** — reporting tables, business metric definitions, dashboard logic
- **`#data-platform`** — free-for-all on any data-related topic. Default if uncertain or if the
  question genuinely cross-cuts (UI + reporting + bidder + data engineering, etc.)
- **`#reporting_helpdesk_ask_anything`** — usable as a catch-all for reporting questions, but
  Ray noted it's not the right home for cross-cutting questions outside the reporting scope.
- **`#dev-mode-support`** — Mode (BI tool) questions: admin/API access, report issues (per Johnny,
  2026-07-16). Johnny is a likely Mode admin (per Robin Fox); Benny understands the mode-assets
  integration (confirmed it's one-way Mode→repo). Brian Gereke also wants API access (ally).

**Heuristic:** if the question touches the *write path* of data (UI, app, Salesforce), tag
`#chapter-ui`. If it touches the *read/transform path* (BQ, SQLMesh, pipelines), `#chapter-data-engineering`
or `#data-platform`. If it's a true cross-section, `#data-platform` is the most lenient channel.
