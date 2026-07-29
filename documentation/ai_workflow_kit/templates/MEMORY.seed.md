# <ORG> Workspace Memory — hot tier (always loaded)

> **This file is the HOT TIER: only facts relevant to (nearly) every session.** Everything else is
> grep-on-demand and costs 0 tokens until you reach for it:
> - **Find a fact:** grep `knowledge/_ROUTING.md` for your term → open the one `memory/<file>.md` it names.
> - **Browse all memory:** `knowledge/_MEMORY_INDEX.md` (by domain) · refresh queue: `knowledge/_MEMORY_LIFECYCLE.md`.
> - **Add a memory:** `/capture` writes `knowledge/memory/<slug>.md` (`doc_type: memory` + keywords). New
>   task-specific facts do **NOT** get a line in this file — only genuinely always-on ones do. That is what
>   keeps this file small and the index unbounded.

## Retrieval rule
- **Load indexes, not the tree.** Start at `knowledge/START_HERE.md` → grep `_ROUTING.md` (keyword→doc) →
  open only the one doc a map names. Indexes are grepped, never ingested whole, so they grow without bound.

## Always-on working rules (how I write & work)
- **BLUF / terse.** Lead every human-facing comm (chat, ticket comment, chat threads, deck, standup) with
  the conclusion; cut filler. If a sentence doesn't change what the reader decides, cut it.
- **No em-dashes** in written deliverables. Use a period or comma.
- **Simplest deliverable** — no invented terms/columns, plain facts + caveats, no unsolicited next-steps.
- **Sparse code comments** — one line max if ever; put the why in the PR/commit/ticket, not block comments.
- **Rank descending** — primary metric, most on top, every table/chart.
- **Commit and push after every meaningful change** — small, frequent commits; never batch.

## Working with the system
- **Tickets** — `/frame` opens a ticket (framing gate blocks `in_progress` until locked); `/capture`
  closes it (routes what was learned to its home doc + memory, rebuilds the index). Don't extend a
  stale/reassigned ticket — start a new one.
- **On-call** — any alert → `/oncall`; classify alert-vs-ticket first; log to §3 incident + §2 signature +
  `incident_log.jsonl`; never hot-patch prod.
- **Background/async work** — never passive-wait; arm a stall-detector Monitor. A HUNG task sends NO
  completion notification, so waiting on the notification alone can stall silently.
- **Commit gate** — a malformed staged file or a bad commit message is blocked by `.githooks/`. Fix with
  `.claude/scripts/verify.sh --fix` then re-stage, or `git commit --no-verify` to bypass.

## Warehouse module (only if this workspace queries a warehouse)
- Always query via `.claude/scripts/bq_run.sh` (perf log + provenance); sample/APPROX first; dry-run
  unfamiliar SQL. Set project/region/datasets in `.claude/scripts/config.env`.

<!-- Add your project's own always-on stack gotchas below this line, one dense block. Keep this file
     small — anything not needed nearly every session belongs in a per-fact memory/<slug>.md instead. -->
