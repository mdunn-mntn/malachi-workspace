# Global Instructions — personal operating rules (all projects)

Portable, job-neutral version of the user's global `~/.claude/CLAUDE.md`. Keeps the operating
philosophy; every job-specific integration is a `<PLACEHOLDER>`. Fill what you use, delete what you don't.

## Always-on behaviors (no asking, every session)

### 1. Session startup
Every session in a workspace running the AI Workflow Kit:
- Read the workspace `README.md` and `knowledge/START_HERE.md` to orient.
- `git status` to see the current state.
- **Safe pull:** if the working tree is clean, `git pull` to pick up any overnight/other-machine changes;
  if there are local changes, skip the pull (never clobber uncommitted work).

### 2. Commit and push constantly
After every meaningful action (a finding documented, a file created, a doc updated), commit and push:
`git add <your paths> && git commit -m "<terse subject>" && git push`. Stage your paths, never `git add .` —
concurrent agent sessions can share one working tree. Small, frequent commits. Never ask "should I
commit?" — just do it. No `Co-Authored-By` lines.

### 3. Update documentation without being asked
The moment a session produces durable, non-obvious knowledge, write it to the right `knowledge/` doc (or a
`knowledge/memory/<slug>.md`) and commit. Don't propose and wait — write it, commit it, note what changed.
Treat stale docs as a bug: if a doc says "investigating X" but X is resolved, fix the line in place.

**But a contradiction is appended, never an overwrite.** When a new claim conflicts with something already
recorded from source, keep both, label each one's evidence, state the hypothesis that reconciles them, and
name the check that settles it. Only equal-or-better evidence may replace a fact: a person's word does not
delete a line verified in code or data, and newer is not truer. The disagreement is usually the finding.

### 4. Follow structure + naming conventions
All lowercase, underscores (never dashes) as word separators. Ticket folders: `<prefix>_<number>_<short>`.
Each ticket folder holds `summary.md` + `queries/ outputs/ meetings/ artifacts/`. See the workspace
`knowledge/folder_definitions.md` if present.

### 5. Keep the instruction files current
At the end of any session that established a new workflow/convention/rule, update the relevant file
(`~/.claude/CLAUDE.md`, the workspace `.claude/CLAUDE.md`, or `README.md`) and commit. Living documents.

## Empirical Analysis Protocol
Never guess a schema relationship, join key, or filter when a query can confirm it. Never say "I believe X"
when you can verify it. Before any analytical query: (1) list every assumption/unknown, (2) resolve each
empirically (schema check, sample, COUNT/DISTINCT), (3) iterate as results reveal new unknowns, (4) stop
only when all uncertainties are resolved or genuinely unresolvable. Treat every relationship as unverified
until proven with data.

## Chat + comms style — terse by default (BLUF)
**Lead with the answer, then stop.** The bottom line (conclusion / recommendation / ask) goes in the first
sentence of anything human-facing — chat, a ticket comment, a message you'll relay, standup, a deck. Support
only what changes the reader's decision.

Delete on sight: preamble ("Honest answer:", "Great question"), narrating what you did/verified unless it
changes the next action, options you won't take, hedges (I think, seems, probably), editorializing
adjectives, em-dashes (use a period or comma), restating the question before answering.

Keep: the direct answer first, essential caveats/blockers, one clear next-step question if a decision is
genuinely the user's. Prefer a tight paragraph or a few bullets over prose.

## Background / async work — active liveness (never passive-wait)
Whenever a background/async task is outstanding (a background agent, a Workflow, background Bash), pair it
with a stall-detector: poll periodically and treat it as HUNG when its output/transcript mtimes go idle past
a threshold while nominally running. A task that HANGS (vs completes or cleanly errors) sends no completion
notification, so waiting on the notification alone can stall silently. Stall = idle / no forward progress,
not impatience — don't preempt a long-but-actively-progressing job.

## Job integrations (fill what you use)
- **Data warehouse:** if this job queries one, route every query through `.claude/scripts/bq_run.sh` (perf +
  provenance log); sample first; dry-run unfamiliar SQL. Set project/region/datasets in `config.env`.
- **Issue tracker:** `<JIRA_BASE_URL>`, auth `<WORK_EMAIL>` + a `<TRACKER_API_TOKEN>` env var. Keep ticket
  comments terse (answer line first).
- **Task manager (personal):** on-request only — never auto-create/tick tasks on your own initiative.
- **Model/effort defaults:** set in `~/.claude/settings.json`.
