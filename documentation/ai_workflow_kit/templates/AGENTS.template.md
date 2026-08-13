# Agent Instructions — <ORG> Workspace

> Portable instruction file. Written to the `AGENTS.md` convention so any harness that reads a
> project instruction file gets the same rules. **Keep one copy**: make the other filename a symlink
> (`ln -s AGENTS.md CLAUDE.md`) so the rules can never diverge between tools.
>
> Hot path only. Behavioral rules and pointers live here; procedure lives behind a trigger (§7) and
> reference lives in `knowledge/` behind the generated indexes. Every line here is paid for on every
> request — if it isn't always-on, it belongs in a triggered file.
>
> Replace every `<PLACEHOLDER>` before relying on this file. See `PORTING.md`.

---

## 0. Response style: terse by default

**This rule outranks everything below it. Nothing in this file is a license to write more.** The
rules below say what to *do*; this one caps what to *say*. Rules about writing, documenting, or
capturing produce **files and commits, not longer replies**.

Lead with the answer, then stop. Default to a tight paragraph or ≤3 short bullets. Going long needs
an explicit ask first, or content that genuinely cannot compress (a required table, a diff, code).

**Delete on sight:** preamble ("Great question", "Here's the thing"); narrating what you did or
didn't verify unless it changes the next action; options you won't take; exhaustive surveys when a
recommendation was wanted; hedges; editorializing adjectives; restating the question; closing
summaries and "let me know if…" lines.

**Keep:** the direct answer first, essential caveats and blockers, one next-step question if the
decision is genuinely the human's.

Under-explaining is the cheap error (they ask a follow-up). Over-explaining is the expensive one (it
buries the answer and doesn't get read).

---

## 1. Session start

- Read `README.md` and `knowledge/START_HERE.md`.
- Check `git status`. Pull only when the tree is clean — never pull over uncommitted work.
- For anything else, **grep the indexes rather than pre-loading docs**.

## 2. Retrieval: load indexes, not the tree

`knowledge/START_HERE.md` (task → doc) → `knowledge/_ROUTING.md` (keyword → doc) → **the one doc**,
or one `##` section of it. The indexes are generated from front-matter and are meant to be **grepped,
never read whole**. Never ingest a directory to find something.

## 3. Commit and push constantly

After every meaningful action — a file created, a doc updated, a finding recorded — commit and push
immediately. Never batch. Never ask "should I commit?".

**Stage your own paths. Never `git add .` or `git add -A`.** Concurrent agent sessions can share one
working tree, and a blanket add sweeps another session's in-flight edits into your commit. Run
`git diff --cached --name-only` before committing; anything you did not touch is someone else's.

No `Co-Authored-By` lines.

## 4. Write new knowledge immediately

The moment a durable, non-obvious fact is **confirmed or disproven**, write it to the owning doc in
`knowledge/` and commit. Do not propose and wait.

**A contradiction is appended, never an overwrite.** When a new claim conflicts with a fact already
recorded from source, keep both, name each one's evidence, state the hypothesis that reconciles them,
and name the check that settles it. Only equal-or-better evidence may replace a fact: a person's
word, however senior, does not silently delete a line verified in code or data. Deleting the old fact
destroys the disagreement, which is usually the finding.

**Treat a stale doc as a bug.** If a doc says "investigating X" and X is resolved, fix the line in
place, in the same beat.

## 5. Empirical analysis protocol

Never guess a schema relationship, join key, or filter condition when a check can confirm it. Never
say "I believe X" when you can verify it. Treat every relationship as unverified until proven.

Before any analytical work: list every assumption and unknown, resolve each empirically, and iterate —
each result reveals new unknowns.

**Figure it out yourself before escalating.** Exhaust the ladder: inspect the schema → sample or
count → the knowledge docs → a different source or approach → re-read the request. Escalate only when
the blocker needs institutional knowledge or access you don't have, and bring what you tried, why
it's unresolvable, who can answer it, and a draft question ready to send.

## 6. Structure and naming

Lowercase and underscores only, no dashes. One folder per unit of work; scaffold it with the
scaffolder script, never by hand. Phases are **headers inside a file**, never folders. Artifact
folders stay flat with monotonic `NN_slug.ext` names.

Full rules: `knowledge/folder_definitions.md`.

## 7. Triggered procedures

Multi-step work with judgement in it lives in a named procedure, not in this file. Harnesses invoke
them differently — the procedure file is the source of truth either way.

Each procedure is one directory holding a `SKILL.md`. Put them in `.agents/skills/` — Codex and
Cursor both read that path, so one copy serves both.

| Procedure | Fires when |
|---|---|
| **frame** | before work starts on any new unit of work |
| **capture** | at any stopping point, or when a durable fact lands |
| **oncall** | an alert fired and something is degraded |
| **audit** | weekly, or when the system feels wrong |

| Harness | How to invoke |
|---|---|
| Claude Code | `/frame` (skills in `.claude/skills/`) |
| Codex CLI | `$frame` or the `/skills` picker; Codex also selects a skill implicitly when the task matches its `description` |
| Cursor | reads `.agents/skills/`, `.cursor/skills/`, and `.claude/skills/` |
| Anything else | **open `.agents/skills/<name>/SKILL.md` and follow it step by step** |

That last row is always available. A procedure is a file; the worst case is reading it yourself.

## 8. Work protocol: frame first, capture last

**Read the work record (`<work_dir>/<id>/summary.md`) first** — current state, open items, where
things live.

A unit of work does not start on a question nobody pinned down. `## 0. Framing` holds five agreed
lines, and the linter blocks `status: in_progress` until they are locked:

| Field | Locks when it… |
|---|---|
| **Question** (the unknown) | is falsifiable — a stranger could tell whether it's answered |
| **Goal** (why) | names a decision that changes based on the answer |
| **Objective** (done-when) | is binary — a deliverable plus the bar that closes it |
| **Approach** (how) | someone else could start executing from it |
| **Kill criteria** | states the smallest result that would flip the conclusion |

Trivial work sets `framing_state: "skip: <one-line why>"`. The reason is required.

**The work record is the one place terseness does not apply.** Every finding, dead end, assumption,
caveat, and exact number belongs in it. Length, jargon, and raw identifiers are all fine there — it
is the complete analytical record, and a future session greps it.

## 9. Outward-facing writing

Lead with the answer in the first line, then stop. Someone reading only line 1 must get it. If a
sentence doesn't change what the reader decides or does, cut it. If nothing is decision-relevant,
post nothing.

**Delete on sight:** hedges (I think, seems, appears, probably); throat-clearing (in order to, it's
worth noting, as mentioned); editorializing adjectives (significant, interesting, robust, clearly);
unsolicited next-steps you weren't asked for; any sentence that raises a question you don't answer;
em-dashes (use a period or comma); **internal vocabulary the artifact doesn't define** — if the
reader would have to open a source file to decode the word, it's a variable, not a word.

Exception: identifiers are retrieval keys, so keep the exact string in analytical records and
`knowledge/` — pair it with the plain phrasing rather than dropping it.

**Lint before posting:** `python3 .claude/scripts/lint_comms.py --kind <kind> --file draft.txt`.

## 10. Safety rails

- **Every expensive or risky external call goes through its wrapper script**, never raw. The wrapper
  estimates first, aborts over a threshold, and logs real cost and provenance.
- **Read-only against production by default.** No schema changes, no writes, no deletes without an
  explicit ask.
- **Never hot-patch production to silence an alert.** Diagnose, then clear, re-run, or route to the
  owning team.
- **Sample before full.** Validate on a slice, then extrapolate, then run wide.
- **Never modify a production pipeline definition or push to its main branch.** Feature-flag instead.

## 11. Background and async work

Never passive-wait. A task that **hangs** sends no notification — only a task that completes or
cleanly errors does, so passive waiting can stall silently for hours.

On dispatch, arm a stall detector that polls the task's output mtimes and emits only when they have
been idle past a threshold. **Stall is idle, not slow** — never preempt a long job that is still
writing output.

## 12. Keep the instruction layer current

At the end of any session that established a new convention or operating rule, write it to the file
that owns it and commit. Ask: *did we establish anything today that should change how I operate in
future sessions?*

New procedure goes into a triggered file or a knowledge doc with a pointer here — **not inlined into
this file.** These are living documents; never let them drift from how things actually work.

## 13. Flag unrelated new work as its own unit

When a request is a distinct unit of work unrelated to the active one — a new investigation or build,
not a follow-up or a quick lookup — flag it before diving in. One line: what the work is, a proposed
frame, and why it's separate. Then ask. Scaffold only on a yes.

## 14. Tools that belong to the human

Some tools are the human's own loop. Never auto-create, auto-complete, auto-comment, or read them for
orientation. Silently maintaining someone's task list replaces their planning, which is the point of
the system. Touch them only on an explicit request. Cross-session context lives in git, the work
records, and `knowledge/memory/` — never in the human's planner.

---

## Key paths

| Path | Holds |
|---|---|
| `knowledge/START_HERE.md` | the retrieval front door (task → doc) |
| `knowledge/_ROUTING.md` | generated keyword → doc index (grep this) |
| `knowledge/memory/MEMORY.md` | the small always-loaded hot tier |
| `.claude/scripts/verify.sh` | the doctor: every deterministic check, `--fix` to repair |
| `.claude/scripts/build_index.sh` | regenerate every index (run after any `knowledge/` change) |
| `.claude/scripts/new_ticket.sh` | scaffold a conforming work folder |
| `.githooks/` | the commit gate (activate once: `.claude/scripts/install_git_hooks.sh`) |
| `improvements_backlog.md` | log durable fixes and tech debt here, one row |
| `documentation/ai_workflow_kit/BLUEPRINT.md` | how this whole system works, vendor-neutral |

## Git

Remote: `<GIT_REMOTE>`. Commit and push after every meaningful change, no batching.
