---
name: capture
description: >-
  Sweep the current session for everything learned and route each fact to its correct home —
  the knowledge/*.md docs, the active ticket's summary.md, the auto-memory files, and the
  self-review — correcting anything now outdated, then commit and push. Invoke when the user
  says "capture", "log what we learned", "update the knowledge docs", "did we miss anything",
  "make sure that's written down", or at any natural stopping point in ticket work.
---

# /capture — Knowledge sweep

The user learns a lot each session and does not want valuable knowledge to evaporate when the
context window closes. This skill is the durable-memory checkpoint: it reviews the **entire
current session**, extracts every fact worth keeping, writes each one where a future session
would actually look for it, fixes anything the session just proved wrong, and commits.

Do the work — write the files, correct the stale lines, commit, and push. Do **not** propose a
list and wait for approval. Report what you changed *after* you've changed it.

**Args (optional):** `/capture` with no args runs a full sweep. `/capture <TI-XXXX>` pins the
active ticket. `/capture <doc>` (e.g. `data_knowledge`, `memory`, `summary`) scopes the sweep
to one destination.

---

## Step 1 — Orient

1. **Identify the active ticket.** In order of preference: the ticket named in `/capture`'s
   args → the ticket folder with uncommitted changes (`git status`) → the ticket dominating
   this conversation → the most recently modified `tickets/**/summary.md`. If it's genuinely
   ambiguous, ask which ticket in one line; otherwise proceed silently.
2. **Confirm the four destinations exist** (they always should): `knowledge/*.md`,
   `tickets/<active>/summary.md`, the memory dir + `MEMORY.md`, `self_review/self_review_2.md`.

## Step 2 — Enumerate candidate learnings (be exhaustive first, filter second)

Scan back over the **whole session** and write yourself an explicit list of every candidate
fact. Casting wide here is the point — completeness is the user's stated fear. Look for:

- New table / column / dataset discovered, or a schema detail confirmed
- Join key confirmed **or disproven**; join cardinality established
- A data-quality issue, gotcha, or footgun found
- Business logic clarified (what a flag/field/status actually means)
- A source-of-truth table or field identified (or one demoted as unreliable)
- BQ vs Greenplum behavioral difference; TTL / partition / clustering behavior confirmed
- Experiment-methodology lesson (covariate choice, test design, inference, a confounder)
- General MNTN business knowledge (product, org, strategy, industry, terminology, a person's ownership)
- **Feedback on how to work** — a correction the user gave, or an approach they confirmed
- A **project status change** — something started/finished/blocked/paused/reassigned
- A ticket finding, dead end, answered question, or new open question

## Step 3 — Apply the quality bar (drop the noise)

Keep a candidate only if it is **durable and non-obvious**. Drop it if:

- The repo already records it (code structure, a prior fix, git history, an existing doc line)
- It only matters to this one conversation (a transient value, a one-off command)
- It's a guess — capture only what was empirically verified this session, or mark it clearly as unconfirmed

If the user asked to "remember" something the repo already captures, save instead the
*non-obvious* thing about it (why it surprised you, the gotcha), not the bare fact.

## Step 4 — Route each survivor to its home

| The fact is about… | Home |
|---|---|
| Table schema, partitions, clustering, join keys, query/perf tips | `knowledge/data_catalog.md` |
| Business logic, gotchas, disambiguation, tribal knowledge, source-of-truth calls | `knowledge/data_knowledge.md` |
| Products, strategy, org, industry, terminology, who-owns-what | `knowledge/mntn_business.md` |
| Experiment methodology, covariate selection, test design, inference lessons | `knowledge/experimentation.md` |
| A finding, dead end, answered/open question, status, conclusion for **this ticket** | `tickets/<active>/summary.md` |
| A cross-session fact about the user, how to work (feedback), a project's state, or an external pointer | a memory file (see Step 6) |
| A completed ticket / meaningful piece of work | `self_review/self_review_2.md` |

A single fact can legitimately land in two places (e.g. a gotcha in `data_knowledge.md` **and**
a one-line memory pointer). That's fine — route it to each home that would want it.

Match the **existing section and voice** of the target doc. These files are large and highly
structured — find the right existing section and extend it; create a new section only when none
fits. Convert relative dates to absolute (today is knowable from the environment).

## Step 5 — Correct what's now outdated (not just append)

This is the half the user most often loses. For each survivor, before writing, `grep` the target
doc for the topic. If the session **contradicted or superseded** an existing line:

- **Edit the stale line in place** — don't leave both the old and new claim sitting in the doc.
- If the old claim is worth remembering as history, mark it explicitly (`(superseded 2026-…: …)`),
  otherwise replace it.
- If a memory file is now wrong, fix it; if it's now false entirely, delete the file **and** its
  `MEMORY.md` line.

Contradiction-hunting is a first-class part of the sweep, equal to adding new facts.

## Step 6 — Memory files (follow the format exactly)

Memory now lives IN the repo at `knowledge/memory/` (the native memory dir is a symlink to it), so each
file is indexed like any knowledge doc. Before creating one, **grep `knowledge/_MEMORY_INDEX.md` and
`knowledge/_ROUTING.md` for the topic** — if a file already covers it, UPDATE that file rather than
duplicating (this is what stops overlap clusters forming). Only create a new file when nothing fits.

Each new file is `knowledge/memory/<slug>.md`, where `<slug>` = the filename stem = the `name:` value:

```markdown
---
name: <slug>                       # MUST equal the filename stem, so [[wikilinks]] resolve
description: <one-line summary used for recall relevance — keep it to ~1 tight sentence>
metadata:
  node_type: memory
  type: user | feedback | project | reference
doc_type: memory                   # folds this file into _ROUTING.md (the one grep surface)
keywords: [term, entity, symptom]  # 5-10 terms a future session would grep to FIND this: topic name + concrete entities (table names, DS ids, people, repos, ticket ids) + synonyms
domain: [<1-3 from the list>]      # workflow · bigquery · experimentation · audience-scoring · bidding · identity · incrementality · pricing · infra · repos · routing-people · jira-process · leadership · project · data-catalog · business
lifecycle: active                  # active | superseded | archived
last_verified: <today>
---

<the fact. For feedback/project, add **Why:** and **How to apply:** lines. Link relatives with [[other_slug]].>
```

**Do NOT add a line to `MEMORY.md`.** `MEMORY.md` is now the small always-loaded HOT TIER — reserved for
facts relevant to (nearly) every session. A new task-specific fact is reached via a `_ROUTING.md` grep +
`_MEMORY_INDEX.md`; it does not go in `MEMORY.md`. Only add a `MEMORY.md` line if the fact is genuinely
always-on (a new global working rule or stack gotcha) — rare.

**Lifecycle (retire, don't delete):** when a project finishes, set its memory's `lifecycle: archived` and
drop any `MEMORY.md` line it had (the file stays grep-reachable). If a fact is superseded, set
`lifecycle: superseded` or edit it in place. Only delete a file when it is entirely false (Step 5).

**After writing/editing any memory file, run** `bash .claude/scripts/build_index.sh` so the new keywords
fold into `_ROUTING.md` and the memory indexes (`_MEMORY_INDEX.md`, `_MEMORY_LIFECYCLE.md`) regenerate.

## Step 7 — Report, then commit

Report a tight, scannable summary of what the sweep did — grouped by destination:

```
📚 knowledge/data_knowledge.md  — added §X (<gotcha>); corrected §Y (<old> → <new>)
📗 knowledge/data_catalog.md    — added <table> join key
📝 ti_1037/summary.md           — logged finding <…>; closed open Q <…>
🧠 memory                       — new feedback_<slug>; updated project_<slug>
📈 self_review_2.md             — entry for <work>
🗑️  removed                      — memory_<slug> (no longer true)
Nothing to add for: mntn_business, experimentation
```

Always state the "nothing to add" destinations too — that's the *verification* the user wants:
proof the sweep looked everywhere, not just where it found something.

Then commit and push everything in one commit (per the workspace constant-commit rule). Memory files
now live in the repo at `knowledge/memory/`, so `git add .` versions them like any other change:

```bash
cd /Users/malachi/Developer/work/mntn/workspace && git add . && \
  git commit -m "TI-XXXX: capture — <one-line of what was logged/corrected>" && \
  git push origin main
```

If the sweep genuinely found nothing durable, say so plainly and skip the commit — don't
manufacture edits to look busy.
