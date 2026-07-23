# Self-Optimizing Context System — Pilot Plan

> Paste this whole file into a **new chat in this workspace** to execute. It is self-contained:
> it carries the prime directive, everything already discovered (so you don't re-inventory), the
> locked decisions, the pipeline, and the runnable workflow script (also at
> `claude-prompts/ticket_context_pilot.js`). Do **not** start executing until the human says go.

---

## 0. Prime directive (the only thing that matters)

The goal is **not** cleanup, formatting, or renaming. The goal is to make every future chat start
from the **most powerful, succinct, correctly-scoped context that exists** — and to know *exactly
where to fetch it*.

Objective function, stated plainly:

> **Minimize the tokens of context a task must load, while maximizing the probability that the
> loaded context is sufficient to answer the task correctly and efficiently.**

The index is the compression. The retrieval eval (§4) is the fitness function. The self-* loop (§10)
is gradient descent on it. The end state is a **self-documenting, self-indexing, self-learning,
self-adjusting** workspace where any new task instantly assembles the most glorious, minimal-yet-
sufficient prompt possible — and gets better every time it's used. Cleanup is a side effect, never
the point.

---

## 1. What already exists (do NOT rebuild — extend it)

This workspace already runs ~two-thirds of this engine, pointed at BQ tables. Reuse it:

- `.claude/scripts/build_index.sh` — **regenerates `tickets/INDEX.md` (epic-aware) and `knowledge/_ROUTING.md`
  from each doc's YAML front-matter.** Runs on any `knowledge/` change.
- `.claude/scripts/lint_tickets.py --check` — structural linter for ticket cards. **Already reports only
  3 violations across 90 cards** (see §2). Structure is healthy; this is not where the value is.
- Per-table catalog `knowledge/bq/<dataset>/<table>.md` + `bq/_COVERAGE.md` (skeleton→enriched→verified)
  — the coverage-ledger pattern to mirror for tickets when scaling.
- Agents: `implementer`, `reviewer-adversarial` (×2, fresh context), `fixer`, `synthesizer`, `curator`
  (`/capture`), `cataloger`. The corpus-crawl loop (implement → 2 reviews → fix) is the template.
- `/capture` skill + `curator` — routes a session's new facts to their home docs.
- Retrieval entry points: `knowledge/START_HERE.md` → `_ROUTING.md` (keyword→doc) → `bq/_TOPICS.md`,
  `bq/_CATALOG_INDEX.md`, `bq/_COVERAGE.md`.

## 2. Current state — already discovered this session (start from here)

- ~69 ticket folders + epics, **90 ticket cards**. `tickets/INDEX.md` is generated, not hand-edited.
- Ticket front-matter carries `doc_type / title / status / date / summary / result`. `summary` + `result`
  are the **index-level** TL;DR and are mostly filled.
- **`_ROUTING.md` has ZERO references to `tickets/`.** Keywords route to BQ tables + knowledge docs only.
  → *Prior-ticket knowledge is not keyword-retrievable.* **This is the #1 gap.**
- `lint_tickets.py` violations (the entire structural debt): `audi_1083_mm_classifying_view` (empty
  `result`), `audi_1091_augmentor_full_source` (no front-matter), `goal_attainment_customer_goal_map`
  (no front-matter). Trivial.
- Loose ends to propose (not auto-fix): stray `tickets/TI_688_IP_Score_Eval.py` (uppercase, not in a
  folder); `ti_xxx_power_analysis_workshop` + `ti_xxx_ticket_theme_analysis` (placeholder numbers);
  `audi_1089` epic children like `ds28_33across` don't follow `prefix_number`.

## 3. The three real gaps (everything else is noise)

1. **No body-level TL;DR card** — the one screen you read *instead of* a 1342-line `summary.md`.
2. **Tickets aren't keyword-routable** — routing ignores them, so past work can't be found by topic.
3. **Knowledge is trapped in ticket summaries** — never delta-extracted into the shared docs.

## 4. The fitness function — the retrieval eval (this is the whole point)

"Good context" is not a vibe; it is a **measurable test**. Given a cold question, can the system route a
fresh chat to the **minimum sufficient** set of docs/tickets/tables/gotchas — and little else?

Canonical probe (make this pass first):

> *"I need MM-campaign performance pre/post after date X — where's the context, the right tables, the
> method, and what did we learn before?"*

A cold chat, using **only** `START_HERE.md` + `_ROUTING.md` + `tickets/INDEX.md`, must reach:
(1) the MM-definition ticket, (2) the pre/post method, (3) the correct perf tables, (4) the
`agg__daily_sum_by_campaign is only from Sep 2025 → use sum_by_campaign_by_day for long pre-periods`
gotcha — **and not a pile of irrelevant docs.**

**Every real cold-start question that fails to route becomes a new eval case → fix the gap → re-run.**
The eval suite is a regression test for context quality: the system provably improves and never regresses.

## 5. The one new artifact — the TL;DR card

Insert at the **top of `summary.md`, immediately after the `---` front-matter close and before the H1**.
Leave the full record below **untouched** (`summary.md` is the complete analytical record; length is fine —
the short artifact is `presentation.md`). Never truncate.

```markdown
## TL;DR
- **Question:**   what was actually asked (1 line)
- **Answer:**     the blessed finding — mirrors front-matter `result:`
- **How:**        method + key tables/technique (1–2 lines)
- **Tables:**     sources touched → what makes this routable
- **Learned:**    1–3 durable facts / gotchas worth keeping
- **Reuse when:** the trigger phrase — "doing MM pre/post", "valuing a 3P vendor"
```

`Reuse when` + `Tables` become a `keywords:` front-matter line. A **one-line extension to
`build_index.sh`** folds those into `_ROUTING.md` so cold chats find the ticket by topic. This finishes
the half-built index feature; it does not add a parallel one.

## 6. Locked decisions (already made with the human)

- **Scope:** pilot **3–5 tickets first**. Tune until the §4 eval passes. Only then sweep all 90, then DE repos.
- **Structure:** auto-apply **safe** fixes (create missing required folders, fill the 3 lint violations,
  rename files to `ti_xxx_*` convention); **propose** risky ones (moves, de-nesting, renumbering) in a gated
  report. Ref-check before any move; prefer archive over delete.
- **Summaries:** **add** the TL;DR card, **keep** the full record. Never compress in place.
- **Facts:** **verify before write.** Cards go straight into their own ticket's `summary.md` (low blast
  radius). Cross-cutting facts destined for `data_catalog.md` / `data_knowledge.md` are **staged for human
  review** (`tickets/_pilot_extracted_facts.md`), never auto-merged (high blast radius — a wrong shared fact
  poisons every future chat).
- **Meta-improvements** to the workflow itself: collected as **proposals**, batched at the end, human-approved,
  committed separately. **Never mutate the workflow mid-run.**

## 7. Pilot set (5 tickets — an MM/pre-post cluster so the eval is meaningful)

- `audi_1083_mm_classifying_view` — MM definitions; also fixes the empty-`result` lint violation
- `audi_1141_mm_vs_3p_by_vertical` — MM vs 3P scorecard
- `ti_390_mmv3_performance` — MM performance (thin)
- `ti_221_pre_post_analysis` — pre/post method (thin, has `queries/`)
- `ti_999_interest_segment_sizing` — 1342-line summary (stress-test card compression)

## 8. The pipeline (deterministic workflow — script in §12 / `ticket_context_pilot.js`)

```
PHASE 1 Extract  [fan-out]  1 read-only agent/ticket → TL;DR card + DELTA facts (grep the docs first;
                            quote source lines; invent nothing)
PHASE 2 Verify   [pipeline] 2 adversarial reviewers/card, source-only ("assume it's wrong"); a card
                            passes only if BOTH confirm
PHASE 3 Land     [barrier]  write cards into each summary.md (parallel, distinct files); add keywords
                            front-matter; stage shared-doc facts for review; build_index.sh; lint; commit
PHASE 4 Eval     [gate]     fresh-context retrieval eval on the §4 MM question → {found, gaps, pass}
```

Read `eval.gaps`, fix routing, re-run Phase 4 until `pass:true`. Then scale.

## 9. Scale path

**5 tickets → all 90 tickets → DE / airflow repos** (`airflow-ti` etc.). Before scaling: make it
**resumable** (a per-ticket coverage ledger mirroring `bq/_COVERAGE.md`) and **commit per-ticket** so the
sweep is reviewable and restartable. Prove the eval on the pilot before spending on volume — same reason
Bun piloted its porting guide on a few files before turning 64 agents loose.

## 10. The self-* loop (how the system keeps improving itself)

- **Self-documenting** — every task completion emits a TL;DR card + delta facts (enforce via the Stop hook /
  `/capture`), so context is captured while fresh, not reconstructed later.
- **Self-indexing** — `build_index.sh` regenerates routing from front-matter on every change; nothing is
  hand-maintained.
- **Self-learning** — the retrieval-eval suite (§4) grows: each cold-start miss becomes a permanent test
  case, so context quality ratchets up and can't silently regress.
- **Self-adjusting** — `request_digest.py` mines the prompt log for recurring question shapes and *proposes*
  new routes/skills; `health_scorecard.py` flags stale / orphan / duplicate docs. Human-gated, append-only,
  no auto-delete. Over time routing reshapes toward the real distribution of questions asked.

## 11. How to run (when the human says go)

Launch the `Workflow` tool with the §12 script (or `scriptPath: claude-prompts/ticket_context_pilot.js`).
It fans out ~5 extractors → 10 adversarial reviewers → writes/commits → runs the eval. Read the returned
`eval` object; if `pass:false`, fix the named `gaps` in routing and re-run only Phase 4. Do not scale until
the eval passes clean on the pilot.

## 12. The workflow script

See `claude-prompts/ticket_context_pilot.js` (identical copy embedded below for portability).

```js
// (identical to claude-prompts/ticket_context_pilot.js — deterministic pilot:
//  Extract → Verify(×2 adversarial) → Land(cards + stage facts + index + commit) → Eval gate)
// Open that file for the full source; it is the runnable artifact.
```
