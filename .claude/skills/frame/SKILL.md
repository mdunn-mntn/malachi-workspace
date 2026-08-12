---
name: frame
description: >-
  Frame a ticket BEFORE work starts — agree the single question it answers, why it matters, what
  "done" looks like, and how we'll answer it. A Socratic interview that pulls the Jira ticket and the
  north-star, sharpens a vague ask into a falsifiable Question / decision-anchored Goal / binary
  Objective / executable Approach, writes them to §0 Framing in summary.md, and locks the framing gate.
  This is the start-of-ticket bookend to /capture (which closes a ticket). Invoke when the user says
  "frame this", "frame TI-XXX", "scope this ticket", "what are we actually trying to answer here",
  "define the question", or before starting analysis on any new ticket.
---

# /frame — Ticket framing gate

A ticket should not go `status: in_progress` on a question nobody has pinned down. This skill runs the
agreement **before** the work: it forces a clear Question, Goal, Objective, and Approach, writes them to
`## 0. Framing` in the ticket's `summary.md`, and flips `framing_state: locked` so `lint_tickets.py`
lets the ticket move to in_progress.

`/frame` **opens** a ticket the way `/capture` **closes** it. Same shape: a judgement interview backed
by a deterministic gate.

**This one DOES pause for the user.** Unlike `/capture`, the whole point is to force *the user's*
thinking, so the interview is interactive — do not silently write a best-guess frame and lock it.

**Args (optional):** `/frame <TI-XXX>` pins the ticket. `/frame` with no args resolves the active ticket
(named ticket → folder with uncommitted changes → the ticket dominating this conversation → most recently
modified `tickets/**/summary.md`). If genuinely ambiguous, ask which ticket in one line.

---

## The frame (5 lines — do not invent more)

Pin each field to a distinct logical role so they can't collapse into each other:

| Field | Role | Locks only when it passes… |
|---|---|---|
| **Question** | the unknown | **Falsifiable** — a stranger could tell whether it's been answered. "Does X move Y, and by how much?" not "look into X." |
| **Goal** | why / the decision | Names a **decision** that changes based on the answer, + who's waiting, + the north-star tie. |
| **Objective** | what / done-when | **Binary** — a concrete deliverable + the bar that closes it. Either it exists and clears the bar, or it doesn't. |
| **Approach** | how | Someone else could **start executing** from it — data sources, method/protocol, assumptions to resolve first. |
| **What would change the answer** | kill criteria | The smallest result that flips the conclusion. Keeps scope honest; stops effort on questions where no result changes the decision. |

Why → What → Unknown → How is also the interview order.

## Step 1 — Orient (read before you ask)

1. **Resolve the ticket** (see Args) and read its `summary.md` — it may already have a partial §0.
2. **Pull the Jira ticket** for the real ask (title, description, comments):
   ```bash
   curl -s -u "malachi@mountain.com:${JIRA_API_TOKEN}" \
     "https://mntn.atlassian.net/rest/api/2/issue/TI-XXX?fields=summary,description,comment,issuetype,parent" \
     | jq '{summary,description:.fields.description,type:.fields.issuetype.name,parent:.fields.parent.key}'
   ```
3. **Read `knowledge/strategic_north_star.md`** — the Goal must tie to a real priority. This is the
   leverage check made concrete: if the ticket connects to no OKR / leadership ask / velocity multiplier,
   surface that here (Tier-4 flag) *before* framing, not after the work.
4. **Note the ticket shape.** If the Question is "did this change move a KPI?" (feature flip, rollout,
   A/B, holdout, vendor lift, BUK, BER-2250), the Approach must adopt the **Experiment Analysis Protocol**
   (`knowledge/experimentation.md` § Standard Analysis Protocol) — power → cohort/flip-date → DiD+bootstrap
   → CausalImpact(VIF→BIC) → scheduled output. Load it before drafting Approach.

## Step 2 — Interview (Socratic; sharpen, don't accept the vague version)

Draft your best first-pass 5-field frame from Step 1, then put the **genuinely unresolved** forks to the
user with `AskUserQuestion` (usually 2–4 questions). Ask only what you can't settle from the ticket + the
north-star + the data docs — don't ask what you can look up. Aim the questions at the failure the field
must pass:

- **Question not falsifiable?** → "What's the specific quantity and comparison — 'X vs Y, by how much'?"
- **Goal has no decision?** → "What decision or action changes based on the answer, and who's waiting on it?"
- **Objective not binary?** → "What's the deliverable, and what bar makes it 'answered' vs 'not yet'?"
- **Approach hand-wavy?** → "Which table is the source of truth, what's the control/comparison, what
  assumption would sink this if wrong?"

Push back on a fuzzy answer once. If the user genuinely wants it loose (exploratory spike), that's a valid
frame — say so and capture it honestly as an exploratory Objective, don't fake precision.

## Step 3 — Write §0 and lock

1. Write the agreed 5 lines into `## 0. Framing` in `summary.md` (replace the `{stub}` text). Keep each
   line tight — this is the analytical record's head, not a presentation.
2. Set the front-matter: `question: "<the one-line falsifiable question>"` and `framing_state: locked`.
3. Seed the rest of the record from the frame: the Approach's assumptions become §1/§3 starting points
   (the empirical-analysis unknowns list); if the Experiment Protocol applies, note it in §3 Plan of Action.
4. **Confirm the gate passes:**
   ```bash
   python3 .claude/scripts/lint_tickets.py --check 2>&1 | grep -i "$(basename $(dirname <summary_path>))" || echo "framing OK"
   ```
   `framing_state: locked` requires a real `question` — the lint will catch a lock with an empty question.

### The skip hatch (trivial tickets only)
CLAUDE.md already says not every ticket needs the full treatment (one-line bug fix, housekeeping, a quick
config change). For those, don't force a frame — set `framing_state: "skip: <one-line why>"` instead of
locking. The gate accepts skip; the reason is required so a skip is a decision, not an oversight. Use this
sparingly — anything that produces an analysis, a number, or a recommendation gets framed.

## Step 4 — Report, then commit

Report the locked frame back in five lines (Question / Goal / Objective / Approach / What-would-change),
then commit:

```bash
cd /Users/malachi/Developer/work/mntn/workspace && \
  git add tickets/<ticket>/ && \
  git commit -m "TI-XXX: frame — lock §0 (question/goal/objective/approach)" && git push origin main
```

**Stage the paths you touched — never `git add .`/`-A`.** Other Claude sessions share this working tree, so a blanket add sweeps their in-flight edits into your commit (observed twice, most recently 2026-08-12, in both directions). `git diff --cached --name-only` before committing. See [[feedback_shared_worktree_commits]].

If the ticket was skipped, say so with the reason and commit the `skip:` state the same way.
