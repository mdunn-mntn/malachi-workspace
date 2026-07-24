---
name: workflow-audit
description: >-
  The System-retro loop — the workflow reviews itself. Runs every read-only check in the kit
  (structure conformance, ticket/framing-gate adherence, knowledge-base health, catalog coverage
  debt, perf drift, request patterns), reasons over the signals, and writes ONE prioritized,
  PROPOSE-ONLY action list to claude-prompts/workflow_audits/. Human-gated: it proposes fixes and
  standards edits, it never executes deletes/moves/edits to knowledge, tickets, or CLAUDE.md.
  Invoke when the user says "audit the workflow", "run the workflow audit", "are we adhering to the
  standard", "what should we improve", "system retro", or on the weekly cloud-routine schedule.
---

# /workflow-audit — the workflow reviews itself (propose-only)

This is the **System-retro loop** from `claude-prompts/self_improvement_engine_plan.md`: the cadence
trigger that turns the always-on deterministic signals into a single, prioritized list of things to
fix or improve — *and* proposes edits to the standards themselves (CLAUDE.md / skills / agents).

## The one hard rule — propose, never execute

This skill has **no delete, move, or edit authority** over knowledge docs, tickets, `.claude/`, or
CLAUDE.md. The **only** files it writes are (1) its own dated report and (2) the git commit of that
report. Every fix it identifies is written as an **AWAITING APPROVAL** item with the exact command or
diff for the human to run. This mirrors the kit's self-improvement principle: *read/append-only, no
delete authority — a human decides.* If you ever feel the urge to "just fix it while I'm here" — don't.
The value is the ranked proposal, not silent drift.

**Args (optional):**
- `/workflow-audit` — full weekly audit (all sections).
- `/workflow-audit adherence` — structure + ticket/framing conformance only.
- `/workflow-audit perf` — perf-drift → doc-update proposals only.
- `/workflow-audit requests` — request-log → /skill proposals only (**local only**; needs the gitignored request log).
- `/workflow-audit retro` — the deeper monthly pass: also challenge whether the *standards themselves*
  are still right (are any CLAUDE.md rules now stale, contradictory, or ignored in practice?).

---

## Step 1 — Gather the deterministic signals

Run the aggregator and read its full output. It never fails; it prints one markdown rollup.

```bash
bash "$CLAUDE_PROJECT_DIR/.claude/scripts/workflow_audit.sh"
```

Note the **Environment** line at the top:
- `local` → the request log is present; the request-mining section (§6) is real.
- `fresh checkout / cloud` → §6 is SKIPPED (the request log is gitignored/local-only). In the report,
  flag request-mining as "run `/workflow-audit requests` on the local Mac" — do **not** pretend it ran.

## Step 2 — Reason over each signal → decide if there's an actionable proposal

For each section of the rollup, decide: is there something a human should *do*, or is this just noise?
Only surface items that change what someone does. Drop clean/green sections to a one-line "✓ clean."

| Signal (rollup §) | Turn into a proposal when… | Proposed action shape |
|---|---|---|
| §1 Structure | a real naming/misfile/junk/empty-dir issue exists (not a blessed carve-out) | exact `git mv` / `rm` / gitignore line, tiered Safe vs Judgment |
| §2 Ticket/framing | a card is `in_progress`/`done` with `framing_state: draft`, or a legacy card was touched recently | `/frame <TI-XXX>` on the specific cards; never blanket-frame every legacy card |
| §3 KB health | `>0 stale docs`, orphans, dup-titles, or `days-since-/capture` is high | name the specific docs; propose `/capture` or a targeted refresh |
| §4 Coverage debt | undocumented-queue > 0, or skeleton docs exist | propose a cataloger pass on the top-N worst |
| §5 Perf drift | a table/query is repeatedly expensive or repeated (cache/dedup candidate) | propose the specific doc-update / query fix, cite the table |
| §6 Requests | a verb+noun shape recurs ≥ the threshold | propose ONE named `/skill` with a one-line spec — human decides |
| §7 Git hygiene | uncommitted work is sitting, or untracked non-ignored files | flag what's uncommitted; propose commit or gitignore |
| §8 Standards drift | a skill/script isn't referenced in CLAUDE.md | propose the CLAUDE.md line to add |

**Ranking:** order the final list by leverage, not by section. A framing-gate violation or a stale
source-of-truth doc outranks 30 empty scaffold dirs. Group into three tiers, matching the existing
`workspace_audit_manifest.md` convention:
- **Tier 1 — Safe** (mechanical, reversible: junk delete, gitignore, empty-dir cleanup)
- **Tier 2 — Judgment** (your call per item: renames, which CSVs to keep, missing cards)
- **Tier 3 — Standards** (reconcile a rule: CLAUDE.md / folder_definitions / a skill or agent)

## Step 3 — System-retro layer (always for `retro`, lightly otherwise)

Look beyond the mechanical signals and ask the questions a hook can't:
1. **Are the standards being followed in practice?** Scan the last ~20 commits and any open tickets. Is
   there a rule in CLAUDE.md that the recent work quietly ignored? (e.g. commits not tied to a ticket,
   presentations built without the critique pass, naming drift.) Name the gap; propose the smallest fix.
2. **Is any standard now stale or self-contradictory?** (e.g. a memory line names a decommissioned
   service; two docs give conflicting guidance.) Propose the exact edit — as a proposal, human-gated.
3. **What's the single highest-leverage workflow improvement** the signals point to this week? State it
   in one line at the top of the report as the **headline**.

Keep this honest and small. One sharp proposed standards edit beats ten vague "consider improving X."

## Step 4 — Write the report (the only file this skill authors)

Write to `claude-prompts/workflow_audits/audit_YYYY_MM_DD.md` (use today's date from the rollup header).
Structure:

```
# Workflow Audit — YYYY-MM-DD

**Headline:** <the one highest-leverage improvement this week, ≤20 words>

**Environment:** <local | cloud>  ·  **Cards:** N (V violations, W warns)  ·  **Coverage:** …

## Tier 1 — Safe (mechanical, reversible)
- [ ] <what> — `<exact command>` — _why_

## Tier 2 — Judgment (your call)
- [ ] <what> — `<exact command or the decision>` — _why_

## Tier 3 — Standards (reconcile a rule)
- [ ] <proposed CLAUDE.md / skill / agent edit, quoted> — _why_

## ✓ Clean this pass
- <one line per green signal>

## Skipped / needs local run
- <e.g. request-mining if this ran in the cloud>
```

Every actionable line is a `- [ ]` checkbox with the exact command/diff and a short _why_. Nothing here
is executed by this skill.

## Step 5 — Commit the report, print a terse digest, stop

```bash
cd "$CLAUDE_PROJECT_DIR" && git add claude-prompts/workflow_audits/ .gitignore && \
  git commit -m "workflow-audit: $(date +%Y-%m-%d) — N proposals (T1 x, T2 y, T3 z)" && git push origin main
```

Then print to the session, terse (lead with the answer):
- The headline.
- Counts per tier.
- The top 3 items by leverage, one line each.
- If cloud: "request-mining skipped — run `/workflow-audit requests` locally."

Do **not** apply any Tier item. The user reviews the report and runs the ones they approve. If the user
replies "do Tier 1" / "apply items 1–3", *then* execute those specific approved items (and only those).

---

## How this is scheduled (the compliant, key-free split)

The audit is split across two machines so no Claude credential ever lives on always-on infra:

- **Pi cron — deterministic half (weekly, Mon 08:00 PT).** `~/run_workflow_audit.sh` on pi5 (source of
  truth: `.claude/scripts/pi_run_workflow_audit.sh`) runs ONLY `workflow_audit.sh` — pure Python + git,
  **no API key, no model** — and commits a dated `signals_<date>.md`. This guarantees the signals are
  captured every week even when the Mac is asleep. **Never add an `ANTHROPIC_API_KEY` to the Pi** — that
  is the pattern MNTN security decommissioned (Slack bot, 2026-06-10).
- **Mac — reasoning half (this skill).** At your next session, `/workflow-audit` runs under your existing
  Claude Code auth: it re-runs the aggregator (fresh signals, now including the local-only request log),
  reasons over it, and writes the ranked propose-only `audit_<date>.md`.

Because the Pi runs on a fresh checkout, its signal file marks §6 request-mining SKIPPED (the request log
is gitignored/local-only). The full request-mining only appears when the skill runs on the Mac. The Pi
never reasons, never proposes, never applies — it only captures and commits raw signals.
