# TI-XXX: Power Analysis Workshop — Pedagogical Deck + Calculator

**Jira:** TBD — ticket creation deferred until Malachi returns from OOO
**Status:** Scaffolded (folder only; awaiting build)
**Date Started:** 2026-05-15 (folder scaffold)
**Date Completed:** TBD
**Assignee:** Malachi

---

## 1. Introduction
A net-new, durable, reusable teaching artifact: a 45–60 minute workshop that explains statistical power in detail, framed through MNTN's incremental lift work, with a live MDE calculator the audience uses on real advertisers. Mixed audience (leadership + ICs). Standalone ticket — not a child of BER-2250 and not a v2 of the TI-917 combined Loom.

Conceptual scaffolding comes from three videos (Khan Academy / StatQuest / UvA) at `/Users/malachi/Library/Mobile Documents/com~apple~CloudDocs/three_videos_transcripts.md`. MNTN substance is already gathered in TI-917 (TI-837 lift + TI-884 power), TI-884 (Lewis-Rao MDE math, measured MNTN CUPED ρ=0.357, top-50 tiering), and TI-933 (Select pool-or-nothing story).

## 2. The Problem
Lift studies at MNTN are routinely reported without a stated MDE, so the room can't tell credible findings from statistical noise. TI-917 produced the screening rule and a Loom; what's missing is a deeper, hands-on teaching artifact that makes the rule self-administered — anyone can plug an advertiser into the calculator and decide whether a test is worth running *before* budget is committed.

## 3. Plan of Action
Full plan: `/Users/malachi/.claude/plans/there-is-a-three-video-generic-bachman.md`

1. Scaffold ticket folder (done 2026-05-15)
2. Create Jira ticket (deferred — TI-XXX, Task, 3 SP, q2_2026, PMO Rep Bryce Wagg, Release Type Backend)
3. Build `ti_xxx_workshop_deck.html` (RevealJS, standalone, 45–60min three-act)
4. Build `ti_xxx_mde_calculator.html` (vanilla JS, single file, port of TI-884 Python logic)
5. Curate `outputs/ti_xxx_screening_examples.csv` (8–12 advertiser rows from TI-884 top-50)
6. Write `artifacts/ti_xxx_facilitator_notes.md` (timing, prompts, hand-raise moments)
7. Write `artifacts/ti_xxx_handout.md` (one-pager takeaway)
8. Run critique (`claude-prompts/presentation_critique.md`) against `presentation.md`
9. Dry run with Alex Knorr or Ryan Kleck
10. Rename folder + files once TI number is assigned

## 4. Investigation & Findings
*(To be filled during build. Source material already gathered in TI-917/TI-884/TI-933.)*

Reuse, don't rebuild:
- TI-884 MDE math + top-50 tier CSV → copy into `outputs/`
- TI-917 spend-threshold curve chart (slide 17) → reuse
- TI-933 per-advertiser CI chart + pooled headline (+2.055pp) → reuse
- `knowledge/experimentation.md` Lewis-Rao section → cite, don't restate

## 5. Solution
*(To be filled when deck + calculator are built.)*

## 6. Questions Answered
*(To be filled.)*

## 7. Data Documentation Updates
None expected — this is a pedagogical artifact, not a data analysis. Any new MDE methodology refinements that emerge during workshop dry runs should land in `knowledge/experimentation.md`.

## 8. Open Items / Follow-ups
- Create Jira ticket on return from OOO
- Confirm workshop slot on the calendar (45–60 min)
- Decide on dry-run partner (Alex Knorr vs Ryan Kleck)
- Decide whether to record (Loom) for async distribution after live delivery
