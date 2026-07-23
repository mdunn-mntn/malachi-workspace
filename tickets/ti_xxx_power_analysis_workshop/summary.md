---
doc_type: ticket
title: "Power Analysis Workshop — Pedagogical Deck + Calculator"
status: in_progress
date: 2026-05-15
summary: "Build a workshop deck and live MDE calculator teaching statistical power at MNTN"
result: "Built and published deck + calculator; awaiting dry run and Jira ticket creation"
keywords: [power analysis, mde calculator, lewis-rao, statistical power, cuped, post-stack variance reduction, workshop deck, revealjs, ti-884, incremental lift, screening rule]
---

## TL;DR

**Q:** What is the TI-XXX Power Analysis Workshop ticket — what was built and its status?

**A:** A standalone, reusable teaching artifact: a 45–60 min workshop teaching statistical power/MDE at MNTN, framed through incremental-lift work, plus a live single-file MDE calculator the audience runs on real advertisers. Built and published (23-slide RevealJS deck + HTML calculator, both on gist.githack), with facilitator notes, a one-pager handout, 6 Tufte charts, a Python reference calculator, and 11 curated advertiser examples. Math verified bit-for-bit against TI-884: Lewis-Rao z-factor 2.80; POST_STACK_MULT=0.5954 (0.934·0.75·0.85); WGU visits MDE raw 0.686% / post-stack 0.408%; Ownerly 5.927% raw (4.7x above reported 0.72%); top-50 tiers 48 visits / 8 CVR / 2 iROAS well-powered (per the copied CSV, treated as source of truth over memory's "11/50 for CVR"). HTML calculator matches Python to <=0.001 pp. Status: built, not yet run; Jira ticket deferred (TI number unassigned, hence the ti_xxx_ folder). Open items: create Jira ticket, book a 45–60 min calendar slot, pick a dry-run partner (Alex Knorr vs Ryan Kleck), decide whether to record a Loom.

**How:** Reused the TI-884 top-50 MDE CSV and math (copied into outputs/), ported TI-884's Python MDE logic to vanilla JS (Beasley-Springer-Moro normInv), regenerated the TI-917 spend curve locally with cohort-actual params (3.5 imps/IP, $24.84 CPM; raw 5% threshold lands at $124k/mo, post-stack at $44k/mo), and stylized the TI-933 pool-or-nothing visualization. Conceptual scaffolding from three concept-explainer videos (Khan Academy / StatQuest / UvA). Deck built via build_workshop_deck.py with charts inlined as base64 (920 KB standalone HTML).

**Learned:**
- Standalone ticket, deliberately NOT a child of BER-2250 and not a v2 of the TI-917 combined Loom — a durable teaching artifact decoupled from any single experiment.
- Jira creation is deferred until return from OOO; the folder stays ti_xxx_ until the TI number is assigned, then rename folder + files.
- Where the numbers come from: WGU/Ownerly/GLD headline contrasts reused from TI-917; Select pool from TI-933; all math cross-validated against TI-884.
- The copied top-50 CSV is treated as source of truth over auto-memory — CSV says 8 CVR well-powered, correcting memory's '11/50'.
- Section 7 declares no knowledge-doc updates expected (pedagogical artifact); any MDE-methodology refinements from dry runs should land in knowledge/experimentation.md.

**Reuse when:**
- Teaching or explaining statistical power / MDE to a mixed leadership+IC audience at MNTN
- Someone needs a live MDE calculator to screen an advertiser before committing test budget
- Reusing TI-884 MDE math, TI-917 spend-curve, or TI-933 pool-or-nothing framing in a new deck
- Planning a workshop dry run or deciding whether to record it as a Loom

# TI-XXX: Power Analysis Workshop — Pedagogical Deck + Calculator

**Jira:** TBD — ticket creation deferred until return from OOO
**Status:** Built; awaiting dry run + Jira creation
**Date Started:** 2026-05-15
**Date Completed:** TBD (after dry run with Alex Knorr or Ryan Kleck)
**Assignee:** Malachi

**Live URLs:**
- Workshop deck: https://gist.githack.com/mdunn-mntn/9ba1ca32c6f7d8d38f7d4e83772e6280/raw/ti_xxx_workshop_deck.html
- MDE calculator: https://gist.githack.com/mdunn-mntn/34c2828f4288d123f5bfaf60f08bc244/raw/ti_xxx_mde_calculator.html

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

Math verified bit-for-bit against TI-884:
- Lewis-Rao z-factor at α=0.05, power=0.80 → **2.80** (JS rational approx: 2.801585).
- WGU visits MDE: raw 0.686% / post-stack 0.408% — matches `ti_884_top50_mde_tiers.csv`.
- Ownerly visits MDE: 5.927% raw — matches TI-884 cross-validation (4.7× above reported 0.72%).
- POST_STACK_MULT = 0.5954 (0.934 · 0.75 · 0.85) — matches TI-884.
- Top-50 tier counts from copied CSV: **48 visits well-powered, 8 CVR, 2 iROAS**. (Memory said "11/50 for CVR" — CSV is the source of truth; 8 is correct as of the TI-884 run.)

Source reuse:
- `outputs/ti_xxx_top50_mde_tiers.csv` — copied from TI-884.
- TI-917 spend curve concept reused but regenerated locally with cohort-actual params (3.5 imps/IP, $24.84 CPM); raw 5% threshold lands at $124k/mo, post-stack at $44k/mo.
- TI-933 pool-or-nothing visualization stylized (exact per-advertiser bounds remain in TI-933's `outputs/`).

## 5. Solution

Built and published:
- `artifacts/ti_xxx_mde_calculator.py` (153 lines) — Python reference, mirrors TI-884 math.
- `artifacts/ti_xxx_mde_calculator.html` — single-file interactive calculator. Beasley-Springer-Moro `normInv` port; matches Python to ≤0.001 pp on all test cases. 8 pre-loaded advertiser examples.
- `artifacts/generate_charts.py` — 6 Tufte-style charts (spend curve, tier waterfall, noise reveal, four states, pool-or-nothing, distribution overlap).
- `artifacts/build_workshop_deck.py` + `ti_xxx_workshop_deck.html` — 23-slide standalone RevealJS deck (920 KB, charts inlined as base64).
- `artifacts/ti_xxx_facilitator_notes.md` — slide-by-slide timing, drill prompts, hand-raise moments, anticipated questions.
- `artifacts/ti_xxx_handout.md` — one-pager with 3 questions, spend-threshold rule, calculator URL.
- `outputs/ti_xxx_screening_examples.csv` — 11 curated advertisers (WGU, Ferguson, Vivint, Hugo, Masterbuilt, Ownerly, GLD, Boll & Branch, Select pool, retargeting cohort, Stage-1 cohort).

## 6. Questions Answered
- **Q:** Should this live under BER-2250?
  **A:** No — standalone ticket. The workshop is a durable, reusable teaching artifact decoupled from any single experiment readout.
- **Q:** Same advertiser examples as TI-917?
  **A:** Yes for headline contrasts (WGU, Ownerly, GLD). New for Select pool (TI-933) and the calculator drill format.

## 7. Data Documentation Updates
None expected — this is a pedagogical artifact, not a data analysis. Any new MDE methodology refinements that emerge during workshop dry runs should land in `knowledge/experimentation.md`.

## 8. Open Items / Follow-ups
- Create Jira ticket on return from OOO
- Confirm workshop slot on the calendar (45–60 min)
- Decide on dry-run partner (Alex Knorr vs Ryan Kleck)
- Decide whether to record (Loom) for async distribution after live delivery
